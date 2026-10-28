import ee
from datetime import date

from utilities.constants import AEZ
from utilities.gee_utils import (
    export_raster_asset_to_gee,
    is_gee_asset_exists,
    ee_initialize,
)

"""
 * Forest Sensitivity Analysis Pipeline — Fire Resistance & Resilience
 * Fire Shock Resistance & Resilience (Harmonized kNDVI + Signed Formulas)
 *
 * AGRICULTURAL YEAR CONVENTION:
 * Year `y` = Jul 1 of `y` -> Jun 30 of `y+1`, matching Scripts 1/3a/3b/fire_index.
 * kNDVI compositing windows, zScore band lookups, and the baseline (Yn_bar)
 * window are all on this convention. Tree-cover asset (Script 1) is also
 * ag-year now, so startYearTree/endYearTree comparisons against `year`
 * carry no fuzz.
 *
 * Signed resistance (both +ve and -ve events):
 * Resistance = Yn_bar / |Ye - Yn_bar| × sign(Ye - Yn_bar)
 *
 * Resilience = |Ye - Yn_bar| / |Ye+1 - Yn_bar| × sign(Ye+1 - Yn_bar)
 * Stored for every fire year that has a next-year kNDVI.
 * classify_fire_metrics averages the yearly class numbers.
 * Resilience is stored only when Ye / Yn_bar < 0.95.
 *
 * Harmonization: Transforms Landsat 8/9 (OLI) to Landsat 5/7 (ETM+)
 * equivalent before computing kNDVI to eliminate sensor-shift bias.
 *
 * Requires:
 * - Forest mask asset (Script 1)
 * - Fire index asset (FRP > 30) — must have been exported with an
 *   end_year >= this script's zMaxYear (fire_index now enforces its
 *   start/end_year fully covers the baseline window, so this holds as
 *   long as fire_index's own defaults/args aren't narrowed).
"""


def forest_fire_sensitivity(aez, start_year=2004, end_year=2024, gee_account_id=None):
    ee_initialize(gee_account_id)

    TREE_COVER_ASSET = f"projects/corestack-datasets-alpha/assets/datasets/hazards/tree_mask/Hybrid_Tree_AEZ_{aez}_{str(2004)}_{str(end_year)}"

    FIRE_INDEX_ASSET = f"projects/corestack-datasets-alpha/assets/datasets/hazards/fire_index/fire_index_FRP30_AEZ_{aez}"

    OUTPUT_DESC = f"fire_metrics_harmonized_kNDVI_AEZ_{aez}"
    OUTPUT_ASSET_ID = f"projects/corestack-datasets-alpha/assets/datasets/hazards/fire_Rt_Re/{OUTPUT_DESC}"

    if is_gee_asset_exists(OUTPUT_ASSET_ID):
        return None, OUTPUT_ASSET_ID

    if end_year is None:
        raise ValueError(
            "end_year must be specified explicitly — agricultural-year pipelines "
            "cannot infer a safe default."
        )

    Z_THRESHOLD = 1.0

    # I'm using 2004-2024 to match the same baseline window I already locked
    # in for drought (Script 2), rain (Script 3b), and the fire z-score
    # itself (fire index script) — keeping all my baselines consistent with
    # each other. These are ag-year labels now, same as everywhere else.
    BASELINE_START_YEAR = 2004
    BASELINE_END_YEAR = 2024

    # zScoreCol needs bands through zMaxYear from the fire_index asset, and
    # kndviCol needs data one year further out than that for resilience on
    # the final analysis year — same completeness reasoning as Script 3b.
    zMinYear = min(start_year, BASELINE_START_YEAR)
    zMaxYear = max(end_year, BASELINE_END_YEAR)

    kndviMinYear = min(start_year, BASELINE_START_YEAR)
    kndviMaxYear = max(end_year + 1, BASELINE_END_YEAR)
    # Last scored year is the last year whose next year is fetched.
    metric_end_year = min(end_year, kndviMaxYear - 1)

    # kndviMaxYear is the furthest single ag-year fetched anywhere in this
    # script; that ag-year isn't complete until Jun 30 of kndviMaxYear+1.
    # Refuse to silently compute on a partial year.
    required_complete_by = date(kndviMaxYear + 1, 6, 30)
    if required_complete_by > date.today():
        raise ValueError(
            f"Agricultural year {kndviMaxYear} (Jul {kndviMaxYear} -> "
            f"Jun {kndviMaxYear + 1}) is not yet complete as of "
            f"{date.today().isoformat()}. Reduce end_year."
        )

    aoi = ee.FeatureCollection(AEZ).filter(ee.Filter.eq("ae_regcode", aez)).geometry()
    # aoi = (
    #     ee.FeatureCollection("projects/ext-datasets/assets/datasets/State_pan_india")
    #     .filter(ee.Filter.eq("Name", "Odisha"))
    #     .geometry()
    # )
    # Tree-mask asset names embed end_year and auto-version; fire-index
    # asset names don't, and silently no-op on re-run unless deleted first
    # (see is_gee_asset_exists guard in fire_index). That mismatch means
    # it's easy to expand end_year, regenerate the tree mask, forget to
    # regenerate fire_index, and end up reading a stale fire-index asset
    # against a fresh tree-mask one. Fail loudly instead.
    if not is_gee_asset_exists(TREE_COVER_ASSET):
        raise ValueError(
            f"Tree cover asset not found: {TREE_COVER_ASSET}. Run "
            f"generate_hybrid_tree_mask with end_year={end_year} first."
        )
    if not is_gee_asset_exists(FIRE_INDEX_ASSET):
        raise ValueError(f"Fire index asset not found: {FIRE_INDEX_ASSET}.")

    requiredZBands = {f"zScore_{y}" for y in range(zMinYear, zMaxYear + 1)}
    availableZBands = set(ee.Image(FIRE_INDEX_ASSET).bandNames().getInfo())
    missingZBands = sorted(requiredZBands - availableZBands)
    if missingZBands:
        raise ValueError(
            f"{FIRE_INDEX_ASSET} is missing bands: {missingZBands}. It was "
            f"likely exported with a smaller end_year — re-run fire_index "
            f"with end_year >= {zMaxYear} (delete the existing asset first, "
            "its name doesn't auto-version)."
        )

    # Loading the assets :=
    treeMeta = ee.Image(TREE_COVER_ASSET)
    startYearTree = treeMeta.select("start_year")
    endYearTree = treeMeta.select("end_year")

    fireIndex = ee.Image(FIRE_INDEX_ASSET)

    # I need zScore bands covering BOTH my analysis window and my baseline
    # window — whichever stretches further in either direction. Right now
    # they're the same range (2004-2024), so this doesn't change anything
    # today, but it protects me for later when I extend END_YEAR and the two
    # windows stop lining up.
    zScoreCol_list = []

    for y in range(zMinYear, zMaxYear + 1):
        zScoreCol_list.append(
            fireIndex.select("zScore_" + str(y)).rename("zScore").set("year", y)
        )

    zScoreCol = ee.ImageCollection(zScoreCol_list)

    # LANDSAT HARMONIZATION & kNDVI :=
    chastainBandNames = ["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2"]
    oliETMSlopes = ee.Image.constant(
        [1.03501, 1.00921, 1.01991, 1.14061, 1.04351, 1.05271]
    )
    oliETMIntercepts = ee.Image.constant(
        [-0.0055, -0.0008, -0.0021, -0.0163, -0.0045, 0.00261]
    )

    # Pre-process Landsat 5/7 (Baseline)
    def prepL57(image):
        qa = image.select("QA_PIXEL")
        mask = qa.bitwiseAnd(1 << 3).eq(0).And(qa.bitwiseAnd(1 << 4).eq(0))

        scaled = (
            image.updateMask(mask)
            .select(["SR_B1", "SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B7"])
            .multiply(0.0000275)
            .add(-0.2)
        )

        return scaled.rename(chastainBandNames).copyProperties(
            image, ["system:time_start"]
        )

    # Pre-process Landsat 8/9 and Harmonize to ETM+
    def prepL89(image):
        qa = image.select("QA_PIXEL")
        mask = qa.bitwiseAnd(1 << 3).eq(0).And(qa.bitwiseAnd(1 << 4).eq(0))

        scaled = (
            image.updateMask(mask)
            .select(["SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B6", "SR_B7"])
            .multiply(0.0000275)
            .add(-0.2)
            .rename(chastainBandNames)
        )

        harmonized = scaled.multiply(oliETMSlopes).add(oliETMIntercepts)
        return harmonized.copyProperties(image, ["system:time_start"])

    # Calculate ag-year median kNDVI (Jul 1(year) -> Jun 30(year+1))
    def getAnnualKNDVI(year):
        start = ee.Date.fromYMD(year, 7, 1)
        # end-exclusive filterDate, so advance 1 day past Jun 30 to include it
        end = ee.Date.fromYMD(ee.Number(year).add(1), 6, 30).advance(1, "day")

        l89 = (
            ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
            .merge(ee.ImageCollection("LANDSAT/LC09/C02/T1_L2"))
            .filterDate(start, end)
            .filterBounds(aoi)
            .map(prepL89)
            .map(
                lambda img: img.normalizedDifference(["NIR", "RED"])
                .pow(2)
                .tanh()
                .rename("kndvi")
            )
        )

        l57 = (
            ee.ImageCollection("LANDSAT/LT05/C02/T1_L2")
            .merge(ee.ImageCollection("LANDSAT/LE07/C02/T1_L2"))
            .filterDate(start, end)
            .filterBounds(aoi)
            .map(prepL57)
            .map(
                lambda img: img.normalizedDifference(["NIR", "RED"])
                .pow(2)
                .tanh()
                .rename("kndvi")
            )
        )

        return l89.merge(l57).median().set("year", year).rename("kndvi")

    kndviYears = ee.List.sequence(kndviMinYear, kndviMaxYear)
    kndviCol = ee.ImageCollection(kndviYears.map(getAnnualKNDVI))

    # BASELINE kNDVI (Yn_bar):=
    # Mean kNDVI across non-anomalous ag-years only

    analysisYears = ee.List.sequence(start_year, metric_end_year)

    # This is my frozen baseline window — the ag-years I use to WORK OUT
    # what Yn_bar (my "normal" kNDVI reference) is. On purpose, kept
    # separate from analysisYears so extending my results later never
    # shifts this.
    baselineYears = ee.List.sequence(BASELINE_START_YEAR, BASELINE_END_YEAR)

    def calc_kndvi(y):
        year = ee.Number(y)
        kndvi = ee.Image(kndviCol.filter(ee.Filter.eq("year", year)).first())
        zScore = (
            ee.Image(zScoreCol.filter(ee.Filter.eq("year", year)).first())
            .resample("bilinear")
            .reproject(crs=kndvi.projection(), scale=30)
        )

        isNormal = zScore.select("zScore").abs().lt(Z_THRESHOLD)
        isForest = startYearTree.lte(year).And(endYearTree.gte(year))

        return kndvi.updateMask(isNormal.And(isForest)).set("year", year)

    Yn_bar = (
        ee.ImageCollection(baselineYears.map(calc_kndvi))
        .mean()
        .rename("kndvi_baseline")
    )

    # SIGNED RESISTANCE & RESILIENCE :=
    def calc_kndvi_ye(y):

        year = ee.Number(y)

        kndviYe = ee.Image(kndviCol.filter(ee.Filter.eq("year", year)).first())
        zScore = (
            ee.Image(zScoreCol.filter(ee.Filter.eq("year", year)).first())
            .resample("bilinear")
            .reproject(crs=kndviYe.projection(), scale=30)
        )

        # Only compute on forest pixels during anomalous fire ag-years
        isAnomalous = zScore.select("zScore").gt(Z_THRESHOLD)
        isForest = startYearTree.lte(year).And(endYearTree.gte(year))
        eventMask = isAnomalous.And(isForest)

        kndvi_next = ee.Image(
            kndviCol.filter(ee.Filter.eq("year", year.add(1))).first()
        )
        scored = eventMask.updateMask(kndvi_next.mask())

        diffRaw = kndviYe.subtract(Yn_bar)
        diffAbs = diffRaw.abs().max(1e-6)
        diffNext = kndvi_next.subtract(Yn_bar)
        diffNextAbs = diffNext.abs().max(1e-6)
        ratio = kndviYe.divide(Yn_bar)
        ratioNext = kndvi_next.divide(Yn_bar)
        deepDrop = ratio.lt(0.95)

        resistance = (
            Yn_bar.divide(diffAbs)
            .multiply(diffRaw.signum())
            .rename("resistance")
            .updateMask(scored)
        )
        resilience = (
            diffAbs.divide(diffNextAbs)
            .multiply(diffNext.signum())
            .rename("resilience")
            .updateMask(scored.And(deepDrop))
        )
        resistance_class = (
            _resistance_class(ratio).rename("resistance_class").updateMask(scored)
        )
        resilience_class = (
            _resilience_class(ratio, ratioNext, kndviYe, kndvi_next)
            .rename("resilience_class")
            .updateMask(scored)
        )

        return ee.Image.cat(
            [resistance, resilience, resistance_class, resilience_class]
        ).set("year", year)

    metricsCol = ee.ImageCollection(analysisYears.map(calc_kndvi_ye))

    yearly_bands = []
    for yy in range(start_year, metric_end_year + 1):
        year_img = ee.Image(metricsCol.filter(ee.Filter.eq("year", yy)).first())
        yearly_bands.append(year_img.select("resistance").rename(f"resistance_{yy}"))
        yearly_bands.append(year_img.select("resilience").rename(f"resilience_{yy}"))
        yearly_bands.append(
            year_img.select("resistance_class").rename(f"resistance_class_{yy}")
        )
        yearly_bands.append(
            year_img.select("resilience_class").rename(f"resilience_class_{yy}")
        )
    finalOutput = ee.Image.cat(yearly_bands).clip(aoi)

    task_id = export_raster_asset_to_gee(
        finalOutput, OUTPUT_DESC, OUTPUT_ASSET_ID, scale=30, region=aoi
    )

    return task_id, OUTPUT_ASSET_ID


def _resistance_class(ratio):
    """Class from Ye/Yn. Lower edge included, upper edge excluded."""
    return (
        ee.Image(1)
        .where(ratio.gte(0.95), 2)
        .where(ratio.gte(1), 3)
        .where(ratio.gte(1.05), 4)
        .toInt16()
    )


def _resilience_class(ratio, ratio_next, kndvi_ye, kndvi_next):
    """Class from the two ratios. Class 0 means resilience is not computed."""
    deep_drop = ratio.lt(0.95)
    return (
        ee.Image(0)
        .where(deep_drop.And(ratio_next.lt(0.95)).And(kndvi_next.lte(kndvi_ye)), 1)
        .where(deep_drop.And(ratio_next.lt(0.95)).And(kndvi_next.gt(kndvi_ye)), 2)
        .where(deep_drop.And(ratio_next.gte(0.95)).And(ratio_next.lt(1)), 3)
        .where(deep_drop.And(ratio_next.gte(1)).And(ratio_next.lt(1.05)), 4)
        .where(deep_drop.And(ratio_next.gte(1.05)), 5)
        .toInt16()
    )


def _round_half_up(image):
    return image.add(0.5).floor().toInt16()


def _mean_classes(img, start_year, end_year):
    resist, resil = [], []
    for year in range(start_year, end_year + 1):
        resist.append(img.select(f"resistance_class_{year}").rename("class"))
        resilience_year = img.select(f"resilience_class_{year}").rename("class")
        resil.append(resilience_year.updateMask(resilience_year.gt(0)))
    resistance_class = _round_half_up(ee.ImageCollection(resist).mean()).rename(
        "resistance_class"
    )
    resilience_class = (
        _round_half_up(ee.ImageCollection(resil).mean())
        .unmask(0)
        .updateMask(resistance_class.mask())
        .rename("resilience_class")
    )
    return resistance_class.addBands(resilience_class)


def classify_fire_metrics(aez, start_year=2004, end_year=None, gee_account_id=None):
    """Mean yearly fire classes and round half up.

    Resistance uses every scored fire year. Resilience leaves out class 0.
    If every year is class 0, the pixel is 0. end_year is the last year that
    has a next-year kNDVI.
    """
    ee_initialize(gee_account_id)
    if end_year is None:
        raise ValueError("end_year must be specified explicitly.")

    root = "projects/corestack-datasets-alpha/assets/datasets/hazards/fire_Rt_Re"
    yearly_id = f"{root}/fire_metrics_harmonized_kNDVI_AEZ_{aez}"
    output_desc = f"fire_metrics_classmean_AEZ_{aez}"
    output_id = f"{root}/{output_desc}"
    if is_gee_asset_exists(output_id):
        return None
    if not is_gee_asset_exists(yearly_id):
        raise ValueError(f"Yearly metrics asset not found: {yearly_id}")

    img = ee.Image(yearly_id)
    aoi = ee.FeatureCollection(AEZ).filter(ee.Filter.eq("ae_regcode", aez)).geometry()
    # aoi = (
    #     ee.FeatureCollection("projects/ext-datasets/assets/datasets/State_pan_india")
    #     .filter(ee.Filter.eq("Name", "Odisha"))
    #     .geometry()
    # )
    final_output = _mean_classes(img, start_year, end_year).clip(aoi)
    return export_raster_asset_to_gee(
        final_output, output_desc, output_id, scale=30, region=aoi
    )
