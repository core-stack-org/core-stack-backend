import ee
from datetime import date

from utilities.constants import AEZ
from utilities.gee_utils import (
    ee_initialize,
    is_gee_asset_exists,
    export_raster_asset_to_gee,
)


def generate_drought_resistance(
    aez, start_year=2004, end_year=None, gee_account_id=None
):
    """
    * Forest Sensitivity Analysis Pipeline — Script 2
    * Drought Resistance & Resilience (Harmonized kNDVI + Signed Formulas)
    *
    * AGRICULTURAL YEAR CONVENTION:
    * Year `y` = Jul 1 of `y` -> Jun 30 of `y+1`, matching the rest of the
    * pipeline. SPEI-12 bands are named "{y}_{y+1}" (e.g. "2004_2005") by
    * compute_spei.R — selected directly by that name here, not by a
    * positional rename.
    *
    * For each forest pixel and drought year that has a next-year kNDVI,
    * stores signed resistance, signed resilience, and the yearly class.
    * A year without Ye+1 is not scored, including resistance.
    * classify_drought_metrics averages the yearly class numbers.
    *
    * Harmonization: Transforms Landsat 8/9 (OLI) to Landsat 5/7 (ETM+)
    * equivalent before computing kNDVI to eliminate sensor-shift bias.
    *
    * Signed resistance (both +ve and -ve events):
    * Resistance = Yn_bar / |Ye - Yn_bar| × sign(Ye - Yn_bar)
    *
    * Resilience = |Ye - Yn_bar| / |Ye+1 - Yn_bar| × sign(Ye+1 - Yn_bar)
    * Stored only when Ye / Yn_bar < 0.95. Otherwise the resilience class
    * is 0. The class comes from the kNDVI ratios.
    *
    * Requires:
    * - Forest mask asset from Script 1
    * - SPEI-12 asset from the SPEI pipeline (compute_spei.R)
    """

    ee_initialize(gee_account_id)

    if end_year is None:
        raise ValueError(
            "end_year must be specified explicitly — agricultural-year pipelines "
            "cannot infer a safe default."
        )

    TREE_COVER_ASSET = f"projects/corestack-datasets-alpha/assets/datasets/hazards/tree_mask/Hybrid_Tree_AEZ_{aez}_{str(2004)}_{str(end_year)}"
    OUTPUT_DESC = f"Drought_Metrics_AEZ_{aez}"
    OUTPUT_ASSET_ID = f"projects/corestack-datasets-alpha/assets/datasets/hazards/drought_Rt_Re/{OUTPUT_DESC}"

    if is_gee_asset_exists(OUTPUT_ASSET_ID):
        return None, OUTPUT_ASSET_ID

    DROUGHT_THRESHOLD = -1.0  # SPEI-3 JAS below this = drought year

    # Fixed baseline window. This is independent of analysis START_YEAR/END_YEAR.
    # Please don't EVER change this once results are published, or old outputs will change when the pipeline timeline is extended.
    # This helps in fixing the Yn_bar (the average of the non-drought year NDVI) to a constant value.
    BASELINE_START_YEAR = 2004  # SPEI has no data before 2004
    BASELINE_END_YEAR = 2024

    speiMinYear = min(start_year, BASELINE_START_YEAR)
    speiMaxYear = max(end_year, BASELINE_END_YEAR)

    kndviMinYear = min(start_year, BASELINE_START_YEAR)
    kndviMaxYear = max(end_year + 1, BASELINE_END_YEAR)
    # Last scored year is the last year whose next year is fetched.
    metric_end_year = min(end_year, kndviMaxYear - 1)

    # kndviMaxYear is the furthest single ag-year fetched anywhere in this
    # script; that ag-year isn't complete until Jun 30 of kndviMaxYear+1.
    required_complete_by = date(kndviMaxYear + 1, 6, 30)
    if required_complete_by > date.today():
        raise ValueError(
            f"Agricultural year {kndviMaxYear} (Jul {kndviMaxYear} -> "
            f"Jun {kndviMaxYear + 1}) is not yet complete as of "
            f"{date.today().isoformat()}. Reduce end_year."
        )

    SPEI3_ASSET = "projects/corestack-datasets-alpha/assets/datasets/SPEI_updated/SPEI3"

    if not is_gee_asset_exists(TREE_COVER_ASSET):
        raise ValueError(
            f"Tree cover asset not found: {TREE_COVER_ASSET}. Run "
            f"generate_hybrid_tree_mask with end_year={end_year} first."
        )
    if not is_gee_asset_exists(SPEI3_ASSET):
        raise ValueError(f"SPEI-3 asset not found: {SPEI3_ASSET}.")

    requiredSpeiBands = {f"y{y}_m07_09" for y in range(speiMinYear, speiMaxYear + 1)}
    availableSpeiBands = set(ee.Image(SPEI3_ASSET).bandNames().getInfo())
    missingSpeiBands = sorted(requiredSpeiBands - availableSpeiBands)
    if missingSpeiBands:
        raise ValueError(
            f"{SPEI3_ASSET} is missing bands: {missingSpeiBands}. Re-run the "
            f"SPEI pipeline with start_year<=2004 and end_year>={speiMaxYear}."
        )

    aoi = ee.FeatureCollection(AEZ).filter(ee.Filter.eq("ae_regcode", aez)).geometry()
    # aoi = (
    #     ee.FeatureCollection("projects/ext-datasets/assets/datasets/State_pan_india")
    #     .filter(ee.Filter.eq("Name", "Odisha"))
    #     .geometry()
    # )
    # Loading the assets :=

    treeMeta = ee.Image(TREE_COVER_ASSET)

    startYear = treeMeta.select("start_year")
    endYear = treeMeta.select("end_year")

    # Load SPEI-3 collection from single multiband asset. SPEI-3 ending
    # September = July–August–September, band "y{y}_m07_09" for ag-year y —
    # select directly by that name rather than assuming band order.
    spei3_raw = ee.Image(SPEI3_ASSET)

    speiImages = []
    for y in range(speiMinYear, speiMaxYear + 1):
        speiImages.append(
            spei3_raw.select(f"y{y}_m07_09").rename("spei").set("year", y)
        )
    speiCol = ee.ImageCollection(speiImages)

    # LANDSAT HARMONIZATION & kNDVI :=

    # Chastain et al. coefficients (OLI to ETM+)
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

        # Apply mask, select optical bands, and apply Collection 2 scale factors
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

        # Apply mask, select optical bands, and apply Collection 2 scale factors
        scaled = (
            image.updateMask(mask)
            .select(["SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B6", "SR_B7"])
            .multiply(0.0000275)
            .add(-0.2)
            .rename(chastainBandNames)
        )

        # Apply Chastain regression model (OLI -> ETM+)
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

    # BASELINE kNDVI (Yn_bar) :=
    # Mean kNDVI across non-drought ag-years only

    # The analysis years will still remain the same, so if some internal year width is given like 2010-2018 for example
    # then too the baseline yn_bar would be same of the bigger normalization. But the analysis will be resulting only of the analysis years.
    analysisYears = ee.List.sequence(start_year, metric_end_year)

    baselineYears = ee.List.sequence(BASELINE_START_YEAR, BASELINE_END_YEAR)

    def calc_kndviNonDrought(y):
        year = ee.Number(y)
        kndvi = kndviCol.filter(ee.Filter.eq("year", year)).first()
        spei = (
            speiCol.filter(ee.Filter.eq("year", year))
            .first()
            .resample("bilinear")
            .reproject(crs=kndvi.projection(), scale=30)
        )
        isNonDrought = spei.gte(DROUGHT_THRESHOLD)
        return kndvi.updateMask(isNonDrought).set("year", year)

    kndviNonDrought = ee.ImageCollection(baselineYears.map(calc_kndviNonDrought))

    Yn_bar = kndviNonDrought.mean().rename("kndvi_baseline")

    # SIGNED RESISTANCE & RESILIENCE :=

    def calc_metrics_col(y):
        year = ee.Number(y)

        kndviYe = kndviCol.filter(ee.Filter.eq("year", year)).first()
        speiYe = (
            speiCol.filter(ee.Filter.eq("year", year))
            .first()
            .resample("bilinear")
            .reproject(crs=kndviYe.projection(), scale=30)
        )

        # Only compute on forest pixels during drought ag-years
        isForest = startYear.lte(year).And(endYear.gte(year))
        isDrought = speiYe.lt(DROUGHT_THRESHOLD)
        eventMask = isForest.And(isDrought)

        kndviNext = kndviCol.filter(ee.Filter.eq("year", year.add(1))).first()
        scored = eventMask.updateMask(kndviNext.mask())

        diffRaw = kndviYe.subtract(Yn_bar)
        diffAbs = diffRaw.abs().max(1e-6)
        diffNext = kndviNext.subtract(Yn_bar)
        diffNextAbs = diffNext.abs().max(1e-6)
        ratio = kndviYe.divide(Yn_bar)
        ratioNext = kndviNext.divide(Yn_bar)
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
            _resilience_class(ratio, ratioNext, kndviYe, kndviNext)
            .rename("resilience_class")
            .updateMask(scored)
        )

        return ee.Image.cat(
            [resistance, resilience, resistance_class, resilience_class]
        ).set("year", year)

    metricsCol = ee.ImageCollection(analysisYears.map(calc_metrics_col))

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


def classify_drought_metrics(aez, start_year=2004, end_year=None, gee_account_id=None):
    """Mean yearly drought classes and round half up.

    Resistance uses every scored drought year. Resilience leaves out class 0.
    If every year is class 0, the pixel is 0. end_year is the last year that
    has a next-year kNDVI.
    """
    ee_initialize(gee_account_id)
    if end_year is None:
        raise ValueError("end_year must be specified explicitly.")

    root = "projects/corestack-datasets-alpha/assets/datasets/hazards/drought_Rt_Re"
    yearly_id = f"{root}/Drought_Metrics_AEZ_{aez}"
    output_desc = f"Drought_Metrics_ClassMean_AEZ_{aez}"
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
