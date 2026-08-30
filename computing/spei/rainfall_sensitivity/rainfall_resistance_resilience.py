import ee

from utilities.constants import AEZ
from utilities.gee_utils import (
    ee_initialize,
    is_gee_asset_exists,
    export_raster_asset_to_gee,
)


def generate_rainfall_resilience(
    aez, start_year=2004, end_year=None, gee_account_id=None
):
    """
    * Forest Sensitivity Analysis Pipeline — Script 3b
    * Heavy Rainfall Resistance & Resilience (Harmonized kNDVI + Signed Formulas)
    *
    * Signed resistance (both +ve and -ve events):
    * Resistance = Yn_bar / |Ye - Yn_bar| × sign(Ye - Yn_bar)
    *
    * Resilience computed ONLY when Ye < Yn_bar (negative effect years):
    * Resilience = |Ye - Yn_bar| / |Ye+1 - Yn_bar| × sign(Ye+1 - Yn_bar)
    *
    * Harmonization: Transforms Landsat 8/9 (OLI) to Landsat 5/7 (ETM+)
    * equivalent before computing kNDVI to eliminate sensor-shift bias.
    *
    * Requires:
    * - Forest mask asset (Script 1)
    * - Rainfall index asset (Script 3a)
    """

    ee_initialize(gee_account_id)
    TREE_COVER_ASSET = f"projects/corestack-datasets-alpha/assets/datasets/SPEI/Hybrid_Tree_AEZ_{aez}_{str(2003)}_{str(end_year)}"
    RAIN_INDEX_ASSET = (
        f"projects/corestack-datasets-alpha/assets/datasets/SPEI/rain_index_AEZ_{aez}"
    )

    OUTPUT_DESC = f"Rain_Metrics_{aez}"  # f"Rain_Metrics_AEZ_{aez}"
    OUTPUT_ASSET_ID = (
        f"projects/corestack-datasets-alpha/assets/datasets/SPEI/{OUTPUT_DESC}"
    )

    if is_gee_asset_exists(OUTPUT_ASSET_ID):
        return None

    Z_THRESHOLD = 1.0

    # This is the same fix I did for the drought script (Script 2). Yn_bar is
    # my baseline — "what kNDVI should normally look like on a healthy,
    # non-anomalous year." If I let Yn_bar be computed only from
    # START_YEAR..END_YEAR, then every time I extend my analysis window in
    # the future, Yn_bar shifts a little, and that quietly changes ALL my
    # past resistance/resilience numbers too — even for years I already
    # published. That's the exact problem I don't want.
    #
    # So I'm freezing the baseline window here, separately from my analysis
    # window. Once I publish results, I'm never touching these two numbers
    # again — if I do, my old outputs will drift.
    #
    # I picked 2004-2024 to match what I already locked in for the drought
    # baseline (Script 2) and for the rain z-score baseline (Script 3a) —
    # keeping all three consistent. 2004 is my floor because that's as far
    # back as my source data goes; 2024 is what I've already confirmed is
    # available and exported (3a's END_YEAR is now 2024, so the zScore bands
    # I need actually exist in the rain index asset).
    BASELINE_START_YEAR = 2004
    BASELINE_END_YEAR = 2024

    aoi = ee.FeatureCollection(AEZ).filter(ee.Filter.eq("ae_regcode", aez)).geometry()
    # aoi = (
    #     ee.FeatureCollection("projects/ext-datasets/assets/datasets/State_pan_india")
    #     .filter(ee.Filter.eq("Name", "Odisha"))
    #     .geometry()
    # )

    treeMeta = ee.Image(TREE_COVER_ASSET)
    startYearTree = treeMeta.select("start_year")
    endYearTree = treeMeta.select("end_year")

    rainIndex = ee.Image(RAIN_INDEX_ASSET)

    # I need zScore bands for every year covering BOTH my baseline window
    # and my analysis window — whichever stretches further in each
    # direction. Before, this loop only went START_YEAR..END_YEAR, which
    # meant if I ever widened my baseline beyond my analysis window, I'd be
    # trying to .select() a band like zScore_2024 that this loop never even
    # asked for. So I'm building the year range from the min/max of both
    # windows, to be safe.
    zMinYear = min(start_year, BASELINE_START_YEAR)
    zMaxYear = max(end_year, BASELINE_END_YEAR)

    zScoreCol_list = []
    for y in range(zMinYear, zMaxYear + 1):
        zScoreCol_list.append(
            rainIndex.select("zScore_" + str(y)).rename("zScore").set("year", y)
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

    # Calculate annual median kNDVI
    def getAnnualKNDVI(year):
        start = ee.Date.fromYMD(year, 1, 1)
        end = ee.Date.fromYMD(year, 12, 31)

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

    # Same idea as zScoreCol above — I need kNDVI images covering whichever
    # is bigger: my baseline window, or my analysis window (+1 year, because
    # resilience for my LAST analysis year needs next-year kNDVI to compare
    # against). So I'm taking the min of the two start years and the max of
    # (END_YEAR + 1) vs BASELINE_END_YEAR.
    kndviMinYear = min(start_year, BASELINE_START_YEAR)
    kndviMaxYear = max(end_year + 1, BASELINE_END_YEAR)

    kndviYears = ee.List.sequence(kndviMinYear, kndviMaxYear)
    kndviCol = ee.ImageCollection(kndviYears.map(getAnnualKNDVI))

    # BASELINE kNDVI (Yn_bar) :=
    # Mean kNDVI across non-anomalous years only

    # This is my actual analysis window — the years I want resistance and
    # resilience results FOR. This stays exactly as before, untouched.

    analysisYears = ee.List.sequence(start_year, end_year)

    # This is my frozen baseline window — the years I use to WORK OUT what
    # Yn_bar (my "normal" kNDVI reference) is. This is separate from
    # analysisYears on purpose, so extending my analysis later doesn't shift
    # Yn_bar and quietly rewrite results I've already published.
    baselineYears = ee.List.sequence(BASELINE_START_YEAR, BASELINE_END_YEAR)

    def calc_Yn_bar(y):
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
        ee.ImageCollection(baselineYears.map(calc_Yn_bar))
        .mean()
        .rename("kndvi_baseline")
    )

    # SIGNED RESISTANCE & RESILIENCE :=

    # This part is completely unchanged from before — it still only runs
    # over analysisYears (my actual START_YEAR..END_YEAR), it just now uses
    # the frozen Yn_bar computed above instead of a Yn_bar that would've
    # silently moved every time I touch START_YEAR/END_YEAR.

    def calc_metrics_cols(y):
        year = ee.Number(y)

        kndviYe = ee.Image(kndviCol.filter(ee.Filter.eq("year", year)).first())
        zScore = (
            ee.Image(zScoreCol.filter(ee.Filter.eq("year", year)).first())
            .resample("bilinear")
            .reproject(crs=kndviYe.projection(), scale=30)
        )

        # Only compute on forest pixels during anomalous rainfall years
        isAnomalous = zScore.select("zScore").gt(Z_THRESHOLD)
        isForest = startYearTree.lte(year).And(endYearTree.gte(year))
        eventMask = isAnomalous.And(isForest)

        diffRaw = kndviYe.subtract(Yn_bar)
        diffAbs = diffRaw.abs().max(1e-6)

        # Resistance: signed, computed for ALL anomalous years (both +ve and -ve)
        resistance = (
            Yn_bar.divide(diffAbs)
            .multiply(diffRaw.signum())
            .rename("resistance")
            .updateMask(eventMask)
        )

        # Resilience: ONLY computed when Ye < Yn_bar (negative effect years)
        isNegativeEffect = kndviYe.lt(Yn_bar)
        resilMask = eventMask.And(isNegativeEffect)

        kndviNext = ee.Image(kndviCol.filter(ee.Filter.eq("year", year.add(1))).first())
        diffNext = kndviNext.subtract(Yn_bar)
        diffNextAbs = diffNext.abs().max(1e-6)

        resilience = (
            diffAbs.divide(diffNextAbs)
            .multiply(diffNext.signum())
            .rename("resilience")
            .updateMask(resilMask)
        )

        return ee.Image.cat([resistance, resilience]).set("year", year)

    metricsCol = ee.ImageCollection(analysisYears.map(calc_metrics_cols))

    # AGGREGATE & EXPORT :=

    meanResist = metricsCol.select("resistance").mean().clip(aoi)
    meanResil = metricsCol.select("resilience").mean().clip(aoi)

    finalOutput = meanResist.rename("resistance").addBands(
        meanResil.rename("resilience")
    )

    task_id = export_raster_asset_to_gee(
        finalOutput, OUTPUT_DESC, OUTPUT_ASSET_ID, scale=30, region=aoi
    )

    return task_id
