import ee

from utilities.constants import AEZ
from utilities.gee_utils import (
    is_gee_asset_exists,
    export_raster_asset_to_gee,
    ee_initialize,
)


def high_wind_sensitivity(aez, start_year=2004, end_year=None, gee_account_id=None):
    """
    * Forest Sensitivity Analysis Pipeline — Script 5b
    * High Windspeed Resistance & Resilience (Harmonized kNDVI + Signed Formulas)
    *
    * For each forest pixel, computes mean resistance and resilience
    * across all high-windspeed years (WSmax > threshold).
    *
    * Harmonization: Transforms Landsat 8/9 (OLI) to Landsat 5/7 (ETM+)
    * equivalent before computing kNDVI to eliminate sensor-shift bias.
    *
    * Signed resistance (both +ve and -ve events):
    * Resistance = Yn_bar / |Ye - Yn_bar| × sign(Ye - Yn_bar)
    *
    * Resilience computed ONLY when Ye < Yn_bar (negative effect years):
    * Resilience = |Ye - Yn_bar| / |Ye+1 - Yn_bar| × sign(Ye+1 - Yn_bar)
    *
    * Requires:
    * - Forest mask asset from Script 1
    * - Windspeed index asset from Script 5a
    """
    ee_initialize(gee_account_id)

    TREE_COVER_ASSET = f"projects/corestack-datasets-alpha/assets/datasets/SPEI/Hybrid_Tree_AEZ_{aez}_{str(2003)}_{str(end_year)}"
    WIND_INDEX_ASSET = (
        f"projects/corestack-datasets-alpha/assets/datasets/SPEI/wind_index_AEZ_{aez}"
    )

    OUTPUT_DESC = f"wind_metrics_harmonized_kNDVI_AEZ_{aez}"
    OUTPUT_ASSET_ID = (
        f"projects/corestack-datasets-alpha/assets/datasets/SPEI/{OUTPUT_DESC}"
    )

    if is_gee_asset_exists(OUTPUT_ASSET_ID):
        return None

    WIND_THRESHOLD = 15

    BASELINE_START_YEAR = 2004
    BASELINE_END_YEAR = 2024

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

    # Load windspeed index from single multiband asset (Script 5a output)
    windIndex_raw = ee.Image(WIND_INDEX_ASSET)

    # I need WSmax bands covering BOTH my analysis window and my baseline
    # window — whichever stretches further. Right now they're the same range
    # (2004-2022), so this doesn't change anything today, but it protects me
    # once the two windows stop lining up in the future.
    wsMinYear = min(start_year, BASELINE_START_YEAR)
    wsMaxYear = max(end_year, BASELINE_END_YEAR)

    wsImages = []
    for y in range(wsMinYear, wsMaxYear + 1):
        wsImages.append(
            windIndex_raw.select(f"WSmax_{y}").rename("windspeed").set("year", y)
        )
    wsCol = ee.ImageCollection(wsImages)

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

    # Load kNDVI for START_YEAR to END_YEAR+1 (need next year for resilience)
    # I need kNDVI images covering whichever is bigger: my baseline window, or
    # my analysis window PLUS ONE year (since resilience for my last analysis
    # year needs next-year kNDVI to compare against).

    kndviMinYear = min(start_year, BASELINE_START_YEAR)
    kndviMaxYear = max(end_year + 1, BASELINE_END_YEAR)

    kndviYears = ee.List.sequence(kndviMinYear, kndviMaxYear)
    kndviCol = ee.ImageCollection(kndviYears.map(getAnnualKNDVI))

    # BASELINE kNDVI (Yn_bar):=
    # Mean kNDVI across non-high-wind years only

    analysisYears = ee.List.sequence(start_year, end_year)

    # This is my frozen baseline window — the years I use to WORK OUT what
    # Yn_bar (my "normal" kNDVI reference) is, kept separate from
    # analysisYears on purpose.
    baselineYears = ee.List.sequence(BASELINE_START_YEAR, BASELINE_END_YEAR)

    def calc_kndvi_non_event(y):
        year = ee.Number(y)
        kndvi = kndviCol.filter(ee.Filter.eq("year", year)).first()
        ws = (
            wsCol.filter(ee.Filter.eq("year", year))
            .first()
            .resample("bilinear")
            .reproject(crs=kndvi.projection(), scale=30)
        )
        isNonEvent = ws.lte(WIND_THRESHOLD)
        return kndvi.updateMask(isNonEvent).set("year", year)

    kndviNonEvent = ee.ImageCollection(baselineYears.map(calc_kndvi_non_event))

    Yn_bar = kndviNonEvent.mean().rename("kndvi_baseline")

    # SIGNED RESISTANCE & RESILIENCE :=
    def calc_metrics_col(y):
        year = ee.Number(y)

        kndviYe = kndviCol.filter(ee.Filter.eq("year", year)).first()
        wsYe = (
            wsCol.filter(ee.Filter.eq("year", year))
            .first()
            .resample("bilinear")
            .reproject(crs=kndviYe.projection(), scale=30)
        )

        # Only compute on forest pixels during high-windspeed years
        # Flag ANY year where WSmax crosses the threshold, however briefly
        isForest = startYear.lte(year).And(endYear.gte(year))
        isHighWind = wsYe.gt(WIND_THRESHOLD)
        eventMask = isForest.And(isHighWind)

        diffRaw = kndviYe.subtract(Yn_bar)
        diffAbs = diffRaw.abs().max(1e-6)

        # Resistance: signed, computed for ALL high-wind years
        resistance = (
            Yn_bar.divide(diffAbs)
            .multiply(diffRaw.signum())
            .rename("resistance")
            .updateMask(eventMask)
        )

        # Resilience: ONLY computed when Ye < Yn_bar (negative effect years)
        isNegativeEffect = kndviYe.lt(Yn_bar)
        resilMask = eventMask.And(isNegativeEffect)

        kndviNext = kndviCol.filter(ee.Filter.eq("year", year.add(1))).first()
        diffNext = kndviNext.subtract(Yn_bar)
        diffNextAbs = diffNext.abs().max(1e-6)

        resilience = (
            diffAbs.divide(diffNextAbs)
            .multiply(diffNext.signum())
            .rename("resilience")
            .updateMask(resilMask)
        )

        return ee.Image.cat([resistance, resilience]).set("year", year)

    metricsCol = ee.ImageCollection(analysisYears.map(calc_metrics_col))

    # AGGREGATE & EXPORT
    meanResistance = metricsCol.select("resistance").mean().clip(aoi)
    meanResilience = metricsCol.select("resilience").mean().clip(aoi)

    finalOutput = meanResistance.rename("resistance").addBands(
        meanResilience.rename("resilience")
    )

    task_id = export_raster_asset_to_gee(
        finalOutput, OUTPUT_DESC, OUTPUT_ASSET_ID, scale=30, region=aoi
    )

    return task_id
