import ee

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
    * For each forest pixel, computes mean resistance and resilience
    * across all drought years (SPEI-12 < threshold).
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
    * - SPEI-12 assets from spei-drought-analysis-pipeline
    """

    ee_initialize(gee_account_id)

    TREE_COVER_ASSET = f"projects/corestack-datasets-alpha/assets/datasets/SPEI/Hybrid_Tree_AEZ_{aez}_{str(2003)}_{str(end_year)}"
    OUTPUT_DESC = f"Drought_Metrics_AEZ_{aez}"
    OUTPUT_ASSET_ID = (
        f"projects/corestack-datasets-alpha/assets/datasets/SPEI/{OUTPUT_DESC}"
    )

    if is_gee_asset_exists(OUTPUT_ASSET_ID):
        return None

    DROUGHT_THRESHOLD = -1.0  # SPEI-12 below this = drought year

    # Fixed baseline window. This is independent of analysis START_YEAR/END_YEAR.
    # Please don't EVER change this once results are published, or old outputs will change when the pipeline timeline is extended.
    # This helps in fixing the Yn_bar (the average of the non-drought year NDVI) to a constant value.
    BASELINE_START_YEAR = 2004  # SPEI has no data before 2004
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

    # Load SPEI-12 collection from single multiband asset
    SPEI12_ASSET = f"projects/corestack-datasets-alpha/assets/datasets/SPEI/SPEI12"
    spei12_raw = ee.Image(SPEI12_ASSET)
    spei12_bandnames = []

    for yn in range(2004, end_year + 1):
        spei12_bandnames.append("y" + str(yn))
    spei12_named = spei12_raw.rename(spei12_bandnames)

    # Building the per-year SPEI collection here.
    speiMinYear = min(start_year, BASELINE_START_YEAR)
    speiMaxYear = max(end_year, BASELINE_END_YEAR)

    speiImages = []
    for y in range(speiMinYear, speiMaxYear + 1):
        speiImages.append(
            spei12_named.select("y" + str(y)).rename("spei").set("year", y)
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
    # kndviYears = ee.List.sequence(start_year, end_year + 1)
    kndviMinYear = min(start_year, BASELINE_START_YEAR)
    kndviMaxYear = max(end_year + 1, BASELINE_END_YEAR)

    kndviYears = ee.List.sequence(kndviMinYear, kndviMaxYear)
    kndviCol = ee.ImageCollection(kndviYears.map(getAnnualKNDVI))

    # BASELINE kNDVI (Yn_bar) :=
    # Mean kNDVI across non-drought years only

    # The analysis years will still remain the same , so if some internal year width is given like 2010-2018 for example
    # then too the baseline yn_bar would be same of the bigger normalization. But the analysis will be resulting only of the analysis years.
    analysisYears = ee.List.sequence(start_year, end_year)

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

        # Only compute on forest pixels during drought years
        isForest = startYear.lte(year).And(endYear.gte(year))
        isDrought = speiYe.lt(DROUGHT_THRESHOLD)
        eventMask = isForest.And(isDrought)

        diffRaw = kndviYe.subtract(Yn_bar)
        diffAbs = diffRaw.abs().max(1e-6)

        # Resistance: signed, computed for ALL drought years
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

    # AGGREGATE & EXPORT :=

    meanResistance = metricsCol.select("resistance").mean().clip(aoi)
    meanResilience = metricsCol.select("resilience").mean().clip(aoi)

    finalOutput = meanResistance.rename("resistance").addBands(
        meanResilience.rename("resilience")
    )

    task_id = export_raster_asset_to_gee(
        finalOutput, OUTPUT_DESC, OUTPUT_ASSET_ID, scale=30, region=aoi
    )

    return task_id
