import ee

from utilities.constants import AEZ
from utilities.gee_utils import export_raster_asset_to_gee, ee_initialize


# /**
#  * Forest Sensitivity Analysis Pipeline — Script 2
#  * Drought Resistance & Resilience
#  *
#  * For each forest pixel, computes mean resistance and resilience
#  * across all drought years (SPEI-12 < threshold).
#  *
#  * Resistance  = Yn_bar / |Ye - Yn_bar|
#  * Resilience  = |Ye - Yn_bar| / |Y(e+1) - Yn_bar|
#  *
#  * Where:
#  *   Yn_bar = mean NDVI across non-drought years (baseline)
#  *   Ye     = NDVI during drought year
#  *   Y(e+1) = NDVI the year after drought
#  *
#  * Requires:
#  *   - Forest mask asset from Script 1
#  *   - SPEI-12 assets from spei-drought-analysis-pipeline
#  */
#
# # CONFIGURATION :=
def generate_drought_resistance(
    aez, start_year=2004, end_year=2022, gee_account_id=None
):
    ee_initialize(7)

    TREE_COVER_ASSET = f"projects/corestack-datasets-alpha/assets/datasets/SPEI/Hybrid_Tree_AEZ_{aez}_Period_{str(2003)}_{str(end_year)}"
    OUTPUT_DESC = f"Drought_Metrics_AEZ_{aez}"
    OUTPUT_ASSET_ID = (
        f"projects/corestack-datasets-alpha/assets/datasets/SPEI/{OUTPUT_DESC}"
    )

    STATE_NAME = "Madhya Pradesh"
    DROUGHT_THRESHOLD = -1.0  # SPEI-12 below this = drought year

    aoi = ee.FeatureCollection(AEZ).filter(ee.Filter.eq("ae_regcode", aez)).geometry()

    # Loading the assets :=

    treeMeta = ee.Image(TREE_COVER_ASSET)
    startYear = treeMeta.select("start_year")
    endYear = treeMeta.select("end_year")

    # Load SPEI-12 collection from single multiband asset
    SPEI12_ASSET = (
        f"projects/corestack-datasets-alpha/assets/datasets/SPEI/SPEI12_{str(aez)}"
    )
    spei12_raw = ee.Image(SPEI12_ASSET)
    spei12_bandnames = []

    # for (yn = 2004 yn <= 2023 yn++) {
    #   spei12_bandnames.push('y' + yn)
    # }
    for yn in range(2004, 2024):
        spei12_bandnames.append("y" + str(yn))

    spei12_named = spei12_raw.rename(spei12_bandnames)

    # Build per-year SPEI collection
    speiImages = []
    # for (y = START_YEAR y <= END_YEAR y++) {
    #   speiImages.push(
    #     spei12_named.select('y' + y)
    #       .rename('spei')
    #       .set('year', y)
    #   )
    # }

    for y in range(start_year, end_year + 1):
        speiImages.append(
            spei12_named.select("y" + str(y)).rename("spei").set("year", y)
        )

    speiCol = ee.ImageCollection(speiImages)

    # LANDSAT NDVI :=
    def maskClouds(image):
        qa = image.select("QA_PIXEL")
        mask = qa.bitwiseAnd(1 << 3).eq(0).And(qa.bitwiseAnd(1 << 4).eq(0))
        return image.updateMask(mask)

    def getAnnualNDVI(year):
        start = ee.Date.fromYMD(year, 1, 1)
        end = ee.Date.fromYMD(year, 12, 31)

        l89 = (
            ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
            .merge(ee.ImageCollection("LANDSAT/LC09/C02/T1_L2"))
            .filterDate(start, end)
            .filterBounds(aoi)
            .map(maskClouds)
            .map(
                lambda img: img.normalizedDifference(["SR_B5", "SR_B4"]).rename("ndvi")
            )
        )

        l57 = (
            ee.ImageCollection("LANDSAT/LT05/C02/T1_L2")
            .merge(ee.ImageCollection("LANDSAT/LE07/C02/T1_L2"))
            .filterDate(start, end)
            .filterBounds(aoi)
            .map(maskClouds)
            .map(
                lambda img: img.normalizedDifference(["SR_B4", "SR_B3"]).rename("ndvi")
            )
        )

        return l89.merge(l57).median().set("year", year).rename("ndvi")

    # Load NDVI for START_YEAR to END_YEAR+1 (need next year for resilience)
    ndviYears = ee.List.sequence(start_year, end_year + 1)
    ndviCol = ee.ImageCollection(ndviYears.map(getAnnualNDVI))

    # BASELINE NDVI (Yn_bar) :=
    # Mean NDVI across non-drought years only
    # Uses simple masked ImageCollection mean — we are trying to avoid GEE array scalign issues here

    analysisYears = ee.List.sequence(start_year, end_year)

    def ndviNonDroughtFunc(y):
        year = ee.Number(y)
        ndvi = ndviCol.filter(ee.Filter.eq("year", year)).first()
        spei = (
            speiCol.filter(ee.Filter.eq("year", year))
            .first()
            .resample("bilinear")
            .reproject(crs=ndvi.projection(), scale=30)
        )

        isNonDrought = spei.gte(DROUGHT_THRESHOLD)
        return ndvi.updateMask(isNonDrought).set("year", year)

    ndviNonDrought = ee.ImageCollection(analysisYears.map(ndviNonDroughtFunc))

    Yn_bar = ndviNonDrought.mean().rename("ndvi_baseline")

    # RESISTANCE & RESILIENCE :=

    def metricsColFunc(y):
        year = ee.Number(y)

        ndviYe = ndviCol.filter(ee.Filter.eq("year", year)).first()
        speiYe = (
            speiCol.filter(ee.Filter.eq("year", year))
            .first()
            .resample("bilinear")
            .reproject(crs=ndviYe.projection(), scale=30)
        )

        # Only compute on forest pixels during drought years
        isForest = startYear.lte(year).And(endYear.gte(year))
        isDrought = speiYe.lt(DROUGHT_THRESHOLD)
        mask = isForest.And(isDrought)

        diff = ndviYe.subtract(Yn_bar).abs().max(1e-6)
        resistance = Yn_bar.divide(diff).rename("resistance")

        ndviNext = ndviCol.filter(ee.Filter.eq("year", year.add(1))).first()
        diffNext = ndviNext.subtract(Yn_bar).abs().max(1e-6)
        resilience = diff.divide(diffNext).rename("resilience")

        return ee.Image.cat([resistance, resilience]).updateMask(mask).set("year", year)

    metricsCol = ee.ImageCollection(analysisYears.map(metricsColFunc))

    # AGGREGATE & EXPORT :=

    meanResistance = metricsCol.select("resistance").mean().clip(aoi)
    meanResilience = metricsCol.select("resilience").mean().clip(aoi)

    finalOutput = meanResistance.rename("resistance").addBands(
        meanResilience.rename("resilience")
    )

    export_raster_asset_to_gee(
        finalOutput, OUTPUT_DESC, OUTPUT_ASSET_ID, scale=30, region=aoi
    )
