import ee

from utilities.constants import AEZ
from utilities.gee_utils import export_raster_asset_to_gee, ee_initialize


def rainfall_resilience(aez, start_year=2004, end_year=2022, gee_account_id=None):
    # /**
    #  * Forest Sensitivity Analysis Pipeline — Script 3b
    #  * Heavy Rainfall Resistance & Resilience
    #  *
    #  * Signed resistance (both +ve and -ve events):
    #  *   Resistance = Yn_bar / |Ye - Yn_bar| × sign(Ye - Yn_bar)
    #  *
    #  * Resilience computed ONLY when Ye < Yn_bar (negative effect years):
    #  *   Resilience = |Ye - Yn_bar| / |Ye+1 - Yn_bar| × sign(Ye+1 - Yn_bar)
    #  *
    #  * Requires:
    #  *   - Forest mask asset (Script 1)
    #  *   - Rainfall index asset (Script 3a)
    #  */
    #
    # # Configuration :=
    #
    ee_initialize(7)
    TREE_COVER_ASSET = f"projects/corestack-datasets-alpha/assets/datasets/SPEI/Hybrid_Tree_AEZ_{aez}_Period_{str(2003)}_{str(end_year)}"
    RAIN_INDEX_ASSET = (
        f"projects/corestack-datasets-alpha/assets/datasets/SPEI/Rain_Index_AEZ_{aez}"
    )

    OUTPUT_DESC = f"Rain_Metrics_AEZ_{aez}"
    OUTPUT_ASSET_ID = (
        f"projects/corestack-datasets-alpha/assets/datasets/SPEI/{OUTPUT_DESC}"
    )

    Z_THRESHOLD = 1.0

    # AOI :=
    aoi = ee.FeatureCollection(AEZ).filter(ee.Filter.eq("ae_regcode", aez)).geometry()

    # Loading the assets :=
    treeMeta = ee.Image(TREE_COVER_ASSET)
    startYearTree = treeMeta.select("start_year")
    endYearTree = treeMeta.select("end_year")

    rainIndex_raw = ee.Image(RAIN_INDEX_ASSET)
    rainBandNames = []

    for yr in range(start_year, end_year + 1):
        rainBandNames.append("Hm_" + str(yr))
        rainBandNames.append("zScore_" + str(yr))

    rainIndex = rainIndex_raw.rename(rainBandNames)

    hmCol_list = []
    zScoreCol_list = []
    for y in range(start_year, end_year + 1):
        hmCol_list.append(rainIndex.select("Hm_" + str(y)).rename("Hm").set("year", y))
        zScoreCol_list.append(
            rainIndex.select("zScore_" + str(y)).rename("zScore").set("year", y)
        )

    hmCol = ee.ImageCollection(hmCol_list)
    zScoreCol = ee.ImageCollection(zScoreCol_list)

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

    ndviCol = ee.ImageCollection(
        ee.List.sequence(start_year, end_year + 1).map(getAnnualNDVI)
    )

    # BASELINE NDVI (Yn_bar) :=
    # Mean NDVI across non-anomalous years only

    analysisYears = ee.List.sequence(start_year, end_year)

    # Yn_bar = ee.ImageCollection(analysisYears.map(function(y) {
    #   year   = ee.Number(y)
    #   ndvi   = ee.Image(ndviCol.filter(ee.Filter.eq('year', year)).first())
    #   zScore = ee.Image(zScoreCol.filter(ee.Filter.eq('year', year)).first())
    #                  .resample('bilinear')
    #                  .reproject({crs: ndvi.projection(), scale: 30})
    #   isNormal = zScore.select('zScore').abs().lt(Z_THRESHOLD)
    #   isForest = startYearTree.lte(year).and(endYearTree.gte(year))
    #   return ndvi.updateMask(isNormal.and(isForest)).set('year', year)
    # })).mean().rename('ndvi_baseline')

    def ndvi_forest(y):
        year = ee.Number(y)
        ndvi = ee.Image(ndviCol.filter(ee.Filter.eq("year", year)).first())
        zScore = (
            ee.Image(zScoreCol.filter(ee.Filter.eq("year", year)).first())
            .resample("bilinear")
            .reproject(crs=ndvi.projection(), scale=30)
        )

        isNormal = zScore.select("zScore").abs().lt(Z_THRESHOLD)
        isForest = startYearTree.lte(year).And(endYearTree.gte(year))
        return ndvi.updateMask(isNormal.And(isForest)).set("year", year)

    ndviNonDrought = ee.ImageCollection(analysisYears.map(ndvi_forest))

    Yn_bar = ndviNonDrought.mean().rename("ndvi_baseline")
    # SIGNED RESISTANCE & RESILIENCE :=

    def metricsColFunc(y):
        year = ee.Number(y)

        ndviYe = ee.Image(ndviCol.filter(ee.Filter.eq("year", year)).first())
        zScore = (
            ee.Image(zScoreCol.filter(ee.Filter.eq("year", year)).first())
            .resample("bilinear")
            .reproject(crs=ndviYe.projection(), scale=30)
        )

        # Only compute on forest pixels during anomalous rainfall years
        isAnomalous = zScore.select("zScore").gt(Z_THRESHOLD)
        isForest = startYearTree.lte(year).And(endYearTree.gte(year))
        eventMask = isAnomalous.And(isForest)

        diffRaw = ndviYe.subtract(Yn_bar)
        diffAbs = diffRaw.abs().max(1e-6)

        # Resistance: signed, computed for ALL anomalous years (both +ve and -ve)
        resistance = (
            Yn_bar.divide(diffAbs)
            .multiply(diffRaw.signum())
            .rename("resistance")
            .updateMask(eventMask)
        )

        # Resilience: ONLY computed when Ye < Yn_bar (negative effect years)
        # This avoids the 2D interpretation problem when Ye > Yn_bar
        # and also thinking about it, resilience only makes sense,
        # when Ye < Yn_bar , as if NDVI has increased from baseline, no point in calculating the recovering
        # as the nae sugegsts. ALthough we are missing out on cases , if increase happened, and due to some lasting effect of rainfall,
        # ndvi decreased in further years. But we're ignoring that case here, just for simplicity of understanding in 2-D.
        isNegativeEffect = ndviYe.lt(Yn_bar)
        resilMask = eventMask.And(isNegativeEffect)

        ndviNext = ee.Image(ndviCol.filter(ee.Filter.eq("year", year.add(1))).first())
        diffNext = ndviNext.subtract(Yn_bar)
        diffNextAbs = diffNext.abs().max(1e-6)

        resilience = (
            diffAbs.divide(diffNextAbs)
            .multiply(diffNext.signum())
            .rename("resilience")
            .updateMask(resilMask)
        )

        return ee.Image.cat([resistance, resilience]).set("year", year)

    metricsCol = ee.ImageCollection(analysisYears.map(metricsColFunc))

    # AGGREGATE & EXPORT :=

    meanResist = metricsCol.select("resistance").mean().clip(aoi)
    meanResil = metricsCol.select("resilience").mean().clip(aoi)

    finalOutput = meanResist.rename("resistance").addBands(
        meanResil.rename("resilience")
    )

    task_id = export_raster_asset_to_gee(
        finalOutput, OUTPUT_DESC, OUTPUT_ASSET_ID, scale=30, region=aoi
    )
