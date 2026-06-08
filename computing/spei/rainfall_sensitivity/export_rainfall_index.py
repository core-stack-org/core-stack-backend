import ee

from utilities.constants import AEZ
from utilities.gee_utils import (
    export_raster_asset_to_gee,
    ee_initialize,
    is_gee_asset_exists,
)


def rainfall_index(aez, start_year=2004, end_year=2022, gee_account_id=None):
    """
    Forest Sensitivity Analysis Pipeline — Script 3a
    Heavy Rainfall Index Export

    Computes two quantities per pixel per year and exports as a single
    multiband asset — one band per year for each quantity:

      Hm_{year}     = annual sum of precipitation on heavy days
      zScore_{year} = z-score of Hm relative to the full period mean/stddev

    Heavy day definition: daily precipitation > long-term 95th percentile (CHIRPS)
    Z-score computed across all years in the period.

    Output asset bands:
      Hm_2004, Hm_2005, ..., Hm_2023      (19 bands)
      zScore_2004, ..., zScore_2023        (19 bands)
      Total: 38 bands

    This asset is the direct input to Script 3b, analogous to how
    SPEI assets are the input to Script 2 (drought).

    Requires: nothing — only public datasets (CHIRPS)
    """

    ee_initialize(gee_account_id)
    OUTPUT_DESC = f"Rain_Index_AEZ_{aez}"
    OUTPUT_ASSET_ID = (
        f"projects/corestack-datasets-alpha/assets/datasets/SPEI/{OUTPUT_DESC}"
    )

    if is_gee_asset_exists(OUTPUT_ASSET_ID):
        return None

    aoi = ee.FeatureCollection(AEZ).filter(ee.Filter.eq("ae_regcode", aez)).geometry()
    # ===========================================================================
    #                    HEAVY RAINFALL INDEX (Hm)
    # ===========================================================================

    chirps = (
        ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")
        .filterBounds(aoi)
        .filterDate("2000-01-01", "2023-12-31")
        .select("precipitation")
    )

    proj = chirps.first().projection()

    # Long-term 95th percentile — defines what counts as a heavy day
    p95 = (
        chirps.reduce(ee.Reducer.percentile([95]))
        .setDefaultProjection(proj)
        .rename("p95")
    )

    # Annual heavy rain sum per year
    years = ee.List.sequence(start_year, end_year)

    def annualHmFunc(y):
        start = ee.Date.fromYMD(y, 1, 1)
        end = ee.Date.fromYMD(y, 12, 31)

        heavySum = (
            chirps.filterDate(start, end)
            .map(lambda img: img.multiply(img.gt(p95)))
            .sum()
            .setDefaultProjection(proj)
            .rename("Hm")
            .set("year", y)
        )
        return heavySum

    annualHm = ee.ImageCollection(years.map(annualHmFunc))

    # ===========================================================================
    #                    Z-SCORE ACROSS ALL YEARS
    # ===========================================================================

    hmMean = annualHm.mean().rename("Hm_mean")
    hmStdDev = annualHm.reduce(ee.Reducer.stdDev()).rename("Hm_stdDev")

    def annualZScoreFunc(y):
        year = ee.Number(y)
        hm = annualHm.filter(ee.Filter.eq("year", year)).first()
        z = hm.subtract(hmMean).divide(hmStdDev).rename("zScore")
        return z.set("year", year)

    annualZScore = ee.ImageCollection(years.map(annualZScoreFunc))

    # ===========================================================================
    #                    STACK INTO SINGLE MULTIBAND IMAGE
    # ===========================================================================

    # Build one image with 38 named bands:
    # Hm_2004 ... Hm_2022, zScore_2004 ... zScore_2022

    def add_bands_for_year(y, img):
        y = ee.Number(y).toInt()
        hm_band = (
            annualHm.filter(ee.Filter.eq("year", y))
            .first()
            .rename(ee.String("Hm_").cat(ee.Number(y).format()))
        )
        z_band = (
            annualZScore.filter(ee.Filter.eq("year", y))
            .first()
            .rename(ee.String("zScore_").cat(ee.Number(y).format()))
        )
        return ee.Image(img).addBands(hm_band).addBands(z_band)

    empty_image = ee.Image().mask(ee.Image(0))
    output_image = ee.Image(years.iterate(add_bands_for_year, empty_image))
    output_image = output_image.select(output_image.bandNames().remove("constant"))

    task_id = export_raster_asset_to_gee(
        output_image.clip(aoi), OUTPUT_DESC, OUTPUT_ASSET_ID, scale=5566, region=aoi
    )

    return task_id
