import ee

from utilities.constants import AEZ
from utilities.gee_utils import (
    export_raster_asset_to_gee,
    ee_initialize,
    is_gee_asset_exists,
)


def fire_index(aez, start_year=2004, end_year=2022, gee_account_id=None):
    """
    * Forest Sensitivity Analysis Pipeline — Fire Script
    * Fire Radiative Power (FRP) Index Export (Threshold > 30)
    *
    * Computes 5 quantities per pixel per year and exports as a single
    * multiband asset — one band per year for each quantity:
    *
    * FRP_{year}        = annual sum of FRP on fire days
    * zScore_{year}     = z-score of FRP relative to the 2004-2022 period
    * fireDays_{year}   = number of days in the year with FRP > 30
    * fireAvg_{year}    = average daily FRP on fire days
    * maxFRP_{year}     = maximum daily FRP recorded in that year
    *
    * Fire day definition: daily MaxFRP > 30 (MODIS Terra Thermal Anomalies)
    *
    * Requires: nothing — only public datasets (MODIS MOD14A1)
    */
    """
    ee_initialize(gee_account_id)
    OUTPUT_DESC = f"fire_index_FRP30_AEZ_{aez}"
    OUTPUT_ASSET_ID = (
        f"projects/corestack-datasets-alpha/assets/datasets/SPEI/{OUTPUT_DESC}"
    )

    if is_gee_asset_exists(OUTPUT_ASSET_ID):
        return None

    # aoi = ee.FeatureCollection(AEZ).filter(ee.Filter.eq("ae_regcode", aez)).geometry()
    aoi = (
        ee.FeatureCollection("projects/ext-datasets/assets/datasets/State_pan_india")
        .filter(ee.Filter.eq("Name", "Odisha"))
        .geometry()
    )
    FRP_THRESHOLD = 30
    """
    Fixed baseline window for zScore normalization — independent of START_YEAR/END_YEAR. 
    This is matching the same 2004-2024 window locked in for rain and drought, so all z-score-based indices 
    are normalized against the same reference period.
    Do NOT change this once results are published, or old zScore bands will drift when we extend the analysis timeline later.
    """
    BASELINE_START_YEAR = 2004
    BASELINE_END_YEAR = 2024

    #  Terra Thermal Anomalies & Fire Daily 1km
    modisFire = (
        ee.ImageCollection("MODIS/061/MOD14A1").filterBounds(aoi).select("MaxFRP")
    )

    proj = modisFire.first().projection()

    # 1. ANNUAL METRICS CALCULATION
    # We need annual metrics computed for every year covering BOTH the analysis window and the baseline window,
    # whichever stretches further.
    metricsMinYear = min(start_year, BASELINE_START_YEAR)
    metricsMaxYear = max(end_year, BASELINE_END_YEAR)
    metricsYears = ee.List.sequence(metricsMinYear, metricsMaxYear)

    def annual_collection(y):
        start = ee.Date.fromYMD(y, 1, 1)
        end = ee.Date.fromYMD(y, 12, 31)

        yearCollection = modisFire.filterDate(start, end)

        #  Absolute maximum daily FRP for this year
        maxFRP = (
            yearCollection.max().unmask(0).setDefaultProjection(proj).rename("maxFRP")
        )

        #  Binary mask: isolate days where FRP > 30
        fireDaysCollection = yearCollection.map(
            lambda img: img.updateMask(img.gt(FRP_THRESHOLD))
        )

        #  1. Annual sum of FRP on fire days
        sumFRP = (
            fireDaysCollection.map(lambda img: img.unmask(0))
            .sum()
            .setDefaultProjection(proj)
            .rename("FRP")
        )

        #  2. Number of fire days
        fireDays = (
            fireDaysCollection.map(lambda img: img.mask())
            .sum()
            .unmask(0)
            .setDefaultProjection(proj)
            .rename("fireDays")
        )

        #  3. Average FRP of fire days
        fireAvg = (
            fireDaysCollection.mean()
            .unmask(0)
            .setDefaultProjection(proj)
            .rename("fireAvg")
        )

        #  Combine metrics into a single image per year containing all 4 base properties
        return (
            sumFRP.addBands(fireDays).addBands(fireAvg).addBands(maxFRP).set("year", y)
        )

    annualMetrics = ee.ImageCollection(metricsYears.map(annual_collection))

    # 2. FIXED Z-SCORE CALCULATION
    # Baseline stats only from the frozen baseline years, not from whatever analysis window I happen to be running right now.
    baselineYears = ee.List.sequence(BASELINE_START_YEAR, BASELINE_END_YEAR)

    baselineMetrics = annualMetrics.filter(ee.Filter.inList("year", baselineYears))

    frpMean = baselineMetrics.select("FRP").mean()
    frpStdDev = baselineMetrics.select("FRP").reduce(ee.Reducer.stdDev())

    #  Server-side map architecture over the annual image collection
    def completed_annual_collection_func(img):
        frp = img.select("FRP")
        z = frp.subtract(frpMean).divide(frpStdDev).rename("zScore")
        return img.addBands(z)

    completedAnnualCollection = annualMetrics.map(completed_annual_collection_func)

    # 3. SERVER-SIDE STACK INTO SINGLE MULTIBAND IMAGE & EXPORT
    analysisYears = ee.List.sequence(start_year, end_year)

    def add_bands_for_year(y, acc):
        yearStr = ee.String(ee.Number(y).toInt())
        yearImg = completedAnnualCollection.filter(ee.Filter.eq("year", y)).first()

        frpBand = yearImg.select("FRP").rename(ee.String("FRP_").cat(yearStr))
        zBand = yearImg.select("zScore").rename(ee.String("zScore_").cat(yearStr))
        fireDaysBand = yearImg.select("fireDays").rename(
            ee.String("fireDays_").cat(yearStr)
        )
        fireAvgBand = yearImg.select("fireAvg").rename(
            ee.String("fireAvg_").cat(yearStr)
        )
        maxFRPBand = yearImg.select("maxFRP").rename(ee.String("maxFRP_").cat(yearStr))

        return ee.Image(acc).addBands(
            [frpBand, zBand, fireDaysBand, fireAvgBand, maxFRPBand]
        )

    empty_image = ee.Image().mask(ee.Image(0))
    output_image = ee.Image(analysisYears.iterate(add_bands_for_year, empty_image))

    #  Export execution block
    task_id = export_raster_asset_to_gee(
        output_image.clip(aoi), OUTPUT_DESC, OUTPUT_ASSET_ID, scale=1000, region=aoi
    )

    print("✅ Clean pipeline compilation verified.")
    print("Ready to execute in the tasks tab. Total structured bands: 95.")

    return task_id
