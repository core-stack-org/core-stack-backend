import ee

from utilities.constants import AEZ
from utilities.gee_utils import (
    ee_initialize,
    is_gee_asset_exists,
    export_raster_asset_to_gee,
)


def rainfall_index(aez, start_year=2004, end_year=None, gee_account_id=None):
    """
    * Forest Sensitivity Analysis Pipeline — Script 3a
    * Heavy Rainfall Index Export (Wet-Days Only Threshold + Extended Metrics)
    *
    * Computes 5 quantities per pixel per year and exports as a single
    * multiband asset — one band per year for each quantity:
    *
    * Hm_{year}         = annual sum of precipitation on heavy days
    * zScore_{year}     = z-score of Hm relative to the 2004-2022 period
    * heavyDays_{year}  = number of days in the year with heavy rainfall
    * heavyAvg_{year}   = average daily precipitation on heavy rainfall days
    * maxDay_{year}     = maximum daily precipitation recorded in that year
    *
    * Heavy day definition: daily precipitation > long-term 95th percentile
    * of WET DAYS ONLY (> 1mm) (CHIRPS)
    *
    * This asset is the direct input to Script 3b, analogous to how
    * SPEI assets are the input to Script 2 (drought).
    *
    * Requires: nothing — only public datasets (CHIRPS)
    """

    ee_initialize(gee_account_id)
    OUTPUT_DESC = f"rain_index_AEZ_{aez}"
    OUTPUT_ASSET_ID = (
        f"projects/corestack-datasets-alpha/assets/datasets/SPEI/{OUTPUT_DESC}"
    )

    if is_gee_asset_exists(OUTPUT_ASSET_ID):
        return None

    # Fixed baseline window for zScore normalization — independent of START_YEAR/END_YEAR.
    # Do NOT change this once results are published, or old zScore bands will drift
    # when the pipeline timeline is extended.
    BASELINE_START_YEAR = 2004
    BASELINE_END_YEAR = 2024

    # 1. AOI
    aoi = ee.FeatureCollection(AEZ).filter(ee.Filter.eq("ae_regcode", aez)).geometry()
    # aoi = (
    #     ee.FeatureCollection("projects/ext-datasets/assets/datasets/State_pan_india")
    #     .filter(ee.Filter.eq("Name", "Odisha"))
    #     .geometry()
    # )

    # 2. BASELINE HEAVY RAINFALL THRESHOLD
    chirps = (
        ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")
        .filterBounds(aoi)
        .filterDate("2000-01-01", ee.Date.fromYMD(BASELINE_END_YEAR, 12, 31))
        .select("precipitation")
    )

    proj = chirps.first().projection()

    # Long-term 95th percentile of WET DAYS ONLY (> 1mm)
    p95 = (
        chirps.map(lambda img: img.updateMask(img.gt(1)))
        .reduce(ee.Reducer.percentile([95]))
        .setDefaultProjection(proj)
        .rename("p95")
    )

    # 3. ANNUAL METRICS CALCULATION

    metricsMinYear = min(start_year, BASELINE_START_YEAR)
    metricsMaxYear = max(end_year, BASELINE_END_YEAR)
    metricsYears = ee.List.sequence(metricsMinYear, metricsMaxYear)

    def calc_annual_metrics(y):
        start = ee.Date.fromYMD(y, 1, 1)
        end = ee.Date.fromYMD(y, 12, 31)

        yearCollection = chirps.filterDate(start, end)

        # Absolute maximum daily rainfall for this year
        maxDay = (
            yearCollection.max().unmask(0).setDefaultProjection(proj).rename("maxDay")
        )

        # Binary mask: isolated heavy rain days
        heavyRainCollection = yearCollection.map(
            lambda img: img.updateMask(img.gt(p95))
        )

        # 1. Annual sum of heavy rainfall
        hm = (
            heavyRainCollection.map(lambda img: img.unmask(0))
            .sum()
            .setDefaultProjection(proj)
            .rename("Hm")
        )

        # 2. Number of heavy days
        heavyDays = (
            heavyRainCollection.map(lambda img: img.mask())
            .sum()
            .unmask(0)
            .setDefaultProjection(proj)
            .rename("heavyDays")
        )

        # 3. Average intensity of heavy days
        heavyAvg = (
            heavyRainCollection.mean()
            .unmask(0)
            .setDefaultProjection(proj)
            .rename("heavyAvg")
        )

        # Combine metrics into a single image per year containing all 4 base properties
        return hm.addBands(heavyDays).addBands(heavyAvg).addBands(maxDay).set("year", y)

    annualMetrics = ee.ImageCollection(metricsYears.map(calc_annual_metrics))

    # 4. FIXED Z-SCORE CALCULATION

    baselineYears = ee.List.sequence(BASELINE_START_YEAR, BASELINE_END_YEAR)

    baselineMetrics = annualMetrics.filter(ee.Filter.inList("year", baselineYears))

    hmMean = baselineMetrics.select("Hm").mean()
    hmStdDev = baselineMetrics.select("Hm").reduce(ee.Reducer.stdDev())

    # Changed to server-side map architecture instead of the annual image collection, done for GEE optimisation.. as it's a better practice.
    def calc_annual_metrics(img):
        hm = img.select("Hm")
        z = hm.subtract(hmMean).divide(hmStdDev).rename("zScore")
        return img.addBands(z)

    completedAnnualCollection = annualMetrics.map(calc_annual_metrics)

    # 5. SERVER-SIDE STACK INTO SINGLE MULTIBAND IMAGE & EXPORT
    analysisYears = ee.List.sequence(start_year, end_year)

    # Changed server-side iteration style to stack and correctly rename bands
    def add_bands_for_year(y, acc):
        yearStr = ee.String(ee.Number(y).toInt())
        yearImg = completedAnnualCollection.filter(ee.Filter.eq("year", y)).first()

        hmBand = yearImg.select("Hm").rename(ee.String("Hm_").cat(yearStr))
        zBand = yearImg.select("zScore").rename(ee.String("zScore_").cat(yearStr))
        heavyDaysBand = yearImg.select("heavyDays").rename(
            ee.String("heavyDays_").cat(yearStr)
        )
        heavyAvgBand = yearImg.select("heavyAvg").rename(
            ee.String("heavyAvg_").cat(yearStr)
        )
        maxDayBand = yearImg.select("maxDay").rename(ee.String("maxDay_").cat(yearStr))

        return (
            ee.Image(acc)
            .addBands(hmBand)
            .addBands(zBand)
            .addBands(heavyDaysBand)
            .addBands(heavyAvgBand)
            .addBands(maxDayBand)
        )

    empty_image = ee.Image().mask(ee.Image(0))
    output_image = ee.Image(analysisYears.iterate(add_bands_for_year, empty_image))

    output_image = output_image.select(output_image.bandNames().remove("constant"))

    task_id = export_raster_asset_to_gee(
        output_image.clip(aoi), OUTPUT_DESC, OUTPUT_ASSET_ID, scale=5566, region=aoi
    )

    return task_id
