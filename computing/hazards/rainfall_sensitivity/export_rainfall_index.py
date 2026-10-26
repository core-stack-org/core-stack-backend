import ee
from datetime import date

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
    * AGRICULTURAL YEAR CONVENTION:
    * Year `y` = Jul 1 of `y` -> Jun 30 of `y+1`. This applies uniformly to
    * the analysis window, the baseline pooling window, and all band labels.
    * e.g. Hm_2004 = sum over Jul 1, 2004 -> Jun 30, 2005.
    *
    * Computes 5 quantities per pixel per agricultural year and exports as a
    * single multiband asset — one band per year for each quantity:
    *
    * Hm_{year}         = sum of precipitation on heavy days within the ag-year
    * zScore_{year}     = z-score of Hm relative to the baseline ag-year period
    * heavyDays_{year}  = number of heavy-rainfall days within the ag-year
    * heavyAvg_{year}   = average daily precipitation on heavy days
    * maxDay_{year}     = maximum daily precipitation recorded in the ag-year
    *
    * Heavy day definition: daily precipitation > long-term 95th percentile
    * of WET DAYS ONLY (> 1mm) (CHIRPS), pooled over the ag-year-aligned
    * baseline window (see BASELINE_START_YEAR/BASELINE_END_YEAR below).
    *
    * This asset is the direct input to Script 3b, analogous to how
    * SPEI assets are the input to Script 2 (drought).
    *
    * Requires: nothing — only public datasets (CHIRPS)
    """

    ee_initialize(gee_account_id)
    OUTPUT_DESC = f"rain_index_AEZ_{aez}"
    OUTPUT_ASSET_ID = f"projects/corestack-datasets-alpha/assets/datasets/hazards/rainfall_index/{OUTPUT_DESC}"

    if is_gee_asset_exists(OUTPUT_ASSET_ID):
        return None

    if end_year is None:
        raise ValueError(
            "end_year must be specified explicitly — agricultural-year pipelines "
            "cannot infer a safe default."
        )

    # An ag-year `y` isn't complete until Jun 30 of y+1 has passed. Refuse to
    # silently compute a partial year as if it were final.
    ag_year_end_date = date(end_year + 1, 6, 30)
    if ag_year_end_date > date.today():
        raise ValueError(
            f"Agricultural year {end_year} (Jul {end_year} -> Jun {end_year + 1}) "
            f"is not yet complete as of {date.today().isoformat()}. Reduce end_year."
        )

    # Fixed baseline window for zScore normalization — independent of START_YEAR/END_YEAR.
    # Do NOT change this once results are published, or old zScore bands will drift.
    # BASELINE_START_YEAR=2004 -> first baseline ag-year is Jul 2004 - Jun 2005.
    # BASELINE_END_YEAR=2024   -> last baseline ag-year is Jul 2024 - Jun 2025.
    BASELINE_START_YEAR = 2004
    BASELINE_END_YEAR = 2024

    # 1. AOI
    aoi = ee.FeatureCollection(AEZ).filter(ee.Filter.eq("ae_regcode", aez)).geometry()
    # aoi = (
    #     ee.FeatureCollection("projects/ext-datasets/assets/datasets/State_pan_india")
    #     .filter(ee.Filter.eq("Name", "Odisha"))
    #     .geometry()
    # )
    # 2. FULL CHIRPS COLLECTION
    # Deliberately unbounded on the end date. This collection feeds BOTH the
    # baseline percentile calc (bounded separately below) AND the per-year
    # metrics for every analysis year requested — including years beyond
    # BASELINE_END_YEAR. Bounding this collection to the baseline window (as
    # the original calendar-year script effectively did) silently starves
    # any analysis year past the baseline of data.
    chirpsFull = (
        ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")
        .filterBounds(aoi)
        .select("precipitation")
    )

    proj = chirpsFull.first().projection()

    # 3. BASELINE HEAVY RAINFALL THRESHOLD
    # Pooled over ag-year-aligned days only: Jul 1, 2000 -> Jun 30, BASELINE_END_YEAR+1.
    # (2000 kept as the same pool-start buffer as the original script, just
    # shifted to Jul 1 so the pool stays ag-year aligned throughout.)
    chirpsBaseline = chirpsFull.filterDate(
        "2000-07-01", ee.Date.fromYMD(BASELINE_END_YEAR + 1, 6, 30).advance(1, "day")
    )

    # Long-term 95th percentile of WET DAYS ONLY (> 1mm)
    p95 = (
        chirpsBaseline.map(lambda img: img.updateMask(img.gt(1)))
        .reduce(ee.Reducer.percentile([95]))
        .setDefaultProjection(proj)
        .rename("p95")
    )

    # 4. ANNUAL METRICS CALCULATION

    metricsMinYear = min(start_year, BASELINE_START_YEAR)
    metricsMaxYear = max(end_year, BASELINE_END_YEAR)
    metricsYears = ee.List.sequence(metricsMinYear, metricsMaxYear)

    def calc_annual_metrics(y):
        start = ee.Date.fromYMD(y, 7, 1)
        # end-exclusive filterDate, so advance 1 day past Jun 30 to include it
        end = ee.Date.fromYMD(ee.Number(y).add(1), 6, 30).advance(1, "day")

        yearCollection = chirpsFull.filterDate(start, end)

        # Absolute maximum daily rainfall for this ag-year
        maxDay = (
            yearCollection.max().unmask(0).setDefaultProjection(proj).rename("maxDay")
        )

        # Binary mask: isolated heavy rain days
        heavyRainCollection = yearCollection.map(
            lambda img: img.updateMask(img.gt(p95))
        )

        # 1. Ag-year sum of heavy rainfall
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

    # 5. FIXED Z-SCORE CALCULATION

    baselineYears = ee.List.sequence(BASELINE_START_YEAR, BASELINE_END_YEAR)

    baselineMetrics = annualMetrics.filter(ee.Filter.inList("year", baselineYears))

    hmMean = baselineMetrics.select("Hm").mean()
    hmStdDev = baselineMetrics.select("Hm").reduce(ee.Reducer.stdDev())

    def add_zscore(img):
        hm = img.select("Hm")
        z = hm.subtract(hmMean).divide(hmStdDev).rename("zScore")
        return img.addBands(z)

    completedAnnualCollection = annualMetrics.map(add_zscore)

    # 6. SERVER-SIDE STACK INTO SINGLE MULTIBAND IMAGE & EXPORT
    analysisYears = ee.List.sequence(start_year, end_year)

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
