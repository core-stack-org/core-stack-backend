import ee
from datetime import date

from utilities.constants import AEZ
from utilities.gee_utils import (
    export_raster_asset_to_gee,
    ee_initialize,
    is_gee_asset_exists,
)


def fire_index(aez, start_year=2004, end_year=2024, gee_account_id=None):
    """
    * Forest Sensitivity Analysis Pipeline — Fire Script
    * Fire Radiative Power (FRP) Index Export (Threshold > 30)
    *
    * AGRICULTURAL YEAR CONVENTION:
    * Year `y` = Jul 1 of `y` -> Jun 30 of `y+1`, matching Scripts 1/3a/3b.
    * MOD14A1 is a continuous daily product with no dataset-end constraint
    * (unlike GLC-FCS30D in Script 1), so this windows the same
    * mechanical way as rainfall_index — straight Jul-Jun per ag-year.
    *
    * This also fixes a real seasonal-splitting issue the old calendar-year
    * windowing had: central India's burn season (~Oct-Nov post-kharif
    * stubble burning through Feb-June dry-season/forest-fire peak) used
    * to get cut in half across the Dec31/Jan1 boundary — Oct-Dec landing
    * in year Y, Jan-June in year Y+1. A Jul-Jun ag-year keeps that whole
    * season in one window.
    *
    * Computes 5 quantities per pixel per ag-year and exports as a single
    * multiband asset — one band per year for each quantity:
    *
    * FRP_{year}        = sum of FRP on fire days within the ag-year
    * zScore_{year}     = z-score of FRP relative to the baseline ag-year period
    * fireDays_{year}   = number of fire days (FRP > 30) within the ag-year
    * fireAvg_{year}    = average daily FRP on fire days
    * maxFRP_{year}     = maximum daily FRP recorded in the ag-year
    *
    * Fire day definition: daily MaxFRP > 30 (MODIS Terra Thermal Anomalies).
    * NOTE: MaxFRP's documented GEE scale factor is 0.1 (raw -> MW) and this
    * threshold is compared against the raw band with no scaling applied —
    * left as-is deliberately, matching the threshold choice already made
    * in the resistance/resilience script for this index.
    *
    * Requires: nothing — only public datasets (MODIS MOD14A1)
    """
    ee_initialize(gee_account_id)
    OUTPUT_DESC = f"fire_index_FRP30_AEZ_{aez}"
    OUTPUT_ASSET_ID = (
        f"projects/corestack-datasets-alpha/assets/datasets/hazards/{OUTPUT_DESC}"
    )

    if is_gee_asset_exists(OUTPUT_ASSET_ID):
        return None

    if end_year is None:
        raise ValueError(
            "end_year must be specified explicitly — agricultural-year pipelines "
            "cannot infer a safe default."
        )

    aoi = ee.FeatureCollection(AEZ).filter(ee.Filter.eq("ae_regcode", aez)).geometry()
    # aoi = (
    #     ee.FeatureCollection("projects/ext-datasets/assets/datasets/State_pan_india")
    #     .filter(ee.Filter.eq("Name", "Odisha"))
    #     .geometry()
    # )
    FRP_THRESHOLD = 30
    """
    Fixed baseline window for zScore normalization — independent of START_YEAR/END_YEAR.
    Matching the same 2004-2024 ag-year window locked in for rain and (once converted)
    drought, so all z-score-based indices are normalized against the same reference period.
    Do NOT change this once results are published, or old zScore bands will drift
    when we extend the analysis timeline later.
    """
    BASELINE_START_YEAR = 2004
    BASELINE_END_YEAR = 2024

    # Only analysisYears (start_year..end_year) get stacked into the final
    # exported asset — NOT the full baseline window. If a caller ever
    # narrows start_year/end_year to something that doesn't fully cover the
    # baseline, downstream scripts (resistance/resilience) that assume
    # zScore_2004..zScore_2024 all exist will fail trying to select a band
    # that was never exported. Fail loudly here instead.
    if start_year > BASELINE_START_YEAR or end_year < BASELINE_END_YEAR:
        raise ValueError(
            f"start_year ({start_year}) and end_year ({end_year}) must fully "
            f"cover the frozen baseline window ({BASELINE_START_YEAR}-"
            f"{BASELINE_END_YEAR}), or the exported asset will be missing "
            "zScore bands that downstream scripts require."
        )

    # MODIS fire is a near-daily-updated product — ag-year end_year isn't
    # complete until Jun 30 of end_year+1 has passed. Refuse to silently
    # compute a partial year.
    required_complete_by = date(end_year + 1, 6, 30)
    if required_complete_by > date.today():
        raise ValueError(
            f"Agricultural year {end_year} (Jul {end_year} -> Jun {end_year + 1}) "
            f"is not yet complete as of {date.today().isoformat()}. Reduce end_year."
        )

    #  Terra Thermal Anomalies & Fire Daily 1km
    modisFire = (
        ee.ImageCollection("MODIS/061/MOD14A1").filterBounds(aoi).select("MaxFRP")
    )

    proj = modisFire.first().projection()

    # 1. ANNUAL METRICS CALCULATION
    # We need ag-year metrics computed for every year covering BOTH the analysis window
    # and the baseline window, whichever stretches further.
    metricsMinYear = min(start_year, BASELINE_START_YEAR)
    metricsMaxYear = max(end_year, BASELINE_END_YEAR)
    metricsYears = ee.List.sequence(metricsMinYear, metricsMaxYear)

    def annual_collection(y):
        start = ee.Date.fromYMD(y, 7, 1)
        # end-exclusive filterDate, so advance 1 day past Jun 30 to include it
        end = ee.Date.fromYMD(ee.Number(y).add(1), 6, 30).advance(1, "day")

        yearCollection = modisFire.filterDate(start, end)

        #  Absolute maximum daily FRP for this ag-year
        maxFRP = (
            yearCollection.max().unmask(0).setDefaultProjection(proj).rename("maxFRP")
        )

        #  Binary mask: isolate days where FRP > 30
        fireDaysCollection = yearCollection.map(
            lambda img: img.updateMask(img.gt(FRP_THRESHOLD))
        )

        #  1. Ag-year sum of FRP on fire days
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

        return (
            ee.Image(acc)
            .addBands(frpBand)
            .addBands(zBand)
            .addBands(fireDaysBand)
            .addBands(fireAvgBand)
            .addBands(maxFRPBand)
        )

    empty_image = ee.Image().mask(ee.Image(0))
    output_image = ee.Image(analysisYears.iterate(add_bands_for_year, empty_image))

    output_image = output_image.select(output_image.bandNames().remove("constant"))

    #  Export execution block
    task_id = export_raster_asset_to_gee(
        output_image.clip(aoi), OUTPUT_DESC, OUTPUT_ASSET_ID, scale=1000, region=aoi
    )

    total_bands = (end_year - start_year + 1) * 5
    print(" Clean pipeline compilation verified.")
    print(f"Ready to execute in the tasks tab. Total structured bands: {total_bands}.")

    return task_id
