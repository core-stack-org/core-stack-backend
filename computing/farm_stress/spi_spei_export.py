"""Historical SPI-1/SPEI-3 input rasters (plan.md Step 1 / Script 01a).

Builds the GSMaP rainfall accumulation in Earth Engine, then pulls the
result straight to local disk via getDownloadURL (see local_download.py)
rather than an async GCS export - these rasters are small enough (single
band, ~11km resolution) that a direct download is simpler than submitting
and polling a batch export task.

The local gamma/log-logistic fitting and monsoon onset detection run
separately (spi_spei_fit.py, monsoon_onset.py, added in a later pass) once
these rasters are on disk.

Datasets are implemented one at a time, per the phased rollout:
rainfall (export_gsmap_*) first, then PET (export_modis_pet_*, prorated
from MOD16A2GF). Water balance (P - PET) follows once PET is validated.
"""

import os
import time
from datetime import datetime, timedelta

import ee

from utilities.gee_utils import ee_initialize, sync_raster_to_gcs, check_task_status, gcs_config
from computing.farm_stress.local_download import download_image
from computing.farm_stress.helper import generate_28day_periods
from computing.farm_stress.config import (
    GSMAP_COLLECTION,
    GSMAP_BAND,
    MODIS_ET_COLLECTION,
    MODIS_PET_BAND,
    MODIS_PET_SCALE_FACTOR,
    SPI_SCALE_M,
    INDIA_BBOX_COORDS,
    LOCAL_DIR_GSMAP_MONTHLY,
    LOCAL_DIR_MODIS_PET_MONTHLY,
    LOCAL_DIR_GSMAP_DAILY,
    GCS_PATH_MODIS_PET_MONTHLY,
)


def export_gsmap_period(
    period_start,
    period_end,
    period_label,
    gee_account_id,
    output_dir=LOCAL_DIR_GSMAP_MONTHLY,
):
    """Download one 28-day GSMaP rainfall accumulation as a single-band
    GeoTIFF straight to local disk.

    gee_account_id: required, no default - this is the GEEAccount id to
    initialize Earth Engine with. A future API entry point will receive
    this from the caller/request rather than assuming a fixed account.
    """
    ee_initialize(gee_account_id)
    region = ee.Geometry.Rectangle(INDIA_BBOX_COORDS)

    # GSMaP's hourlyPrecipRate is an instantaneous rate (mm/hr) sampled once
    # an hour, not a pre-accumulated hourly total. To get a total accumulated
    # depth over the period: mean(rate over the window) x (hours in window).
    # This is more robust to missing hourly images than a raw sum, since
    # .mean() divides by however many images actually exist, instead of
    # silently treating gaps as zero rainfall.
    hourly = ee.ImageCollection(GSMAP_COLLECTION).select(GSMAP_BAND).filterBounds(region)
    window = hourly.filterDate(period_start, ee.Date(period_end).advance(1, "day"))
    window_hours = ee.Date(period_end).advance(1, "day").difference(
        ee.Date(period_start), "hour"
    )
    period_rainfall = window.mean().multiply(window_hours).rename("precip_mm").clip(region)

    output_path = f"{output_dir.rstrip('/')}/precip_{period_label}.tif"
    download_image(period_rainfall, region, output_path, SPI_SCALE_M)
    print(f"Downloaded -> {output_path}")
    return output_path


def export_gsmap_historical_archive(
    gee_account_id,
    start_year=2000,
    end_year=2025,
    output_dir=LOCAL_DIR_GSMAP_MONTHLY,
    overwrite=False,
    sleep_seconds=0.2,
):
    """Download the full historical GSMaP rainfall archive: one 28-day-period
    GeoTIFF per real EPOCH_ANCHOR period from start_year through end_year.

    gee_account_id: required, no default - see export_gsmap_period.

    Safe to interrupt and re-run: files already on disk are skipped unless
    overwrite=True, so a partial run resumes where it left off instead of
    re-downloading everything.
    """
    ee_initialize(gee_account_id)
    region = ee.Geometry.Rectangle(INDIA_BBOX_COORDS)
    hourly = ee.ImageCollection(GSMAP_COLLECTION).select(GSMAP_BAND).filterBounds(region)

    periods = generate_28day_periods(start_year, end_year)
    output_dir = output_dir.rstrip("/")
    print(f"{len(periods)} periods to process ({start_year}-{end_year})")

    downloaded, skipped = [], []
    for i, period in enumerate(periods, start=1):
        output_path = f"{output_dir}/precip_{period['label']}.tif"
        if os.path.exists(output_path) and not overwrite:
            skipped.append(output_path)
            continue

        print(
            f"[{i}/{len(periods)}] downloading {period['label']} "
            f"({period['period_start']} to {period['period_end']})"
        )
        window = hourly.filterDate(
            period["period_start"], ee.Date(period["period_end"]).advance(1, "day")
        )
        window_hours = ee.Date(period["period_end"]).advance(1, "day").difference(
            ee.Date(period["period_start"]), "hour"
        )
        image = window.mean().multiply(window_hours).rename("precip_mm").clip(region)
        download_image(image, region, output_path, SPI_SCALE_M)
        downloaded.append(output_path)

        if sleep_seconds:
            time.sleep(sleep_seconds)

    print(
        f"Done. Downloaded {len(downloaded)} new file(s), "
        f"skipped {len(skipped)} already on disk."
    )
    return {"downloaded": downloaded, "skipped": skipped}


def export_gsmap_daily_archive(
    gee_account_id,
    start_year=2000,
    end_year=2025,
    output_dir=LOCAL_DIR_GSMAP_DAILY,
    overwrite=False,
    sleep_seconds=0.2,
):
    """Download daily GSMaP rainfall totals for May 1 - Sep 30 each year,
    needed for monsoon onset detection - a day-by-day scan (first 5-day
    burst >=20mm with no >10-day dry spell in the following 21 days) that
    the 28-day period totals can't answer.

    Uses the same direct getDownloadURL path as export_gsmap_historical_archive:
    a single day's GSMaP total is tiny, no reduceResolution/aggregation is
    involved, so there's no size-limit issue like PET had.

    gee_account_id: required, no default - see export_gsmap_period.

    Safe to interrupt and re-run: files already on disk are skipped unless
    overwrite=True.
    """
    ee_initialize(gee_account_id)
    region = ee.Geometry.Rectangle(INDIA_BBOX_COORDS)
    hourly = ee.ImageCollection(GSMAP_COLLECTION).select(GSMAP_BAND).filterBounds(region)

    dates = []
    for year in range(start_year, end_year + 1):
        day = datetime(year, 5, 1)
        season_end = datetime(year, 9, 30)
        while day <= season_end:
            dates.append(day.strftime("%Y-%m-%d"))
            day += timedelta(days=1)

    output_dir = output_dir.rstrip("/")
    print(f"{len(dates)} days to process ({start_year}-{end_year}, May 1 - Sep 30 each year)")

    downloaded, skipped = [], []
    for i, date_str in enumerate(dates, start=1):
        label = date_str.replace("-", "")
        output_path = f"{output_dir}/daily_{label}.tif"
        if os.path.exists(output_path) and not overwrite:
            skipped.append(output_path)
            continue

        window = hourly.filterDate(date_str, ee.Date(date_str).advance(1, "day"))
        image = window.mean().multiply(24).rename("precip_mm").clip(region)
        download_image(image, region, output_path, SPI_SCALE_M)
        downloaded.append(output_path)

        if i % 50 == 0 or i == len(dates):
            print(f"[{i}/{len(dates)}] {label} done")

        if sleep_seconds:
            time.sleep(sleep_seconds)

    print(
        f"Done. Downloaded {len(downloaded)} new file(s), "
        f"skipped {len(skipped)} already on disk."
    )
    return {"downloaded": downloaded, "skipped": skipped}


def export_modis_pet_historical_archive(
    gee_account_id,
    start_year=2000,
    end_year=2025,
    output_dir=LOCAL_DIR_MODIS_PET_MONTHLY,
    overwrite=False,
    poll_seconds=30,
):
    """Download the full historical MOD16A2GF PET archive: one 28-day-period
    GeoTIFF per real EPOCH_ANCHOR period, prorated from 8-day composites and
    aggregated 500m -> 11km to match the GSMaP rainfall grid.

    MOD16A2GF's PET band is already an accumulated depth for its 8-day
    composite (mm/8day, after the x0.1 scale factor) - not a rate like
    GSMaP - so composites are combined by day-overlap proration rather than
    a mean-x-hours conversion:
      1. Every composite overlapping the 28-day period contributes.
      2. Its weight is the fraction of its own days that fall inside this
         period: overlap_days / composite_length_days. A composite fully
         inside the period gets weight 1; one straddling a boundary gets
         partial weight on each side.
      3. composite_length_days comes from the composite's own
         system:time_start/system:time_end, not a hardcoded 8 - the last
         composite of each year is only 5-6 days.
      4. Weighted composites are summed -> the period's PET total at 500m,
         then reduceResolution+reproject aggregates that down to 11km.

    Unlike rainfall, this can't use the direct getDownloadURL path: a
    full-India image at MODIS's native 500m is ~140MB, well over Earth
    Engine's ~48MB direct-download request cap (confirmed empirically -
    it fails even for a single raw composite, before any of our own
    processing). reduceResolution needs that full native-resolution input
    materialized to aggregate correctly, so this uses the async batch
    export (GCS) + download path instead. Rainfall never needed this
    because GSMaP is already ~11km natively - no fine-to-coarse
    aggregation, so no native-resolution materialization was ever
    required for it.

    Every period's export task is submitted up front, then waited on as
    one batch, then downloaded - not submit-wait-download one period at a
    time. Earth Engine runs many export tasks concurrently server-side, so
    waiting for each one individually before submitting the next serialises
    work that GEE could otherwise be doing in parallel.

    gee_account_id: required, no default - see export_gsmap_period.

    Safe to interrupt and re-run: files already on disk are skipped unless
    overwrite=True, so a partial run resumes where it left off instead of
    re-downloading everything.
    """
    ee_initialize(gee_account_id)
    region = ee.Geometry.Rectangle(INDIA_BBOX_COORDS)

    def scale_pet(img):
        img = ee.Image(img)
        return ee.Image(
            img.multiply(MODIS_PET_SCALE_FACTOR).copyProperties(
                img, ["system:time_start", "system:time_end"]
            )
        )

    # ImageCollection.sum() (used below, per-period) drops the concrete
    # 500m MODIS pixel grid and returns an "unbounded" default projection,
    # which reduceResolution then refuses to work with. Capture the native
    # projection once here and reattach it to each period's summed image.
    native_projection = ee.ImageCollection(MODIS_ET_COLLECTION).first().select(MODIS_PET_BAND).projection()

    pet_col = (
        ee.ImageCollection(MODIS_ET_COLLECTION)
        .select(MODIS_PET_BAND)
        .filterBounds(region)
        .map(scale_pet)
    )

    periods = generate_28day_periods(start_year, end_year)
    output_dir = output_dir.rstrip("/")
    print(f"{len(periods)} periods to process ({start_year}-{end_year})")

    # Pass 1: build and submit every period's export task without waiting.
    pending = []  # (period, task_id, layer_name, output_path)
    skipped = []
    for i, period in enumerate(periods, start=1):
        output_path = f"{output_dir}/pet_{period['label']}.tif"
        if os.path.exists(output_path) and not overwrite:
            skipped.append(output_path)
            continue

        # Half-open interval [period_start_ms, period_end_ms), matching how
        # composite system:time_start/time_end are defined, so the overlap
        # arithmetic below is consistent at both ends.
        period_start_ms = ee.Date(period["period_start"]).millis()
        period_end_ms = ee.Date(period["period_end"]).advance(1, "day").millis()

        # Widen the filterDate a bit beyond the period so boundary
        # composites (which only partially overlap) are still included.
        overlapping = pet_col.filterDate(
            ee.Date(period["period_start"]).advance(-8, "day"),
            ee.Date(period["period_end"]).advance(9, "day"),
        )

        def prorate(img):
            img = ee.Image(img)
            c_start = ee.Number(img.get("system:time_start"))
            c_end = ee.Number(img.get("system:time_end"))
            overlap_ms = c_end.min(period_end_ms).subtract(c_start.max(period_start_ms)).max(0)
            composite_ms = c_end.subtract(c_start)
            weight = overlap_ms.divide(composite_ms)
            # .toFloat() forces a homogeneous pixel type across every
            # composite: without it, each image's declared numeric range
            # differs by its own weight, and ImageCollection.sum() rejects
            # the mismatch ("Expected a homogeneous image collection...").
            return img.multiply(weight).toFloat()

        pet_28d_500m = overlapping.map(prorate).sum().setDefaultProjection(native_projection)
        pet_28d_11km = (
            pet_28d_500m.reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1024)
            .reproject(crs="EPSG:4326", scale=SPI_SCALE_M)
            .rename("pet_mm")
            .clip(region)
        )

        layer_name = f"pet_{period['label']}"
        task_id = sync_raster_to_gcs(
            pet_28d_11km, SPI_SCALE_M, layer_name, gcs_path=GCS_PATH_MODIS_PET_MONTHLY
        )
        print(f"[{i}/{len(periods)}] submitted {period['label']} -> task {task_id}")
        pending.append((period, task_id, layer_name, output_path))

    # Pass 2: wait for the whole batch at once. GEE processes many export
    # tasks concurrently, so this is not "sum of each task's own runtime" -
    # it's roughly however long the slowest wave of the batch takes.
    print(f"Submitted {len(pending)} task(s), waiting for the batch to finish...")
    check_task_status([task_id for _, task_id, _, _ in pending], sleep_time=poll_seconds)

    # Pass 3: download every finished file from GCS.
    bucket = gcs_config(gee_account_id)
    downloaded = []
    for period, task_id, layer_name, output_path in pending:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        blob = bucket.blob(f"{GCS_PATH_MODIS_PET_MONTHLY.rstrip('/')}/{layer_name}.tif")
        blob.download_to_filename(output_path)
        print(f"Downloaded -> {output_path}")
        downloaded.append(output_path)

    print(
        f"Done. Downloaded {len(downloaded)} new file(s), "
        f"skipped {len(skipped)} already on disk."
    )
    return {"downloaded": downloaded, "skipped": skipped}
