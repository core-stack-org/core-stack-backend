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
    EXPORT_SCALE_M,
    INDIA_BBOX_COORDS,
    LOCAL_DIR_GSMAP_MONTHLY,
    LOCAL_DIR_MODIS_PET_MONTHLY,
    LOCAL_DIR_GSMAP_DAILY,
    GCS_PATH_MODIS_PET_MONTHLY,
    LOCAL_DIR_GSMAP_500M,
    LOCAL_DIR_MODIS_PET_500M,
    GCS_PATH_GSMAP_500M,
    GCS_PATH_MODIS_PET_500M,
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


# ── 500m SPEI-3 revision ────────────────────────────────────────────────────
# GSMaP is only ~11km natively, so there's no genuine 500m rainfall
# information to export - export_gsmap_500m_archive resamples the same
# 11km field onto a 500m grid via bilinear interpolation (smoother than
# GEE's default nearest-neighbor, which would otherwise render as
# visibly blocky 11km cells at 500m). export_modis_pet_500m_archive
# reuses export_modis_pet_historical_archive's exact 28-day proration
# logic, just stopping before its reduceResolution-to-11km step - PET is
# already natively 500m, so no resampling choice applies to it.
#
# Both export yearly multi-band assets (one band per 28-day period that
# falls in that calendar year, ~13 bands/year) rather than one task per
# period - 340 periods as individual tasks would repeat the exact quota
# problem VCI's per-composite export hit (~575 tasks) before being
# restructured the same way. _split_yearly_bands_to_periods() below then
# splits each downloaded yearly file back into the single-band
# per-period files (precip_{label}.tif / pet_{label}.tif) the rest of
# the pipeline (water_balance.py, spei_fit.py) already expects - a cheap
# local step, no extra GEE cost. Band order isn't read back from any
# stored metadata: generate_28day_periods() is a pure/deterministic
# function, so both the export step and the split step independently
# compute the identical sorted period list for a given year and agree on
# which band is which without needing a sidecar file.


def _periods_by_year(start_year, end_year):
    """{year: [period, ...]} - periods grouped by the calendar year of
    their period_start, sorted chronologically within each year. Pure
    function of generate_28day_periods, safe to call independently at
    export time and at split time and get the same grouping both times.
    """
    periods = generate_28day_periods(start_year, end_year)
    by_year = {}
    for period in periods:
        year = int(period["period_start"][:4])
        by_year.setdefault(year, []).append(period)
    for year in by_year:
        by_year[year].sort(key=lambda p: p["period_start"])
    return by_year


def export_gsmap_500m_archive(
    gee_account_id,
    start_year=2000,
    end_year=2025,
    output_dir=LOCAL_DIR_GSMAP_500M,
    overwrite=False,
    poll_seconds=30,
):
    """Download the historical GSMaP rainfall archive resampled to 500m
    (bilinear), as yearly multi-band GCS exports later split into the
    per-period files the rest of the pipeline expects (see module
    comment above) - the 500m companion to export_gsmap_historical_archive.

    gee_account_id: required, no default - see export_gsmap_period.

    Safe to interrupt and re-run: years whose split-out period files are
    all already on disk are skipped unless overwrite=True.
    """
    ee_initialize(gee_account_id)
    region = ee.Geometry.Rectangle(INDIA_BBOX_COORDS)
    hourly = ee.ImageCollection(GSMAP_COLLECTION).select(GSMAP_BAND).filterBounds(region)

    by_year = _periods_by_year(start_year, end_year)
    output_dir = output_dir.rstrip("/")
    print(f"{len(by_year)} year(s) to process ({start_year}-{end_year})")

    pending = []  # (year, periods, task_id, layer_name)
    skipped = []
    for year in sorted(by_year):
        periods = by_year[year]
        if not overwrite and all(
            os.path.exists(f"{output_dir}/precip_{p['label']}.tif") for p in periods
        ):
            skipped.append(year)
            continue

        band_images = []
        for period in periods:
            window = hourly.filterDate(
                period["period_start"], ee.Date(period["period_end"]).advance(1, "day")
            )
            window_hours = ee.Date(period["period_end"]).advance(1, "day").difference(
                ee.Date(period["period_start"]), "hour"
            )
            # .resample('bilinear') changes how the image is interpolated at
            # its next reprojection (the implicit one Export.image.toCloudStorage
            # performs to reach scale=EXPORT_SCALE_M/500m below) - without it,
            # GEE defaults to nearest-neighbor, which would just tile each
            # ~11km GSMaP cell into a blocky grid of identical 500m pixels
            # instead of a smooth interpolated surface.
            band = (
                window.mean()
                .multiply(window_hours)
                .resample("bilinear")
                .rename(f"period_{period['label']}")
            )
            band_images.append(band)

        combined = ee.Image.cat(band_images).clip(region)
        layer_name = f"precip_500m_{year}"
        task_id = sync_raster_to_gcs(combined, EXPORT_SCALE_M, layer_name, gcs_path=GCS_PATH_GSMAP_500M)
        print(f"Submitted {year} ({len(periods)} bands) -> task {task_id}")
        pending.append((year, periods, task_id, layer_name))

    print(f"Submitted {len(pending)} year(s), waiting for the batch to finish...")
    check_task_status([task_id for _, _, task_id, _ in pending], sleep_time=poll_seconds)

    bucket = gcs_config(gee_account_id)
    os.makedirs(output_dir, exist_ok=True)
    downloaded = []
    for year, periods, task_id, layer_name in pending:
        yearly_path = f"{output_dir}/_yearly_{layer_name}.tif"
        blob = bucket.blob(f"{GCS_PATH_GSMAP_500M.rstrip('/')}/{layer_name}.tif")
        blob.download_to_filename(yearly_path)
        print(f"Downloaded -> {yearly_path}, splitting into {len(periods)} period file(s) ...")
        split_paths = _split_yearly_bands_to_periods(yearly_path, periods, output_dir, "precip")
        os.remove(yearly_path)
        downloaded.extend(split_paths)

    print(
        f"Done. Downloaded/split {len(downloaded)} period file(s) across "
        f"{len(pending)} year(s), skipped {len(skipped)} year(s) already on disk."
    )
    return {"downloaded": downloaded, "skipped_years": skipped}


def export_modis_pet_500m_archive(
    gee_account_id,
    start_year=2000,
    end_year=2025,
    output_dir=LOCAL_DIR_MODIS_PET_500M,
    overwrite=False,
    poll_seconds=30,
):
    """Download the historical MOD16A2GF PET archive at its true native
    500m resolution, as yearly multi-band GCS exports later split into
    per-period files (see module comment above) - the 500m companion to
    export_modis_pet_historical_archive.

    Identical 28-day proration logic (day-overlap weighted sum of 8-day
    composites) as the 11km version - this is that one with the
    reduceResolution/reproject-to-11km step removed, keeping pet_28d_500m
    directly instead of aggregating it down first.

    gee_account_id: required, no default - see export_gsmap_period.

    Safe to interrupt and re-run: years whose split-out period files are
    all already on disk are skipped unless overwrite=True.
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

    native_projection = ee.ImageCollection(MODIS_ET_COLLECTION).first().select(MODIS_PET_BAND).projection()

    pet_col = (
        ee.ImageCollection(MODIS_ET_COLLECTION)
        .select(MODIS_PET_BAND)
        .filterBounds(region)
        .map(scale_pet)
    )

    by_year = _periods_by_year(start_year, end_year)
    output_dir = output_dir.rstrip("/")
    print(f"{len(by_year)} year(s) to process ({start_year}-{end_year})")

    pending = []
    skipped = []
    for year in sorted(by_year):
        periods = by_year[year]
        if not overwrite and all(
            os.path.exists(f"{output_dir}/pet_{p['label']}.tif") for p in periods
        ):
            skipped.append(year)
            continue

        band_images = []
        for period in periods:
            period_start_ms = ee.Date(period["period_start"]).millis()
            period_end_ms = ee.Date(period["period_end"]).advance(1, "day").millis()

            overlapping = pet_col.filterDate(
                ee.Date(period["period_start"]).advance(-8, "day"),
                ee.Date(period["period_end"]).advance(9, "day"),
            )

            def prorate(img, period_start_ms=period_start_ms, period_end_ms=period_end_ms):
                img = ee.Image(img)
                c_start = ee.Number(img.get("system:time_start"))
                c_end = ee.Number(img.get("system:time_end"))
                overlap_ms = c_end.min(period_end_ms).subtract(c_start.max(period_start_ms)).max(0)
                composite_ms = c_end.subtract(c_start)
                weight = overlap_ms.divide(composite_ms)
                return img.multiply(weight).toFloat()

            band = (
                overlapping.map(prorate)
                .sum()
                .setDefaultProjection(native_projection)
                .rename(f"period_{period['label']}")
            )
            band_images.append(band)

        combined = ee.Image.cat(band_images).clip(region)
        layer_name = f"pet_500m_{year}"
        task_id = sync_raster_to_gcs(combined, EXPORT_SCALE_M, layer_name, gcs_path=GCS_PATH_MODIS_PET_500M)
        print(f"Submitted {year} ({len(periods)} bands) -> task {task_id}")
        pending.append((year, periods, task_id, layer_name))

    print(f"Submitted {len(pending)} year(s), waiting for the batch to finish...")
    check_task_status([task_id for _, _, task_id, _ in pending], sleep_time=poll_seconds)

    bucket = gcs_config(gee_account_id)
    os.makedirs(output_dir, exist_ok=True)
    downloaded = []
    for year, periods, task_id, layer_name in pending:
        yearly_path = f"{output_dir}/_yearly_{layer_name}.tif"
        blob = bucket.blob(f"{GCS_PATH_MODIS_PET_500M.rstrip('/')}/{layer_name}.tif")
        blob.download_to_filename(yearly_path)
        print(f"Downloaded -> {yearly_path}, splitting into {len(periods)} period file(s) ...")
        split_paths = _split_yearly_bands_to_periods(yearly_path, periods, output_dir, "pet")
        os.remove(yearly_path)
        downloaded.extend(split_paths)

    print(
        f"Done. Downloaded/split {len(downloaded)} period file(s) across "
        f"{len(pending)} year(s), skipped {len(skipped)} year(s) already on disk."
    )
    return {"downloaded": downloaded, "skipped_years": skipped}


def _split_yearly_bands_to_periods(yearly_path, periods, output_dir, file_prefix):
    """Split one yearly multi-band GeoTIFF (bands in the same order as
    `periods`, sorted chronologically - see _periods_by_year) into
    individual single-band {file_prefix}_{label}.tif files.

    Band descriptions aren't relied on for this correspondence (GCS
    exports don't reliably preserve them, confirmed with the VCI COGs
    earlier in this project) - band index -> period is purely positional,
    matching the exact order the bands were requested in at export time.
    """
    import rasterio

    written = []
    with rasterio.open(yearly_path) as src:
        if src.count != len(periods):
            raise ValueError(
                f"{yearly_path}: expected {len(periods)} bands (one per period), got {src.count}"
            )
        profile = src.profile
        profile.update(count=1)
        for band_index, period in enumerate(periods, start=1):
            out_path = f"{output_dir}/{file_prefix}_{period['label']}.tif"
            with rasterio.open(out_path, "w", **profile) as dst:
                dst.write(src.read(band_index), 1)
            written.append(out_path)
    return written


def download_500m_archive_from_gcs(
    gee_account_id,
    dataset,
    start_year=2000,
    end_year=2025,
    output_dir=None,
    overwrite=False,
):
    """Download + split whatever yearly 500m files already exist in GCS,
    WITHOUT submitting any new export tasks - a standalone recovery path
    for when export_gsmap_500m_archive/export_modis_pet_500m_archive's
    own download step didn't complete (e.g. the process was interrupted
    right after the GCS export finished but before/during download).

    Re-running the full export_* functions in that situation would be
    wasteful and risky: since the split-out period files aren't on disk
    yet, they'd conclude those years still need fitting and resubmit new
    export tasks for work that's already sitting in the bucket, burning
    quota for nothing. This function only ever reads from GCS - it never
    calls Export.image.toCloudStorage.

    A full-India 500m yearly image is large enough that GEE splits it
    into multiple spatial shards per year rather than one file (same
    behaviour hit with the VCI 2023 export earlier in this project) -
    e.g. "precip_500m_20000000000000-0000000000.tif" (GEE's tile-offset
    suffix appended directly to the "precip_500m_2000" fileNamePrefix,
    no separator). So this can't just look for one exact filename per
    year - it lists everything under the GCS prefix, groups blobs by
    which year they belong to (prefix match), downloads every shard for
    a year, and mosaics them with rasterio.merge before splitting into
    periods.

    dataset: "gsmap" or "pet" - which archive to pull.

    Safe to interrupt and re-run: years already fully split locally are
    skipped unless overwrite=True.
    """
    import rasterio
    from rasterio.merge import merge as rasterio_merge

    if dataset == "gsmap":
        gcs_path, output_dir, layer_prefix, file_prefix = (
            GCS_PATH_GSMAP_500M,
            output_dir or LOCAL_DIR_GSMAP_500M,
            "precip_500m",
            "precip",
        )
    elif dataset == "pet":
        gcs_path, output_dir, layer_prefix, file_prefix = (
            GCS_PATH_MODIS_PET_500M,
            output_dir or LOCAL_DIR_MODIS_PET_500M,
            "pet_500m",
            "pet",
        )
    else:
        raise ValueError(f"dataset must be 'gsmap' or 'pet', got {dataset!r}")

    output_dir = output_dir.rstrip("/")
    os.makedirs(output_dir, exist_ok=True)
    by_year = _periods_by_year(start_year, end_year)

    bucket = gcs_config(gee_account_id)
    all_blob_names = [
        blob.name.rsplit("/", 1)[-1] for blob in bucket.list_blobs(prefix=gcs_path.rstrip("/") + "/")
    ]
    print(f"{len(all_blob_names)} blob(s) found under gs://.../{gcs_path.rstrip('/')}/")

    # Group blobs by year via prefix match: every shard for year Y is
    # named "{layer_prefix}_{Y}<shard-suffix>.tif" - the shard suffix
    # itself varies (tile offsets) and isn't parsed, just matched as
    # "starts with the year's prefix".
    blobs_by_year = {}
    for name in all_blob_names:
        for year in by_year:
            if name.startswith(f"{layer_prefix}_{year}"):
                blobs_by_year.setdefault(year, []).append(name)
                break

    downloaded, skipped, missing_in_gcs = [], [], []
    for year in sorted(by_year):
        periods = by_year[year]
        if not overwrite and all(
            os.path.exists(f"{output_dir}/{file_prefix}_{p['label']}.tif") for p in periods
        ):
            skipped.append(year)
            continue

        shard_names = blobs_by_year.get(year)
        if not shard_names:
            print(f"{year}: no blobs matching {layer_prefix}_{year}* found in GCS yet, skipping")
            missing_in_gcs.append(year)
            continue

        shard_paths = []
        for shard_name in shard_names:
            shard_path = f"{output_dir}/_shard_{shard_name}"
            bucket.blob(f"{gcs_path.rstrip('/')}/{shard_name}").download_to_filename(shard_path)
            shard_paths.append(shard_path)
        print(f"{year}: downloaded {len(shard_paths)} shard(s), mosaicking ...")

        yearly_path = f"{output_dir}/_yearly_{layer_prefix}_{year}.tif"
        if len(shard_paths) == 1:
            os.rename(shard_paths[0], yearly_path)
        else:
            srcs = [rasterio.open(p) for p in shard_paths]
            mosaic, mosaic_transform = rasterio_merge(srcs)
            profile = srcs[0].profile.copy()
            profile.update(
                height=mosaic.shape[1], width=mosaic.shape[2], transform=mosaic_transform, count=mosaic.shape[0]
            )
            for src in srcs:
                src.close()
            with rasterio.open(yearly_path, "w", **profile) as dst:
                dst.write(mosaic)
            for p in shard_paths:
                os.remove(p)

        print(f"  mosaicked -> {yearly_path}, splitting into {len(periods)} period file(s) ...")
        split_paths = _split_yearly_bands_to_periods(yearly_path, periods, output_dir, file_prefix)
        os.remove(yearly_path)
        downloaded.extend(split_paths)

    print(
        f"Done. Downloaded/split {len(downloaded)} period file(s), "
        f"skipped {len(skipped)} year(s) already on disk, "
        f"{len(missing_in_gcs)} year(s) not yet in GCS."
    )
    return {"downloaded": downloaded, "skipped_years": skipped, "missing_in_gcs": missing_in_gcs}


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
