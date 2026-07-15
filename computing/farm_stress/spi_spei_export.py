"""Historical SPI-1/SPEI-3 input rasters (plan.md Step 1 / Script 01a).

Builds the GSMaP rainfall accumulation in Earth Engine, then pulls the
result straight to local disk via getDownloadURL (see local_download.py)
rather than an async GCS export - these rasters are small enough (single
band, ~11km resolution) that a direct download is simpler than submitting
and polling a batch export task.

The local gamma/log-logistic fitting and monsoon onset detection run
separately (spi_spei_fit.py, monsoon_onset.py, added in a later pass) once
these rasters are on disk.

Rainfall is implemented first, one dataset at a time, per the phased
rollout: PET (prorated MOD16A2GF) and water balance follow once rainfall
is validated.
"""

import os
import time

import ee

from utilities.gee_utils import ee_initialize
from computing.farm_stress.local_download import download_image
from computing.farm_stress.helper import generate_28day_periods
from computing.farm_stress.config import (
    GSMAP_COLLECTION,
    GSMAP_BAND,
    SPI_SCALE_M,
    INDIA_BBOX_COORDS,
    LOCAL_DIR_GSMAP_MONTHLY,
)


def get_india_bbox():
    return ee.Geometry.Rectangle(INDIA_BBOX_COORDS)


def get_gsmap_hourly(region):
    """Hourly GSMaP precip-rate collection (mm/hr), filtered to the region.

    v8/operational is a single continuous collection (1998-present) - no
    reanalysis/operational merge needed, unlike v6.
    """
    return ee.ImageCollection(GSMAP_COLLECTION).select(GSMAP_BAND).filterBounds(region)


def compute_period_rainfall(period_start, period_end, region):
    """Total accumulated rainfall (mm) over [period_start, period_end] inclusive.

    GSMaP's hourlyPrecipRate is an instantaneous rate (mm/hr) sampled once an
    hour, not a pre-accumulated per-hour total. The physically correct way to
    turn that into an accumulated depth over a window is:
        accumulated_mm = mean(hourly rate over the window) x (window_hours)
    rather than summing raw hourly rate values (which double-counts, since
    each value already represents an hourly rate rather than an hourly
    depth). This form also generalises cleanly to any window length instead
    of requiring a separate "daily" intermediate step.
    """
    hourly = get_gsmap_hourly(region)
    window = hourly.filterDate(period_start, ee.Date(period_end).advance(1, "day"))

    window_hours = ee.Date(period_end).advance(1, "day").difference(
        ee.Date(period_start), "hour"
    )
    mean_rate = window.mean()
    return mean_rate.multiply(window_hours).rename("precip_mm")


def export_gsmap_period(
    period_start,
    period_end,
    period_label,
    gee_account_id=22,
    output_dir=LOCAL_DIR_GSMAP_MONTHLY,
):
    """Download one 28-day GSMaP rainfall accumulation as a single-band
    GeoTIFF straight to local disk. period_label identifies the file
    (encoded in the filename, e.g. precip_2023_07.tif) - direct downloads
    don't carry ee.Image .set() properties through as GeoTIFF tags the way
    an asset/GCS export would, so the filename is the source of truth here.
    """
    ee_initialize(gee_account_id)
    region = get_india_bbox()

    period_rainfall = compute_period_rainfall(period_start, period_end, region).clip(
        region
    )

    output_path = f"{output_dir.rstrip('/')}/precip_{period_label}.tif"
    download_image(period_rainfall, region, output_path, SPI_SCALE_M)
    print(f"Downloaded -> {output_path}")
    return output_path


def export_gsmap_historical_archive(
    start_year=2000,
    end_year=2025,
    gee_account_id=22,
    output_dir=LOCAL_DIR_GSMAP_MONTHLY,
    overwrite=False,
    sleep_seconds=0.2,
):
    """Download the full historical GSMaP rainfall archive: one 28-day-period
    GeoTIFF per real EPOCH_ANCHOR period from start_year through end_year.

    Safe to interrupt and re-run: files already on disk are skipped unless
    overwrite=True, so a partial run resumes where it left off instead of
    re-downloading everything.
    """
    ee_initialize(gee_account_id)
    region = get_india_bbox()
    periods = generate_28day_periods(start_year, end_year)
    output_dir = output_dir.rstrip("/")

    print(f"{len(periods)} periods to process ({start_year}-{end_year})")
    downloaded, skipped = [], []
    for i, period in enumerate(periods, start=1):
        label = period["label"]
        output_path = f"{output_dir}/precip_{label}.tif"

        if os.path.exists(output_path) and not overwrite:
            skipped.append(output_path)
            continue

        print(
            f"[{i}/{len(periods)}] downloading {label} "
            f"({period['period_start']} to {period['period_end']})"
        )
        image = compute_period_rainfall(
            period["period_start"], period["period_end"], region
        ).clip(region)
        download_image(image, region, output_path, SPI_SCALE_M)
        downloaded.append(output_path)

        if sleep_seconds:
            time.sleep(sleep_seconds)

    print(
        f"Done. Downloaded {len(downloaded)} new file(s), "
        f"skipped {len(skipped)} already on disk."
    )
    return {"downloaded": downloaded, "skipped": skipped}
