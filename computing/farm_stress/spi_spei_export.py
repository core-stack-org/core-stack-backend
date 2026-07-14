"""GEE-side exports for the historical SPI-1/SPEI-3 archive (plan.md Step 1 /
Script 01a). This module only builds and submits Earth Engine export tasks -
the local gamma/log-logistic fitting and monsoon onset detection run
separately (spi_spei_fit.py, monsoon_onset.py, added in a later pass) once
these rasters are downloaded.

Rainfall is exported first, one dataset at a time, per the phased rollout:
this file currently implements the GSMaP rainfall accumulation export only.
PET (prorated MOD16A2) and water balance follow once rainfall is validated.
"""

import ee

from utilities.gee_utils import ee_initialize, sync_raster_to_gcs
from computing.farm_stress.config import (
    GSMAP_COLLECTION,
    GSMAP_BAND,
    SPI_SCALE_M,
    INDIA_BBOX_COORDS,
    GCS_PATH_GSMAP_MONTHLY,
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
    gcs_path=GCS_PATH_GSMAP_MONTHLY,
):
    """Export one 28-day GSMaP rainfall accumulation as a single-band GeoTIFF
    to GCS. This is the single-file smoke test for the historical rainfall
    export: exactly one period, to validate the merge/accumulation/export
    path before looping over all ~325 historical periods.
    """
    ee_initialize(gee_account_id)
    region = get_india_bbox()

    period_rainfall = (
        compute_period_rainfall(period_start, period_end, region)
        .clip(region)
        .set(
            {
                "period_label": period_label,
                "period_start": period_start,
                "period_end": period_end,
            }
        )
    )

    layer_name = f"precip_{period_label}"
    task_id = sync_raster_to_gcs(
        period_rainfall, SPI_SCALE_M, layer_name, gcs_path=gcs_path
    )
    print(f"Started export task {task_id} -> gs://{gcs_path}{layer_name}.tif")
    return task_id
