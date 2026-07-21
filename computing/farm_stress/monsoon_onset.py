"""Monsoon onset detection (plan.md Script 01c).

Per-pixel, per-year scan of the daily GSMaP rainfall archive (Script 01a
Part D) for the first date on/after May 15 where the 5-day forward
cumulative rainfall >= 20mm, with no subsequent dry spell (consecutive
days < 1mm) exceeding 10 days within the following 21 days. If no such
date is found by August 31, that pixel/year is left NaN.

Vectorised across pixels rather than a per-pixel Python loop: for each
candidate day, the 20mm/dry-spell condition is checked across the whole
~97,000-pixel grid at once via numpy, using a "still searching" mask so
pixels that already found an earlier onset date aren't overwritten by a
later match (mirroring the "break" in plan.md's per-pixel description).
"""

import os
from datetime import datetime, timedelta

import numpy as np
import rasterio

from computing.farm_stress.config import LOCAL_DIR_GSMAP_DAILY, LOCAL_DIR_MONSOON_ONSET


def _max_consecutive_dry_run(is_dry):
    """is_dry: (n_days, n_pixels) bool array -> (n_pixels,) longest run of
    True (dry days) along axis 0, per pixel column."""
    n_days, n_pixels = is_dry.shape
    run = np.zeros(n_pixels, dtype=np.int32)
    max_run = np.zeros(n_pixels, dtype=np.int32)
    for day in range(n_days):
        run = np.where(is_dry[day], run + 1, 0)
        max_run = np.maximum(max_run, run)
    return max_run


def scan_onset(rain_flat, season_start, may15_idx, aug31_idx):
    """Core onset-detection scan, decoupled from file I/O so it can be
    tested directly against synthetic arrays.

    rain_flat: (n_days, n_pixels) daily rainfall, day 0 = season_start.
    Returns onset_doy: (n_pixels,), NaN where no onset found by aug31_idx.
    """
    n_pixels = rain_flat.shape[1]
    onset_doy = np.full(n_pixels, np.nan)
    still_searching = np.ones(n_pixels, dtype=bool)

    for d_idx in range(may15_idx, aug31_idx + 1):
        if not still_searching.any():
            break

        cum5 = rain_flat[d_idx : d_idx + 5].sum(axis=0)
        is_dry = rain_flat[d_idx : d_idx + 21] < 1.0
        max_dry_run = _max_consecutive_dry_run(is_dry)

        candidate_ok = (cum5 >= 20.0) & (max_dry_run <= 10) & still_searching
        doy = (season_start + timedelta(days=d_idx)).timetuple().tm_yday
        onset_doy[candidate_ok] = doy
        still_searching[candidate_ok] = False

    return onset_doy


def detect_onset_for_year(year, daily_dir=LOCAL_DIR_GSMAP_DAILY):
    """Detect monsoon onset day-of-year per pixel for one year.

    Returns (onset_doy, profile): onset_doy is (rows, cols), NaN where no
    onset was found by August 31; profile is the rasterio profile of the
    input daily rasters (all share the same grid).
    """
    season_start = datetime(year, 5, 1)
    season_end = datetime(year, 9, 30)

    dates = []
    d = season_start
    while d <= season_end:
        dates.append(d)
        d += timedelta(days=1)

    arrays = []
    profile = None
    for d in dates:
        path = f"{daily_dir.rstrip('/')}/daily_{d.strftime('%Y%m%d')}.tif"
        with rasterio.open(path) as src:
            if profile is None:
                profile = src.profile
            arrays.append(src.read(1).astype(np.float64))
    rain = np.stack(arrays, axis=0)
    n_days, rows, cols = rain.shape
    rain_flat = rain.reshape(n_days, rows * cols)

    may15_idx = (datetime(year, 5, 15) - season_start).days
    aug31_idx = (datetime(year, 8, 31) - season_start).days

    onset_doy = scan_onset(rain_flat, season_start, may15_idx, aug31_idx)
    return onset_doy.reshape(rows, cols), profile


def detect_monsoon_onset_archive(
    start_year=2000,
    end_year=2025,
    daily_dir=LOCAL_DIR_GSMAP_DAILY,
    output_dir=LOCAL_DIR_MONSOON_ONSET,
    overwrite=False,
):
    """Detect monsoon onset per pixel for every year, save one raster per
    year plus a multi-year climatological median (per-pixel median onset
    DOY across all years with a detected onset).
    """
    output_dir = output_dir.rstrip("/")
    os.makedirs(output_dir, exist_ok=True)

    onset_by_year = []
    profile = None
    for year in range(start_year, end_year + 1):
        out_path = f"{output_dir}/onset_doy_{year}.tif"
        if os.path.exists(out_path) and not overwrite:
            print(f"{year}: already exists, skipping")
            with rasterio.open(out_path) as src:
                onset_by_year.append(src.read(1))
                if profile is None:
                    profile = src.profile
            continue

        print(f"Detecting onset for {year} ...")
        onset_doy, this_profile = detect_onset_for_year(year, daily_dir)
        if profile is None:
            profile = this_profile

        out_profile = profile.copy()
        out_profile.update(count=1, dtype="float64", nodata=np.nan)
        with rasterio.open(out_path, "w", **out_profile) as dst:
            dst.write(onset_doy, 1)
        n_detected = int(np.sum(~np.isnan(onset_doy)))
        print(f"  saved -> {out_path} (onset detected for {n_detected}/{onset_doy.size} pixels)")
        onset_by_year.append(onset_doy)

    print("Computing multi-year climatological median onset DOY ...")
    stack = np.stack(onset_by_year, axis=0)
    clim_median = np.nanmedian(stack, axis=0)

    clim_profile = profile.copy()
    clim_profile.update(count=1, dtype="float64", nodata=np.nan)
    clim_path = f"{output_dir}/onset_doy_climatological.tif"
    with rasterio.open(clim_path, "w", **clim_profile) as dst:
        dst.write(clim_median, 1)
    print(f"Saved climatological median -> {clim_path}")

    return {"output_dir": output_dir, "n_years": len(onset_by_year)}
