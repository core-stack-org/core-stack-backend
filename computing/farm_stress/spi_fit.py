"""SPI-1 gamma distribution fitting (plan.md Script 01b Part A).

Per-pixel, per-calendar-month gamma fit on the 28-day rainfall
accumulations, following plan.md's method exactly: a Method-of-Moments
estimate is the *starting point* for scipy's MLE optimiser, not the final
answer. This is expensive - up to ~97,000 pixels x 12 months (~1.16M
individual fits) - so it's parallelised with multiprocessing.Pool and is
meant to run on a workstation, not a lightweight sandbox.

Zero rainfall is handled as a mixed distribution: gamma is fit only to
the non-zero values, and p_zero (the fraction of historical periods with
exactly zero rainfall at that pixel/month) is blended in when converting
to the standardised SPI-1 z-score.
"""

import multiprocessing
import os
from datetime import datetime

import numpy as np
import rasterio
from scipy import stats
from scipy.special import ndtri

from computing.farm_stress.helper import generate_28day_periods
from computing.farm_stress.config import (
    LOCAL_DIR_GSMAP_MONTHLY,
    LOCAL_DIR_SPI1_PARAMS,
    LOCAL_DIR_SPI1_TIMESERIES,
    LOCAL_DIR_SPI1_PARAMS_500M,
    LOCAL_DIR_SPI1_TIMESERIES_500M,
)


def fit_gamma_mle(data):
    """Fit gamma(alpha, beta) to the non-zero values of `data`, via MLE
    started from a Method-of-Moments guess (alpha = mean^2/var,
    beta = var/mean), per plan.md. Returns (alpha, beta, p_zero).

    alpha/beta are NaN when there are too few non-zero samples (<2) or a
    degenerate (zero-variance) sample to fit a distribution against.
    """
    data = np.asarray(data, dtype=np.float64)
    n = data.size
    nonzero = data[data > 0]
    p_zero = 1.0 - nonzero.size / n

    if nonzero.size < 2:
        return np.nan, np.nan, p_zero

    mean = nonzero.mean()
    var = nonzero.var(ddof=1)
    if var <= 0:
        return np.nan, np.nan, p_zero

    alpha_init = mean**2 / var
    beta_init = var / mean

    try:
        alpha, _, beta = stats.gamma.fit(nonzero, alpha_init, floc=0, scale=beta_init)
    except Exception:
        # MLE optimiser failed to converge - fall back to the MoM estimate
        # it was started from, rather than losing the pixel entirely.
        alpha, beta = alpha_init, beta_init

    return alpha, beta, p_zero


def gamma_to_spi(x, alpha, beta, p_zero):
    """Standardised SPI z-score for value(s) x under the fitted mixed
    zero/gamma distribution: normal_ppf(p_zero + (1 - p_zero) * gamma_cdf(x)).
    Vectorised over numpy arrays (alpha/beta/p_zero may be per-pixel arrays).
    """
    x = np.asarray(x, dtype=np.float64)
    cdf = np.where(
        np.isnan(alpha) | np.isnan(beta),
        np.nan,
        stats.gamma.cdf(np.clip(x, 0, None), a=alpha, loc=0, scale=beta),
    )
    h = p_zero + (1 - p_zero) * cdf
    h = np.clip(h, 1e-10, 1 - 1e-10)  # keep ndtri away from +/-inf at 0/1
    return ndtri(h)


def _fit_pixel_chunk(chunk):
    """chunk: (n_samples, n_pixels_in_chunk) -> (n_pixels_in_chunk, 3)."""
    n_pixels = chunk.shape[1]
    out = np.full((n_pixels, 3), np.nan, dtype=np.float64)
    for j in range(n_pixels):
        out[j] = fit_gamma_mle(chunk[:, j])
    return out


def _read_stack(paths):
    """Read single-band GeoTIFFs into a (n, rows, cols) array, plus the
    rasterio profile of the first file (all share the same grid)."""
    arrays = []
    profile = None
    for path in paths:
        with rasterio.open(path) as src:
            if profile is None:
                profile = src.profile
            arrays.append(src.read(1).astype(np.float64))
    return np.stack(arrays, axis=0), profile


def fit_spi1_archive(
    start_year=2000,
    end_year=2025,
    precip_dir=LOCAL_DIR_GSMAP_MONTHLY,
    params_dir=LOCAL_DIR_SPI1_PARAMS,
    timeseries_dir=LOCAL_DIR_SPI1_TIMESERIES,
    n_workers=None,
    chunk_size=2000,
):
    """Fit SPI-1 gamma parameters per pixel per calendar month, then apply
    them to every period to produce the full SPI-1 standardised timeseries.
    """
    periods = generate_28day_periods(start_year, end_year)
    for period in periods:
        start = datetime.strptime(period["period_start"], "%Y-%m-%d")
        end = datetime.strptime(period["period_end"], "%Y-%m-%d")
        # Calendar month of the period's midpoint, matching plan.md's
        # calendar_month_label convention - only the month number matters
        # here, so all years' "July-ish" periods are fit together.
        period["month"] = (start + (end - start) / 2).month

    precip_paths = [f"{precip_dir.rstrip('/')}/precip_{p['label']}.tif" for p in periods]
    print(f"Loading {len(precip_paths)} rainfall rasters ...")
    stack, profile = _read_stack(precip_paths)
    n_periods, rows, cols = stack.shape
    n_pixels = rows * cols
    flat = stack.reshape(n_periods, n_pixels)

    months = np.array([p["month"] for p in periods])
    n_workers = n_workers or multiprocessing.cpu_count()

    alpha_by_month = np.full((12, n_pixels), np.nan)
    beta_by_month = np.full((12, n_pixels), np.nan)
    p_zero_by_month = np.full((12, n_pixels), np.nan)

    params_dir = params_dir.rstrip("/")
    timeseries_dir = timeseries_dir.rstrip("/")
    os.makedirs(params_dir, exist_ok=True)
    os.makedirs(timeseries_dir, exist_ok=True)

    for month in range(1, 13):
        month_data = flat[months == month]
        print(
            f"Fitting month {month:02d} "
            f"({month_data.shape[0]} samples/pixel, {n_pixels} pixels, {n_workers} workers) ..."
        )

        chunks = [month_data[:, i : i + chunk_size] for i in range(0, n_pixels, chunk_size)]
        with multiprocessing.Pool(n_workers) as pool:
            results = pool.map(_fit_pixel_chunk, chunks)
        params = np.concatenate(results, axis=0)

        alpha_by_month[month - 1] = params[:, 0]
        beta_by_month[month - 1] = params[:, 1]
        p_zero_by_month[month - 1] = params[:, 2]

        month_profile = profile.copy()
        month_profile.update(count=3, dtype="float64", nodata=np.nan)
        out_path = f"{params_dir}/spi1_params_month{month:02d}.tif"
        with rasterio.open(out_path, "w", **month_profile) as dst:
            dst.write(alpha_by_month[month - 1].reshape(rows, cols), 1)
            dst.write(beta_by_month[month - 1].reshape(rows, cols), 2)
            dst.write(p_zero_by_month[month - 1].reshape(rows, cols), 3)
        print(f"  saved -> {out_path}")

    print("Applying fitted parameters to all periods to build the SPI-1 timeseries ...")
    ts_profile = profile.copy()
    ts_profile.update(count=1, dtype="float64", nodata=np.nan)
    for i, period in enumerate(periods):
        month = period["month"]
        spi1 = gamma_to_spi(
            flat[i], alpha_by_month[month - 1], beta_by_month[month - 1], p_zero_by_month[month - 1]
        ).reshape(rows, cols)

        out_path = f"{timeseries_dir}/spi1_{period['label']}.tif"
        with rasterio.open(out_path, "w", **ts_profile) as dst:
            dst.write(spi1, 1)

        if (i + 1) % 50 == 0 or i == len(periods) - 1:
            print(f"  [{i + 1}/{len(periods)}] {period['label']} done")

    print("Done.")
    return {"params_dir": params_dir, "timeseries_dir": timeseries_dir, "n_periods": n_periods}


def fit_spi1_archive_banded_tiled(
    precip_paths_by_year,
    start_year=2000,
    end_year=2025,
    params_dir=LOCAL_DIR_SPI1_PARAMS_500M,
    timeseries_dir=LOCAL_DIR_SPI1_TIMESERIES_500M,
    row_chunk=150,
    n_workers=None,
    chunk_size=2000,
):
    """500m version of fit_spi1_archive - same gamma MLE math
    (fit_gamma_mle/gamma_to_spi/_fit_pixel_chunk, all unchanged), but
    reading yearly-banded rainfall (one multi-band file per year, from
    the already-downloaded 500m SPEI-3 rainfall - SPI-1 is rainfall-only,
    so no new export was needed) and row-tiled, mirroring
    fit_spei3_archive_banded_tiled's I/O pattern.

    Unlike SPEI-3's log-logistic PWM fit (closed-form, vectorised across
    an entire strip's pixels in one pass), gamma MLE has no such
    shortcut - each pixel still needs its own scipy.optimize call. That
    call is cheap in isolation (~0.05ms, measured - only 2 parameters
    over ~26 samples, nothing like phenology's 6-parameter curve fit),
    so this stays chunked + multiprocessing.Pool, same as the 11km
    version, just applied per row-strip instead of the whole grid. The
    Pool is created once for the whole run (not per strip/month) to
    avoid repeated process-spawn overhead across ~n_strips x 12 calls.

    No rolling-window history requirement (unlike SPEI-3's 3-period
    sum) - every period gets a mask-free fit attempt, so there's no
    has_history equivalent here.

    precip_paths_by_year: {year: file_path} - explicit map, same
    reasoning as compute_water_balance_archive_banded's precip/pet maps
    (actual files came from a manual merge workflow, not a fixed
    guessable naming pattern).

    Output: one spi1_{year}.tif per year (timeseries) + 12
    spi1_params_month{MM}.tif (params) - yearly-banded, matching the
    500m SPEI-3 output layout.

    Safe to interrupt and re-run at the year/month granularity only in
    the sense that this doesn't resume a partial run - same caveat as
    fit_spei3_archive_banded_tiled (windowed writes into a file of
    unknown partial state aren't safe to reason about).
    """
    from rasterio.windows import Window

    from computing.farm_stress.spi_spei_export import _periods_by_year

    by_year = _periods_by_year(start_year, end_year)
    periods = []
    for year in sorted(by_year):
        periods.extend(by_year[year])

    for period in periods:
        start = datetime.strptime(period["period_start"], "%Y-%m-%d")
        end = datetime.strptime(period["period_end"], "%Y-%m-%d")
        period["month"] = (start + (end - start) / 2).month
    months = np.array([p["month"] for p in periods])
    n_periods = len(periods)

    missing = [y for y in by_year if not (precip_paths_by_year.get(y) and os.path.exists(precip_paths_by_year[y]))]
    if missing:
        raise FileNotFoundError(f"{len(missing)} year(s) missing precip file(s), e.g. {missing[:3]}")

    with rasterio.open(precip_paths_by_year[sorted(by_year)[0]]) as src:
        profile = src.profile
        rows, cols = src.height, src.width

    params_dir = params_dir.rstrip("/")
    timeseries_dir = timeseries_dir.rstrip("/")
    os.makedirs(params_dir, exist_ok=True)
    os.makedirs(timeseries_dir, exist_ok=True)

    # BIGTIFF=YES: same reasoning as the SPEI-3 banded fit - a full-India
    # 500m float64 yearly file can exceed classic TIFF's 4GB limit.
    param_profile = profile.copy()
    param_profile.update(count=3, dtype="float64", nodata=np.nan, BIGTIFF="YES")
    param_paths = [f"{params_dir}/spi1_params_month{m:02d}.tif" for m in range(1, 13)]
    param_dsts = [rasterio.open(p, "w", **param_profile) for p in param_paths]

    ts_dsts_by_year = {}
    for year in by_year:
        ts_profile = profile.copy()
        ts_profile.update(count=len(by_year[year]), dtype="float64", nodata=np.nan, BIGTIFF="YES")
        ts_dsts_by_year[year] = rasterio.open(f"{timeseries_dir}/spi1_{year}.tif", "w", **ts_profile)

    n_workers = n_workers or multiprocessing.cpu_count()
    pool = multiprocessing.Pool(n_workers)

    try:
        n_strips = (rows + row_chunk - 1) // row_chunk
        print(
            f"Fitting {rows}x{cols} grid in {n_strips} row-strip(s) of up to "
            f"{row_chunk} rows each, {n_workers} workers ..."
        )

        for s in range(n_strips):
            row_off = s * row_chunk
            strip_h = min(row_chunk, rows - row_off)
            window = Window(0, row_off, cols, strip_h)
            n_strip_pixels = strip_h * cols

            strip = np.empty((n_periods, strip_h, cols), dtype=np.float64)
            idx = 0
            for year in sorted(by_year):
                n_bands_year = len(by_year[year])
                with rasterio.open(precip_paths_by_year[year]) as src:
                    strip[idx : idx + n_bands_year] = src.read(window=window)
                idx += n_bands_year

            flat = strip.reshape(n_periods, n_strip_pixels)
            del strip

            alpha_by_month = np.full((12, n_strip_pixels), np.nan)
            beta_by_month = np.full((12, n_strip_pixels), np.nan)
            p_zero_by_month = np.full((12, n_strip_pixels), np.nan)

            for m in range(1, 13):
                month_data = flat[months == m]
                chunks = [month_data[:, i : i + chunk_size] for i in range(0, n_strip_pixels, chunk_size)]
                results = pool.map(_fit_pixel_chunk, chunks)
                params = np.concatenate(results, axis=0)

                alpha_by_month[m - 1] = params[:, 0]
                beta_by_month[m - 1] = params[:, 1]
                p_zero_by_month[m - 1] = params[:, 2]

                param_dsts[m - 1].write(alpha_by_month[m - 1].reshape(strip_h, cols), 1, window=window)
                param_dsts[m - 1].write(beta_by_month[m - 1].reshape(strip_h, cols), 2, window=window)
                param_dsts[m - 1].write(p_zero_by_month[m - 1].reshape(strip_h, cols), 3, window=window)

            idx = 0
            for year in sorted(by_year):
                n_bands_year = len(by_year[year])
                for local_band in range(n_bands_year):
                    global_idx = idx + local_band
                    m = periods[global_idx]["month"]
                    spi1 = gamma_to_spi(
                        flat[global_idx], alpha_by_month[m - 1], beta_by_month[m - 1], p_zero_by_month[m - 1]
                    ).reshape(strip_h, cols)
                    ts_dsts_by_year[year].write(spi1, local_band + 1, window=window)
                idx += n_bands_year
            del flat

            print(f"  [{s + 1}/{n_strips}] rows {row_off}-{row_off + strip_h} done")
    finally:
        pool.close()
        pool.join()
        for dst in param_dsts + list(ts_dsts_by_year.values()):
            dst.close()

    print("Done.")
    return {"params_dir": params_dir, "timeseries_dir": timeseries_dir, "n_periods": n_periods}
