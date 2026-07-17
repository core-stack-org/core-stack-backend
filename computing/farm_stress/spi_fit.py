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
