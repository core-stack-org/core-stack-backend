"""SPEI-3 log-logistic distribution fitting (plan.md Script 01b Part B).

Per-pixel, per-calendar-month log-logistic fit on the rolling 3-period
water balance sum, via probability-weighted moments (PWM) - the actual
Vicente-Serrano et al. (2010) method, not the circular/unusable formulas
literally transcribed in plan.md (see conversation: those formulas reduce
to alpha = alpha/beta, which only holds for beta=1 - a drafting error,
not a real fitting procedure). This is the standard method used by the
reference SPEI R package.

Unlike SPI-1's gamma MLE (which needs a per-pixel scipy.optimize call),
PWMs are a closed-form linear combination of sorted order statistics, so
the whole pixel grid fits in one vectorised numpy pass per month - no
multiprocessing needed here.
"""

import os
from datetime import datetime

import numpy as np
import rasterio
from scipy.special import gamma as gamma_func
from scipy.special import ndtri

from computing.farm_stress.helper import generate_28day_periods
from computing.farm_stress.config import (
    LOCAL_DIR_WATER_BALANCE_MONTHLY,
    LOCAL_DIR_SPEI3_PARAMS,
    LOCAL_DIR_SPEI3_TIMESERIES,
)


def fit_loglogistic_pwm(data_2d):
    """Fit log-logistic(gamma_loc, alpha_scale, beta_shape) per pixel via
    probability-weighted moments. data_2d: (n_samples, n_pixels).
    Returns (gamma_loc, alpha_scale, beta_shape), each (n_pixels,), NaN
    where the fit is undefined (too few samples, degenerate beta).
    """
    n = data_2d.shape[0]
    n_pixels = data_2d.shape[1]

    if n < 3:
        nan = np.full(n_pixels, np.nan)
        return nan, nan, nan

    sorted_data = np.sort(data_2d, axis=0)
    i = np.arange(1, n + 1, dtype=np.float64).reshape(-1, 1)

    w0 = sorted_data.mean(axis=0)
    w1 = ((n - i) / (n - 1) * sorted_data).mean(axis=0)
    w2 = ((n - i) * (n - i - 1) / ((n - 1) * (n - 2)) * sorted_data).mean(axis=0)

    with np.errstate(invalid="ignore", divide="ignore"):
        beta = (2 * w1 - w0) / (6 * w1 - w0 - 6 * w2)
        g1 = gamma_func(1 + 1 / beta)
        g2 = gamma_func(1 - 1 / beta)
        alpha = (w0 - 2 * w1) * beta / (g1 * g2)
        gamma_loc = w0 - alpha * g1 * g2

    # Guard against degenerate fits (beta <= 0, non-finite results from a
    # pathological/near-constant sample) rather than propagating garbage.
    # No upper bound on beta: even when the PWM denominator lands near zero
    # and beta blows up, the resulting SPEI value near the observed data
    # range stays well-behaved (different (gamma, alpha, beta) triples can
    # describe nearly the same distribution over the data actually seen),
    # so an inflated beta is kept rather than discarded as NaN.
    invalid = (
        ~np.isfinite(beta)
        | ~np.isfinite(alpha)
        | ~np.isfinite(gamma_loc)
        | (beta <= 0)
        | (alpha <= 0)
    )
    beta = np.where(invalid, np.nan, beta)
    alpha = np.where(invalid, np.nan, alpha)
    gamma_loc = np.where(invalid, np.nan, gamma_loc)

    return gamma_loc, alpha, beta


def loglogistic_to_spei(x, gamma_loc, alpha, beta):
    """Standardised SPEI z-score for value(s) x under the fitted
    log-logistic distribution: normal_ppf(F(x)), where
    F(x) = 1 / (1 + ((x - gamma_loc) / alpha) ** -beta).
    Vectorised over numpy arrays.
    """
    x = np.asarray(x, dtype=np.float64)
    # x can occasionally fall at/below gamma_loc (a moment-based location,
    # not a strict lower bound) - clip the base away from <=0 so the
    # negative exponent doesn't produce NaN/inf, matching the reference
    # SPEI package's handling of this edge case.
    base = np.clip((x - gamma_loc) / alpha, 1e-10, None)
    cdf = np.where(
        np.isnan(alpha) | np.isnan(beta) | np.isnan(gamma_loc),
        np.nan,
        1.0 / (1.0 + base ** (-beta)),
    )
    cdf = np.clip(cdf, 1e-10, 1 - 1e-10)
    return ndtri(cdf)


def _read_stack(paths):
    arrays = []
    profile = None
    for path in paths:
        with rasterio.open(path) as src:
            if profile is None:
                profile = src.profile
            arrays.append(src.read(1).astype(np.float64))
    return np.stack(arrays, axis=0), profile


def fit_spei3_archive(
    start_year=2000,
    end_year=2025,
    wb_dir=LOCAL_DIR_WATER_BALANCE_MONTHLY,
    params_dir=LOCAL_DIR_SPEI3_PARAMS,
    timeseries_dir=LOCAL_DIR_SPEI3_TIMESERIES,
):
    """Fit SPEI-3 log-logistic parameters per pixel per calendar month from
    the rolling 3-period water balance sum, then apply them to every period
    to produce the full SPEI-3 standardised timeseries.
    """
    periods = generate_28day_periods(start_year, end_year)
    for period in periods:
        start = datetime.strptime(period["period_start"], "%Y-%m-%d")
        end = datetime.strptime(period["period_end"], "%Y-%m-%d")
        period["month"] = (start + (end - start) / 2).month

    wb_paths = [f"{wb_dir.rstrip('/')}/wb_{p['label']}.tif" for p in periods]
    print(f"Loading {len(wb_paths)} water-balance rasters ...")
    stack, profile = _read_stack(wb_paths)
    n_periods, rows, cols = stack.shape
    n_pixels = rows * cols
    flat = stack.reshape(n_periods, n_pixels)

    # Rolling 3-period sum: wb3[t] = wb[t] + wb[t-1] + wb[t-2]. Periods are
    # continuous across the whole 2000-2025 span (no per-calendar-year
    # reset - see generate_28day_periods), so this needs no special-casing
    # at year boundaries. The first 2 periods overall have no prior
    # history and are left as NaN.
    wb3 = np.full_like(flat, np.nan)
    wb3[2:] = flat[2:] + flat[1:-1] + flat[:-2]

    months = np.array([p["month"] for p in periods])

    gamma_by_month = np.full((12, n_pixels), np.nan)
    alpha_by_month = np.full((12, n_pixels), np.nan)
    beta_by_month = np.full((12, n_pixels), np.nan)

    params_dir = params_dir.rstrip("/")
    timeseries_dir = timeseries_dir.rstrip("/")
    os.makedirs(params_dir, exist_ok=True)
    os.makedirs(timeseries_dir, exist_ok=True)

    # wb3 is only undefined for the first 2 periods overall (no 3-period
    # history yet) - that's a fixed position in the sequence, not something
    # to detect via "is every pixel in the whole grid finite this period".
    # Ocean/permanently-masked pixels are NaN in every period regardless;
    # fit_loglogistic_pwm handles that fine per-pixel (a column of NaN in,
    # NaN out), without needing to discard other pixels' valid data.
    has_history = np.arange(n_periods) >= 2

    for month in range(1, 13):
        month_mask = (months == month) & has_history
        month_data = wb3[month_mask]
        print(f"Fitting month {month:02d} ({month_data.shape[0]} samples/pixel, {n_pixels} pixels) ...")

        gamma_loc, alpha, beta = fit_loglogistic_pwm(month_data)
        gamma_by_month[month - 1] = gamma_loc
        alpha_by_month[month - 1] = alpha
        beta_by_month[month - 1] = beta

        month_profile = profile.copy()
        month_profile.update(count=3, dtype="float64", nodata=np.nan)
        out_path = f"{params_dir}/spei3_params_month{month:02d}.tif"
        with rasterio.open(out_path, "w", **month_profile) as dst:
            dst.write(gamma_loc.reshape(rows, cols), 1)
            dst.write(alpha.reshape(rows, cols), 2)
            dst.write(beta.reshape(rows, cols), 3)
        print(f"  saved -> {out_path}")

    print("Applying fitted parameters to all periods to build the SPEI-3 timeseries ...")
    ts_profile = profile.copy()
    ts_profile.update(count=1, dtype="float64", nodata=np.nan)
    for i, period in enumerate(periods):
        month = period["month"]
        spei3 = loglogistic_to_spei(
            wb3[i], gamma_by_month[month - 1], alpha_by_month[month - 1], beta_by_month[month - 1]
        ).reshape(rows, cols)

        out_path = f"{timeseries_dir}/spei3_{period['label']}.tif"
        with rasterio.open(out_path, "w", **ts_profile) as dst:
            dst.write(spei3, 1)

        if (i + 1) % 50 == 0 or i == len(periods) - 1:
            print(f"  [{i + 1}/{len(periods)}] {period['label']} done")

    print("Done.")
    return {"params_dir": params_dir, "timeseries_dir": timeseries_dir, "n_periods": n_periods}
