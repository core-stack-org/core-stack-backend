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
from rasterio.windows import Window
from scipy.special import gamma as gamma_func
from scipy.special import ndtri

from computing.farm_stress.helper import generate_28day_periods
from computing.farm_stress.config import (
    LOCAL_DIR_WATER_BALANCE_MONTHLY,
    LOCAL_DIR_SPEI3_PARAMS,
    LOCAL_DIR_SPEI3_TIMESERIES,
    LOCAL_DIR_WATER_BALANCE_500M,
    LOCAL_DIR_SPEI3_PARAMS_500M,
    LOCAL_DIR_SPEI3_TIMESERIES_500M,
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


def fit_spei3_archive_banded_tiled(
    start_year=2000,
    end_year=2025,
    wb_dir=LOCAL_DIR_WATER_BALANCE_500M,
    params_dir=LOCAL_DIR_SPEI3_PARAMS_500M,
    timeseries_dir=LOCAL_DIR_SPEI3_TIMESERIES_500M,
    row_chunk=150,
):
    """Row-tiled SPEI-3 fit reading yearly multi-band water balance
    (wb_{year}.tif, from compute_water_balance_archive_banded) instead
    of one file per period, and writing yearly multi-band SPEI-3 output
    (spei3_{year}.tif) instead of one file per period - matches the
    500m pipeline's space-saving layout end to end (26 output files
    instead of 340).

    Same row-strip memory strategy and same underlying math as
    fit_spei3_archive_tiled (each pixel's fit depends only on its own
    time series, so this is purely a different file layout, not a
    different algorithm) - only fewer file opens per strip now (~26
    yearly files instead of 340 period files).

    Every output file (12 monthly params + one per year for the
    timeseries) is opened once, up front, and written to window-by-
    window as each strip is processed. Same ulimit caveat as
    fit_spei3_archive_tiled if this errors with "too many open files",
    though there are far fewer handles here (~26 vs ~340) so it's less
    likely to matter.

    Doesn't support resuming a partial run, same reason as
    fit_spei3_archive_tiled - re-run from scratch if interrupted.
    """
    from computing.farm_stress.spi_spei_export import _periods_by_year

    by_year = _periods_by_year(start_year, end_year)
    periods = []
    for year in sorted(by_year):
        periods.extend(by_year[year])  # already chronologically sorted within each year

    for period in periods:
        start = datetime.strptime(period["period_start"], "%Y-%m-%d")
        end = datetime.strptime(period["period_end"], "%Y-%m-%d")
        period["month"] = (start + (end - start) / 2).month
    months = np.array([p["month"] for p in periods])
    n_periods = len(periods)
    has_history = np.arange(n_periods) >= 2

    wb_dir = wb_dir.rstrip("/")
    wb_paths_by_year = {y: f"{wb_dir}/wb_{y}.tif" for y in by_year}
    missing = [y for y, p in wb_paths_by_year.items() if not os.path.exists(p)]
    if missing:
        raise FileNotFoundError(
            f"{len(missing)} year(s) missing water-balance file(s) (e.g. {missing[:3]}) - "
            "run compute_water_balance_archive_banded first"
        )

    with rasterio.open(wb_paths_by_year[sorted(by_year)[0]]) as src:
        profile = src.profile
        rows, cols = src.height, src.width

    params_dir = params_dir.rstrip("/")
    timeseries_dir = timeseries_dir.rstrip("/")
    os.makedirs(params_dir, exist_ok=True)
    os.makedirs(timeseries_dir, exist_ok=True)

    param_profile = profile.copy()
    param_profile.update(count=3, dtype="float64", nodata=np.nan)
    param_paths = [f"{params_dir}/spei3_params_month{m:02d}.tif" for m in range(1, 13)]
    param_dsts = [rasterio.open(p, "w", **param_profile) for p in param_paths]

    ts_dsts_by_year = {}
    for year in by_year:
        ts_profile = profile.copy()
        ts_profile.update(count=len(by_year[year]), dtype="float64", nodata=np.nan)
        ts_dsts_by_year[year] = rasterio.open(f"{timeseries_dir}/spei3_{year}.tif", "w", **ts_profile)

    try:
        n_strips = (rows + row_chunk - 1) // row_chunk
        print(f"Fitting {rows}x{cols} grid in {n_strips} row-strip(s) of up to {row_chunk} rows each ...")

        for s in range(n_strips):
            row_off = s * row_chunk
            strip_h = min(row_chunk, rows - row_off)
            window = Window(0, row_off, cols, strip_h)

            # Read this row-strip from every YEARLY file (not every
            # period file) and reassemble into the same continuous
            # (n_periods, strip_h, cols) order as `periods`.
            strip = np.empty((n_periods, strip_h, cols), dtype=np.float64)
            idx = 0
            for year in sorted(by_year):
                n_bands_year = len(by_year[year])
                with rasterio.open(wb_paths_by_year[year]) as src:
                    strip[idx : idx + n_bands_year] = src.read(window=window)
                idx += n_bands_year

            flat = strip.reshape(n_periods, strip_h * cols)
            del strip
            wb3 = np.full_like(flat, np.nan)
            wb3[2:] = flat[2:] + flat[1:-1] + flat[:-2]
            del flat

            gamma_by_month = np.full((12, strip_h * cols), np.nan)
            alpha_by_month = np.full((12, strip_h * cols), np.nan)
            beta_by_month = np.full((12, strip_h * cols), np.nan)

            for m in range(1, 13):
                month_mask = (months == m) & has_history
                gamma_loc, alpha, beta = fit_loglogistic_pwm(wb3[month_mask])
                gamma_by_month[m - 1] = gamma_loc
                alpha_by_month[m - 1] = alpha
                beta_by_month[m - 1] = beta

                param_dsts[m - 1].write(gamma_loc.reshape(strip_h, cols), 1, window=window)
                param_dsts[m - 1].write(alpha.reshape(strip_h, cols), 2, window=window)
                param_dsts[m - 1].write(beta.reshape(strip_h, cols), 3, window=window)

            idx = 0
            for year in sorted(by_year):
                n_bands_year = len(by_year[year])
                for local_band in range(n_bands_year):
                    global_idx = idx + local_band
                    m = periods[global_idx]["month"]
                    spei3 = loglogistic_to_spei(
                        wb3[global_idx], gamma_by_month[m - 1], alpha_by_month[m - 1], beta_by_month[m - 1]
                    ).reshape(strip_h, cols)
                    ts_dsts_by_year[year].write(spei3, local_band + 1, window=window)
                idx += n_bands_year

            print(f"  [{s + 1}/{n_strips}] rows {row_off}-{row_off + strip_h} done")
    finally:
        for dst in param_dsts + list(ts_dsts_by_year.values()):
            dst.close()

    print("Done.")
    return {"params_dir": params_dir, "timeseries_dir": timeseries_dir, "n_periods": n_periods}


def fit_spei3_archive_tiled(
    start_year=2000,
    end_year=2025,
    wb_dir=LOCAL_DIR_WATER_BALANCE_500M,
    params_dir=LOCAL_DIR_SPEI3_PARAMS_500M,
    timeseries_dir=LOCAL_DIR_SPEI3_TIMESERIES_500M,
    row_chunk=150,
):
    """Row-tiled version of fit_spei3_archive, for grids too large to
    hold in memory all at once - the 500m full-India grid is ~500x more
    pixels than the 11km one fit_spei3_archive was written for
    (~46.7M vs ~97K), and loading all 340 periods x that many pixels x
    8 bytes would be >100GB, infeasible on this machine.

    Produces IDENTICAL results to fit_spei3_archive (verified against it
    on synthetic data before this was used on real 500m data) - each
    pixel's fit depends only on its own time series, never on any other
    pixel, so splitting the grid into horizontal row-strips and fitting
    each strip independently doesn't change the math, only how much of
    the archive is held in memory at once. row_chunk=150 keeps one
    strip's full-history read (150 rows x full width x 340 periods x
    8 bytes) to a few GB.

    Every output file (12 monthly params + up to 340 timeseries rasters)
    is opened once, up front, and written to window-by-window as each
    strip is processed, rather than reopened per strip - keeps ~350+
    file handles open for the duration of the run. If this hits an OS
    "too many open files" error, raise the process's file descriptor
    ulimit rather than reducing row_chunk (row_chunk controls memory per
    strip, not file-handle count, which is fixed by n_periods regardless).

    Unlike fit_spei3_archive, this doesn't support resuming a partial
    run (every output file is truncated and rewritten from row 0) -
    windowed writes into a partially-existing file of unknown state
    would be unsafe to reason about; re-run the whole thing if
    interrupted.
    """
    periods = generate_28day_periods(start_year, end_year)
    for period in periods:
        start = datetime.strptime(period["period_start"], "%Y-%m-%d")
        end = datetime.strptime(period["period_end"], "%Y-%m-%d")
        period["month"] = (start + (end - start) / 2).month
    months = np.array([p["month"] for p in periods])
    n_periods = len(periods)
    has_history = np.arange(n_periods) >= 2

    wb_dir = wb_dir.rstrip("/")
    wb_paths = [f"{wb_dir}/wb_{p['label']}.tif" for p in periods]
    missing = [p for p in wb_paths if not os.path.exists(p)]
    if missing:
        raise FileNotFoundError(
            f"{len(missing)} water-balance file(s) missing (e.g. {missing[:3]}) - "
            "run the 500m water balance step first"
        )

    with rasterio.open(wb_paths[0]) as src:
        profile = src.profile
        rows, cols = src.height, src.width

    params_dir = params_dir.rstrip("/")
    timeseries_dir = timeseries_dir.rstrip("/")
    os.makedirs(params_dir, exist_ok=True)
    os.makedirs(timeseries_dir, exist_ok=True)

    param_profile = profile.copy()
    param_profile.update(count=3, dtype="float64", nodata=np.nan)
    ts_profile = profile.copy()
    ts_profile.update(count=1, dtype="float64", nodata=np.nan)

    param_paths = [f"{params_dir}/spei3_params_month{m:02d}.tif" for m in range(1, 13)]
    ts_paths = [f"{timeseries_dir}/spei3_{p['label']}.tif" for p in periods]

    param_dsts = [rasterio.open(p, "w", **param_profile) for p in param_paths]
    ts_dsts = [rasterio.open(p, "w", **ts_profile) for p in ts_paths]

    try:
        n_strips = (rows + row_chunk - 1) // row_chunk
        print(f"Fitting {rows}x{cols} grid in {n_strips} row-strip(s) of up to {row_chunk} rows each ...")

        for s in range(n_strips):
            row_off = s * row_chunk
            strip_h = min(row_chunk, rows - row_off)
            window = Window(0, row_off, cols, strip_h)

            strip = np.empty((n_periods, strip_h, cols), dtype=np.float64)
            for i, path in enumerate(wb_paths):
                with rasterio.open(path) as src:
                    strip[i] = src.read(1, window=window)

            flat = strip.reshape(n_periods, strip_h * cols)
            del strip
            wb3 = np.full_like(flat, np.nan)
            wb3[2:] = flat[2:] + flat[1:-1] + flat[:-2]
            del flat

            gamma_by_month = np.full((12, strip_h * cols), np.nan)
            alpha_by_month = np.full((12, strip_h * cols), np.nan)
            beta_by_month = np.full((12, strip_h * cols), np.nan)

            for m in range(1, 13):
                month_mask = (months == m) & has_history
                gamma_loc, alpha, beta = fit_loglogistic_pwm(wb3[month_mask])
                gamma_by_month[m - 1] = gamma_loc
                alpha_by_month[m - 1] = alpha
                beta_by_month[m - 1] = beta

                param_dsts[m - 1].write(gamma_loc.reshape(strip_h, cols), 1, window=window)
                param_dsts[m - 1].write(alpha.reshape(strip_h, cols), 2, window=window)
                param_dsts[m - 1].write(beta.reshape(strip_h, cols), 3, window=window)

            for i, period in enumerate(periods):
                m = period["month"]
                spei3 = loglogistic_to_spei(
                    wb3[i], gamma_by_month[m - 1], alpha_by_month[m - 1], beta_by_month[m - 1]
                ).reshape(strip_h, cols)
                ts_dsts[i].write(spei3, 1, window=window)

            print(f"  [{s + 1}/{n_strips}] rows {row_off}-{row_off + strip_h} done")
    finally:
        for dst in param_dsts + ts_dsts:
            dst.close()

    print("Done.")
    return {"params_dir": params_dir, "timeseries_dir": timeseries_dir, "n_periods": n_periods}
