"""Phenology curve fitting (plan.md Step 3 / Script 03b).

Per-pixel, per-year double-logistic curve fit (Elmore et al. 2012) to the
23-period VCI timeseries, extracting green-up, peak, and green-down DOY
analytically from the fitted parameters. Fit against VCI (not raw NDVI) -
plan.md's own Script 03b input list is the VCI GeoTIFFs, which are
already cloud/outlier-filtered (percentile-rescaled), a better fitting
target than raw NDVI.

Model:
    VCI(t) = c1 + c2 * [1/(1+exp(-c3*(t-c4))) - 1/(1+exp(-c5*(t-c6)))]

    c1 = background level, c2 = seasonal amplitude, c4/c6 = green-up/
    green-down inflection points (DOY), c3/c5 = steepness of each limb.

Expensive at 500m over agricultural India (~10M+ pixels x 26 years), so
parallelised with multiprocessing.Pool, same chunked pattern as spi_fit.py
- meant to run on a workstation, not a lightweight sandbox.
"""

import multiprocessing
import os

import numpy as np
import rasterio
from scipy.optimize import curve_fit
from scipy.special import expit

from computing.farm_stress.config import (
    PHENOLOGY_MIN_VALID_PERIODS,
    MODIS_16DAY_PERIOD_START_DOYS,
    LOCAL_DIR_VCI_RASTERS,
    LOCAL_DIR_PHENOLOGY,
)

# Loose parameter bounds, mainly to stop curve_fit wandering to
# nonsensical/runaway solutions on noisy pixels - not a tight physical
# constraint. c4/c6 (DOY) bounded a little past [1, 365] since a
# green-down inflection can fall past year-end for a late-season pixel.
PARAM_BOUNDS = (
    [-50, 0, 0.001, 1, 0.001, 1],  # lower: c1, c2, c3, c4, c5, c6
    [150, 200, 5, 400, 5, 400],  # upper
)

# Caps how many attempts curve_fit gets before giving up on a stubborn
# pixel. Timed on 300 real AEZ 5/2000 pixels at the old maxfev=2000: mean
# 17.24ms/fit (vs. 1.96ms on clean synthetic data), with a heavy tail -
# 4.3% of pixels over 50ms, worst case 437.88ms - clearly a handful of
# noisy/ambiguous pixels burning through most of their iteration budget
# without converging. Lowering the cap makes those pixels fail faster
# instead of dragging out the whole run; the tradeoff is that a pixel
# that would have eventually converged on attempt #600 now gives up
# early and is marked NaN instead. 500 is a first guess, not tuned -
# worth re-timing after this change to see how much it actually helps
# and whether the NaN rate rises enough to matter.
MAXFEV = 500


def double_logistic(t, c1, c2, c3, c4, c5, c6):
    """The Elmore et al. 2012 double-logistic seasonal curve model.

    Uses scipy.special.expit (a numerically-stable 1/(1+exp(-x))) rather
    than raw np.exp: curve_fit's search routinely pushes c3/c5*(t-c4/c6)
    to large magnitudes while exploring parameter space, which overflows
    plain np.exp(-x) (-> inf, RuntimeWarning, and occasionally inf/nan
    propagating into the fit) even though the true sigmoid value at those
    extremes is just 0 or 1.
    """
    return c1 + c2 * (expit(c3 * (t - c4)) - expit(c5 * (t - c6)))


def _initial_guess(t, y):
    """Heuristic starting point for curve_fit, derived from the pixel's
    own data - same spirit as spi_fit.py's Method-of-Moments starting
    guess for the gamma MLE. A fixed generic guess converges poorly on
    atypical pixels (double-cropped, badly masked, etc).
    """
    c1 = float(np.min(y))
    c2 = float(np.max(y) - c1)
    if c2 <= 0:
        return None  # degenerate: flat series, nothing to fit

    half = c1 + c2 / 2
    peak_idx = int(np.argmax(y))

    rising_t, rising_y = t[: peak_idx + 1], y[: peak_idx + 1]
    c4 = float(rising_t[np.argmin(np.abs(rising_y - half))]) if rising_t.size else float(t[0])

    falling_t, falling_y = t[peak_idx:], y[peak_idx:]
    c6 = float(falling_t[np.argmin(np.abs(falling_y - half))]) if falling_t.size else float(t[-1])

    if c6 <= c4:
        c6 = c4 + 30  # keep green-down after green-up so the model starts sane

    return [c1, c2, 0.15, c4, 0.15, c6]


def fit_phenology(vci_series, doy_array):
    """Fit one pixel/year. vci_series and doy_array are both length-23
    (NaN entries in vci_series are dropped before fitting).

    Returns (greenup_doy, peak_doy, greendown_doy), all NaN if there
    aren't enough valid periods or the fit fails/is degenerate.
    """
    vci_series = np.asarray(vci_series, dtype=np.float64)
    doy_array = np.asarray(doy_array, dtype=np.float64)
    mask = ~np.isnan(vci_series)

    if mask.sum() < PHENOLOGY_MIN_VALID_PERIODS:
        return np.nan, np.nan, np.nan

    t, y = doy_array[mask], vci_series[mask]
    p0 = _initial_guess(t, y)
    if p0 is None:
        return np.nan, np.nan, np.nan

    try:
        popt, _ = curve_fit(double_logistic, t, y, p0=p0, bounds=PARAM_BOUNDS, maxfev=MAXFEV)
    except Exception:
        return np.nan, np.nan, np.nan

    c1, c2, c3, c4, c5, c6 = popt
    if c3 <= 0 or c5 <= 0:
        return np.nan, np.nan, np.nan  # shouldn't happen given bounds, guard anyway

    greenup_doy = c4 - np.log(4) / c3
    peak_doy = (c4 + c6) / 2
    greendown_doy = c6 + np.log(4) / c5
    return greenup_doy, peak_doy, greendown_doy


def _fit_pixel_chunk(args):
    """args: (vci_chunk (23, n_pixels_in_chunk), doy_array) -> (n_pixels_in_chunk, 3)."""
    vci_chunk, doy_array = args
    n_pixels = vci_chunk.shape[1]
    out = np.full((n_pixels, 3), np.nan, dtype=np.float64)
    for j in range(n_pixels):
        out[j] = fit_phenology(vci_chunk[:, j], doy_array)
    return out


def batch_fit_phenology_year(vci_year_stack, doy_array, n_workers=None, chunk_size=50_000):
    """Fit every pixel in one year's (23, rows, cols) VCI stack.

    Pre-filters to only pixels with >= PHENOLOGY_MIN_VALID_PERIODS valid
    periods before dispatching to workers - at 500m over India, the vast
    majority of pixels (ocean, non-agri, or too cloud-masked) would fail
    that check trivially, and iterating all of them through curve_fit
    setup/multiprocessing overhead is wasted work at this scale (same
    lesson as the climatology's redundant-decompression bug: filter
    before the expensive part, not after).

    Returns three (rows, cols) arrays: greenup_doy, peak_doy, greendown_doy.
    """
    n_periods, rows, cols = vci_year_stack.shape
    flat = vci_year_stack.reshape(n_periods, rows * cols)

    valid_count = np.sum(~np.isnan(flat), axis=0)
    fit_idx = np.where(valid_count >= PHENOLOGY_MIN_VALID_PERIODS)[0]

    greenup = np.full(rows * cols, np.nan, dtype=np.float64)
    peak = np.full(rows * cols, np.nan, dtype=np.float64)
    greendown = np.full(rows * cols, np.nan, dtype=np.float64)

    if fit_idx.size == 0:
        return greenup.reshape(rows, cols), peak.reshape(rows, cols), greendown.reshape(rows, cols)

    to_fit = flat[:, fit_idx]
    n_workers = n_workers or multiprocessing.cpu_count()
    chunks = [
        (to_fit[:, i : i + chunk_size], doy_array) for i in range(0, to_fit.shape[1], chunk_size)
    ]

    with multiprocessing.Pool(n_workers) as pool:
        results = pool.map(_fit_pixel_chunk, chunks)
    params = np.concatenate(results, axis=0)  # (n_fit_pixels, 3)

    greenup[fit_idx] = params[:, 0]
    peak[fit_idx] = params[:, 1]
    greendown[fit_idx] = params[:, 2]

    return greenup.reshape(rows, cols), peak.reshape(rows, cols), greendown.reshape(rows, cols)


def _load_year_vci_stack(path, periods_in_file, n_periods=23):
    """Read one year's VCI COG into a full (23, rows, cols) array, NaN-
    filling any periods missing from that year (e.g. year 2000, whose
    MODIS NDVI collection only starts 2000-02-18 - see
    vci_climatology.py's _get_year_band_periods for why band position
    can't just be assumed to equal period_of_year for partial years).
    """
    with rasterio.open(path) as src:
        rows, cols = src.height, src.width
        raw = src.read().astype(np.float32)  # (n_bands_in_file, rows, cols)

    stack = np.full((n_periods, rows, cols), np.nan, dtype=np.float32)
    for band_index, period in enumerate(periods_in_file):
        stack[period] = raw[band_index]
    return stack


def fit_phenology_archive(
    gee_account_id,
    start_year=2000,
    end_year=2025,
    rasters_dir=LOCAL_DIR_VCI_RASTERS,
    output_dir=LOCAL_DIR_PHENOLOGY,
    overwrite=False,
    n_workers=None,
    chunk_size=50_000,
):
    """Fit double-logistic phenology per pixel per year (2000-2025),
    write per-year greenup/peak/greendown rasters, then a multi-year
    median (climatological phenology) across all years fit.

    Safe to interrupt and re-run: years already on disk are skipped
    unless overwrite=True (climatology is always recomputed at the end,
    since it's cheap and depends on whichever years exist on disk).
    """
    from computing.farm_stress.vci_climatology import _get_year_band_periods  # avoid import cycle at module load

    doy_array = np.array(MODIS_16DAY_PERIOD_START_DOYS, dtype=np.float64)
    years = list(range(start_year, end_year + 1))
    available = {y: f"{rasters_dir}/vci_{y}_cog.tif" for y in years if os.path.exists(f"{rasters_dir}/vci_{y}_cog.tif")}
    missing_years = [y for y in years if y not in available]
    if missing_years:
        print(f"Warning: missing local VCI rasters for years {missing_years}, proceeding without them")

    to_fit_years = [
        y
        for y in available
        if overwrite
        or not all(
            os.path.exists(f"{output_dir}/{name}_{y}.tif") for name in ("greenup", "peak", "greendown")
        )
    ]
    skipped_years = [y for y in available if y not in to_fit_years]

    os.makedirs(output_dir, exist_ok=True)
    profile = None
    computed_years = []

    if to_fit_years:
        print(f"Fetching band-order metadata from GEE for {len(to_fit_years)} year(s) ...")
        year_periods = _get_year_band_periods(gee_account_id, sorted(to_fit_years))

        for i, year in enumerate(sorted(to_fit_years), start=1):
            path = available[year]
            with rasterio.open(path) as src:
                if profile is None:
                    profile = src.profile
            stack = _load_year_vci_stack(path, year_periods[year])

            print(f"[{i}/{len(to_fit_years)}] Fitting {year} ({stack.shape[1]}x{stack.shape[2]} pixels) ...")
            greenup, peak, greendown = batch_fit_phenology_year(
                stack, doy_array, n_workers=n_workers, chunk_size=chunk_size
            )
            del stack

            out_profile = profile.copy()
            out_profile.update(count=1, dtype="float64", nodata=np.nan)
            for name, arr in (("greenup", greenup), ("peak", peak), ("greendown", greendown)):
                out_path = f"{output_dir}/{name}_{year}.tif"
                with rasterio.open(out_path, "w", **out_profile) as dst:
                    dst.write(arr, 1)
            print(f"  {year} done -> {output_dir}/{{greenup,peak,greendown}}_{year}.tif")
            computed_years.append(year)
    else:
        print("All years already fit, skipping straight to climatology.")
        with rasterio.open(available[sorted(available)[0]]) as src:
            profile = src.profile

    # Climatological phenology: per-pixel median across whichever years
    # ended up with a fitted raster (freshly computed this run, plus any
    # skipped-because-already-there from a prior run).
    fitted_years = [
        y for y in available if all(os.path.exists(f"{output_dir}/{name}_{y}.tif") for name in ("greenup", "peak", "greendown"))
    ]
    print(f"\nComputing climatological phenology (median across {len(fitted_years)} year(s)) ...")
    for name in ("greenup", "peak", "greendown"):
        layers = []
        for y in fitted_years:
            with rasterio.open(f"{output_dir}/{name}_{y}.tif") as src:
                layers.append(src.read(1))
        stack = np.stack(layers, axis=0)
        with np.errstate(invalid="ignore"):
            median = np.nanmedian(stack, axis=0)
        out_profile = profile.copy()
        out_profile.update(count=1, dtype="float64", nodata=np.nan)
        out_path = f"{output_dir}/{name}_clim.tif"
        with rasterio.open(out_path, "w", **out_profile) as dst:
            dst.write(median, 1)
        print(f"  {name}_clim.tif -> {out_path}")

    print(f"\nDone. Fit {len(computed_years)} year(s), skipped {len(skipped_years)}.")
    return {
        "computed_years": computed_years,
        "skipped_years": skipped_years,
        "missing_years": missing_years,
        "output_dir": output_dir,
    }
