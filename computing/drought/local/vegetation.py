"""Vegetation stage (VCI) from MODIS MOD09A1, on the 500 m grid of the region.

For every year from 2000, each 4-week window gets the highest NDVI and NDWI of its
composites (those with at least 4 of their 8 days in the window). Clouds (method
note, "Cloud masking for VCI"): an image is clear when StateQA's cloud state is
clear or not set, with no cloud shadow and no internal cloud flag. The highest
clear value is used; if no image is clear, the highest of all images, and a flag
records that.

VCI of a season's window = (value - lowest) / (highest - lowest) x 100 over the
same window in every year from 2000 up to the season, for NDVI and for NDWI;
the lower of the two is kept (manual section 3.2.2).

    NDVI = (NIR - red) / (NIR + red)   = (b02 - b01) / (b02 + b01)
    NDWI = (NIR - SWIR) / (NIR + SWIR) = (b02 - b06) / (b02 + b06)

Files, under drought/<region>/<version>/vegetation/:
  window_max/<year>.tif   per window: ndvi_x10000_wNN, ndwi_x10000_wNN, cloud_fallback_wNN
  vci_<season>.tif        per window: vci_wNN (0-100), cloud_fallback_wNN
"""

import logging
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import rasterio
from rasterio.windows import Window
from rasterio.windows import transform as window_transform

from computing.drought.local.config import ONSET_FIRST_WEEK, VCI_HISTORY_START, Paths
from computing.drought.local.grid import blocks, load_grid
from computing.drought.local.store import INT16_NODATA, to_int16, write_stack
from computing.drought.local.weeks import season_window_starts, vci_composites, vci_history_years, window_dates

log = logging.getLogger(__name__)

REFLECTANCE_NODATA = -28672
STATE_NODATA = 65535
WINDOW_STARTS = list(season_window_starts(ONSET_FIRST_WEEK))
BLOCK_PX = 512


def vegetation_dir(paths):
    return paths.version_dir / "vegetation"


def window_max_file(paths, year):
    return vegetation_dir(paths) / "window_max" / f"{year}.tif"


def vci_file(paths, season):
    return vegetation_dir(paths) / f"vci_{season}.tif"


def window_max_names():
    names = []
    for w in WINDOW_STARTS:
        names += [f"ndvi_x10000_w{w:02d}", f"ndwi_x10000_w{w:02d}", f"cloud_fallback_w{w:02d}"]
    return names


# ---------------------------------------------------------------- per-image values


def clear_mask(state):
    """Clear: cloud state clear (0) or not set (3), no cloud shadow (bit 2), no internal cloud flag (bit 10)."""
    state = np.asarray(state).astype(np.int64)
    cloud_state = state & 3
    return (state != STATE_NODATA) & ((cloud_state == 0) | (cloud_state == 3)) \
        & (((state >> 2) & 1) == 0) & (((state >> 10) & 1) == 0)


def normalized_difference(a, b):
    """(a - b) / (a + b) from scaled reflectance; NaN where either is no-data or the sum is 0."""
    a = np.asarray(a, dtype=np.float32)
    b = np.asarray(b, dtype=np.float32)
    bad = (a == REFLECTANCE_NODATA) | (b == REFLECTANCE_NODATA) | (a + b == 0)
    with np.errstate(divide="ignore", invalid="ignore"):
        out = (a - b) / (a + b)
    out[bad] = np.nan
    return out


def window_max(values, clear):
    """values: list of arrays (one per image, NaN = no value); clear: matching boolean arrays.
    Returns the highest clear value, or the highest of all where no image is clear, and
    a flag that is True where the fallback was used."""
    best_clear = np.full(values[0].shape, -np.inf, dtype=np.float32)
    best_any = np.full(values[0].shape, -np.inf, dtype=np.float32)
    for v, c in zip(values, clear):
        v = np.where(np.isnan(v), -np.inf, v)
        best_any = np.maximum(best_any, v)
        best_clear = np.maximum(best_clear, np.where(c, v, -np.inf))
    fallback = np.isinf(best_clear) & ~np.isinf(best_any)
    out = np.where(np.isinf(best_clear), best_any, best_clear)
    out[np.isinf(out)] = np.nan
    return out, fallback


def vci(value, history):
    """(value - lowest) / (highest - lowest) x 100 over history (years, ...), NaN-aware."""
    history = np.asarray(history, dtype=np.float32)
    with np.errstate(invalid="ignore", divide="ignore"), np.testing.suppress_warnings() as sup:
        sup.filter(RuntimeWarning)
        low, high = np.nanmin(history, axis=0), np.nanmax(history, axis=0)
        out = (np.asarray(value, dtype=np.float32) - low) / (high - low) * 100
    out[~(high > low)] = np.nan
    return np.clip(out, 0, 100)


# ---------------------------------------------------------------- window maxima


def _read(paths, start, suffix, window):
    path = paths.modis_layer("mod09a1", start, suffix)
    if not path.is_file():
        return None
    with rasterio.open(path) as src:
        return src.read(1, window=window)


def block_window_max(paths, year, window):
    """Window maxima for one block and year, shape (3 x windows, rows, cols), int16."""
    images = {}
    for w in WINDOW_STARTS:
        for start in vci_composites(*window_dates(year, w)):
            if start in images:
                continue
            red, nir, swir, state = (_read(paths, start, s, window) for s in ("b01", "b02", "b06", "state"))
            if red is None or nir is None or swir is None or state is None:
                images[start] = None  # missing at NASA
                continue
            images[start] = (normalized_difference(nir, red), normalized_difference(nir, swir), clear_mask(state))
    bands = []
    shape = (window.height, window.width)
    for w in WINDOW_STARTS:
        members = [images[s] for s in vci_composites(*window_dates(year, w)) if images.get(s) is not None]
        if not members:
            bands += [np.full(shape, INT16_NODATA, np.int16)] * 2 + [np.zeros(shape, np.int16)]
            continue
        ndvi, fallback = window_max([m[0] for m in members], [m[2] for m in members])
        ndwi, _ = window_max([m[1] for m in members], [m[2] for m in members])
        bands += [to_int16(ndvi, 10000), to_int16(ndwi, 10000), fallback.astype(np.int16)]
    return np.stack(bands)


def _block_job(args):
    data_dir, lulc_dir, year, window = args
    return window, block_window_max(Paths(data_dir, lulc_dir), year, window)


def ensure_window_max(paths, years, region, workers=8):
    grid = load_grid(paths)
    block_list = list(blocks(grid, region, BLOCK_PX))
    transform = window_transform(region, grid.transform)
    inside = grid.mask[region.row_off:region.row_off + region.height, region.col_off:region.col_off + region.width]
    names = window_max_names()
    for year in years:
        path = window_max_file(paths, year)
        if path.is_file():
            continue
        log.info("Vegetation window maxima %s: %d blocks", year, len(block_list))
        data = np.full((len(names), region.height, region.width), INT16_NODATA, np.int16)
        jobs = [(paths.data_dir, paths.lulc_dir, year, b) for b in block_list]
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for b, arr in pool.map(_block_job, jobs):
                r, c = b.row_off - region.row_off, b.col_off - region.col_off
                data[:, r:r + b.height, c:c + b.width] = arr
        data[:, ~inside] = INT16_NODATA
        write_stack(path, data, transform, grid.crs, names, INT16_NODATA,
                    {"year": year, "region": paths.region_key, "clear": "cloud state clear/not set, no shadow, no internal cloud"})


# ---------------------------------------------------------------- VCI


def _read_band(path, name):
    with rasterio.open(path) as src:
        data = src.read(list(src.descriptions).index(name) + 1).astype(np.float32)
    data[data == INT16_NODATA] = np.nan
    return data


def season_vci(paths, season, region):
    grid = load_grid(paths)
    years = list(vci_history_years(season))
    bands, names = [], []
    for w in WINDOW_STARTS:
        per_index = []
        for index in ("ndvi", "ndwi"):
            name = f"{index}_x10000_w{w:02d}"
            history = np.stack([_read_band(window_max_file(paths, y), name) for y in years])
            per_index.append(vci(history[-1], history))
        value = np.fmin(per_index[0], per_index[1])  # the lower of the two; NaN only if both are
        fallback = _read_band(window_max_file(paths, season), f"cloud_fallback_w{w:02d}")
        bands += [to_int16(value), np.nan_to_num(fallback).astype(np.int16)]
        names += [f"vci_w{w:02d}", f"cloud_fallback_w{w:02d}"]
    write_stack(vci_file(paths, season), np.stack(bands), window_transform(region, grid.transform), grid.crs,
                names, INT16_NODATA, {"season": season, "history": f"{years[0]}-{years[-1]}", "region": paths.region_key})


def run(paths, seasons, region, workers=8):
    """Vegetation stage for the seasons. Returns the seasons written."""
    todo = [s for s in seasons if not vci_file(paths, s).is_file()]
    if not todo:
        return []
    ensure_window_max(paths, range(VCI_HISTORY_START, max(todo) + 1), region, workers)
    for season in todo:
        log.info("VCI season %s", season)
        season_vci(paths, season, region)
    return todo
