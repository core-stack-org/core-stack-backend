"""MAI stage from MODIS MOD16A2GF, on the 500 m grid of the region.

For each season window, ET and PET are summed over the 8-day composites that
overlap it, each counted by the share of its days inside the window (method
note, "MODIS 8-day composites in a 4-week window"). MAI = ET / PET x 100
(manual section 3.2.3.2). Values above 32700 are NASA's codes for water, urban,
barren and other land without ET; such pixels get no MAI.
The window ET and PET are kept as well, so the MWS stage can divide their sums.

Files, under drought/<region>/<version>/moisture/:
  mai_<season>.tif   per window: mai_wNN (%), et_wNN and pet_wNN (0.1 mm)
"""

import logging
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import rasterio
from rasterio.windows import transform as window_transform

from computing.drought.local.config import ONSET_FIRST_WEEK, Paths
from computing.drought.local.grid import blocks, load_grid
from computing.drought.local.store import INT16_NODATA, band_names, to_int16, write_stack
from computing.drought.local.weeks import mai_weights, season_window_starts, window_dates

log = logging.getLogger(__name__)

ET_SCALE = 0.1  # kg/m2 per 8 days (= mm) per stored unit
VALID_MAX = 32700  # larger values are land-cover codes or no data
WINDOW_STARTS = list(season_window_starts(ONSET_FIRST_WEEK))
BLOCK_PX = 512
SUM_SCALE = 10  # window ET and PET are stored in 0.1 mm
MAI_NAMES = [f"mai_w{w:02d}" for w in WINDOW_STARTS]
ET_NAMES = [f"et_w{w:02d}" for w in WINDOW_STARTS]
PET_NAMES = [f"pet_w{w:02d}" for w in WINDOW_STARTS]


def mai_file(paths, season):
    return paths.version_dir / "moisture" / f"mai_{season}.tif"


def is_current(paths, season):
    """The season's file exists and has the window ET and PET (files from before October 2026 do not)."""
    path = mai_file(paths, season)
    return path.is_file() and set(ET_NAMES + PET_NAMES) <= set(band_names(path))


def window_sums(et, pet, weights):
    """et, pet: lists of raw composite arrays (or None when the file is missing); weights: matching shares.
    Window ET and PET in mm, NaN where any composite is missing or coded, or where PET is 0."""
    shape = next(a.shape for a in et if a is not None)
    et_sum = np.zeros(shape, dtype=np.float64)
    pet_sum = np.zeros(shape, dtype=np.float64)
    bad = np.zeros(shape, dtype=bool)
    for e, p, w in zip(et, pet, weights):
        if e is None or p is None:
            bad[:] = True
            continue
        e, p = e.astype(np.float64), p.astype(np.float64)
        bad |= (e > VALID_MAX) | (p > VALID_MAX) | (e < 0) | (p < 0)
        et_sum += w * e * ET_SCALE
        pet_sum += w * p * ET_SCALE
    bad |= ~(pet_sum > 0)
    et_sum[bad], pet_sum[bad] = np.nan, np.nan
    return et_sum, pet_sum


def window_mai(et, pet, weights):
    """MAI in %, NaN where window_sums gives no ET and PET."""
    et_sum, pet_sum = window_sums(et, pet, weights)
    return et_sum / pet_sum * 100


def _read(paths, start, suffix, window):
    path = paths.modis_layer("mod16a2gf", start, suffix)
    if not path.is_file():
        return None
    with rasterio.open(path) as src:
        return src.read(1, window=window)


def block_mai(paths, season, window):
    cache = {}

    def layer(start, suffix):
        if (start, suffix) not in cache:
            cache[(start, suffix)] = _read(paths, start, suffix, window)
        return cache[(start, suffix)]

    empty = np.full((window.height, window.width), INT16_NODATA, np.int16)
    mai, et_bands, pet_bands = [], [], []
    for w in WINDOW_STARTS:
        weights = mai_weights(*window_dates(season, w))
        starts = list(weights)
        et = [layer(s, "ET") for s in starts]
        pet = [layer(s, "PET") for s in starts]
        if all(a is None for a in et):
            for bands in (mai, et_bands, pet_bands):
                bands.append(empty)
            continue
        et_sum, pet_sum = window_sums(et, pet, [weights[s] for s in starts])
        mai.append(to_int16(et_sum / pet_sum * 100))
        et_bands.append(to_int16(et_sum, SUM_SCALE))
        pet_bands.append(to_int16(pet_sum, SUM_SCALE))
    return np.stack(mai + et_bands + pet_bands)


def _block_job(args):
    data_dir, lulc_dir, season, window = args
    return window, block_mai(Paths(data_dir, lulc_dir), season, window)


def run(paths, seasons, region, workers=8):
    """MAI stage for the seasons. Returns the seasons written."""
    todo = [s for s in seasons if not is_current(paths, s)]
    if not todo:
        return []
    grid = load_grid(paths)
    block_list = list(blocks(grid, region, BLOCK_PX))
    inside = grid.mask[region.row_off:region.row_off + region.height, region.col_off:region.col_off + region.width]
    names = MAI_NAMES + ET_NAMES + PET_NAMES
    for season in todo:
        log.info("MAI season %s: %d blocks", season, len(block_list))
        data = np.full((len(names), region.height, region.width), INT16_NODATA, np.int16)
        jobs = [(paths.data_dir, paths.lulc_dir, season, b) for b in block_list]
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for b, arr in pool.map(_block_job, jobs):
                r, c = b.row_off - region.row_off, b.col_off - region.col_off
                data[:, r:r + b.height, c:c + b.width] = arr
        data[:, ~inside] = INT16_NODATA
        write_stack(mai_file(paths, season), data, window_transform(region, grid.transform), grid.crs, names,
                    INT16_NODATA, {"season": season, "region": paths.region_key,
                                   "units": "mai: % (ET / PET); et, pet: 0.1 mm per window"})
    return todo
