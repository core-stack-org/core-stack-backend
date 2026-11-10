"""Cropland stage: how much of each 500 m pixel is kharif crop, from the 10 m LULC v3.

Each 10 m LULC cell is counted in the 500 m pixel that contains its centre, so a
pixel's counts add up to its roughly 2,100 LULC cells. Per season this gives the
kharif-sown share of every pixel (classes 8, 10 and 11: single kharif, double and
triple crop). From the 2017-2024 seasons it gives:
  - the area-sown normal: the average kharif-sown share;
  - the cropland mask: pixels where kharif crop (8, 10 and 11 together) is the most
    common land category on that average, even if under half.

Files, under drought/<region>/<version>/cropland/:
  share_<year>.tif        kharif_cells, valid_cells, area_sown_pct (season vs the normal, %)
  normal_<a>_<b>.tif      kharif_share_x10000, cropland, top_other_class, top_other_share_x10000
"""

import logging
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import rasterio
from rasterio.windows import Window
from rasterio.windows import transform as window_transform

from computing.drought.local.config import SOWN_NORMAL_YEARS
from computing.drought.local.grid import EARTH_RADIUS, blocks, load_grid
from computing.drought.local.store import write_stack

log = logging.getLogger(__name__)

KHARIF_CLASSES = (8, 10, 11)
N_CLASSES = 12
OTHER_CLASSES = tuple(c for c in range(1, N_CLASSES + 1) if c not in KHARIF_CLASSES)
UINT16_NODATA = 65535
BLOCK_PX = 128  # 500 m pixels per block side: about 6,000 x 6,000 LULC cells
ROWS_PER_CHUNK = 512


def cropland_dir(paths):
    return paths.version_dir / "cropland"


def share_file(paths, year):
    return cropland_dir(paths) / f"share_{year}.tif"


def normal_file(paths):
    a, b = SOWN_NORMAL_YEARS
    return cropland_dir(paths) / f"normal_{a}_{b}.tif"


def normal_years():
    return list(range(SOWN_NORMAL_YEARS[0], SOWN_NORMAL_YEARS[1] + 1))


SOWN_NORMAL_YEARS_SET = set(normal_years())



# ---------------------------------------------------------------- counting


def lulc_classes(values):
    """Class numbers 1-12 as uint8; anything else (NaN, 0, out of range) becomes 0.
    The float64 originals are rounded first."""
    values = np.asarray(values)
    if values.dtype.kind == "f":
        values = np.where(np.isnan(values), 0, np.rint(values))
    values = values.astype(np.int64)
    values[(values < 1) | (values > N_CLASSES)] = 0
    return values.astype(np.uint8)


def block_bounds_lonlat(transform, window):
    """Longitude/latitude box around a block of the sinusoidal grid, with a small margin."""
    cols = np.array([window.col_off, window.col_off + window.width])
    rows = np.linspace(window.row_off, window.row_off + window.height, 9)
    x = transform.c + cols * transform.a
    y = transform.f + rows * transform.e
    lat = y / EARTH_RADIUS  # radians
    lon = x[None, :] / (EARTH_RADIUS * np.cos(lat)[:, None])
    lon, lat = np.degrees(lon), np.degrees(lat)
    pad = 0.001
    return lon.min() - pad, lat.min() - pad, lon.max() + pad, lat.max() + pad


def count_block(lulc_path, transform, window):
    """Counts of each LULC class (index 1-12; index 0 unused) in every 500 m pixel of the block,
    shape (13, rows, cols), uint16."""
    h, w = window.height, window.width
    counts = np.zeros((N_CLASSES + 1) * h * w, dtype=np.int64)
    west, south, east, north = block_bounds_lonlat(transform, window)
    with rasterio.open(lulc_path) as src:
        lt = src.transform
        c0 = max(int(np.floor((west - lt.c) / lt.a)), 0)
        c1 = min(int(np.ceil((east - lt.c) / lt.a)), src.width)
        r0 = max(int(np.floor((lt.f - north) / -lt.e)), 0)
        r1 = min(int(np.ceil((lt.f - south) / -lt.e)), src.height)
        if c0 >= c1 or r0 >= r1:
            return counts.reshape(N_CLASSES + 1, h, w).astype(np.uint16)
        lon = np.radians(lt.c + (np.arange(c0, c1) + 0.5) * lt.a)
        for start in range(r0, r1, ROWS_PER_CHUNK):
            stop = min(start + ROWS_PER_CHUNK, r1)
            classes = lulc_classes(src.read(1, window=Window(c0, start, c1 - c0, stop - start)))
            lat = np.radians(lt.f + (np.arange(start, stop) + 0.5) * lt.e)
            row = np.floor((transform.f - EARTH_RADIUS * lat) / -transform.e).astype(np.int64) - window.row_off
            x = EARTH_RADIUS * lon[None, :] * np.cos(lat)[:, None]
            col = np.floor((x - transform.c) / transform.a).astype(np.int64) - window.col_off
            row = np.broadcast_to(row[:, None], col.shape)
            keep = (classes > 0) & (row >= 0) & (row < h) & (col >= 0) & (col < w)
            key = (row[keep] * w + col[keep]) * (N_CLASSES + 1) + classes[keep]
            counts += np.bincount(key, minlength=counts.size)
    return counts.reshape(h, w, N_CLASSES + 1).transpose(2, 0, 1).astype(np.uint16)


def _block_job(args):
    lulc_paths, transform, window = args
    return window, {year: count_block(path, transform, window) for year, path in lulc_paths.items()}


# ---------------------------------------------------------------- normal and area sown


def kharif_and_valid(counts):
    kharif = counts[list(KHARIF_CLASSES)].sum(axis=0, dtype=np.int64)
    valid = counts[1:].sum(axis=0, dtype=np.int64)
    return kharif, valid


def normal_from_mean_shares(mean_shares):
    """mean_shares: (13, ...) average share of each class over the normal years.
    Returns kharif share, cropland flag, the largest other class and its share."""
    kharif = mean_shares[list(KHARIF_CLASSES)].sum(axis=0)
    others = mean_shares[list(OTHER_CLASSES)]
    top = others.argmax(axis=0)
    top_class = np.array(OTHER_CLASSES)[top]
    top_share = np.take_along_axis(others, top[None], axis=0)[0]
    cropland = (kharif > 0) & (kharif >= top_share)
    return kharif, cropland, top_class, top_share


def area_sown_pct(kharif, valid, normal_share):
    """This season's kharif-sown share as % of the normal share; NaN where undefined."""
    with np.errstate(divide="ignore", invalid="ignore"):
        share = np.where(valid > 0, kharif / valid, np.nan)
        return np.where(normal_share > 0, share / normal_share * 100, np.nan)


def _uint16(values, scale=1.0):
    values = np.asarray(values, dtype=np.float64) * scale
    out = np.clip(np.round(np.nan_to_num(values, nan=0)), 0, UINT16_NODATA - 1)
    out[np.isnan(values)] = UINT16_NODATA
    return out.astype(np.uint16)


# ---------------------------------------------------------------- stage


def run(paths, seasons, region, workers=8):
    """Cropland stage for the region (a grid window) and seasons. Returns the seasons written."""
    grid = load_grid(paths)
    have_normal = normal_file(paths).is_file()
    todo = [s for s in seasons if not share_file(paths, s).is_file()]
    years = sorted(set(todo) | (set() if have_normal else set(normal_years())))
    if not years:
        return []
    missing = [y for y in years if not paths.lulc_file(y).is_file()]
    if missing:
        raise FileNotFoundError("LULC not found: " + ", ".join(paths.lulc_file(y).name for y in missing))

    h, w = region.height, region.width
    kharif = {y: np.zeros((h, w), np.uint16) for y in years}
    valid = {y: np.zeros((h, w), np.uint16) for y in years}
    share_sum = np.zeros((N_CLASSES + 1, h, w), np.float32) if not have_normal else None
    share_years = np.zeros((h, w), np.uint8) if not have_normal else None

    jobs = [({y: str(paths.lulc_file(y)) for y in years}, grid.transform, b) for b in blocks(grid, region, BLOCK_PX)]
    log.info("Cropland: %d blocks x %d LULC years with %d workers", len(jobs), len(years), workers)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for n, (b, per_year) in enumerate(pool.map(_block_job, jobs, chunksize=1), start=1):
            rs = slice(b.row_off - region.row_off, b.row_off - region.row_off + b.height)
            cs = slice(b.col_off - region.col_off, b.col_off - region.col_off + b.width)
            for year, counts in per_year.items():
                k, v = kharif_and_valid(counts)
                kharif[year][rs, cs], valid[year][rs, cs] = k, v
                if share_sum is not None and year in SOWN_NORMAL_YEARS_SET:
                    with np.errstate(divide="ignore", invalid="ignore"):
                        shares = np.where(v > 0, counts / np.maximum(v, 1), 0)
                    share_sum[:, rs, cs] += shares.astype(np.float32)
                    share_years[rs, cs] += (v > 0).astype(np.uint8)
            if n % 50 == 0 or n == len(jobs):
                log.info("Cropland: %d of %d blocks", n, len(jobs))

    rr, cc = slice(region.row_off, region.row_off + h), slice(region.col_off, region.col_off + w)
    inside = grid.mask[rr, cc]
    transform = window_transform(region, grid.transform)
    tags = {"normal_years": f"{SOWN_NORMAL_YEARS[0]}-{SOWN_NORMAL_YEARS[1]}",
            "kharif_classes": "8 10 11", "region": paths.region_key}

    if not have_normal:
        with np.errstate(divide="ignore", invalid="ignore"):
            mean_shares = np.where(share_years > 0, share_sum / np.maximum(share_years, 1), 0)
        normal, cropland, top_class, top_share = normal_from_mean_shares(mean_shares)
        bands = [_uint16(normal, 10000), cropland.astype(np.uint16), top_class.astype(np.uint16), _uint16(top_share, 10000)]
        data = np.stack(bands)
        data[:, ~inside | (share_years == 0)] = UINT16_NODATA
        write_stack(normal_file(paths), data, transform, grid.crs,
                    ["kharif_share_x10000", "cropland", "top_other_class", "top_other_share_x10000"],
                    UINT16_NODATA, tags)
        normal_share = normal.astype(np.float64)
        normal_share[~inside | (share_years == 0)] = np.nan
    else:
        with rasterio.open(normal_file(paths)) as src:
            raw = src.read(1).astype(np.float64)
        raw[raw == UINT16_NODATA] = np.nan
        normal_share = raw / 10000

    written = []
    for year in years:
        if share_file(paths, year).is_file():
            continue
        sown = area_sown_pct(kharif[year].astype(np.float64), valid[year].astype(np.float64), normal_share)
        data = np.stack([kharif[year], valid[year], _uint16(sown)])
        data[:, ~inside] = UINT16_NODATA
        write_stack(share_file(paths, year), data, transform, grid.crs,
                    ["kharif_cells", "valid_cells", "area_sown_pct"], UINT16_NODATA, {**tags, "season": year})
        written.append(year)
    return written
