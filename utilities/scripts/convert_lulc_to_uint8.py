#!/usr/bin/env python3
"""
Convert pan-India LULC rasters from float64 (NaN no-data) to uint8 (no-data 0).

Per pixel:
- NaN                                   -> 0
- within --tol of a whole number        -> rounded (float noise, e.g. 5.999999999999999 -> 6)
- further than --tol from a whole number -> snapped to the closest class among its
  8 neighbours that are clean whole numbers (1-12); rounded if it has none.
  These are the blended pixels on river-basin boundaries, e.g. 3.26.

The output keeps the same grid, is written as EPSG:4326, and is checked for that
before it is moved into place.

Example:
    python utilities/scripts/convert_lulc_to_uint8.py \
        data/base_layers/lulc/lulc_v3_20??_20??.tif \
        --output-dir data/base_layers/lulc_uint8
"""

import argparse
import os
import time
from concurrent.futures import ProcessPoolExecutor

# GDAL's block cache defaults to 5% of RAM per process. Every input block is read
# only once here, so with many workers that cache just fills RAM (and gets the run
# OOM-killed). Set before rasterio loads GDAL; worker processes inherit it.
os.environ.setdefault("GDAL_CACHEMAX", "256")  # MB per process

import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.windows import Window

MAX_CLASS = 12
NEIGHBOURS = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]
EPSG_4326_WKT_END = 'AUTHORITY["EPSG","4326"]]'

_datasets = {}


def _open(path):
    # Keep one open handle per worker process instead of reopening for every tile.
    if path not in _datasets:
        _datasets[path] = rasterio.open(path)
    return _datasets[path]


def _read_with_halo(src, col, row, width, height):
    # Read the tile plus a 1-pixel border so edge pixels can see their neighbours.
    # Outside the raster the border stays NaN.
    c0, r0 = max(col - 1, 0), max(row - 1, 0)
    c1, r1 = min(col + width + 1, src.width), min(row + height + 1, src.height)
    data = src.read(1, window=Window(c0, r0, c1 - c0, r1 - r0))
    a = np.full((height + 2, width + 2), np.nan)
    a[r0 - row + 1 : r1 - row + 1, c0 - col + 1 : c1 - col + 1] = data
    return a


def convert_tile(job):
    path, col, row, width, height, tol = job
    a = _read_with_halo(_open(path), col, row, width, height)
    finite = np.isfinite(a)
    stats = np.zeros(4, np.int64)  # nan, float-noise rounded, snapped, no clean neighbour
    if not finite[1:-1, 1:-1].any():
        stats[0] = width * height
        return col, row, np.zeros((height, width), np.uint8), stats

    rounded = np.rint(np.where(finite, a, 0))
    fractional = finite & (np.abs(a - rounded) > tol)
    clean = finite & ~fractional & (rounded >= 1) & (rounded <= MAX_CLASS)
    out = rounded.copy()

    rows, cols = np.nonzero(fractional[1:-1, 1:-1])
    rows, cols = rows + 1, cols + 1
    best_dist = np.full(rows.size, np.inf)
    for dr, dc in NEIGHBOURS:
        cand = rounded[rows + dr, cols + dc]
        dist = np.where(clean[rows + dr, cols + dc], np.abs(a[rows, cols] - cand), np.inf)
        better = dist < best_dist
        best_dist[better] = dist[better]
        out[rows[better], cols[better]] = cand[better]

    core = out[1:-1, 1:-1]
    if core.min() < 0 or core.max() > MAX_CLASS:
        raise ValueError(f"{path}: value outside 0-{MAX_CLASS} in tile col={col} row={row}")

    inner = (slice(1, -1), slice(1, -1))
    stats[0] = (~finite[inner]).sum()
    stats[1] = (finite & ~fractional & (a != rounded))[inner].sum()
    stats[2] = rows.size
    stats[3] = np.isinf(best_dist).sum()
    return col, row, core.astype(np.uint8), stats


def convert_file(path, output_dir, workers, tile, tol):
    out_path = os.path.join(output_dir, os.path.basename(path))
    if os.path.exists(out_path):
        print(f"skip (already exists): {out_path}")
        return
    tmp_path = out_path + ".tmp"

    with rasterio.open(path) as src:
        width, height = src.width, src.height
        profile = {
            "driver": "GTiff",
            "width": width,
            "height": height,
            "count": 1,
            "dtype": "uint8",
            "nodata": 0,
            "crs": CRS.from_epsg(4326),
            "transform": src.transform,
            "tiled": True,
            "blockxsize": 256,
            "blockysize": 256,
            "compress": "deflate",
            "predictor": 2,
            "bigtiff": "YES",
            "num_threads": "ALL_CPUS",
        }

    jobs = [
        (path, c, r, min(tile, width - c), min(tile, height - r), tol)
        for r in range(0, height, tile)
        for c in range(0, width, tile)
    ]
    print(f"{os.path.basename(path)}: {len(jobs)} tiles, {workers} workers")

    totals = np.zeros(4, np.int64)
    start = time.time()
    batch = workers * 4  # keeps only a few tiles in memory at a time
    with rasterio.open(tmp_path, "w", **profile) as dst, ProcessPoolExecutor(workers) as pool:
        for i in range(0, len(jobs), batch):
            for col, row, data, stats in pool.map(convert_tile, jobs[i : i + batch]):
                dst.write(data, 1, window=Window(col, row, data.shape[1], data.shape[0]))
                totals += stats
            done = min(i + batch, len(jobs))
            print(f"  {done}/{len(jobs)} tiles, {time.time() - start:.0f}s", end="\r")
    print()

    with rasterio.open(tmp_path) as check:
        if not check.crs.to_wkt().rstrip().endswith(EPSG_4326_WKT_END):
            raise RuntimeError(
                f"{tmp_path} was not written as EPSG:4326 (PROJ problem in this "
                "environment?). Not moving it into place."
            )
    os.replace(tmp_path, out_path)

    nan, noise, snapped, no_neighbour = totals.tolist()
    print(f"  written: {out_path} ({time.time() - start:.0f}s)")
    print(
        f"  NaN -> 0: {nan:,} | float noise rounded: {noise:,} | "
        f"fractional snapped: {snapped - no_neighbour:,} | "
        f"fractional rounded (no clean neighbour): {no_neighbour:,}"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("inputs", nargs="+", help="float64 LULC GeoTIFFs to convert")
    parser.add_argument("--output-dir", required=True, help="folder for the uint8 files (not the input folder)")
    parser.add_argument("--workers", type=int, default=os.cpu_count(), help="worker processes (default: all cores)")
    parser.add_argument("--tile", type=int, default=2048, help="tile size in pixels, multiple of 256 (default: 2048)")
    parser.add_argument("--tol", type=float, default=1e-3, help="distance from a whole number treated as float noise (default: 0.001)")
    args = parser.parse_args()

    if args.tile % 256:
        parser.error("--tile must be a multiple of 256")
    os.makedirs(args.output_dir, exist_ok=True)
    for path in args.inputs:
        if os.path.samefile(os.path.dirname(os.path.abspath(path)), args.output_dir):
            parser.error("--output-dir must not be the input folder")

    for path in args.inputs:
        convert_file(path, args.output_dir, args.workers, args.tile, args.tol)


if __name__ == "__main__":
    main()
