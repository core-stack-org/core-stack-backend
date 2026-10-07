"""The 500 m MODIS grid every drought output uses, and how other data map onto it.

The grid and India mask come from periodic/modis_india_mask.tif, which the
download script writes, so the outputs line up pixel for pixel with the MODIS inputs.
"""

import re
from dataclasses import dataclass

import numpy as np
import rasterio
from rasterio.transform import Affine
from rasterio.windows import Window

EARTH_RADIUS = 6371007.181  # MODIS sinusoidal sphere, metres
GRID_X0, GRID_Y0 = -20015109.354, 10007554.677
TILE_PX = 2400
TILE_RE = re.compile(r"^h(\d\d)v(\d\d)$")


@dataclass
class Grid:
    transform: Affine
    crs: object
    width: int
    height: int
    mask: np.ndarray  # True inside India + 10 km

    @property
    def pixel(self):
        return self.transform.a

    @property
    def col0(self):
        return round((self.transform.c - GRID_X0) / self.pixel)

    @property
    def row0(self):
        return round((GRID_Y0 - self.transform.f) / self.pixel)

    @property
    def full(self):
        return Window(0, 0, self.width, self.height)


def load_grid(paths):
    if not paths.modis_mask.is_file():
        raise FileNotFoundError(
            f"{paths.modis_mask} not found. Run utilities/scripts/download_drought_inputs.py first."
        )
    with rasterio.open(paths.modis_mask) as src:
        return Grid(src.transform, src.crs, src.width, src.height, src.read(1).astype(bool))


def tile_window(grid, tile):
    """The part of the grid covered by a MODIS tile, e.g. "h25v07"."""
    match = TILE_RE.match(tile or "")
    if not match:
        raise ValueError(f"tile must look like h25v07, got {tile!r}")
    h, v = (int(x) for x in match.groups())
    col, row = h * TILE_PX - grid.col0, v * TILE_PX - grid.row0
    c0, r0 = max(col, 0), max(row, 0)
    c1, r1 = min(col + TILE_PX, grid.width), min(row + TILE_PX, grid.height)
    if c0 >= c1 or r0 >= r1:
        raise ValueError(f"tile {tile} is outside the India grid")
    return Window(c0, r0, c1 - c0, r1 - r0)


def blocks(grid, region=None, size=1024):
    """Windows of up to size x size pixels covering region (default: the whole grid),
    skipping blocks with no pixel inside India."""
    region = region or grid.full
    c_end, r_end = region.col_off + region.width, region.row_off + region.height
    for r in range(region.row_off, r_end, size):
        for c in range(region.col_off, c_end, size):
            w = Window(c, r, min(size, c_end - c), min(size, r_end - r))
            if grid.mask[r:r + w.height, c:c + w.width].any():
                yield w


def window_mask(grid, window):
    r, c = window.row_off, window.col_off
    return grid.mask[r:r + window.height, c:c + window.width]


def pixel_lonlat(grid, window):
    """Longitude and latitude (degrees) of each pixel centre in window."""
    cols = np.arange(window.col_off, window.col_off + window.width) + 0.5
    rows = np.arange(window.row_off, window.row_off + window.height) + 0.5
    x = grid.transform.c + cols * grid.transform.a
    y = grid.transform.f + rows * grid.transform.e
    xx, yy = np.meshgrid(x, y)
    lat = yy / EARTH_RADIUS
    lon = xx / (EARTH_RADIUS * np.cos(lat))
    return np.degrees(lon), np.degrees(lat)


def lookup_index(lon, lat, transform, shape):
    """Row and column of the pixel of a lon/lat raster (e.g. CHIRPS) under each point.
    Points outside it get -1. This is nearest neighbour, as in GEE."""
    if transform.b or transform.d:
        raise ValueError("rotated rasters are not supported")
    col = np.floor((np.asarray(lon) - transform.c) / transform.a).astype(np.int64)
    row = np.floor((np.asarray(lat) - transform.f) / transform.e).astype(np.int64)
    outside = (col < 0) | (row < 0) | (col >= shape[1]) | (row >= shape[0])
    col[outside], row[outside] = -1, -1
    return row, col
