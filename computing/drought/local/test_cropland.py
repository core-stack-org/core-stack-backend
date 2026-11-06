"""Tests for the cropland stage (synthetic data only).

Run from the repo root:
    python -m unittest computing.drought.local.test_cropland -v
"""

import math
import tempfile
import unittest
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import Affine, from_origin
from rasterio.windows import Window

from computing.drought.local import cropland as cl
from computing.drought.local.grid import EARTH_RADIUS

PIXEL = 1111950.5196666666 / 2400
GRID_TRANSFORM = Affine(PIXEL, 0, 6498424.162018612, 0, -PIXEL, 4133212.7441443047)  # the real India grid


class ClassTests(unittest.TestCase):
    def test_lulc_classes(self):
        got = cl.lulc_classes(np.array([np.nan, 8.0, 7.6, 13.0, 0.0, 12.2]))
        self.assertEqual(got.tolist(), [0, 8, 8, 0, 0, 12])

    def test_most_common_category(self):
        shares = np.zeros((13, 3))
        shares[[8, 10, 6], 0] = [0.30, 0.25, 0.35]  # kharif 0.55 together vs forest 0.35: cropland
        shares[[8, 6], 1] = [0.40, 0.60]  # forest wins
        shares[[8, 6, 7, 12], 2] = [0.35, 0.25, 0.20, 0.20]  # crop wins with under half
        kharif, cropland, top_class, top_share = cl.normal_from_mean_shares(shares)
        self.assertEqual(cropland.tolist(), [True, False, True])
        self.assertEqual(top_class.tolist(), [6, 6, 6])
        np.testing.assert_allclose(kharif, [0.55, 0.40, 0.35])

    def test_area_sown(self):
        got = cl.area_sown_pct(np.array([75.0, 10.0, 10.0]), np.array([100.0, 0.0, 100.0]), np.array([0.5, 0.5, 0.0]))
        self.assertAlmostEqual(float(got[0]), 150.0)
        self.assertTrue(np.isnan(got[1]) and np.isnan(got[2]))


class CountTests(unittest.TestCase):
    def test_counts_match_pixel_area(self):
        window = Window(3000, 4500, 2, 2)  # 2 x 2 pixels in central India
        west, south, east, north = cl.block_bounds_lonlat(GRID_TRANSFORM, window)
        res = 0.0001  # about 11 m
        width, height = int((east - west) / res) + 1, int((north - south) / res) + 1
        data = np.full((height, width), 8, dtype=np.uint8)
        data[:, width // 2:] = 6  # east half forest
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "lulc.tif"
            with rasterio.open(path, "w", driver="GTiff", width=width, height=height, count=1, dtype="uint8",
                               crs="EPSG:4326", transform=from_origin(west, north, res, res), nodata=0) as dst:
                dst.write(data, 1)
            counts = cl.count_block(path, GRID_TRANSFORM, window)
        self.assertEqual(counts.shape, (13, 2, 2))
        total = counts.sum(axis=0)
        lat = math.radians((GRID_TRANSFORM.f - (window.row_off + 1) * PIXEL) / EARTH_RADIUS * 180 / math.pi)
        cell_area = (res * math.pi / 180 * EARTH_RADIUS * math.cos(lat)) * (res * math.pi / 180 * EARTH_RADIUS)
        expected = PIXEL * PIXEL / cell_area
        np.testing.assert_allclose(total, expected, rtol=0.03)
        self.assertEqual(int(counts[[1, 2, 3, 4, 5, 7, 9, 10, 11, 12]].sum()), 0)
        self.assertGreater(int(counts[8].sum()), 0)
        self.assertGreater(int(counts[6].sum()), 0)


if __name__ == "__main__":
    unittest.main()
