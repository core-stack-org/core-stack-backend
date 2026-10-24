import tempfile
from pathlib import Path

import numpy as np
import rasterio
from django.test import SimpleTestCase
from rasterio.transform import from_origin

from computing.forest_additionality.GEE_Manager import GEEManager


class ComputeDistanceTests(SimpleTestCase):
    def test_writes_distance_to_target_pixel_in_metres(self):
        data = np.ones((5, 5), dtype=np.uint8)
        data[2, 2] = 0  # the only target pixel
        profile = dict(
            driver="GTiff",
            height=5,
            width=5,
            count=1,
            dtype="uint8",
            crs="EPSG:32643",
            transform=from_origin(0, 50, 10, 10),  # 10 m pixels
        )
        with tempfile.TemporaryDirectory() as tmp:
            source, output = Path(tmp, "in.tif"), Path(tmp, "out.tif")
            with rasterio.open(source, "w", **profile) as dst:
                dst.write(data, 1)

            # compute_distance never uses self, so no Earth Engine session is needed.
            GEEManager.compute_distance(None, str(source), str(output), target_value=0)

            with rasterio.open(output) as result:
                distance = result.read(1)

        self.assertEqual(distance[2, 2], 0)
        self.assertAlmostEqual(float(distance[2, 3]), 10.0, places=4)
        self.assertAlmostEqual(float(distance[0, 0]), float(np.hypot(20, 20)), places=3)
