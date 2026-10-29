from pathlib import Path
from tempfile import TemporaryDirectory

import geopandas as gpd
import pandas as pd
from django.test import SimpleTestCase
from shapely.geometry import Point

from computing.local_compute_helper import write_vector_output
from computing.misc.nrega_local_compute import _compute_nrega_for_watersheds


class LocalNregaTests(SimpleTestCase):
    def test_timestamp_attributes_can_be_written_to_geopackage(self):
        watersheds = gpd.GeoDataFrame(
            geometry=[Point(0, 0)],
            crs="EPSG:4326",
        )
        nrega = gpd.GeoDataFrame(
            {
                "work_start": pd.to_datetime(["2026-08-05"]),
                "geometry": [Point(0, 0)],
            },
            crs="EPSG:4326",
        )

        result = _compute_nrega_for_watersheds(watersheds, nrega)

        self.assertEqual(result.loc[0, "work_start"], "2026-08-05T00:00:00")
        with TemporaryDirectory() as output_dir:
            output_path = Path(output_dir) / "nrega.gpkg"
            write_vector_output(result, output_path, "nrega")
            self.assertTrue(output_path.exists())
