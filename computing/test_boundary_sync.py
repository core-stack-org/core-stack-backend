import tempfile
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
from django.test import SimpleTestCase
from shapely.geometry import box

from computing import local_compute_helper as helper


class SyncTehsilBoundariesTests(SimpleTestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        self.admin_dir = root / "admin"
        self.mws_dir = root / "mws"
        gdf = gpd.GeoDataFrame({"uid": ["a"]}, geometry=[box(0, 0, 1, 1)], crs=4326)
        for base, name in ((self.admin_dir, "banka"), (self.mws_dir, "banka")):
            (base / "bihar" / "banka").mkdir(parents=True)
            gdf.to_file(base / "bihar" / "banka" / f"{name}.geojson", driver="GeoJSON")
        for target, path in (
            ("LOCAL_ADMIN_BOUNDARY_DIR", self.admin_dir),
            ("PRECOMPUTED_TEHSIL_WATERSHED_DIR", self.mws_dir),
        ):
            p = patch.object(helper, target, path)
            p.start()
            self.addCleanup(p.stop)

    @patch.object(helper, "_layer_exists_on_all_targets", return_value=False)
    @patch.object(helper, "push_local_vector_to_geoserver")
    def test_pushes_both_layers(self, push, _exists):
        push.return_value = {"status_code": 201}
        with patch.object(
            helper,
            "resolve_precomputed_vector_file",
            return_value=self.mws_dir / "bihar" / "banka" / "banka.geojson",
        ):
            helper.sync_tehsil_boundaries_to_geoserver("Bihar", "Banka", "Banka")
        pushed = [(c.args[2], c.args[1]) for c in push.call_args_list]
        self.assertEqual(
            pushed,
            [
                ("panchayat_boundaries", "banka_banka"),
                ("mws", "mws_banka_banka"),
            ],
        )
        self.assertTrue(all(c.args[3] == "gpkg" for c in push.call_args_list))

    @patch.object(helper, "_layer_exists_on_all_targets", return_value=True)
    @patch.object(helper, "push_local_vector_to_geoserver")
    def test_existing_layers_skipped_unless_overwrite(self, push, _exists):
        push.return_value = {"status_code": 201}
        with patch.object(
            helper,
            "resolve_precomputed_vector_file",
            return_value=self.mws_dir / "bihar" / "banka" / "banka.geojson",
        ):
            helper.sync_tehsil_boundaries_to_geoserver("Bihar", "Banka", "Banka")
            push.assert_not_called()
            helper.sync_tehsil_boundaries_to_geoserver(
                "Bihar", "Banka", "Banka", overwrite=True
            )
        self.assertEqual(push.call_count, 2)

    def test_missing_admin_file_raises(self):
        with patch.object(helper, "LOCAL_ADMIN_BOUNDARY_DIR", self.admin_dir / "nope"):
            with self.assertRaises(FileNotFoundError):
                helper.sync_tehsil_boundaries_to_geoserver("Bihar", "Banka", "Banka")
