import tempfile
from functools import partial
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
        self.mws_path = (
            self.mws_dir / "bihar" / "banka" / "banka" / "filtered_mws_banka_banka_uid.geojson"
        )
        self.mws_path.parent.mkdir(parents=True)
        gdf.to_file(self.mws_path, driver="GeoJSON")
        admin_path = self.admin_dir / "bihar" / "banka" / "banka" / "banka_banka.geojson"
        admin_path.parent.mkdir(parents=True)
        gdf.to_file(admin_path, driver="GeoJSON")
        for target, path in (
            ("LOCAL_ADMIN_BOUNDARY_DIR", self.admin_dir),
            ("PRECOMPUTED_TEHSIL_WATERSHED_DIR", self.mws_dir),
        ):
            p = patch.object(helper, target, path)
            p.start()
            self.addCleanup(p.stop)
        p = patch.object(
            helper,
            "resolve_precomputed_vector_file",
            partial(
                helper.resolve_precomputed_vector_file,
                precomputed_roi_dir=self.mws_dir,
            ),
        )
        p.start()
        self.addCleanup(p.stop)

    @patch.object(helper, "_layer_exists_on_all_targets", return_value=False)
    @patch.object(helper, "push_local_vector_to_geoserver")
    def test_pushes_both_layers(self, push, _exists):
        push.return_value = {"status_code": 201}
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


class ResolveTehsilMwsFileTests(SimpleTestCase):
    def test_resolves_filtered_mws_file_in_tehsil_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = (
                root
                / "west_bengal"
                / "north_twenty-four_parganas"
                / "barasat"
                / "filtered_mws_north_twenty-four_parganas_barasat_uid.geojson"
            )
            path.parent.mkdir(parents=True)
            path.write_text("{}")
            self.assertEqual(
                helper.resolve_precomputed_vector_file(
                    "West Bengal",
                    "North Twenty-Four Parganas",
                    "Barasat",
                    precomputed_roi_dir=root,
                ),
                path,
            )

    def test_flat_legacy_file_is_not_resolved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "bihar" / "banka").mkdir(parents=True)
            (root / "bihar" / "banka" / "banka.geojson").write_text("{}")
            with self.assertRaises(FileNotFoundError):
                helper.resolve_precomputed_vector_file(
                    "Bihar", "Banka", "Banka", precomputed_roi_dir=root
                )


class ResolveAdminBoundaryFileTests(SimpleTestCase):
    def test_hyphenated_names_use_underscore_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = (
                root / "meghalaya" / "ri_bhoi" / "ribhoi" / "ri-bhoi_ribhoi.geojson"
            )
            path.parent.mkdir(parents=True)
            path.write_text("{}")
            with patch.object(helper, "LOCAL_ADMIN_BOUNDARY_DIR", root):
                self.assertEqual(
                    helper._resolve_local_admin_boundary_file(
                        "Meghalaya", "Ri-Bhoi", "Ribhoi"
                    ),
                    path,
                )
