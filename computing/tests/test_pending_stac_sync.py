import os
import shutil
import tempfile
from io import StringIO
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pystac
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase

from computing.STAC_specs.stac_collection import STACCollectionGenerator, STACConfig


def _fake_item(item_id):
    return pystac.Item(
        id=item_id,
        geometry={
            "type": "Polygon",
            "coordinates": [[[84.0, 24.0], [85.0, 24.0], [85.0, 25.0], [84.0, 25.0], [84.0, 24.0]]],
        },
        bbox=[84.0, 24.0, 85.0, 25.0],
        datetime=None,
        properties={"start_datetime": "2024-01-01T00:00:00Z", "end_datetime": "2024-12-31T00:00:00Z"},
    )


class TehsilBatchStacSyncTests(SimpleTestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(prefix="stac_batch_test_")
        self.config = STACConfig(
            geoserver_base_url="https://geoserver.example.com/geoserver",
            local_data_dir=self.tmp_dir,
            stac_files_dir=os.path.join(self.tmp_dir, "STAC_output_merged"),
            thumbnail_dir=os.path.join(self.tmp_dir, "thumbnails"),
            style_file_dir=os.path.join(self.tmp_dir, "styles"),
            layer_desc_csv=os.path.join(self.tmp_dir, "layer_desc.csv"),
            layer_map_csv=os.path.join(self.tmp_dir, "layer_map.csv"),
            column_desc_csv=os.path.join(self.tmp_dir, "col_desc.csv"),
            s3_uri="s3://test-bucket/",
        )

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    @patch("computing.STAC_specs.stac_collection._mark_layer_stac_generated")
    @patch("computing.STAC_specs.stac_collection.VectorSTACItemBuilder.build")
    @patch("computing.STAC_specs.stac_collection.RasterSTACItemBuilder.build")
    def test_sync_tehsil_layers_uploads_to_s3_once_and_marks_layers(
        self, mock_raster_build, mock_vector_build, mock_mark
    ):
        mock_raster_build.return_value = _fake_item("bihar_banka_banka_lulc_2023")
        mock_vector_build.return_value = _fake_item("bihar_banka_banka_terrain_clusters")

        gen = STACCollectionGenerator(config=self.config)
        gen.s3_syncer = MagicMock()
        gen.s3_syncer.upload_paths.return_value = True

        targets = [
            {
                "layer_type": "raster",
                "layer_name": "land_use_land_cover_raster",
                "start_year": "2023",
                "end_year": "2023",
                "layer_ids": [101],
            },
            {
                "layer_type": "vector",
                "layer_name": "terrain_clusters",
                "start_year": "",
                "end_year": "",
                "layer_ids": [102, 103],
            },
        ]

        res = gen.sync_tehsil_layers(
            "Bihar", "Banka", "Banka", targets, upload_to_s3=True
        )

        self.assertEqual(len(res["built"]), 2)
        self.assertEqual(res["failed"], [])
        self.assertTrue(res["s3_synced"])
        # Only ONE targeted S3 upload call for the entire tehsil batch
        gen.s3_syncer.upload_paths.assert_called_once()
        # All 3 layer IDs marked generated after S3 upload succeeded
        marked = [c.args[0] for c in mock_mark.call_args_list]
        self.assertEqual(marked, [101, 102, 103])

    @patch("computing.STAC_specs.stac_collection._mark_layer_stac_generated")
    @patch("computing.STAC_specs.stac_collection.VectorSTACItemBuilder.build")
    def test_sync_tehsil_layers_does_not_mark_layers_when_s3_fails(
        self, mock_vector_build, mock_mark
    ):
        mock_vector_build.return_value = _fake_item("bihar_banka_banka_terrain_clusters")

        gen = STACCollectionGenerator(config=self.config)
        gen.s3_syncer = MagicMock()
        gen.s3_syncer.upload_paths.return_value = False

        res = gen.sync_tehsil_layers(
            "Bihar",
            "Banka",
            "Banka",
            [
                {
                    "layer_type": "vector",
                    "layer_name": "terrain_clusters",
                    "start_year": "",
                    "end_year": "",
                    "layer_ids": [201],
                }
            ],
            upload_to_s3=True,
        )

        self.assertEqual(len(res["built"]), 1)
        self.assertFalse(res["s3_synced"])
        mock_mark.assert_not_called()

    @patch("computing.STAC_specs.stac_collection._mark_layer_stac_generated")
    @patch("computing.STAC_specs.stac_collection.VectorSTACItemBuilder.build")
    @patch("computing.STAC_specs.stac_collection.RasterSTACItemBuilder.build")
    def test_sync_tehsil_layers_continues_when_one_layer_fails(
        self, mock_raster_build, mock_vector_build, mock_mark
    ):
        mock_raster_build.return_value = None
        mock_vector_build.return_value = _fake_item("bihar_banka_banka_terrain_clusters")

        gen = STACCollectionGenerator(config=self.config)
        res = gen.sync_tehsil_layers(
            "Bihar",
            "Banka",
            "Banka",
            [
                {
                    "layer_type": "raster",
                    "layer_name": "missing_raster",
                    "start_year": "",
                    "end_year": "",
                    "layer_ids": [301],
                },
                {
                    "layer_type": "vector",
                    "layer_name": "terrain_clusters",
                    "start_year": "",
                    "end_year": "",
                    "layer_ids": [302],
                },
            ],
            upload_to_s3=False,
        )

        self.assertEqual(len(res["built"]), 1)
        self.assertEqual(len(res["failed"]), 1)
        self.assertEqual(res["failed"][0]["layer_name"], "missing_raster")
        mock_mark.assert_called_once_with(302)


class PendingStacSyncPlanningAndCommandTests(SimpleTestCase):
    @staticmethod
    def _fake_layer(lid, state, district, block, dataset_name, layer_name):
        return SimpleNamespace(
            id=lid,
            layer_name=layer_name,
            dataset=SimpleNamespace(name=dataset_name),
            state=SimpleNamespace(state_name=state),
            district=SimpleNamespace(district_name=district),
            block=SimpleNamespace(tehsil_name=block),
        )

    @patch("computing.pending_stac_sync.stac_task_kwargs_for_layer")
    @patch("computing.pending_stac_sync.pending_stac_layers_queryset")
    def test_plan_groups_by_tehsil_dedups_and_tracks_unmapped(
        self, mock_qs, mock_resolve
    ):
        from computing.pending_stac_sync import plan_pending_stac_sync

        l1 = self._fake_layer(1, "Bihar", "Banka", "Banka", "LULC_level_3", "LULC_23_24_banka_banka_level_3")
        l2 = self._fake_layer(2, "Bihar", "Banka", "Banka", "LULC_level_3", "LULC_23_24_banka_banka_level_3")
        l3 = self._fake_layer(3, "Bihar", "Banka", "Banka", "LULC_level_1", "LULC_23_24_banka_banka_level_1")
        l4 = self._fake_layer(4, "Odisha", "Angul", "Angul", "Terrain Vector", "angul_angul_terrain_clusters")
        mock_qs.return_value = [l1, l2, l3, l4]

        def resolve(layer):
            if layer.id in (1, 2):
                return {
                    "layer_type": "raster",
                    "state": "Bihar",
                    "district": "Banka",
                    "block": "Banka",
                    "layer_name": "land_use_land_cover_raster",
                    "start_year": "2023",
                    "end_year": "2023",
                }
            if layer.id == 3:
                return None
            return {
                "layer_type": "vector",
                "state": "Odisha",
                "district": "Angul",
                "block": "Angul",
                "layer_name": "terrain_clusters",
                "start_year": "",
                "end_year": "",
            }

        mock_resolve.side_effect = resolve

        plan = plan_pending_stac_sync(limit_tehsils=1)
        self.assertEqual(plan["layers_found"], 4)
        self.assertEqual(plan["tehsils_found"], 2)
        self.assertEqual(len(plan["tehsil_plans"]), 1)
        self.assertEqual(plan["tehsil_plans"][0]["state"], "Bihar")
        self.assertEqual(plan["tehsil_plans"][0]["targets"][0]["layer_ids"], [1, 2])
        self.assertEqual(len(plan["skipped_layers"]), 1)
        self.assertEqual(plan["skipped_layers"][0]["layer_id"], 3)

    @patch("computing.pending_stac_sync.plan_pending_stac_sync")
    def test_sync_pending_stac_refreshes_metadata_once_and_runs_per_tehsil(
        self, mock_plan
    ):
        from computing.pending_stac_sync import sync_pending_stac

        mock_plan.return_value = {
            "layers_found": 2,
            "tehsils_found": 2,
            "skipped_layers": [],
            "tehsil_plans": [
                {
                    "state": "Bihar",
                    "district": "Banka",
                    "block": "Banka",
                    "targets": [{"layer_type": "vector", "layer_name": "a", "layer_ids": [1]}],
                },
                {
                    "state": "Odisha",
                    "district": "Angul",
                    "block": "Angul",
                    "targets": [{"layer_type": "vector", "layer_name": "b", "layer_ids": [2]}],
                },
            ],
        }
        gen = MagicMock()
        gen.sync_tehsil_layers.side_effect = [
            {"state": "Bihar", "district": "Banka", "block": "Banka", "built": [{}], "failed": [], "s3_synced": True, "s3_paths_count": 3},
            {"state": "Odisha", "district": "Angul", "block": "Angul", "built": [{}], "failed": [], "s3_synced": True, "s3_paths_count": 3},
        ]

        summary = sync_pending_stac(
            upload_to_s3=True,
            overwrite_metadata=True,
            generator=gen,
        )

        gen.metadata.refresh_metadata.assert_called_once()
        self.assertEqual(gen.sync_tehsil_layers.call_count, 2)
        for c in gen.sync_tehsil_layers.call_args_list:
            self.assertFalse(c.kwargs["overwrite_metadata"])
            self.assertTrue(c.kwargs["upload_to_s3"])
        self.assertEqual(summary["tehsils_processed"], 2)
        self.assertEqual(summary["targets_built"], 2)
        self.assertEqual(summary["s3_sync_failures"], 0)

    def test_management_command_force_requires_yes_or_dry_run(self):
        with self.assertRaises(CommandError):
            call_command("sync_pending_stac", force=True)

    @patch("computing.management.commands.sync_pending_stac.sync_pending_stac")
    def test_management_command_dry_run_output(self, mock_sync):
        mock_sync.return_value = {
            "upload_to_s3": True,
            "layers_found": 1,
            "tehsils_found": 1,
            "tehsils_selected": 1,
            "tehsils_processed": 0,
            "targets_planned": 1,
            "targets_built": 0,
            "targets_failed": 0,
            "s3_sync_failures": 0,
            "skipped_unmapped": 0,
            "skipped_layers": [],
            "tehsil_plans": [
                {
                    "state": "Bihar",
                    "district": "Banka",
                    "block": "Banka",
                    "targets": [
                        {
                            "layer_type": "raster",
                            "layer_name": "land_use_land_cover_raster",
                            "start_year": "2023",
                            "layer_ids": [10],
                        }
                    ],
                }
            ],
            "tehsil_results": [],
        }

        out = StringIO()
        call_command("sync_pending_stac", dry_run=True, upload_to_s3=True, stdout=out)
        output = out.getvalue()
        self.assertIn("DRY RUN", output)
        self.assertIn("S3 upload ON", output)
        self.assertIn("Bihar/Banka/Banka", output)
        self.assertIn("land_use_land_cover_raster [2023]", output)
