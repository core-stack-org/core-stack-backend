from unittest.mock import patch

from django.test import SimpleTestCase

from computing.local_compute_helper import queue_local_raster_for_geoserver
from computing.tasks import GeoServerPublishError, publish_local_layer


class GeoServerQueueTests(SimpleTestCase):
    @patch("computing.tasks.publish_local_layer.apply_async")
    def test_raster_publication_is_sent_to_serial_queue(self, apply_async):
        apply_async.return_value.id = "task-id"

        response = queue_local_raster_for_geoserver(
            file_path="/tmp/layer.tif",
            layer_name="layer",
            workspace="workspace",
            style_name="style",
            layer_id=42,
        )

        self.assertEqual(response["status_code"], 202)
        self.assertEqual(response["task_id"], "task-id")
        self.assertEqual(apply_async.call_args.kwargs["queue"], "geoserver")
        self.assertEqual(
            apply_async.call_args.kwargs["kwargs"]["layer_id"],
            42,
        )

    @patch("computing.utils.update_layer_sync_status")
    @patch("computing.local_compute_helper.push_local_raster_to_geoserver")
    def test_publish_task_updates_metadata_after_success(
        self,
        push_raster,
        update_sync,
    ):
        push_raster.return_value = {"status_code": 201}

        response = publish_local_layer.run(
            layer_type="raster",
            path="/tmp/layer.tif",
            layer_name="layer",
            workspace="workspace",
            layer_id=42,
        )

        self.assertEqual(response["status_code"], 201)
        update_sync.assert_called_once_with(
            layer_id=42,
            sync_to_geoserver=True,
            is_stac_specs_generated=False,
        )

    @patch("computing.local_compute_helper.push_local_raster_to_geoserver")
    def test_publish_task_raises_retryable_error_on_failure(self, push_raster):
        push_raster.return_value = {"status_code": 500}

        with self.assertRaises(GeoServerPublishError):
            publish_local_layer.run(
                layer_type="raster",
                path="/tmp/layer.tif",
                layer_name="layer",
                workspace="workspace",
            )


class GeeLayerSyncTests(SimpleTestCase):
    GEE_VECTOR = {
        "layer_id": 1,
        "layer_name": "vec",
        "path": "projects/p/assets/vec",
        "workspace": "ws",
        "style_name": None,
        "layer_type": "vector",
        "state": "s",
        "district": "d",
        "block": "b",
    }
    GEE_RASTER = {**GEE_VECTOR, "layer_id": 2, "layer_name": "ras", "layer_type": "raster"}

    def setUp(self):
        # asset type lookup falls back to the dataset type, without hitting GEE
        patcher = patch("ee.data.getAsset", side_effect=RuntimeError("no gee"))
        patcher.start()
        self.addCleanup(patcher.stop)

    @patch("computing.tasks.publish_gee_layer.apply_async")
    @patch("computing.pending_geoserver_sync._sync_boundaries")
    @patch("utilities.gee_utils.gcs_file_exists")
    @patch("utilities.gee_utils.is_gee_asset_exists")
    @patch("utilities.gee_utils.ee_initialize")
    @patch("computing.pending_geoserver_sync.fetch_unsynced_layers")
    def test_gee_layers_reported_in_gee_section(
        self, fetch, ee_init, asset_exists, gcs_exists, _boundaries, apply_async
    ):
        from computing.pending_geoserver_sync import queue_unsynced_layers

        apply_async.return_value.id = "t"
        asset_exists.return_value = True
        gcs_exists.return_value = False
        fetch.side_effect = lambda *a: (
            [] if a[4] == "local" else [self.GEE_VECTOR, self.GEE_RASTER]
        )

        summary = queue_unsynced_layers()

        self.assertEqual(summary["found"], 0)
        gee = summary["gee"]
        self.assertEqual(gee["vector"], {"found": 1, "queued": 1, "skipped": 0})
        self.assertEqual(gee["raster"], {"found": 1, "queued": 0, "skipped": 1})
        self.assertEqual(gee["skipped_layers"][0]["reason"], "no gcs file")
        self.assertEqual(apply_async.call_count, 1)
        self.assertEqual(apply_async.call_args.kwargs["queue"], "geoserver")

    @patch("computing.tasks.publish_gee_layer.apply_async")
    @patch("utilities.gee_utils.is_gee_asset_exists", return_value=False)
    @patch("utilities.gee_utils.ee_initialize")
    @patch("computing.pending_geoserver_sync.fetch_unsynced_layers")
    def test_missing_gee_asset_is_skipped_and_dry_run_queues_nothing(
        self, fetch, _ee, _exists, apply_async
    ):
        from computing.pending_geoserver_sync import queue_unsynced_layers

        fetch.return_value = [self.GEE_VECTOR]

        summary = queue_unsynced_layers(source="gee", dry_run=True)

        self.assertEqual(summary["gee"]["skipped_layers"][0]["reason"], "gee asset missing")
        apply_async.assert_not_called()

    @patch("computing.utils.update_layer_sync_status")
    @patch("utilities.gee_utils.sync_raster_gcs_to_geoserver")
    def test_publish_gee_raster_updates_flag_on_success(self, push, update_sync):
        from computing.tasks import publish_gee_layer

        push.return_value = "File response: ok"

        publish_gee_layer.run(
            layer_type="raster",
            asset_id="projects/p/assets/ras",
            layer_name="ras",
            workspace="ws",
            state="s",
            layer_id=2,
        )

        update_sync.assert_called_once_with(
            layer_id=2, sync_to_geoserver=True, is_stac_specs_generated=False
        )

    @patch("utilities.gee_utils.sync_raster_gcs_to_geoserver")
    def test_publish_gee_skipped_result_raises(self, push):
        from computing.tasks import publish_gee_layer

        push.return_value = {"skipped": True}

        with self.assertRaises(GeoServerPublishError):
            publish_gee_layer.run(
                layer_type="raster",
                asset_id="a",
                layer_name="ras",
                workspace="ws",
                state="s",
                layer_id=2,
            )


class ForceSyncTests(SimpleTestCase):
    @patch("computing.pending_geoserver_sync._sync_boundaries")
    @patch("computing.pending_geoserver_sync.fetch_unsynced_layers", return_value=[])
    def test_force_is_passed_to_both_sources(self, fetch, _boundaries):
        from computing.pending_geoserver_sync import queue_unsynced_layers

        summary = queue_unsynced_layers(force=True, dry_run=True)

        self.assertTrue(summary["force"])
        self.assertEqual([c.args[-1] for c in fetch.call_args_list], [True, True])

    def test_command_requires_yes_for_force(self):
        from django.core.management.base import CommandError
        from django.core.management import call_command

        with self.assertRaises(CommandError):
            call_command("sync_pending_geoserver", force=True)


class RelativePathAndRoutingTests(SimpleTestCase):
    def _entry(self, **kw):
        base = {
            "layer_id": 1,
            "layer_name": "kullu_kullu_cluster",
            "path": "data/terrain/terrain_clusters_local/himachal_pradesh/kullu/kullu/kullu_kullu_cluster.gpkg",
            "workspace": "terrain",
            "style_name": None,
            "layer_type": "vector",
            "state": "s",
            "district": "d",
            "block": "b",
        }
        base.update(kw)
        return base

    def test_relative_path_resolved_under_data_dir(self):
        import tempfile
        from pathlib import Path

        from computing import config_loader
        from computing.pending_geoserver_sync import _publish_args

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "terrain" / "x.gpkg"
            target.parent.mkdir()
            target.touch()
            with patch.object(config_loader, "DATA_DIR", Path(tmp)):
                args = _publish_args(self._entry(path="data/terrain/x.gpkg"))
        self.assertEqual(args["path"], str(Path(tmp) / "terrain" / "x"))
        self.assertEqual(args["file_type"], "gpkg")

    def test_missing_relative_file_is_unusable(self):
        from computing.pending_geoserver_sync import _publish_args

        self.assertIsNone(_publish_args(self._entry(path="data/nope/none.gpkg")))

    def test_soil_health_raster_expands_per_nutrient(self):
        from computing.pending_geoserver_sync import _expand_soil_health_raster

        entry = self._entry(
            layer_name="kullu_kullu_soil_health_raster",
            workspace="soil_health_raster",
            layer_type="raster",
            path="data/soil_health/h/k/k/kullu_kullu_soil_health_raster_OC_OLM.tif",
        )
        out = _expand_soil_health_raster(entry)
        self.assertEqual(
            [e["layer_name"] for e in out],
            [f"kullu_kullu_soil_health_raster_{n}" for n in ("N", "K", "P", "OC", "OC_OLM")],
        )
        self.assertTrue(out[2]["path"].endswith("kullu_kullu_soil_health_raster_P.tif"))
        self.assertEqual([e["layer_id"] for e in out], [None] * 4 + [1])

    @patch("computing.tasks.publish_gee_layer.apply_async")
    @patch("computing.tasks.publish_local_layer.apply_async")
    @patch("computing.pending_geoserver_sync._sync_boundaries")
    @patch("utilities.gee_utils.is_gee_asset_exists", return_value=True)
    @patch("utilities.gee_utils.ee_initialize")
    @patch("computing.pending_geoserver_sync.fetch_unsynced_layers")
    def test_locally_flagged_gee_asset_goes_to_gee_publish(
        self, fetch, _ee, _exists, _b, local_apply, gee_apply
    ):
        from computing.pending_geoserver_sync import queue_unsynced_layers

        gee_apply.return_value.id = "t"
        swb = self._entry(
            layer_name="surface_waterbodies_kullu_kullu",
            workspace="swb",
            path="projects/p/assets/swb4_kullu_kullu",
        )
        fetch.side_effect = lambda *a: [swb] if a[4] == "local" else []

        summary = queue_unsynced_layers()

        self.assertEqual(summary["found"], 0)
        self.assertEqual(summary["gee"]["queued"], 1)
        gee_apply.assert_called_once()
        local_apply.assert_not_called()


class GeeLayerKindTests(SimpleTestCase):
    ENTRY = {
        "layer_id": 3,
        "layer_name": "stream_order_x_raster",
        "path": "projects/p/assets/stream_order_x_raster",
        "workspace": "stream_order",
        "style_name": "default_style",
        "layer_type": "vector",  # shared dataset says vector
        "state": "s",
        "district": "d",
        "block": "b",
    }

    @patch("computing.tasks.publish_gee_layer.apply_async")
    @patch("utilities.gee_utils.gcs_file_exists", return_value=True)
    @patch("utilities.gee_utils.ee_initialize")
    @patch("ee.data.getAsset", return_value={"type": "IMAGE"})
    @patch("computing.pending_geoserver_sync.fetch_unsynced_layers")
    def test_image_asset_published_as_raster_despite_vector_dataset(
        self, fetch, _get, _ee, _gcs, apply_async
    ):
        from computing.pending_geoserver_sync import queue_unsynced_layers

        apply_async.return_value.id = "t"
        fetch.side_effect = lambda *a: [self.ENTRY] if a[4] == "gee" else []

        summary = queue_unsynced_layers(source="gee", sync_boundaries=False)

        self.assertEqual(summary["gee"]["raster"]["queued"], 1)
        self.assertEqual(summary["gee"]["vector"]["found"], 0)
        self.assertEqual(apply_async.call_args.kwargs["kwargs"]["layer_type"], "raster")

    @patch("utilities.gee_utils.ee_initialize", side_effect=RuntimeError("no gee"))
    def test_falls_back_to_name_suffix_when_asset_unreadable(self, _ee):
        from computing.pending_geoserver_sync import _gee_layer_kind

        self.assertEqual(_gee_layer_kind(self.ENTRY, None, [False]), "raster")
        vec = {**self.ENTRY, "layer_name": "stream_order_x_vector"}
        self.assertEqual(_gee_layer_kind(vec, None, [False]), "vector")


class GpkgPipelineSyncTests(SimpleTestCase):
    def _entries(self, tmp):
        import os

        main = os.path.join(tmp, "facilities_k_k.gpkg")
        points = os.path.join(tmp, "facilities_k_k.facility_points.gpkg")
        for f in (main, points):
            open(f, "w").close()
        base = {
            "workspace": "facilities_proximity",
            "style_name": None,
            "layer_type": "vector",
            "state": "s",
            "district": "d",
            "block": "b",
        }
        return [
            {**base, "layer_id": 1, "layer_name": "facilities_k_k", "path": main},
            {
                **base,
                "layer_id": 2,
                "layer_name": "facilities_k_k_tehsil_facility_collection",
                "path": points,
            },
            {
                **base,
                "layer_id": 3,
                "layer_name": "facilities_k_k_village_nearest_facility_collection",
                "path": points,
            },
        ]

    @patch("computing.tasks.publish_local_layer.apply_async")
    def test_facility_point_layers_share_one_task(self, apply_async):
        import tempfile

        from computing.pending_geoserver_sync import _queue_local_entries

        apply_async.return_value.id = "t"
        with tempfile.TemporaryDirectory() as tmp:
            summary = _queue_local_entries(self._entries(tmp), dry_run=False)

        self.assertEqual(summary["queued"], 3)
        self.assertEqual(apply_async.call_count, 2)
        kwargs = [c.kwargs["kwargs"] for c in apply_async.call_args_list]
        main = next(k for k in kwargs if k["layer_name"] == "facilities_k_k")
        points = next(k for k in kwargs if k["layer_name"] != "facilities_k_k")
        self.assertEqual(main["gpkg_layers"], {"facilities_k_k": None})
        self.assertEqual(points["store_name"], "facilities_k_k_facility_points")
        self.assertEqual(
            points["gpkg_layers"],
            {
                "facilities_k_k_tehsil_facility_collection": "tehsil_facility_collection",
                "facilities_k_k_village_nearest_facility_collection": "village_nearest_facility_collection",
            },
        )
        self.assertEqual(points["extra_layer_ids"], [3])

    @patch("computing.utils.update_layer_sync_status", return_value=1)
    @patch("utilities.pipelines.publish.publish_gpkg_layers")
    def test_publish_local_layer_publishes_gpkg_tables_and_flips_all_flags(
        self, publish, update_sync
    ):
        from computing.tasks import publish_local_layer

        publish_local_layer.run(
            layer_type="vector",
            path="/x/f.facility_points",
            layer_name="a",
            workspace="ws",
            file_type="gpkg",
            layer_id=2,
            gpkg_layers={"a": "t1", "b": "t2"},
            store_name="f_facility_points",
            extra_layer_ids=[3],
        )

        publish.assert_called_once()
        self.assertEqual(publish.call_args.args[0], "/x/f.facility_points.gpkg")
        self.assertEqual(publish.call_args.kwargs["store_name"], "f_facility_points")
        self.assertEqual(
            [c.kwargs["layer_id"] for c in update_sync.call_args_list], [2, 3]
        )
