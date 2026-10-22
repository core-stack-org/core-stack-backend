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
