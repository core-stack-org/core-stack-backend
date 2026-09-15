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
