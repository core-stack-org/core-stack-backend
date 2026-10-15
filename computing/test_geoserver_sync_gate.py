from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from computing import celery_task_logging
from computing.local_compute_helper import queue_local_raster_for_geoserver
from utilities import geoserver_sync_gate as gate

LOCATION = ("Maharashtra", "Pune", "Mulshi Paud")
ACTIVE = frozenset({gate._key(*LOCATION)})


def raster_call():
    return queue_local_raster_for_geoserver(
        file_path="/tmp/layer.tif", layer_name="layer", workspace="ws", layer_id=1
    )


@patch("utilities.geoserver_sync_gate.fetch_active_tehsil_keys", return_value=ACTIVE)
@patch("computing.tasks.publish_local_layer.apply_async")
class GateTests(SimpleTestCase):
    def test_requested_and_active_pushes(self, apply_async, _active):
        apply_async.return_value.id = "t"
        with gate.geoserver_sync_context(True, *LOCATION):
            response = raster_call()
        self.assertTrue(response["queued"])
        apply_async.assert_called_once()

    def test_requested_but_inactive_stays_local(self, apply_async, _active):
        with gate.geoserver_sync_context(True, "Maharashtra", "Pune", "Other"):
            response = raster_call()
        self.assertTrue(response["skipped"])
        self.assertEqual(response["status_code"], 202)
        apply_async.assert_not_called()

    def test_not_requested_stays_local_even_if_active(self, apply_async, _active):
        with gate.geoserver_sync_context(False, *LOCATION):
            response = raster_call()
        self.assertTrue(response["skipped"])
        apply_async.assert_not_called()

    def test_no_context_is_unchanged(self, apply_async, _active):
        apply_async.return_value.id = "t"
        response = raster_call()
        self.assertTrue(response["queued"])

    def test_inactive_when_active_lookup_fails(self, apply_async, active):
        active.side_effect = RuntimeError("prod down")
        self.assertFalse(gate.is_tehsil_active(*LOCATION))


class ParseBoolTests(SimpleTestCase):
    def test_values(self):
        self.assertTrue(gate.parse_bool("true"))
        self.assertTrue(gate.parse_bool("1"))
        self.assertFalse(gate.parse_bool(None))
        self.assertFalse(gate.parse_bool("false"))


class CeleryPropagationTests(SimpleTestCase):
    def test_context_round_trips_through_headers(self):
        headers = {}
        with gate.geoserver_sync_context(False, *LOCATION):
            celery_task_logging.propagate_geoserver_sync_gate(headers=headers)
        self.assertIn(gate.CELERY_HEADER, headers)
        self.assertIsNone(gate.get_context())

        task = SimpleNamespace(
            request=SimpleNamespace(**{gate.CELERY_HEADER: headers[gate.CELERY_HEADER]})
        )
        celery_task_logging.restore_geoserver_sync_gate(task_id="1", task=task)
        self.assertTrue(gate.should_skip_sync())
        celery_task_logging.clear_geoserver_sync_gate(task_id="1")
        self.assertIsNone(gate.get_context())

    def test_no_header_without_context(self):
        headers = {}
        celery_task_logging.propagate_geoserver_sync_gate(headers=headers)
        self.assertEqual(headers, {})


class ActiveSourceTests(SimpleTestCase):
    @override_settings(PROD_BACKEND_URL="https://prod.example")
    @patch("computing.bulk_layer_generation.get_active_locations_from_api")
    def test_uses_prod_api_when_configured(self, api):
        from computing.bulk_layer_generation import Location

        api.return_value = [Location(*LOCATION)]
        gate._active_cache["fetched_at"] = 0.0
        self.assertTrue(gate.is_tehsil_active("maharashtra", "pune", "mulshi-paud"))
        self.assertFalse(gate.is_tehsil_active("maharashtra", "pune", "nowhere"))
        api.assert_called_once()


class PublishArgsTests(SimpleTestCase):
    def test_vector_path_with_extension_is_split(self):
        import tempfile, os
        from computing.pending_geoserver_sync import _publish_args

        with tempfile.TemporaryDirectory() as tmp:
            gpkg = os.path.join(tmp, "layer.gpkg")
            open(gpkg, "w").close()
            args = _publish_args(
                {"path": gpkg, "workspace": "ws", "layer_name": "layer",
                 "layer_type": "vector", "layer_id": 7}
            )
        self.assertEqual(args["path"], gpkg[:-5])
        self.assertEqual(args["file_type"], "gpkg")
        self.assertEqual(args["layer_id"], 7)

    def test_gee_asset_or_missing_file_is_unusable(self):
        from computing.pending_geoserver_sync import _publish_args

        base = {"workspace": "ws", "layer_name": "layer", "layer_type": "raster"}
        self.assertIsNone(_publish_args({**base, "path": "projects/ee/asset"}))
        self.assertIsNone(_publish_args({**base, "path": "/nonexistent/x.tif"}))


class BoundaryGateTests(SimpleTestCase):
    @patch("computing.local_compute_helper.sync_tehsil_boundaries_to_geoserver")
    @patch("computing.local_compute_helper.ensure_tehsil_watershed")
    @patch("computing.local_compute_helper._resolve_local_admin_boundary_file")
    def test_boundaries_stay_local_when_gate_closed(self, _admin, _mws, sync):
        from computing.local_compute_helper import prepare_local_tehsil_boundaries

        with gate.geoserver_sync_context(False, *LOCATION):
            result = prepare_local_tehsil_boundaries(*LOCATION)
        self.assertEqual(result, {"skipped": True})
        sync.assert_not_called()

    @patch("computing.local_compute_helper.sync_tehsil_boundaries_to_geoserver")
    @patch("computing.local_compute_helper.ensure_tehsil_watershed")
    @patch("computing.local_compute_helper._resolve_local_admin_boundary_file")
    def test_boundaries_pushed_without_gate(self, _admin, _mws, sync):
        from computing.local_compute_helper import prepare_local_tehsil_boundaries

        prepare_local_tehsil_boundaries(*LOCATION)
        sync.assert_called_once()
