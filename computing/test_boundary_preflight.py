import ast
from pathlib import Path
from unittest.mock import patch

from django.test import SimpleTestCase
from rest_framework.decorators import (
    api_view,
    authentication_classes,
    permission_classes,
)
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.test import APIRequestFactory

from computing import local_compute_helper as helper
from computing.bulk_layer_generation import run_pipeline
from computing.layer_dependency.layer_generation_in_order import layer_generate_map
from utilities.layer_generation_mode import require_local_tehsil_boundaries

PREPARE = "computing.local_compute_helper.prepare_local_tehsil_boundaries"
LOCATION = {"state": "Bihar", "district": "Banka", "block": "Banka"}


class PrepareLocalTehsilBoundariesTests(SimpleTestCase):
    @patch.object(helper, "sync_tehsil_boundaries_to_geoserver")
    @patch.object(helper, "ensure_tehsil_watershed")
    @patch.object(helper, "_resolve_local_admin_boundary_file")
    def test_reports_every_missing_boundary_before_syncing(self, admin, mws, sync):
        admin.side_effect = FileNotFoundError("admin missing")
        mws.side_effect = FileNotFoundError("mws missing")

        with self.assertRaises(helper.TehsilBoundaryError) as ctx:
            helper.prepare_local_tehsil_boundaries("Bihar", "Banka", "Banka")

        self.assertEqual(ctx.exception.stage, "boundary_check")
        self.assertIn("admin missing", str(ctx.exception))
        self.assertIn("mws missing", str(ctx.exception))
        sync.assert_not_called()

    @patch.object(helper, "sync_tehsil_boundaries_to_geoserver")
    @patch.object(helper, "ensure_tehsil_watershed")
    @patch.object(helper, "_resolve_local_admin_boundary_file")
    def test_geoserver_failure_is_boundary_sync_stage(self, _admin, _mws, sync):
        sync.side_effect = RuntimeError("geoserver down")

        with self.assertRaises(helper.TehsilBoundaryError) as ctx:
            helper.prepare_local_tehsil_boundaries("Bihar", "Banka", "Banka")

        self.assertEqual(ctx.exception.stage, "boundary_sync")
        self.assertIn("geoserver down", str(ctx.exception))

    @patch.object(helper, "sync_tehsil_boundaries_to_geoserver")
    @patch.object(helper, "ensure_tehsil_watershed")
    @patch.object(helper, "_resolve_local_admin_boundary_file")
    def test_syncs_when_both_boundaries_exist(self, _admin, _mws, sync):
        helper.prepare_local_tehsil_boundaries("Bihar", "Banka", "Banka")
        sync.assert_called_once_with("Bihar", "Banka", "Banka", overwrite=False)


class RequireLocalTehsilBoundariesTests(SimpleTestCase):
    def setUp(self):
        self.calls = []

        def view(request):
            self.calls.append(request.data)
            return Response({"Success": "initiated"})

        def as_api_view(func):
            func = authentication_classes([])(func)
            func = permission_classes([AllowAny])(func)
            return api_view(["POST"])(func)

        self.view = as_api_view(require_local_tehsil_boundaries()(view))
        self.local_view = as_api_view(
            require_local_tehsil_boundaries(default_compute="local")(view)
        )
        self.factory = APIRequestFactory()

    def _post(self, view, **data):
        return view(self.factory.post("/", data, format="json"))

    @patch(PREPARE)
    def test_local_request_prepares_boundaries_before_view(self, prepare):
        response = self._post(self.view, compute="local", **LOCATION)
        self.assertEqual(response.status_code, 200)
        prepare.assert_called_once_with("Bihar", "Banka", "Banka")
        self.assertEqual(len(self.calls), 1)

    @patch(PREPARE)
    def test_missing_boundary_returns_400_without_running_view(self, prepare):
        prepare.side_effect = helper.TehsilBoundaryError("boundary_check", "missing")
        response = self._post(self.view, compute="local", **LOCATION)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data["stage"], "boundary_check")
        self.assertFalse(response.data["layer_generated"])
        self.assertEqual(self.calls, [])

    @patch(PREPARE)
    def test_sync_failure_returns_502_without_running_view(self, prepare):
        prepare.side_effect = helper.TehsilBoundaryError("boundary_sync", "down")
        response = self._post(self.view, compute="local", **LOCATION)
        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.data["stage"], "boundary_sync")
        self.assertEqual(self.calls, [])

    @patch(PREPARE)
    def test_skips_gee_pan_india_and_incomplete_location(self, prepare):
        self._post(self.view, **LOCATION)
        self._post(self.view, compute="gee", **LOCATION)
        self._post(self.view, compute="local", pan_india="true", **LOCATION)
        self._post(self.view, compute="local", state="Bihar", district="Banka")
        prepare.assert_not_called()
        self.assertEqual(len(self.calls), 4)

    @patch(PREPARE)
    def test_local_default_applies_without_compute_field(self, prepare):
        self._post(self.local_view, **LOCATION)
        prepare.assert_called_once()


class PipelineBoundaryPreflightTests(SimpleTestCase):
    @patch("computing.bulk_layer_generation._task_registry")
    @patch(PREPARE)
    def test_run_pipeline_stops_before_runner_on_boundary_failure(
        self, prepare, task_registry
    ):
        runner = task_registry.return_value.__getitem__.return_value
        prepare.side_effect = helper.TehsilBoundaryError("boundary_check", "missing")
        task_registry.return_value = {"lulc_v3": runner}

        with self.assertRaises(helper.TehsilBoundaryError):
            run_pipeline("lulc_v3", LOCATION, compute="local")
        runner.assert_not_called()

    @patch("computing.layer_dependency.layer_generation_in_order.load_map_config")
    @patch(PREPARE)
    def test_layer_map_stops_before_any_layer_on_boundary_failure(
        self, prepare, load_map_config
    ):
        prepare.side_effect = helper.TehsilBoundaryError("boundary_sync", "down")

        result = layer_generate_map.run(
            map_order="map_1", gee_account_id=None, compute="local", **LOCATION
        )

        self.assertIn("boundary_sync failed", result)
        load_map_config.assert_not_called()


class LayerApiCoverageTests(SimpleTestCase):
    """Every tehsil layer API that can run locally must check boundaries first."""

    NOT_TEHSIL_SCOPED = {"et_download", "generate_ltp_stp", "generate_ltp_stp_change"}

    def test_local_capable_views_are_guarded(self):
        source = (Path(__file__).parent / "api.py").read_text()
        unguarded = []
        for node in ast.parse(source).body:
            if not isinstance(node, ast.FunctionDef) or node.name.startswith("_"):
                continue
            body = ast.get_source_segment(source, node)
            local_capable = (
                "_get_compute_mode(" in body
                or 'request.data.get("compute")' in body
                or "_generate_tehsil_hydrology(" in body
                or "_local.apply_async(" in body
                or "_local_task.apply_async(" in body
            )
            decorators = [ast.unparse(d) for d in node.decorator_list]
            guarded = any("require_local_tehsil_boundaries" in d for d in decorators)
            if local_capable and not guarded and node.name not in self.NOT_TEHSIL_SCOPED:
                unguarded.append(node.name)
        self.assertEqual(unguarded, [])
