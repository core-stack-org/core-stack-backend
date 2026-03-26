from __future__ import annotations

import os
import unittest

from computing.misc import internal_api_initialisation_test as init_test


class InternalApiInitialisationHelperTests(unittest.TestCase):
    def test_sample_value_handles_numeric_route_names(self) -> None:
        self.assertEqual(init_test.sample_value("project_id"), "1")
        self.assertEqual(init_test.sample_value("pk"), "1")
        self.assertEqual(init_test.sample_value("uuid"), "sample-uuid")

    def test_normalize_path_cleans_regex_fragments(self) -> None:
        raw_path = r"^api/v1/items/(?P<item_id>[^/.]+)/(?P<format>[a-z0-9]+)/$"
        self.assertEqual(init_test.normalize_path(raw_path), "/api/v1/items/1/json/")

    def test_build_next_step_guidance_flags_missing_admin_boundary_first(self) -> None:
        result = init_test.build_next_step_guidance(
            require_gee=False,
            sample=None,
            auth_result=init_test.CheckResult("PASS", "jwt-auth", "ok"),
            gee_probe_result=init_test.CheckResult("WARN", "gee-probe", "skip"),
            gcs_upload_result=init_test.CheckResult("WARN", "gcs-upload-probe", "skip"),
            geoserver_result=init_test.CheckResult("WARN", "geoserver-probe", "skip"),
            admin_boundary_result=init_test.CheckResult("FAIL", "admin-boundary-compute", "missing"),
            first_api_result=init_test.CheckResult("WARN", "first-computing-api", "skip"),
        )

        self.assertEqual(result.level, "FAIL")
        self.assertIn("admin boundary", result.detail)

    def test_build_next_step_guidance_reports_ready_state(self) -> None:
        sample = init_test.SampleLocation(
            state="assam",
            district="baksa",
            block="tamulpur",
            source_file=init_test.ROOT_DIR / "data" / "admin-boundary" / "input" / "assam" / "baksa.geojson",
        )
        result = init_test.build_next_step_guidance(
            require_gee=True,
            sample=sample,
            auth_result=init_test.CheckResult("PASS", "jwt-auth", "ok"),
            gee_probe_result=init_test.CheckResult("PASS", "gee-probe", "ok"),
            gcs_upload_result=init_test.CheckResult("PASS", "gcs-upload-probe", "ok"),
            geoserver_result=init_test.CheckResult("PASS", "geoserver-probe", "ok"),
            admin_boundary_result=init_test.CheckResult("PASS", "admin-boundary-compute", "ok"),
            first_api_result=init_test.CheckResult("PASS", "first-computing-api", "ok"),
        )

        self.assertEqual(result.level, "PASS")
        self.assertIn("POST /api/v1/generate_block_layer/", result.detail)
        self.assertIn("state=assam", result.detail)

    def test_low_thread_runtime_defaults_are_pinned(self) -> None:
        for env_name in (
            "OPENBLAS_NUM_THREADS",
            "OMP_NUM_THREADS",
            "MKL_NUM_THREADS",
            "NUMEXPR_NUM_THREADS",
            "VECLIB_MAXIMUM_THREADS",
            "BLIS_NUM_THREADS",
            "GOTO_NUM_THREADS",
        ):
            self.assertEqual(os.environ.get(env_name), "1")
