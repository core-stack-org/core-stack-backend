from unittest import TestCase
from unittest.mock import MagicMock, patch

import numpy as np
from django.urls import reverse
from rest_framework.test import APISimpleTestCase

from computing.soil_type.soil_type_local import (
    _aggregate_values,
    run_soil_type_local,
)


class SoilTypeAggregationTests(TestCase):
    def test_mean_ignores_nan_and_zero_nodata(self):
        spec = {"aggregation": "mean"}
        self.assertEqual(_aggregate_values([0, 1.0, 2.0, np.nan], spec), 1.5)

    def test_mode_is_decoded(self):
        spec = {
            "aggregation": "mode",
            "mapping": {1: "Coarse", 2: "Medium"},
            "zero_is_nodata": True,
        }
        self.assertEqual(_aggregate_values([0, 1, 2, 2, np.nan], spec), "Medium")

    def test_drainage_zero_is_a_valid_class(self):
        spec = {
            "aggregation": "mode",
            "mapping": {0: "Excessively drained", 1: "Well drained"},
            "zero_is_nodata": False,
        }
        self.assertEqual(
            _aggregate_values([0, 0, 1, np.nan], spec),
            "Excessively drained",
        )


class SoilTypePublicationTests(TestCase):
    def _run_with_mocks(self, geoserver_status):
        geometries = MagicMock()
        result = MagicMock()
        patchers = (
            patch(
                "computing.soil_type.soil_type_local.load_precomputed_watersheds",
                return_value=(geometries, "/tmp/watersheds.gpkg"),
            ),
            patch(
                "computing.soil_type.soil_type_local."
                "compute_soil_properties_for_geometries",
                return_value=result,
            ),
            patch(
                "computing.soil_type.soil_type_local.build_output_vector_path",
                return_value="/tmp/soil_type_test.gpkg",
            ),
            patch(
                "computing.soil_type.soil_type_local.write_vector_output",
                return_value="/tmp/soil_type_test.gpkg",
            ),
            patch(
                "computing.soil_type.soil_type_local.queue_local_vector_for_geoserver",
                return_value={"status_code": geoserver_status},
            ),
            patch(
                "computing.soil_type.soil_type_local.save_layer_info_to_db",
                return_value=42,
            ),
        )
        mocks = [patcher.start() for patcher in patchers]
        self.addCleanup(lambda: [patcher.stop() for patcher in reversed(patchers)])

        success = run_soil_type_local(
            state="Puducherry",
            district="Puducherry",
            block="Bahur",
        )
        return success, mocks

    def test_success_queues_publication_with_layer_metadata(self):
        success, mocks = self._run_with_mocks(202)
        geoserver_mock = mocks[4]
        save_layer_mock = mocks[5]

        self.assertTrue(success)
        geoserver_mock.assert_called_once_with(
            path="/tmp/soil_type_test",
            workspace="soil_type",
            layer_name="soil_type_puducherry_bahur",
            file_type="gpkg",
            layer_id=42,
        )
        self.assertEqual(
            save_layer_mock.call_args.kwargs["dataset_name"],
            "Soil Type",
        )
        self.assertEqual(
            save_layer_mock.call_args.kwargs["layer_name"],
            "soil_type_puducherry_bahur",
        )
    def test_failed_queueing_keeps_unsynced_metadata(self):
        success, mocks = self._run_with_mocks(500)

        self.assertFalse(success)
        mocks[5].assert_called_once()


class SoilTypeAPITests(APISimpleTestCase):
    def setUp(self):
        self.client.force_authenticate(user=MagicMock(is_authenticated=True))

    @patch("computing.api.generate_soil_type_local.apply_async")
    def test_generate_soil_type_queues_local_task(self, apply_async):
        response = self.client.post(
            reverse("generate_soil_type"),
            {
                "state": "Puducherry",
                "district": "Puducherry",
                "block": "Bahur",
                "compute": "local",
            },
            format="json",
        )

        self.assertEqual(response.status_code, 200)
        apply_async.assert_called_once_with(
            kwargs={
                "state": "puducherry",
                "district": "puducherry",
                "block": "bahur",
            },
            queue="nrm",
        )

    @patch("computing.api.generate_soil_type_local.apply_async")
    def test_generate_soil_type_rejects_missing_location(self, apply_async):
        response = self.client.post(
            reverse("generate_soil_type"),
            {"state": "Puducherry", "compute": "local"},
            format="json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {"Exception": "Missing required fields: district, block"},
        )
        apply_async.assert_not_called()

    @patch("computing.api.generate_soil_type_local.apply_async")
    def test_generate_soil_type_rejects_non_local_compute(self, apply_async):
        response = self.client.post(
            reverse("generate_soil_type"),
            {
                "state": "Puducherry",
                "district": "Puducherry",
                "block": "Bahur",
                "compute": "gee",
            },
            format="json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {"Exception": "Soil type only supports compute=local"},
        )
        apply_async.assert_not_called()
