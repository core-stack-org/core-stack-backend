from unittest import TestCase
from unittest.mock import MagicMock, patch

import numpy as np

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
                "computing.soil_type.soil_type_local.push_local_vector_to_geoserver",
                return_value={"status_code": geoserver_status},
            ),
            patch(
                "computing.soil_type.soil_type_local.save_layer_info_to_db",
                return_value=42,
            ),
            patch(
                "computing.soil_type.soil_type_local.update_layer_sync_status"
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

    def test_success_creates_workspace_and_syncs_cloud_metadata(self):
        success, mocks = self._run_with_mocks(201)
        geoserver_mock = mocks[4]
        save_layer_mock = mocks[5]
        update_sync_mock = mocks[6]

        self.assertTrue(success)
        geoserver_mock.assert_called_once_with(
            path="/tmp/soil_type_test",
            workspace="soil_type",
            layer_name="soil_type_puducherry_bahur",
            file_type="gpkg",
        )
        self.assertEqual(
            save_layer_mock.call_args.kwargs["dataset_name"],
            "Soil Type",
        )
        self.assertEqual(
            save_layer_mock.call_args.kwargs["layer_name"],
            "soil_type_puducherry_bahur",
        )
        update_sync_mock.assert_called_once_with(
            layer_id=42,
            sync_to_geoserver=True,
        )

    def test_failed_publication_does_not_update_cloud_metadata(self):
        success, mocks = self._run_with_mocks(500)

        self.assertFalse(success)
        mocks[5].assert_not_called()
        mocks[6].assert_not_called()
