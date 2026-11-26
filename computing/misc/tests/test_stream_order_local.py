import tempfile
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from django.test import SimpleTestCase
from rasterio.transform import from_origin
from rest_framework.test import APIRequestFactory, force_authenticate
from shapely.geometry import box

from computing import api as computing_api
from computing.bulk_layer_generation import (
    LOCAL_TASK_DATASETS,
    get_regeneration_dataset_names,
)
from computing.layer_dependency.layer_generation_in_order import (
    GEE_TASK_REGISTRY,
    LOCAL_TASK_REGISTRY,
)
from computing.misc import stream_order_local_compute as sol
from computing.misc.stream_order import generate_stream_order
from stats_generator.utils import create_excel_for_stream_order


def _write_test_raster(path, array, transform, crs="EPSG:4326", nodata=0):
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=array.shape[0],
        width=array.shape[1],
        count=1,
        dtype=array.dtype,
        crs=crs,
        transform=transform,
        nodata=nodata,
    ) as dst:
        dst.write(array, 1)
    return path


class StreamOrderLocalComputeTests(SimpleTestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)

    def _make_watersheds(self, crs="EPSG:4326"):
        if crs == "EPSG:4326":
            # 10x10 raster at origin (86.0, 25.0) with 0.001 deg pixel size:
            # x in [86.000, 86.010], y in [24.990, 25.000]
            # Poly 1 covers rows 0..3, cols 0..4 (4x5 = 20 pixels)
            # Poly 2 covers rows 5..8, cols 0..4 (4x5 = 20 pixels)
            poly1 = box(86.000, 24.996, 86.005, 25.000)
            poly2 = box(86.000, 24.991, 86.005, 24.995)
        else:
            # Projected 10x10 raster at origin (0, 1000) with 100m pixels
            poly1 = box(0.0, 600.0, 500.0, 1000.0)
            poly2 = box(0.0, 100.0, 500.0, 500.0)
        return gpd.GeoDataFrame(
            {
                "uid": ["1_1", "1_2"],
                "area_in_ha": [100.0, 100.0],
                "geometry": [poly1, poly2],
            },
            crs=crs,
        )

    def test_end_to_end_tehsil_raster_and_vector_percentages(self):
        # 10x10 raster in projected EPSG:3857 so every pixel has equal area -> exact percentages
        arr = np.zeros((10, 10), dtype=np.int16)
        # Inside poly1 (rows 0..3, cols 0..4 => 20 pixels):
        # 4 pixels of order 1 (20%), 2 pixels of order 3 (10%), 14 pixels nodata=0
        arr[0, 0:4] = 1
        arr[1, 0:2] = 3
        # Inside poly2 (rows 5..8, cols 0..4 => 20 pixels):
        # no stream pixels (all 0%!)

        raster_path = _write_test_raster(
            self.tmp / "Pan_India_stream_order_raster.tif",
            arr,
            transform=from_origin(0.0, 1000.0, 100.0, 100.0),
            crs="EPSG:3857",
            nodata=0,
        )

        # Write precomputed tehsil MWS file in EPSG:4326 (reprojected from EPSG:3857)
        precomputed_dir = self.tmp / "tehsil_mws"
        mws_path = (
            precomputed_dir
            / "bihar"
            / "banka"
            / "banka"
            / "filtered_mws_banka_banka_uid.geojson"
        )
        mws_path.parent.mkdir(parents=True, exist_ok=True)
        self._make_watersheds(crs="EPSG:3857").to_crs("EPSG:4326").to_file(
            mws_path, driver="GeoJSON"
        )

        out_root = self.tmp / "out_stream_order"

        with (
            patch.object(sol, "LOCAL_STREAM_ORDER_OUTPUT", out_root),
            patch.object(sol, "PAN_INDIA_STREAM_ORDER_PATH", raster_path),
            patch.object(
                sol, "save_layer_info_to_db", side_effect=[101, 102]
            ) as mock_save_db,
            patch.object(
                sol,
                "queue_local_raster_for_geoserver",
                return_value={"status_code": 202},
            ) as mock_queue_raster,
            patch.object(
                sol,
                "queue_local_vector_for_geoserver",
                return_value={"status_code": 202},
            ) as mock_queue_vector,
        ):
            ok = sol.generate_stream_order_local.run(
                state="Bihar",
                district="Banka",
                block="Banka",
                gee_account_id="ignored",
                precomputed_roi_dir=precomputed_dir,
            )

        self.assertTrue(ok)

        expected_raster = (
            out_root / "bihar" / "banka" / "banka" / "stream_order_banka_banka_raster.tif"
        )
        expected_vector = (
            out_root / "bihar" / "banka" / "banka" / "stream_order_banka_banka_vector.gpkg"
        )
        self.assertTrue(expected_raster.is_file())
        self.assertTrue(expected_vector.is_file())

        # Verify DB metadata calls
        self.assertEqual(mock_save_db.call_count, 2)
        raster_db_kwargs = mock_save_db.call_args_list[0].kwargs
        vector_db_kwargs = mock_save_db.call_args_list[1].kwargs
        self.assertEqual(raster_db_kwargs["layer_name"], "stream_order_banka_banka_raster")
        self.assertEqual(raster_db_kwargs["dataset_name"], "Stream Order")
        self.assertEqual(raster_db_kwargs["misc"], {"is_generated_locally": True})
        self.assertEqual(vector_db_kwargs["layer_name"], "stream_order_banka_banka_vector")
        self.assertEqual(vector_db_kwargs["dataset_name"], "Stream Order")
        self.assertEqual(vector_db_kwargs["misc"], {"is_generated_locally": True})

        # Verify GeoServer queueing
        mock_queue_raster.assert_called_once_with(
            file_path=str(expected_raster),
            layer_name="stream_order_banka_banka_raster",
            workspace="stream_order",
            style_name="stream_order",
            layer_id=101,
        )
        mock_queue_vector.assert_called_once_with(
            path=str(expected_vector.with_suffix("")),
            layer_name="stream_order_banka_banka_vector",
            workspace="stream_order",
            file_type="gpkg",
            layer_id=102,
        )

        # Verify output vector schema & exact percentages
        result_gdf = gpd.read_file(expected_vector, layer="stream_order_banka_banka_vector")
        self.assertEqual(result_gdf.crs.to_epsg(), 4326)
        for order in range(1, 12):
            self.assertIn(str(order), result_gdf.columns)

        row1 = result_gdf[result_gdf["uid"] == "1_1"].iloc[0]
        self.assertAlmostEqual(row1["1"], 20.0, places=5)
        self.assertAlmostEqual(row1["3"], 10.0, places=5)
        for order in (2, 4, 5, 6, 7, 8, 9, 10, 11):
            self.assertAlmostEqual(row1[str(order)], 0.0, places=5)

        row2 = result_gdf[result_gdf["uid"] == "1_2"].iloc[0]
        for order in range(1, 12):
            self.assertAlmostEqual(row2[str(order)], 0.0, places=5)

        # Verify compatibility with stats_generator.utils.create_excel_for_stream_order
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {"properties": row.drop("geometry").to_dict()}
                for _, row in result_gdf.iterrows()
            ],
        }
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            create_excel_for_stream_order(geojson, writer)
        buf.seek(0)
        sheet = pd.read_excel(buf, sheet_name="stream_order")
        self.assertEqual(sheet["UID"].tolist(), ["1_1", "1_2"])
        self.assertAlmostEqual(sheet.loc[0, "order_1_area_percent"], 20.0, places=5)
        self.assertAlmostEqual(sheet.loc[0, "order_3_area_percent"], 10.0, places=5)
        self.assertAlmostEqual(sheet.loc[1, "order_1_area_percent"], 0.0, places=5)

    def test_geographic_crs_and_custom_roi(self):
        arr = np.zeros((10, 10), dtype=np.int16)
        arr[0:2, 0:5] = 2  # 10 out of 20 pixels in poly1 (~50%)
        arr[2, 0:5] = 11  # 5 out of 20 pixels in poly1 (~25%)

        raster_path = _write_test_raster(
            self.tmp / "stream_order_4326.tif",
            arr,
            transform=from_origin(86.0, 25.0, 0.001, 0.001),
            crs="EPSG:4326",
            nodata=0,
        )
        roi_path = self.tmp / "custom_roi.geojson"
        self._make_watersheds(crs="EPSG:4326").to_file(roi_path, driver="GeoJSON")
        out_root = self.tmp / "out_custom"

        with (
            patch.object(sol, "LOCAL_STREAM_ORDER_OUTPUT", out_root),
            patch.object(sol, "PAN_INDIA_STREAM_ORDER_PATH", raster_path),
            patch.object(sol, "save_layer_info_to_db") as mock_save_db,
        ):
            ok = sol.generate_stream_order_local.run(
                asset_suffix="my_project_42",
                roi_path=str(roi_path),
                push_to_geoserver=False,
                sync_layer_metadata=True,
            )

        self.assertTrue(ok)
        mock_save_db.assert_not_called()
        out_vector = out_root / "custom" / "stream_order_my_project_42_vector.gpkg"
        self.assertTrue(out_vector.is_file())

        gdf = gpd.read_file(out_vector, layer="stream_order_my_project_42_vector")
        row1 = gdf[gdf["uid"] == "1_1"].iloc[0]
        self.assertAlmostEqual(row1["2"], 50.0, delta=0.1)
        self.assertAlmostEqual(row1["11"], 25.0, delta=0.1)
        self.assertAlmostEqual(row1["1"], 0.0, places=5)

    def test_registry_and_bulk_dataset_registration(self):
        self.assertIs(LOCAL_TASK_REGISTRY["stream_order"], sol.generate_stream_order_local)
        self.assertIs(
            LOCAL_TASK_REGISTRY["generate_stream_order"], sol.generate_stream_order_local
        )
        self.assertIs(GEE_TASK_REGISTRY["stream_order"], generate_stream_order)
        self.assertIs(GEE_TASK_REGISTRY["generate_stream_order"], generate_stream_order)
        self.assertEqual(
            LOCAL_TASK_DATASETS[sol.generate_stream_order_local.name], ("Stream Order",)
        )
        self.assertEqual(get_regeneration_dataset_names("stream_order"), ("Stream Order",))

    @patch("computing.local_compute_helper.prepare_local_tehsil_boundaries")
    @patch.object(computing_api, "generate_stream_order_local_task")
    @patch.object(computing_api, "generate_stream_order")
    def test_api_routes_compute_local_and_gee(self, mock_gee, mock_local, mock_prepare):
        factory = APIRequestFactory()
        user = MagicMock(is_authenticated=True)
        payload = {"state": "Bihar", "district": "Banka", "block": "Banka"}

        def _post(body):
            req = factory.post("/stream_order/", body, format="json")
            force_authenticate(req, user=user)
            return computing_api.stream_order(req)

        resp_gee = _post(payload)
        self.assertEqual(resp_gee.status_code, 200)
        mock_gee.apply_async.assert_called_once()
        mock_local.apply_async.assert_not_called()
        mock_prepare.assert_not_called()

        mock_gee.reset_mock()
        resp_local = _post({**payload, "compute": "local"})
        self.assertEqual(resp_local.status_code, 200)
        mock_prepare.assert_called_once_with("Bihar", "Banka", "Banka")
        mock_local.apply_async.assert_called_once_with(
            kwargs={
                "state": "bihar",
                "district": "banka",
                "block": "banka",
                "gee_account_id": None,
            },
            queue="nrm",
        )
        mock_gee.apply_async.assert_not_called()

        resp_invalid = _post({**payload, "compute": "invalid"})
        self.assertEqual(resp_invalid.status_code, 400)
