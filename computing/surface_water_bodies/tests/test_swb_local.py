from inspect import signature
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, call, patch

import pandas as pd
from django.test import SimpleTestCase

from computing.layer_dependency.layer_generation_in_order import get_args, load_map_config
from computing.surface_water_bodies.swb_local import (
    _complete_swb_pipeline,
    _continue_swb_in_gee,
    _convert_area_columns_to_hectares,
    _delete_existing_swb_outputs,
    _final_layer_name,
    _layer_name,
    _restore_legacy_columns,
    generate_swb_layer as generate_swb_layer_local,
    run_swb_local,
)
from computing.surface_water_bodies.swb3 import (
    waterbody_catchment_streamorder_properties,
)
from computing.utils import generate_swb_layer_with_max_so_catchment
from utilities.constants import (
    CATCHMENT_AREA,
    PAN_INDIA_DRAINAGE_LINES_DATASET,
    STREAM_ORDER_ASSET,
)


class LocalSwbContinuationTests(SimpleTestCase):
    @patch("computing.surface_water_bodies.swb_local.get_gee_dir_path")
    @patch("computing.surface_water_bodies.swb_local.is_gee_asset_exists")
    @patch("computing.surface_water_bodies.swb_local.ee.data.deleteAsset")
    def test_overwrite_deletes_local_and_gee_outputs(
        self,
        delete_asset,
        asset_exists,
        get_gee_dir_path,
    ):
        get_gee_dir_path.return_value = "projects/example/state/district/block/"
        asset_exists.return_value = True

        with TemporaryDirectory() as output_dir:
            output_path = Path(output_dir) / "swb.gpkg"
            output_path.touch()

            _delete_existing_swb_outputs(
                output_path=output_path,
                swb2_asset_id=(
                    "projects/example/state/district/block/"
                    "swb2_district_block_local"
                ),
                asset_suffix="district_block",
                asset_folder_list=["state", "district", "block"],
                app_type="MWS",
            )

            self.assertFalse(output_path.exists())

        self.assertEqual(
            delete_asset.call_args_list,
            [
                call(
                    "projects/example/state/district/block/"
                    "swb4_district_block"
                ),
                call(
                    "projects/example/state/district/block/"
                    "swb3_district_block"
                ),
                call(
                    "projects/example/state/district/block/"
                    "swb2_district_block_local"
                ),
            ],
        )

    @patch("computing.surface_water_bodies.swb_local._complete_swb_pipeline")
    @patch("computing.surface_water_bodies.swb_local._push_local_swb_to_geoserver")
    @patch("computing.surface_water_bodies.swb_local.make_asset_public")
    @patch("computing.surface_water_bodies.swb_local.is_gee_asset_exists")
    @patch("computing.surface_water_bodies.swb_local._create_local_swb_output")
    @patch("computing.surface_water_bodies.swb_local.build_output_vector_path")
    @patch("computing.surface_water_bodies.swb_local._resolve_gee_asset_id")
    @patch("computing.surface_water_bodies.swb_local.gdf_to_ee_fc")
    @patch("computing.surface_water_bodies.swb_local.ee_initialize")
    @patch("computing.surface_water_bodies.swb_local._union_geometry")
    @patch("computing.surface_water_bodies.swb_local._resolve_roi_gdf")
    def test_existing_swb2_asset_recreates_and_syncs_missing_local_output(
        self,
        resolve_roi,
        union_geometry,
        ee_initialize,
        gdf_to_ee_fc,
        resolve_asset,
        build_output_path,
        create_local_output,
        asset_exists,
        make_public,
        push_swb2,
        complete_pipeline,
    ):
        roi_gdf = MagicMock()
        roi_gdf.__len__.return_value = 1
        resolve_roi.return_value = roi_gdf
        union_geometry.return_value.is_empty = False
        gdf_to_ee_fc.return_value = MagicMock()
        resolve_asset.return_value = (
            "swb2_bid_bid_local",
            "projects/example/swb2_bid_bid_local",
            ["maharashtra", "bid", "bid"],
        )
        output_path = Path("/tmp/nonexistent-swb2-output.gpkg")
        build_output_path.return_value = output_path
        create_local_output.return_value = (MagicMock(), str(output_path))
        asset_exists.return_value = True
        events = []
        push_swb2.side_effect = lambda **kwargs: events.append("swb2") or True
        complete_pipeline.side_effect = (
            lambda **kwargs: events.append("complete") or True
        )

        result = run_swb_local(
            state="Maharashtra",
            district="Bid",
            block="Bid",
            sync_layer_metadata=False,
            use_gee=True,
        )

        create_local_output.assert_called_once()
        push_swb2.assert_called_once_with(
            output_path=str(output_path),
            layer_name="surface_waterbodies_bid_bid",
        )
        complete_pipeline.assert_called_once()
        self.assertEqual(events, ["swb2", "complete"])
        self.assertTrue(result)

    def test_reconstructs_area_ored_before_converting_annual_areas(self):
        source = pd.DataFrame(
            {
                "area_2020": [500, 0],
                "k_2020": [83.3333, 0],
                "area_2021": [600, 240],
                "k_2021": [100, 20],
                "total_area_m2": [550, 1200],
            }
        )

        converted, _ = _convert_area_columns_to_hectares(source)

        self.assertAlmostEqual(converted.loc[0, "area_ored"], 0.06, places=6)
        self.assertAlmostEqual(converted.loc[1, "area_ored"], 0.12, places=6)
        self.assertEqual(converted["area_2020"].tolist(), [0.05, 0])
        self.assertEqual(converted["total_area"].tolist(), [0.055, 0.12])

    def test_preserves_existing_area_ored(self):
        source = pd.DataFrame(
            {
                "area_ored": [0.25],
                "area_2025": [500],
                "k_2025": [100],
            }
        )

        converted, _ = _convert_area_columns_to_hectares(source)

        self.assertEqual(converted.loc[0, "area_ored"], 0.25)

    def test_restores_legacy_annual_columns(self):
        source = pd.DataFrame(
            {
                "wb_id": ["waterbody-1"],
                "area_2018": [0.14],
                "k_2019": [20],
                "kr_2020": [30],
                "krz_2021": [40],
                "area_ored": [0.25],
            }
        )

        result = _restore_legacy_columns(source)

        self.assertEqual(
            list(result.columns),
            [
                "wb_id",
                "area_17-18",
                "k_18-19",
                "kr_19-20",
                "krz_20-21",
                "area_ored",
            ],
        )

    def test_swb_enrichment_defaults_to_pan_india_assets(self):
        raster_parameters = signature(
            generate_swb_layer_with_max_so_catchment
        ).parameters
        swb3_parameters = signature(
            waterbody_catchment_streamorder_properties
        ).parameters

        self.assertEqual(
            raster_parameters["stream_order_asset_id"].default,
            STREAM_ORDER_ASSET,
        )
        self.assertEqual(
            raster_parameters["catchment_area_asset_id"].default,
            CATCHMENT_AREA,
        )
        self.assertEqual(
            swb3_parameters["drainage_lines_asset_id"].default,
            PAN_INDIA_DRAINAGE_LINES_DATASET,
        )

    def test_geoserver_layers_have_no_local_suffix(self):
        expected_layer_name = "surface_waterbodies_dumka_jarmundi"
        self.assertEqual(
            _layer_name("dumka_jarmundi"),
            expected_layer_name,
        )
        self.assertEqual(
            _final_layer_name("dumka_jarmundi"),
            expected_layer_name,
        )

    def test_local_map_passes_gee_account_to_swb(self):
        args = get_args(
            iterator_name={
                "name": "generate_swb",
                "use_global_args": True,
                "pass_gee_account_id": True,
            },
            global_args={"start_year": 2017, "end_year": 2024},
            gee_account_id="22",
            compute="local",
        )

        self.assertEqual(
            args,
            {
                "start_year": 2017,
                "end_year": 2024,
                "gee_account_id": "22",
            },
        )

    def test_local_swb_task_accepts_overwrite(self):
        self.assertIn("overwrite", signature(generate_swb_layer_local.run).parameters)

    def test_local_map_runs_swb_without_generated_gee_dependencies(self):
        swb_node = next(
            node
            for node in load_map_config("dynamic_layers", compute="local")
            if node["name"] == "generate_swb"
        )

        self.assertTrue(swb_node["pass_gee_account_id"])

    @patch("computing.surface_water_bodies.swb_local.make_asset_public")
    @patch("computing.surface_water_bodies.swb_local.is_gee_asset_exists")
    @patch("computing.surface_water_bodies.swb_local.check_task_status")
    @patch("computing.surface_water_bodies.swb_local.waterbody_wbc_intersection")
    @patch(
        "computing.surface_water_bodies.swb_local.waterbody_catchment_streamorder_properties"
    )
    def test_runs_swb3_and_swb4_without_local_suffix(
        self,
        generate_swb3,
        generate_swb4,
        check_task_status,
        is_gee_asset_exists,
        make_asset_public,
    ):
        generate_swb3.return_value = ("swb3-task", "swb3-asset")
        generate_swb4.return_value = ("swb4-task", "swb4-asset")
        is_gee_asset_exists.return_value = True
        roi = object()
        completed_stages = []

        result = _continue_swb_in_gee(
            state="odisha",
            asset_suffix="district_block",
            asset_folder_list=["odisha", "district", "block"],
            app_type="MWS",
            gee_account_id="account",
            roi=roi,
            stage_completed=lambda stage, asset_id: completed_stages.append(
                (stage, asset_id)
            ),
        )

        generate_swb3.assert_called_once_with(
            roi=roi,
            asset_suffix="district_block",
            asset_folder_list=["odisha", "district", "block"],
            app_type="MWS",
            gee_account_id="account",
            swb2_asset_suffix="district_block_local",
        )
        generate_swb4.assert_called_once_with(
            roi=roi,
            state="odisha",
            asset_suffix="district_block",
            asset_folder_list=["odisha", "district", "block"],
            app_type="MWS",
        )
        self.assertEqual(
            check_task_status.call_args_list,
            [call(["swb3-task"]), call(["swb4-task"])],
        )
        self.assertEqual(
            make_asset_public.call_args_list,
            [call("swb3-asset"), call("swb4-asset")],
        )
        self.assertEqual(
            completed_stages,
            [("swb3", "swb3-asset"), ("swb4", "swb4-asset")],
        )
        self.assertEqual(result, ("district_block", "swb4-asset"))

    @patch("computing.surface_water_bodies.swb_local.make_asset_public")
    @patch("computing.surface_water_bodies.swb_local.is_gee_asset_exists")
    @patch("computing.surface_water_bodies.swb_local.check_task_status")
    @patch("computing.surface_water_bodies.swb_local.waterbody_wbc_intersection")
    @patch(
        "computing.surface_water_bodies.swb_local.waterbody_catchment_streamorder_properties"
    )
    def test_retains_swb3_when_swb4_fails(
        self,
        generate_swb3,
        generate_swb4,
        check_task_status,
        is_gee_asset_exists,
        make_asset_public,
    ):
        generate_swb3.return_value = ("swb3-task", "swb3-asset")
        generate_swb4.side_effect = RuntimeError("SWB4 failed")
        is_gee_asset_exists.return_value = True
        completed_stages = []

        result = _continue_swb_in_gee(
            state="odisha",
            asset_suffix="district_block",
            asset_folder_list=["odisha", "district", "block"],
            app_type="MWS",
            gee_account_id="account",
            roi=object(),
            stage_completed=lambda stage, asset_id: completed_stages.append(
                (stage, asset_id)
            ),
        )

        self.assertEqual(completed_stages, [("swb3", "swb3-asset")])
        make_asset_public.assert_called_once_with("swb3-asset")
        self.assertEqual(result, ("district_block", "swb3-asset"))

    @patch("computing.surface_water_bodies.swb_local._sync_swb_stage")
    @patch("computing.surface_water_bodies.swb_local._continue_swb_in_gee")
    def test_syncs_geoserver_after_each_successful_gee_stage(
        self,
        continue_swb,
        sync_stage,
    ):
        def complete_stages(**kwargs):
            kwargs["stage_completed"]("swb3", "swb3-asset")
            kwargs["stage_completed"]("swb4", "swb4-asset")

        continue_swb.side_effect = complete_stages

        result = _complete_swb_pipeline(
            state="odisha",
            district="district",
            block="block",
            asset_suffix="district_block",
            asset_folder_list=["odisha", "district", "block"],
            app_type="MWS",
            gee_account_id="account",
            roi=object(),
            start_year=2017,
            end_year=2024,
            push_to_geoserver=True,
            sync_layer_metadata=True,
        )

        self.assertEqual(
            sync_stage.call_args_list,
            [
                call(
                    state="odisha",
                    district="district",
                    block="block",
                    layer_name="surface_waterbodies_district_block",
                    asset_suffix="district_block",
                    asset_id="swb3-asset",
                    source_stage="swb3",
                    start_year=2017,
                    end_year=2024,
                    push_to_geoserver=True,
                    sync_layer_metadata=True,
                ),
                call(
                    state="odisha",
                    district="district",
                    block="block",
                    layer_name="surface_waterbodies_district_block",
                    asset_suffix="district_block",
                    asset_id="swb4-asset",
                    source_stage="swb4",
                    start_year=2017,
                    end_year=2024,
                    push_to_geoserver=True,
                    sync_layer_metadata=True,
                ),
            ],
        )
        self.assertTrue(result)


class LocalOnlySwbTests(SimpleTestCase):
    @patch("computing.surface_water_bodies.swb_local._complete_swb_pipeline_local")
    @patch("computing.surface_water_bodies.swb_local.ee_initialize")
    @patch("computing.surface_water_bodies.swb_local.export_vector_asset_to_gee")
    @patch("computing.surface_water_bodies.swb_local._resolve_roi_gdf")
    def test_default_run_never_touches_gee(
        self, resolve_roi, export_to_gee, ee_initialize, complete_local
    ):
        import geopandas as gpd
        from shapely.geometry import box

        resolve_roi.return_value = gpd.GeoDataFrame(
            geometry=[box(0, 0, 1, 1)], crs="EPSG:4326"
        )
        complete_local.return_value = True

        with TemporaryDirectory() as output_dir, patch(
            "computing.surface_water_bodies.swb_local.LOCAL_OUTPUT_BASE_DIR",
            Path(output_dir),
        ):
            result = run_swb_local(state="Odisha", district="D", block="B")

        self.assertTrue(result)
        ee_initialize.assert_not_called()
        export_to_gee.assert_not_called()
        complete_local.assert_called_once()

    def test_swb4_local_matches_closest_census_point_and_marks_rest_na(self):
        import geopandas as gpd
        from shapely.geometry import Point, box

        from computing.surface_water_bodies.swb4_local import (
            waterbody_wbc_intersection_local,
        )

        swb = gpd.GeoDataFrame(
            {"wb_id": ["a", "b"], "area_ored": [1.0, 2.0]},
            geometry=[box(77.0, 12.0, 77.001, 12.001), box(77.1, 12.1, 77.101, 12.101)],
            crs="EPSG:4326",
        )
        census = gpd.GeoDataFrame(
            {
                "unique_id": ["u1", "u2"],
                "State Name": ["KARNATAKA", "KERALA"],
                "water_spread_area_of_water_body": [1.0, 2.0],
                "storage_capacity_water_body_original": [None, None],
                "max_depth_water_body_fully_filled": [None, None],
                "water_body_name": ["pond", "other state"],
            },
            geometry=[Point(77.0005, 12.0005), Point(77.1005, 12.1005)],
            crs="EPSG:4326",
        )
        with TemporaryDirectory() as folder:
            census_path = Path(folder) / "census.gpkg"
            census.to_file(census_path, driver="GPKG")
            result = waterbody_wbc_intersection_local(swb, "Karnataka", census_path)

        self.assertEqual(list(result["census_id"]), ["u1", "NA"])
        self.assertEqual(list(result["water_body_name"]), ["pond", "NA"])
