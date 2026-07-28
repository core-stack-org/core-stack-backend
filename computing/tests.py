from unittest.mock import call, patch

from django.test import SimpleTestCase

from computing.layer_dependency.layer_generation_in_order import get_args
from computing.surface_water_bodies.swb_local import (
    _continue_swb_in_gee,
    _final_layer_name,
)


class LocalSwbContinuationTests(SimpleTestCase):
    def test_final_geoserver_layer_has_no_local_suffix(self):
        self.assertEqual(
            _final_layer_name("dumka_jarmundi"),
            "surface_waterbodies_dumka_jarmundi",
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

        result = _continue_swb_in_gee(
            state="odisha",
            asset_suffix="district_block",
            asset_folder_list=["odisha", "district", "block"],
            app_type="MWS",
            gee_account_id="account",
            roi=roi,
        )

        generate_swb3.assert_called_once_with(
            roi=roi,
            asset_suffix="district_block",
            asset_folder_list=["odisha", "district", "block"],
            app_type="MWS",
            gee_account_id="account",
            supporting_asset_suffix="district_block",
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
        self.assertEqual(result, ("district_block", "swb3-asset"))
