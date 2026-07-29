from io import StringIO
from unittest.mock import call, patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase, TestCase

from computing.bulk_layer_generation import Location, get_active_locations, run_pipeline
from computing.layer_dependency.layer_generation_in_order import get_args
from computing.surface_water_bodies.swb_local import (
    _continue_swb_in_gee,
    _final_layer_name,
)
from computing.tasks import bulk_generate_layer
from geoadmin.models import DistrictSOI, StateSOI, TehsilSOI


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


class BulkPipelineRegistryTests(SimpleTestCase):
    @patch("computing.bulk_layer_generation.import_string")
    def test_registered_pipeline_builds_standard_payload(self, import_string):
        runner = import_string.return_value
        location = {
            "state": "Jharkhand",
            "district": "Dumka",
            "block": "Masalia",
        }

        run_pipeline("antyodaya", location, overwrite=False)

        runner.assert_called_once_with(
            {
                "scope": {
                    "level": "tehsil",
                    "state_name": "Jharkhand",
                    "district_name": "Dumka",
                    "tehsil_name": "Masalia",
                },
                "outputs": {},
                "publish": {
                    "sync_to_geoserver": True,
                    "overwrite": False,
                    "register_layers": True,
                    "use_pregenerated": False,
                },
            }
        )

    @patch("computing.tasks.run_pipeline")
    def test_bulk_task_runs_registered_pipeline(self, run_registered_pipeline):
        location = {
            "state": "Jharkhand",
            "district": "Dumka",
            "block": "Masalia",
        }

        bulk_generate_layer.run("livestocks", location, overwrite=False)

        run_registered_pipeline.assert_called_once_with(
            "livestocks",
            location,
            overwrite=False,
            compute="local",
            start_year=None,
            end_year=None,
            gee_account_id=None,
        )

    @patch("computing.bulk_layer_generation._task_registry")
    def test_existing_registry_pipeline_receives_supported_arguments(
        self, task_registry
    ):
        def lulc(
            state,
            district,
            block,
            start_year,
            end_year,
            gee_account_id=None,
        ):
            return (
                state,
                district,
                block,
                start_year,
                end_year,
                gee_account_id,
            )

        task_registry.return_value = {"lulc_v3": lulc}

        result = run_pipeline(
            "lulc_v3",
            {
                "state": "Jharkhand",
                "district": "Dumka",
                "block": "Masalia",
            },
            compute="local",
            start_year=2018,
            end_year=2024,
        )

        self.assertEqual(
            result,
            ("Jharkhand", "Dumka", "Masalia", 2018, 2024, None),
        )


class BulkLayerCommandTests(SimpleTestCase):
    def test_command_requires_explicit_scope(self):
        with self.assertRaisesMessage(CommandError, "Specify --all-active"):
            call_command("bulk_generate_layers", "antyodaya")

    @patch("computing.bulk_layer_generation._task_registry", return_value={})
    def test_command_rejects_unknown_pipeline(self, task_registry):
        with self.assertRaisesMessage(CommandError, "Unknown local pipeline"):
            call_command("bulk_generate_layers", "missing", "--all-active")

    @patch(
        "computing.management.commands.bulk_generate_layers."
        "bulk_generate_layer.apply_async"
    )
    @patch(
        "computing.management.commands.bulk_generate_layers.get_active_locations"
    )
    def test_command_dry_run_does_not_enqueue(
        self, get_locations, apply_async
    ):
        get_locations.return_value = [
            Location("Jharkhand", "Dumka", "Jarmundi")
        ]
        output = StringIO()

        call_command(
            "bulk_generate_layers",
            "antyodaya",
            "--all-active",
            "--limit=1",
            "--dry-run",
            stdout=output,
        )

        apply_async.assert_not_called()
        self.assertIn("Dry run complete", output.getvalue())

    @patch(
        "computing.management.commands.bulk_generate_layers."
        "bulk_generate_layer.apply_async"
    )
    @patch(
        "computing.management.commands.bulk_generate_layers.get_active_locations"
    )
    def test_command_routes_tasks_to_bulk_queue(
        self, get_locations, apply_async
    ):
        get_locations.return_value = [
            Location("Jharkhand", "Dumka", "Masalia")
        ]
        apply_async.return_value.id = "task-id"

        call_command(
            "bulk_generate_layers",
            "livestocks",
            "--block=Masalia",
            "--no-overwrite",
            stdout=StringIO(),
        )

        apply_async.assert_called_once_with(
            kwargs={
                "pipeline": "livestocks",
                "location": {
                    "state": "Jharkhand",
                    "district": "Dumka",
                    "block": "Masalia",
                },
                "overwrite": False,
                "compute": "local",
                "start_year": None,
                "end_year": None,
                "gee_account_id": None,
            },
            queue="layer_bulk",
        )


class BulkLayerGenerationTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        active_state = StateSOI.objects.create(
            state_name="Jharkhand", active_status=True
        )
        inactive_state = StateSOI.objects.create(
            state_name="Odisha", active_status=False
        )
        dumka = DistrictSOI.objects.create(
            state=active_state,
            district_name="Dumka",
            active_status=True,
        )
        inactive_district = DistrictSOI.objects.create(
            state=active_state,
            district_name="Inactive District",
            active_status=False,
        )
        inactive_state_district = DistrictSOI.objects.create(
            state=inactive_state,
            district_name="Mayurbhanj",
            active_status=True,
        )
        TehsilSOI.objects.create(
            district=dumka, tehsil_name="Masalia", active_status=True
        )
        TehsilSOI.objects.create(
            district=dumka, tehsil_name="Jarmundi", active_status=True
        )
        TehsilSOI.objects.create(
            district=dumka, tehsil_name="Inactive Block", active_status=False
        )
        TehsilSOI.objects.create(
            district=inactive_district,
            tehsil_name="Active Block",
            active_status=True,
        )
        TehsilSOI.objects.create(
            district=inactive_state_district,
            tehsil_name="Baripada",
            active_status=True,
        )

    def test_active_locations_require_active_hierarchy_and_are_ordered(self):
        locations = get_active_locations()

        self.assertEqual(
            [location.block for location in locations],
            ["Jarmundi", "Masalia"],
        )

    def test_active_locations_apply_case_insensitive_filters_and_limit(self):
        locations = get_active_locations(
            state="jharkhand",
            district="dumka",
            block="jarmundi",
            limit=1,
        )

        self.assertEqual(len(locations), 1)
        self.assertEqual(
            locations[0].asdict(),
            {
                "state": "Jharkhand",
                "district": "Dumka",
                "block": "Jarmundi",
            },
        )
