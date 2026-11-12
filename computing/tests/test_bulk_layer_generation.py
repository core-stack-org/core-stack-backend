from io import StringIO
from unittest.mock import MagicMock, patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase, TestCase, override_settings

from computing.bulk_layer_generation import (
    Location,
    get_active_locations,
    get_active_locations_from_api,
    get_locally_generated_locations,
    get_regeneration_dataset_names,
    pipeline_names,
    run_pipeline,
)
from computing.cropping_intensity.cropping_intensity import (
    generate_gee_asset,
    _pan_india_lulc_asset_id,
)
from computing.layer_dependency.layer_generation_in_order import get_args, load_map_config
from computing.models import Dataset, Layer
from computing.tasks import bulk_generate_layer
from computing.zoi_layers.zoi import generate_zoi, _resolve_zoi_time_window
from geoadmin.models import DistrictSOI, StateSOI, TehsilSOI


class BulkPipelineRegistryTests(SimpleTestCase):
    @patch(
        "computing.cropping_intensity.cropping_intensity."
        "export_vector_asset_to_gee",
        return_value="task-id",
    )
    @patch(
        "computing.cropping_intensity.cropping_intensity.is_gee_asset_exists",
        return_value=False,
    )
    @patch("computing.cropping_intensity.cropping_intensity.ee")
    def test_zoi_lulc_does_not_replace_output_asset_id(
        self, ee, asset_exists, export_vector
    ):
        output_asset_id = "projects/example/cropping_intensity_zoi_location"

        task_id, returned_asset_id = generate_gee_asset(
            roi=MagicMock(),
            asset_id=output_asset_id,
            description="cropping_intensity_zoi_location",
            asset_suffix="location",
            asset_folder_list=["state", "district", "block"],
            app_type="MWS",
            start_year=2017,
            end_year=2017,
            zoi="projects/example/zoi_location",
        )

        ee.Image.assert_any_call(_pan_india_lulc_asset_id(2017))
        export_vector.assert_called_once()
        self.assertEqual(export_vector.call_args.args[2], output_asset_id)
        self.assertEqual((task_id, returned_asset_id), ("task-id", output_asset_id))

    def test_pan_india_lulc_asset_id_uses_agricultural_year(self):
        self.assertEqual(
            _pan_india_lulc_asset_id(2017),
            "projects/corestack-datasets/assets/datasets/"
            "LULC_v3_river_basin/pan_india_lulc_v3_2017_2018",
        )

    @patch("computing.local_compute_helper.prepare_local_tehsil_boundaries")
    @patch("computing.layer_dependency.layer_generation_in_order.generate_hydrology_local")
    def test_hydrology_runners_bind_is_annual(self, mock_hydrology, _prepare):
        from computing.layer_dependency.layer_generation_in_order import (
            LOCAL_TASK_REGISTRY,
        )

        location = {"state": "s", "district": "d", "block": "b"}
        for name, expected in (
            ("hydrology_annual", True),
            ("hydrology_fortnightly", False),
        ):
            mock_hydrology.reset_mock()
            run_pipeline(name, location, compute="local", start_year=2017, end_year=2024)
            self.assertIs(mock_hydrology.call_args.kwargs["is_annual"], expected)

    def test_zoi_is_registered_for_local_bulk_generation(self):
        self.assertIn("generate_zoi", pipeline_names("local"))
        self.assertEqual(
            get_regeneration_dataset_names("generate_zoi"),
            ("Surface Water Bodies",),
        )

    def test_local_zoi_map_runs_after_swb_with_gee_account(self):
        zoi_node = next(
            node
            for node in load_map_config("dynamic_layers", compute="local")
            if node["name"] == "generate_zoi"
        )

        self.assertEqual(zoi_node["depends_on"], ["generate_swb"])
        self.assertTrue(zoi_node["use_global_args"])
        self.assertTrue(zoi_node["pass_gee_account_id"])

        args = get_args(
            iterator_name=zoi_node,
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

    def test_zoi_resolves_hydrological_years_to_dates(self):
        self.assertEqual(
            _resolve_zoi_time_window(start_year=2017, end_year=2024),
            ("2017-07-01", "2025-06-30", 2017, 2024),
        )

    @patch("computing.zoi_layers.zoi.generate_zoi_ci")
    @patch("computing.zoi_layers.zoi.generate_zoi1")
    @patch("computing.zoi_layers.zoi.ee_initialize")
    def test_zoi_task_accepts_hydrological_years(
        self,
        ee_initialize,
        generate_zoi1,
        generate_zoi_ci,
    ):
        result = generate_zoi.run(
            state="odisha",
            district="bhadrak",
            block="chandabali",
            gee_account_id="22",
            start_year=2017,
            end_year=2024,
        )

        self.assertTrue(result)
        ee_initialize.assert_called_once_with("22")
        self.assertEqual(generate_zoi1.call_args.kwargs["start_date"], "2017-07-01")
        self.assertEqual(generate_zoi1.call_args.kwargs["end_date"], "2025-06-30")
        self.assertEqual(generate_zoi_ci.call_args.kwargs["start_year"], 2017)
        self.assertEqual(generate_zoi_ci.call_args.kwargs["end_year"], 2024)

    def test_all_local_pipelines_define_regeneration_datasets(self):
        for pipeline in pipeline_names("local"):
            self.assertTrue(
                get_regeneration_dataset_names(pipeline),
                pipeline,
            )

    @patch("computing.local_compute_helper.prepare_local_tehsil_boundaries")
    @patch("computing.bulk_layer_generation.import_string")
    def test_registered_pipeline_builds_standard_payload(
        self, import_string, _prepare_boundaries
    ):
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

    @patch("computing.local_compute_helper.prepare_local_tehsil_boundaries")
    @patch("computing.bulk_layer_generation._task_registry")
    def test_existing_registry_pipeline_receives_supported_arguments(
        self, task_registry, _prepare_boundaries
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
        get_locations.assert_called_once_with(
            blocks=["Masalia"],
            limit=None,
        )

    @patch(
        "computing.management.commands.bulk_generate_layers."
        "bulk_generate_layer.apply_async"
    )
    @patch(
        "computing.management.commands.bulk_generate_layers.get_active_locations"
    )
    def test_command_accepts_multiple_blocks(self, get_locations, apply_async):
        get_locations.return_value = [
            Location("Jharkhand", "Dumka", "Jarmundi"),
            Location("Jharkhand", "Dumka", "Masalia"),
        ]
        apply_async.return_value.id = "task-id"

        call_command(
            "bulk_generate_layers",
            "lulc_v3",
            "--district=Dumka",
            "--block=Jarmundi",
            "--block=Masalia",
            "--start-year=2018",
            "--end-year=2024",
            stdout=StringIO(),
        )

        get_locations.assert_called_once_with(
            district="Dumka",
            blocks=["Jarmundi", "Masalia"],
            limit=None,
        )
        self.assertEqual(apply_async.call_count, 2)

    @patch(
        "computing.management.commands.bulk_generate_layers."
        "get_active_locations_from_api"
    )
    def test_command_loads_locations_from_prod_api(self, get_locations):
        get_locations.return_value = [
            Location("Jharkhand", "Dumka", "Masalia")
        ]

        call_command(
            "bulk_generate_layers",
            "livestocks",
            "--all-active",
            "--from-prod-api",
            "--dry-run",
            stdout=StringIO(),
        )

        get_locations.assert_called_once_with(limit=None)

    @patch(
        "computing.management.commands.bulk_generate_layers."
        "bulk_generate_layer.apply_async"
    )
    @patch(
        "computing.management.commands.bulk_generate_layers."
        "get_locally_generated_locations"
    )
    def test_command_regenerates_locally_generated_locations(
        self, get_locations, apply_async
    ):
        get_locations.return_value = [
            Location("Jharkhand", "Dumka", "Masalia")
        ]
        apply_async.return_value.id = "task-id"

        call_command(
            "bulk_generate_layers",
            "livestocks",
            "--regenerate-local",
            "--district=Dumka",
            stdout=StringIO(),
        )

        get_locations.assert_called_once_with(
            dataset_names=("Livestock Census 2019",),
            district="Dumka",
            limit=None,
        )
        apply_async.assert_called_once()

    def test_change_detection_regeneration_uses_vector_dataset(self):
        self.assertEqual(
            get_regeneration_dataset_names("change_detection_vector"),
            ("Change Detection Vector",),
        )


@override_settings(PROD_BACKEND_URL="https://geoserver.core-stack.org/")
class ActiveLocationsApiTests(SimpleTestCase):
    @patch("computing.bulk_layer_generation.requests.get")
    def test_filters_and_limits_api_locations(self, get):
        get.return_value.json.return_value = [
            {
                "label": "Jharkhand",
                "district": [
                    {
                        "label": "Dumka",
                        "blocks": [
                            {"label": "Jarmundi"},
                            {"label": "Masalia"},
                        ],
                    }
                ],
            },
            {
                "label": "Odisha",
                "district": [
                    {
                        "label": "Mayurbhanj",
                        "blocks": [{"label": "Baripada"}],
                    }
                ],
            },
        ]

        locations = get_active_locations_from_api(
            state="jharkhand",
            district="dumka",
            blocks=["MASALIA", "jarmundi"],
            limit=1,
        )

        self.assertEqual(
            locations,
            [Location("Jharkhand", "Dumka", "Jarmundi")],
        )
        get.assert_called_once_with(
            "https://geoserver.core-stack.org/api/v1/proposed_blocks/",
            timeout=30,
        )
        get.return_value.raise_for_status.assert_called_once_with()

    @override_settings(PROD_BACKEND_URL="")
    def test_requires_prod_backend_url(self):
        with self.assertRaisesMessage(
            ValueError, "PROD_BACKEND_URL is not configured"
        ):
            get_active_locations_from_api()


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

    def test_active_locations_accept_multiple_blocks(self):
        locations = get_active_locations(
            state="jharkhand",
            district="dumka",
            blocks=["masalia", "JARMUNDI"],
        )

        self.assertEqual(
            [location.block for location in locations],
            ["Jarmundi", "Masalia"],
        )

    def test_locally_generated_locations_are_distinct_and_filterable(self):
        dataset = Dataset.objects.create(name="Local Dataset")
        other_dataset = Dataset.objects.create(name="Other Local Dataset")
        jarmundi = TehsilSOI.objects.get(tehsil_name="Jarmundi")
        masalia = TehsilSOI.objects.get(tehsil_name="Masalia")
        for layer_name in ("local_one", "local_two"):
            Layer.objects.create(
                dataset=dataset,
                layer_name=layer_name,
                state=jarmundi.district.state,
                district=jarmundi.district,
                block=jarmundi,
                misc={"is_generated_locally": True},
            )
        Layer.objects.create(
            dataset=other_dataset,
            layer_name="other_local_layer",
            state=masalia.district.state,
            district=masalia.district,
            block=masalia,
            misc={"is_generated_locally": True},
        )

        locations = get_locally_generated_locations(
            dataset_names=("Local Dataset",),
            district="dumka",
            blocks=["JARMUNDI", "Masalia"],
        )

        self.assertEqual(
            [location.asdict() for location in locations],
            [
                {
                    "state": "Jharkhand",
                    "district": "Dumka",
                    "block": "Jarmundi",
                }
            ],
        )
