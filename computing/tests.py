from io import StringIO
from inspect import signature
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, call, patch

import geopandas as gpd
import pandas as pd
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase, TestCase, override_settings
from shapely.geometry import Point

from computing.bulk_layer_generation import (
    Location,
    get_active_locations,
    get_active_locations_from_api,
    get_locally_generated_locations,
    get_regeneration_dataset_names,
    pipeline_names,
    run_pipeline,
)
from computing.layer_dependency.layer_generation_in_order import get_args, load_map_config
from computing.local_compute_helper import write_vector_output
from computing.misc.nrega_local_compute import _compute_nrega_for_watersheds
from computing.models import Dataset, Layer
from computing.surface_water_bodies.swb_local import (
    _continue_swb_in_gee,
    _convert_area_columns_to_hectares,
    _final_layer_name,
    _layer_name,
    run_swb_local,
)
from computing.surface_water_bodies.swb3 import (
    waterbody_catchment_streamorder_properties,
)
from computing.utils import generate_swb_layer_with_max_so_catchment
from computing.tasks import bulk_generate_layer
from geoadmin.models import DistrictSOI, StateSOI, TehsilSOI
from utilities.constants import (
    CATCHMENT_AREA,
    PAN_INDIA_DRAINAGE_LINES_DATASET,
    STREAM_ORDER_ASSET,
)


class LocalNregaTests(SimpleTestCase):
    def test_timestamp_attributes_can_be_written_to_geopackage(self):
        watersheds = gpd.GeoDataFrame(
            geometry=[Point(0, 0)],
            crs="EPSG:4326",
        )
        nrega = gpd.GeoDataFrame(
            {
                "work_start": pd.to_datetime(["2026-08-05"]),
                "geometry": [Point(0, 0)],
            },
            crs="EPSG:4326",
        )

        result = _compute_nrega_for_watersheds(watersheds, nrega)

        self.assertEqual(result.loc[0, "work_start"], "2026-08-05T00:00:00")
        with TemporaryDirectory() as output_dir:
            output_path = Path(output_dir) / "nrega.gpkg"
            write_vector_output(result, output_path, "nrega")
            self.assertTrue(output_path.exists())


class LocalSwbContinuationTests(SimpleTestCase):
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
    def test_all_local_pipelines_define_regeneration_datasets(self):
        for pipeline in pipeline_names("local"):
            self.assertTrue(
                get_regeneration_dataset_names(pipeline),
                pipeline,
            )

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
