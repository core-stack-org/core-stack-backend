from io import StringIO
from pathlib import Path
import tempfile
from unittest.mock import MagicMock, patch

import geopandas as gpd
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase
from shapely.geometry import Point, Polygon

from computing.bulk_layer_generation import Location, get_layer_sync_filter
from computing.zoi_layers import zoi_local


def _sample_swb_gdf():
    poly = Polygon(
        [
            (73.85, 18.50),
            (73.851, 18.50),
            (73.851, 18.501),
            (73.85, 18.501),
        ]
    )
    return gpd.GeoDataFrame(
        {"UID": ["wb_1"], "wb_id": ["wb_1"], "area_ored": [1.5]},
        geometry=[poly],
        crs="EPSG:4326",
    )


def _sample_ndvi_geojson():
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "UID": "wb_1",
                    "wb_id": "wb_1",
                    "zoi": 561.0,
                    "cropping_intensity_2024": 1.25,
                    "NDVI_2024": '{"2024-07-01": 0.45}',
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [73.85, 18.50],
                            [73.855, 18.50],
                            [73.855, 18.505],
                            [73.85, 18.505],
                            [73.85, 18.50],
                        ]
                    ],
                },
            }
        ],
    }


class ZoiLocalPipelineTests(SimpleTestCase):
    def test_layer_sync_filters_disambiguate_swb_and_zoi(self):
        self.assertEqual(
            get_layer_sync_filter("generate_swb"),
            (("Surface Water Bodies",), "surface_waterbodies_"),
        )
        self.assertEqual(
            get_layer_sync_filter("generate_zoi"),
            (("Surface Water Bodies",), "waterbodies_zoi_"),
        )

    def test_zoi_output_path_does_not_create_dirs_when_create_dir_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_root = Path(tmp)
            with patch.object(zoi_local, "ZOI_OUTPUT_BASE_DIR", tmp_root):
                path = zoi_local.zoi_output_path(
                    "Maharashtra", "Pune", "Mulshi", create_dir=False
                )
                self.assertFalse(path.parent.exists())
                self.assertEqual(path.name, "waterbodies_zoi_pune_mulshi.gpkg")

    def test_run_zoi_local_happy_path_queues_zoi2_once_without_gee(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_root = Path(tmp)
            swb_root = tmp_root / "swb"
            zoi_root = tmp_root / "zoi"
            ci_root = tmp_root / "cropping_intensity"

            with (
                patch.object(zoi_local, "SWB_OUTPUT_BASE_DIR", swb_root),
                patch.object(zoi_local, "ZOI_OUTPUT_BASE_DIR", zoi_root),
                patch.object(zoi_local, "ZOI_CI_OUTPUT_BASE_DIR", ci_root),
                patch.object(zoi_local, "add_zoi_cropping_intensity") as add_ci,
                patch.object(
                    zoi_local, "save_layer_info_to_db", side_effect=[42, 43]
                ) as save_db,
                patch.object(
                    zoi_local,
                    "queue_local_vector_for_geoserver",
                    return_value={"status_code": 202, "queued": True, "task_id": "t1"},
                ) as queue_gs,
                patch.object(zoi_local, "export_zoi_to_gee_for_ndvi") as export_gee,
            ):
                swb_path = zoi_local._swb_input_path(
                    "maharashtra", "pune", "mulshi", "pune_mulshi"
                )
                _sample_swb_gdf().to_file(
                    swb_path,
                    driver="GPKG",
                    layer="surface_waterbodies_pune_mulshi",
                )

                def fake_ci(rings_gdf, start_year, end_year, lulc_dir=None):
                    enriched = rings_gdf.copy()
                    enriched["cropping_intensity_2024"] = 1.5
                    return enriched

                add_ci.side_effect = fake_ci

                output_path = zoi_local.run_zoi_local(
                    state="Maharashtra",
                    district="Pune",
                    block="Mulshi",
                    start_year=2024,
                    end_year=2024,
                    start_date="2024-07-01",
                    end_date="2025-06-30",
                )

                self.assertTrue(output_path.exists())
                saved_gdf = gpd.read_file(output_path)
                self.assertIn("cropping_intensity_2024", saved_gdf.columns)
                self.assertIn("zoi_area", saved_gdf.columns)

                ci_output_path = zoi_local.zoi_ci_output_path(
                    "maharashtra", "pune", "mulshi", create_dir=False
                )
                self.assertTrue(ci_output_path.exists())
                ci_gdf = gpd.read_file(ci_output_path)
                self.assertIn("cropping_intensity_2024", ci_gdf.columns)
                self.assertIn("zoi_area", ci_gdf.columns)

                # Two DB saves and two GeoServer queues: swb:waterbodies_zoi_* and crop_intensity:*_intensity_ZOI.
                self.assertEqual(save_db.call_count, 2)
                swb_save_kwargs = save_db.call_args_list[0].kwargs
                self.assertEqual(swb_save_kwargs["dataset_name"], "Surface Water Bodies")
                self.assertEqual(swb_save_kwargs["layer_name"], "waterbodies_zoi_pune_mulshi")
                self.assertEqual(swb_save_kwargs["asset_id"], str(output_path))
                self.assertEqual(swb_save_kwargs["misc"]["source_stage"], "zoi2_local")
                self.assertTrue(swb_save_kwargs["misc"]["is_generated_locally"])
                self.assertEqual(swb_save_kwargs["misc"]["local_vector_path"], str(output_path))

                ci_save_kwargs = save_db.call_args_list[1].kwargs
                self.assertEqual(ci_save_kwargs["dataset_name"], "Cropping Intensity")
                self.assertEqual(ci_save_kwargs["layer_name"], "pune_mulshi_intensity_ZOI")
                self.assertEqual(ci_save_kwargs["asset_id"], str(ci_output_path))
                self.assertEqual(ci_save_kwargs["misc"]["start_year"], 2024)
                self.assertEqual(ci_save_kwargs["misc"]["end_year"], 2024)
                self.assertEqual(ci_save_kwargs["misc"]["source_stage"], "zoi2_local")
                self.assertTrue(ci_save_kwargs["misc"]["is_generated_locally"])
                self.assertEqual(ci_save_kwargs["misc"]["local_vector_path"], str(ci_output_path))

                self.assertEqual(queue_gs.call_count, 2)
                queue_gs.assert_any_call(
                    path=str(output_path.with_suffix("")),
                    layer_name="waterbodies_zoi_pune_mulshi",
                    workspace="swb",
                    file_type="gpkg",
                    layer_id=42,
                )
                queue_gs.assert_any_call(
                    path=str(ci_output_path.with_suffix("")),
                    layer_name="pune_mulshi_intensity_ZOI",
                    workspace="crop_intensity",
                    file_type="gpkg",
                    layer_id=43,
                )
                export_gee.assert_not_called()

    def test_run_zoi_local_falls_back_to_zoi1_when_zoi2_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_root = Path(tmp)
            swb_root = tmp_root / "swb"
            zoi_root = tmp_root / "zoi"

            with (
                patch.object(zoi_local, "SWB_OUTPUT_BASE_DIR", swb_root),
                patch.object(zoi_local, "ZOI_OUTPUT_BASE_DIR", zoi_root),
                patch.object(
                    zoi_local,
                    "add_zoi_cropping_intensity",
                    side_effect=RuntimeError("missing lulc raster"),
                ),
                patch.object(
                    zoi_local, "save_layer_info_to_db", return_value=7
                ) as save_db,
                patch.object(
                    zoi_local,
                    "queue_local_vector_for_geoserver",
                    return_value={"status_code": 202, "queued": True, "task_id": "t2"},
                ) as queue_gs,
            ):
                swb_path = zoi_local._swb_input_path(
                    "maharashtra", "pune", "mulshi", "pune_mulshi"
                )
                _sample_swb_gdf().to_file(
                    swb_path,
                    driver="GPKG",
                    layer="surface_waterbodies_pune_mulshi",
                )

                output_path = zoi_local.run_zoi_local(
                    state="Maharashtra",
                    district="Pune",
                    block="Mulshi",
                    start_year=2024,
                    end_year=2024,
                    start_date="2024-07-01",
                    end_date="2025-06-30",
                )

                self.assertTrue(output_path.exists())
                saved_gdf = gpd.read_file(output_path)
                self.assertIn("zoi", saved_gdf.columns)
                self.assertIn("zoi_area", saved_gdf.columns)

                save_db.assert_called_once()
                self.assertEqual(
                    save_db.call_args.kwargs["misc"]["source_stage"], "zoi1_local"
                )
                queue_gs.assert_called_once()

    def test_generate_zoi_default_skips_ndvi_and_gee_init(self):
        with (
            patch.object(zoi_local, "ee_initialize") as ee_init,
            patch.object(zoi_local, "run_zoi_local") as run_local,
            patch.object(
                zoi_local, "sync_local_zoi_to_gee_and_run_ndvi"
            ) as run_ndvi_sync,
        ):
            result = zoi_local.generate_zoi(
                state="maharashtra",
                district="pune",
                block="mulshi",
                start_year=2024,
                end_year=2024,
            )
            self.assertTrue(result)
            ee_init.assert_not_called()
            run_local.assert_called_once()
            run_ndvi_sync.assert_not_called()

    def test_generate_zoi_with_run_ndvi_true_triggers_gee_ndvi(self):
        with (
            patch.object(zoi_local, "run_zoi_local") as run_local,
            patch.object(
                zoi_local, "sync_local_zoi_to_gee_and_run_ndvi"
            ) as run_ndvi_sync,
        ):
            result = zoi_local.generate_zoi(
                state="maharashtra",
                district="pune",
                block="mulshi",
                start_year=2024,
                end_year=2024,
                gee_account_id="acc1",
                run_ndvi=True,
                fast_ndvi=True,
            )
            self.assertTrue(result)
            run_local.assert_called_once()
            run_ndvi_sync.assert_called_once()
            kwargs = run_ndvi_sync.call_args.kwargs
            self.assertEqual(kwargs["gee_account_id"], "acc1")
            self.assertTrue(kwargs["overwrite_gee_upload"])
            self.assertTrue(kwargs["fast_ndvi"])

    def test_sync_local_zoi_to_gee_and_run_ndvi_updates_gpkg_db_and_geoserver(self):
        with tempfile.TemporaryDirectory() as tmp:
            zoi_root = Path(tmp) / "zoi"
            with patch.object(zoi_local, "ZOI_OUTPUT_BASE_DIR", zoi_root):
                local_path = zoi_local.zoi_output_path(
                    "maharashtra", "pune", "mulshi", create_dir=True
                )
                _sample_swb_gdf().to_file(
                    local_path,
                    driver="GPKG",
                    layer="waterbodies_zoi_pune_mulshi",
                )

                fake_fc = MagicMock()
                fake_fc.getInfo.return_value = _sample_ndvi_geojson()

                with (
                    patch.object(zoi_local, "ee_initialize") as ee_init,
                    patch.object(
                        zoi_local,
                        "get_gee_dir_path",
                        return_value="projects/demo/maharashtra/pune/mulshi/",
                    ),
                    patch.object(
                        zoi_local,
                        "is_gee_asset_exists",
                        side_effect=[False, True],  # upload missing, ndvi created
                    ),
                    patch.object(
                        zoi_local,
                        "export_zoi_to_gee_for_ndvi",
                        return_value="projects/demo/maharashtra/pune/mulshi/zoi_pune_mulshi",
                    ) as export_gee,
                    patch(
                        "computing.zoi_layers.zoi3.get_ndvi_for_zoi",
                        return_value="projects/demo/maharashtra/pune/mulshi/pune_mulshi",
                    ) as run_zoi3,
                    patch.object(zoi_local, "make_asset_public") as make_public,
                    patch.object(zoi_local.ee, "FeatureCollection", return_value=fake_fc),
                    patch.object(
                        zoi_local, "save_layer_info_to_db", return_value=99
                    ) as save_db,
                    patch.object(
                        zoi_local,
                        "push_local_vector_to_geoserver",
                        return_value={"status_code": 201},
                    ) as push_gs,
                    patch.object(zoi_local, "update_layer_sync_status") as update_sync,
                ):
                    result = zoi_local.sync_local_zoi_to_gee_and_run_ndvi(
                        state="maharashtra",
                        district="pune",
                        block="mulshi",
                        start_year=2024,
                        end_year=2024,
                        gee_account_id="acc1",
                        sync_geoserver_inline=True,
                    )

                    self.assertEqual(result["stage"], "zoi3_ndvi")
                    self.assertEqual(
                        result["ndvi_asset_id"],
                        "projects/demo/maharashtra/pune/mulshi/pune_mulshi",
                    )
                    self.assertEqual(result["layer_id"], 99)

                    ee_init.assert_called_once_with("acc1")
                    export_gee.assert_called_once()
                    run_zoi3.assert_called_once()
                    self.assertFalse(run_zoi3.call_args.kwargs["sync_to_geoserver"])
                    make_public.assert_called_once_with(
                        "projects/demo/maharashtra/pune/mulshi/pune_mulshi"
                    )

                    # Local .gpkg now contains NDVI column
                    updated_gdf = gpd.read_file(local_path)
                    self.assertIn("NDVI_2024", updated_gdf.columns)

                    # DB metadata keeps local path + zoi3_ndvi stage + GEE asset refs
                    save_db.assert_called_once()
                    self.assertEqual(
                        save_db.call_args.kwargs["asset_id"], str(local_path)
                    )
                    misc = save_db.call_args.kwargs["misc"]
                    self.assertEqual(misc["source_stage"], "zoi3_ndvi")
                    self.assertEqual(
                        misc["gee_ndvi_asset_id"],
                        "projects/demo/maharashtra/pune/mulshi/pune_mulshi",
                    )
                    push_gs.assert_called_once()
                    update_sync.assert_called_once_with(
                        layer_id=99,
                        sync_to_geoserver=True,
                        is_stac_specs_generated=False,
                    )

    def test_sync_local_zoi_upload_only_skips_ndvi_and_geoserver(self):
        with tempfile.TemporaryDirectory() as tmp:
            zoi_root = Path(tmp) / "zoi"
            with patch.object(zoi_local, "ZOI_OUTPUT_BASE_DIR", zoi_root):
                local_path = zoi_local.zoi_output_path(
                    "maharashtra", "pune", "mulshi", create_dir=True
                )
                _sample_swb_gdf().to_file(
                    local_path,
                    driver="GPKG",
                    layer="waterbodies_zoi_pune_mulshi",
                )

                with (
                    patch.object(zoi_local, "ee_initialize"),
                    patch.object(
                        zoi_local,
                        "get_gee_dir_path",
                        return_value="projects/demo/maharashtra/pune/mulshi/",
                    ),
                    patch.object(zoi_local, "is_gee_asset_exists", return_value=False),
                    patch.object(
                        zoi_local,
                        "export_zoi_to_gee_for_ndvi",
                        return_value="projects/demo/maharashtra/pune/mulshi/zoi_pune_mulshi",
                    ) as export_gee,
                    patch("computing.zoi_layers.zoi3.get_ndvi_for_zoi") as run_zoi3,
                ):
                    result = zoi_local.sync_local_zoi_to_gee_and_run_ndvi(
                        state="maharashtra",
                        district="pune",
                        block="mulshi",
                        upload_only=True,
                    )
                    self.assertEqual(result["stage"], "uploaded")
                    self.assertIsNone(result["ndvi_asset_id"])
                    export_gee.assert_called_once()
                    run_zoi3.assert_not_called()

    def test_sync_local_zoi_raises_if_local_gpkg_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(zoi_local, "ZOI_OUTPUT_BASE_DIR", Path(tmp) / "zoi"):
                with self.assertRaises(FileNotFoundError):
                    zoi_local.sync_local_zoi_to_gee_and_run_ndvi(
                        state="maharashtra",
                        district="pune",
                        block="mulshi",
                        start_year=2024,
                        end_year=2024,
                    )

    def test_sync_local_zoi_skip_existing_ndvi_reuses_gee_asset_and_queues_geoserver(self):
        with tempfile.TemporaryDirectory() as tmp:
            zoi_root = Path(tmp) / "zoi"
            with patch.object(zoi_local, "ZOI_OUTPUT_BASE_DIR", zoi_root):
                local_path = zoi_local.zoi_output_path(
                    "maharashtra", "pune", "mulshi", create_dir=True
                )
                _sample_swb_gdf().to_file(
                    local_path,
                    driver="GPKG",
                    layer="waterbodies_zoi_pune_mulshi",
                )

                fake_fc = MagicMock()
                fake_fc.getInfo.return_value = _sample_ndvi_geojson()

                with (
                    patch.object(zoi_local, "ee_initialize"),
                    patch.object(
                        zoi_local,
                        "get_gee_dir_path",
                        return_value="projects/demo/maharashtra/pune/mulshi/",
                    ),
                    patch.object(zoi_local, "is_gee_asset_exists", return_value=True),
                    patch.object(zoi_local, "export_zoi_to_gee_for_ndvi") as export_gee,
                    patch("computing.zoi_layers.zoi3.get_ndvi_for_zoi") as run_zoi3,
                    patch.object(zoi_local, "make_asset_public"),
                    patch.object(zoi_local.ee, "FeatureCollection", return_value=fake_fc),
                    patch.object(zoi_local, "save_layer_info_to_db", return_value=55),
                    patch.object(
                        zoi_local,
                        "queue_local_vector_for_geoserver",
                        return_value={"status_code": 202, "queued": True},
                    ) as queue_gs,
                ):
                    result = zoi_local.sync_local_zoi_to_gee_and_run_ndvi(
                        state="maharashtra",
                        district="pune",
                        block="mulshi",
                        start_year=2024,
                        end_year=2024,
                        skip_existing_ndvi=True,
                        sync_geoserver_inline=False,
                    )
                    self.assertEqual(result["stage"], "zoi3_ndvi")
                    export_gee.assert_not_called()
                    run_zoi3.assert_not_called()
                    queue_gs.assert_called_once()

    def test_skip_existing_uses_layer_prefix_for_zoi_and_swb(self):
        from types import SimpleNamespace
        from utilities.skip_existing import maybe_skip, skip_existing_context

        zoi_task = SimpleNamespace(name="computing.zoi_layers.zoi_local.generate_zoi")
        swb_task = SimpleNamespace(
            name="computing.surface_water_bodies.swb_local.generate_swb_layer"
        )

        with (
            patch(
                "computing.bulk_layer_generation.datasets_exist_locally",
                return_value=True,
            ) as mock_exists,
            skip_existing_context(True, "maharashtra", "pune", "mulshi"),
        ):
            self.assertIsNotNone(maybe_skip(zoi_task))
            mock_exists.assert_called_with(
                ("Surface Water Bodies",),
                {"state": "maharashtra", "district": "pune", "block": "mulshi"},
                layer_name_prefix="waterbodies_zoi_",
            )

            self.assertIsNotNone(maybe_skip(swb_task))
            mock_exists.assert_called_with(
                ("Surface Water Bodies",),
                {"state": "maharashtra", "district": "pune", "block": "mulshi"},
                layer_name_prefix="surface_waterbodies_",
            )

    def test_layer_generation_in_order_skip_uses_prefix_for_zoi_and_swb(self):
        from types import SimpleNamespace
        from computing.layer_dependency.layer_generation_in_order import (
            _layer_already_generated,
        )

        zoi_task = SimpleNamespace(name="computing.zoi_layers.zoi_local.generate_zoi")
        swb_task = SimpleNamespace(
            name="computing.surface_water_bodies.swb_local.generate_swb_layer"
        )
        with patch(
            "computing.bulk_layer_generation.datasets_exist_locally",
            return_value=True,
        ) as mock_exists:
            self.assertTrue(
                _layer_already_generated(zoi_task, "maharashtra", "pune", "mulshi")
            )
            mock_exists.assert_called_with(
                ("Surface Water Bodies",),
                {"state": "maharashtra", "district": "pune", "block": "mulshi"},
                layer_name_prefix="waterbodies_zoi_",
            )
            self.assertTrue(
                _layer_already_generated(swb_task, "maharashtra", "pune", "mulshi")
            )
            mock_exists.assert_called_with(
                ("Surface Water Bodies",),
                {"state": "maharashtra", "district": "pune", "block": "mulshi"},
                layer_name_prefix="surface_waterbodies_",
            )


class SyncZoiNdviGeeCommandTests(SimpleTestCase):
    def test_command_requires_location_scope(self):
        with self.assertRaises(CommandError):
            call_command("sync_zoi_ndvi_gee", "--start-year=2024", "--end-year=2024")

    def test_command_dry_run_skips_missing_and_reports_ready(self):
        with tempfile.TemporaryDirectory() as tmp:
            zoi_root = Path(tmp) / "zoi"
            with patch.object(zoi_local, "ZOI_OUTPUT_BASE_DIR", zoi_root):
                ready_path = zoi_local.zoi_output_path(
                    "maharashtra", "pune", "mulshi", create_dir=True
                )
                ready_path.write_bytes(b"placeholder")

                locations = [
                    Location(state="maharashtra", district="pune", block="mulshi"),
                    Location(state="maharashtra", district="pune", block="velhe"),
                ]
                stdout = StringIO()
                with patch(
                    "computing.management.commands.sync_zoi_ndvi_gee.get_active_locations",
                    return_value=locations,
                ):
                    call_command(
                        "sync_zoi_ndvi_gee",
                        "--all-active",
                        "--start-year=2024",
                        "--end-year=2024",
                        "--dry-run",
                        stdout=stdout,
                    )
                output = stdout.getvalue()
                self.assertIn("1 ready, 1 missing local ZOI", output)
                self.assertIn("maharashtra/pune/mulshi: ready", output)
                self.assertIn("maharashtra/pune/velhe: missing local ZOI", output)

    def test_command_queues_celery_tasks_for_present_locations(self):
        with tempfile.TemporaryDirectory() as tmp:
            zoi_root = Path(tmp) / "zoi"
            with patch.object(zoi_local, "ZOI_OUTPUT_BASE_DIR", zoi_root):
                ready_path = zoi_local.zoi_output_path(
                    "maharashtra", "pune", "mulshi", create_dir=True
                )
                ready_path.write_bytes(b"placeholder")

                locations = [
                    Location(state="maharashtra", district="pune", block="mulshi"),
                ]
                stdout = StringIO()
                fake_async = MagicMock(id="task-abc")
                with (
                    patch(
                        "computing.management.commands.sync_zoi_ndvi_gee.get_active_locations",
                        return_value=locations,
                    ),
                    patch(
                        "computing.management.commands.sync_zoi_ndvi_gee.sync_zoi_ndvi_gee.apply_async",
                        return_value=fake_async,
                    ) as apply_async,
                ):
                    call_command(
                        "sync_zoi_ndvi_gee",
                        "--state=maharashtra",
                        "--start-year=2024",
                        "--end-year=2025",
                        "--fast-ndvi",
                        "--queue=waterbody",
                        stdout=stdout,
                    )
                apply_async.assert_called_once()
                kwargs = apply_async.call_args.kwargs["kwargs"]
                self.assertEqual(kwargs["state"], "maharashtra")
                self.assertEqual(kwargs["start_date"], "2024-07-01")
                self.assertEqual(kwargs["end_date"], "2026-06-30")
                self.assertTrue(kwargs["fast_ndvi"])
                self.assertEqual(apply_async.call_args.kwargs["queue"], "waterbody")
                self.assertIn("Queued 1 task(s); 0 missing local ZOI", stdout.getvalue())

    def test_command_sync_mode_calls_inline_runner(self):
        with tempfile.TemporaryDirectory() as tmp:
            zoi_root = Path(tmp) / "zoi"
            with patch.object(zoi_local, "ZOI_OUTPUT_BASE_DIR", zoi_root):
                ready_path = zoi_local.zoi_output_path(
                    "maharashtra", "pune", "mulshi", create_dir=True
                )
                ready_path.write_bytes(b"placeholder")

                locations = [
                    Location(state="maharashtra", district="pune", block="mulshi"),
                ]
                stdout = StringIO()
                with (
                    patch(
                        "computing.management.commands.sync_zoi_ndvi_gee.get_active_locations",
                        return_value=locations,
                    ),
                    patch(
                        "computing.management.commands.sync_zoi_ndvi_gee.sync_local_zoi_to_gee_and_run_ndvi",
                        return_value={
                            "stage": "zoi3_ndvi",
                            "zoi_asset_id": "projects/demo/zoi_pune_mulshi",
                            "ndvi_asset_id": "projects/demo/pune_mulshi",
                        },
                    ) as inline_run,
                ):
                    call_command(
                        "sync_zoi_ndvi_gee",
                        "--all-active",
                        "--start-year=2024",
                        "--end-year=2024",
                        "--sync",
                        stdout=stdout,
                    )
                inline_run.assert_called_once()
                self.assertTrue(inline_run.call_args.kwargs["sync_geoserver_inline"])
                self.assertIn("Completed 1 location(s)", stdout.getvalue())
