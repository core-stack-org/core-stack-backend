from unittest.mock import patch

from django.test import SimpleTestCase

from computing.local_compute_helper import queue_local_raster_for_geoserver
from computing.tasks import GeoServerPublishError, publish_local_layer


class GeoServerQueueTests(SimpleTestCase):
    @patch("computing.tasks.publish_local_layer.apply_async")
    def test_raster_publication_is_sent_to_serial_queue(self, apply_async):
        apply_async.return_value.id = "task-id"

        response = queue_local_raster_for_geoserver(
            file_path="/tmp/layer.tif",
            layer_name="layer",
            workspace="workspace",
            style_name="style",
            layer_id=42,
        )

        self.assertEqual(response["status_code"], 202)
        self.assertEqual(response["task_id"], "task-id")
        self.assertEqual(apply_async.call_args.kwargs["queue"], "geoserver")
        self.assertEqual(
            apply_async.call_args.kwargs["kwargs"]["layer_id"],
            42,
        )

    @patch("computing.utils.update_layer_sync_status")
    @patch("computing.local_compute_helper.push_local_raster_to_geoserver")
    def test_publish_task_updates_metadata_after_success(
        self,
        push_raster,
        update_sync,
    ):
        push_raster.return_value = {"status_code": 201}

        response = publish_local_layer.run(
            layer_type="raster",
            path="/tmp/layer.tif",
            layer_name="layer",
            workspace="workspace",
            layer_id=42,
        )

        self.assertEqual(response["status_code"], 201)
        update_sync.assert_called_once_with(
            layer_id=42,
            sync_to_geoserver=True,
            is_stac_specs_generated=False,
        )

    @patch("computing.local_compute_helper.push_local_raster_to_geoserver")
    def test_publish_task_raises_retryable_error_on_failure(self, push_raster):
        push_raster.return_value = {"status_code": 500}

        with self.assertRaises(GeoServerPublishError):
            publish_local_layer.run(
                layer_type="raster",
                path="/tmp/layer.tif",
                layer_name="layer",
                workspace="workspace",
            )


class GeeLayerSyncTests(SimpleTestCase):
    GEE_VECTOR = {
        "layer_id": 1,
        "layer_name": "vec",
        "path": "projects/p/assets/vec",
        "workspace": "ws",
        "style_name": None,
        "layer_type": "vector",
        "state": "s",
        "district": "d",
        "block": "b",
    }
    GEE_RASTER = {**GEE_VECTOR, "layer_id": 2, "layer_name": "ras", "layer_type": "raster"}

    def setUp(self):
        # asset type lookup falls back to the dataset type, without hitting GEE
        patcher = patch("ee.data.getAsset", side_effect=RuntimeError("no gee"))
        patcher.start()
        self.addCleanup(patcher.stop)

    @patch("computing.tasks.publish_gee_layer.apply_async")
    @patch("computing.pending_geoserver_sync._sync_boundaries")
    @patch("utilities.gee_utils.gcs_file_exists")
    @patch("utilities.gee_utils.is_gee_asset_exists")
    @patch("utilities.gee_utils.ee_initialize")
    @patch("computing.pending_geoserver_sync.fetch_unsynced_layers")
    def test_gee_layers_reported_in_gee_section(
        self, fetch, ee_init, asset_exists, gcs_exists, _boundaries, apply_async
    ):
        from computing.pending_geoserver_sync import queue_unsynced_layers

        apply_async.return_value.id = "t"
        asset_exists.return_value = True
        gcs_exists.return_value = False
        fetch.side_effect = lambda *a: (
            [] if a[4] == "local" else [self.GEE_VECTOR, self.GEE_RASTER]
        )

        summary = queue_unsynced_layers()

        self.assertEqual(summary["found"], 0)
        gee = summary["gee"]
        self.assertEqual(gee["vector"], {"found": 1, "queued": 1, "skipped": 0})
        self.assertEqual(gee["raster"], {"found": 1, "queued": 0, "skipped": 1})
        self.assertEqual(gee["skipped_layers"][0]["reason"], "no gcs file")
        self.assertEqual(apply_async.call_count, 1)
        self.assertEqual(apply_async.call_args.kwargs["queue"], "geoserver")

    @patch("computing.tasks.publish_gee_layer.apply_async")
    @patch("utilities.gee_utils.is_gee_asset_exists", return_value=False)
    @patch("utilities.gee_utils.ee_initialize")
    @patch("computing.pending_geoserver_sync.fetch_unsynced_layers")
    def test_missing_gee_asset_is_skipped_and_dry_run_queues_nothing(
        self, fetch, _ee, _exists, apply_async
    ):
        from computing.pending_geoserver_sync import queue_unsynced_layers

        fetch.return_value = [self.GEE_VECTOR]

        summary = queue_unsynced_layers(source="gee", dry_run=True)

        self.assertEqual(summary["gee"]["skipped_layers"][0]["reason"], "gee asset missing")
        apply_async.assert_not_called()

    @patch("computing.utils.update_layer_sync_status")
    @patch("utilities.gee_utils.sync_raster_gcs_to_geoserver")
    def test_publish_gee_raster_updates_flag_on_success(self, push, update_sync):
        from computing.tasks import publish_gee_layer

        push.return_value = "File response: ok"

        publish_gee_layer.run(
            layer_type="raster",
            asset_id="projects/p/assets/ras",
            layer_name="ras",
            workspace="ws",
            state="s",
            layer_id=2,
        )

        update_sync.assert_called_once_with(
            layer_id=2, sync_to_geoserver=True, is_stac_specs_generated=False
        )

    @patch("utilities.gee_utils.sync_raster_gcs_to_geoserver")
    def test_publish_gee_skipped_result_raises(self, push):
        from computing.tasks import publish_gee_layer

        push.return_value = {"skipped": True}

        with self.assertRaises(GeoServerPublishError):
            publish_gee_layer.run(
                layer_type="raster",
                asset_id="a",
                layer_name="ras",
                workspace="ws",
                state="s",
                layer_id=2,
            )


class ForceSyncTests(SimpleTestCase):
    @patch("computing.pending_geoserver_sync._sync_boundaries")
    @patch("computing.pending_geoserver_sync.fetch_unsynced_layers", return_value=[])
    def test_force_is_passed_to_both_sources(self, fetch, _boundaries):
        from computing.pending_geoserver_sync import queue_unsynced_layers

        summary = queue_unsynced_layers(force=True, dry_run=True)

        self.assertTrue(summary["force"])
        self.assertEqual([c.args[-1] for c in fetch.call_args_list], [True, True])

    def test_command_requires_yes_for_force(self):
        from django.core.management.base import CommandError
        from django.core.management import call_command

        with self.assertRaises(CommandError):
            call_command("sync_pending_geoserver", force=True)


class RelativePathAndRoutingTests(SimpleTestCase):
    def _entry(self, **kw):
        base = {
            "layer_id": 1,
            "layer_name": "kullu_kullu_cluster",
            "path": "data/terrain/terrain_clusters_local/himachal_pradesh/kullu/kullu/kullu_kullu_cluster.gpkg",
            "workspace": "terrain",
            "style_name": None,
            "layer_type": "vector",
            "state": "s",
            "district": "d",
            "block": "b",
        }
        base.update(kw)
        return base

    def test_relative_path_resolved_under_data_dir(self):
        import tempfile
        from pathlib import Path

        from computing import config_loader
        from computing.pending_geoserver_sync import _publish_args

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "terrain" / "x.gpkg"
            target.parent.mkdir()
            target.touch()
            with patch.object(config_loader, "DATA_DIR", Path(tmp)):
                args = _publish_args(self._entry(path="data/terrain/x.gpkg"))
        self.assertEqual(args["path"], str(Path(tmp) / "terrain" / "x"))
        self.assertEqual(args["file_type"], "gpkg")

    def test_tree_health_raster_placeholder_style_is_replaced(self):
        import tempfile
        from pathlib import Path

        from computing import config_loader
        from computing.pending_geoserver_sync import _publish_args

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "tree_health" / "ch.tif"
            target.parent.mkdir()
            target.touch()
            entry = self._entry(
                path="data/tree_health/ch.tif",
                layer_type="raster",
                workspace="tree_ch_raster",
                style_name="default_style",
            )
            with patch.object(config_loader, "DATA_DIR", Path(tmp)):
                args = _publish_args(entry)
                explicit = _publish_args({**entry, "style_name": "custom_style"})
        self.assertEqual(args["style_name"], "tree_ch_style")
        self.assertEqual(explicit["style_name"], "custom_style")

    def test_missing_relative_file_is_unusable(self):
        from computing.pending_geoserver_sync import _publish_args

        self.assertIsNone(_publish_args(self._entry(path="data/nope/none.gpkg")))

    def test_soil_health_raster_expands_per_nutrient(self):
        from computing.pending_geoserver_sync import _expand_soil_health_raster

        entry = self._entry(
            layer_name="kullu_kullu_soil_health_raster",
            workspace="soil_health_raster",
            layer_type="raster",
            path="data/soil_health/h/k/k/kullu_kullu_soil_health_raster_OC_OLM.tif",
        )
        out = _expand_soil_health_raster(entry)
        self.assertEqual(
            [e["layer_name"] for e in out],
            [f"kullu_kullu_soil_health_raster_{n}" for n in ("N", "K", "P", "OC", "OC_OLM")],
        )
        self.assertTrue(out[2]["path"].endswith("kullu_kullu_soil_health_raster_P.tif"))
        self.assertEqual([e["layer_id"] for e in out], [None] * 4 + [1])

    @patch("computing.tasks.publish_gee_layer.apply_async")
    @patch("computing.tasks.publish_local_layer.apply_async")
    @patch("computing.pending_geoserver_sync._sync_boundaries")
    @patch("utilities.gee_utils.is_gee_asset_exists", return_value=True)
    @patch("utilities.gee_utils.ee_initialize")
    @patch("computing.pending_geoserver_sync.fetch_unsynced_layers")
    def test_locally_flagged_gee_asset_goes_to_gee_publish(
        self, fetch, _ee, _exists, _b, local_apply, gee_apply
    ):
        from computing.pending_geoserver_sync import queue_unsynced_layers

        gee_apply.return_value.id = "t"
        swb = self._entry(
            layer_name="surface_waterbodies_kullu_kullu",
            workspace="swb",
            path="projects/p/assets/swb4_kullu_kullu",
        )
        fetch.side_effect = lambda *a: [swb] if a[4] == "local" else []

        summary = queue_unsynced_layers()

        self.assertEqual(summary["found"], 0)
        self.assertEqual(summary["gee"]["queued"], 1)
        gee_apply.assert_called_once()
        local_apply.assert_not_called()


class GeeLayerKindTests(SimpleTestCase):
    ENTRY = {
        "layer_id": 3,
        "layer_name": "stream_order_x_raster",
        "path": "projects/p/assets/stream_order_x_raster",
        "workspace": "stream_order",
        "style_name": "default_style",
        "layer_type": "vector",  # shared dataset says vector
        "state": "s",
        "district": "d",
        "block": "b",
    }

    @patch("computing.tasks.publish_gee_layer.apply_async")
    @patch("utilities.gee_utils.gcs_file_exists", return_value=True)
    @patch("utilities.gee_utils.ee_initialize")
    @patch("ee.data.getAsset", return_value={"type": "IMAGE"})
    @patch("computing.pending_geoserver_sync.fetch_unsynced_layers")
    def test_image_asset_published_as_raster_despite_vector_dataset(
        self, fetch, _get, _ee, _gcs, apply_async
    ):
        from computing.pending_geoserver_sync import queue_unsynced_layers

        apply_async.return_value.id = "t"
        fetch.side_effect = lambda *a: [self.ENTRY] if a[4] == "gee" else []

        summary = queue_unsynced_layers(source="gee", sync_boundaries=False)

        self.assertEqual(summary["gee"]["raster"]["queued"], 1)
        self.assertEqual(summary["gee"]["vector"]["found"], 0)
        self.assertEqual(apply_async.call_args.kwargs["kwargs"]["layer_type"], "raster")

    @patch("utilities.gee_utils.ee_initialize", side_effect=RuntimeError("no gee"))
    def test_falls_back_to_name_suffix_when_asset_unreadable(self, _ee):
        from computing.pending_geoserver_sync import _gee_layer_kind

        self.assertEqual(_gee_layer_kind(self.ENTRY, None, [False]), "raster")
        vec = {**self.ENTRY, "layer_name": "stream_order_x_vector"}
        self.assertEqual(_gee_layer_kind(vec, None, [False]), "vector")

    def test_local_tif_published_as_raster_despite_vector_dataset(self):
        import tempfile
        from computing.pending_geoserver_sync import _publish_args

        with tempfile.TemporaryDirectory() as tmp:
            tif = f"{tmp}/stream_order_x_raster.tif"
            open(tif, "w").close()
            entry = {**self.ENTRY, "path": tif}
            args = _publish_args(entry)

        self.assertIsNotNone(args)
        self.assertEqual(args["layer_type"], "raster")
        self.assertEqual(args["file_type"], "tif")
        self.assertEqual(args["path"], tif)
        self.assertEqual(args["style_name"], "stream_order")


class GpkgPipelineSyncTests(SimpleTestCase):
    def _entries(self, tmp):
        import os

        main = os.path.join(tmp, "facilities_k_k.gpkg")
        points = os.path.join(tmp, "facilities_k_k.facility_points.gpkg")
        for f in (main, points):
            open(f, "w").close()
        base = {
            "workspace": "facilities_proximity",
            "style_name": None,
            "layer_type": "vector",
            "state": "s",
            "district": "d",
            "block": "b",
        }
        return [
            {**base, "layer_id": 1, "layer_name": "facilities_k_k", "path": main},
            {
                **base,
                "layer_id": 2,
                "layer_name": "facilities_k_k_tehsil_facility_collection",
                "path": points,
            },
            {
                **base,
                "layer_id": 3,
                "layer_name": "facilities_k_k_village_nearest_facility_collection",
                "path": points,
            },
        ]

    @patch("computing.tasks.publish_local_layer.apply_async")
    def test_facility_point_layers_share_one_task(self, apply_async):
        import tempfile

        from computing.pending_geoserver_sync import _queue_local_entries

        apply_async.return_value.id = "t"
        with tempfile.TemporaryDirectory() as tmp:
            summary = _queue_local_entries(self._entries(tmp), dry_run=False)

        self.assertEqual(summary["queued"], 3)
        self.assertEqual(apply_async.call_count, 2)
        kwargs = [c.kwargs["kwargs"] for c in apply_async.call_args_list]
        main = next(k for k in kwargs if k["layer_name"] == "facilities_k_k")
        points = next(k for k in kwargs if k["layer_name"] != "facilities_k_k")
        self.assertEqual(main["gpkg_layers"], {"facilities_k_k": None})
        self.assertEqual(points["store_name"], "facilities_k_k_facility_points")
        self.assertEqual(
            points["gpkg_layers"],
            {
                "facilities_k_k_tehsil_facility_collection": "tehsil_facility_collection",
                "facilities_k_k_village_nearest_facility_collection": "village_nearest_facility_collection",
            },
        )
        self.assertEqual(points["extra_layer_ids"], [3])

    @patch("computing.utils.update_layer_sync_status", return_value=1)
    @patch("utilities.pipelines.publish.publish_gpkg_layers")
    def test_publish_local_layer_publishes_gpkg_tables_and_flips_all_flags(
        self, publish, update_sync
    ):
        from computing.tasks import publish_local_layer

        publish_local_layer.run(
            layer_type="vector",
            path="/x/f.facility_points",
            layer_name="a",
            workspace="ws",
            file_type="gpkg",
            layer_id=2,
            gpkg_layers={"a": "t1", "b": "t2"},
            store_name="f_facility_points",
            extra_layer_ids=[3],
        )

        publish.assert_called_once()
        self.assertEqual(publish.call_args.args[0], "/x/f.facility_points.gpkg")
        self.assertEqual(publish.call_args.kwargs["store_name"], "f_facility_points")
        self.assertEqual(
            [c.kwargs["layer_id"] for c in update_sync.call_args_list], [2, 3]
        )


class PushGeeAssetCommandTests(SimpleTestCase):
    ARGS = (
        "push_gee_asset",
        "--asset=projects/p/assets/zoi_ci",
        "--layer-name=kullu_kullu_intensity_ZOI",
        "--workspace=crop_intensity",
    )

    @patch("computing.tasks.publish_gee_layer.apply_async")
    def test_queues_vector_push_without_a_layer_row(self, apply_async):
        from django.core.management import call_command

        apply_async.return_value.id = "t1"

        call_command(*self.ARGS, "--folder=kullu_kullu", "--gee-account-id=22")

        kwargs = apply_async.call_args.kwargs
        self.assertEqual(kwargs["queue"], "geoserver")
        self.assertEqual(
            kwargs["kwargs"],
            {
                "layer_type": "vector",
                "asset_id": "projects/p/assets/zoi_ci",
                "layer_name": "kullu_kullu_intensity_ZOI",
                "workspace": "crop_intensity",
                "state": "kullu_kullu",
                "style_name": None,
                "layer_id": None,
                "gee_account_id": 22,
            },
        )

    @patch("computing.tasks.publish_gee_layer.apply_async")
    def test_rejects_non_gee_asset(self, apply_async):
        from django.core.management import CommandError, call_command

        with self.assertRaises(CommandError):
            call_command(
                "push_gee_asset",
                "--asset=data/local.gpkg",
                "--layer-name=x",
                "--workspace=w",
            )
        apply_async.assert_not_called()


class DataDirPathRebaseTests(SimpleTestCase):
    def _entry(self, **kw):
        base = {
            "layer_id": 1,
            "layer_name": "kullu_kullu_cluster",
            "path": "",
            "workspace": "terrain",
            "style_name": None,
            "layer_type": "vector",
            "state": "Himachal Pradesh",
            "district": "Kullu",
            "block": "Kullu",
        }
        base.update(kw)
        return base

    def test_absolute_path_from_other_data_dir_is_rebased(self):
        import tempfile
        from pathlib import Path

        from computing import config_loader
        from computing.pending_geoserver_sync import _publish_args

        with tempfile.TemporaryDirectory() as tmp:
            target = (
                Path(tmp)
                / "layers"
                / "himachal_pradesh"
                / "kullu"
                / "kullu"
                / "terrain"
                / "terrain_clusters_local"
                / "kullu_kullu_cluster.gpkg"
            )
            target.parent.mkdir(parents=True)
            target.touch()
            stale_abs = (
                "/var/tmp/other-core-stack-data/layers/himachal_pradesh/kullu/kullu/"
                "terrain/terrain_clusters_local/kullu_kullu_cluster.gpkg"
            )
            with patch.object(config_loader, "DATA_DIR", Path(tmp)):
                args = _publish_args(self._entry(path=stale_abs))
        self.assertIsNotNone(args)
        self.assertEqual(args["path"], str(target.with_suffix("")))
        self.assertEqual(args["file_type"], "gpkg")

    def test_active_data_dir_takes_precedence_over_old_absolute_path(self):
        import tempfile
        from pathlib import Path

        from computing import config_loader
        from computing.pending_geoserver_sync import _publish_args

        with tempfile.TemporaryDirectory() as old_root, tempfile.TemporaryDirectory() as new_root:
            rel = Path(
                "layers/himachal_pradesh/kullu/kullu/terrain/terrain_clusters_local/kullu_kullu_cluster.gpkg"
            )
            old_file = Path(old_root) / rel
            new_file = Path(new_root) / rel
            old_file.parent.mkdir(parents=True)
            new_file.parent.mkdir(parents=True)
            old_file.touch()
            new_file.touch()
            with patch.object(config_loader, "DATA_DIR", Path(new_root)):
                args = _publish_args(self._entry(path=str(old_file)))
        self.assertIsNotNone(args)
        self.assertEqual(args["path"], str(new_file.with_suffix("")))

    def test_legacy_layout_falls_back_to_config_yaml_layer_dir(self):
        import tempfile
        from pathlib import Path

        from computing import config_loader
        from computing.pending_geoserver_sync import _publish_args

        with tempfile.TemporaryDirectory() as tmp:
            # New layout on disk under DATA_DIR
            terrain_target = (
                Path(tmp)
                / "layers"
                / "himachal_pradesh"
                / "kullu"
                / "kullu"
                / "terrain"
                / "terrain_clusters_local"
                / "kullu_kullu_cluster.gpkg"
            )
            terrain_target.parent.mkdir(parents=True)
            terrain_target.touch()

            drought_target = (
                Path(tmp)
                / "layers"
                / "bihar"
                / "banka"
                / "banka"
                / "drought_causality"
                / "banka_banka_drought_causality.gpkg"
            )
            drought_target.parent.mkdir(parents=True)
            drought_target.touch()

            with patch.object(config_loader, "DATA_DIR", Path(tmp)):
                terrain_args = _publish_args(
                    self._entry(
                        path="data/terrain/terrain_clusters_local/himachal_pradesh/kullu/kullu/kullu_kullu_cluster.gpkg"
                    )
                )
                drought_args = _publish_args(
                    self._entry(
                        layer_name="banka_banka_drought_causality",
                        workspace="drought_causality",
                        state="Bihar",
                        district="Banka",
                        block="Banka",
                        path="/app/data/fc_to_shape/Bihar/banka_banka_drought_causality.gpkg",
                    )
                )

        self.assertIsNotNone(terrain_args)
        self.assertEqual(terrain_args["path"], str(terrain_target.with_suffix("")))
        self.assertIsNotNone(drought_args)
        self.assertEqual(drought_args["path"], str(drought_target.with_suffix("")))

    def test_legacy_unconfigured_subdir_reorders_into_layers_dir(self):
        import tempfile
        from pathlib import Path

        from computing import config_loader
        from computing.pending_geoserver_sync import _publish_args

        with tempfile.TemporaryDirectory() as tmp:
            target = (
                Path(tmp)
                / "layers"
                / "himachal_pradesh"
                / "kullu"
                / "kullu"
                / "tree_health"
                / "ch"
                / "ch_kullu_kullu_2017_2022.tif"
            )
            target.parent.mkdir(parents=True)
            target.touch()

            entry = self._entry(
                layer_name="ch_kullu_kullu_2017_2022",
                workspace="tree_ch_raster",
                layer_type="raster",
                style_name="default_style",
                path="data/tree_health/ch/himachal_pradesh/kullu/kullu/ch_kullu_kullu_2017_2022.tif",
            )
            with patch.object(config_loader, "DATA_DIR", Path(tmp)):
                args = _publish_args(entry)
        self.assertIsNotNone(args)
        self.assertEqual(args["path"], str(target))
        self.assertEqual(args["style_name"], "tree_ch_style")

    def test_soil_health_raster_expansion_rebases_to_active_data_dir(self):
        import tempfile
        from pathlib import Path

        from computing import config_loader
        from computing.pending_geoserver_sync import (
            SOIL_HEALTH_NUTRIENTS,
            _expand_soil_health_raster,
            _publish_args,
        )

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "layers" / "himachal_pradesh" / "kullu" / "kullu" / "soil_health"
            out_dir.mkdir(parents=True)
            for n in SOIL_HEALTH_NUTRIENTS:
                (out_dir / f"kullu_kullu_soil_health_raster_{n}.tif").touch()

            entry = self._entry(
                layer_name="kullu_kullu_soil_health_raster",
                workspace="soil_health_raster",
                layer_type="raster",
                path="/old/host/data/layers/himachal_pradesh/kullu/kullu/soil_health/kullu_kullu_soil_health_raster_OC_OLM.tif",
            )
            with patch.object(config_loader, "DATA_DIR", Path(tmp)):
                expanded = _expand_soil_health_raster(entry)
                published = [_publish_args(e) for e in expanded]

        self.assertEqual(len(published), len(SOIL_HEALTH_NUTRIENTS))
        self.assertTrue(all(p is not None for p in published))
        self.assertTrue(all(p["path"].startswith(str(out_dir)) for p in published))

    def test_layer_output_present_rebases_and_checks_relative_paths(self):
        import tempfile
        from pathlib import Path
        from types import SimpleNamespace

        from computing import config_loader
        from computing.bulk_layer_generation import _layer_output_present

        location = {"state": "Himachal Pradesh", "district": "Kullu", "block": "Kullu"}
        dataset = SimpleNamespace(workspace="terrain", layer_type="vector")

        with tempfile.TemporaryDirectory() as tmp:
            target = (
                Path(tmp)
                / "layers"
                / "himachal_pradesh"
                / "kullu"
                / "kullu"
                / "terrain"
                / "terrain_clusters_local"
                / "kullu_kullu_cluster.gpkg"
            )
            target.parent.mkdir(parents=True)
            target.touch()

            relocated_layer = SimpleNamespace(
                gee_asset_path="/other/data/layers/himachal_pradesh/kullu/kullu/terrain/terrain_clusters_local/kullu_kullu_cluster.gpkg",
                dataset=dataset,
            )
            missing_relative_layer = SimpleNamespace(
                gee_asset_path="data/terrain/missing.gpkg",
                dataset=dataset,
            )
            gee_layer = SimpleNamespace(
                gee_asset_path="projects/p/assets/swb",
                dataset=dataset,
            )
            with patch.object(config_loader, "DATA_DIR", Path(tmp)):
                self.assertTrue(_layer_output_present(relocated_layer, location=location))
                self.assertFalse(_layer_output_present(missing_relative_layer, location=location))
                self.assertTrue(_layer_output_present(gee_layer, location=location))

    def test_fc_to_shape_dir_uses_data_dir_and_creates_parents(self):
        import tempfile
        from pathlib import Path

        from computing import config_loader
        from computing.utils import _fc_to_shape_dir

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(config_loader, "DATA_DIR", Path(tmp)):
                created = _fc_to_shape_dir("bihar")
            self.assertEqual(Path(created), Path(tmp) / "fc_to_shape" / "bihar")
            self.assertTrue(Path(created).is_dir())


class BulkSyncStacControlTests(SimpleTestCase):
    @patch("computing.utils.update_layer_sync_status", return_value=42)
    @patch("computing.local_compute_helper.push_local_raster_to_geoserver")
    def test_publish_local_layer_skip_stac_preserves_flag_and_skips_trigger(
        self, push_raster, update_sync
    ):
        push_raster.return_value = {"status_code": 201}

        publish_local_layer.run(
            layer_type="raster",
            path="/tmp/layer.tif",
            layer_name="layer",
            workspace="workspace",
            layer_id=42,
            skip_stac=True,
        )

        update_sync.assert_called_once_with(
            layer_id=42,
            sync_to_geoserver=True,
            skip_stac=True,
        )

    @patch("computing.tasks.publish_local_layer.apply_async")
    def test_queue_local_entries_sets_skip_stac_by_default(self, apply_async):
        import tempfile
        from pathlib import Path

        from computing.pending_geoserver_sync import _queue_local_entries

        apply_async.return_value.id = "t"
        with tempfile.TemporaryDirectory() as tmp:
            tif = Path(tmp) / "layer.tif"
            tif.touch()
            entry = {
                "layer_id": 10,
                "layer_name": "layer",
                "path": str(tif),
                "workspace": "ws",
                "style_name": None,
                "layer_type": "raster",
                "state": "s",
                "district": "d",
                "block": "b",
            }
            _queue_local_entries([entry], dry_run=False)
            self.assertTrue(apply_async.call_args.kwargs["kwargs"]["skip_stac"])

            apply_async.reset_mock()
            _queue_local_entries([entry], dry_run=False, skip_stac=False)
            self.assertNotIn("skip_stac", apply_async.call_args.kwargs["kwargs"])

    @patch("computing.STAC_specs.stac_collection.generate_stac_collection_task.apply_async")
    @patch("computing.signals.stac_task_kwargs_for_layer")
    def test_signal_respects_skip_flag_setting_and_update_fields(
        self, mock_kwargs, mock_apply_async
    ):
        from types import SimpleNamespace
        from django.test import override_settings

        from computing.signals import trigger_stac_on_geoserver_sync

        mock_kwargs.return_value = {
            "layer_type": "vector",
            "state": "Bihar",
            "district": "Banka",
            "block": "Banka",
            "layer_name": "terrain_clusters",
            "start_year": "",
            "end_year": "",
            "upload_to_s3": False,
            "overwrite_metadata": False,
            "layer_id": 9,
        }

        layer = SimpleNamespace(
            id=9,
            is_sync_to_geoserver=True,
            is_stac_specs_generated=False,
            _skip_stac_trigger=True,
        )
        trigger_stac_on_geoserver_sync(None, layer, created=False, update_fields=["is_sync_to_geoserver"])
        mock_apply_async.assert_not_called()

        layer._skip_stac_trigger = False
        trigger_stac_on_geoserver_sync(None, layer, created=False, update_fields=["misc"])
        mock_apply_async.assert_not_called()

        with override_settings(STAC_AUTO_TRIGGER_ON_SYNC=False):
            trigger_stac_on_geoserver_sync(
                None, layer, created=False, update_fields=["is_sync_to_geoserver"]
            )
            mock_apply_async.assert_not_called()

        with override_settings(STAC_AUTO_TRIGGER_ON_SYNC=True, STAC_CELERY_QUEUE="stac"):
            trigger_stac_on_geoserver_sync(
                None, layer, created=False, update_fields=["is_sync_to_geoserver"]
            )
            mock_apply_async.assert_called_once()
            self.assertEqual(mock_apply_async.call_args.kwargs["queue"], "stac")


class RasterStyleResolutionTests(SimpleTestCase):
    def test_resolves_placeholder_styles_for_all_raster_workspaces(self):
        from computing.pending_geoserver_sync import _resolve_raster_style

        cases = [
            ("LULC_level_3", "LULC_23_24_banka_banka_level_3", "default_style", "lulc_level_3_style"),
            ("LULC_level_2", "LULC_23_24_banka_banka_level_2", None, "lulc_level_2_style"),
            ("LULC_level_1", "LULC_23_24_banka_banka_level_1", "", "lulc_level_1_style"),
            ("terrain", "banka_banka_terrain_raster", "default_style", "terrain_raster"),
            ("restoration", "banka_banka_restoration_raster", "default_style", "restoration_style"),
            ("plantation", "banka_banka_site_suitability_raster", "default_style", "site_suitability"),
            ("clart", "banka_banka_clart", "default_style", "testClart"),
            ("stream_order", "banka_banka_stream_order_raster", "default_style", "stream_order"),
            ("tree_ch_raster", "banka_banka_ch_2022", "default_style", "tree_ch_style"),
            ("tree_ccd_raster", "banka_banka_ccd_2022", "default_style", "tree_ccd_style"),
            ("tree_overall_raster", "banka_banka_overall", "default_style", "tree_overall_style"),
            ("change_detection", "change_banka_banka_Urbanization", "default_style", "urbanization"),
            ("change_detection", "change_banka_banka_Afforestation", "default_style", "afforestation"),
            ("change_detection", "change_banka_banka_CropIntensity", None, "cropintensity"),
            ("change_detection", "change_banka_banka_ShrubChange", "", "shrubchange"),
            ("soil_health_raster", "banka_banka_soil_health_raster_N", "default_style", None),
            # Explicit non-placeholder styles are preserved
            ("LULC_level_3", "layer", "custom_lulc_style", "custom_lulc_style"),
        ]
        for ws, name, raw, expected in cases:
            with self.subTest(ws=ws, name=name, raw=raw):
                self.assertEqual(_resolve_raster_style(ws, name, raw), expected)

    @patch("computing.tasks.publish_gee_layer.apply_async")
    @patch("computing.pending_geoserver_sync._gee_skip_reason", return_value=None)
    @patch("computing.pending_geoserver_sync._gee_layer_kind")
    def test_queue_gee_entries_resolves_raster_and_strips_vector_placeholder_style(
        self, mock_kind, _mock_skip, mock_apply_async
    ):
        from computing.pending_geoserver_sync import _queue_gee_entries

        mock_apply_async.return_value.id = "t-gee"
        mock_kind.side_effect = ["raster", "vector"]
        entries = [
            {
                "layer_id": 1,
                "layer_name": "LULC_23_24_banka_banka_level_3",
                "path": "projects/e/a/lulc",
                "workspace": "LULC_level_3",
                "style_name": "default_style",
                "layer_type": "raster",
                "state": "Bihar",
                "district": "Banka",
                "block": "Banka",
            },
            {
                "layer_id": 2,
                "layer_name": "banka_banka_mws",
                "path": "projects/e/a/mws",
                "workspace": "filtered_mws",
                "style_name": "default_style",
                "layer_type": "vector",
                "state": "Bihar",
                "district": "Banka",
                "block": "Banka",
            },
        ]

        _queue_gee_entries(entries, dry_run=False, gee_account_id="acc")
        self.assertEqual(mock_apply_async.call_count, 2)
        raster_kwargs = mock_apply_async.call_args_list[0].kwargs["kwargs"]
        vector_kwargs = mock_apply_async.call_args_list[1].kwargs["kwargs"]
        self.assertEqual(raster_kwargs["style_name"], "lulc_level_3_style")
        self.assertIsNone(vector_kwargs["style_name"])

    def test_push_raster_applies_style_on_http_200_overwrite(self):
        from unittest.mock import MagicMock

        from computing.local_compute_helper import _push_raster_to_geoserver_instance

        geo = MagicMock()
        geo.create_coveragestore.return_value = {"status_code": 200}
        geo.get_style.return_value = {"style": {"name": "lulc_level_3_style"}}
        geo.publish_style.return_value = 200
        geo.get_layer.return_value = {"layer": {"name": "l"}}

        res = _push_raster_to_geoserver_instance(
            geo, "/tmp/a.tif", "l", "LULC_level_3", "lulc_level_3_style"
        )
        self.assertEqual(res["status_code"], 200)
        geo.configure_coverage.assert_called_once()
        geo.publish_style.assert_called_once_with(
            layer_name="l", style_name="lulc_level_3_style", workspace="LULC_level_3"
        )

    def test_push_raster_deletes_orphaned_store_and_retries_on_http_500(self):
        from unittest.mock import MagicMock

        from computing.local_compute_helper import _push_raster_to_geoserver_instance
        from utilities.geoserver_utils import GeoserverException

        geo = MagicMock()
        geo.create_coveragestore.side_effect = [
            GeoserverException(500, b"java.lang.NullPointerException"),
            {"status_code": 201},
        ]
        geo.get_style.return_value = {"style": {"name": "terrain_raster"}}
        geo.publish_style.return_value = 200
        geo.get_layer.return_value = {"layer": {"name": "kullu_kullu_terrain_raster"}}

        res = _push_raster_to_geoserver_instance(
            geo,
            "/tmp/kullu_kullu_terrain_raster.tif",
            "kullu_kullu_terrain_raster",
            "terrain",
            "terrain_raster",
        )
        self.assertEqual(res["status_code"], 201)
        self.assertEqual(geo.create_coveragestore.call_count, 2)
        geo.delete_raster_store.assert_called_once_with(
            workspace="terrain", store="kullu_kullu_terrain_raster"
        )
        geo.configure_coverage.assert_called_once()
        geo.publish_style.assert_called_once_with(
            layer_name="kullu_kullu_terrain_raster",
            style_name="terrain_raster",
            workspace="terrain",
        )

    def test_push_raster_self_heals_orphaned_store_returning_200_without_layer(self):
        from unittest.mock import MagicMock

        from computing.local_compute_helper import _push_raster_to_geoserver_instance
        from utilities.geoserver_utils import GeoserverException

        geo = MagicMock()
        geo.create_coveragestore.side_effect = [
            {"status_code": 200},
            {"status_code": 201},
        ]
        geo.get_layer.side_effect = [
            GeoserverException(404, b"No such layer"),
            {"layer": {"name": "overall_change_raster_kullu_kullu"}},
        ]
        geo.get_style.return_value = {"style": {"name": "tree_overall_style"}}
        geo.publish_style.return_value = 200

        res = _push_raster_to_geoserver_instance(
            geo,
            "/tmp/overall_change_raster_kullu_kullu.tif",
            "overall_change_raster_kullu_kullu",
            "tree_overall_raster",
            "tree_overall_style",
        )
        self.assertEqual(res["status_code"], 201)
        self.assertEqual(geo.create_coveragestore.call_count, 2)
        geo.delete_raster_store.assert_called_once_with(
            workspace="tree_overall_raster", store="overall_change_raster_kullu_kullu"
        )
        geo.configure_coverage.assert_called_once()
        geo.publish_style.assert_called_once_with(
            layer_name="overall_change_raster_kullu_kullu",
            style_name="tree_overall_style",
            workspace="tree_overall_raster",
        )

    def test_push_raster_cleans_up_orphaned_store_when_layer_verification_fails(self):
        from unittest.mock import MagicMock

        from computing.local_compute_helper import _push_raster_to_geoserver_instance
        from utilities.geoserver_utils import GeoserverException

        geo = MagicMock()
        geo.create_coveragestore.return_value = {"status_code": 201}
        geo.get_layer.side_effect = GeoserverException(404, b"No such layer")

        res = _push_raster_to_geoserver_instance(
            geo,
            "/tmp/kullu_kullu_terrain_raster.tif",
            "kullu_kullu_terrain_raster",
            "terrain",
            None,
        )
        self.assertEqual(res["status_code"], 500)
        self.assertEqual(geo.create_coveragestore.call_count, 2)
        self.assertEqual(geo.delete_raster_store.call_count, 2)
        geo.publish_style.assert_not_called()
