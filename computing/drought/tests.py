"""Tests for the local drought layer of a tehsil (cut from the pan-India MWS layers), its API, and the
pan-India APIs that download the inputs and make the pan-India layers.

Run in the container:
    docker compose exec backend python manage.py test computing.drought.tests
"""

import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

import fiona
import geopandas as gpd
import pandas as pd
from django.test import override_settings
from django.urls import reverse
from rest_framework.test import APISimpleTestCase
from shapely.geometry import box, mapping

from computing.drought import drought_local_compute as dl
from computing.drought.local.config import Paths


def write_season(paths, year, rows):
    """A small pan-India season layer; rows are (uid, drlb, drysp, w_mod)."""
    path = dl.season_layer_path(year, paths)
    path.parent.mkdir(parents=True, exist_ok=True)
    rd = f"rd{year % 100}-5-7"
    columns = {f"drlb_{year}": "str", f"drysp_{year}": "int", f"w_mod_{year}": "int", f"pcr_k_{year}": "float", rd: "float"}
    schema = {"geometry": "Polygon", "properties": {**dl.drought_mws.ATTRIBUTES, **columns}}
    with fiona.open(path, "w", driver="GPKG", layer=path.stem, crs="EPSG:4326", schema=schema) as dst:
        for i, (uid, drlb, drysp, w_mod) in enumerate(rows):
            properties = {"uid": uid, "area_in_ha": 100.0, "bacode": "1", "sbcode": "A", "wsconc": "B1A01",
                          f"drlb_{year}": drlb, f"drysp_{year}": drysp, f"w_mod_{year}": w_mod,
                          f"pcr_k_{year}": 95.5, rd: -10.0}
            dst.write({"geometry": mapping(box(i, 0, i + 1, 1)), "properties": properties})


class TehsilDroughtTests(TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.paths = Paths(self.tmp, self.tmp)
        write_season(self.paths, 2017, [("1_1", "[0,2,2]", 4, 2), ("1_2", None, 0, None), ("9_9", "[1]", 0, 0)])
        write_season(self.paths, 2018, [("1_1", "[3]", 6, 0), ("1_2", None, 0, None)])
        self.watersheds = gpd.GeoDataFrame(
            {"uid": ["1_2", "1_1"], "area_in_ha": [50.0, 60.0]},
            geometry=[box(1, 0, 2, 1), box(0, 0, 1, 1)],
            crs="EPSG:4326",
        )

    def test_joins_every_season_by_uid_in_tehsil_order(self):
        got = dl.tehsil_drought(self.watersheds, 2017, 2018, self.paths)
        self.assertEqual(got.uid.tolist(), ["1_2", "1_1"])
        self.assertEqual((got.loc[1, "drlb_2017"], got.loc[1, "drlb_2018"]), ("[0,2,2]", "[3]"))
        self.assertEqual(got.loc[1, "rd17-5-7"], -10.0)
        self.assertEqual(got.loc[1, "avg_dryspell"], 5.0)  # mean of drysp, as the GEE merge
        self.assertEqual(got.crs.to_epsg(), 4326)

    def test_mws_without_cropland_gets_an_empty_label_list(self):
        got = dl.tehsil_drought(self.watersheds, 2017, 2018, self.paths)
        self.assertEqual(got.loc[0, "drlb_2017"], "[]")
        self.assertTrue(pd.isna(got.loc[0, "w_mod_2017"]))
        self.assertEqual(str(got["w_mod_2017"].dtype), "Int64")

    def test_uid_missing_from_a_season_is_an_error(self):
        watersheds = self.watersheds.assign(uid=["1_2", "7_7"])
        with self.assertRaises(ValueError):
            dl.tehsil_drought(watersheds, 2017, 2018, self.paths)

    def test_default_range_and_missing_seasons(self):
        self.assertEqual(dl.season_range(None, None, self.paths), (2017, 2018))
        self.assertEqual(dl.missing_drought_mws_layers(2017, 2019, self.paths), [2019])

    def test_publishes_the_gee_layer_name_in_the_drought_workspace(self):
        with patch.object(dl, "load_precomputed_watersheds", return_value=(self.watersheds, "/tmp/w.gpkg")), \
                patch.object(dl, "save_layer_info_to_db", return_value=7) as save, \
                patch.object(dl, "queue_local_vector_for_geoserver", return_value={"status_code": 202}) as queue:
            ok = dl.run_drought_local("Karnataka", "Raichur", "Devadurga", 2017, 2018,
                                      paths=self.paths, output_base_dir=self.tmp / "out")
        self.assertTrue(ok)
        out = self.tmp / "out" / "karnataka" / "raichur" / "devadurga" / "raichur_devadurga_drought"
        queue.assert_called_once_with(path=str(out), layer_name="raichur_devadurga_drought",
                                      workspace="drought", file_type="gpkg", layer_id=7)
        kwargs = save.call_args.kwargs
        self.assertEqual((kwargs["dataset_name"], kwargs["layer_name"]), ("Drought", "raichur_devadurga_drought"))
        self.assertEqual((kwargs["misc"]["start_year"], kwargs["misc"]["end_year"]), (2017, 2018))
        self.assertTrue(kwargs["misc"]["is_generated_locally"])
        with fiona.open(f"{out}.gpkg") as src:
            self.assertEqual(len(src), 2)

    def test_missing_season_stops_before_any_output(self):
        with patch.object(dl, "load_precomputed_watersheds") as load:
            with self.assertRaises(FileNotFoundError):
                dl.run_drought_local("Karnataka", "Raichur", "Devadurga", 2017, 2019, paths=self.paths)
        load.assert_not_called()


class DroughtAPITests(APISimpleTestCase):
    payload = {"state": "Karnataka", "district": "Raichur", "block": "Devadurga",
               "start_year": 2017, "end_year": 2024, "gee_account_id": 1}

    def setUp(self):
        self.client.force_authenticate(user=MagicMock(is_authenticated=True))

    def kwargs(self):
        return {key: self.payload[key] for key in ("state", "district", "block", "start_year", "end_year", "gee_account_id")}

    @patch("computing.api.missing_drought_mws_layers", return_value=[])
    @patch("computing.api.calculate_drought_local.apply_async")
    def test_local_compute_queues_the_clip(self, apply_async, _):
        response = self.client.post(reverse("generate_drought_layer"), {**self.payload, "compute": "local"}, format="json")
        self.assertEqual(response.status_code, 200)
        apply_async.assert_called_once_with(kwargs=self.kwargs(), queue="nrm")

    @patch("computing.api.missing_drought_mws_layers", return_value=[2024])
    @patch("computing.api.calculate_drought_local.apply_async")
    def test_missing_pan_india_layer_is_rejected(self, apply_async, _):
        response = self.client.post(reverse("generate_drought_layer"), {**self.payload, "compute": "local"}, format="json")
        self.assertEqual(response.status_code, 400)
        self.assertIn("2024", response.data["Exception"])
        apply_async.assert_not_called()

    @patch("computing.api.calculate_drought.apply_async")
    def test_gee_stays_the_default(self, apply_async):
        response = self.client.post(reverse("generate_drought_layer"), self.payload, format="json")
        self.assertEqual(response.status_code, 200)
        apply_async.assert_called_once_with(kwargs=self.kwargs(), queue="nrm")


@override_settings(HEAVY_WORKER_ENABLED=True, HEAVY_TASK_QUEUE="heavy")
class PanIndiaDroughtInputsAPITests(APISimpleTestCase):
    def setUp(self):
        self.client.force_authenticate(user=MagicMock(is_authenticated=True))
        patcher = patch("computing.api.drought_inputs_not_writable", return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def post(self, payload):
        return self.client.post(reverse("pan_india_drought_inputs"), payload, format="json")

    @patch("computing.api.drought_download_in_progress", return_value=False)
    @patch("computing.api.download_drought_inputs_task.apply_async")
    def test_queues_on_the_heavy_worker(self, apply_async, _):
        apply_async.return_value = MagicMock(id="task-1")
        response = self.post({"products": "chirps,mod16a2gf", "end_year": 2026})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["task_id"], "task-1")
        apply_async.assert_called_once_with(
            kwargs={"products": ["chirps", "mod16a2gf"], "end_year": 2026, "dry_run": False}, queue="heavy")

    @patch("computing.api.download_drought_inputs_task.apply_async")
    def test_bad_requests_are_refused(self, apply_async):
        for payload in ({"state": "karnataka"}, {"products": ["foo"]}, {"compute": "gee"}, {"pan_india": True}):
            self.assertEqual(self.post(payload).status_code, 400, payload)
        apply_async.assert_not_called()

    @patch("computing.api.drought_download_in_progress", return_value=True)
    @patch("computing.api.download_drought_inputs_task.apply_async")
    def test_running_download_is_409(self, apply_async, _):
        self.assertEqual(self.post({}).status_code, 409)
        apply_async.assert_not_called()

    @patch("computing.api.drought_download_in_progress", return_value=False)
    @patch("computing.api.download_drought_inputs_task.apply_async")
    def test_folders_of_another_user_are_reported_at_once(self, apply_async, _):
        with patch("computing.api.drought_inputs_not_writable", return_value="Not writable ... chown -R 1001:1001"):
            response = self.post({})
        self.assertEqual(response.status_code, 500)
        self.assertIn("chown", response.data["Exception"])
        apply_async.assert_not_called()

    @override_settings(HEAVY_WORKER_ENABLED=False)
    @patch("computing.api.download_drought_inputs_task.apply_async")
    def test_no_heavy_worker_is_503(self, apply_async):
        self.assertEqual(self.post({}).status_code, 503)
        apply_async.assert_not_called()


@override_settings(HEAVY_WORKER_ENABLED=True, HEAVY_TASK_QUEUE="heavy")
class PanIndiaDroughtAPITests(APISimpleTestCase):
    def setUp(self):
        self.client.force_authenticate(user=MagicMock(is_authenticated=True))
        for name in ("drought_run_in_progress", "drought_not_writable"):
            patcher = patch(f"computing.api.{name}", return_value=None if name == "drought_not_writable" else False)
            patcher.start()
            self.addCleanup(patcher.stop)

    def post(self, payload):
        return self.client.post(reverse("pan_india_drought"), payload, format="json")

    @patch("computing.api.drought_status.season_states", return_value=[(2025, "waiting", "LULC: lulc_v3_2025_2026.tif not found")])
    @patch("computing.api.drought_status.stage_can_run", side_effect=lambda paths, stage, season: season <= 2024)
    @patch("computing.api.generate_drought_pan_india_task.apply_async")
    def test_queues_the_ready_seasons(self, apply_async, _, __):
        apply_async.return_value = MagicMock(id="task-2")
        response = self.post({"start_year": 2023, "end_year": 2025, "rerun_stages": ["mws"]})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["seasons"], [2023, 2024])
        self.assertIn("2025", {str(k) for k in response.data["waiting"]})
        apply_async.assert_called_once_with(
            kwargs={"seasons": [2023, 2024], "stages": ["rain", "cropland", "vegetation", "mai", "classify", "mws"],
                    "rerun_stages": ["mws"]},
            queue="heavy")

    @patch("computing.api.drought_status.season_states", return_value=[(2025, "waiting", "LULC: lulc_v3_2025_2026.tif not found")])
    @patch("computing.api.drought_status.stage_can_run", return_value=False)
    @patch("computing.api.generate_drought_pan_india_task.apply_async")
    def test_no_ready_season_is_400(self, apply_async, _, __):
        response = self.post({"start_year": 2025, "end_year": 2025})
        self.assertEqual(response.status_code, 400)
        self.assertIn("lulc_v3_2025_2026", response.data["Exception"])
        apply_async.assert_not_called()

    @patch("computing.api.generate_drought_pan_india_task.apply_async")
    def test_bad_requests_are_refused(self, apply_async):
        for payload in ({"stages": "rain", "rerun_stages": "mws"}, {"stages": "foo"}, {"start_year": 2016},
                        {"start_year": 2024, "end_year": 2023}, {"district": "raichur"}):
            self.assertEqual(self.post(payload).status_code, 400, payload)
        apply_async.assert_not_called()

    @patch("computing.api.generate_drought_pan_india_task.apply_async")
    def test_folders_of_another_user_are_reported_at_once(self, apply_async):
        with patch("computing.api.drought_not_writable", return_value="Not writable ... chown -R 1001:1001"):
            response = self.post({})
        self.assertEqual(response.status_code, 500)
        apply_async.assert_not_called()

    @patch("computing.api.generate_drought_pan_india_task.apply_async")
    def test_running_pipeline_is_409(self, apply_async):
        with patch("computing.api.drought_run_in_progress", return_value=True):
            self.assertEqual(self.post({}).status_code, 409)
        apply_async.assert_not_called()

