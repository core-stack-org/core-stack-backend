"""Tests for the local drought and drought causality layers of a tehsil (cut from the pan-India MWS
layers), their APIs, and the pan-India APIs that download the inputs and make the pan-India layers.

Run in the container:
    docker compose exec backend python manage.py test computing.drought.tests
"""

import json
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

from computing.drought import drought_causality_local as dc
from computing.drought import drought_local_compute as dl
from computing.drought.local.config import Paths

CAUSE = "moderate: scanty rain + SPI; VCI moderate, MAI severe, area sown normal"


def write_season(paths, year, rows, causality=True, rd_value=-10.0, sown=95.5):
    """A small pan-India season layer; rows are (uid, drought_labels, dryspell_length, moderate weeks).
    MWS 1_1 is in a drought year (causality filled); the others are not. rd_value and sown: every
    MWS's rainfall deviation of the 21 May window and area sown (None: empty)."""
    path = dl.season_layer_path(year, paths)
    path.parent.mkdir(parents=True, exist_ok=True)
    rd = f"monthly_rainfall_deviation_{year}-5-21"
    vci, rfdev = f"vci_{year}-7-9", f"rfdev_class_{year}-7-9"  # per-window values, not in the tehsil layer
    sm, mild = f"severe_moderate_drought_causality_{year}", f"mild_drought_causality_{year}"
    columns = {f"drought_labels_{year}": "str", f"dryspell_length_{year}": "int",
               f"number_of_weeks_in_moderate_drought_{year}": "int", f"percent_of_area_cropped_kharif_{year}": "float",
               rd: "float", vci: "float", rfdev: "str", **({sm: "str", mild: "str"} if causality else {})}
    schema = {"geometry": "Polygon", "properties": {**dl.drought_mws.ATTRIBUTES, **columns}}
    with fiona.open(path, "w", driver="GPKG", layer=path.stem, crs="EPSG:4326", schema=schema) as dst:
        for i, (uid, drlb, drysp, w_mod) in enumerate(rows):
            properties = {"uid": uid, "area_in_ha": 100.0, "bacode": "1", "sbcode": "A", "wsconc": "B1A01",
                          f"drought_labels_{year}": drlb, f"dryspell_length_{year}": drysp,
                          f"number_of_weeks_in_moderate_drought_{year}": w_mod,
                          f"percent_of_area_cropped_kharif_{year}": sown, rd: rd_value, vci: 55.0, rfdev: "Normal rf"}
            if causality:
                drought_year = uid == "1_1"
                properties[sm] = json.dumps({CAUSE: 2}) if drought_year else None
                properties[mild] = json.dumps({}) if drought_year else None
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
        self.assertEqual((got.loc[1, "drought_labels_2017"], got.loc[1, "drought_labels_2018"]), ("[0,2,2]", "[3]"))
        self.assertEqual(got.loc[1, "monthly_rainfall_deviation_2017-5-21"], -10.0)
        self.assertEqual(got.loc[1, "avg_dryspell"], 5.0)  # mean of dryspell_length, as the GEE merge
        self.assertEqual(got.crs.to_epsg(), 4326)

    def test_tehsil_layer_leaves_out_the_other_window_values_and_causality(self):
        got = dl.tehsil_drought(self.watersheds, 2017, 2018, self.paths)
        for name in ("vci_2017-7-9", "rfdev_class_2018-7-9", "severe_moderate_drought_causality_2017",
                     "mild_drought_causality_2018"):
            self.assertNotIn(name, got.columns)
        self.assertEqual(sorted(c for c in got.columns if "2017" in c),
                         ["drought_labels_2017", "dryspell_length_2017", "monthly_rainfall_deviation_2017-5-21",
                          "number_of_weeks_in_moderate_drought_2017", "percent_of_area_cropped_kharif_2017"])

    def test_mws_without_cropland_gets_an_empty_label_list(self):
        got = dl.tehsil_drought(self.watersheds, 2017, 2018, self.paths)
        self.assertEqual(got.loc[0, "drought_labels_2017"], "[]")
        moderate = "number_of_weeks_in_moderate_drought_2017"
        self.assertTrue(pd.isna(got.loc[0, moderate]))
        self.assertEqual(str(got[moderate].dtype), "Int64")

    def test_columns_empty_for_every_mws_keep_their_pan_india_type(self):
        # 2019: no value for either MWS of the tehsil (as a window before the tehsil's onset); the
        # written layer must still type them as the pan-India layer does, not as text
        write_season(self.paths, 2019, [("1_1", None, None, None), ("1_2", None, None, None)], rd_value=None, sown=None)
        got = dl.tehsil_drought(self.watersheds, 2019, 2019, self.paths)
        out = self.tmp / "tehsil.gpkg"
        dl.write_vector_output(got, out, "tehsil")
        with fiona.open(out) as src:
            kinds = {name: kind.split(":")[0] for name, kind in src.schema["properties"].items()}
        self.assertEqual(kinds["monthly_rainfall_deviation_2019-5-21"], "float")
        self.assertEqual(kinds["percent_of_area_cropped_kharif_2019"], "float")
        self.assertEqual(kinds["dryspell_length_2019"], "int")
        self.assertEqual(kinds["number_of_weeks_in_moderate_drought_2019"], "int")
        self.assertEqual(kinds["drought_labels_2019"], "str")

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


class TehsilCausalityTests(TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.paths = Paths(self.tmp, self.tmp)
        write_season(self.paths, 2017, [("1_1", "[0,2,2]", 4, 2), ("1_2", None, 0, None)])
        write_season(self.paths, 2018, [("1_1", "[3]", 6, 0), ("1_2", None, 0, None)])
        self.watersheds = gpd.GeoDataFrame(
            {"uid": ["1_2", "1_1"], "area_in_ha": [50.0, 60.0], "bacode": ["1", "1"]},
            geometry=[box(1, 0, 2, 1), box(0, 0, 1, 1)],
            crs="EPSG:4326",
        )

    def test_joins_the_causality_columns_only(self):
        got = dc.tehsil_causality(self.watersheds, 2017, 2018, self.paths)
        self.assertEqual(list(got.columns), ["uid", "area_in_ha", "geometry",
                                             "severe_moderate_drought_causality_2017", "mild_drought_causality_2017",
                                             "severe_moderate_drought_causality_2018", "mild_drought_causality_2018"])
        self.assertEqual(json.loads(got.loc[1, "severe_moderate_drought_causality_2018"]), {CAUSE: 2})
        self.assertEqual(json.loads(got.loc[1, "mild_drought_causality_2017"]), {})
        self.assertIsNone(got.loc[0, "severe_moderate_drought_causality_2017"])  # not a drought year

    def test_layer_made_before_causality_is_refused(self):
        write_season(self.paths, 2019, [("1_1", "[1]", 0, 0), ("1_2", None, 0, None)], causality=False)
        self.assertEqual(dc.seasons_without_causality(2017, 2019, self.paths), [2019])
        with self.assertRaisesRegex(ValueError, "made before drought causality"):
            dc.tehsil_causality(self.watersheds, 2017, 2019, self.paths)

    def test_publishes_the_gee_layer_name_in_the_drought_causality_workspace(self):
        with patch.object(dl, "load_precomputed_watersheds", return_value=(self.watersheds, "/tmp/w.gpkg")), \
                patch.object(dl, "save_layer_info_to_db", return_value=8) as save, \
                patch.object(dl, "queue_local_vector_for_geoserver", return_value={"status_code": 202}) as queue:
            ok = dc.run_drought_causality_local("Karnataka", "Raichur", "Devadurga", 2017, 2018,
                                                paths=self.paths, output_base_dir=self.tmp / "out")
        self.assertTrue(ok)
        out = self.tmp / "out" / "karnataka" / "raichur" / "devadurga" / "raichur_devadurga_drought_causality"
        queue.assert_called_once_with(path=str(out), layer_name="raichur_devadurga_drought_causality",
                                      workspace="drought_causality", file_type="gpkg", layer_id=8)
        kwargs = save.call_args.kwargs
        self.assertEqual((kwargs["dataset_name"], kwargs["algorithm"]), ("Drought Causality", "local_drought_causality"))
        self.assertTrue(kwargs["misc"]["is_generated_locally"])
        with fiona.open(f"{out}.gpkg") as src:
            self.assertEqual(len(src), 2)
            self.assertIn("severe_moderate_drought_causality_2018", src.schema["properties"])


class DroughtAPITests(APISimpleTestCase):
    payload = {"state": "Karnataka", "district": "Raichur", "block": "Devadurga",
               "start_year": 2017, "end_year": 2024, "gee_account_id": 1}

    def setUp(self):
        self.client.force_authenticate(user=MagicMock(is_authenticated=True))
        # the local tehsil boundary preflight (require_local_tehsil_boundaries) is tested elsewhere
        patcher = patch("computing.local_compute_helper.prepare_local_tehsil_boundaries")
        self.prepare = patcher.start()
        self.addCleanup(patcher.stop)

    def kwargs(self):
        return {key: self.payload[key] for key in ("state", "district", "block", "start_year", "end_year", "gee_account_id")}

    @patch("computing.api.missing_drought_mws_layers", return_value=[])
    @patch("computing.api.calculate_drought_local.apply_async")
    def test_local_compute_queues_the_clip(self, apply_async, _):
        response = self.client.post(reverse("generate_drought_layer"), {**self.payload, "compute": "local"}, format="json")
        self.assertEqual(response.status_code, 200)
        apply_async.assert_called_once_with(kwargs=self.kwargs(), queue="nrm")
        self.prepare.assert_called_once()  # boundaries checked before queueing

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
        self.prepare.assert_not_called()  # the boundary check is for local requests only


class TehsilBoundaryGuardTests(APISimpleTestCase):
    """Both drought tehsil APIs stop a local request whose tehsil boundaries are missing."""

    payload = {"state": "Karnataka", "district": "Raichur", "block": "Devadurga", "compute": "local"}

    def setUp(self):
        self.client.force_authenticate(user=MagicMock(is_authenticated=True))

    @patch("computing.api.drought_causality_local.apply_async")
    @patch("computing.api.calculate_drought_local.apply_async")
    def test_missing_boundary_stops_both_apis_before_queueing(self, drought, causality):
        from computing.local_compute_helper import TehsilBoundaryError

        error = TehsilBoundaryError("boundary_check", "Local boundaries missing for karnataka/raichur/devadurga")
        with patch("computing.local_compute_helper.prepare_local_tehsil_boundaries", side_effect=error):
            for name in ("generate_drought_layer", "mws_drought_causality"):
                response = self.client.post(reverse(name), self.payload, format="json")
                self.assertEqual(response.status_code, 400, name)
                self.assertEqual(response.data["stage"], "boundary_check", name)
        drought.assert_not_called()
        causality.assert_not_called()


@patch("computing.api.seasons_without_causality", return_value=[])
@patch("computing.api.missing_drought_mws_layers", return_value=[])
class DroughtCausalityAPITests(APISimpleTestCase):
    payload = {"state": "Karnataka", "district": "Raichur", "block": "Devadurga",
               "start_year": 2017, "end_year": 2024, "gee_account_id": 1}

    def setUp(self):
        self.client.force_authenticate(user=MagicMock(is_authenticated=True))
        # the local tehsil boundary preflight (require_local_tehsil_boundaries) is tested elsewhere
        patcher = patch("computing.local_compute_helper.prepare_local_tehsil_boundaries")
        self.prepare = patcher.start()
        self.addCleanup(patcher.stop)

    def post(self, payload):
        return self.client.post(reverse("mws_drought_causality"), payload, format="json")

    @patch("computing.api.drought_causality_local.apply_async")
    def test_local_compute_queues_the_clip(self, apply_async, *_):
        response = self.post({**self.payload, "compute": "local"})
        self.assertEqual(response.status_code, 200, response.data)
        apply_async.assert_called_once_with(
            kwargs={"state": "karnataka", "district": "raichur", "block": "devadurga",
                    "start_year": 2017, "end_year": 2024},
            queue="nrm")
        self.prepare.assert_called_once()  # boundaries checked before queueing

    @patch("computing.api.drought_causality_local.apply_async")
    def test_missing_or_old_pan_india_layers_are_rejected(self, apply_async, missing, without):
        missing.return_value = [2024]
        response = self.post({**self.payload, "compute": "local"})
        self.assertEqual(response.status_code, 400)
        self.assertIn("2024", response.data["Exception"])
        missing.return_value, without.return_value = [], [2017, 2018]
        response = self.post({**self.payload, "compute": "local"})
        self.assertEqual(response.status_code, 400)
        self.assertIn("before drought causality", response.data["Exception"])
        apply_async.assert_not_called()

    @patch("computing.api.drought_causality_local.apply_async")
    @patch("computing.api.drought_causality.apply_async")
    def test_bad_requests_are_refused(self, gee, local, *_):
        for payload in ({"district": "Raichur", "block": "Devadurga", "compute": "local"},
                        {**self.payload, "compute": "elsewhere"},
                        {"state": "Karnataka", "district": "Raichur", "block": "Devadurga"}):  # GEE needs the years
            self.assertEqual(self.post(payload).status_code, 400, payload)
        gee.assert_not_called()
        local.assert_not_called()

    @patch("computing.api.drought_causality.apply_async")
    def test_gee_stays_the_default(self, apply_async, *_):
        response = self.post(self.payload)
        self.assertEqual(response.status_code, 200)
        apply_async.assert_called_once_with(args=["karnataka", "raichur", "devadurga", 2017, 2024, 1], queue="nrm")


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

    @patch("computing.api.drought_status.season_states", return_value=[(2025, "waiting", "LULC: LULC_v4_new_panindia_2025-07-01_2026-06-30_temporal_corrected.tif not found")])
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

    @patch("computing.api.drought_status.season_states", return_value=[(2025, "waiting", "LULC: LULC_v4_new_panindia_2025-07-01_2026-06-30_temporal_corrected.tif not found")])
    @patch("computing.api.drought_status.stage_can_run", return_value=False)
    @patch("computing.api.generate_drought_pan_india_task.apply_async")
    def test_no_ready_season_is_400(self, apply_async, _, __):
        response = self.post({"start_year": 2025, "end_year": 2025})
        self.assertEqual(response.status_code, 400)
        self.assertIn("LULC_v4_new_panindia_2025-07-01_2026-06-30", response.data["Exception"])
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

