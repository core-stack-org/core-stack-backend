"""Unit tests: envelope, units, and JSON format for every public /api/v2/ route."""

import json
import re
from datetime import timedelta
from unittest.mock import patch

from django.test import SimpleTestCase, TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from geoadmin.models import StateSOI, UserAPIKey
from users.models import User
from utilities.openmeteo_format import (
    ACTIVE_LOCATION_FIELD_HINTS,
    ADMIN_DETAIL_FIELD_HINTS,
    GENERATED_LAYER_FIELD_UNITS,
    MWS_BY_LATLON_FIELD_HINTS,
    MWS_GEOMETRY_FIELD_HINTS,
    MWS_REPORT_FIELD_HINTS,
    VILLAGE_GEOMETRY_FIELD_HINTS,
    annual_structure_from_dict,
    error_envelope,
    flat_active_locations_payload,
    flat_admin_detail_payload,
    flat_generated_layers_payload,
    flat_kyl_indicator_payload,
    flat_mws_by_latlon_payload,
    flat_mws_geometry_payload,
    flat_mws_report_url_payload,
    flat_village_geometries_payload,
    success_envelope,
    tehsil_structure_from_dict,
)

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SUCCESS_ENVELOPE_KEYS = {"status", "error_message", "data"}

V2_NAMED_URLS = (
    "get_admin_details_by_lat_lon_v2",
    "get_mwsid_by_latlon_v2",
    "get_tehsil_data_v2",
    "get-mws-data-v2",
    "get_mws_kyl_indicators_v2",
    "get_generated_layer_urls_v2",
    "get_mws_report_urls_v2",
    "get_mws_geometries_v2",
    "get_village_geometries_v2",
    "get_active_locations_v2",
    "generate_waterbodies_data_v2",
    "generate_waterbody_data_v2",
)

GEO = {
    "state": "Rajasthan",
    "district": "Bhilwara",
    "tehsil": "Mandalgarh",
}
MWS = {**GEO, "mws_id": "12_100174"}
LATLON = {"latitude": "25.20231618101583", "longitude": "75.0868641493802"}


def assert_json_roundtrip(testcase, body):
    encoded = json.dumps(body, allow_nan=False)
    decoded = json.loads(encoded)
    testcase.assertEqual(decoded["status"], body["status"])
    return decoded


def assert_success_envelope(testcase, body, extra_keys=()):
    testcase.assertTrue(SUCCESS_ENVELOPE_KEYS.issubset(set(body.keys())))
    testcase.assertEqual(set(body.keys()), SUCCESS_ENVELOPE_KEYS | set(extra_keys))
    testcase.assertEqual(body["status"], "success")
    testcase.assertIsNone(body["error_message"])
    testcase.assertIn("data", body)
    return assert_json_roundtrip(testcase, body)


def assert_error_envelope(testcase, body):
    testcase.assertEqual(body["status"], "error")
    testcase.assertIn("error_message", body)
    testcase.assertIsInstance(body["error_message"], str)
    testcase.assertGreater(len(body["error_message"]), 0)
    json.dumps(body, allow_nan=False)


def assert_string_unit_map(testcase, unit_map, map_name):
    testcase.assertIsInstance(unit_map, dict, msg=map_name)
    testcase.assertGreater(len(unit_map), 0, msg=f"{map_name} must not be empty")
    for key, value in unit_map.items():
        testcase.assertIsInstance(key, str, msg=f"{map_name} key {key!r}")
        testcase.assertIsInstance(value, str, msg=f"{map_name}[{key}]={value!r}")


def assert_aligned_series(testcase, block, units, time_key="time", time_pattern=None):
    testcase.assertIsInstance(block, dict)
    testcase.assertIsInstance(units, dict)
    testcase.assertIn(time_key, block)
    times = block[time_key]
    testcase.assertIsInstance(times, list)
    testcase.assertGreater(len(times), 0)
    n = len(times)
    for t in times:
        testcase.assertIsInstance(t, str)
        if time_pattern is not None:
            testcase.assertRegex(t, time_pattern)
    for metric, series in block.items():
        if metric == time_key:
            continue
        testcase.assertIsInstance(series, list, msg=metric)
        testcase.assertEqual(len(series), n, msg=f"{metric} length != {n}")
        testcase.assertIn(metric, units, msg=f"missing unit for {metric}")
        for value in series:
            if value is None:
                continue
            testcase.assertTrue(
                isinstance(value, (int, float, str, dict, list)),
                msg=f"{metric} unexpected type {type(value)}",
            )
            if isinstance(value, float):
                testcase.assertEqual(value, round(value, 2), msg=metric)


class EnvelopeAndTransformerTests(SimpleTestCase):
    def test_success_envelope_keys_and_json(self):
        envelope = success_envelope({"ok": True})
        self.assertEqual(set(envelope.keys()), SUCCESS_ENVELOPE_KEYS)
        self.assertEqual(envelope["status"], "success")
        self.assertIsNone(envelope["error_message"])
        self.assertEqual(envelope["data"], {"ok": True})
        assert_json_roundtrip(self, envelope)

    def test_error_envelope_keys_and_json(self):
        envelope = error_envelope("missing params", details={"field": "state"})
        self.assertEqual(envelope["status"], "error")
        self.assertEqual(envelope["error_message"], "missing params")
        self.assertEqual(envelope["error"], "missing params")
        self.assertEqual(envelope["details"], {"field": "state"})
        json.dumps(envelope, allow_nan=False)

    def test_annual_structure_units_alignment_and_rounding(self):
        payload = annual_structure_from_dict(
            {
                "uid": "12_100174",
                "area_in_ha": 10.555,
                "precipitation_2017": 100.129,
                "precipitation_2018": 110.1,
                "g_2017": 5.019,
                "g_2018": 6.0,
            }
        )
        self.assertEqual(
            set(payload.keys()),
            {"metadata", "metadata_units", "annual", "annual_units"},
        )
        self.assertEqual(payload["metadata"]["uid"], "12_100174")
        self.assertEqual(payload["metadata"]["area_in_ha"], 10.56)
        self.assertEqual(payload["metadata_units"]["uid"], "id")
        self.assertEqual(payload["metadata_units"]["area_in_ha"], "ha")
        self.assertEqual(payload["annual"]["time"], ["2017", "2018"])
        self.assertEqual(payload["annual"]["precipitation"], [100.13, 110.1])
        self.assertEqual(payload["annual"]["g"], [5.02, 6.0])
        self.assertEqual(payload["annual_units"]["time"], "agricultural_year")
        self.assertEqual(payload["annual_units"]["precipitation"], "mm")
        self.assertEqual(payload["annual_units"]["g"], "mm")
        assert_aligned_series(self, payload["annual"], payload["annual_units"])
        json.dumps(payload, allow_nan=False)

    def test_antyodaya_et_token_does_not_match_market(self):
        payload = annual_structure_from_dict(
            {
                "vill_id": "123",
                "availability_of_markets_2017": 1,
                "internet_penetration_2017": 0.5,
            }
        )
        self.assertEqual(payload["annual_units"]["availability_of_markets"], "value")
        self.assertEqual(payload["annual_units"]["internet_penetration"], "value")
        self.assertNotEqual(payload["annual_units"]["availability_of_markets"], "mm")

    def test_tehsil_structure_has_data_and_units_maps(self):
        payload = tehsil_structure_from_dict(
            {
                "drought": [
                    {
                        "uid": "12_1",
                        "area_in_ha": 12.345,
                        "no_drought_2017": 10,
                        "no_drought_2018": 12,
                    }
                ],
                "stream_order": [
                    {"order_1_percent": 40.12, "order_2_percent": 59.88},
                ],
            }
        )
        self.assertEqual(set(payload.keys()), {"tehsil_data", "tehsil_units"})
        self.assertIn("drought", payload["tehsil_data"])
        self.assertIn("stream_order", payload["tehsil_data"])
        drought_row = payload["tehsil_data"]["drought"][0]
        self.assertIn("annual", drought_row)
        self.assertIn("annual_units", drought_row)
        self.assertEqual(drought_row["annual_units"]["no_drought"], "weeks")
        stream = payload["tehsil_data"]["stream_order"][0]
        self.assertEqual(stream["order"], ["1", "2"])
        self.assertEqual(payload["tehsil_units"]["stream_order"]["order"], "order")
        self.assertEqual(payload["tehsil_units"]["stream_order"]["value"], "%")
        json.dumps(payload, allow_nan=False)

    def test_flat_admin_detail_payload(self):
        payload = flat_admin_detail_payload(
            {"State": "Rajasthan", "District": "Bhilwara", "Tehsil": "Mandalgarh"}
        )
        self.assertEqual(set(payload.keys()), {"admin_details", "admin_field_hints"})
        self.assertEqual(payload["admin_details"]["State"], "Rajasthan")
        self.assertEqual(payload["admin_field_hints"], dict(ADMIN_DETAIL_FIELD_HINTS))

    def test_flat_mws_by_latlon_payload(self):
        payload = flat_mws_by_latlon_payload(
            {
                "uid": "12_100174",
                "State": "Rajasthan",
                "District": "Bhilwara",
                "Tehsil": "Mandalgarh",
            }
        )
        self.assertEqual(set(payload.keys()), {"mws_details", "mws_field_hints"})
        self.assertEqual(payload["mws_details"]["uid"], "12_100174")
        self.assertEqual(payload["mws_field_hints"], dict(MWS_BY_LATLON_FIELD_HINTS))

    def test_flat_mws_by_latlon_reads_mws_id_when_uid_missing(self):
        payload = flat_mws_by_latlon_payload(
            {
                "mws_id": "12_100174",
                "State": "RAJASTHAN",
                "District": "BHILWARA",
                "Tehsil": "MANDALGARH",
            }
        )
        self.assertEqual(payload["mws_details"]["uid"], "12_100174")

    def test_flat_kyl_indicator_payload_single_row(self):
        payload = flat_kyl_indicator_payload(
            [
                {
                    "mws_id": "12_100174",
                    "avg_precipitation": 800.129,
                    "cropping_intensity_avg": 1.5,
                }
            ]
        )
        self.assertEqual(set(payload.keys()), {"indicators", "indicator_units"})
        self.assertIsInstance(payload["indicators"], dict)
        self.assertEqual(payload["indicators"]["avg_precipitation"], 800.13)
        self.assertEqual(payload["indicator_units"]["mws_id"], "id")
        self.assertEqual(payload["indicator_units"]["avg_precipitation"], "mm")
        self.assertEqual(payload["indicator_units"]["cropping_intensity_avg"], "ratio")

    def test_flat_generated_layers_payload(self):
        payload = flat_generated_layers_payload(
            [
                {
                    "layer_name": "admin_boundary",
                    "dataset_name": "admin",
                    "layer_type": "vector",
                    "layer_url": "https://example.test/wfs",
                    "layer_version": "1.0",
                    "style_url": "",
                    "gee_asset_path": None,
                }
            ]
        )
        self.assertEqual(set(payload.keys()), {"layers", "layer_field_units"})
        self.assertEqual(len(payload["layers"]), 1)
        self.assertEqual(payload["layer_field_units"], dict(GENERATED_LAYER_FIELD_UNITS))

    def test_flat_report_geometry_village_and_locations(self):
        report = flat_mws_report_url_payload(
            {"Mws_report_url": "https://example.test/report.pdf"}
        )
        self.assertEqual(set(report.keys()), {"report", "report_field_hints"})
        self.assertEqual(report["report_field_hints"], dict(MWS_REPORT_FIELD_HINTS))

        geom = flat_mws_geometry_payload(
            {
                "uid": "12_100174",
                "state": "rajasthan",
                "district": "bhilwara",
                "tehsil": "mandalgarh",
                "geometry": {"type": "Polygon", "coordinates": []},
            }
        )
        self.assertEqual(set(geom.keys()), {"mws_geometry", "mws_geometry_field_hints"})
        self.assertEqual(
            geom["mws_geometry_field_hints"], dict(MWS_GEOMETRY_FIELD_HINTS)
        )

        villages = flat_village_geometries_payload(
            [
                {
                    "village_id": "101",
                    "village_name": "Sample",
                    "state": "rajasthan",
                    "district": "bhilwara",
                    "tehsil": "mandalgarh",
                    "geometry": {"type": "Polygon", "coordinates": []},
                }
            ]
        )
        self.assertEqual(set(villages.keys()), {"villages", "village_field_hints"})
        self.assertEqual(
            villages["village_field_hints"], dict(VILLAGE_GEOMETRY_FIELD_HINTS)
        )

        locations = flat_active_locations_payload(
            [{"label": "Rajasthan", "value": 1, "district": []}]
        )
        self.assertEqual(set(locations.keys()), {"locations", "location_field_hints"})
        self.assertEqual(
            locations["location_field_hints"], dict(ACTIVE_LOCATION_FIELD_HINTS)
        )


class V2NamedUrlTests(SimpleTestCase):
    def test_every_v2_route_reverses(self):
        for name in V2_NAMED_URLS:
            path = reverse(name)
            self.assertTrue(path.startswith("/api/v2/"), msg=name)


class V2ApiTestCase(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = User.objects.create_user(
            username="v2_format_tester",
            email="v2_format_tester@example.com",
            password="password123",
        )
        self.api_key_obj, self.api_key = UserAPIKey.objects.create_key(
            user=self.user,
            name="v2-format-test-key",
            expires_at=timezone.now() + timedelta(days=30),
        )
        self.api_key_obj.api_key = self.api_key
        self.api_key_obj.save()

    def _get(self, url_name, params=None, *, with_key=True):
        headers = {}
        if with_key:
            headers["HTTP_X_API_KEY"] = self.api_key
        return self.client.get(reverse(url_name), params or {}, **headers)


class AdminByLatLonV2Tests(V2ApiTestCase):
    url_name = "get_admin_details_by_lat_lon_v2"

    def test_missing_api_key_returns_401(self):
        response = self._get(self.url_name, LATLON, with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_params_returns_400_error_envelope(self):
        response = self._get(self.url_name, {"latitude": "25.45"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert_error_envelope(self, response.json())

    def test_invalid_lat_lon_returns_400(self):
        response = self._get(self.url_name, {"latitude": "abc", "longitude": "75"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert_error_envelope(self, response.json())

    @patch("public_api.api.get_location_info_by_lat_lon")
    def test_success_format(self, mock_lookup):
        mock_lookup.return_value = {
            "State": "Rajasthan",
            "District": "Bhilwara",
            "Tehsil": "Mandalgarh",
        }
        response = self._get(self.url_name, LATLON)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response["Content-Type"].split(";")[0], "application/json")
        body = response.json()
        assert_success_envelope(self, body)
        data = body["data"]
        self.assertEqual(set(data.keys()), {"admin_details", "admin_field_hints"})
        self.assertEqual(data["admin_details"]["District"], "Bhilwara")
        assert_string_unit_map(self, data["admin_field_hints"], "admin_field_hints")


class MwsByLatLonV2Tests(V2ApiTestCase):
    url_name = "get_mwsid_by_latlon_v2"

    def test_missing_api_key_returns_401(self):
        response = self._get(self.url_name, LATLON, with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_params_returns_400_error_envelope(self):
        response = self._get(self.url_name, {"longitude": "75.05"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert_error_envelope(self, response.json())

    @patch("public_api.api.get_mws_id_by_lat_lon")
    def test_success_format(self, mock_lookup):
        mock_lookup.return_value = {
            "mws_id": "12_100174",
            "State": "Rajasthan",
            "District": "Bhilwara",
            "Tehsil": "Mandalgarh",
        }
        response = self._get(self.url_name, LATLON)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        data = body["data"]
        self.assertEqual(set(data.keys()), {"mws_details", "mws_field_hints"})
        self.assertEqual(data["mws_details"]["uid"], "12_100174")
        assert_string_unit_map(self, data["mws_field_hints"], "mws_field_hints")


class TehsilDataV2Tests(V2ApiTestCase):
    url_name = "get_tehsil_data_v2"

    def test_missing_api_key_returns_401(self):
        response = self._get(self.url_name, GEO, with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_params_returns_400_error_envelope(self):
        response = self._get(self.url_name, {"state": "Rajasthan"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert_error_envelope(self, response.json())

    @patch("public_api.api.get_tehsil_json")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_success_format(self, _mock_exists, mock_json):
        mock_json.return_value = {
            "drought": [
                {
                    "uid": "12_1",
                    "area_in_ha": 12.345,
                    "no_drought_2017": 10,
                    "no_drought_2018": 12,
                }
            ]
        }
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        data = body["data"]
        self.assertEqual(set(data.keys()), {"tehsil_data", "tehsil_units"})
        self.assertIn("drought", data["tehsil_data"])
        drought_row = data["tehsil_data"]["drought"][0]
        self.assertIn("annual", drought_row)
        self.assertIn("annual_units", drought_row)
        assert_aligned_series(
            self, drought_row["annual"], drought_row["annual_units"]
        )


class KylIndicatorsV2Tests(V2ApiTestCase):
    url_name = "get_mws_kyl_indicators_v2"

    def test_missing_api_key_returns_401(self):
        response = self._get(self.url_name, MWS, with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_params_returns_400_error_envelope(self):
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert_error_envelope(self, response.json())

    def test_invalid_mws_id_returns_400(self):
        response = self._get(self.url_name, {**GEO, "mws_id": "bad-id"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert_error_envelope(self, response.json())

    @patch("public_api.api.get_mws_json_from_kyl_indicator")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_success_format(self, _mock_exists, mock_kyl):
        mock_kyl.return_value = [
            {
                "mws_id": "12_100174",
                "avg_precipitation": 800.129,
                "cropping_intensity_avg": 1.5,
                "drought_category": "mild",
            }
        ]
        response = self._get(self.url_name, MWS)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        data = body["data"]
        self.assertEqual(set(data.keys()), {"indicators", "indicator_units"})
        self.assertEqual(data["indicators"]["mws_id"], "12_100174")
        self.assertEqual(data["indicator_units"]["avg_precipitation"], "mm")
        self.assertEqual(data["indicator_units"]["cropping_intensity_avg"], "ratio")
        self.assertEqual(data["indicator_units"]["drought_category"], "category")
        assert_string_unit_map(self, data["indicator_units"], "indicator_units")


class GeneratedLayerUrlsV2Tests(V2ApiTestCase):
    url_name = "get_generated_layer_urls_v2"

    def test_missing_api_key_returns_401(self):
        response = self._get(self.url_name, GEO, with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_params_returns_400_error_envelope(self):
        response = self._get(self.url_name, {"state": "Rajasthan"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert_error_envelope(self, response.json())

    @patch("public_api.api.fetch_generated_layer_urls")
    def test_success_format(self, mock_layers):
        mock_layers.return_value = [
            {
                "layer_name": "admin_boundary",
                "dataset_name": "admin",
                "layer_type": "vector",
                "layer_url": "https://example.test/wfs",
                "layer_version": "1.0",
                "style_url": "",
                "gee_asset_path": None,
            }
        ]
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        data = body["data"]
        self.assertEqual(set(data.keys()), {"layers", "layer_field_units"})
        self.assertEqual(len(data["layers"]), 1)
        self.assertEqual(data["layer_field_units"], dict(GENERATED_LAYER_FIELD_UNITS))

    @patch("public_api.api.fetch_generated_layer_urls", return_value=[])
    def test_not_found_error_envelope(self, _mock_layers):
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        assert_error_envelope(self, response.json())

    @patch(
        "public_api.api.fetch_generated_layer_urls",
        side_effect=StateSOI.DoesNotExist("StateSOI matching query does not exist."),
    )
    def test_missing_state_soi_returns_404_not_500(self, _mock_layers):
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        body = response.json()
        assert_error_envelope(self, body)
        self.assertEqual(body["error_message"], "State, district, or tehsil not found.")


class MwsReportV2Tests(V2ApiTestCase):
    url_name = "get_mws_report_urls_v2"

    def test_missing_api_key_returns_401(self):
        response = self._get(self.url_name, MWS, with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_params_returns_400_error_envelope(self):
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert_error_envelope(self, response.json())

    @patch("public_api.api.generate_mws_report_url")
    def test_success_format(self, mock_report):
        mock_report.return_value = (
            {"Mws_report_url": "https://example.test/report.pdf"},
            None,
        )
        response = self._get(self.url_name, MWS)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        data = body["data"]
        self.assertEqual(set(data.keys()), {"report", "report_field_hints"})
        self.assertEqual(
            data["report"]["Mws_report_url"], "https://example.test/report.pdf"
        )
        assert_string_unit_map(self, data["report_field_hints"], "report_field_hints")


class MwsGeometriesV2Tests(V2ApiTestCase):
    url_name = "get_mws_geometries_v2"

    def test_missing_api_key_returns_401(self):
        response = self._get(self.url_name, MWS, with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_params_returns_400_error_envelope(self):
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert_error_envelope(self, response.json())

    @patch("public_api.api.get_mws_geometry")
    def test_success_format(self, mock_geom):
        mock_geom.return_value = (
            {
                "uid": "12_100174",
                "state": "rajasthan",
                "district": "bhilwara",
                "tehsil": "mandalgarh",
                "geometry": {"type": "Polygon", "coordinates": [[[75.0, 25.0]]]},
            },
            None,
        )
        response = self._get(self.url_name, MWS)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        data = body["data"]
        self.assertEqual(set(data.keys()), {"mws_geometry", "mws_geometry_field_hints"})
        self.assertEqual(data["mws_geometry"]["uid"], "12_100174")
        self.assertEqual(data["mws_geometry"]["geometry"]["type"], "Polygon")
        assert_string_unit_map(
            self, data["mws_geometry_field_hints"], "mws_geometry_field_hints"
        )


class VillageGeometriesV2Tests(V2ApiTestCase):
    url_name = "get_village_geometries_v2"

    def test_missing_api_key_returns_401(self):
        response = self._get(self.url_name, GEO, with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_params_returns_400_error_envelope(self):
        response = self._get(self.url_name, {"state": "Rajasthan"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert_error_envelope(self, response.json())

    def test_non_numeric_village_id_returns_400(self):
        response = self._get(self.url_name, {**GEO, "village_id": "abc"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert_error_envelope(self, response.json())

    @patch("public_api.api.get_village_geometries")
    def test_success_format(self, mock_geom):
        mock_geom.return_value = (
            [
                {
                    "village_id": "101",
                    "village_name": "Sample",
                    "state": "rajasthan",
                    "district": "bhilwara",
                    "tehsil": "mandalgarh",
                    "geometry": {"type": "Polygon", "coordinates": []},
                }
            ],
            None,
        )
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        data = body["data"]
        self.assertEqual(set(data.keys()), {"villages", "village_field_hints"})
        self.assertEqual(len(data["villages"]), 1)
        self.assertEqual(data["villages"][0]["village_id"], "101")
        assert_string_unit_map(self, data["village_field_hints"], "village_field_hints")


class ActiveLocationsV2Tests(V2ApiTestCase):
    url_name = "get_active_locations_v2"

    def test_missing_api_key_returns_401(self):
        response = self._get(self.url_name, with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    @patch("public_api.api.get_activated_location_json")
    def test_success_format(self, mock_locations):
        mock_locations.return_value = [
            {
                "label": "Rajasthan",
                "value": 1,
                "state_id": "8",
                "district": [
                    {
                        "label": "Bhilwara",
                        "value": 1,
                        "district_id": "123",
                        "blocks": [{"label": "Mandalgarh", "value": 1}],
                    }
                ],
            }
        ]
        response = self._get(self.url_name)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        data = body["data"]
        self.assertEqual(set(data.keys()), {"locations", "location_field_hints"})
        self.assertEqual(data["locations"][0]["label"], "Rajasthan")
        self.assertEqual(
            data["location_field_hints"], dict(ACTIVE_LOCATION_FIELD_HINTS)
        )
