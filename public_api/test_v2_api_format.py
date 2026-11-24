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


SAMPLE_MWS_POLYGON = [
    [
        [75.02716659, 25.2401886],
        [75.0868641493802, 25.20231618101583],
        [75.091234567, 25.251234567],
        [75.02716659, 25.2401886],
    ]
]


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

    def test_success_envelope_keeps_actual_polygon_coordinates(self):
        envelope = success_envelope(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"area": 10.555},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": SAMPLE_MWS_POLYGON,
                        },
                    }
                ],
            }
        )
        feature = envelope["data"]["features"][0]
        self.assertEqual(feature["geometry"]["type"], "Polygon")
        self.assertEqual(feature["geometry"]["coordinates"], SAMPLE_MWS_POLYGON)
        self.assertEqual(feature["properties"]["area"], 10.56)

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
        self.assertEqual(payload["metadata"]["area"], 10.55)
        self.assertNotIn("area_in_ha", payload["metadata"])
        self.assertEqual(payload["metadata_units"]["uid"], "id")
        self.assertEqual(payload["metadata_units"]["area"], "ha")
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
        self.assertEqual(drought_row["metadata"]["area"], 12.35)
        self.assertNotIn("area_in_ha", drought_row["metadata"])
        self.assertEqual(drought_row["metadata_units"]["area"], "ha")
        self.assertEqual(payload["tehsil_units"]["drought"]["area"], "ha")
        stream = payload["tehsil_data"]["stream_order"][0]
        self.assertEqual(stream["order"], ["1", "2"])
        self.assertEqual(payload["tehsil_units"]["stream_order"]["order"], "order")
        self.assertEqual(payload["tehsil_units"]["stream_order"]["value"], "%")
        json.dumps(payload, allow_nan=False)

    def test_metadata_and_annual_keys_do_not_embed_units(self):
        payload = annual_structure_from_dict(
            {
                "UID": "12_100174_101",
                "area_in_ha": 10.5,
                "sum_area_in_ha": 20.25,
                "total_swb_area_in_ha": 1.2,
                "drainage_density_weighted_in_km_per_km2": 0.51,
                "drysp_unit_4_weeks_2017": 2,
                "drysp_unit_4_weeks_2018": 3,
                "cropping_intensity_unit_less_2017": 1.16,
                "cropping_intensity_unit_less_2018": 1.2,
                "mild_drought_causality_2017": {"deficit": "mild", "other": "none"},
                "mild_drought_causality_2018": {"deficit": "none", "other": "mild"},
            }
        )
        self.assertEqual(payload["metadata"]["UID"], "12_100174_101")
        self.assertEqual(payload["metadata"]["area"], 10.5)
        self.assertEqual(payload["metadata"]["sum_area"], 20.25)
        self.assertEqual(payload["metadata"]["total_swb_area"], 1.2)
        self.assertEqual(payload["metadata"]["drainage_density_weighted"], 0.51)
        self.assertNotIn("area_in_ha", payload["metadata"])
        self.assertEqual(payload["metadata_units"]["area"], "ha")
        self.assertEqual(payload["metadata_units"]["sum_area"], "ha")
        self.assertEqual(payload["metadata_units"]["total_swb_area"], "ha")
        self.assertEqual(payload["metadata_units"]["drainage_density_weighted"], "km/km2")
        self.assertEqual(payload["annual"]["dry_spell"], [2, 3])
        self.assertNotIn("drysp_unit_4_weeks", payload["annual"])
        self.assertEqual(payload["annual_units"]["dry_spell"], "weeks")
        self.assertEqual(payload["annual"]["cropping_intensity"], [1.16, 1.2])
        self.assertNotIn("cropping_intensity_unit_less", payload["annual"])
        self.assertEqual(payload["annual_units"]["cropping_intensity"], "ratio")
        self.assertEqual(payload["annual_units"]["mild_drought_causality"], "category")
        self.assertEqual(payload["annual_units"]["mild_drought_causality.keys"], "label")
        assert_aligned_series(self, payload["annual"], payload["annual_units"])
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
                    "lulc_crop_percent": 40.12,
                }
            ]
        )
        self.assertEqual(
            set(payload.keys()),
            {"indicators", "indicator_units", "indicator_legends"},
        )
        self.assertIsInstance(payload["indicators"], dict)
        self.assertEqual(payload["indicators"]["avg_precipitation"], 800.13)
        self.assertEqual(payload["indicators"]["lulc_crop"], 40.12)
        self.assertNotIn("lulc_crop_percent", payload["indicators"])
        self.assertEqual(payload["indicator_units"]["mws_id"], "id")
        self.assertEqual(payload["indicator_units"]["avg_precipitation"], "mm")
        self.assertEqual(payload["indicator_units"]["cropping_intensity_avg"], "ratio")
        self.assertEqual(payload["indicator_units"]["lulc_crop"], "%")

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
                "geometry": {
                    "type": "Polygon",
                    "coordinates": SAMPLE_MWS_POLYGON,
                },
            }
        )
        self.assertEqual(set(geom.keys()), {"mws_geometry", "mws_geometry_field_hints"})
        self.assertEqual(
            geom["mws_geometry"]["geometry"]["coordinates"], SAMPLE_MWS_POLYGON
        )
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
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": SAMPLE_MWS_POLYGON,
                    },
                }
            ]
        )
        self.assertEqual(set(villages.keys()), {"villages", "village_field_hints"})
        self.assertEqual(
            villages["villages"][0]["geometry"]["coordinates"], SAMPLE_MWS_POLYGON
        )
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
        self.assertEqual(drought_row["metadata"]["area"], 12.35)
        self.assertNotIn("area_in_ha", drought_row["metadata"])
        assert_aligned_series(
            self, drought_row["annual"], drought_row["annual_units"]
        )

    @patch("public_api.api.get_tehsil_json")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_data_filter_keeps_requested_sheets(self, _mock_exists, mock_json):
        mock_json.return_value = {
            "croppingDrought_kharif": [
                {"uid": "12_1", "area_in_ha": 10, "no_drought_2017": 2}
            ],
            "stream_order": [{"uid": "12_1", "stream_order": 3}],
            "mws": [{"uid": "12_1", "area_in_ha": 10}],
        }
        response = self._get(
            self.url_name, {**GEO, "data": "croppingdrought_kharif,stream_order"}
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        self.assertEqual(
            set(body["data"]["tehsil_data"].keys()),
            {"croppingdrought_kharif", "stream_order"},
        )
        self.assertNotIn("mws", body["data"]["tehsil_data"])

    @patch("public_api.api.get_tehsil_json")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_drought_and_ndvi_shrub_aliases_map_to_real_sheets(
        self, _mock_exists, mock_json
    ):
        mock_json.return_value = {
            "croppingDrought_kharif": [
                {"uid": "12_1", "area_in_ha": 10, "no_drought_2017": 2}
            ],
            "ndvi": [{"uid": "12_1", "ndvi_2017": 0.42}],
            "mws": [{"uid": "12_1", "area_in_ha": 10}],
        }

        drought_resp = self._get(self.url_name, {**GEO, "data": "drought"})
        self.assertEqual(drought_resp.status_code, status.HTTP_200_OK)
        drought_body = drought_resp.json()
        assert_success_envelope(self, drought_body)
        self.assertEqual(
            set(drought_body["data"]["tehsil_data"].keys()),
            {"croppingdrought_kharif"},
        )

        ndvi_resp = self._get(self.url_name, {**GEO, "data": "ndvi_shrub"})
        self.assertEqual(ndvi_resp.status_code, status.HTTP_200_OK)
        ndvi_body = ndvi_resp.json()
        assert_success_envelope(self, ndvi_body)
        self.assertEqual(set(ndvi_body["data"]["tehsil_data"].keys()), {"ndvi"})

    @patch("public_api.api.get_tehsil_json")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_missing_valid_dataset_returns_404_data_not_generated(
        self, _mock_exists, mock_json
    ):
        mock_json.return_value = {
            "stream_order": [{"uid": "12_1", "stream_order": 3}],
            "mws": [{"uid": "12_1", "area_in_ha": 10}],
        }
        response = self._get(self.url_name, {**GEO, "data": "drought"})
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        body = response.json()
        assert_error_envelope(self, body)
        self.assertEqual(body["code"], "DATA_NOT_GENERATED")
        self.assertIn("croppingdrought_kharif", body["error_message"])
        self.assertEqual(
            body["details"]["missing_datasets"], ["croppingdrought_kharif"]
        )
        self.assertEqual(
            body["details"]["available_datasets"], ["mws", "stream_order"]
        )

    @patch("public_api.api.get_tehsil_json")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_previously_missing_five_pipeline_sheets_are_accepted(
        self, _mock_exists, mock_json
    ):
        mock_json.return_value = {
            "forest_fire": [{"uid": "12_1", "fire_count_per_year": 2}],
            "forest_fringes": [{"uid": "12_1", "fringe_area": 5.0}],
            "ndvi": [{"uid": "12_1", "ndvi_2017": 0.42}],
            "tree_in_grassland": [{"uid": "12_1", "tig_area": 1.2}],
            "village_GP": [{"village_name": "Rampur", "gp_name": "Rampur GP"}],
            "mws": [{"uid": "12_1", "area_in_ha": 10}],
        }
        response = self._get(
            self.url_name,
            {
                **GEO,
                "data": "forest_fire,forest_fringes,ndvi,tree_in_grassland,village_GP",
            },
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        self.assertEqual(
            set(body["data"]["tehsil_data"].keys()),
            {
                "forest_fire",
                "forest_fringes",
                "ndvi",
                "tree_in_grassland",
                "village_gp",
            },
        )
        self.assertNotIn("mws", body["data"]["tehsil_data"])

    def test_every_pipeline_sheet_is_accepted_by_data_filter(self):
        import ast
        from pathlib import Path
        from public_api.dataset_filters import (
            TEHSIL_DATA_TYPE_DOCS,
            filter_tehsil_payload,
            parse_tehsil_data_filter,
        )
        from stats_generator.sheet_names import (
            EXCEL_SHEET_BY_KEY,
            PIPELINE_EXCEL_SHEETS,
            TEHSIL_SHEET_SPECS,
        )

        # 1. Inspect every .to_excel(..., sheet_name=...) call in stats_generator/utils.py.
        utils_src = Path(__file__).resolve().parents[1] / "stats_generator" / "utils.py"
        tree = ast.parse(utils_src.read_text(encoding="utf-8"))
        discovered_sheets = set()
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "to_excel"
            ):
                for kw in node.keywords:
                    if kw.arg != "sheet_name":
                        continue
                    if isinstance(kw.value, ast.Constant) and isinstance(
                        kw.value.value, str
                    ):
                        discovered_sheets.add(kw.value.value)
                    elif (
                        isinstance(kw.value, ast.Subscript)
                        and isinstance(kw.value.value, ast.Name)
                        and kw.value.value.id == "EXCEL_SHEET_BY_KEY"
                        and isinstance(kw.value.slice, ast.Constant)
                    ):
                        discovered_sheets.add(
                            EXCEL_SHEET_BY_KEY[kw.value.slice.value]
                        )

        self.assertEqual(discovered_sheets, set(PIPELINE_EXCEL_SHEETS))

        # 2. Every pipeline sheet must pass parse_tehsil_data_filter and match filter_tehsil_payload.
        for excel_sheet, api_key, _desc in TEHSIL_SHEET_SPECS:
            parsed_raw = parse_tehsil_data_filter([excel_sheet])
            parsed_canon = parse_tehsil_data_filter([api_key])
            self.assertEqual(parsed_raw, {api_key}, msg=f"sheet={excel_sheet}")
            self.assertEqual(parsed_canon, {api_key}, msg=f"key={api_key}")

            sample_payload = {
                "tehsil_data": {api_key: [{"uid": "12_1"}]},
                "tehsil_units": {api_key: {"uid": "id"}},
            }
            filtered = filter_tehsil_payload(sample_payload, parsed_raw)
            self.assertIn(api_key, filtered["tehsil_data"], msg=f"sheet={excel_sheet}")

        # 3. Reverse parity: every documented canonical data= token (besides "all") is a real sheet.
        documented_keys = {key for key, _ in TEHSIL_DATA_TYPE_DOCS if key != "all"}
        spec_keys = {api_key for _, api_key, _ in TEHSIL_SHEET_SPECS}
        self.assertEqual(documented_keys, spec_keys)

    @patch("public_api.api.get_tehsil_json")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_unknown_data_filter_returns_400(self, _mock_exists, mock_json):
        mock_json.return_value = {"drought": [{"uid": "12_1"}]}
        response = self._get(self.url_name, {**GEO, "data": "not_a_real_sheet"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        body = response.json()
        assert_error_envelope(self, body)
        self.assertIn("Unknown data filter", body["error_message"])
        mock_json.assert_not_called()

    @patch("public_api.api.get_tehsil_json")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_regenerate_false_is_parsed_as_bool_false_not_truthy_string(
        self, _mock_exists, mock_json
    ):
        mock_json.return_value = {
            "aquifer_vector": [{"uid": "12_1", "area_in_ha": 100.0}]
        }
        for raw in ("false", "False", "0", "no", ""):
            mock_json.reset_mock()
            response = self._get(self.url_name, {**GEO, "regenerate": raw})
            self.assertEqual(response.status_code, status.HTTP_200_OK, msg=raw)
            self.assertIs(mock_json.call_args.args[3], False, msg=raw)

    @patch("public_api.api.get_tehsil_json")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_regenerate_true_forbidden_for_regular_api_key_user(
        self, _mock_exists, mock_json
    ):
        response = self._get(self.url_name, {**GEO, "regenerate": "true"})
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        body = response.json()
        assert_error_envelope(self, body)
        self.assertIn("regenerate", body["error_message"].lower())
        mock_json.assert_not_called()

        # v1 route also blocks non-admin API key regenerate=true
        v1_resp = self.client.get(
            reverse("get-tehsil-data"),
            {**GEO, "regenerate": "true"},
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(v1_resp.status_code, status.HTTP_403_FORBIDDEN)
        mock_json.assert_not_called()

    @patch("public_api.api.get_tehsil_json")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_regenerate_true_allowed_for_admin_and_jwt_users(
        self, _mock_exists, mock_json
    ):
        import jwt
        from django.conf import settings

        mock_json.return_value = {
            "aquifer_vector": [{"uid": "12_1", "area_in_ha": 100.0}]
        }

        # 1. JWT-authenticated regular user -> allowed
        token = jwt.encode(
            {"user_id": self.user.id}, settings.SECRET_KEY, algorithm="HS256"
        )
        jwt_resp = self.client.get(
            reverse(self.url_name),
            {**GEO, "regenerate": "true"},
            HTTP_AUTHORIZATION=f"Bearer {token}",
        )
        self.assertEqual(jwt_resp.status_code, status.HTTP_200_OK)
        self.assertIs(mock_json.call_args.args[3], True)

        # 2. Staff/admin API-key user -> allowed
        mock_json.reset_mock()
        self.user.is_staff = True
        self.user.save(update_fields=["is_staff"])
        admin_resp = self._get(self.url_name, {**GEO, "regenerate": "true"})
        self.assertEqual(admin_resp.status_code, status.HTTP_200_OK)
        self.assertIs(mock_json.call_args.args[3], True)

    @patch("public_api.views.pd.read_excel")
    @patch("public_api.views.excel_file_exists")
    def test_get_tehsil_json_uses_cache_when_regenerate_is_string_false(
        self, mock_exists, mock_read_excel
    ):
        import json
        import tempfile
        from pathlib import Path
        from public_api.views import get_tehsil_json

        with tempfile.TemporaryDirectory() as tmpdir:
            xlsx_path = Path(tmpdir) / "sample.xlsx"
            json_path = Path(tmpdir) / "sample.json"
            json_path.write_text(
                json.dumps({"aquifer_vector": [{"uid": "12_1"}]}),
                encoding="utf-8",
            )
            mock_exists.return_value = (str(xlsx_path), True)
            result = get_tehsil_json("Rajasthan", "Bhilwara", "Mandalgarh", "false")
            self.assertEqual(result, {"aquifer_vector": [{"uid": "12_1"}]})
            mock_read_excel.assert_not_called()

    @patch("public_api.views.pd.read_excel")
    @patch("public_api.views.excel_file_exists")
    def test_get_tehsil_json_rebuilds_when_excel_is_newer_than_cache(
        self, mock_exists, mock_read_excel
    ):
        import json
        import os
        import tempfile
        from pathlib import Path
        import pandas as pd
        from public_api.views import get_tehsil_json

        with tempfile.TemporaryDirectory() as tmpdir:
            xlsx_path = Path(tmpdir) / "sample.xlsx"
            json_path = Path(tmpdir) / "sample.json"
            xlsx_path.write_bytes(b"fake-xlsx")
            json_path.write_text(
                json.dumps({"drought": [{"uid": "12_1", "year": 2022}]}),
                encoding="utf-8",
            )
            # Simulate pipeline rewriting Excel after the JSON cache was built
            os.utime(json_path, (1_700_000_000, 1_700_000_000))
            os.utime(xlsx_path, (1_700_000_100, 1_700_000_100))

            mock_exists.return_value = (str(xlsx_path), True)
            mock_read_excel.return_value = {
                "drought": pd.DataFrame([{"UID": "12_1", "year": 2024}])
            }

            result = get_tehsil_json("Rajasthan", "Bhilwara", "Mandalgarh", False)
            self.assertEqual(result, {"drought": [{"uid": "12_1", "year": 2024}]})
            mock_read_excel.assert_called_once()
            self.assertEqual(
                json.loads(json_path.read_text(encoding="utf-8")),
                {"drought": [{"uid": "12_1", "year": 2024}]},
            )

            # Subsequent call with unchanged Excel should hit the refreshed cache
            mock_read_excel.reset_mock()
            cached = get_tehsil_json("Rajasthan", "Bhilwara", "Mandalgarh", False)
            self.assertEqual(cached, {"drought": [{"uid": "12_1", "year": 2024}]})
            mock_read_excel.assert_not_called()

    @patch("public_api.views.pd.read_excel")
    @patch("public_api.views.excel_file_exists")
    def test_get_tehsil_json_recovers_from_corrupt_cache_file(
        self, mock_exists, mock_read_excel
    ):
        import json
        import os
        import tempfile
        from pathlib import Path
        import pandas as pd
        from public_api.views import get_tehsil_json

        with tempfile.TemporaryDirectory() as tmpdir:
            xlsx_path = Path(tmpdir) / "sample.xlsx"
            json_path = Path(tmpdir) / "sample.json"
            xlsx_path.write_bytes(b"fake-xlsx")
            # Simulate a half-written / truncated JSON cache newer than the Excel
            json_path.write_text('{"drought": [{"uid": "12_1"', encoding="utf-8")
            os.utime(xlsx_path, (1_700_000_000, 1_700_000_000))
            os.utime(json_path, (1_700_000_100, 1_700_000_100))

            mock_exists.return_value = (str(xlsx_path), True)
            mock_read_excel.return_value = {
                "drought": pd.DataFrame([{"UID": "12_1", "year": 2024}])
            }

            result = get_tehsil_json("Rajasthan", "Bhilwara", "Mandalgarh", False)
            self.assertEqual(result, {"drought": [{"uid": "12_1", "year": 2024}]})
            mock_read_excel.assert_called_once()
            self.assertEqual(
                json.loads(json_path.read_text(encoding="utf-8")),
                {"drought": [{"uid": "12_1", "year": 2024}]},
            )

    @patch("public_api.views.pd.read_excel")
    @patch("public_api.views.excel_file_exists")
    def test_get_tehsil_json_writes_cache_atomically_via_os_replace(
        self, mock_exists, mock_read_excel
    ):
        import json
        import os
        import tempfile
        from pathlib import Path
        import pandas as pd
        from public_api.views import get_tehsil_json

        with tempfile.TemporaryDirectory() as tmpdir:
            xlsx_path = Path(tmpdir) / "sample.xlsx"
            json_path = Path(tmpdir) / "sample.json"
            xlsx_path.write_bytes(b"fake-xlsx")
            mock_exists.return_value = (str(xlsx_path), True)
            mock_read_excel.return_value = {
                "aquifer_vector": pd.DataFrame([{"UID": "12_1", "area": 10.5}])
            }

            replace_calls = []
            real_replace = os.replace

            def _spy_replace(src, dst):
                replace_calls.append((src, dst))
                self.assertEqual(os.path.dirname(src), tmpdir)
                self.assertNotEqual(src, dst)
                self.assertTrue(os.path.exists(src))
                return real_replace(src, dst)

            with patch("public_api.views.os.replace", side_effect=_spy_replace):
                result = get_tehsil_json("Rajasthan", "Bhilwara", "Mandalgarh", True)

            self.assertEqual(result, {"aquifer_vector": [{"uid": "12_1", "area": 10.5}]})
            self.assertEqual(len(replace_calls), 1)
            self.assertEqual(replace_calls[0][1], str(json_path))
            # No leftover temp files
            self.assertEqual(sorted(os.listdir(tmpdir)), ["sample.json", "sample.xlsx"])

            # If a later write fails mid-dump, existing valid cache stays intact and temp file is cleaned up
            with patch("public_api.views.json.dump", side_effect=RuntimeError("disk full")):
                with self.assertRaises(RuntimeError):
                    get_tehsil_json("Rajasthan", "Bhilwara", "Mandalgarh", True)

            self.assertEqual(
                json.loads(json_path.read_text(encoding="utf-8")),
                {"aquifer_vector": [{"uid": "12_1", "area": 10.5}]},
            )
            self.assertEqual(sorted(os.listdir(tmpdir)), ["sample.json", "sample.xlsx"])


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
        self.assertEqual(
            set(data.keys()),
            {"indicators", "indicator_units", "indicator_legends"},
        )
        self.assertEqual(data["indicators"]["mws_id"], "12_100174")
        self.assertEqual(data["indicator_units"]["avg_precipitation"], "mm")
        self.assertEqual(data["indicator_units"]["cropping_intensity_avg"], "ratio")
        self.assertEqual(data["indicator_units"]["drought_category"], "category")
        assert_string_unit_map(self, data["indicator_units"], "indicator_units")

    @patch("public_api.api.get_mws_json_from_kyl_indicator")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_fields_filter_keeps_requested_keys_plus_mws_id(
        self, _mock_exists, mock_kyl
    ):
        mock_kyl.return_value = [
            {
                "mws_id": "12_100174",
                "avg_precipitation": 800.129,
                "cropping_intensity_avg": 1.5,
                "drought_category": "mild",
            }
        ]
        response = self._get(
            self.url_name, {**MWS, "fields": "avg_precipitation,drought_category"}
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        data = body["data"]
        self.assertEqual(
            set(data["indicators"].keys()),
            {"mws_id", "avg_precipitation", "drought_category"},
        )
        self.assertNotIn("cropping_intensity_avg", data["indicators"])
        self.assertEqual(
            set(data["indicator_units"].keys()),
            {"mws_id", "avg_precipitation", "drought_category"},
        )

    @patch("public_api.api.get_mws_json_from_kyl_indicator")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_unknown_fields_returns_400_listing_valid_names(
        self, _mock_exists, mock_kyl
    ):
        mock_kyl.return_value = [
            {
                "mws_id": "12_100174",
                "avg_precipitation": 800.129,
                "cropping_intensity_avg": 1.5,
                "drought_category": "mild",
            }
        ]
        response = self._get(
            self.url_name, {**MWS, "fields": "avg_precipitation,bogus_metric"}
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        body = response.json()
        assert_error_envelope(self, body)
        self.assertIn("bogus_metric", body["error_message"])
        self.assertIn("avg_precipitation", body["error_message"])
        self.assertIn("drought_category", body["error_message"])

    @patch("public_api.api.get_mws_json_from_kyl_indicator")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_avg_cropped_units_are_percent_of_cropped_area(
        self, _mock_exists, mock_kyl
    ):
        mock_kyl.return_value = [
            {
                "mws_id": "12_100174",
                "avg_single_cropped": 41.25,
                "avg_double_cropped": 22.5,
                "avg_triple_cropped": 3.75,
            }
        ]
        response = self._get(self.url_name, MWS)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        units = body["data"]["indicator_units"]
        for key in ("avg_single_cropped", "avg_double_cropped", "avg_triple_cropped"):
            self.assertEqual(units[key], "% of cropped area")
            self.assertNotEqual(units[key], "ha")

    @patch("public_api.api.get_mws_json_from_kyl_indicator")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_avg_surface_water_units_and_null_preserved(self, _mock_exists, mock_kyl):
        mock_kyl.return_value = [
            {
                "mws_id": "12_100174",
                "avg_kharif_surface_water_mws": 80.5,
                "avg_rabi_surface_water_mws": None,
                "avg_zaid_surface_water_mws": None,
            }
        ]
        response = self._get(self.url_name, MWS)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        units = body["data"]["indicator_units"]
        indicators = body["data"]["indicators"]
        for key in (
            "avg_kharif_surface_water_mws",
            "avg_rabi_surface_water_mws",
            "avg_zaid_surface_water_mws",
        ):
            self.assertEqual(units[key], "% of that year's water area")
            self.assertNotEqual(units[key], "mm")
        self.assertEqual(indicators["avg_kharif_surface_water_mws"], 80.5)
        self.assertIsNone(indicators["avg_rabi_surface_water_mws"])
        self.assertIsNone(indicators["avg_zaid_surface_water_mws"])

    def test_avg_season_surface_water_percent_computation_and_null_on_no_data(self):
        import json
        import pandas as pd
        from stats_generator.mws_indicators import (
            _kyl_df_to_json_records,
            _prepare_kyl_results_df,
            compute_avg_season_surface_water_percent,
        )

        # Valid years: 2018-2019 -> 80%, 2019-2020 -> skipped (total=0), 2020-2021 -> 60%
        df = pd.DataFrame(
            [
                {
                    "UID": "12_100174",
                    "total_area_in_ha_2018-2019": 10.0,
                    "kharif_area_in_ha_2018-2019": 8.0,
                    "rabi_area_in_ha_2018-2019": None,
                    "total_area_in_ha_2019-2020": 0.0,
                    "kharif_area_in_ha_2019-2020": 0.0,
                    "rabi_area_in_ha_2019-2020": 0.0,
                    "total_area_in_ha_2020-2021": 20.0,
                    "kharif_area_in_ha_2020-2021": 12.0,
                    "rabi_area_in_ha_2020-2021": None,
                }
            ]
        )
        self.assertEqual(compute_avg_season_surface_water_percent(df, "kharif"), 70.0)
        # Rabi has no valid year -> None (not 0)
        self.assertIsNone(compute_avg_season_surface_water_percent(df, "rabi"))
        # Zaid columns absent -> None (not 0)
        self.assertIsNone(compute_avg_season_surface_water_percent(df, "zaid"))
        # Empty dataframe -> None (not 0)
        self.assertIsNone(
            compute_avg_season_surface_water_percent(pd.DataFrame(), "kharif")
        )

        # Mixed rows: float + None must stay JSON null (not -9999 or NaN)
        rows = [
            {
                "mws_id": "12_1",
                "avg_kharif_surface_water_mws": 70.0,
                "avg_rabi_surface_water_mws": None,
                "avg_zaid_surface_water_mws": None,
                "avg_precipitation": 800.0,
            },
            {
                "mws_id": "12_2",
                "avg_kharif_surface_water_mws": None,
                "avg_rabi_surface_water_mws": None,
                "avg_zaid_surface_water_mws": None,
                "avg_precipitation": None,
            },
        ]
        prepared = _prepare_kyl_results_df(rows)
        records = _kyl_df_to_json_records(prepared)
        serialized = json.dumps(records, allow_nan=False)
        parsed = json.loads(serialized)
        self.assertEqual(parsed[0]["avg_kharif_surface_water_mws"], 70.0)
        self.assertIsNone(parsed[0]["avg_rabi_surface_water_mws"])
        self.assertIsNone(parsed[1]["avg_kharif_surface_water_mws"])
        # Non-nullable columns keep legacy -9999 fill behavior
        self.assertEqual(parsed[1]["avg_precipitation"], -9999.0)

    @patch("public_api.api.get_mws_json_from_kyl_indicator")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_cropping_intensity_change_area_unit_and_legacy_rename(
        self, _mock_exists, mock_kyl
    ):
        from utilities.openmeteo_format import indicator_unit_for_key

        # Direct canonical key
        mock_kyl.return_value = [
            {"mws_id": "12_100174", "cropping_intensity_change_area": 245.67}
        ]
        response = self._get(self.url_name, MWS)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()["data"]
        self.assertEqual(data["indicators"]["cropping_intensity_change_area"], 245.67)
        self.assertEqual(data["indicator_units"]["cropping_intensity_change_area"], "ha")
        self.assertNotIn("degradation_cropping_intensity", data["indicators"])

        # Pre-existing cached KYL JSON still carrying legacy key is renamed transparently
        mock_kyl.return_value = [
            {"mws_id": "12_100174", "degradation_cropping_intensity": 312.45}
        ]
        response = self._get(
            self.url_name, {**MWS, "fields": "cropping_intensity_change_area"}
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()["data"]
        self.assertEqual(data["indicators"]["cropping_intensity_change_area"], 312.45)
        self.assertEqual(data["indicator_units"]["cropping_intensity_change_area"], "ha")
        self.assertNotIn("degradation_cropping_intensity", data["indicators"])
        self.assertEqual(indicator_unit_for_key("cropping_intensity_change_area"), "ha")
        self.assertEqual(indicator_unit_for_key("degradation_cropping_intensity"), "ha")

    @patch("public_api.api.get_mws_json_from_kyl_indicator")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_water_to_cropped_area_units_and_legacy_wsr_rename(
        self, _mock_exists, mock_kyl
    ):
        mock_kyl.return_value = [
            {
                "mws_id": "12_100174",
                "avg_wsr_ratio_kharif": 15.0,
                "avg_wsr_ratio_rabi": 12.5,
                "avg_wsr_ratio_zaid": None,
            }
        ]
        response = self._get(self.url_name, MWS)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()["data"]
        for season, expected in (
            ("kharif", 15.0),
            ("rabi", 12.5),
            ("zaid", None),
        ):
            key = f"avg_water_surface_to_cropped_area_{season}"
            self.assertEqual(data["indicators"][key], expected)
            self.assertEqual(data["indicator_units"][key], "%")
            self.assertNotIn(f"avg_wsr_ratio_{season}", data["indicators"])

    def test_avg_season_water_to_cropped_percent_computation(self):
        import json
        import pandas as pd
        from stats_generator.mws_indicators import (
            _kyl_df_to_json_records,
            _prepare_kyl_results_df,
            compute_avg_season_water_to_cropped_percent,
        )

        swb_df = pd.DataFrame(
            [
                {
                    "UID": "12_100174",
                    "kharif_area_in_ha_2017-2018": 10.0,
                    "kharif_area_in_ha_2018-2019": 20.0,
                    "rabi_area_in_ha_2017-2018": 5.0,
                    "rabi_area_in_ha_2018-2019": 15.0,
                    "zaid_area_in_ha_2017-2018": 2.0,
                    "zaid_area_in_ha_2018-2019": 4.0,
                }
            ]
        )
        crp_df = pd.DataFrame(
            [
                {
                    "UID": "12_100174",
                    # Year 1: kharif=100 (10%), rabi=50 (10%), zaid=0 (skipped)
                    "single_kharif_cropped_area_in_ha_2017-2018": 60.0,
                    "single_non_kharif_cropped_area_in_ha_2017-2018": 10.0,
                    "doubly_cropped_area_in_ha_2017-2018": 40.0,
                    "triply_cropped_area_in_ha_2017-2018": 0.0,
                    # Year 2: kharif=100 (20%), rabi=50 (30%), zaid=2 (200%)
                    "single_kharif_cropped_area_in_ha_2018-2019": 58.0,
                    "single_non_kharif_cropped_area_in_ha_2018-2019": 8.0,
                    "doubly_cropped_area_in_ha_2018-2019": 40.0,
                    "triply_cropped_area_in_ha_2018-2019": 2.0,
                }
            ]
        )
        # Per-year averages (NOT divided by number of years a second time)
        self.assertEqual(
            compute_avg_season_water_to_cropped_percent(swb_df, crp_df, "kharif"),
            15.0,
        )
        self.assertEqual(
            compute_avg_season_water_to_cropped_percent(swb_df, crp_df, "rabi"),
            20.0,
        )
        # Zaid uses triple-cropped area only, skipping year 1 where triple=0
        self.assertEqual(
            compute_avg_season_water_to_cropped_percent(swb_df, crp_df, "zaid"),
            200.0,
        )

        # When all years have 0 triple-cropped area -> None (JSON null, not 0 or -9999)
        crp_zero_zaid = crp_df.copy()
        crp_zero_zaid["triply_cropped_area_in_ha_2018-2019"] = 0.0
        zaid_none = compute_avg_season_water_to_cropped_percent(
            swb_df, crp_zero_zaid, "zaid"
        )
        self.assertIsNone(zaid_none)
        records = _kyl_df_to_json_records(
            _prepare_kyl_results_df(
                [
                    {"mws_id": "12_1", "avg_water_surface_to_cropped_area_zaid": 200.0},
                    {"mws_id": "12_2", "avg_water_surface_to_cropped_area_zaid": None},
                ]
            )
        )
        parsed = json.loads(json.dumps(records, allow_nan=False))
        self.assertIsNone(parsed[1]["avg_water_surface_to_cropped_area_zaid"])

    @patch("public_api.api.get_mws_json_from_kyl_indicator")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_soil_terrain_canopy_and_boolean_units_not_value(
        self, _mock_exists, mock_kyl
    ):
        expected_units = {
            "nitrogen_levels": "kg/ha",
            "phosphoros_levels": "kg/ha",
            "potassium_levels": "kg/ha",
            "organic_carbon": "%",
            "organic_carbon_tree_cover": "%",
            "soil_ph": "pH",
            "soil_drainage": "code",
            "soil_texture": "code",
            "reduction_canopy_density_height": "%",
            "increase_canopy_density_height": "%",
            "reduction_in_shrubland_cover": "ha",
            "forest_to_farm": "ha",
            "degradation_in_forests": "ha",
            "trees_in_forest_loss": "ha",
            "trees_in_forest_fringe_loss": "ha",
            "trees_in_grassland_loss": "ha",
            "relief": "m",
            "mean_elevation": "m",
            "relative_mean_elevation": "m",
            "drainage_density": "km/km²",
            "canal_available": "boolean",
            "river_available": "boolean",
            "mws_intersect_swb": "list",
            "distance_to_bank": "km",
            "dist_to_coops": "km",
            "shrub_trend": "code",
            "trend_annual_rainfall": "code",
            "kharif_annual_rainfall_ratio": "ratio",
            "monsoon_onset_day_range": "days",
            "shg_penetration": "score",
            "max_livestock_score": "score",
        }
        row = {"mws_id": "12_100174"}
        for key in expected_units:
            row[key] = True if expected_units[key] == "boolean" else 1.5
        mock_kyl.return_value = [row]

        response = self._get(self.url_name, MWS)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        units = response.json()["data"]["indicator_units"]
        for key, expected_unit in expected_units.items():
            self.assertEqual(units[key], expected_unit, msg=key)
            self.assertNotEqual(units[key], "value", msg=key)

    def test_every_emitted_kyl_indicator_has_non_value_unit(self):
        import ast
        from pathlib import Path
        from utilities.openmeteo_format import _canonicalize_indicator_row

        src = (
            Path(__file__).resolve().parent.parent
            / "stats_generator"
            / "mws_indicators.py"
        ).read_text(encoding="utf-8")
        tree = ast.parse(src)
        emitted_keys = []
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "append"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "results"
                and len(node.args) == 1
                and isinstance(node.args[0], ast.Dict)
            ):
                for k in node.args[0].keys:
                    if isinstance(k, ast.Constant) and isinstance(k.value, str):
                        emitted_keys.append(k.value)
        self.assertGreater(len(emitted_keys), 50)
        unknown_value_units = []
        for key in emitted_keys:
            _, units = _canonicalize_indicator_row({key: 1})
            for canon_key, unit in units.items():
                if unit == "value":
                    unknown_value_units.append((key, canon_key))
        self.assertEqual(
            unknown_value_units,
            [],
            msg=f"KYL indicators still resolving to 'value': {unknown_value_units}",
        )

    @patch("public_api.api.get_mws_json_from_kyl_indicator")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_coded_fields_return_indicator_legends_and_respect_fields_filter(
        self, _mock_exists, mock_kyl
    ):
        mock_kyl.return_value = [
            {
                "mws_id": "12_100174",
                "avg_precipitation": 800.0,
                "drought_category": 2,
                "aquifer_class": 0,
                "soge_class": 3,
                "soil_drainage": 3,
                "soil_texture": 1,
                "trend_annual_rainfall": "-1",
                "shrub_trend": 1,
                "mining": 0,
            }
        ]
        response = self._get(self.url_name, MWS)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        legends = response.json()["data"]["indicator_legends"]
        self.assertEqual(legends["aquifer_class"]["0"], "Hard Rock")
        self.assertEqual(legends["aquifer_class"]["1"], "Alluvial")
        self.assertIn("two or more years", legends["drought_category"]["2"].lower())
        self.assertEqual(legends["soge_class"]["3"], "Over Exploited")
        # Verify non-monotonic drainage map matches mws_indicators.py
        self.assertEqual(legends["soil_drainage"]["3"], "Imperfectly drained")
        self.assertIn("Poorly drained", legends["soil_drainage"]["2"])
        self.assertEqual(legends["trend_annual_rainfall"]["-1"], "decreasing")
        self.assertEqual(legends["shrub_trend"]["1"], "increasing")
        self.assertEqual(legends["mining"]["0"], "No mining record")
        self.assertNotIn("avg_precipitation", legends)

        # fields= must trim indicator_legends to only the requested coded fields
        filtered_resp = self._get(
            self.url_name, {**MWS, "fields": "aquifer_class,avg_precipitation"}
        )
        self.assertEqual(filtered_resp.status_code, status.HTTP_200_OK)
        filtered_legends = filtered_resp.json()["data"]["indicator_legends"]
        self.assertEqual(set(filtered_legends.keys()), {"aquifer_class"})

    def test_every_coded_kyl_unit_has_legend(self):
        from stats_generator.kyl_legends import KYL_INDICATOR_LEGENDS
        from utilities.openmeteo_format import KYL_INDICATOR_UNIT_OVERRIDES

        coded_keys = {
            k
            for k, u in KYL_INDICATOR_UNIT_OVERRIDES.items()
            if u in {"code", "class", "flag"}
        } | {"drought_category"}
        missing = sorted(coded_keys - set(KYL_INDICATOR_LEGENDS.keys()))
        self.assertEqual(
            missing,
            [],
            msg=f"Coded KYL indicators missing legends: {missing}",
        )


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


ALL_MWS_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"uid": "12_100174"},
            "geometry": {"type": "Polygon", "coordinates": SAMPLE_MWS_POLYGON},
        }
    ],
}


class MwsGeometriesV1Tests(V2ApiTestCase):
    url_name = "get-mws-geometries"

    def test_missing_api_key_returns_401(self):
        response = self._get(self.url_name, GEO, with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_params_returns_legacy_error(self):
        response = self._get(self.url_name, {"state": "Rajasthan"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        body = response.json()
        self.assertEqual(
            body["error"], "All parameters (state, district, tehsil) are required"
        )
        self.assertNotIn("status", body)
        self.assertNotIn("data", body)

    @patch("public_api.api.get_mws_geometries_data")
    def test_returns_raw_feature_collection(self, mock_all):
        mock_all.return_value = (True, ALL_MWS_GEOJSON)
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        self.assertEqual(body["type"], "FeatureCollection")
        self.assertNotIn("status", body)
        self.assertNotIn("data", body)
        self.assertEqual(len(body["features"]), 1)
        self.assertEqual(body["features"][0]["properties"]["uid"], "12_100174")
        mock_all.assert_called_once()


class MwsGeometriesV2Tests(V2ApiTestCase):
    url_name = "get_mws_geometries_v2"

    def test_missing_api_key_returns_401(self):
        response = self._get(self.url_name, MWS, with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_params_returns_400_error_envelope(self):
        response = self._get(self.url_name, {"state": "Rajasthan"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        body = response.json()
        assert_error_envelope(self, body)
        self.assertIn("'state', 'district', and 'tehsil'", body["error_message"])
        self.assertNotIn("mws_id", body["error_message"])

    @patch("public_api.api.get_mws_geometries_data")
    def test_without_mws_id_returns_all_geometries(self, mock_all):
        mock_all.return_value = (True, ALL_MWS_GEOJSON)
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        data = body["data"]
        self.assertEqual(data["type"], "FeatureCollection")
        self.assertEqual(len(data["features"]), 1)
        self.assertEqual(data["features"][0]["properties"]["uid"], "12_100174")
        self.assertEqual(
            data["features"][0]["geometry"]["coordinates"], SAMPLE_MWS_POLYGON
        )
        mock_all.assert_called_once()

    @patch("public_api.api.get_mws_geometry")
    @patch("public_api.api.get_mws_geometries_data")
    def test_without_mws_id_falls_back_to_gee(self, mock_all, mock_geom):
        mock_all.return_value = (False, "GeoServer request failed with status 404")
        mock_geom.return_value = (ALL_MWS_GEOJSON, None)
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        self.assertEqual(body["data"]["type"], "FeatureCollection")
        mock_geom.assert_called_once()
        _state, _district, _tehsil = mock_geom.call_args.args[:3]
        self.assertIsNone(mock_geom.call_args.kwargs.get("mws_id"))

    @patch("public_api.api.get_mws_geometry")
    def test_with_mws_id_returns_single_geometry(self, mock_geom):
        mock_geom.return_value = (
            {
                "uid": "12_100174",
                "state": "rajasthan",
                "district": "bhilwara",
                "tehsil": "mandalgarh",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": SAMPLE_MWS_POLYGON,
                },
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
        self.assertEqual(
            data["mws_geometry"]["geometry"]["coordinates"], SAMPLE_MWS_POLYGON
        )
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

    @patch("public_api.api.get_village_geometries_data")
    def test_returns_feature_collection_with_actual_coordinates(self, mock_all):
        mock_all.return_value = (
            True,
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"vill_ID": 101, "vill_name": "Sample"},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": SAMPLE_MWS_POLYGON,
                        },
                    }
                ],
            },
        )
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        data = body["data"]
        self.assertEqual(data["type"], "FeatureCollection")
        self.assertEqual(len(data["features"]), 1)
        self.assertEqual(data["features"][0]["properties"]["vill_ID"], 101)
        self.assertEqual(
            data["features"][0]["geometry"]["coordinates"], SAMPLE_MWS_POLYGON
        )
        mock_all.assert_called_once()

    @patch("public_api.api.get_village_geometries_data")
    def test_village_id_filters_feature_collection(self, mock_all):
        mock_all.return_value = (
            True,
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"vill_ID": 101, "vill_name": "Keep"},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": SAMPLE_MWS_POLYGON,
                        },
                    },
                    {
                        "type": "Feature",
                        "properties": {"vill_ID": 202, "vill_name": "Drop"},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": SAMPLE_MWS_POLYGON,
                        },
                    },
                ],
            },
        )
        response = self._get(self.url_name, {**GEO, "village_id": "101"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        features = body["data"]["features"]
        self.assertEqual(len(features), 1)
        self.assertEqual(features[0]["properties"]["vill_ID"], 101)

    @patch("public_api.api.get_village_geometries")
    @patch("public_api.api.get_village_geometries_data")
    def test_falls_back_to_geopandas_feature_collection(self, mock_wfs, mock_geom):
        mock_wfs.return_value = (False, "GeoServer request failed with status 404")
        mock_geom.return_value = (
            [
                {
                    "village_id": "101",
                    "village_name": "Sample",
                    "state": "rajasthan",
                    "district": "bhilwara",
                    "tehsil": "mandalgarh",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": SAMPLE_MWS_POLYGON,
                    },
                }
            ],
            None,
        )
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        self.assertEqual(body["data"]["type"], "FeatureCollection")
        self.assertEqual(
            body["data"]["features"][0]["geometry"]["coordinates"], SAMPLE_MWS_POLYGON
        )
        mock_geom.assert_called_once()


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

    @patch("public_api.api.get_activated_location_json")
    def test_state_district_tehsil_filter(self, mock_locations):
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
                        "blocks": [
                            {"label": "Mandalgarh", "value": 1},
                            {"label": "Jahazpur", "value": 2},
                        ],
                    },
                    {
                        "label": "Jaipur",
                        "value": 2,
                        "district_id": "124",
                        "blocks": [{"label": "Amber", "value": 1}],
                    },
                ],
            },
            {
                "label": "Uttar Pradesh",
                "value": 2,
                "state_id": "9",
                "district": [
                    {
                        "label": "Jaunpur",
                        "value": 1,
                        "district_id": "200",
                        "blocks": [{"label": "Badlapur", "value": 1}],
                    }
                ],
            },
        ]
        response = self._get(
            self.url_name,
            {"state": "Rajasthan", "district": "Bhilwara", "tehsil": "Mandalgarh"},
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body)
        locations = body["data"]["locations"]
        self.assertEqual(len(locations), 1)
        self.assertEqual(locations[0]["label"], "Rajasthan")
        self.assertEqual(len(locations[0]["district"]), 1)
        self.assertEqual(locations[0]["district"][0]["label"], "Bhilwara")
        self.assertEqual(
            [block["label"] for block in locations[0]["district"][0]["blocks"]],
            ["Mandalgarh"],
        )
