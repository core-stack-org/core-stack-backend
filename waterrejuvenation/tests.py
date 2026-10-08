"""Unit tests: Open-Meteo envelope + annual format for waterbody /api/v2/ routes."""

import json
from datetime import timedelta
from unittest.mock import patch

from django.test import SimpleTestCase, TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from geoadmin.models import UserAPIKey
from users.models import User
from utilities.openmeteo_format import success_envelope, waterbody_structure_from_dict

SUCCESS_ENVELOPE_KEYS = {"status", "error_message", "data"}
GEO = {
    "state": "Rajasthan",
    "district": "Bhilwara",
    "tehsil": "Mandalgarh",
}
UID = "12_100174_101"

SAMPLE_WATERBODY = {
    "UID": "12_100174_101",
    "sum": 2.555,
    "zoi": 3,
    "zoi_area": 10.129,
    "total_cropable_area_ever_hydroyear_2017_2024": 50.019,
    "cropping_intensity_2017": 1.234,
    "cropping_intensity_2018": 1.456,
    "single_cropped_area_2017": 1.119,
    "single_cropped_area_2018": 1.221,
    "doubly_cropped_area_2017": 0.5,
    "doubly_cropped_area_2018": 0.6,
    "zoi_properties": {"extra_flag": 1},
}


def _merged_dataset():
    return {UID: dict(SAMPLE_WATERBODY)}


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
    return assert_json_roundtrip(testcase, body)


def assert_error_envelope(testcase, body):
    testcase.assertEqual(body["status"], "error")
    testcase.assertIn("error_message", body)
    testcase.assertIsInstance(body["error_message"], str)
    testcase.assertGreater(len(body["error_message"]), 0)
    json.dumps(body, allow_nan=False)


def assert_waterbody_item(testcase, item):
    testcase.assertIsInstance(item, dict)
    for key in ("metadata", "metadata_units", "annual", "annual_units"):
        testcase.assertIn(key, item)
    metadata = item["metadata"]
    meta_units = item["metadata_units"]
    annual = item["annual"]
    annual_units = item["annual_units"]

    testcase.assertEqual(metadata.get("UID") or metadata.get("uid"), UID)
    testcase.assertEqual(meta_units.get("UID") or meta_units.get("uid"), "id")
    if "sum" in metadata:
        testcase.assertEqual(meta_units["sum"], "ha")
        testcase.assertEqual(metadata["sum"], 2.56)
    if "zoi" in metadata:
        testcase.assertEqual(meta_units["zoi"], "count")
    if "zoi_area" in metadata:
        testcase.assertEqual(meta_units["zoi_area"], "ha")
    if "total_cropable_area_ever_hydroyear" in metadata:
        testcase.assertEqual(
            meta_units["total_cropable_area_ever_hydroyear"], "ha"
        )
        testcase.assertEqual(metadata["total_cropable_area_ever_hydroyear"], 50.02)

    testcase.assertEqual(annual["time"], ["2017", "2018"])
    testcase.assertEqual(annual_units["time"], "agricultural_year")
    testcase.assertEqual(annual_units["cropping_intensity"], "ratio")
    testcase.assertEqual(annual_units["single_cropped_area"], "ha")
    testcase.assertEqual(annual_units["doubly_cropped_area"], "ha")
    n = len(annual["time"])
    for metric, series in annual.items():
        if metric == "time":
            continue
        testcase.assertIsInstance(series, list)
        testcase.assertEqual(len(series), n, msg=metric)
        testcase.assertIn(metric, annual_units)
        for value in series:
            if isinstance(value, float):
                testcase.assertEqual(value, round(value, 2), msg=metric)
    testcase.assertEqual(annual["cropping_intensity"], [1.23, 1.46])


class WaterbodyStructureTests(SimpleTestCase):
    def test_annual_block_units_and_envelope_json(self):
        inner = waterbody_structure_from_dict(SAMPLE_WATERBODY)
        assert_waterbody_item(self, inner)
        self.assertIn("extra_flag", inner["metadata"])
        envelope = success_envelope(inner)
        self.assertEqual(set(envelope.keys()), SUCCESS_ENVELOPE_KEYS)
        assert_json_roundtrip(self, envelope)
        assert_waterbody_item(self, envelope["data"])


class WaterbodyV2ApiTestCase(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = User.objects.create_user(
            username="wb_v2_tester",
            email="wb_v2_tester@example.com",
            password="password123",
        )
        self.api_key_obj, self.api_key = UserAPIKey.objects.create_key(
            user=self.user,
            name="wb-v2-test-key",
            expires_at=timezone.now() + timedelta(days=30),
        )
        self.api_key_obj.api_key = self.api_key
        self.api_key_obj.save()

    def _get(self, url_name, params=None, *, with_key=True):
        headers = {}
        if with_key:
            headers["HTTP_X_API_KEY"] = self.api_key
        return self.client.get(reverse(url_name), params or {}, **headers)


class WaterbodiesByAdminV2Tests(WaterbodyV2ApiTestCase):
    url_name = "generate_waterbodies_data_v2"

    def test_missing_api_key_returns_401(self):
        response = self._get(self.url_name, GEO, with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_params_returns_400_error_envelope(self):
        response = self._get(self.url_name, {"state": "Rajasthan"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert_error_envelope(self, response.json())

    @patch("waterrejuvenation.api._load_or_generate_merged_data")
    def test_success_format(self, mock_load):
        mock_load.return_value = _merged_dataset()
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response["Content-Type"].split(";")[0], "application/json")
        body = response.json()
        assert_success_envelope(self, body, extra_keys=("location",))
        self.assertEqual(
            body["location"],
            {"state": "RAJASTHAN", "district": "bhilwara", "tehsil": "mandalgarh"},
        )
        self.assertIsInstance(body["data"], list)
        self.assertEqual(len(body["data"]), 1)
        assert_waterbody_item(self, body["data"][0])

    @patch("waterrejuvenation.api._load_or_generate_merged_data", return_value=None)
    def test_unavailable_dataset_error_envelope(self, _mock_load):
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)
        assert_error_envelope(self, response.json())


class WaterbodyByUidV2Tests(WaterbodyV2ApiTestCase):
    url_name = "generate_waterbody_data_v2"

    def test_missing_api_key_returns_401(self):
        response = self._get(self.url_name, {**GEO, "uid": UID}, with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_uid_returns_400_error_envelope(self):
        response = self._get(self.url_name, GEO)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert_error_envelope(self, response.json())

    @patch("waterrejuvenation.api._load_or_generate_merged_data")
    def test_success_format(self, mock_load):
        mock_load.return_value = _merged_dataset()
        response = self._get(self.url_name, {**GEO, "uid": UID})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        assert_success_envelope(self, body, extra_keys=("location", "uid"))
        self.assertEqual(body["uid"], UID)
        self.assertEqual(body["location"]["tehsil"], "mandalgarh")
        assert_waterbody_item(self, body["data"])

    @patch("waterrejuvenation.api._load_or_generate_merged_data")
    def test_unknown_uid_returns_404_error_envelope(self, mock_load):
        mock_load.return_value = _merged_dataset()
        response = self._get(self.url_name, {**GEO, "uid": "99_99_99"})
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        assert_error_envelope(self, response.json())
