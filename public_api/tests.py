"""Tests for public API v2 get_mws_data: Open-Meteo fortnight format and units."""

import json
import re
from datetime import timedelta
from unittest.mock import patch

from django.test import SimpleTestCase, TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from geoadmin.models import UserAPIKey
from users.models import User
from utilities.openmeteo_format import (
    fortnight_structure_from_mws,
    legacy_hourly_to_fortnight_inner_block,
    success_envelope,
)

EXPECTED_FORTNIGHT_UNITS = {
    "time": "iso8601",
    "time_step": "15_days",
    "et": "mm",
    "runoff": "mm",
    "precipitation": "mm",
}

FORTNIGHT_METRIC_KEYS = ("et", "runoff", "precipitation")
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _sample_mws_timeseries():
    return {
        "mws_id": "12_208104",
        "time_series": [
            {
                "date": "2024-01-01",
                "et": 2.555,
                "runoff": 1.333,
                "precipitation": 10.219,
            },
            {
                "date": "2024-01-15",
                "et": 3.14159,
                "runoff": 0.8,
                "precipitation": 5.4,
            },
        ],
    }


def _assert_fortnight_inner_block(testcase, data):
    """Validate Open-Meteo-style MWS fortnight block: keys, units, alignment, types."""
    testcase.assertIsInstance(data, dict)
    testcase.assertEqual(
        set(data.keys()),
        {"metadata", "fortnight", "fortnight_units"},
    )
    testcase.assertNotIn("time_series", data)
    testcase.assertNotIn("hourly", data)
    testcase.assertNotIn("hourly_units", data)

    metadata = data["metadata"]
    testcase.assertIsInstance(metadata, dict)
    testcase.assertIn("mws_id", metadata)
    testcase.assertIsInstance(metadata["mws_id"], str)

    fortnight = data["fortnight"]
    units = data["fortnight_units"]
    testcase.assertIsInstance(fortnight, dict)
    testcase.assertIsInstance(units, dict)

    testcase.assertEqual(units, EXPECTED_FORTNIGHT_UNITS)
    testcase.assertEqual(
        set(fortnight.keys()),
        {"time", *FORTNIGHT_METRIC_KEYS},
    )

    times = fortnight["time"]
    testcase.assertIsInstance(times, list)
    testcase.assertGreater(len(times), 0)
    for t in times:
        testcase.assertIsInstance(t, str)
        testcase.assertRegex(t, ISO_DATE_RE)

    n = len(times)
    for metric in FORTNIGHT_METRIC_KEYS:
        series = fortnight[metric]
        testcase.assertIsInstance(series, list)
        testcase.assertEqual(
            len(series),
            n,
            msg=f"{metric} length must match time ({n})",
        )
        for value in series:
            testcase.assertTrue(
                value is None or isinstance(value, (int, float)),
                msg=f"{metric} values must be numeric or null, got {type(value)}",
            )
            if isinstance(value, float):
                # round_floats(..., precision=2)
                testcase.assertEqual(value, round(value, 2))


class FortnightStructureFromMwsTests(SimpleTestCase):
    def test_units_and_aligned_arrays(self):
        payload = fortnight_structure_from_mws(_sample_mws_timeseries())
        _assert_fortnight_inner_block(self, payload)
        self.assertEqual(payload["metadata"]["mws_id"], "12_208104")
        self.assertEqual(payload["fortnight"]["time"], ["2024-01-01", "2024-01-15"])
        self.assertEqual(payload["fortnight"]["et"], [2.56, 3.14])
        self.assertEqual(payload["fortnight"]["runoff"], [1.33, 0.8])
        self.assertEqual(payload["fortnight"]["precipitation"], [10.22, 5.4])

    def test_success_envelope_json_formatting(self):
        inner = fortnight_structure_from_mws(_sample_mws_timeseries())
        envelope = success_envelope(inner)

        self.assertEqual(set(envelope.keys()), {"status", "error_message", "data"})
        self.assertEqual(envelope["status"], "success")
        self.assertIsNone(envelope["error_message"])
        _assert_fortnight_inner_block(self, envelope["data"])

        # Must serialize cleanly as JSON (no NaN/Infinity, native types only).
        encoded = json.dumps(envelope)
        decoded = json.loads(encoded)
        self.assertEqual(decoded["status"], "success")
        _assert_fortnight_inner_block(self, decoded["data"])

    def test_empty_time_series_keeps_units(self):
        payload = fortnight_structure_from_mws({"mws_id": "1_2", "time_series": []})
        self.assertEqual(payload["fortnight"]["time"], [])
        self.assertEqual(payload["fortnight_units"], EXPECTED_FORTNIGHT_UNITS)
        for metric in FORTNIGHT_METRIC_KEYS:
            self.assertEqual(payload["fortnight"][metric], [])

    def test_legacy_hourly_keys_migrate_to_fortnight(self):
        legacy = {
            "metadata": {"mws_id": "12_208104"},
            "hourly": {
                "time": ["2024-01-01"],
                "et": [1.0],
                "runoff": [2.0],
                "precipitation": [3.0],
            },
            "hourly_units": dict(EXPECTED_FORTNIGHT_UNITS),
        }
        migrated = legacy_hourly_to_fortnight_inner_block(legacy)
        self.assertIn("fortnight", migrated)
        self.assertIn("fortnight_units", migrated)
        self.assertNotIn("hourly", migrated)
        self.assertNotIn("hourly_units", migrated)
        self.assertEqual(migrated["fortnight_units"], EXPECTED_FORTNIGHT_UNITS)


class GetMwsDataV2ApiTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = User.objects.create_user(
            username="mws_v2_tester",
            email="mws_v2_tester@example.com",
            password="password123",
        )
        self.api_key_obj, self.api_key = UserAPIKey.objects.create_key(
            user=self.user,
            name="mws-v2-test-key",
            expires_at=timezone.now() + timedelta(days=30),
        )
        self.api_key_obj.api_key = self.api_key
        self.api_key_obj.save()
        self.url = reverse("get-mws-data-v2")
        self.query = {
            "state": "Uttar Pradesh",
            "district": "Jaunpur",
            "tehsil": "Badlapur",
            "mws_id": "12_208104",
        }

    def _get(self, *, with_key=True, extra_query=None, **kwargs):
        params = dict(self.query)
        if extra_query:
            params.update(extra_query)
        headers = {}
        if with_key:
            headers["HTTP_X_API_KEY"] = self.api_key
        return self.client.get(self.url, params, **headers, **kwargs)

    def test_missing_api_key_returns_401(self):
        response = self._get(with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_params_returns_400_error_envelope(self):
        response = self.client.get(
            self.url,
            {"state": "Uttar Pradesh"},
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        body = response.json()
        self.assertEqual(body["status"], "error")
        self.assertIn("error_message", body)
        self.assertIsInstance(body["error_message"], str)

    @patch("public_api.api._save_mws_v2_to_mongo")
    @patch("public_api.api._load_mws_v2_from_mongo", return_value=None)
    @patch("public_api.api.get_mws_time_series_data")
    def test_success_validates_units_and_json_format(
        self, mock_get_mws, _mock_load, mock_save
    ):
        mock_get_mws.return_value = _sample_mws_timeseries()

        response = self._get()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response["Content-Type"].split(";")[0], "application/json")

        body = response.json()
        self.assertEqual(set(body.keys()), {"status", "error_message", "data"})
        self.assertEqual(body["status"], "success")
        self.assertIsNone(body["error_message"])
        _assert_fortnight_inner_block(self, body["data"])

        # Round-trip JSON encoding of the live response body.
        reloaded = json.loads(json.dumps(body))
        _assert_fortnight_inner_block(self, reloaded["data"])

        mock_save.assert_called_once()
        saved_payload = mock_save.call_args[0][4]
        _assert_fortnight_inner_block(self, saved_payload)

    @patch("public_api.api._save_mws_v2_to_mongo")
    @patch("public_api.api._load_mws_v2_from_mongo")
    @patch("public_api.api.get_mws_time_series_data")
    def test_cached_legacy_hourly_is_exposed_as_fortnight(
        self, mock_get_mws, mock_load, mock_save
    ):
        mock_load.return_value = {
            "metadata": {"mws_id": "12_208104"},
            "hourly": {
                "time": ["2024-01-01", "2024-01-15"],
                "et": [2.5, 3.1],
                "runoff": [1.3, 0.8],
                "precipitation": [10.2, 5.4],
            },
            "hourly_units": dict(EXPECTED_FORTNIGHT_UNITS),
        }

        response = self._get()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        _assert_fortnight_inner_block(self, body["data"])
        mock_get_mws.assert_not_called()
        mock_save.assert_not_called()

    @patch("public_api.api._save_mws_v2_to_mongo")
    @patch("public_api.api._load_mws_v2_from_mongo", return_value=None)
    @patch("public_api.api.get_mws_time_series_data", return_value=None)
    def test_not_found_error_envelope(self, *_mocks):
        response = self._get()
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        body = response.json()
        self.assertEqual(body["status"], "error")
        self.assertEqual(body["error_message"], "Data not found for the given mws_id")
