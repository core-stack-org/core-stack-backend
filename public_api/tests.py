"""Tests for public API v2 get_mws_data: Open-Meteo fortnight format and units."""

import json
import re
from datetime import timedelta
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from geoadmin.models import DistrictSOI, StateSOI, TehsilSOI, UserAPIKey
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

    def test_sanitizes_empty_strings_and_drops_trailing_empty_fortnights(self):
        payload = fortnight_structure_from_mws(
            {
                "mws_id": "12_208104",
                "time_series": [
                    {"date": "2024-07-01", "et": 3.5, "runoff": 0.0, "precipitation": 0.0},
                    {"date": "2024-07-16", "et": 4.0, "runoff": "", "precipitation": None},
                    {"date": "2024-08-01", "et": None, "runoff": "", "precipitation": None},
                ],
            }
        )
        _assert_fortnight_inner_block(self, payload)
        self.assertEqual(payload["fortnight"]["time"], ["2024-07-01", "2024-07-16"])
        self.assertEqual(payload["fortnight"]["et"], [3.5, 4.0])
        self.assertEqual(payload["fortnight"]["runoff"], [0.0, None])
        self.assertEqual(payload["fortnight"]["precipitation"], [0.0, None])

        # Cached Mongo payload with "" and trailing all-null period is cleaned too
        cleaned = legacy_hourly_to_fortnight_inner_block(
            {
                "metadata": {"mws_id": "12_208104"},
                "fortnight": {
                    "time": ["2024-07-01", "2024-07-16"],
                    "et": [3.5, ""],
                    "runoff": [0.0, ""],
                    "precipitation": [12.0, ""],
                },
                "fortnight_units": dict(EXPECTED_FORTNIGHT_UNITS),
            }
        )
        self.assertEqual(cleaned["fortnight"]["time"], ["2024-07-01"])
        self.assertEqual(cleaned["fortnight"]["et"], [3.5])

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

    @patch("public_api.api._save_mws_v2_to_mongo")
    @patch("public_api.api._load_mws_v2_from_mongo", return_value=None)
    @patch("public_api.api.get_mws_time_series_data")
    def test_fields_filter_keeps_requested_metrics_and_caches_full_payload(
        self, mock_get_mws, _mock_load, mock_save
    ):
        mock_get_mws.return_value = _sample_mws_timeseries()

        response = self._get(extra_query={"fields": "et,precipitation"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        self.assertEqual(body["status"], "success")
        data = body["data"]
        self.assertEqual(data["metadata"]["mws_id"], "12_208104")
        self.assertEqual(
            set(data["fortnight"].keys()), {"time", "et", "precipitation"}
        )
        self.assertNotIn("runoff", data["fortnight"])
        self.assertEqual(
            set(data["fortnight_units"].keys()),
            {"time", "time_step", "et", "precipitation"},
        )
        # Full unfiltered payload must still be stored in Mongo cache.
        mock_save.assert_called_once()
        saved_payload = mock_save.call_args[0][4]
        self.assertEqual(
            set(saved_payload["fortnight"].keys()),
            {"time", "et", "runoff", "precipitation"},
        )

    @patch("public_api.api._save_mws_v2_to_mongo")
    @patch("public_api.api._load_mws_v2_from_mongo")
    @patch("public_api.api.get_mws_time_series_data")
    def test_fields_filter_applies_on_cache_hit(
        self, mock_get_mws, mock_load, mock_save
    ):
        mock_load.return_value = {
            "metadata": {"mws_id": "12_208104"},
            "fortnight": {
                "time": ["2024-01-01", "2024-01-15"],
                "et": [2.5, 3.1],
                "runoff": [1.3, 0.8],
                "precipitation": [10.2, 5.4],
            },
            "fortnight_units": dict(EXPECTED_FORTNIGHT_UNITS),
        }

        response = self._get(extra_query={"fields": "runoff"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()["data"]
        self.assertEqual(set(data["fortnight"].keys()), {"time", "runoff"})
        self.assertEqual(
            set(data["fortnight_units"].keys()), {"time", "time_step", "runoff"}
        )
        mock_get_mws.assert_not_called()
        mock_save.assert_not_called()

    @patch("public_api.api._load_mws_v2_from_mongo")
    @patch("public_api.api.get_mws_time_series_data")
    def test_unknown_field_returns_400_with_valid_names(
        self, mock_get_mws, mock_load
    ):
        response = self._get(extra_query={"fields": "et,not_a_real_metric"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        body = response.json()
        self.assertEqual(body["status"], "error")
        self.assertIn("not_a_real_metric", body["error_message"])
        for valid in ("et", "runoff", "precipitation"):
            self.assertIn(valid, body["error_message"])
        mock_load.assert_not_called()
        mock_get_mws.assert_not_called()


class GetMwsDataMultiWordGeoLookupTests(TestCase):
    """End-to-end SOI lookup tests for get_mws_data (v1 and v2) with multi-word names."""

    def setUp(self):
        self.client = APIClient()
        self.user = User.objects.create_user(
            username="mws_geo_tester",
            email="mws_geo_tester@example.com",
            password="password123",
        )
        self.api_key_obj, self.api_key = UserAPIKey.objects.create_key(
            user=self.user,
            name="mws-geo-test-key",
            expires_at=timezone.now() + timedelta(days=30),
        )
        self.api_key_obj.api_key = self.api_key
        self.api_key_obj.save()

        self.url_v2 = reverse("get-mws-data-v2")
        self.url_v1 = reverse("get-mws-data")

        self.karnataka = StateSOI.objects.create(state_name="Karnataka")
        self.chik_ballapur = DistrictSOI.objects.create(
            state=self.karnataka, district_name="Chik Ballapur"
        )
        self.bagepalli = TehsilSOI.objects.create(
            district=self.chik_ballapur, tehsil_name="Bagepalli"
        )

        # Multi-word state, district, and tehsil all at once.
        self.uttar_pradesh = StateSOI.objects.create(state_name="Uttar Pradesh")
        self.sant_kabir_nagar = DistrictSOI.objects.create(
            state=self.uttar_pradesh, district_name="Sant Kabir Nagar"
        )
        self.khalil_abad = TehsilSOI.objects.create(
            district=self.sant_kabir_nagar, tehsil_name="Khalil Abad"
        )

    @staticmethod
    def _mock_geoserver_response():
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {
            "features": [
                {
                    "properties": {
                        "2024-01-01": json.dumps(
                            {"ET": 2.555, "RunOff": 1.333, "Precipitation": 10.219}
                        ),
                        "2024-01-15": json.dumps(
                            {"ET": 3.14159, "RunOff": 0.8, "Precipitation": 5.4}
                        ),
                    }
                }
            ]
        }
        return resp

    @patch("public_api.api._save_mws_v2_to_mongo")
    @patch("public_api.api._load_mws_v2_from_mongo", return_value=None)
    @patch("public_api.views.requests.get")
    def test_two_word_district_succeeds_v2(
        self, mock_requests_get, _mock_load, _mock_save
    ):
        mock_requests_get.return_value = self._mock_geoserver_response()

        response = self.client.get(
            self.url_v2,
            {
                "state": "Karnataka",
                "district": "Chik Ballapur",
                "tehsil": "Bagepalli",
                "mws_id": "12_208104",
            },
            HTTP_X_API_KEY=self.api_key,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        self.assertEqual(body["status"], "success")
        _assert_fortnight_inner_block(self, body["data"])
        self.assertEqual(
            mock_requests_get.call_args.kwargs["params"]["typeName"],
            "mws_layers:deltaG_fortnight_chik_ballapur_bagepalli",
        )

    @patch("public_api.api._save_mws_v2_to_mongo")
    @patch("public_api.api._load_mws_v2_from_mongo", return_value=None)
    @patch("public_api.views.requests.get")
    def test_multi_word_state_district_and_tehsil_succeed_v2(
        self, mock_requests_get, _mock_load, _mock_save
    ):
        mock_requests_get.return_value = self._mock_geoserver_response()

        response = self.client.get(
            self.url_v2,
            {
                "state": "Uttar Pradesh",
                "district": "Sant Kabir Nagar",
                "tehsil": "Khalil Abad",
                "mws_id": "12_208104",
            },
            HTTP_X_API_KEY=self.api_key,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        self.assertEqual(body["status"], "success")
        _assert_fortnight_inner_block(self, body["data"])
        self.assertEqual(
            mock_requests_get.call_args.kwargs["params"]["typeName"],
            "mws_layers:deltaG_fortnight_sant_kabir_nagar_khalil_abad",
        )

    @patch("public_api.api._save_mws_v2_to_mongo")
    @patch("public_api.api._load_mws_v2_from_mongo", return_value=None)
    def test_unknown_district_returns_clean_404_with_accepted_name(self, *_mocks):
        response = self.client.get(
            self.url_v2,
            {
                "state": "Karnataka",
                "district": "Chikkaballapur",
                "tehsil": "Bagepalli",
                "mws_id": "12_208104",
            },
            HTTP_X_API_KEY=self.api_key,
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        body = response.json()
        self.assertEqual(body["status"], "error")
        msg = body["error_message"]
        self.assertIn("unknown district", msg.lower())
        self.assertIn("Chik Ballapur", msg)
        self.assertNotIn("DistrictSOI matching query does not exist", msg)

    @patch("public_api.api._save_mws_v2_to_mongo")
    @patch("public_api.api._load_mws_v2_from_mongo", return_value=None)
    def test_unknown_state_and_tehsil_return_clean_404(self, *_mocks):
        state_resp = self.client.get(
            self.url_v2,
            {
                "state": "Karnatka",
                "district": "Chik Ballapur",
                "tehsil": "Bagepalli",
                "mws_id": "12_208104",
            },
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(state_resp.status_code, status.HTTP_404_NOT_FOUND)
        state_msg = state_resp.json()["error_message"]
        self.assertIn("unknown state", state_msg.lower())
        self.assertIn("Karnataka", state_msg)
        self.assertNotIn("StateSOI matching query does not exist", state_msg)

        tehsil_resp = self.client.get(
            self.url_v2,
            {
                "state": "Karnataka",
                "district": "Chik Ballapur",
                "tehsil": "Bagepali",
                "mws_id": "12_208104",
            },
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(tehsil_resp.status_code, status.HTTP_404_NOT_FOUND)
        tehsil_msg = tehsil_resp.json()["error_message"]
        self.assertIn("unknown tehsil", tehsil_msg.lower())
        self.assertIn("Bagepalli", tehsil_msg)
        self.assertNotIn("TehsilSOI matching query does not exist", tehsil_msg)

    @patch("public_api.views.requests.get")
    def test_v1_two_word_district_and_unknown_district(self, mock_requests_get):
        mock_requests_get.return_value = self._mock_geoserver_response()

        ok_resp = self.client.get(
            self.url_v1,
            {
                "state": "Karnataka",
                "district": "Chik Ballapur",
                "tehsil": "Bagepalli",
                "mws_id": "12_208104",
            },
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(ok_resp.status_code, status.HTTP_200_OK)
        self.assertEqual(ok_resp.json()["mws_id"], "12_208104")

        err_resp = self.client.get(
            self.url_v1,
            {
                "state": "Karnataka",
                "district": "Chikkaballapur",
                "tehsil": "Bagepalli",
                "mws_id": "12_208104",
            },
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(err_resp.status_code, status.HTTP_404_NOT_FOUND)
        err_msg = err_resp.json()["error"]
        self.assertIn("unknown district", err_msg.lower())
        self.assertIn("Chik Ballapur", err_msg)

    @patch("public_api.api._save_mws_v2_to_mongo")
    @patch("public_api.api._load_mws_v2_from_mongo", return_value=None)
    @patch("public_api.views.requests.get")
    def test_missing_and_unparseable_fortnights_return_null_and_drop_trailing_empty(
        self, mock_requests_get, *_mocks
    ):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {
            "features": [
                {
                    "properties": {
                        "uid": "12_208104",
                        # Real zero rain & runoff must stay 0.0
                        "2024-07-01": json.dumps(
                            {"ET": 3.5, "RunOff": 0, "Precipitation": 0.0}
                        ),
                        # Partial missing + bad string -> null, never 0.0 or ""
                        "2024-07-16": json.dumps(
                            {"ET": 4.25, "RunOff": None, "Precipitation": "bad"}
                        ),
                        # Corrupt JSON in middle -> all nulls, never ""
                        "2024-08-01": "{not valid json",
                        # Subsequent valid period -> keeps interior gap above
                        "2024-08-16": json.dumps(
                            {"ET": 5.1, "RunOff": 2.2, "Precipitation": 48.9}
                        ),
                        # Trailing empty/unpopulated periods -> dropped!
                        "2024-09-01": "{}",
                        "2024-09-16": None,
                    }
                }
            ]
        }
        mock_requests_get.return_value = mock_resp

        v2_resp = self.client.get(
            self.url_v2,
            {
                "state": "Karnataka",
                "district": "Chik Ballapur",
                "tehsil": "Bagepalli",
                "mws_id": "12_208104",
            },
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(v2_resp.status_code, status.HTTP_200_OK)
        fortnight = v2_resp.json()["data"]["fortnight"]
        self.assertEqual(
            fortnight["time"],
            ["2024-07-01", "2024-07-16", "2024-08-01", "2024-08-16"],
        )
        self.assertEqual(fortnight["et"], [3.5, 4.25, None, 5.1])
        self.assertEqual(fortnight["runoff"], [0.0, None, None, 2.2])
        self.assertEqual(fortnight["precipitation"], [0.0, None, None, 48.9])

        # v1 must also never emit "" in number fields and must drop trailing empty periods
        v1_resp = self.client.get(
            self.url_v1,
            {
                "state": "Karnataka",
                "district": "Chik Ballapur",
                "tehsil": "Bagepalli",
                "mws_id": "12_208104",
            },
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(v1_resp.status_code, status.HTTP_200_OK)
        ts = v1_resp.json()["time_series"]
        self.assertEqual([r["date"] for r in ts], ["2024-07-01", "2024-07-16", "2024-08-01", "2024-08-16"])
        self.assertIsNone(ts[1]["runoff"])
        self.assertIsNone(ts[1]["precipitation"])
        self.assertIsNone(ts[2]["et"])
        for row in ts:
            for key in ("et", "runoff", "precipitation", "ndvi_crop", "ndvi_shrub", "ndvi_tree"):
                self.assertNotEqual(row[key], "")


class WfsHelperAndP0FixesTests(TestCase):
    """Verify P0 honest-failure fixes, zero-default forest_fire/mining, and API docs."""

    def setUp(self):
        self.client = APIClient()
        self.user = User.objects.create_user(
            username="p0_tester",
            email="p0_tester@example.com",
            password="testpassword123",
        )
        self.api_key_obj, self.api_key = UserAPIKey.objects.create_key(
            user=self.user,
            name="p0-test-key",
            expires_at=timezone.now() + timedelta(days=7),
        )
        self.api_key_obj.api_key = self.api_key
        self.api_key_obj.save()
        self.punjab = StateSOI.objects.create(state_name="Punjab")
        self.ludhiana_d = DistrictSOI.objects.create(
            state=self.punjab, district_name="Ludhiana"
        )
        self.ludhiana_t = TehsilSOI.objects.create(
            district=self.ludhiana_d, tehsil_name="Ludhiana"
        )

    @patch("public_api.views.requests.get")
    def test_wfs_helper_maps_http200_xml_exception_to_layer_not_published(self, mock_get):
        from public_api.views import LayerNotPublished, wfs_get_features

        xml_resp = MagicMock()
        xml_resp.status_code = 200
        xml_resp.headers = {"Content-Type": "application/xml"}
        xml_resp.text = '<?xml version="1.0"?><ServiceExceptionReport><ServiceException>Unknown feature type</ServiceException></ServiceExceptionReport>'
        mock_get.return_value = xml_resp

        with self.assertRaises(LayerNotPublished):
            wfs_get_features("mws", "mws:mws_leh_leh")

    @patch("public_api.views.requests.get")
    def test_get_mwsid_by_latlon_v2_returns_404_layer_not_published_for_xml(self, mock_get):
        admin_resp = MagicMock()
        admin_resp.status_code = 200
        admin_resp.headers = {"Content-Type": "application/json"}
        admin_resp.text = '{"features": []}'
        admin_resp.json.return_value = {
            "features": [
                {
                    "properties": {
                        "STATE": "LADAKH",
                        "District": "LEH",
                        "TEHSIL": "LEH",
                    }
                }
            ]
        }
        mws_xml_resp = MagicMock()
        mws_xml_resp.status_code = 200
        mws_xml_resp.headers = {"Content-Type": "text/xml"}
        mws_xml_resp.text = "<ServiceExceptionReport>Feature type mws:mws_leh_leh unknown</ServiceExceptionReport>"
        mock_get.side_effect = [admin_resp, mws_xml_resp]

        resp = self.client.get(
            reverse("get_mwsid_by_latlon_v2"),
            {"latitude": "34.15", "longitude": "77.57"},
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        body = resp.json()
        self.assertEqual(body["status"], "error")
        self.assertEqual(body["code"], "LAYER_NOT_PUBLISHED")

    @patch("public_api.views.requests.get")
    def test_get_admin_details_by_latlon_v2_returns_502_on_upstream_error(self, mock_get):
        import requests as req_lib

        mock_get.side_effect = req_lib.exceptions.ConnectionError("geoserver down")
        resp = self.client.get(
            reverse("get_admin_details_by_lat_lon_v2"),
            {"latitude": "30.9", "longitude": "75.85"},
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(resp.status_code, status.HTTP_502_BAD_GATEWAY)
        body = resp.json()
        self.assertEqual(body["status"], "error")
        self.assertEqual(body["code"], "UPSTREAM_UNAVAILABLE")

    @patch("public_api.api._load_mws_v2_from_mongo", return_value=None)
    @patch("public_api.views.requests.get")
    def test_get_mws_data_v2_returns_404_layer_not_published_with_fallback(
        self, mock_get, _mock_mongo
    ):
        xml_resp = MagicMock()
        xml_resp.status_code = 200
        xml_resp.headers = {"Content-Type": "application/xml"}
        xml_resp.text = "<ServiceExceptionReport>Unknown layer</ServiceExceptionReport>"
        mock_get.return_value = xml_resp

        resp = self.client.get(
            reverse("get-mws-data-v2"),
            {
                "state": "Punjab",
                "district": "Ludhiana",
                "tehsil": "Ludhiana",
                "mws_id": "13_119514",
            },
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        body = resp.json()
        self.assertEqual(body["code"], "LAYER_NOT_PUBLISHED")
        self.assertEqual(
            body["details"]["fallback"],
            "get_tehsil_data?data=hydrological_seasonal",
        )

    @patch("public_api.api.excel_file_exists", return_value=("/tmp/missing.xlsx", False))
    def test_kyl_endpoints_respect_false_excel_file_exists_tuple(self, _mock_exists):
        params = {
            "state": "Punjab",
            "district": "Ludhiana",
            "tehsil": "Ludhiana",
            "mws_id": "13_119514",
        }
        v2_resp = self.client.get(
            reverse("get_mws_kyl_indicators_v2"),
            params,
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(v2_resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(v2_resp.json()["code"], "DATA_NOT_GENERATED")

        v1_resp = self.client.get(
            reverse("get-mws-kyl-indicator"),
            params,
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(v1_resp.status_code, status.HTTP_404_NOT_FOUND)

    @patch("public_api.api.get_tehsil_json")
    @patch("public_api.api.excel_file_exists", return_value=("/tmp/x.xlsx", True))
    def test_tehsil_data_v2_returns_zero_for_absent_forest_fire_and_mining(
        self, _mock_exists, mock_json
    ):
        mock_json.return_value = {
            "mws": [{"uid": "13_1"}, {"uid": "13_2"}],
            "forest_fire": [
                {
                    "uid": "13_1",
                    "fire_count_per_year": -9999,
                    "fire_frp_max_in_mw": None,
                    "fire_frp_mean_in_mw": -9999,
                }
            ],
        }
        # 1. Existing forest_fire with -9999/None -> normalized to 0
        fire_resp = self.client.get(
            reverse("get_tehsil_data_v2"),
            {
                "state": "Punjab",
                "district": "Ludhiana",
                "tehsil": "Ludhiana",
                "data": "forest_fire",
            },
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(fire_resp.status_code, status.HTTP_200_OK)
        fire_rows = fire_resp.json()["data"]["tehsil_data"]["forest_fire"]
        self.assertEqual(fire_rows[0]["fire_count_per_year"], 0)
        self.assertEqual(fire_rows[0]["fire_frp_max_in_mw"], 0)
        self.assertEqual(fire_rows[0]["fire_frp_mean_in_mw"], 0)

        # 2. Missing mining sheet -> 200 OK with mining_count=0 per MWS (not 404)
        mining_resp = self.client.get(
            reverse("get_tehsil_data_v2"),
            {
                "state": "Punjab",
                "district": "Ludhiana",
                "tehsil": "Ludhiana",
                "data": "mining",
            },
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(mining_resp.status_code, status.HTTP_200_OK)
        mining_data = mining_resp.json()["data"]
        mining_rows = mining_data["tehsil_data"]["mining"]
        self.assertEqual(
            mining_rows,
            [{"uid": "13_1", "mining": 0}, {"uid": "13_2", "mining": 0}],
        )
        self.assertEqual(mining_data["tehsil_units"]["mining"]["mining"], "count")

    def test_tehsil_data_type_docs_has_no_duplicates_and_docs_mention_mws_overlap(self):
        from nrm_app.urls import _PUBLIC_API_REDOC_DESCRIPTION
        from public_api.dataset_filters import TEHSIL_DATA_TYPE_DOCS
        from public_api.swagger_schemas import (
            mws_geometries_schema_v2,
            tehsil_data_schema_v2,
        )

        keys = [k for k, _ in TEHSIL_DATA_TYPE_DOCS]
        self.assertEqual(len(keys), len(set(keys)))
        for doc in (
            _PUBLIC_API_REDOC_DESCRIPTION,
            tehsil_data_schema_v2["operation_description"],
            mws_geometries_schema_v2["operation_description"],
        ):
            self.assertIn("overlapping", doc.lower())
            self.assertIn("de-duplicate", doc.lower())
