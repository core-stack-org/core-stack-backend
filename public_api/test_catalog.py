"""Tests for RFC 9727 and the v2 public API property catalog."""

from datetime import timedelta
from unittest.mock import patch

from django.test import SimpleTestCase, TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from geoadmin.models import UserAPIKey
from public_api.catalog import (
    CATALOG_BY_ID,
    MWS_FORTNIGHT_FIELD_NAMES,
    PUBLIC_API_CATALOG,
    filter_mws_fortnight_fields,
    parse_mws_fields_filter,
)
from users.models import User
from utilities.openmeteo_format import fortnight_structure_from_mws


class CatalogModuleTests(SimpleTestCase):
    def test_catalog_covers_every_v2_dataset_and_waterbody_route(self):
        ids = {entry["id"] for entry in PUBLIC_API_CATALOG}
        self.assertIn("get_mws_data", ids)
        self.assertIn("get_tehsil_data", ids)
        self.assertIn("get_waterbodies_data_by_admin", ids)
        self.assertEqual(len(ids), len(PUBLIC_API_CATALOG))

    def test_tehsil_catalog_lists_column_units(self):
        entry = CATALOG_BY_ID["get_tehsil_data"]
        by_name = {item["name"]: item for item in entry["properties"]}
        drought = by_name["drought"]["unit"]
        self.assertIsInstance(drought, dict)
        self.assertEqual(drought["area"], "ha")
        self.assertEqual(drought["no_drought"], "weeks")
        self.assertEqual(by_name["stream_order"]["unit"]["value"], "%")
        self.assertEqual(by_name["stream_order"]["unit"]["order"], "order")
        self.assertEqual(by_name["hydrological_annual"]["unit"]["precipitation"], "mm")
        for item in entry["properties"]:
            self.assertIsInstance(item["unit"], dict)
            self.assertTrue(item["unit"])

    def test_mws_catalog_lists_selectable_fortnight_metrics(self):
        entry = CATALOG_BY_ID["get_mws_data"]
        selectable = {
            item["name"] for item in entry["properties"] if item["selectable"]
        }
        self.assertEqual(selectable, MWS_FORTNIGHT_FIELD_NAMES)
        self.assertEqual(entry["select_param"], "fields")

    def test_parse_and_filter_mws_fields(self):
        self.assertIsNone(parse_mws_fields_filter([]))
        self.assertEqual(parse_mws_fields_filter(["et,runoff"]), {"et", "runoff"})
        with self.assertRaises(ValueError):
            parse_mws_fields_filter(["not_a_metric"])

        payload = fortnight_structure_from_mws(
            {
                "mws_id": "12_208104",
                "time_series": [
                    {"date": "2024-01-01", "et": 1, "runoff": 2, "precipitation": 3}
                ],
            }
        )
        filtered = filter_mws_fortnight_fields(payload, {"et"})
        self.assertEqual(set(filtered["fortnight"].keys()), {"time", "et"})
        self.assertNotIn("runoff", filtered["fortnight"])
        self.assertEqual(payload["fortnight"]["runoff"], [2])


class CatalogApiTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = User.objects.create_user(
            username="catalog_tester",
            email="catalog_tester@example.com",
            password="password123",
        )
        self.api_key_obj, self.api_key = UserAPIKey.objects.create_key(
            user=self.user,
            name="catalog-test-key",
            expires_at=timezone.now() + timedelta(days=30),
        )
        self.api_key_obj.api_key = self.api_key
        self.api_key_obj.save()

    def _get(self, url_name, params=None, *, with_key=True, **kwargs):
        headers = {}
        if with_key:
            headers["HTTP_X_API_KEY"] = self.api_key
        return self.client.get(reverse(url_name), params or {}, **headers, **kwargs)

    def test_rfc9727_catalog_is_public_linkset(self):
        response = self.client.get(reverse("rfc9727-api-catalog"))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("application/linkset+json", response["Content-Type"])
        self.assertIn("rfc9727", response["Content-Type"])
        body = response.json()
        self.assertIn("linkset", body)
        item = body["linkset"][0]
        self.assertTrue(item["anchor"].endswith("/api/v2/"))
        self.assertTrue(item["service-desc"][0]["href"].endswith("/swagger.json"))
        self.assertTrue(item["service-doc"][0]["href"].endswith("/redoc/"))

    def test_catalog_list_requires_api_key(self):
        response = self._get("get_public_api_catalog_v2", with_key=False)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_catalog_list_success(self):
        response = self._get("get_public_api_catalog_v2")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        self.assertEqual(body["status"], "success")
        data = body["data"]
        self.assertEqual(data["catalog"]["standards"], ["openapi", "rfc9727"])
        self.assertIn("selectable", data["catalog"]["description"])
        self.assertIn("tehsil_units", data["catalog"]["description"])
        ids = [item["id"] for item in data["apis"]]
        self.assertIn("get_mws_data", ids)
        self.assertIn("get_waterbody_data", ids)
        mws = next(item for item in data["apis"] if item["id"] == "get_mws_data")
        self.assertEqual(mws["select_param"], "fields")
        self.assertTrue(mws["properties_url"].endswith("/api/v2/catalog/get_mws_data/"))

    def test_catalog_list_group_filter(self):
        response = self._get("get_public_api_catalog_v2", {"group": "waterbody"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        ids = [item["id"] for item in response.json()["data"]["apis"]]
        self.assertEqual(set(ids), {"get_waterbodies_data_by_admin", "get_waterbody_data"})

        bad = self._get("get_public_api_catalog_v2", {"group": "nope"})
        self.assertEqual(bad.status_code, status.HTTP_400_BAD_REQUEST)

    def test_catalog_item_properties(self):
        response = self.client.get(
            reverse("get_public_api_catalog_item_v2", kwargs={"api_id": "get_mws_data"}),
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()["data"]
        names = {item["name"] for item in data["properties"]}
        self.assertTrue({"et", "runoff", "precipitation", "time"}.issubset(names))
        et = next(item for item in data["properties"] if item["name"] == "et")
        self.assertTrue(et["selectable"])
        self.assertEqual(et["unit"], "mm")

    def test_unknown_catalog_id_returns_404(self):
        response = self.client.get(
            reverse("get_public_api_catalog_item_v2", kwargs={"api_id": "not_an_api"}),
            HTTP_X_API_KEY=self.api_key,
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(response.json()["status"], "error")


class MwsFieldsFilterApiTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = User.objects.create_user(
            username="fields_tester",
            email="fields_tester@example.com",
            password="password123",
        )
        self.api_key_obj, self.api_key = UserAPIKey.objects.create_key(
            user=self.user,
            name="fields-test-key",
            expires_at=timezone.now() + timedelta(days=30),
        )
        self.api_key_obj.api_key = self.api_key
        self.api_key_obj.save()
        self.url = reverse("get-mws-data-v2")
        self.params = {
            "state": "Uttar Pradesh",
            "district": "Jaunpur",
            "tehsil": "Badlapur",
            "mws_id": "12_208104",
            "fields": "et,runoff",
        }

    @patch("public_api.api._save_mws_v2_to_mongo")
    @patch("public_api.api._load_mws_v2_from_mongo", return_value=None)
    @patch("public_api.api.get_mws_time_series_data")
    def test_fields_keeps_only_requested_metrics(self, mock_get_mws, _load, mock_save):
        mock_get_mws.return_value = {
            "mws_id": "12_208104",
            "time_series": [
                {"date": "2024-01-01", "et": 2.5, "runoff": 1.3, "precipitation": 10.2}
            ],
        }
        response = self.client.get(self.url, self.params, HTTP_X_API_KEY=self.api_key)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        fortnight = response.json()["data"]["fortnight"]
        self.assertEqual(set(fortnight.keys()), {"time", "et", "runoff"})
        saved = mock_save.call_args[0][4]
        self.assertIn("precipitation", saved["fortnight"])

    def test_unknown_fields_returns_400(self):
        params = dict(self.params, fields="not_a_metric")
        response = self.client.get(self.url, params, HTTP_X_API_KEY=self.api_key)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Unknown fields", response.json()["error_message"])
