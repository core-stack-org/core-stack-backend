import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import requests
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase, TestCase, override_settings
from rest_framework.test import APIClient

from computing.bulk_layer_generation import Location
from stats_generator import local_layers
from stats_generator.models import LayerInfo
from stats_generator.mws_indicators import generate_mws_data_for_kyl_filters
from stats_generator.utils import (
    fetch_drought_layer_years,
    fetch_layers_for_excel_generation,
    get_vector_layer_geoserver,
)


class LayerInfoApiTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.raster_layer = LayerInfo.objects.create(
            layer_name="lulc_{district}_{block}",
            layer_type="raster",
            workspace="lulc",
            excel_to_be_generated=False,
            start_year=2017,
            end_year=2023,
        )
        cls.drought_layer = LayerInfo.objects.create(
            layer_name="{district}_{block}_drought",
            layer_type="vector",
            workspace="drought",
            excel_to_be_generated=True,
            sheet_name="croppingDrought_kharif",
            start_year=2017,
            end_year=2024,
        )
        cls.mining_layer = LayerInfo.objects.create(
            layer_name="{district}_{block}_mining",
            layer_type="vector",
            workspace="mining",
            excel_to_be_generated=True,
            sheet_name="mining",
            can_be_absent=True,
        )
        cls.disabled_vector = LayerInfo.objects.create(
            layer_name="{district}_{block}_disabled",
            layer_type="vector",
            workspace="disabled_ws",
            excel_to_be_generated=False,
        )

    def setUp(self):
        self.client = APIClient()

    @override_settings(PROD_BACKEND_API_KEY="")
    def test_lists_all_rows_and_supports_filters(self):
        resp = self.client.get("/api/v1/layer_info/")
        self.assertEqual(resp.status_code, 200)
        names = [row["layer_name"] for row in resp.json()["layers"]]
        self.assertEqual(len(names), 4)
        self.assertEqual(
            set(names),
            {
                self.raster_layer.layer_name,
                self.drought_layer.layer_name,
                self.mining_layer.layer_name,
                self.disabled_vector.layer_name,
            },
        )

        resp = self.client.get(
            "/api/v1/layer_info/",
            {"layer_type": "vector", "excel_to_be_generated": "true"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(
            {row["workspace"] for row in resp.json()["layers"]},
            {"drought", "mining"},
        )

        resp = self.client.get(
            "/api/v1/layer_info/",
            {"layer_type": "vector", "excel_to_be_generated": "false"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(
            [row["workspace"] for row in resp.json()["layers"]],
            ["disabled_ws"],
        )

        resp = self.client.get(
            "/api/v1/layer_info/",
            {"layer_type": "vector", "workspace": "drought"},
        )
        self.assertEqual(resp.status_code, 200)
        layers = resp.json()["layers"]
        self.assertEqual(len(layers), 1)
        self.assertEqual(layers[0]["start_year"], 2017)
        self.assertEqual(layers[0]["end_year"], 2024)

    @override_settings(PROD_BACKEND_API_KEY="")
    def test_rejects_invalid_boolean_filter_and_write_methods(self):
        resp = self.client.get(
            "/api/v1/layer_info/", {"excel_to_be_generated": "maybe"}
        )
        self.assertEqual(resp.status_code, 400)

        post_resp = self.client.post("/api/v1/layer_info/", {})
        self.assertEqual(post_resp.status_code, 405)

    @override_settings(PROD_BACKEND_API_KEY="top-secret")
    def test_validates_api_key_when_configured(self):
        self.assertEqual(self.client.get("/api/v1/layer_info/").status_code, 401)
        self.assertEqual(
            self.client.get(
                "/api/v1/layer_info/", HTTP_X_API_KEY="wrong"
            ).status_code,
            401,
        )
        ok = self.client.get(
            "/api/v1/layer_info/", HTTP_X_API_KEY="top-secret"
        )
        self.assertEqual(ok.status_code, 200)
        self.assertEqual(len(ok.json()["layers"]), 4)


class FetchLayerInfoTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        LayerInfo.objects.create(
            layer_name="local_only_{district}_{block}",
            layer_type="vector",
            workspace="local_ws",
            excel_to_be_generated=True,
            start_year=2015,
            end_year=2016,
        )
        LayerInfo.objects.create(
            layer_name="{district}_{block}_drought",
            layer_type="vector",
            workspace="drought",
            excel_to_be_generated=True,
            start_year=2015,
            end_year=2016,
        )

    @override_settings(
        PROD_BACKEND_URL="https://prod.example/",
        PROD_BACKEND_API_KEY="secret-key",
    )
    @patch("stats_generator.utils.requests.get")
    def test_fetch_layers_uses_prod_api_when_configured(self, get):
        get.return_value = MagicMock(
            json=lambda: {
                "layers": [
                    {
                        "id": 99,
                        "layer_name": "prod_{district}_{block}",
                        "layer_type": "vector",
                        "workspace": "prod_ws",
                        "excel_to_be_generated": True,
                        "start_year": 2018,
                        "end_year": 2025,
                    }
                ]
            }
        )
        layers = fetch_layers_for_excel_generation()
        self.assertEqual(
            layers,
            [
                {
                    "layer_name": "prod_{district}_{block}",
                    "workspace": "prod_ws",
                    "start_year": 2018,
                    "end_year": 2025,
                }
            ],
        )
        get.assert_called_once_with(
            "https://prod.example/api/v1/layer_info/",
            params={"layer_type": "vector", "excel_to_be_generated": "true"},
            headers={"X-Api-Key": "secret-key"},
            timeout=30,
        )
        get.return_value.raise_for_status.assert_called_once_with()

    @override_settings(PROD_BACKEND_URL="https://prod.example")
    @patch("stats_generator.utils.requests.get")
    def test_fetch_layers_fails_instead_of_falling_back_to_local_db(self, get):
        get.side_effect = requests.ConnectionError("prod down")
        with self.assertRaisesRegex(RuntimeError, "Failed to fetch LayerInfo"):
            fetch_layers_for_excel_generation()

        get.side_effect = None
        get.return_value = MagicMock(json=lambda: {"unexpected": []})
        with self.assertRaisesRegex(RuntimeError, "invalid response"):
            fetch_layers_for_excel_generation()

        get.return_value = MagicMock(json=lambda: {"layers": [{"workspace": "x"}]})
        with self.assertRaisesRegex(RuntimeError, "malformed layer entry"):
            fetch_layers_for_excel_generation()

    @override_settings(PROD_BACKEND_URL="")
    def test_fetch_layers_reads_local_db_when_prod_url_unset(self):
        layers = fetch_layers_for_excel_generation()
        self.assertEqual(
            {layer["workspace"] for layer in layers}, {"local_ws", "drought"}
        )

    @override_settings(
        PROD_BACKEND_URL="https://prod.example",
        PROD_BACKEND_API_KEY="secret-key",
    )
    @patch("stats_generator.utils.requests.get")
    def test_fetch_drought_years_uses_prod_api_when_configured(self, get):
        get.return_value = MagicMock(
            json=lambda: {
                "layers": [
                    {
                        "layer_name": "{district}_{block}_drought",
                        "workspace": "drought",
                        "start_year": 2017,
                        "end_year": 2019,
                    }
                ]
            }
        )
        self.assertEqual(fetch_drought_layer_years(), ["2017", "2018", "2019"])
        get.assert_called_once_with(
            "https://prod.example/api/v1/layer_info/",
            params={"layer_type": "vector", "workspace": "drought"},
            headers={"X-Api-Key": "secret-key"},
            timeout=30,
        )

    @override_settings(PROD_BACKEND_URL="https://prod.example")
    @patch("stats_generator.utils.requests.get")
    def test_fetch_drought_years_fails_instead_of_falling_back_to_local_db(self, get):
        get.side_effect = requests.Timeout("timed out")
        with self.assertRaisesRegex(RuntimeError, "Failed to fetch LayerInfo"):
            fetch_drought_layer_years()

        get.side_effect = None
        get.return_value = MagicMock(json=lambda: {"layers": []})
        with self.assertRaisesRegex(RuntimeError, "Expected 1 drought LayerInfo"):
            fetch_drought_layer_years()

        get.return_value = MagicMock(
            json=lambda: {"layers": [{"workspace": "drought", "start_year": None, "end_year": 2024}]}
        )
        with self.assertRaisesRegex(RuntimeError, "missing start_year/end_year"):
            fetch_drought_layer_years()

    @override_settings(PROD_BACKEND_URL="")
    def test_fetch_drought_years_reads_local_db_when_prod_url_unset(self):
        self.assertEqual(fetch_drought_layer_years(), ["2015", "2016"])

    @override_settings(PROD_BACKEND_URL="https://prod.example")
    @patch("stats_generator.utils.requests.get", side_effect=requests.ConnectionError("boom"))
    def test_excel_generation_surfaces_prod_error_without_creating_workbook(self, _get):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.xlsx"
            with self.assertRaisesRegex(RuntimeError, "Failed to fetch LayerInfo"):
                get_vector_layer_geoserver("bihar", "banka", "banka", xlsx_file=str(out))
            self.assertFalse(out.exists())

    @override_settings(PROD_BACKEND_URL="https://prod.example")
    @patch(
        "stats_generator.mws_indicators.fetch_drought_layer_years",
        side_effect=RuntimeError("prod LayerInfo unreachable"),
    )
    def test_mws_indicators_fails_instead_of_swallowing_prod_error(self, fetch_years):
        with tempfile.TemporaryDirectory() as tmp:
            xl_dir = Path(tmp) / "data/stats_excel_files/BIHAR/BANKA"
            xl_dir.mkdir(parents=True)
            xl_path = xl_dir / "banka_banka.xlsx"
            with pd.ExcelWriter(xl_path, engine="openpyxl") as writer:
                pd.DataFrame(
                    {"UID": ["1_1"], "Precipitation_2020": [100.0], "Precipitation_2021": [110.0]}
                ).to_excel(writer, sheet_name="hydrological_annual", index=False)

            with patch("stats_generator.mws_indicators.EXCEL_PATH", tmp):
                resp = generate_mws_data_for_kyl_filters(
                    "bihar", "banka", "banka", "json", regenerate=1
                )

        fetch_years.assert_called_once_with()
        self.assertEqual(resp.status_code, 500)
        self.assertIn("prod LayerInfo unreachable", resp.data["message"])


class ActiveLocationSlugsTests(SimpleTestCase):
    @override_settings(PROD_BACKEND_URL="https://prod.example")
    @patch("computing.bulk_layer_generation.get_active_locations")
    @patch("computing.bulk_layer_generation.get_active_locations_from_api")
    def test_uses_prod_api_when_configured(self, from_api, from_db):
        from_api.return_value = [Location("Jharkhand", "Dumka", "Mulshi Paud")]
        slugs = local_layers.active_location_slugs()
        self.assertEqual(slugs, {("jharkhand", "dumka", "mulshi_paud")})
        from_api.assert_called_once_with()
        from_db.assert_not_called()

        self.assertTrue(
            local_layers.is_active_location("Jharkhand", "Dumka", "mulshi_paud")
        )
        self.assertFalse(
            local_layers.is_active_location("Jharkhand", "Dumka", "other")
        )

    @override_settings(PROD_BACKEND_URL="https://prod.example")
    @patch("computing.bulk_layer_generation.get_active_locations")
    @patch(
        "computing.bulk_layer_generation.get_active_locations_from_api",
        side_effect=ValueError("prod API failed"),
    )
    def test_fails_instead_of_falling_back_to_local_db(self, from_api, from_db):
        with self.assertRaisesRegex(ValueError, "prod API failed"):
            local_layers.active_location_slugs()
        from_api.assert_called_once_with()
        from_db.assert_not_called()

    @override_settings(PROD_BACKEND_URL="")
    @patch("computing.bulk_layer_generation.get_active_locations_from_api")
    @patch("computing.bulk_layer_generation.get_active_locations")
    def test_uses_local_db_when_prod_url_unset(self, from_db, from_api):
        from_db.return_value = [Location("Bihar", "Banka", "Banka")]
        self.assertEqual(
            local_layers.active_location_slugs(), {("bihar", "banka", "banka")}
        )
        from_db.assert_called_once_with()
        from_api.assert_not_called()


class GenerateLocalStatsExcelCommandTests(SimpleTestCase):
    @override_settings(PROD_BACKEND_URL="https://prod.example")
    @patch(
        "stats_generator.management.commands.generate_local_stats_excel.fetch_layers_for_excel_generation",
        return_value=[
            {"layer_name": "mws_{district}_{block}", "workspace": "mws", "start_year": None, "end_year": None}
        ],
    )
    @patch(
        "stats_generator.management.commands.generate_local_stats_excel.get_active_locations"
    )
    @patch(
        "stats_generator.management.commands.generate_local_stats_excel.get_active_locations_from_api",
        return_value=[Location("Bihar", "Banka", "Banka")],
    )
    @patch(
        "stats_generator.local_layers.active_location_slugs",
        return_value={("bihar", "banka", "banka")},
    )
    def test_active_only_uses_prod_api_when_configured(
        self, _active_slugs, from_api, from_db, _layers
    ):
        call_command("generate_local_stats_excel", active_only=True, dry_run=True)
        from_api.assert_called_once()
        from_db.assert_not_called()

    @override_settings(PROD_BACKEND_URL="https://prod.example")
    @patch(
        "stats_generator.management.commands.generate_local_stats_excel.get_all_locations",
        return_value=[Location("Bihar", "Banka", "Banka")],
    )
    @patch(
        "stats_generator.local_layers.active_location_slugs",
        side_effect=ValueError("prod active locations failed"),
    )
    def test_raises_command_error_when_prod_fetch_fails(self, _active, _all):
        with self.assertRaisesRegex(CommandError, "prod active locations failed"):
            call_command("generate_local_stats_excel", dry_run=True)
