from io import BytesIO

import pandas as pd
from django.test import SimpleTestCase

from stats_generator.utils import (
    create_excel_crop_inten,
    create_excel_for_forest_fire,
    create_excel_for_mining,
)


class CreateExcelCropIntensityTest(SimpleTestCase):
    def test_uses_named_croppable_area_with_gee_sum_fallback(self):
        data = {
            "features": [
                {
                    "properties": {
                        "uid": "local",
                        "total_cropable_area_ever_hydroyear_2017_2022": 125.5,
                    }
                },
                {"properties": {"uid": "gee", "sum": 750000}},
                {"properties": {"uid": "missing"}},
            ]
        }
        output = BytesIO()

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            create_excel_crop_inten(data, None, writer, 2022, 2022)

        output.seek(0)
        result = pd.read_excel(output, sheet_name="croppingIntensity_annual")
        areas_by_uid = result.set_index("UID")["sum_area_in_ha"].to_dict()

        self.assertEqual(
            areas_by_uid,
            {"gee": 75.0, "local": 125.5, "missing": 0.0},
        )


class CreateExcelForestFireAndMiningTest(SimpleTestCase):
    def test_forest_fire_fills_missing_with_zero_never_minus_9999(self):
        data = {
            "features": [
                {
                    "properties": {
                        "uid": "12_1",
                        "fire_count_per_year": None,
                        "fire_frp_max": None,
                        "fire_frp_mean": -9999,
                    }
                },
                {
                    "properties": {
                        "uid": "12_2",
                        "fire_count_per_year": 1.5,
                        "fire_frp_max": 22.345,
                        "fire_frp_mean": 11.1,
                    }
                },
            ]
        }
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            create_excel_for_forest_fire(data, writer)
        output.seek(0)
        df = pd.read_excel(output, sheet_name="forest_fire").set_index("UID")
        self.assertEqual(df.loc["12_1", "fire_count_per_year"], 0)
        self.assertEqual(df.loc["12_1", "fire_frp_max_in_MW"], 0)
        self.assertEqual(df.loc["12_1", "fire_frp_mean_in_MW"], 0)
        self.assertEqual(df.loc["12_2", "fire_count_per_year"], 1.5)
        self.assertEqual(df.loc["12_2", "fire_frp_max_in_MW"], 22.34)

    def test_forest_fire_and_mining_handle_empty_features_cleanly(self):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            create_excel_for_forest_fire({"features": []}, writer)
            create_excel_for_mining({"features": []}, writer)
        output.seek(0)
        sheets = pd.read_excel(output, sheet_name=None)
        self.assertIn("forest_fire", sheets)
        self.assertIn("mining", sheets)
        self.assertTrue(sheets["forest_fire"].empty)
        self.assertTrue(sheets["mining"].empty)
