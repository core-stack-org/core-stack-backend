from io import BytesIO
import os
import tempfile
from unittest.mock import patch

import pandas as pd
from django.test import SimpleTestCase

from stats_generator.utils import create_excel_crop_inten, get_vector_layer_geoserver


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


class EmptyStatsWorkbookTest(SimpleTestCase):
    def test_creates_informative_sheet_when_no_layers_are_written(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("stats_generator.utils.EXCEL_PATH", temp_dir):
                with patch(
                    "stats_generator.utils.fetch_layers_for_excel_generation",
                    return_value=[],
                ):
                    results = get_vector_layer_geoserver(
                        "jharkhand", "dumka", "masalia"
                    )

            workbook_path = os.path.join(
                temp_dir,
                "data",
                "stats_excel_files",
                "JHARKHAND",
                "DUMKA",
                "dumka_masalia.xlsx",
            )
            self.assertEqual(results, [])
            with pd.ExcelFile(workbook_path) as workbook:
                self.assertEqual(workbook.sheet_names, ["No data"])
            message = pd.read_excel(workbook_path, sheet_name="No data").iloc[0, 0]
            self.assertEqual(message, "No data available for the selected layers.")
