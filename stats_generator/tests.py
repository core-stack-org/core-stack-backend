from io import BytesIO

import pandas as pd
from django.test import SimpleTestCase

from stats_generator.utils import create_excel_crop_inten


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
