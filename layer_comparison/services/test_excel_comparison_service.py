import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from openpyxl import Workbook

from layer_comparison.services.excel_comparison_service import (
    get_comparable_sheets,
    get_scatter_points,
)
from layer_comparison.services.excel_comparison_service import (
    _rows_by_key,
    _sheet_metadata,
)


class ExcelComparisonServiceTests(unittest.TestCase):
    def test_sheet_metadata_finds_shared_numeric_metrics_and_uid_key(self):
        old_book = Workbook()
        new_book = Workbook()
        old_sheet = old_book.active
        new_sheet = new_book.active
        old_sheet.append(["UID", "rainfall", "old_only"])
        old_sheet.append(["mws-1", 10, 4])
        new_sheet.append(["uid", "Rainfall", "new_only"])
        new_sheet.append(["mws-1", 12, 9])

        metadata = _sheet_metadata(old_sheet, new_sheet)

        self.assertEqual(metadata["match_key"], "UID")
        self.assertEqual(metadata["match_method"], "column")
        self.assertEqual(metadata["metrics"], ["rainfall"])
        old_book.close()
        new_book.close()

    def test_keyed_rows_keep_numeric_values_and_skip_missing_values(self):
        workbook = Workbook()
        sheet = workbook.active
        sheet.append(["UID", "value"])
        sheet.append(["mws-1", 3.5])
        sheet.append(["mws-2", None])
        rows = _rows_by_key(sheet, 0, 1)

        self.assertEqual(rows, {"mws-1": [3.5]})
        workbook.close()

    def test_sheet_without_common_numeric_columns_is_omitted(self):
        old_book = Workbook()
        new_book = Workbook()
        old_book.active.append(["UID", "category"])
        old_book.active.append(["mws-1", "forest"])
        new_book.active.append(["UID", "category"])
        new_book.active.append(["mws-1", "water"])

        self.assertIsNone(_sheet_metadata(old_book.active, new_book.active))
        old_book.close()
        new_book.close()

    def test_workbooks_are_paired_by_location_and_scatter_uses_uid(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            old_root = Path(temp_dir) / "old"
            new_root = Path(temp_dir) / "new"
            for root in (old_root, new_root):
                location_dir = root / "JHARKHAND" / "DUMKA"
                location_dir.mkdir(parents=True)
                workbook = Workbook()
                sheet = workbook.active
                sheet.title = "rainfall"
                sheet.append(["UID", "annual_total"])
                workbook.save(location_dir / "dumka_masalia.xlsx")
                workbook.close()

            old_path = old_root / "JHARKHAND" / "DUMKA" / "dumka_masalia.xlsx"
            new_path = new_root / "JHARKHAND" / "DUMKA" / "dumka_masalia.xlsx"
            old_workbook = Workbook()
            old_workbook.active.title = "rainfall"
            old_workbook.active.append(["UID", "annual_total"])
            old_workbook.active.append(["mws-1", 10])
            old_workbook.save(old_path)
            old_workbook.close()
            new_workbook = Workbook()
            new_workbook.active.title = "rainfall"
            new_workbook.active.append(["UID", "annual_total"])
            new_workbook.active.append(["mws-1", 12])
            new_workbook.save(new_path)
            new_workbook.close()

            with patch(
                "layer_comparison.services.excel_comparison_service._excel_roots",
                return_value=(old_root, new_root),
            ):
                sheets = get_comparable_sheets("Jharkhand", "Dumka", "Masalia")
                points = get_scatter_points(
                    "Jharkhand", "Dumka", "Masalia", "rainfall", "annual_total"
                )

        self.assertEqual([sheet["name"] for sheet in sheets["sheets"]], ["rainfall"])
        self.assertEqual(points["match_key"], "UID")
        self.assertEqual(
            points["points"],
            [{"id": "mws-1", "old": 10.0, "new": 12.0}],
        )


if __name__ == "__main__":
    unittest.main()
