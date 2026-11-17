import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from openpyxl import Workbook

from layer_comparison.services import excel_comparison_service as service
from layer_comparison.services.excel_comparison_service import (
    clear_analysis_cache,
    get_comparable_sheets,
    get_multi_metric_slice,
    get_scatter_points,
    get_workbook_analysis,
)
from layer_comparison.services.excel_comparison_service import (
    _rows_by_key,
    _natural_key,
    _sheet_metadata,
    summarize_pairs,
)


class ExcelComparisonServiceTests(unittest.TestCase):
    def setUp(self):
        clear_analysis_cache()

    def _write_workbook(self, root, rows):
        location_dir = root / "JHARKHAND" / "DUMKA"
        location_dir.mkdir(parents=True, exist_ok=True)
        path = location_dir / "dumka_masalia.xlsx"
        workbook = Workbook()
        workbook.active.title = "rainfall"
        workbook.active.append(["UID", "annual_total"])
        for row in rows:
            workbook.active.append(row)
        workbook.save(path)
        workbook.close()
        return path

    def test_workbook_analysis_is_cached_until_a_workbook_changes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            old_root = Path(temp_dir) / "old"
            new_root = Path(temp_dir) / "new"
            self._write_workbook(old_root, [["mws-1", 10]])
            new_path = self._write_workbook(new_root, [["mws-1", 12]])

            with patch(
                "layer_comparison.services.excel_comparison_service._excel_roots",
                return_value=(old_root, new_root),
            ), patch(
                "layer_comparison.services.excel_comparison_service.load_workbook",
                wraps=service.load_workbook,
            ) as loader:
                first = get_workbook_analysis("Jharkhand", "Dumka", "Masalia")
                second = get_workbook_analysis("Jharkhand", "Dumka", "Masalia")
                self.assertIs(first, second)
                self.assertEqual(loader.call_count, 2)  # old + new, opened once

                self._write_workbook(new_root, [["mws-1", 12], ["mws-2", 99]])
                stat = new_path.stat()
                os.utime(new_path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
                third = get_workbook_analysis("Jharkhand", "Dumka", "Masalia")

        self.assertEqual(loader.call_count, 4)
        self.assertEqual(third["sheets"][0]["unmatched"]["new_only"], ["mws-2"])

    def test_comparison_uses_gee_workbook_not_legacy_backup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("django.conf.settings.EXCEL_PATH", temp_dir):
                old_root, new_root = service._excel_roots()

        data_root = Path(temp_dir) / "data"
        self.assertEqual(old_root, data_root / "stats_excel_files_gee")
        self.assertEqual(new_root, data_root / "stats_excel_files")

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
            [{"id": "mws-1", "old": 10.0, "new": 12.0, "diff": 2.0, "pct_diff": 20.0}],
        )
        self.assertEqual(points["summary"]["count"], 1)
        self.assertEqual(points["summary"]["higher_in_new"], 1)
        self.assertEqual(points["unmatched"]["old_only_count"], 0)

    def test_summary_reports_agreement_statistics(self):
        pairs = [
            {"id": "a", "old": 1.0, "new": 1.0},
            {"id": "b", "old": 2.0, "new": 3.0},
            {"id": "c", "old": 3.0, "new": 2.0},
            {"id": "d", "old": 4.0, "new": 8.0},
        ]

        summary = summarize_pairs(pairs)

        self.assertEqual(summary["count"], 4)
        self.assertEqual(summary["exact_matches"], 1)
        self.assertEqual(summary["higher_in_new"], 2)
        self.assertEqual(summary["lower_in_new"], 1)
        self.assertAlmostEqual(summary["bias"], 1.0)
        self.assertAlmostEqual(summary["mae"], 1.5)
        self.assertEqual(summary["max_abs_diff_id"], "d")
        self.assertIsNotNone(summary["pearson_r"])

    def test_natural_key_sorts_case_insensitively_with_numbers(self):
        names = ["Water 10", "terrain", "water 2", "Croppingintensity", "water 1"]

        self.assertEqual(
            sorted(names, key=_natural_key),
            ["Croppingintensity", "terrain", "water 1", "water 2", "Water 10"],
        )

    def test_summary_handles_constant_values(self):
        summary = summarize_pairs([{"id": "a", "old": 5.0, "new": 5.0}])

        self.assertIsNone(summary["pearson_r"])
        self.assertIsNone(summary["slope"])
        self.assertEqual(summary["exact_matches"], 1)

    def test_summary_counts_values_within_each_tolerance(self):
        pairs = [
            {"id": "a", "old": 100.0, "new": 100.0},
            {"id": "b", "old": 100.0, "new": 103.0},
            {"id": "c", "old": 100.0, "new": 120.0},
            {"id": "d", "old": 0.0, "new": 1.0},
        ]

        within = summarize_pairs(pairs)["within_tolerance"]

        self.assertEqual(within["0"], 1)
        self.assertEqual(within["5"], 2)
        self.assertEqual(within["25"], 3)
        self.assertEqual(within["50"], 3)  # old == 0 never counts as within

    def test_workbook_analysis_covers_all_sheets_and_opens_worst_metric(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            old_root = Path(temp_dir) / "old"
            new_root = Path(temp_dir) / "new"
            # Created out of alphabetical order to check the result is sorted.
            sheets = {
                "terrain": (
                    ["UID", "slope"],
                    [["mws-1", 1.5]],
                    [["mws-1", 1.5]],
                ),
                "rainfall": (
                    ["UID", "agrees", "differs"],
                    [["mws-1", 10, 10], ["mws-2", 20, 20], ["mws-3", 30, 30]],
                    [["mws-1", 10, 15], ["mws-2", 20, 5], ["mws-3", 30, 30], ["mws-4", 1, 1]],
                ),
            }
            for root, rows_index in ((old_root, 1), (new_root, 2)):
                location_dir = root / "JHARKHAND" / "DUMKA"
                location_dir.mkdir(parents=True)
                workbook = Workbook()
                workbook.remove(workbook.active)
                for name, spec in sheets.items():
                    sheet = workbook.create_sheet(name)
                    sheet.append(spec[0])
                    for row in spec[rows_index]:
                        sheet.append(row)
                workbook.save(location_dir / "dumka_masalia.xlsx")
                workbook.close()

            with patch(
                "layer_comparison.services.excel_comparison_service._excel_roots",
                return_value=(old_root, new_root),
            ):
                analysis = get_workbook_analysis("Jharkhand", "Dumka", "Masalia")

        self.assertEqual([sheet["name"] for sheet in analysis["sheets"]], ["rainfall", "terrain"])
        rainfall = analysis["sheets"][0]
        self.assertEqual([metric["name"] for metric in rainfall["metrics"]], ["differs", "agrees"])
        self.assertEqual(rainfall["default_metric"], "differs")
        self.assertEqual(rainfall["detail"]["points"][0]["id"], "mws-2")  # largest |diff| first
        self.assertEqual(rainfall["unmatched"]["new_only"], ["mws-4"])
        self.assertIn("5", analysis["tolerance_levels"])
        self.assertIsNone(rainfall["multi_property"])  # only 2 metrics -> no multi view

    def test_temporal_multi_property_groups_periods_and_switches_slices(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            old_root = Path(temp_dir) / "old"
            new_root = Path(temp_dir) / "new"
            headers = [
                "UID",
                "area_in_ha",
                "tree_forest_area_in_ha_2022",
                "cropland_area_in_ha_2022",
                "built-up_area_in_ha_2022",
                "tree_forest_area_in_ha_2023",
                "cropland_area_in_ha_2023",
                "built-up_area_in_ha_2023",
            ]
            old_rows = [
                ["mws-1", 100.0, 60.0, 30.0, 10.0, 60.0, 30.0, 10.0],
                ["mws-2", 200.0, 120.0, 60.0, 20.0, 120.0, 60.0, 20.0],
            ]
            # 2022 matches closely; 2023 has a large forest -> cropland swap on mws-2.
            new_rows = [
                ["mws-1", 100.0, 60.0, 30.0, 10.0, 55.0, 35.0, 10.0],
                ["mws-2", 200.0, 120.0, 60.0, 20.0, 80.0, 100.0, 20.0],
            ]
            for root, rows in ((old_root, old_rows), (new_root, new_rows)):
                location_dir = root / "MAHARASHTRA" / "NASHIK"
                location_dir.mkdir(parents=True)
                wb = Workbook()
                wb.active.title = "lulc_vector"
                wb.active.append(headers)
                for row in rows:
                    wb.active.append(row)
                wb.save(location_dir / "nashik_kalwan.xlsx")
                wb.close()

            with patch(
                "layer_comparison.services.excel_comparison_service._excel_roots",
                return_value=(old_root, new_root),
            ):
                analysis = get_workbook_analysis("Maharashtra", "Nashik", "Kalwan")
                slice_2022 = get_multi_metric_slice(
                    "Maharashtra", "Nashik", "Kalwan", "lulc_vector", period="2022"
                )

        multi = analysis["sheets"][0]["multi_property"]
        self.assertIsNotNone(multi)
        self.assertEqual(multi["mode"], "temporal")
        self.assertEqual(multi["periods"], ["2022", "2023"])
        self.assertEqual(multi["default_period"], "2023")
        # area_in_ha is excluded as metadata; the 3 LULC classes are kept.
        self.assertEqual(len(multi["grid"]), 3)
        default_slice = multi["slice"]
        self.assertTrue(default_slice["compositional"])
        self.assertEqual(default_slice["unit"], "ha")
        self.assertEqual(default_slice["rows"][0]["id"], "mws-2")
        self.assertAlmostEqual(default_slice["rows"][0]["l1"], 80.0)
        # On-demand period fetch preserves the exact same class ordering.
        self.assertEqual(slice_2022["period"], "2022")
        self.assertEqual(
            [p["label"] for p in slice_2022["properties"]],
            [p["label"] for p in default_slice["properties"]],
        )
        self.assertAlmostEqual(slice_2022["rows"][0]["l1"], 0.0)

    def test_flat_mixed_unit_multi_property_disables_compositional_bars(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            old_root = Path(temp_dir) / "old"
            new_root = Path(temp_dir) / "new"
            headers = ["UID", "runoff_in_mm", "storage_in_bcm", "dry_spell_in_weeks"]
            old_rows = [["mws-1", 400.0, 0.25, 4.0], ["mws-2", 500.0, 0.30, 3.0]]
            new_rows = [["mws-1", 450.0, 0.20, 6.0], ["mws-2", 505.0, 0.30, 3.0]]
            for root, rows in ((old_root, old_rows), (new_root, new_rows)):
                location_dir = root / "JHARKHAND" / "DUMKA"
                location_dir.mkdir(parents=True)
                wb = Workbook()
                wb.active.title = "hydrology"
                wb.active.append(headers)
                for row in rows:
                    wb.active.append(row)
                wb.save(location_dir / "dumka_masalia.xlsx")
                wb.close()

            with patch(
                "layer_comparison.services.excel_comparison_service._excel_roots",
                return_value=(old_root, new_root),
            ):
                analysis = get_workbook_analysis("Jharkhand", "Dumka", "Masalia")

        multi = analysis["sheets"][0]["multi_property"]
        self.assertEqual(multi["mode"], "flat")
        self.assertFalse(multi["slice"]["same_unit"])
        self.assertFalse(multi["slice"]["compositional"])
        self.assertEqual(multi["slice"]["rows"][0]["id"], "mws-1")


if __name__ == "__main__":
    unittest.main()
