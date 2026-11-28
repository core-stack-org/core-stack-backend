import json
import os
from pathlib import Path
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd
from django.test import SimpleTestCase

from dpr import gen_mws_report, gen_tehsil_report


def _write_workbook(base_dir: Path, state: str, district: str, block: str, sheets: dict):
    """Write a multi-sheet .xlsx workbook at the exact path expected by dpr report helpers."""
    out_dir = base_dir / state.upper() / district.upper()
    out_dir.mkdir(parents=True, exist_ok=True)
    xlsx_path = out_dir / f"{district.lower()}_{block.lower()}.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    return xlsx_path


def _sample_mws_sheets():
    """Minimal stats Excel sheets matching stats_generator column conventions."""
    cropping_annual = pd.DataFrame(
        {
            "UID": ["12_1001", "12_1002"],
            "sum_area_in_ha": [500.0, 800.0],
            "single_kharif_cropped_area_in_ha_2023-2024": [120.0, 200.0],
            "single_non_kharif_cropped_area_in_ha_2023-2024": [40.0, 60.0],
            "doubly_cropped_area_in_ha_2023-2024": [80.0, 100.0],
            "triply_cropped_area_in_ha_2023-2024": [10.0, 20.0],
            "single_kharif_cropped_area_in_ha_2024-2025": [150.0, 220.0],
            "single_non_kharif_cropped_area_in_ha_2024-2025": [50.0, 70.0],
            "doubly_cropped_area_in_ha_2024-2025": [90.0, 110.0],
            "triply_cropped_area_in_ha_2024-2025": [15.0, 25.0],
        }
    )

    swb_annual = pd.DataFrame(
        {
            "UID": ["12_1001", "12_1002"],
            "kharif_area_in_ha_2024-2025": [12.5, 20.0],
            "rabi_area_in_ha_2024-2025": [8.25, 15.0],
            "zaid_area_in_ha_2024-2025": [2.5, 4.0],
        }
    )

    hydro_annual = pd.DataFrame(
        {
            "UID": ["12_1001", "12_1002"],
            "Precipitation_in_mm_2020-2021": [1050.0, 980.0],
            "ET_in_mm_2020-2021": [760.0, 810.0],
            "RunOff_in_mm_2020-2021": [200.0, 180.0],
            "DeltaG_in_mm_2020-2021": [90.0, -10.0],
            "G_in_mm_2020-2021": [90.0, -10.0],
            "Precipitation_in_mm_2021-2022": [1100.0, 950.0],
            "ET_in_mm_2021-2022": [780.0, 820.0],
            "RunOff_in_mm_2021-2022": [210.0, 190.0],
            "DeltaG_in_mm_2021-2022": [110.0, -60.0],
            "G_in_mm_2021-2022": [110.0, -60.0],
            "Precipitation_in_mm_2022-2023": [850.0, 700.0],
            "ET_in_mm_2022-2023": [740.0, 790.0],
            "RunOff_in_mm_2022-2023": [120.0, 90.0],
            "DeltaG_in_mm_2022-2023": [-10.0, -180.0],
            "G_in_mm_2022-2023": [100.0, -240.0],
            "Precipitation_in_mm_2023-2024": [1200.0, 800.0],
            "ET_in_mm_2023-2024": [810.0, 850.0],
            "RunOff_in_mm_2023-2024": [260.0, 150.0],
            "DeltaG_in_mm_2023-2024": [130.0, -200.0],
            "G_in_mm_2023-2024": [230.0, -440.0],
            "Precipitation_in_mm_2024-2025": [1250.0, 750.0],
            "ET_in_mm_2024-2025": [820.0, 860.0],
            "RunOff_in_mm_2024-2025": [280.0, 140.0],
            "DeltaG_in_mm_2024-2025": [150.0, -250.0],
            "G_in_mm_2024-2025": [380.0, -690.0],
        }
    )

    # Includes trailing unpopulated 2025-2026 all-zero columns (matching current Excel output)
    hydro_seasonal = pd.DataFrame(
        {
            "UID": ["12_1001", "12_1002"],
            "precipitation_kharif_in_mm_2024-2025": [581.06, 490.0],
            "precipitation_rabi_in_mm_2024-2025": [12.35, 10.0],
            "precipitation_zaid_in_mm_2024-2025": [337.05, 250.0],
            "precipitation_kharif_in_mm_2025-2026": [0.0, 0.0],
            "precipitation_rabi_in_mm_2025-2026": [0.0, 0.0],
            "precipitation_zaid_in_mm_2025-2026": [0.0, 0.0],
            "et_kharif_in_mm_2024-2025": [524.56, 510.0],
            "et_rabi_in_mm_2024-2025": [58.19, 60.0],
            "et_zaid_in_mm_2024-2025": [226.22, 210.0],
            "et_kharif_in_mm_2025-2026": [0.0, 0.0],
            "et_rabi_in_mm_2025-2026": [0.0, 0.0],
            "et_zaid_in_mm_2025-2026": [0.0, 0.0],
            "runoff_kharif_in_mm_2024-2025": [92.16, 80.0],
            "runoff_rabi_in_mm_2024-2025": [0.0, 1.5],
            "runoff_zaid_in_mm_2024-2025": [33.71, 20.0],
            "runoff_kharif_in_mm_2025-2026": [0.0, 0.0],
            "runoff_rabi_in_mm_2025-2026": [0.0, 0.0],
            "runoff_zaid_in_mm_2025-2026": [0.0, 0.0],
            "delta g_kharif_in_mm_2021-2022": [80.0, -20.0],
            "delta g_rabi_in_mm_2021-2022": [20.0, -20.0],
            "delta g_zaid_in_mm_2021-2022": [10.0, -20.0],
            "delta g_kharif_in_mm_2022-2023": [10.0, -80.0],
            "delta g_rabi_in_mm_2022-2023": [-10.0, -50.0],
            "delta g_zaid_in_mm_2022-2023": [-10.0, -50.0],
            "delta g_kharif_in_mm_2023-2024": [90.0, -100.0],
            "delta g_rabi_in_mm_2023-2024": [20.0, -50.0],
            "delta g_zaid_in_mm_2023-2024": [20.0, -50.0],
            "delta g_kharif_in_mm_2024-2025": [100.0, -120.0],
            "delta g_rabi_in_mm_2024-2025": [25.0, -65.0],
            "delta g_zaid_in_mm_2024-2025": [25.0, -65.0],
        }
    )

    drought_kharif = pd.DataFrame(
        {
            "UID": ["12_1001", "12_1002"],
            "Moderate_2021": [1, 3],
            "Severe_2021": [0, 2],
            "monsoon_onset_2021": ["2021-06-15", "2021-06-20"],
            "Moderate_2022": [3, 4],
            "Severe_2022": [2, 3],
            "monsoon_onset_2022": ["2022-06-25", "2022-07-01"],
            "Moderate_2023": [0, 2],
            "Severe_2023": [0, 1],
            "monsoon_onset_2023": ["2023-06-18", "2023-06-22"],
            "Moderate_2024": [1, 3],
            "Severe_2024": [0, 3],
            "monsoon_onset_2024": ["2024-06-14", "2024-06-28"],
        }
    )

    soge_vector = pd.DataFrame(
        {
            "UID": ["12_1001", "12_1002"],
            "class_name": ["Safe", "Over_Exploited"],
        }
    )

    return {
        "croppingIntensity_annual": cropping_annual,
        "surfaceWaterBodies_annual": swb_annual,
        "hydrological_annual": hydro_annual,
        "hydrological_seasonal": hydro_seasonal,
        "croppingDrought_kharif": drought_kharif,
        "soge_vector": soge_vector,
    }


class MwsReportTests(SimpleTestCase):
    def test_get_latest_col_val_skips_trailing_all_zero_year(self):
        df = pd.DataFrame(
            {
                "UID": ["u1", "u2"],
                "et_kharif_in_mm_2024-2025": [524.56, 480.0],
                "et_kharif_in_mm_2025-2026": [0.0, 0.0],
            }
        )
        self.assertAlmostEqual(
            gen_mws_report.get_latest_col_val(df, "et_kharif_in_mm_", "u1"), 524.56
        )

    def test_get_latest_col_val_preserves_genuine_zero_in_populated_year(self):
        df = pd.DataFrame(
            {
                "UID": ["u1", "u2"],
                "runoff_rabi_in_mm_2023-2024": [14.2, 22.0],
                "runoff_rabi_in_mm_2024-2025": [0.0, 3.5],
                "runoff_rabi_in_mm_2025-2026": [0.0, 0.0],
            }
        )
        # u1 genuinely had 0.0 in 2024-2025 while u2 had 3.5; should NOT fall back to 2023-2024.
        self.assertEqual(
            gen_mws_report.get_latest_col_val(df, "runoff_rabi_in_mm_", "u1"), 0.0
        )
        self.assertAlmostEqual(
            gen_mws_report.get_latest_col_val(df, "runoff_rabi_in_mm_", "u2"), 3.5
        )

    def test_get_latest_col_val_picks_newest_year_once_populated(self):
        df = pd.DataFrame(
            {
                "UID": ["u1", "u2"],
                "precipitation_kharif_in_mm_2024-2025": [581.0, 490.0],
                "precipitation_kharif_in_mm_2025-2026": [612.5, 530.0],
            }
        )
        self.assertAlmostEqual(
            gen_mws_report.get_latest_col_val(df, "precipitation_kharif_in_mm_", "u1"),
            612.5,
        )

    def test_get_cropping_water_hydro_data_returns_nonzero_seasonal_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            _write_workbook(tmp_dir, "bihar", "banka", "banka", _sample_mws_sheets())
            with patch.object(gen_mws_report, "DATA_DIR_TEMP", f"{tmp_dir}/"):
                data = gen_mws_report.get_cropping_water_hydro_data(
                    "bihar", "banka", "banka", "12_1001"
                )
            self.assertEqual(data["crop_kharif"], 255.0)
            self.assertEqual(data["crop_rabi"], 155.0)
            self.assertEqual(data["crop_zaid"], 15.0)
            self.assertEqual(data["water_kharif"], 12.5)
            self.assertEqual(data["rf_kharif"], 581.06)
            self.assertEqual(data["rf_rabi"], 12.35)
            self.assertEqual(data["rf_zaid"], 337.05)
            self.assertEqual(data["et_kharif"], 524.56)
            self.assertEqual(data["et_rabi"], 58.19)
            self.assertEqual(data["et_zaid"], 226.22)
            self.assertEqual(data["ro_kharif"], 92.16)
            self.assertEqual(data["ro_rabi"], 0.0)
            self.assertEqual(data["ro_zaid"], 33.71)

    def test_get_water_balance_data_returns_annual_series(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            _write_workbook(tmp_dir, "bihar", "banka", "banka", _sample_mws_sheets())
            with patch.object(gen_mws_report, "DATA_DIR_TEMP", f"{tmp_dir}/"):
                (
                    trend,
                    wb_desc,
                    good_rf,
                    bad_rf,
                    mean_wb,
                    precip,
                    runoff,
                    et,
                    dg,
                    years,
                ) = gen_mws_report.get_water_balance_data(
                    "bihar", "banka", "banka", "12_1001"
                )
            self.assertEqual(precip, [1050.0, 1100.0, 850.0, 1200.0, 1250.0])
            self.assertEqual(runoff, [200.0, 210.0, 120.0, 260.0, 280.0])
            self.assertEqual(et, [760.0, 780.0, 740.0, 810.0, 820.0])
            self.assertEqual(dg, [90.0, 110.0, -10.0, 130.0, 150.0])
            self.assertTrue(len(years) == 5)
            self.assertGreater(mean_wb, 0)
            self.assertIn("water balance", wb_desc.lower())
            self.assertTrue(good_rf)
            self.assertTrue(bad_rf)

    def test_get_fortnightly_water_balance_data_parses_wfs_payload(self):
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.json.return_value = {
            "features": [
                {
                    "properties": {
                        "uid": "12_1001",
                        "2024-07-01": json.dumps(
                            {"Precipitation": 210.5, "ET": 80.25, "RunOff": 27.67}
                        ),
                        "2024-07-15": json.dumps(
                            {"Precipitation": 122.3, "ET": 76.1, "RunOff": 14.5}
                        ),
                        "2024-07-29": "not-valid-json",
                    }
                }
            ]
        }
        with patch.object(gen_mws_report.requests, "get", return_value=fake_resp):
            labels, precip, et, runoff = (
                gen_mws_report.get_fortnightly_water_balance_data(
                    "bihar", "banka", "banka", "12_1001"
                )
            )
        self.assertEqual(labels, ["2024-07-01", "2024-07-15"])
        self.assertEqual(precip, [210.5, 122.3])
        self.assertEqual(et, [80.25, 76.1])
        self.assertEqual(runoff, [27.67, 14.5])


class TehsilReportTests(SimpleTestCase):
    def test_get_agri_water_stress_data_identifies_stressed_mws(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            _write_workbook(tmp_dir, "bihar", "banka", "banka", _sample_mws_sheets())
            with patch.object(gen_tehsil_report, "DATA_DIR_TEMP", f"{tmp_dir}/"):
                res = gen_tehsil_report.get_agri_water_stress_data(
                    "bihar", "banka", "banka"
                )
            # 12_1001 is Safe + increasing G -> False (0.0)
            # 12_1002 is Over_Exploited + decreasing G -> True (1.0), area 800.0 ha
            self.assertFalse(res["mws_pattern"]["12_1001"])
            self.assertTrue(res["mws_pattern"]["12_1002"])
            self.assertEqual(res["mws_intensity"]["12_1001"], 0.0)
            self.assertEqual(res["mws_intensity"]["12_1002"], 1.0)
            self.assertEqual(res["total_area"], 800.0)

    def test_read_sheet_cache_invalidates_when_workbook_mtime_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            xlsx_path = _write_workbook(
                tmp_dir,
                "bihar",
                "banka",
                "banka",
                {"soge_vector": pd.DataFrame({"UID": ["u1"], "class_name": ["Safe"]})},
            )
            df1 = gen_tehsil_report.read_sheet(str(xlsx_path), "soge_vector")
            self.assertEqual(df1.iloc[0]["class_name"], "Safe")

            # Rewrite workbook and bump mtime so cache refreshes
            _write_workbook(
                tmp_dir,
                "bihar",
                "banka",
                "banka",
                {
                    "soge_vector": pd.DataFrame(
                        {"UID": ["u1"], "class_name": ["Critical"]}
                    )
                },
            )
            new_mtime = os.path.getmtime(xlsx_path) + 5
            os.utime(xlsx_path, (new_mtime, new_mtime))

            df2 = gen_tehsil_report.read_sheet(str(xlsx_path), "soge_vector")
            self.assertEqual(df2.iloc[0]["class_name"], "Critical")
