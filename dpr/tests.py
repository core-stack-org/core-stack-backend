from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from dpr.plans_export import ALL_SECTIONS, export_plans_csv, parse_sections

_ORG_ID = "00000000-0000-0000-0000-000000000001"


class ParseSectionsTests(SimpleTestCase):
    def test_default_is_all_sections(self):
        self.assertEqual(parse_sections(None), ALL_SECTIONS)
        self.assertEqual(parse_sections(""), ALL_SECTIONS)

    def test_normalises_case_and_order(self):
        self.assertEqual(parse_sections("gfe"), "EFG")
        self.assertEqual(parse_sections("eeF"), "EF")

    def test_rejects_invalid_sections(self):
        for bad in ("A", "BX", "1", "E,F"):
            with self.assertRaises(ValueError):
                parse_sections(bad)


class _FakeWrapperCursor:
    def __init__(self, raw_cursor):
        self.cursor = raw_cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        pass

    def fetchone(self):
        return ('"Plan ID", "Section"',)


class ExportPlansCsvCursorTests(SimpleTestCase):
    def _run_with_raw_cursor(self, raw_cursor):
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = _FakeWrapperCursor(raw_cursor)

        @contextmanager
        def noop_atomic():
            yield

        with patch("dpr.plans_export.transaction.atomic", noop_atomic), patch(
            "dpr.plans_export.connection", fake_conn
        ):
            return export_plans_csv(_ORG_ID, "BC")

    def test_psycopg2_copy_expert(self):
        seen = {}

        class Psycopg2Cursor:
            def copy_expert(self, sql, buf):
                seen["sql"] = sql
                buf.write(b"a,b\n1,2\n")

        out = self._run_with_raw_cursor(Psycopg2Cursor())
        self.assertEqual(out, b"a,b\n1,2\n")
        self.assertIn(
            "TO STDOUT WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')", seen["sql"]
        )

    def test_psycopg3_copy_context_manager(self):
        seen = {}

        class Psycopg3Cursor:
            @contextmanager
            def copy(self, sql):
                seen["sql"] = sql
                yield iter([memoryview(b"a,b\n"), memoryview(b"1,2\n")])

        out = self._run_with_raw_cursor(Psycopg3Cursor())
        self.assertEqual(out, b"a,b\n1,2\n")
        self.assertIn(
            "TO STDOUT WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')", seen["sql"]
        )


class MwsReportNanFilteringTests(SimpleTestCase):
    def test_lcw_factory_mining_filter_placeholder_nan_rows(self):
        import pandas as pd

        from dpr import gen_mws_report

        sheets = {
            "lcw_conflict": pd.DataFrame(
                {
                    "UID": ["u_empty", "u_valid"],
                    "title_of_conflict": [None, "Land Dispute"],
                    "link_to_conflict": [None, None],
                }
            ),
            "factory_csr": pd.DataFrame(
                {
                    "UID": ["u_empty", "u_valid"],
                    "Company_Name": [None, "Acme Plant"],
                    "ADDRESS": [None, "Main Rd Email : a@b.com"],
                    "LOCATION T": [None, None],
                }
            ),
            "mining": pd.DataFrame(
                {
                    "UID": ["u_empty", "u_valid", "u_unknown"],
                    "division": [None, "Iron Ore Div", "unknown"],
                    "sector_moefcc": [None, None, "Mining"],
                    "village": [None, "Rampur", "Rampur"],
                }
            ),
        }

        with patch.object(
            gen_mws_report, "read_excel_sheet", side_effect=lambda _p, s: sheets[s]
        ):
            self.assertEqual(
                gen_mws_report.get_land_conflict_industrial_data("s", "d", "b", "u_empty"),
                [],
            )
            self.assertEqual(
                gen_mws_report.get_land_conflict_industrial_data("s", "d", "b", "u_valid"),
                [{"title": "Land Dispute", "link": ""}],
            )

            self.assertEqual(
                gen_mws_report.get_factory_data("s", "d", "b", "u_empty"), []
            )
            self.assertEqual(
                gen_mws_report.get_factory_data("s", "d", "b", "u_valid"),
                [{"name": "Acme Plant", "address": "Main Rd", "type": ""}],
            )

            self.assertEqual(
                gen_mws_report.get_mining_data("s", "d", "b", "u_empty"), []
            )
            self.assertEqual(
                gen_mws_report.get_mining_data("s", "d", "b", "u_unknown"), []
            )
            self.assertEqual(
                gen_mws_report.get_mining_data("s", "d", "b", "u_valid"),
                [{"division": "Iron Ore Div", "sector": "", "village": "Rampur"}],
            )
