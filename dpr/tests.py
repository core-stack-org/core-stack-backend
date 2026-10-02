from django.test import SimpleTestCase

from dpr.plans_export import ALL_SECTIONS, parse_sections


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
