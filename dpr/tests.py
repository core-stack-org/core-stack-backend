import copy
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.template.loader import render_to_string
from django.test import SimpleTestCase, override_settings
from django.urls import reverse

from .ceew_tehsil_report import (
    _format_one_decimal,
    _trend_class,
    get_ceew_tehsil_context,
)


def _annual(values):
    return {"annual": {"yearly_series": {"scenario": values}}}


def _years(start, end, value):
    return {str(year): float(value(year)) for year in range(start, end + 1)}


def _combined_series(observed_value, projected_value):
    values = _years(1981, 2024, observed_value)
    values.update(_years(2025, 2099, projected_value))
    return values


def _test_profile():
    observed_drought = _years(1981, 2024, lambda year: 40 if year <= 2016 else 50)
    projected_drought = _years(
        2025,
        2099,
        lambda year: 100 - (year - 2025) + (((year - 2025) % 5) - 2) * 3,
    )
    observed_dry_spell = _years(1981, 2024, lambda year: 70)
    projected_dry_spell = _years(
        2025,
        2099,
        lambda year: 80 - 0.5 * (year - 2025) + (((year - 2025) % 4) - 1.5),
    )

    def temperature(base, annual_change):
        return _combined_series(
            lambda year: base + 0.01 * (year - 1981),
            lambda year: base + 0.4 + annual_change * (year - 2025),
        )

    def modelled_temperature(base, annual_change):
        values = _years(2017, 2024, lambda year: base + 0.3)
        values.update(
            _years(
                2025,
                2099,
                lambda year: base + 0.4 + annual_change * (year - 2025),
            )
        )
        return values

    profile = {
        "location": {
            "state_name": "Example State",
            "district_name": "Example District",
        },
        "climate": {
            "rcpssp": {
                "imd": {
                    "precipitation": {
                        "derived_metrics": {
                            "6_month_drought_severity": _annual(observed_drought),
                            "longest_dry_spell": _annual(observed_dry_spell),
                            "unusually_heavy_rainy_days": _annual(
                                _years(
                                    1981,
                                    2024,
                                    lambda year: 20 if year <= 2016 else 25,
                                )
                            ),
                        },
                        "time_series_metrics": {
                            "total_rainfall": _annual(
                                _years(
                                    1981,
                                    2024,
                                    lambda year: 1000 if year <= 2016 else 1100,
                                )
                            )
                        },
                    },
                    "temperature": {
                        "maximum_temperature": _annual(temperature(31.0, 0.03)),
                        "average_temperature": _annual(temperature(25.0, 0.035)),
                        "minimum_temperature": _annual(temperature(19.0, 0.04)),
                    },
                    "hot_weather": {
                        "unusually_hot_days": _annual(
                            _years(1981, 2024, lambda year: 10)
                        ),
                        "unusually_warm_nights": _annual(
                            _years(1981, 2024, lambda year: 12)
                        ),
                    },
                    "cold_weather": {
                        "unusually_cold_days": _annual(
                            _years(1981, 2024, lambda year: 4)
                        ),
                        "unusually_cold_nights": _annual(
                            _years(1981, 2024, lambda year: 3)
                        ),
                    },
                },
                "RCP85": {
                    "precipitation": {
                        "derived_metrics": {
                            "6_month_drought_severity": _annual(projected_drought),
                            "longest_dry_spell": _annual(projected_dry_spell),
                            "unusually_heavy_rainy_days": _annual(
                                _years(
                                    2025, 2099, lambda year: 30 + 0.2 * (year - 2025)
                                )
                            ),
                        },
                        "time_series_metrics": {
                            "total_rainfall": _annual(
                                _years(
                                    2025, 2099, lambda year: 1200 + 5 * (year - 2025)
                                )
                            )
                        },
                    },
                    "temperature": {
                        "maximum_temperature": _annual(
                            modelled_temperature(31.0, 0.03)
                        ),
                        "average_temperature": _annual(
                            modelled_temperature(25.0, 0.035)
                        ),
                        "minimum_temperature": _annual(
                            modelled_temperature(19.0, 0.04)
                        ),
                    },
                    "hot_weather": {
                        "unusually_hot_days": _annual(
                            _years(2025, 2099, lambda year: 10 + 0.4 * (year - 2025))
                        ),
                        "unusually_warm_nights": _annual(
                            _years(2025, 2099, lambda year: 12 + 0.6 * (year - 2025))
                        ),
                    },
                    "cold_weather": {
                        "unusually_cold_days": _annual(
                            _years(2025, 2099, lambda year: 0)
                        ),
                        "unusually_cold_nights": _annual(
                            _years(2025, 2099, lambda year: 0)
                        ),
                    },
                },
            }
        },
    }
    profile["climate"]["rcpssp"]["RCP45"] = copy.deepcopy(
        profile["climate"]["rcpssp"]["RCP85"]
    )
    return profile


class CEEWTehsilContextTests(SimpleTestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        district_directory = (
            Path(self.temporary_directory.name) / "example_state" / "example_district"
        )
        district_directory.mkdir(parents=True)
        self.profile_path = (
            district_directory / "example_state_example_district_profile.json"
        )
        self.profile_path.write_text(json.dumps(_test_profile()), encoding="utf-8")
        self.settings_override = override_settings(
            CEEW_DISTRICT_DATA_DIR=self.temporary_directory.name
        )
        self.settings_override.enable()

    def tearDown(self):
        self.settings_override.disable()
        self.temporary_directory.cleanup()

    def test_builds_approved_periods_and_dynamic_text(self):
        context = get_ceew_tehsil_context("Example State", "Example District")

        self.assertTrue(context["available"])
        self.assertEqual(context["drought"]["baseline_mean"], "40.0")
        self.assertEqual(context["drought"]["recent_mean"], "50.0")
        self.assertIn(
            "continuously bettering scenario", context["drought"]["projected_text"]
        )
        self.assertEqual(
            set(context["charts"]["rainfall"]["observed"]),
            {str(year) for year in range(1981, 2025)},
        )
        self.assertEqual(
            set(context["charts"]["rainfall"]["rcp45"]),
            {str(year) for year in range(2025, 2100)},
        )
        self.assertEqual(
            set(context["charts"]["rainfall"]["rcp85"]),
            {str(year) for year in range(2025, 2100)},
        )
        self.assertEqual(
            context["temperature"]["projected_phrase"],
            "all three annual temperature series show clear increasing trends",
        )
        self.assertEqual(
            context["unusual_temperature"]["overall_phrase"], "hotter conditions"
        )
        self.assertEqual(
            context["unusual_temperature"]["metrics"]["cold_day"]["projected_phrase"],
            "almost no",
        )

    def test_workbook_trend_classes_and_rounding_are_reproduced(self):
        increasing = _years(
            2025,
            2099,
            lambda year: (year - 2025) + (((year - 2025) % 5) - 2) * 3,
        )
        decreasing = {year: 100 - value for year, value in increasing.items()}
        unchanged = _years(2025, 2099, lambda year: 7)

        self.assertEqual(_trend_class(increasing, 2025, 2099), "increasing")
        self.assertEqual(_trend_class(decreasing, 2025, 2099), "decreasing")
        self.assertEqual(_trend_class(unchanged, 2025, 2099), "unchanged")
        self.assertEqual(_trend_class({}, 2025, 2099), "insufficient_data")
        self.assertEqual(_format_one_decimal(56.25), "56.3")

    def test_missing_profile_omits_climate_context(self):
        context = get_ceew_tehsil_context("Example State", "Missing District")
        self.assertEqual(context, {"available": False})

    def test_people_facing_partials_keep_approved_order_without_mockup_marking(self):
        climate = get_ceew_tehsil_context("Example State", "Example District")
        climate_html = render_to_string(
            "dpr/ceew_climate_context.html",
            {"ceew_climate": climate, "block": "Example Tehsil"},
        )
        drought_html = render_to_string(
            "dpr/ceew_drought_context.html",
            {"ceew_climate": climate, "block": "Example Tehsil"},
        )

        self.assertLess(
            climate_html.index("ceewTemperatureRangeChart"),
            climate_html.index("ceewHotDaysChart"),
        )
        self.assertLess(
            climate_html.index("ceewAnnualRainfallChart"),
            climate_html.index("ceewUnusualRainfallChart"),
        )
        self.assertNotIn("MockUp", climate_html + drought_html)
        self.assertNotIn("color:#ff0000", climate_html + drought_html)
        self.assertIn(
            "Example Tehsil lies in <em>Example District</em> district",
            climate_html,
        )
        self.assertIn(
            "Example Tehsil lies in <em>Example District</em> district",
            drought_html,
        )
        self.assertIn("4.5 W/m² of radiative forcing", climate_html)
        self.assertIn("8.5 W/m² by 2100", climate_html)
        self.assertIn("RCP4.5 and RCP8.5 modelled", climate_html + drought_html)
        self.assertIn("actual future observations may differ", climate_html)

    @patch("dpr.api.get_tehsil_data", return_value={})
    @patch(
        "dpr.api.get_pattern_intensity",
        return_value={
            "intensity": {},
            "mws_active_patterns": {},
            "pattern_display_mapping": {},
            "active_patterns": [],
            "village_active_patterns": [],
        },
    )
    @patch("dpr.api.get_agri_water_stress_data", return_value={})
    @patch("dpr.api.get_agri_water_drought_data", return_value=({}, {}))
    @patch("dpr.api.get_agri_water_irrigation_data", return_value=({}, {}))
    @patch("dpr.api.get_agri_low_yield_data", return_value=({}, {}))
    @patch("dpr.api.get_forest_degrad_data", return_value=({}, {}))
    @patch("dpr.api.get_mining_presence_data", return_value=({}, {}))
    @patch("dpr.api.get_socio_economic_caste_data", return_value=({}, {}))
    @patch("dpr.api.get_socio_economic_nrega_data", return_value=({}, {}))
    @patch("dpr.api.get_fishery_water_potential_data", return_value=({}, {}))
    @patch("dpr.api.get_agroforestry_transition_data", return_value=({}, {}))
    def test_django_tehsil_report_route_includes_district_climate(self, *mocks):
        response = self.client.get(
            reverse("generate_tehsil_report"),
            {
                "state": "Example State",
                "district": "Example District",
                "block": "Example Tehsil",
            },
        )

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("District Level Drought Context", html)
        self.assertIn("District Climate Context", html)
        self.assertIn("ceewTemperatureRangeChart", html)
        self.assertIn("ceew-climate-data", html)
        self.assertIn('"rcp45"', html)
        self.assertLess(
            html.index("District Level Drought Context"),
            html.index("High Irrigation Risk"),
        )
        self.assertLess(html.index("Fishery"), html.index("District Climate Context"))
