"""Tests for GEE asset-name resolution (no Django, no Earth Engine needed)."""

import re
import sys
import types
import unittest
from unittest.mock import patch

from stats_generator.gee_layer_source import (
    GEELayerSource,
    NOT_FROM_GEE,
    _to_geoserver_schema,
)
from stats_generator.layer_sources import LayerUnavailable

S = "dumka_masalia"


def _stub_gee_utils():
    # Same rule as utilities.gee_utils.valid_gee_text, without importing Django.
    module = types.ModuleType("utilities.gee_utils")
    module.valid_gee_text = lambda text: re.sub(r"[^a-zA-Z0-9 ,:;_-]", "", text).replace(" ", "_")
    return {"utilities": types.ModuleType("utilities"), "utilities.gee_utils": module}


class GEELayerSourceResolveTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.dict(sys.modules, _stub_gee_utils())
        patcher.start()
        self.addCleanup(patcher.stop)
        self.source = GEELayerSource("Jharkhand", "Dumka", "Masalia")

    def given_assets(self, *names):
        self.source._asset_ids = {f"{self.source.folder}/{name}" for name in names}

    def assertResolves(self, workspace, layer_name, expected, start_year=None, end_year=None):
        self.assertEqual(
            self.source.resolve(workspace, layer_name, start_year, end_year),
            f"{self.source.folder}/{expected}",
        )

    def test_block_folder_uses_requested_project(self):
        self.assertEqual(
            self.source.folder,
            "projects/ee-corestackdev/assets/apps/mws/jharkhand/dumka/masalia",
        )

    def test_hydrology_layers(self):
        self.given_assets(f"deltaG_well_depth_{S}", f"deltaG_fortnight_{S}", f"filtered_mws_{S}_uid")
        self.assertResolves("mws_layers", f"deltaG_well_depth_{S}", f"deltaG_well_depth_{S}")
        self.assertResolves("mws_layers", f"deltaG_fortnight_{S}", f"deltaG_fortnight_{S}")
        self.assertResolves("mws", f"mws_{S}", f"filtered_mws_{S}_uid")

    def test_swb_prefers_enriched_swb4_then_swb3(self):
        self.given_assets(f"swb3_{S}", f"swb4_{S}")
        self.assertResolves("swb", f"surface_waterbodies_{S}", f"swb4_{S}")
        self.given_assets(f"swb3_{S}")
        self.assertResolves("swb", f"surface_waterbodies_{S}", f"swb3_{S}")

    def test_names_that_differ_from_geoserver(self):
        self.given_assets(
            f"{S}_terrain_clusters",
            f"{S}_lulcXslopes_clusters",
            f"{S}_lulcXplains_clusters",
            f"forest_fringe_{S}",
            f"admin_boundary_{S}",
            f"{S}_lcw_conflict",
        )
        self.assertResolves("terrain", f"{S}_cluster", f"{S}_terrain_clusters")
        self.assertResolves("terrain_lulc", f"{S}_lulc_slope", f"{S}_lulcXslopes_clusters")
        self.assertResolves("terrain_lulc", f"{S}_lulc_plain", f"{S}_lulcXplains_clusters")
        self.assertResolves("forest_fringes", f"forest_fringes_{S}", f"forest_fringe_{S}")
        self.assertResolves("panchayat_boundaries", S, f"admin_boundary_{S}")
        self.assertResolves("lcw", f"{S}_lcw_conflict", f"{S}_lcw_conflict")

    def test_year_suffixed_assets_use_exact_years_when_present(self):
        self.given_assets(f"ccd_vector_{S}_2017_2022", f"ccd_vector_{S}_2017_2023")
        self.assertResolves("tree_ccd_vector", "x", f"ccd_vector_{S}_2017_2022", 2017, 2022)

    def test_year_suffixed_assets_fall_back_to_widest_range(self):
        self.given_assets(
            f"ccd_vector_{S}_2017_2022",
            f"ccd_vector_{S}_2017_2023",
            f"ccd_vector_{S}_2019_2023",
            f"ccd_vector_{S}_2017",  # yearly intermediate, must be ignored
        )
        with self.assertLogs("stats_generator.gee_layer_source", level="WARNING"):
            self.assertResolves("tree_ccd_vector", "x", f"ccd_vector_{S}_2017_2023", 2017, 2024)

    def test_change_detection_appends_years_to_geoserver_name(self):
        self.given_assets(f"change_vector_{S}_Afforestation_2017_2023")
        self.assertResolves(
            "change_detection",
            f"change_vector_{S}_Afforestation",
            f"change_vector_{S}_Afforestation_2017_2023",
        )

    def test_soge_only_serves_2020(self):
        self.given_assets(f"soge_vector_{S}")
        self.assertResolves("soge", f"soge_vector_{S}_2020", f"soge_vector_{S}", 2020)
        with self.assertRaisesRegex(LayerUnavailable, "only has 2020"):
            self.source.resolve("soge", f"soge_vector_{S}_2023", 2023, None)

    def test_layers_without_gee_original_are_skipped(self):
        self.given_assets()
        for workspace in NOT_FROM_GEE:
            with self.subTest(workspace=workspace):
                with self.assertRaisesRegex(LayerUnavailable, "not available from GEE"):
                    self.source.resolve(workspace, "x")

    def test_missing_asset_and_unknown_workspace_are_skipped(self):
        self.given_assets()
        with self.assertRaisesRegex(LayerUnavailable, "GEE asset not found"):
            self.source.resolve("crop_intensity", f"{S}_intensity")
        with self.assertRaisesRegex(LayerUnavailable, "no GEE mapping"):
            self.source.resolve("brand_new_workspace", "x")


class GeoServerSchemaTests(unittest.TestCase):
    def test_soge_discharge_name_is_restored(self):
        geojson = {"features": [{"properties": {"nat_discha": 1.5, "uid": "1"}}]}
        properties = _to_geoserver_schema("soge", geojson)["features"][0]["properties"]
        self.assertEqual(properties, {"nat_discharge": 1.5, "uid": "1"})

    def test_ndvi_dates_are_sorted_after_uid(self):
        geojson = {"features": [{"properties": {"2020-01-05": 0.3, "uid": "1", "2019-12-01": 0.2}}]}
        properties = _to_geoserver_schema("ndvi_timeseries", geojson)["features"][0]["properties"]
        self.assertEqual(list(properties), ["uid", "2019-12-01", "2020-01-05"])


if __name__ == "__main__":
    unittest.main()
