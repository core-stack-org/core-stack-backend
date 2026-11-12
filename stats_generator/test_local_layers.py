import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
from django.test import SimpleTestCase
from shapely.geometry import Point, box

from stats_generator import local_layers
from stats_generator.local_layers import (
    LocalLayerSource,
    SheetTemplateStore,
    _integral_floats_to_int,
    find_spec,
    local_paths_for,
)
from stats_generator.utils import get_vector_layer_geoserver


def _write_gpkg(path, gdf):
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(path, driver="GPKG")
    return path


class FindSpecTests(SimpleTestCase):
    def test_matches_config_entry_and_captures_params(self):
        spec, params = find_spec(
            "change_detection", "change_vector_banka_banka_Afforestation", "banka", "banka"
        )
        self.assertEqual(spec.name, "change detection vector")
        self.assertEqual(params, {"param_name": "Afforestation"})

    def test_captures_years_in_layer_name(self):
        spec, params = find_spec(
            "canopy_height_vector", "ch_vector_banka_banka_2017_2023", "banka", "banka"
        )
        self.assertEqual(params, {"start_year": "2017", "end_year": "2023"})

    def test_same_workspace_picks_matching_layer(self):
        slope, _ = find_spec("terrain_lulc", "banka_banka_lulc_slope", "banka", "banka")
        plain, _ = find_spec("terrain_lulc", "banka_banka_lulc_plain", "banka", "banka")
        self.assertEqual(slope.name, "terrain_lulc_slope_cluster")
        self.assertEqual(plain.name, "terrain_lulc_plain_cluster")

    def test_per_year_layer_with_and_without_year(self):
        spec, params = find_spec("soge", "soge_vector_banka_banka", "banka", "banka")
        self.assertTrue(spec.per_year)
        self.assertEqual(params, {})
        _, params = find_spec("soge", "soge_vector_banka_banka_2022", "banka", "banka")
        self.assertEqual(params, {"year": "2022"})

    def test_other_tehsil_or_unknown_workspace_does_not_match(self):
        self.assertIsNone(find_spec("mining", "x_y_mining", "banka", "banka")[0])
        self.assertIsNone(find_spec("no_such_ws", "banka_banka", "banka", "banka")[0])

    def test_gee_only_layers_have_no_local_path(self):
        spec, _ = find_spec(
            "stream_order", "stream_order_banka_banka_vector", "banka", "banka"
        )
        self.assertFalse(spec.is_local)


class LocalPathTests(SimpleTestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.data_dir = Path(tmp.name)
        patcher = patch.object(local_layers, "DATA_DIR", self.data_dir)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_wildcards_unknown_placeholders(self):
        target = (
            self.data_dir / "change_detection/change_detection_vector_local/bihar/banka/banka"
            / "change_vector_banka_banka_Afforestation_2017_2024.gpkg"
        )
        target.parent.mkdir(parents=True)
        target.touch()
        (target.parent / "change_vector_banka_banka_Urbanization_2017_2024.gpkg").touch()
        spec, params = find_spec(
            "change_detection", "change_vector_banka_banka_Afforestation", "banka", "banka"
        )
        self.assertEqual(local_paths_for(spec, params, "bihar", "banka", "banka"), [target])

    def test_hyphenated_names_fall_back_to_underscores(self):
        target = self.data_dir / "layers/mining/bihar/east_champaran/x/east_champaran_x_mining.gpkg"
        target.parent.mkdir(parents=True)
        target.touch()
        spec, params = find_spec(
            "mining", "east-champaran_x_mining", "east-champaran", "x"
        )
        self.assertEqual(
            local_paths_for(spec, params, "bihar", "east-champaran", "x"), [target]
        )


class IntegralFloatTests(SimpleTestCase):
    def test_whole_number_floats_become_ints_and_real_floats_stay(self):
        gdf = gpd.GeoDataFrame(
            {
                "vill_ID": [244706.0, None],
                "area": [1.5, 2.0],
                "geometry": [Point(0, 0), Point(1, 1)],
            },
            crs="EPSG:4326",
        )
        out = _integral_floats_to_int(gdf)
        self.assertEqual(str(out["vill_ID"].dtype), "Int64")
        self.assertEqual(out["vill_ID"].iloc[0], 244706)
        self.assertEqual(str(out["area"].dtype), "float64")


class LocalLayerSourceTests(SimpleTestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.data_dir = Path(tmp.name)
        patcher = patch.object(local_layers, "DATA_DIR", self.data_dir)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _mining_file(self):
        gdf = gpd.GeoDataFrame(
            {"uid": ["1_1"], "id": [7.0], "geometry": [box(0, 0, 1, 1)]}, crs="EPSG:4326"
        )
        return _write_gpkg(
            self.data_dir / "layers/mining/bihar/banka/banka/banka_banka_mining.gpkg", gdf
        )

    def test_reads_local_file_as_geoserver_shaped_geojson(self):
        path = self._mining_file()
        source = LocalLayerSource("Bihar", "Banka", "Banka")
        data = source.fetch("mining", "banka_banka_mining")
        self.assertEqual(data["features"][0]["properties"]["uid"], "1_1")
        self.assertEqual(data["features"][0]["properties"]["id"], 7)
        self.assertIn("crs", data)
        self.assertEqual(source.origins["banka_banka_mining"], local_layers.ORIGIN_LOCAL)
        self.assertEqual(source.paths["banka_banka_mining"], str(path))

    @patch("stats_generator.local_layers.requests.get")
    def test_missing_layer_of_inactive_tehsil_is_not_fetched(self, get):
        source = LocalLayerSource("bihar", "banka", "banka", is_active=False)
        self.assertIsNone(source.fetch("mining", "banka_banka_mining"))
        get.assert_not_called()
        self.assertEqual(source.origins["banka_banka_mining"], local_layers.ORIGIN_MISSING)

    @patch("stats_generator.local_layers.requests.get")
    def test_missing_layer_of_active_tehsil_comes_from_geoserver(self, get):
        get.return_value = MagicMock(json=lambda: {"features": [{"properties": {}}]})
        source = LocalLayerSource("bihar", "banka", "banka", is_active=True)
        self.assertEqual(len(source.fetch("mining", "banka_banka_mining")["features"]), 1)
        self.assertIn("typeName=mining:banka_banka_mining", get.call_args[0][0])
        self.assertEqual(source.origins["banka_banka_mining"], local_layers.ORIGIN_GEOSERVER)

    @patch("stats_generator.local_layers.requests.get")
    def test_fallback_can_be_disabled(self, get):
        source = LocalLayerSource(
            "bihar", "banka", "banka", is_active=True, geoserver_fallback=False
        )
        self.assertIsNone(source.fetch("mining", "banka_banka_mining"))
        get.assert_not_called()

    def test_soge_years_are_read_oldest_first(self):
        base = self.data_dir / "layers/SOGE_vector/bihar/banka/banka"
        for year in (2022, 2011):
            gdf = gpd.GeoDataFrame(
                {"uid": [str(year)], "geometry": [box(0, 0, 1, 1)]}, crs="EPSG:4326"
            )
            _write_gpkg(base / f"soge_vector_banka_banka_{year}.gpkg", gdf)
        source = LocalLayerSource("bihar", "banka", "banka")
        years = source.fetch_years("soge", "soge_vector_banka_banka")
        self.assertEqual([y for y, _ in years], [2011, 2022])


class LocalExcelGenerationTests(SimpleTestCase):
    """get_vector_layer_geoserver in local mode, with one layer missing."""

    LAYERS = [
        {"layer_name": "mws_{district}_{block}", "workspace": "mws",
         "start_year": None, "end_year": None},
        {"layer_name": "{district}_{block}_mining", "workspace": "mining",
         "start_year": None, "end_year": None},
    ]

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.data_dir = self.tmp / "data"
        patchers = [
            patch.object(local_layers, "DATA_DIR", self.data_dir),
            patch.object(
                local_layers, "sheet_templates", SheetTemplateStore(self.tmp / "t.json")
            ),
            patch(
                "stats_generator.utils.fetch_layers_for_excel_generation",
                return_value=self.LAYERS,
            ),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)
        mws = gpd.GeoDataFrame(
            {"uid": ["1_2", "1_1"], "area_in_ha": [10.0, 20.0],
             "wsconc": ["a", "b"], "bacode": [1, 1], "sbcode": [2, 2],
             "geometry": [box(0, 0, 1, 1), box(1, 0, 2, 1)]},
            crs="EPSG:4326",
        )
        _write_gpkg(
            self.data_dir / "base_layers/tehsil_mws/bihar/banka/banka"
            / "filtered_mws_banka_banka_uid.geojson",
            mws,
        )

    def _run(self, name):
        source = LocalLayerSource("bihar", "banka", "banka")
        out = self.tmp / name
        results = get_vector_layer_geoserver(
            "bihar", "banka", "banka", layer_source=source, xlsx_file=str(out)
        )
        return results, pd.read_excel(out, sheet_name=None)

    def test_missing_layer_gets_blank_sheet_with_learned_headers(self):
        local_layers.sheet_templates.learn(
            "mining|{district}_{block}_mining",
            {"mining": ["UID", "division", "proposal"]},
        )
        results, sheets = self._run("out.xlsx")

        self.assertEqual(
            [(r["workspace"], r["origin"]) for r in results],
            [("mws", "local"), ("mining", "missing")],
        )
        self.assertEqual(sheets["mws"]["UID"].tolist(), ["1_1", "1_2"])
        mining = sheets["mining"]
        self.assertEqual(list(mining.columns), ["UID", "division", "proposal"])
        self.assertEqual(mining["UID"].tolist(), ["1_1", "1_2"])
        self.assertTrue(mining[["division", "proposal"]].isna().all().all())

    def test_learns_headers_of_written_sheets(self):
        self._run("out.xlsx")
        self.assertEqual(
            local_layers.sheet_templates.get("mws|mws_{district}_{block}"),
            {"mws": ["UID", "area_in_ha", "watershed_code", "basin_code", "sub_basin_code"]},
        )


class GeoserverModeUnchangedTests(SimpleTestCase):
    @patch(
        "stats_generator.utils.fetch_layers_for_excel_generation",
        return_value=[{"layer_name": "mws_{district}_{block}", "workspace": "mws",
                       "start_year": None, "end_year": None}],
    )
    @patch("stats_generator.utils.requests.get")
    def test_default_mode_fetches_from_geoserver(self, get, _layers):
        get.return_value = MagicMock(
            json=lambda: {"features": [{"properties": {"uid": "1_1", "area_in_ha": 3}}]}
        )
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "x.xlsx"
            results = get_vector_layer_geoserver("bihar", "banka", "banka", xlsx_file=str(out))
            sheets = pd.read_excel(out, sheet_name=None)
        get.assert_called_once()
        self.assertIn("typeName=mws:mws_banka_banka", get.call_args[0][0])
        self.assertEqual(results, [{"layer": "mws_banka_banka", "status": "success", "workspace": "mws"}])
        self.assertEqual(sheets["mws"]["UID"].tolist(), ["1_1"])


class ComputeParamApiTests(SimpleTestCase):
    """Optional ?compute=local on the stats excel APIs."""

    def _get(self, view, **params):
        from rest_framework.test import APIRequestFactory

        query = {"state": "Bihar", "district": "Nalanda", "block": "Hilsa", **params}
        return view(APIRequestFactory().get("/x/", query))

    @patch("stats_generator.utils.get_vector_layer_geoserver")
    @patch("stats_generator.utils.generate_local_stats_excel")
    def test_download_generates_locally_only_with_compute_local(self, local, geo):
        from stats_generator import api

        with tempfile.TemporaryDirectory() as tmp, patch(
            "stats_generator.utils.EXCEL_PATH", tmp
        ):
            self._get(api.download_stats_excel_file, compute="local")
            local.assert_called_once_with("bihar", "nalanda", "hilsa")
            geo.assert_not_called()

            local.reset_mock()
            self._get(api.download_stats_excel_file)
            geo.assert_called_once_with("bihar", "nalanda", "hilsa")
            local.assert_not_called()

    @patch("stats_generator.utils.get_vector_layer_geoserver")
    @patch("stats_generator.utils.generate_local_stats_excel")
    def test_generate_passes_compute_through(self, local, geo):
        from stats_generator import api

        with patch("stats_generator.api.generate_stats_excel_file") as gen:
            from django.http import HttpResponse

            gen.return_value = HttpResponse(b"ok")
            self._get(api.generate_stats_excel_file_data, compute="LOCAL")
            gen.assert_called_once_with("bihar", "nalanda", "hilsa", compute="local")
            gen.reset_mock()
            self._get(api.generate_stats_excel_file_data)
            gen.assert_called_once_with("bihar", "nalanda", "hilsa", compute="gee")

    def test_invalid_compute_is_rejected(self):
        from stats_generator import api

        response = self._get(api.generate_stats_excel_file_data, compute="foo")
        self.assertEqual(response.status_code, 500)
        self.assertIn("compute must be", response.data["message"])


class WriterFailureTests(LocalExcelGenerationTests):
    """A sheet writer crashing in local mode blanks that layer, not the tehsil."""

    def test_crashing_writer_leaves_blank_sheet(self):
        local_layers.sheet_templates.learn(
            "mws|mws_{district}_{block}", {"mws": ["UID", "area_in_ha"]}
        )
        with patch("stats_generator.utils.create_excel_for_mws", side_effect=AttributeError("boom")):
            results, sheets = self._run("crash.xlsx")
        self.assertEqual(results[0]["status"], "failed")
        self.assertEqual(sheets["mws"]["UID"].tolist(), ["1_1", "1_2"])
        self.assertTrue(sheets["mws"]["area_in_ha"].isna().all())

    def test_learns_headers_of_written_sheets(self):  # covered by the parent
        pass

    def test_missing_layer_gets_blank_sheet_with_learned_headers(self):
        pass


class GeoserverModeStillRaisesTests(SimpleTestCase):
    @patch(
        "stats_generator.utils.fetch_layers_for_excel_generation",
        return_value=[{"layer_name": "mws_{district}_{block}", "workspace": "mws",
                       "start_year": None, "end_year": None}],
    )
    @patch("stats_generator.utils.requests.get")
    def test_writer_error_propagates_in_geoserver_mode(self, get, _layers):
        get.return_value = MagicMock(json=lambda: {"features": []})
        with tempfile.TemporaryDirectory() as tmp, patch(
            "stats_generator.utils.create_excel_for_mws", side_effect=AttributeError("boom")
        ):
            # Not swallowed (closing the then-empty workbook may raise IndexError on top).
            with self.assertRaises((AttributeError, IndexError)):
                get_vector_layer_geoserver(
                    "bihar", "banka", "banka", xlsx_file=str(Path(tmp) / "x.xlsx")
                )


class SwbSeasonalAreaTests(SimpleTestCase):
    """k_/kr_/krz_ are % of area_ored, so ha = area_ored * pct / 100."""

    def _sheet(self, properties):
        from io import BytesIO

        from stats_generator.utils import create_excel_for_swb

        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            create_excel_for_swb(
                {"features": [{"properties": {"MWS_UID": "1_1", **properties}}]},
                None, writer, 2022, 2022,
            )
        buf.seek(0)
        return pd.read_excel(buf, sheet_name="surfaceWaterBodies_annual").iloc[0]

    def test_seasonal_hectares_use_area_ored(self):
        row = self._sheet({
            "area_ored": 10.0, "area_22-23": 6.0,
            "k_22-23": 60.0, "kr_22-23": 30.0, "krz_22-23": 10.0,
        })
        self.assertAlmostEqual(row["total_area_in_ha_2022-2023"], 6.0)
        self.assertAlmostEqual(row["kharif_area_in_ha_2022-2023"], 6.0)  # was 3.6
        self.assertAlmostEqual(row["rabi_area_in_ha_2022-2023"], 3.0)    # was 1.8
        self.assertAlmostEqual(row["zaid_area_in_ha_2022-2023"], 1.0)    # was 0.6
        self.assertAlmostEqual(row["total_swb_area_in_ha"], 10.0)

    def test_area_ored_derived_when_missing(self):
        row = self._sheet({"area_22-23": 6.0, "k_22-23": 60.0, "kr_22-23": 30.0})
        self.assertAlmostEqual(row["rabi_area_in_ha_2022-2023"], 3.0)
