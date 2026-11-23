import importlib
import os
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch


class DataDirAndLocalOutputPathsTests(TestCase):
    """Verify DATA_DIR relocation and config.yaml <-> config_loader.py alignment."""

    def test_config_loader_outputs_match_config_yaml(self):
        from computing import config_loader

        state, district, block = "bihar", "banka", "banka"

        expected_pairs = [
            (config_loader.CHANGE_DETECTION_RASTER_OUTPUT_DIR, "change detection"),
            (config_loader.CHANGE_DETECTION_VECTOR_OUTPUT_DIR, "change detection vector"),
            (config_loader.TERRAIN_RASTER_OUTPUT_DIR, "generate_terrain_raster"),
            (config_loader.TERRAIN_CLUSTER_OUTPUT_DIR, "generate_terrain_descriptor"),
            (config_loader.SOIL_HEALTH_RASTER_OUTPUT_DIR, "clip_soil_health"),
            (config_loader.SOIL_HEALTH_VECTOR_OUTPUT_DIR, "soil_health_vector"),
            (config_loader.CROPPING_INTENSITY_OUTPUT_DIR, "cropping intensity"),
            (config_loader.LULC_VECTOR_OUTPUT_DIR, "lulc vector"),
            (config_loader.LULC_V3_OUTPUT_DIR, "lulc v3"),
            (config_loader.LULC_V4_OUTPUT_DIR, "lulc v4"),
            (config_loader.LULC_SLOPE_CLUSTER_OUTPUT_DIR, "lulc slope clusters"),
            (config_loader.LULC_PLAIN_CLUSTER_OUTPUT_DIR, "lulc plain clusters"),
            (config_loader.AQUIFER_VECTOR_OUTPUT_DIR, "aquifer vector"),
            (config_loader.SWB_VECTOR_OUTPUT_DIR, "generate_swb"),
            (config_loader.SOIL_TYPE_OUTPUT_DIR, "soil type"),
            (config_loader.HYDROLOGY_LOCAL_OUTPUT_DIR, "hydrology_fortnightly"),
            (config_loader.LOCAL_DRAINAGE_LINES_OUTPUT, "generate_drainage_layer"),
            (config_loader.LOCAL_DRAINAGE_DENSITY_OUTPUT, "drainage_density_vector"),
            (config_loader.LOCAL_CANAL_OUTPUT, "generate_canal_vector"),
            (config_loader.LOCAL_RIVER_OUTPUT, "generate_river_data"),
            (config_loader.LOCAL_FABDEM_OUTPUT, "dem_vector"),
            (config_loader.LOCAL_AGROECOLOGICAL_OUTPUT, "generate_agroecological"),
            (config_loader.LOCAL_LCW_OUTPUT, "generate_lcw"),
            (config_loader.LOCAL_SOGE_OUTPUT, "soge_vector"),
            (config_loader.LOCAL_FACTORY_CSR_OUTPUT, "generate_factory_csr"),
            (config_loader.LOCAL_GREEN_CREDIT_OUTPUT, "generate_green_credit"),
            (config_loader.LOCAL_MINING_OUTPUT, "generate_mining"),
            (config_loader.LOCAL_NATURALDEPRESSION_OUTPUT, "generate_natural_depression"),
            (
                config_loader.LOCAL_DISTANCETONEARESTDRAINAGE_OUTPUT,
                "generate_distance_nearest_DL",
            ),
            (
                config_loader.LOCAL_CATCHMENT_AREA_OUTPUT,
                "generate_catchment_area_singleflow",
            ),
            (config_loader.LOCAL_SLOPE_PERCENTAGE_OUTPUT, "generate_slope_percentage"),
            (
                config_loader.LOCAL_MWS_CONNECTIVITY_OUTPUT,
                "generate_mws_connectivity_data",
            ),
            (config_loader.LOCAL_MWS_CENTROID_OUTPUT, "generate_mws_centroid"),
            (config_loader.NREGA_LOCAL_OUTPUT, "generate_nrega_layer"),
            (config_loader.LOCAL_RESTORATION_OUTPUT, "restoration_opportunity_vector"),
            (config_loader.LOCAL_SITE_SUITABILITY_OUTPUT, "site_suitability"),
            (config_loader.LOCAL_FACILITIES_OUTPUT, "generate_facilities_proximity"),
            (config_loader.LOCAL_ANTYODAYA_2020_OUTPUT, "generate_antyodaya"),
            (config_loader.LOCAL_LIVESTOCKS_OUTPUT, "generate_livestocks"),
            (config_loader.LOCAL_FOREST_FRINGE_OUTPUT, "forest fringe"),
            (config_loader.LOCAL_TREE_IN_GRASSLAND_OUTPUT, "tree in grassland"),
            (config_loader.LOCAL_FOREST_FIRE_OUTPUT, "forest fire"),
        ]

        for output_const, config_name in expected_pairs:
            with self.subTest(layer=config_name):
                entry = config_loader._derived_layer(config_name)
                self.assertIsNotNone(entry, f"Missing derived_layer: {config_name}")
                template = entry["local_path"]
                expected_dir = Path(
                    template.replace("{DATA_DIR}", str(config_loader.DATA_DIR))
                    .replace("{state}", state)
                    .replace("{district}", district)
                    .replace("{block}", block)
                    .replace("{filename}", "")
                    .rstrip("/")
                )
                actual_dir = config_loader.layer_dir(output_const, state, district, block)
                self.assertEqual(actual_dir, expected_dir)

    def test_relocated_data_dir_propagates_to_modules(self):
        from computing import config_loader

        with tempfile.TemporaryDirectory() as tmp:
            custom_data_dir = Path(tmp).resolve()
            with patch.dict(os.environ, {"DATA_DIR": str(custom_data_dir)}):
                importlib.reload(config_loader)
                try:
                    self.assertEqual(config_loader.DATA_DIR, custom_data_dir)

                    from computing.hydrology_gpu import watershed_boundary
                    from computing.misc.antyodaya import pipeline as antyodaya_pipeline
                    from computing.misc.facilities import pipeline as facilities_pipeline
                    from computing.misc.livestocks import pipeline as livestocks_pipeline
                    from computing.mws import (
                        et_download,
                        generate_hydrology_local,
                        runoff_gpu,
                    )
                    from computing.tree_health.local import (
                        canopy_height_local,
                        ccd_local,
                        overall_change_local,
                    )
                    from utilities import constants, download_gpkg_from_geoserver

                    for mod in (
                        constants,
                        download_gpkg_from_geoserver,
                        watershed_boundary,
                        canopy_height_local,
                        ccd_local,
                        overall_change_local,
                        runoff_gpu,
                        et_download,
                        generate_hydrology_local,
                        livestocks_pipeline,
                        antyodaya_pipeline,
                        facilities_pipeline,
                    ):
                        importlib.reload(mod)

                    self.assertEqual(
                        Path(constants.BASE_RESOURCES_DIR),
                        custom_data_dir / "base_resources",
                    )
                    self.assertEqual(
                        download_gpkg_from_geoserver.BASE_OUTPUT_DIR,
                        custom_data_dir / "base_layers",
                    )
                    self.assertEqual(
                        canopy_height_local.LOCAL_CH_BASE_DIR,
                        custom_data_dir / "base_layers/tree_health/ch",
                    )
                    self.assertEqual(
                        ccd_local.LOCAL_CCD_BASE_DIR,
                        custom_data_dir / "base_layers/tree_health/ccd",
                    )
                    self.assertEqual(
                        overall_change_local.LOCAL_TREE_CHANGE_BASE_DIR,
                        custom_data_dir / "base_layers/tree_health",
                    )
                    self.assertEqual(
                        generate_hydrology_local.HYDROLOGY_BASE_LAYER_ROOT,
                        custom_data_dir / "base_layers/hydrology",
                    )
                    self.assertEqual(
                        runoff_gpu.PAN_INDIA_RUNOFF_OUTPUT_ROOT,
                        custom_data_dir / "base_layers/hydrology/runoff",
                    )
                    self.assertEqual(
                        et_download.PAN_INDIA_ET_OUTPUT_ROOT,
                        custom_data_dir / "base_layers/hydrology/et",
                    )
                    self.assertEqual(
                        watershed_boundary.DEFAULT_BOUNDARY_OUTPUT_ROOT,
                        custom_data_dir / "hydrology_gpu/boundaries",
                    )
                    self.assertEqual(
                        watershed_boundary.DEFAULT_PAN_INDIA_DOWNLOAD_BOUNDARY,
                        custom_data_dir
                        / "base_layers/PanIndia_Boundaries/india_state_outer_no_islands.geojson",
                    )

                    # Verify livestocks / antyodaya / facilities output roots honor DATA_DIR
                    for mod, expected_dir in (
                        (livestocks_pipeline, "livestocks20"),
                        (antyodaya_pipeline, "antyodaya20"),
                        (facilities_pipeline, "facilities"),
                    ):
                        cfg = mod.load_config(mod.CONFIG_PATH)
                        root = mod._output_root_path(cfg["output"]["root"])
                        self.assertEqual(root, custom_data_dir / "layers")
                        self.assertEqual(cfg["output"]["directory_name"], expected_dir)
                        # Non-template overrides still resolve normally.
                        self.assertEqual(
                            mod._output_root_path("/tmp/custom_out"),
                            Path("/tmp/custom_out"),
                        )
                finally:
                    # Restore default DATA_DIR after test
                    os.environ.pop("DATA_DIR", None)
                    importlib.reload(config_loader)

    def test_local_layer_map_uses_lulc_and_lulc_version_v4(self):
        import json

        from computing import bulk_layer_generation, config_loader
        from computing.drought.local.config import Paths as DroughtPaths
        from computing.hydrology_gpu import config as hydro_config
        from computing.layer_dependency.layer_generation_in_order import (
            LOCAL_TASK_REGISTRY,
        )
        from computing.lulc.clip_lulc_local import clip_lulc_local

        map_path = (
            config_loader.PROJECT_ROOT
            / "data/layers/layer_dependency/local/local_layer_map.json"
        )
        layer_map = json.loads(map_path.read_text(encoding="utf-8"))
        dynamic_nodes = layer_map["dynamic_layers"]
        dynamic_names = {node["name"] for node in dynamic_nodes}

        self.assertIn("lulc", dynamic_names)
        self.assertNotIn("lulc_v3", dynamic_names)
        for node in dynamic_nodes:
            for dep in node.get("depends_on", []):
                self.assertIn(dep, dynamic_names)
            self.assertIn(node["name"], LOCAL_TASK_REGISTRY)
            for child in node.get("children", []):
                self.assertIn(child["name"], LOCAL_TASK_REGISTRY)

        for node in layer_map["static_layers"]:
            self.assertIn(node["name"], LOCAL_TASK_REGISTRY)

        self.assertIs(LOCAL_TASK_REGISTRY["lulc"], clip_lulc_local)
        self.assertIs(LOCAL_TASK_REGISTRY["lulc_v3"], clip_lulc_local)
        self.assertIs(LOCAL_TASK_REGISTRY["lulc_v4"], clip_lulc_local)
        self.assertIn(
            "computing.lulc.clip_lulc_local.clip_lulc_local",
            bulk_layer_generation.LOCAL_TASK_DATASETS,
        )
        self.assertEqual(config_loader.LULC_VERSION, "v4")
        self.assertEqual(
            DroughtPaths(config_loader.DATA_DIR, config_loader.LULC_BASE_DIR)
            .lulc_file(2024)
            .name,
            "lulc_v4_2024_2025.tif",
        )
        self.assertTrue(hydro_config.LULC_PATH.endswith("lulc_v4_2024_2025.tif"))

