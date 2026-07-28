from nrm_app.celery import app
from computing.misc.admin_boundary import generate_tehsil_shape_file_data
from computing.misc.nrega import clip_nrega_district_block
from computing.mws.mws import mws_layer
from computing.mws.generate_hydrology import generate_hydrology
from computing.lulc.lulc_v3 import clip_lulc_v3
from computing.lulc.lulc_vector import vectorise_lulc
from computing.cropping_intensity.cropping_intensity import generate_cropping_intensity
from computing.surface_water_bodies.swb import generate_swb_layer
from computing.drought.drought import calculate_drought
from computing.drought.drought_causality import drought_causality
from computing.crop_grid.crop_grid import create_crop_grids
from computing.change_detection.change_detection import get_change_detection
from computing.change_detection.change_detection_vector import (
    vectorise_change_detection,
)
from computing.misc.restoration_opportunity import generate_restoration_opportunity
from computing.misc.aquifer_vector import generate_aquifer_vector
from computing.terrain_descriptor.terrain_raster import terrain_raster
from computing.terrain_descriptor.terrain_clusters import generate_terrain_clusters
from computing.lulc_X_terrain.lulc_on_plain_cluster import lulc_on_plain_cluster
from computing.lulc_X_terrain.lulc_on_slope_cluster import lulc_on_slope_cluster
from computing.misc.soge_vector import generate_soge_vector
from computing.misc.stream_order import generate_stream_order
from computing.misc.drainage_lines import clip_drainage_lines
from computing.clart.clart import generate_clart_layer
from computing.tree_health.gee.canopy_height import tree_health_ch_raster
from computing.tree_health.gee.canopy_height_vector import tree_health_ch_vector
from computing.tree_health.gee.ccd import tree_health_ccd_raster
from computing.tree_health.gee.ccd_vector import tree_health_ccd_vector
from computing.tree_health.gee.overall_change import tree_health_overall_change_raster
from computing.tree_health.gee.overall_change_vector import (
    tree_health_overall_change_vector,
)
from computing.tree_health.local.canopy_height_local import tree_health_ch_raster_local
from computing.tree_health.local.canopy_height_vector_local import (
    tree_health_ch_vector_local,
)
from computing.tree_health.local.ccd_local import tree_health_ccd_raster_local
from computing.tree_health.local.ccd_vector_local import tree_health_ccd_vector_local
from computing.tree_health.local.overall_change_local import (
    tree_health_overall_change_raster_local,
)
from computing.tree_health.local.overall_change_vector_local import (
    tree_health_overall_change_vector_local,
)
from computing.soil_health.soil_health import soil_health_local
from computing.soil_type.soil_type_local import generate_soil_type_local
from computing.misc.naturaldepression import generate_natural_depression_data
from computing.misc.distancetonearestdrainage import (
    generate_distance_to_nearest_drainage_line,
)
from computing.misc.catchment_area import generate_catchment_area_singleflow
from computing.misc.slope_percentage import generate_slope_percentage_data
from computing.misc.lcw_conflict import generate_lcw_conflict_data
from computing.misc.agroecological_space import generate_agroecological_data
from computing.misc.factory_csr import generate_factory_csr_data
from computing.misc.green_credit import generate_green_credit_data
from computing.misc.mining_data import generate_mining_data
from computing.plantation.site_suitability import site_suitability
from computing.mws.mws_connectivity import generate_mws_connectivity_data
from computing.misc.ndvi_time_series import ndvi_timeseries
from computing.zoi_layers.zoi import generate_zoi
from computing.mws.mws_centroid import generate_mws_centroid_data
from computing.change_detection.change_detection_local import (
    get_change_detection as get_change_detection_local,
)
from computing.change_detection.change_detection_vector_local import (
    vectorise_change_detection as vectorise_change_detection_local,
)
from computing.cropping_intensity.cropping_intesity_local import (
    generate_cropping_intensity as generate_cropping_intensity_local,
)
from computing.lulc.lulc_v3_local import clip_lulc_v3 as clip_lulc_v3_local
from computing.lulc.lulc_vector_local import vectorise_lulc as vectorise_lulc_local
from computing.lulc_X_terrain.lulc_on_plain_cluster_local import (
    lulc_on_plain_cluster_local,
)
from computing.lulc_X_terrain.lulc_on_slope_cluster_local import (
    lulc_on_slope_cluster_local,
)
from computing.misc.agroecological_space_local_compute import (
    generate_agroecological_data_local,
)
from computing.misc.aquifer_vector_local import (
    generate_aquifer_vector as generate_aquifer_vector_local,
)
from computing.misc.catchment_area_local_compute import (
    generate_catchment_area_singleflow_local,
)
from computing.misc.distancetonearestdrainage_local_compute import (
    generate_distance_to_nearest_drainage_line_local,
)
from computing.misc.drainage_lines_local_compute import (
    clip_drainage_lines as clip_drainage_lines_local,
)
from computing.misc.facilities_proximity_local_compute import (
    generate_facilities_proximity_local,
)
from computing.misc.antyodaya_local_compute import generate_antyodaya_data_local
from computing.misc.canal_local_compute import canal_vector as canal_vector_local
from computing.misc.digital_elevation_model_local import (
    generate_febdem_raster_vector_clip,
)
from computing.misc.drainage_density_local_compute import drainage_density
from computing.misc.livestocks_local_compute import generate_livestocks_data_local
from computing.misc.river_local_compute import river_vector as river_vector_local
from computing.misc.factory_csr_local_compute import generate_factory_csr_data_local
from computing.misc.green_credit_local_compute import generate_green_credit_data_local
from computing.misc.lcw_conflict_local_compute import generate_lcw_conflict_data_local
from computing.misc.mining_data_local_compute import generate_mining_data_local
from computing.misc.naturaldepression_local_compute import (
    generate_natural_depression_data_local,
)
from computing.misc.nrega_local_compute import generate_nrega_data_local
from computing.misc.restoration_opportunity_local_compute import (
    generate_restoration_opportunity_local,
)
from computing.misc.slope_percentage_local_compute import (
    generate_slope_percentage_data_local,
)
from computing.misc.soge_vector_local_compute import generate_soge_vector_local
from computing.mws.mws_centroid_local_compute import generate_mws_centroid_data_local
from computing.mws.mws_connectivity_local_compute import mws_connectivity_vector
from computing.surface_water_bodies.swb_local import (
    generate_swb_layer as generate_swb_layer_local,
)
from computing.terrain_descriptor.terrain_clusters_local import (
    generate_terrain_clusters as generate_terrain_clusters_local,
)
from computing.terrain_descriptor.terrain_compute_all_local import (
    generate_terrain_compute_all,
)
from computing.terrain_descriptor.terrain_raster_fabdem_local import (
    generate_terrain_raster_clip as terrain_raster_local,
)
from stats_generator.utils import generate_stats_excel_file
from utilities.gee_utils import valid_gee_text
import os
import logging
from nrm_app.celery import app
from computing.models import Layer
import json

logger = logging.getLogger(__name__)

VALID_COMPUTE_TYPES = {"gee", "local"}

CONFIG_DIR = os.path.dirname(__file__)
EXTERNAL_CONFIG_DIR = os.path.join("data", "layers", "layer_dependency")

MAP_CONFIG_FILES = {
    "gee": "layer_map.json",
    "local": "local_layer_map.json",
}

END_YEAR_RULE_FILES = {
    "gee": "end_year_rules.json",
    "local": "local_end_year_rules.json",
}


GEE_TASK_REGISTRY = {
    "generate_tehsil_shape_file_data": generate_tehsil_shape_file_data,
    "clip_nrega_district_block": clip_nrega_district_block,
    "generate_nrega_data": clip_nrega_district_block,
    "generate_nrega_layer": clip_nrega_district_block,
    "mws_layer": mws_layer,
    "generate_mws_layer": mws_layer,
    "generate_hydrology": generate_hydrology,
    "clip_lulc_v3": clip_lulc_v3,
    "lulc_v3": clip_lulc_v3,
    "vectorise_lulc": vectorise_lulc,
    "lulc_vector": vectorise_lulc,
    "generate_cropping_intensity": generate_cropping_intensity,
    "generate_ci_layer": generate_cropping_intensity,
    "generate_swb_layer": generate_swb_layer,
    "generate_swb": generate_swb_layer,
    "calculate_drought": calculate_drought,
    "generate_drought_layer": calculate_drought,
    "drought_causality": drought_causality,
    "mws_drought_causality": drought_causality,
    "create_crop_grids": create_crop_grids,
    "crop_grid": create_crop_grids,
    "get_change_detection": get_change_detection,
    "change_detection": get_change_detection,
    "vectorise_change_detection": vectorise_change_detection,
    "change_detection_vector": vectorise_change_detection,
    "generate_restoration_opportunity": generate_restoration_opportunity,
    "restoration_opportunity": generate_restoration_opportunity,
    "generate_aquifer_vector": generate_aquifer_vector,
    "aquifer_vector": generate_aquifer_vector,
    "terrain_raster": terrain_raster,
    "generate_terrain_raster": terrain_raster,
    "generate_terrain_clusters": generate_terrain_clusters,
    "generate_terrain_descriptor": generate_terrain_clusters,
    "lulc_on_plain_cluster": lulc_on_plain_cluster,
    "terrain_lulc_plain_cluster": lulc_on_plain_cluster,
    "lulc_on_slope_cluster": lulc_on_slope_cluster,
    "terrain_lulc_slope_cluster": lulc_on_slope_cluster,
    "generate_soge_vector": generate_soge_vector,
    "soge_vector": generate_soge_vector,
    "generate_stream_order": generate_stream_order,
    "stream_order": generate_stream_order,
    "clip_drainage_lines": clip_drainage_lines,
    "generate_drainage_layer": clip_drainage_lines,
    "generate_clart_layer": generate_clart_layer,
    "generate_clart": generate_clart_layer,
    "tree_health_ch_raster": tree_health_ch_raster,
    "tree_health_ch_vector": tree_health_ch_vector,
    "tree_health_ccd_raster": tree_health_ccd_raster,
    "tree_health_ccd_vector": tree_health_ccd_vector,
    "tree_health_overall_change_raster": tree_health_overall_change_raster,
    "tree_health_overall_change_vector": tree_health_overall_change_vector,
    "generate_natural_depression_data": generate_natural_depression_data,
    "generate_natural_depression": generate_natural_depression_data,
    "generate_distance_to_nearest_drainage_line": generate_distance_to_nearest_drainage_line,
    "generate_distance_nearest_DL": generate_distance_to_nearest_drainage_line,
    "generate_catchment_area_singleflow": generate_catchment_area_singleflow,
    "generate_slope_percentage_data": generate_slope_percentage_data,
    "generate_slope_percentage": generate_slope_percentage_data,
    "generate_lcw_conflict_data": generate_lcw_conflict_data,
    "generate_lcw": generate_lcw_conflict_data,
    "generate_agroecological_data": generate_agroecological_data,
    "generate_agroecological": generate_agroecological_data,
    "generate_factory_csr_data": generate_factory_csr_data,
    "generate_factory_csr": generate_factory_csr_data,
    "generate_green_credit_data": generate_green_credit_data,
    "generate_green_credit": generate_green_credit_data,
    "generate_mining_data": generate_mining_data,
    "generate_mining": generate_mining_data,
    "site_suitability": site_suitability,
    "plantation_site_suitability": site_suitability,
    "generate_mws_connectivity_data": generate_mws_connectivity_data,
    "generate_mws_connectivity": generate_mws_connectivity_data,
    "ndvi_timeseries": ndvi_timeseries,
    "generate_ndvi_timeseries": ndvi_timeseries,
    "generate_zoi": generate_zoi,
    "generate_zoi_data": generate_zoi,
    "generate_mws_centroid_data": generate_mws_centroid_data,
    "generate_mws_centroid": generate_mws_centroid_data,
}

LOCAL_TASK_REGISTRY = {
    "clip_nrega_district_block": generate_nrega_data_local,
    "generate_nrega_data": generate_nrega_data_local,
    "generate_nrega_layer": generate_nrega_data_local,
    "clip_lulc_v3": clip_lulc_v3_local,
    "lulc_v3": clip_lulc_v3_local,
    "vectorise_lulc": vectorise_lulc_local,
    "lulc_vector": vectorise_lulc_local,
    "generate_cropping_intensity": generate_cropping_intensity_local,
    "generate_ci_layer": generate_cropping_intensity_local,
    "generate_swb_layer": generate_swb_layer_local,
    "generate_swb": generate_swb_layer_local,
    "get_change_detection": get_change_detection_local,
    "change_detection": get_change_detection_local,
    "vectorise_change_detection": vectorise_change_detection_local,
    "change_detection_vector": vectorise_change_detection_local,
    "generate_aquifer_vector": generate_aquifer_vector_local,
    "aquifer_vector": generate_aquifer_vector_local,
    "terrain_raster": terrain_raster_local,
    "generate_terrain_raster": terrain_raster_local,
    "generate_terrain_clusters": generate_terrain_clusters_local,
    "generate_terrain_descriptor": generate_terrain_clusters_local,
    "generate_terrain_compute_all": generate_terrain_compute_all,
    "lulc_on_plain_cluster": lulc_on_plain_cluster_local,
    "terrain_lulc_plain_cluster": lulc_on_plain_cluster_local,
    "lulc_on_slope_cluster": lulc_on_slope_cluster_local,
    "terrain_lulc_slope_cluster": lulc_on_slope_cluster_local,
    "generate_soge_vector": generate_soge_vector_local,
    "soge_vector": generate_soge_vector_local,
    "clip_drainage_lines": clip_drainage_lines_local,
    "generate_drainage_layer": clip_drainage_lines_local,
    "generate_natural_depression_data": generate_natural_depression_data_local,
    "generate_natural_depression": generate_natural_depression_data_local,
    "generate_distance_to_nearest_drainage_line": generate_distance_to_nearest_drainage_line_local,
    "generate_distance_nearest_DL": generate_distance_to_nearest_drainage_line_local,
    "generate_catchment_area_singleflow": generate_catchment_area_singleflow_local,
    "generate_slope_percentage_data": generate_slope_percentage_data_local,
    "generate_slope_percentage": generate_slope_percentage_data_local,
    "generate_lcw_conflict_data": generate_lcw_conflict_data_local,
    "generate_lcw": generate_lcw_conflict_data_local,
    "generate_agroecological_data": generate_agroecological_data_local,
    "generate_agroecological": generate_agroecological_data_local,
    "generate_factory_csr_data": generate_factory_csr_data_local,
    "generate_factory_csr": generate_factory_csr_data_local,
    "generate_green_credit_data": generate_green_credit_data_local,
    "generate_green_credit": generate_green_credit_data_local,
    "generate_mining_data": generate_mining_data_local,
    "generate_mining": generate_mining_data_local,
    "generate_restoration_opportunity": generate_restoration_opportunity_local,
    "restoration_opportunity": generate_restoration_opportunity_local,
    "generate_mws_connectivity_data": mws_connectivity_vector,
    "generate_mws_connectivity": mws_connectivity_vector,
    "generate_facilities_proximity_task": generate_facilities_proximity_local,
    "generate_facilities_proximity": generate_facilities_proximity_local,
    "generate_livestocks": generate_livestocks_data_local,
    "generate_antyodaya": generate_antyodaya_data_local,
    "generate_density_vector": drainage_density,
    "generate_river_data": river_vector_local,
    "generate_canal_vector": canal_vector_local,
    "generate_dem_raster_vector": generate_febdem_raster_vector_clip,
    "generate_mws_centroid_data": generate_mws_centroid_data_local,
    "generate_mws_centroid": generate_mws_centroid_data_local,
    "tree_health_ch_raster": tree_health_ch_raster_local,
    "tree_health_ch_vector": tree_health_ch_vector_local,
    "tree_health_ccd_raster": tree_health_ccd_raster_local,
    "tree_health_ccd_vector": tree_health_ccd_vector_local,
    "tree_health_overall_change_raster": tree_health_overall_change_raster_local,
    "tree_health_overall_change_vector": tree_health_overall_change_vector_local,
    "soil_health": soil_health_local,
    "generate_soil_type": generate_soil_type_local,
    "soil_type": generate_soil_type_local,
}

TASK_REGISTRIES = {
    "gee": GEE_TASK_REGISTRY,
    "local": LOCAL_TASK_REGISTRY,
}


@app.task(bind=True)
def layer_generate_map(
    self,
    state,
    district,
    block,
    map_order,
    gee_account_id,
    start_year=None,
    end_year=None,
    compute="gee",
):
    """
    This function take state, district,block and map_order(map to trigger, it can be map_1, map_2_1, map_2_2, map_3, map_4). One map trigger more numbers of pipeline.

    Sibling nodes (nodes that don't depend on each other) keep running even if one of
    them fails; only nodes that declare a dependency on a failed node are skipped.
    """
    compute = normalize_compute(compute)
    log_ctx = f"state={state}, district={district}, block={block}, map={map_order}, compute={compute}"
    status = {}

    # checking:- is mws layer generated?
    try:
        if compute == "gee" and map_order in ["map_2_1", "map_2_2", "map_3", "map_4"]:
            layer = (
                Layer.objects.filter(
                    layer_name=f"mws_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
                )
                .order_by("-layer_version")
                .first()
            )
            if not layer:
                logger.error(f"MWS layer missing, cannot proceed ({log_ctx})")
                return f"check mws layer for {district}_{block}"
    except Exception as e:
        logger.exception(f"Exception while checking mws layer ({log_ctx})")
        return f"exception occur while checking mws for {district}_{block} as: {e}"

    global_args = {}
    if start_year:
        global_args["start_year"] = start_year
    if end_year:
        global_args["end_year"] = end_year

    # Load JSON configuration
    map_config = load_map_config(map_order, compute=compute)

    if not map_config:
        logger.error(f"Map configuration not found ({log_ctx})")
        return f"Map configuration not found for {map_order} using compute={compute}"

    validation_errors = validate_map_config(map_config, compute=compute)
    if validation_errors:
        logger.error(
            f"Invalid map configuration ({log_ctx}): {'; '.join(validation_errors)}"
        )
        return (
            f"Invalid {compute} map configuration for {map_order}: "
            f"{'; '.join(validation_errors)}"
        )

    logger.info(f"Starting layer generation ({log_ctx})")
    task_registry = TASK_REGISTRIES[compute]
    for func in map_config:
        run_node_tree(
            node=func,
            task_registry=task_registry,
            compute=compute,
            state=state,
            district=district,
            block=block,
            global_args=global_args,
            gee_account_id=gee_account_id,
            status=status,
        )

    failed_nodes = sorted(name for name, ok in status.items() if not ok)
    succeeded_nodes = sorted(name for name, ok in status.items() if ok)
    if failed_nodes:
        logger.error(
            f"Layer generation completed with failures ({log_ctx}): "
            f"failed={failed_nodes}, succeeded={succeeded_nodes}"
        )
    else:
        logger.info(
            f"Layer generation completed successfully ({log_ctx}): "
            f"succeeded={succeeded_nodes}"
        )

    return f"{status = }"


def normalize_compute(compute):
    compute = str(compute or "gee").strip().lower()
    if compute not in VALID_COMPUTE_TYPES:
        raise ValueError("compute must be either 'gee' or 'local'")
    return compute


def _candidate_config_paths(filename):
    return [
        os.path.join(EXTERNAL_CONFIG_DIR, filename),
        os.path.join(CONFIG_DIR, filename),
    ]


def _load_json_config(filename, default=None):
    for config_path in _candidate_config_paths(filename):
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                return json.load(f)
    if default is not None:
        return default
    raise FileNotFoundError(
        f"Layer dependency config not found. Checked: "
        f"{_candidate_config_paths(filename)}"
    )


def load_map_config(map_order, compute="gee"):
    """
    Load map configuration from JSON file based on map_order.
    """
    compute = normalize_compute(compute)
    all_configs = _load_json_config(MAP_CONFIG_FILES[compute], default={})
    return all_configs.get(map_order, [])


def load_end_year_rules(compute="gee"):
    """
    Load end year rules from JSON.
    """
    compute = normalize_compute(compute)
    return _load_json_config(END_YEAR_RULE_FILES[compute], default={})


def flatten_map_nodes(map_config):
    nodes = []
    for node in map_config:
        nodes.append(node)
        nodes.extend(flatten_map_nodes(node.get("children", [])))
    return nodes


def validate_layer_map_request(map_order, compute="gee"):
    compute = normalize_compute(compute)
    map_config = load_map_config(map_order, compute=compute)
    if not map_config:
        return [f"Map configuration not found for {map_order} using compute={compute}"]
    return validate_map_config(map_config, compute=compute)


def validate_map_config(map_config, compute="gee"):
    compute = normalize_compute(compute)
    task_registry = TASK_REGISTRIES[compute]
    node_names = {node.get("name") for node in flatten_map_nodes(map_config)}
    validator_names = set(DependencyValidator.names_for_compute(compute))

    unsupported_nodes = sorted(
        node_name for node_name in node_names if node_name not in task_registry
    )
    unsupported_deps = sorted(
        dep
        for node in flatten_map_nodes(map_config)
        for dep in node.get("depends_on", [])
        if dep not in node_names and dep not in validator_names
    )

    errors = []
    if unsupported_nodes:
        errors.append(f"unsupported {compute} nodes: {', '.join(unsupported_nodes)}")
    if unsupported_deps:
        errors.append(
            f"unsupported {compute} dependencies: {', '.join(unsupported_deps)}"
        )
    return errors


# check the dependency layer is available or not
class DependencyValidator:
    GEE_VALIDATORS = {}
    LOCAL_VALIDATORS = {}

    @staticmethod
    def _local_layer_exists(layer_name=None, dataset_name=None):
        qs = Layer.objects.filter(misc__is_generated_locally=True)
        if layer_name:
            qs = qs.filter(layer_name=layer_name)
        if dataset_name:
            qs = qs.filter(dataset__name=dataset_name)
        return qs.exists()

    @classmethod
    def get_checker(cls, dep, compute):
        compute = normalize_compute(compute)
        if compute == "local":
            return cls.LOCAL_VALIDATORS.get(dep)
        return cls.GEE_VALIDATORS.get(dep)

    @classmethod
    def names_for_compute(cls, compute):
        compute = normalize_compute(compute)
        if compute == "local":
            return set(cls.LOCAL_VALIDATORS)
        return set(cls.GEE_VALIDATORS)

    @staticmethod
    def clip_lulc_v3(district, block):
        return (
            Layer.objects.filter(
                layer_name__icontains=f"{valid_gee_text(district)}_{valid_gee_text(block)}_level_"
            ).count()
            == 24
        )

    @staticmethod
    def terrain_raster(district, block):
        return Layer.objects.filter(
            layer_name=f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_terrain_raster"
        ).exists()

    @staticmethod
    def generate_catchment_area_singleflow(district, block):
        return Layer.objects.filter(
            layer_name=f"catchment_area_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_raster"
        ).exists()

    @staticmethod
    def generate_stream_order(district, block):
        return Layer.objects.filter(
            layer_name=f"stream_order_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_vector"
        ).exists()

    @staticmethod
    def clip_drainage_lines(district, block):
        return Layer.objects.filter(
            layer_name=f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
            dataset__name="Drainage",
        ).exists()

    @staticmethod
    def generate_cropping_intensity(district, block):
        return Layer.objects.filter(
            layer_name=f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_intensity"
        ).exists()

    @staticmethod
    def generate_swb_layer(district, block):
        return Layer.objects.filter(
            layer_name=f"surface_waterbodies_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
        ).exists()

    @staticmethod
    def generate_tehsil_shape_file_data(district, block):
        return Layer.objects.filter(
            layer_name=f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
            dataset__name="Admin Boundary",
        ).exists()

    @staticmethod
    def local_layer_generated(district, block):
        return (
            Layer.objects.filter(
                layer_name__icontains=valid_gee_text(district.lower()),
                misc__is_generated_locally=True,
            )
            .filter(layer_name__icontains=valid_gee_text(block.lower()))
            .exists()
        )


DependencyValidator.GEE_VALIDATORS = {
    name: getattr(DependencyValidator, name)
    for name in [
        "clip_lulc_v3",
        "terrain_raster",
        "generate_catchment_area_singleflow",
        "generate_stream_order",
        "clip_drainage_lines",
        "generate_cropping_intensity",
        "generate_swb_layer",
        "generate_tehsil_shape_file_data",
    ]
}

DependencyValidator.LOCAL_VALIDATORS = {
    # Local ordered maps mostly depend on nodes generated earlier in the same run.
    # This fallback is for externally pre-generated local dependencies.
    "local_layer_generated": DependencyValidator.local_layer_generated,
}


def run_node_tree(
    node,
    task_registry,
    compute,
    state,
    district,
    block,
    global_args,
    gee_account_id,
    status,
):
    node_func_name = node["name"]
    node_func_obj = task_registry[node_func_name]
    args = get_args(
        iterator_name=node,
        global_args=global_args,
        gee_account_id=gee_account_id,
        compute=compute,
    )
    deps = node.get("depends_on", [])
    run_layer_with_dependency(
        deps=deps,
        node_func_name=node_func_name,
        node_func_obj=node_func_obj,
        compute=compute,
        state=state,
        district=district,
        block=block,
        args=args,
        status=status,
    )

    if not status.get(node_func_name, False):
        # Only nodes depending on this one are affected; siblings keep running.
        return

    for child in node.get("children", []):
        run_node_tree(
            node=child,
            task_registry=task_registry,
            compute=compute,
            state=state,
            district=district,
            block=block,
            global_args=global_args,
            gee_account_id=gee_account_id,
            status=status,
        )


def run_layer_with_dependency(
    deps, node_func_name, node_func_obj, compute, state, district, block, args, status
):
    """
    This function checks dependency of layer if it is generated or not and call the pipeline functions and maintain status of each function,
    """
    log_ctx = (
        f"node={node_func_name}, state={state}, district={district}, block={block}"
    )
    for dep in deps:
        if status.get(dep, False):
            continue
        checker = DependencyValidator.get_checker(dep, compute)
        status[dep] = checker(district, block) if checker else False
        if not status[dep]:
            logger.warning(
                f"Skipping {node_func_name} because dependency {dep} failed or was not executed ({log_ctx})"
            )
            status[node_func_name] = False
            break
    else:
        try:
            end_year_rules = load_end_year_rules(compute=compute)
            if node_func_name in end_year_rules:
                args["end_year"] = end_year_rules[node_func_name]
            if node_func_name == "site_suitability":
                args["project_id"] = None
            logger.info(
                f"Running {node_func_name} with args={args}, depends_on={deps} ({log_ctx})"
            )
            if node_func_name == "generate_stats_excel_file":
                result = node_func_obj(state, district, block)
            else:
                result = (
                    node_func_obj(state=state, district=district, block=block, **args)
                    if args
                    else node_func_obj(state, district, block)
                )
            if result:
                status[node_func_name] = True
                logger.info(f"Completed {node_func_name} ({log_ctx}): result={result}")
            else:
                status[node_func_name] = False
                logger.warning(
                    f"{node_func_name} returned a falsy result, treating as failed ({log_ctx}): result={result}"
                )
        except Exception:
            status[node_func_name] = False
            logger.exception(f"{node_func_name} raised an error ({log_ctx})")


def get_args(iterator_name, global_args, gee_account_id, compute="gee"):
    """
    This function merge the global agrs and local args(define in json maps) return combination of both.
    """
    arg = iterator_name.get("args", {})
    args = dict(arg)
    if normalize_compute(compute) == "gee" or iterator_name.get(
        "pass_gee_account_id", False
    ):
        args["gee_account_id"] = gee_account_id
    if iterator_name.get("use_global_args", False):
        args = {
            **global_args,
            **args,
        }
    return args
