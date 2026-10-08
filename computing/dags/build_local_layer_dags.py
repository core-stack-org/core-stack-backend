#!/usr/bin/env python3
"""
Build STACD catalog DAGs (dag.json + algorithms/*) from
core-stack-backend/computing/layer_dependency/local_layer_map.json,
split into a Dynamic_Layers DAG and a Static_Layers DAG.

Mirrors the STAC catalog layout under stacd_catalog_data/dags/<dag>_v1/
used by the other DAGs in this repo (see CoreStack_DAG_LocalCompute_v1).

Usage:
    python3 build_local_layer_dags.py
"""
import json
import uuid
from pathlib import Path

STAC_EXTENSIONS = ["https://github.com/saharsh-laud/stacd-spec/v1.0.0/schema.json"]
REGISTERED_AT = "2026-09-09T00:00:00Z"
CODE_ASSET = "https://github.com/core-stack-org/core-stack-backend/tree/corestack-lite"

LAYER_MAP_PATH = Path(
    "../core-stack-backend/computing/layer_dependency/local_layer_map.json"
)
CATALOG_ROOT = Path("stacd_catalog_data/dags")

# json node "name" -> (algorithm_id, output_dataset_id, description)
NODE_META = {
    # dynamic_layers
    "generate_nrega_layer": ("NREGA_Clip", "NREGA_Layer", "Clips the NREGA shapefile to the local watershed boundary."),
    "lulc_v3": ("LULC_Algorithm", "LULC_Raster", "Generates yearly LULC rasters (start_year to end_year) clipped to the local watershed boundary."),
    "lulc_vector": ("LULC_Vectorization", "LULC_Vector", "Vectorizes the local LULC raster into per-MWS area statistics."),
    "change_detection": ("Change_Detection", "Change_Detection_Asset", "Computes change-detection classes from local LULC rasters."),
    "change_detection_vector": ("Change_Detection_Vector", "Change_Detection_Vector_Asset", "Vectorizes the local change-detection raster."),
    "generate_terrain_raster": ("Terrain_Algorithm", "Terrain_Raster", "Generates FABDEM terrain raster clipped to the local watershed boundary."),
    "generate_terrain_descriptor": ("Terrain_Clusters", "Terrain_Vector", "Generates terrain cluster vector per MWS from the local terrain raster."),
    "terrain_lulc_plain_cluster": ("LULC_Terrain_Plain", "LULC_Terrain_Plain_Asset", "Computes LULC proportions on plain terrain clusters."),
    "terrain_lulc_slope_cluster": ("LULC_Terrain_Slope", "LULC_Terrain_Slope_Asset", "Computes LULC proportions on slope terrain clusters."),
    "generate_ci_layer": ("Cropping_Intensity", "Cropping_Intensity_Asset", "Derives cropping intensity from local LULC rasters."),
    "generate_swb": ("SWB_Layer", "SWB_Layer_Asset", "Generates the surface water body layer from precomputed local watershed geometry."),
    "generate_zoi": ("ZOI", "ZOI_Asset", "Generates the Zone of Influence layer from the local SWB layer."),
    "tree_health_ch_raster": ("Tree_Health_CH_Raster", "Tree_Health_CH_Raster_Asset", "Generates the canopy height raster from local LULC rasters."),
    "tree_health_ch_vector": ("Tree_Health_CH_Vector", "Tree_Health_CH_Vector_Asset", "Vectorizes the local canopy height raster."),
    "tree_health_ccd_raster": ("Tree_Health_CCD_Raster", "Tree_Health_CCD_Raster_Asset", "Generates the canopy cover density raster from local LULC rasters."),
    "tree_health_ccd_vector": ("Tree_Health_CCD_Vector", "Tree_Health_CCD_Vector_Asset", "Vectorizes the local canopy cover density raster."),
    "tree_health_overall_change_raster": ("Tree_Health_OC_Raster", "Tree_Health_OC_Raster_Asset", "Generates the overall tree-health change raster from local change-detection output."),
    "tree_health_overall_change_vector": ("Tree_Health_OC_Vector", "Tree_Health_OC_Vector_Asset", "Vectorizes the local overall tree-health change raster."),
    "soil_health": ("Soil_Health", "Soil_Health_Asset", "Generates soil health nutrient statistics from local watershed geometry."),
    # static_layers
    "aquifer_vector": ("Aquifer_Vector", "Aquifer_Vector_Asset", "Generates the aquifer vector layer from local watershed geometry."),
    "generate_livestocks": ("Livestock", "Livestock_Asset", "Generates livestock census statistics from local admin geometry."),
    "generate_antyodaya": ("Antyodaya", "Antyodaya_Asset", "Generates Mission Antyodaya statistics from local admin geometry."),
    "generate_density_vector": ("Drainage_Density", "Drainage_Density_Asset", "Generates the drainage density layer from local watershed geometry."),
    "generate_river_data": ("River", "River_Asset", "Generates the river vector layer from local watershed geometry."),
    "generate_canal_vector": ("Canal", "Canal_Asset", "Generates the canal vector layer from local watershed geometry."),
    "generate_dem_raster_vector": ("Digital_Elevation_Model", "Digital_Elevation_Model_Asset", "Generates FABDEM raster and vector outputs from local watershed geometry."),
    "generate_facilities_proximity": ("Facilities_Proximity", "Facilities_Proximity_Asset", "Generates facilities proximity statistics from local admin geometry."),
    "generate_drainage_layer": ("Drainage_Lines", "Drainage_Lines_Asset", "Clips drainage lines to local watershed geometry."),
    "restoration_opportunity": ("Restoration_Opportunity", "Restoration_Opportunity_Asset", "Generates the restoration opportunity layer from local watershed geometry."),
    "soge_vector": ("SOGE_Vector", "SOGE_Vector_Asset", "Generates the SOGE vector layer from local watershed geometry."),
    "generate_lcw": ("LCW_Conflict", "LCW_Conflict_Asset", "Generates land-crop-water conflict data from local watershed geometry."),
    "generate_agroecological": ("Agro_Ecological", "Agro_Ecological_Asset", "Generates agro-ecological zone data from local watershed geometry."),
    "generate_factory_csr": ("Factory_CSR", "Factory_CSR_Asset", "Generates factory/CSR zone data from local watershed geometry."),
    "generate_green_credit": ("Green_Credit", "Green_Credit_Asset", "Generates green credit zone data from local watershed geometry."),
    "generate_mining": ("Mining", "Mining_Asset", "Generates mining zone data from local watershed geometry."),
    "generate_natural_depression": ("Natural_Depression", "Natural_Depression_Asset", "Generates natural depression data from local watershed geometry."),
    "generate_distance_nearest_DL": ("Dist_to_Drainage", "Dist_to_Drainage_Asset", "Computes distance to nearest drainage line from local watershed geometry."),
    "generate_catchment_area_singleflow": ("Catchment_Area", "Catchment_Area_Asset", "Generates single-flow catchment area from local watershed geometry."),
    "generate_slope_percentage": ("Slope_Percentage", "Slope_Percentage_Asset", "Generates slope percentage data from local watershed geometry."),
    "generate_mws_connectivity_data": ("MWS_Connectivity", "MWS_Connectivity_Asset", "Generates MWS connectivity data from local watershed geometry."),
    "generate_mws_centroid": ("MWS_Centroid", "MWS_Centroid_Asset", "Generates MWS centroid points from local watershed geometry."),
    "soil_type": ("Soil_Type", "Soil_Type_Asset", "Generates soil type vector data from local watershed geometry."),
}

# nodes whose implicit (no depends_on) input is admin geometry rather than
# the precomputed watershed boundary
ADMIN_ROOT_NODES = {"generate_livestocks", "generate_antyodaya", "generate_facilities_proximity"}

DEFAULT_ROOT_DATASET = "MWS_Boundaries"
ADMIN_ROOT_DATASET = "Admin_Boundary_Asset"

BASE_PARAMS = [{"name": "state", "type": "string"}, {"name": "district", "type": "string"}, {"name": "block", "type": "string"}]


def node_params(node):
    params = list(BASE_PARAMS)
    if node.get("use_global_args"):
        params += [{"name": "start_year", "type": "integer"}, {"name": "end_year", "type": "integer"}]
    if node.get("pass_gee_account_id"):
        params.append({"name": "gee_account_id", "type": "integer"})
    return params


def flatten(layer_list):
    for node in layer_list:
        yield node
        yield from flatten(node.get("children", []))


def build_graph(layer_list):
    """Returns (algorithms: {id: {...}}, datasets: set, edges: [(src, dst, type)])."""
    algorithms = {}
    datasets = set()
    edges = []
    output_of = {}

    def visit(node, parent_output):
        json_name = node["name"]
        algo_id, output_ds, description = NODE_META[json_name]
        deps = node.get("depends_on", [])
        if deps:
            inputs = [output_of[dep] for dep in deps]
        elif parent_output:
            inputs = [parent_output]
        else:
            root = ADMIN_ROOT_DATASET if json_name in ADMIN_ROOT_NODES else DEFAULT_ROOT_DATASET
            inputs = [root]
        datasets.update(inputs)
        datasets.add(output_ds)
        for src in inputs:
            edges.append((src, algo_id, "input"))
        edges.append((algo_id, output_ds, "output"))
        algorithms[algo_id] = {
            "description": description,
            "params": node_params(node),
            "input_datasets": inputs,
            "outputs": [output_ds],
        }
        output_of[json_name] = output_ds
        for child in node.get("children", []):
            visit(child, output_ds)

    for node in layer_list:
        visit(node, None)
    return algorithms, datasets, edges


def dag_parameters(algorithms):
    names = list(dict.fromkeys(p["name"] for algo in algorithms.values() for p in algo["params"]))
    order = ["state", "district", "block", "start_year", "end_year", "gee_account_id"]
    return [n for n in order if n in names]


def stac_node(node_id, node_type):
    return {"id": node_id, "type": node_type, "label": node_id}


def build_dag_json(dag_id, title, description, algorithms, datasets, edges):
    nodes = [stac_node(algo_id, "algorithm") for algo_id in algorithms] + [
        stac_node(ds_id, "dataset") for ds_id in sorted(datasets)
    ]
    return {
        "stac_version": "1.0.0",
        "stac_extensions": STAC_EXTENSIONS,
        "type": "Catalog",
        "id": f"{dag_id}_v1",
        "title": title,
        "description": description,
        "stacd:type": "dag",
        "stacd:dag_id": dag_id,
        "stacd:uuid": str(uuid.uuid4()),
        "stacd:version": "1",
        "stacd:created_at": REGISTERED_AT,
        "stacd:graph": {
            "nodes": nodes,
            "edges": [{"source": s, "target": t, "type": ty} for s, t, ty in edges],
        },
        "stacd:parameters": dag_parameters(algorithms),
        "links": [
            {"rel": "self", "href": "./dag.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "child", "href": "./algorithms/catalog.json", "type": "application/json", "title": "Algorithms"},
        ],
    }


def build_algorithm_json(dag_id, algo_id, spec):
    return {
        "stac_version": "1.0.0",
        "stac_extensions": STAC_EXTENSIONS,
        "type": "Catalog",
        "id": algo_id,
        "title": algo_id,
        "description": spec["description"],
        "stacd:type": "algorithm",
        "stacd:dag_id": dag_id,
        "stacd:dag_version": "1",
        "stacd:algo_version": "1",
        "stacd:parameters": spec["params"],
        "stacd:input_datasets": spec["input_datasets"],
        "stacd:output_datasets": spec["outputs"],
        "links": [
            {"rel": "self", "href": "./algorithm.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            {"rel": "root", "href": "../../../../catalog.json", "type": "application/json"},
            {
                "rel": "alternate",
                "href": "http://localhost:8002/datasets/catalog.json",
                "type": "application/json",
                "title": "View Output Datasets in STAC Browser",
            },
        ]
        + [
            {
                "rel": "stacd:input",
                "href": "http://localhost:8002/datasets/catalog.json",
                "type": "application/json",
                "title": f"Input Dataset: {ds} (Browse in STAC Browser)",
                "stacd:dataset_type": ds,
            }
            for ds in spec["input_datasets"]
        ]
        + [
            {
                "rel": "stacd:output",
                "href": "http://localhost:8002/datasets/catalog.json",
                "type": "application/json",
                "title": f"Output Dataset: {ds} (Browse in STAC Browser)",
                "stacd:dataset_type": ds,
            }
            for ds in spec["outputs"]
        ],
    }


def build_version_json(dag_id, algo_id, json_name):
    return {
        "stac_version": "1.0.0",
        "stac_extensions": STAC_EXTENSIONS,
        "type": "Feature",
        "id": f"{algo_id}_v1",
        "geometry": None,
        "properties": {
            "datetime": REGISTERED_AT,
            "title": f"{algo_id} v1",
            "stacd:algorithm_id": algo_id,
            "stacd:dag_id": dag_id,
            "stacd:dag_version": "1",
            "stacd:version": "1",
            "stacd:is_active": True,
            "stacd:execution_modes": {
                "api": {
                    "enabled": True,
                    "priority": 1,
                    "url": f"http://core-stack:8000/api/v1/{json_name}/",
                }
            },
            "stacd:assets": {"code": CODE_ASSET},
            "stacd:registered_at": REGISTERED_AT,
        },
        "links": [
            {"rel": "self", "href": "./v1.json", "type": "application/json"},
            {"rel": "parent", "href": "../algorithm.json", "type": "application/json"},
            {"rel": "collection", "href": "../algorithm.json", "type": "application/json"},
        ],
    }


def build_algorithms_catalog_json(dag_id, algorithms):
    return {
        "stac_version": "1.0.0",
        "stac_extensions": STAC_EXTENSIONS,
        "type": "Catalog",
        "id": f"{dag_id}_v1_algorithms",
        "title": f"Algorithms - {dag_id.replace('_', ' ')} v1",
        "description": f"All algorithms in {dag_id}_v1",
        "links": [
            {"rel": "self", "href": "./catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../dag.json", "type": "application/json"},
            {"rel": "root", "href": "../../../catalog.json", "type": "application/json"},
        ]
        + [
            {
                "rel": "child",
                "href": f"./{algo_id}/algorithm.json",
                "type": "application/json",
                "title": algo_id.replace("_", " "),
            }
            for algo_id in sorted(algorithms)
        ],
    }


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def build_dag(dag_id, title, description, layer_list, json_name_of):
    algorithms, datasets, edges = build_graph(layer_list)
    dag_dir = CATALOG_ROOT / f"{dag_id}_v1"
    write_json(dag_dir / "dag.json", build_dag_json(dag_id, title, description, algorithms, datasets, edges))
    write_json(dag_dir / "algorithms" / "catalog.json", build_algorithms_catalog_json(dag_id, algorithms))
    for algo_id, spec in algorithms.items():
        algo_dir = dag_dir / "algorithms" / algo_id
        write_json(algo_dir / "algorithm.json", build_algorithm_json(dag_id, algo_id, spec))
        write_json(algo_dir / "versions" / "v1.json", build_version_json(dag_id, algo_id, json_name_of[algo_id]))
    print(f"✓ {dag_id}_v1: {len(algorithms)} algorithms, {len(datasets)} datasets -> {dag_dir}")


def main():
    layer_map = json.loads(LAYER_MAP_PATH.read_text())

    json_name_of = {
        NODE_META[node["name"]][0]: node["name"] for node in flatten(layer_map["dynamic_layers"] + layer_map["static_layers"])
    }

    build_dag(
        "CoreStack_DAG_LocalCompute_DynamicLayers",
        "STACD Local Compute DAG - Dynamic Layers (v1)",
        "Local-compute pipeline for dynamic layers (yearly/derived layers with inter-dependencies) from local_layer_map.json.",
        layer_map["dynamic_layers"],
        json_name_of,
    )
    build_dag(
        "CoreStack_DAG_LocalCompute_StaticLayers",
        "STACD Local Compute DAG - Static Layers (v1)",
        "Local-compute pipeline for static layers (independent, dependency-free layers) from local_layer_map.json.",
        layer_map["static_layers"],
        json_name_of,
    )


if __name__ == "__main__":
    main()
