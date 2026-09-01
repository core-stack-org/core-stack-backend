import os
from pathlib import Path

import yaml

_CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Root directory for all downloaded/generated local compute data. Defaults to
# `<repo>/data` for local development, but should be set to a directory
# mounted from the host (e.g. a Docker volume) in containerized deployments.
DATA_DIR: Path = (
    Path(os.environ.get("DATA_DIR", str(PROJECT_ROOT / "data"))).expanduser().resolve()
)


def _load(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f) or {}


_cfg = _load(_CONFIG_PATH)


def _abs(rel_path: str) -> Path:
    resolved = rel_path.replace("{DATA_DIR}", str(DATA_DIR))
    base = resolved.split("{")[0].rstrip("/")
    return Path(base)


def _layer_key(name: str) -> str:
    return name.strip().lower().replace(" ", "_").replace("-", "_")


def _layer_matches(layer: dict, key: str) -> bool:
    names = [layer.get("name", ""), *layer.get("aliases", [])]
    return key in {_layer_key(name) for name in names}


def _format_periodic_value(template: str, year: int) -> str:
    return template.replace("{year+1}", str(year + 1)).replace("{year}", str(year))


def _expand_periodic_layer(layer: dict) -> list[dict]:
    if not layer.get("periodicity"):
        return [layer]

    if layer.get("periodicity") != "annual":
        raise ValueError(
            f"Unsupported periodicity for base layer '{layer.get('name')}': "
            f"{layer.get('periodicity')}"
        )

    expanded_layers = []
    for year in range(int(layer["start_year"]), int(layer["end_year"]) + 1):
        filename = _format_periodic_value(layer["filename"], year)
        expanded = dict(layer)
        expanded["year"] = year
        expanded["filename"] = filename
        expanded["local_path"] = _format_periodic_value(
            layer["local_path"].replace("{filename}", filename), year
        )
        source = layer.get("source")
        if source:
            expanded["source"] = _format_periodic_value(
                source.replace("{filename}", filename), year
            )
        expanded_layers.append(expanded)

    return expanded_layers


def _expand_manifest_layers(layers: list[dict]) -> list[dict]:
    expanded_layers = []
    for layer in layers:
        expanded_layers.extend(_expand_periodic_layer(layer))
    return expanded_layers


def _walk_manifest_layers(node):
    if isinstance(node, list):
        yield from _expand_manifest_layers(node)
        return

    if not isinstance(node, dict):
        return

    for value in node.values():
        yield from _walk_manifest_layers(value)


def _manifest_base_layers() -> list[dict]:
    base_layers = _cfg.get("base_layers", {})
    return list(_walk_manifest_layers(base_layers))


def _base_layer(name: str, *, required: bool = True) -> dict | None:
    key = _layer_key(name)
    for layer in _manifest_base_layers():
        if _layer_matches(layer, key):
            return layer
    if required:
        raise KeyError(f"No base layer found in {_CONFIG_PATH.name} for name: {name}")
    return None


def _base_layer_path(
    name: str,
    fallback: str | None = None,
    allowed_suffixes: tuple[str, ...] | None = None,
) -> Path:
    layer = _base_layer(name, required=False)
    if layer and layer.get("local_path"):
        local_path = _abs(layer["local_path"])
        if not allowed_suffixes or local_path.suffix.lower() in allowed_suffixes:
            return local_path
    if fallback:
        return _abs(fallback)
    raise KeyError(f"No local_path found in {_CONFIG_PATH.name} for base layer: {name}")


def _derived_layer(name: str) -> dict | None:
    key = _layer_key(name)
    for layer in _cfg.get("derived_layers", []):
        if _layer_matches(layer, key) and layer.get("local_path"):
            return layer
    return None


def _derived_output_dir(name: str) -> Path:
    layer = _derived_layer(name)
    if not layer:
        raise KeyError(
            f"No derived layer found in {_CONFIG_PATH.name} for name: {name}"
        )
    return _abs(layer["local_path"])


# ---------------------------------------------------------------------------
# Input paths
# ---------------------------------------------------------------------------

LULC_BASE_DIR: Path = _abs(
    next(
        layer["local_path"]
        for layer in _manifest_base_layers()
        if layer["local_path"].startswith("{DATA_DIR}/base_layers/lulc/")
    )
).parent

TERRAIN_RASTER_PATH: Path = _base_layer_path("terrain")

SOIL_RASTER_PATH: Path = _base_layer_path(
    "hydrological soil group",
    allowed_suffixes=(".tif", ".tiff"),
)

AEZ_VECTOR_PATH: Path = _base_layer_path("aez")

PRECOMPUTED_TEHSIL_WATERSHED_DIR: Path = DATA_DIR / "base_layers/tehsil_watersheds"

MICROWATERSHED_PATH: Path = _base_layer_path("mws")

AQUIFER_VECTOR_PATH: Path = _base_layer_path(
    "aquifer",
    fallback="{DATA_DIR}/base_layers/Aquifer_vector.geojson",
    allowed_suffixes=(".geojson", ".gpkg", ".shp"),
)

SWB_VECTOR_PATH: Path = _base_layer_path(
    "surface water bodies",
    fallback="{DATA_DIR}/base_layers/pan_india_waterbodies.geojson",
)

SOI_TEHSIL_PATH: Path = _base_layer_path("admin boundaries")

SOIL_TYPE_BASE_DIR: Path = _base_layer_path("soil type")
SOIL_TYPE_RASTER_PATHS: dict[str, Path] = {
    "available_water_capacity": SOIL_TYPE_BASE_DIR / "available_water_capacity.tif",
    "soil_drainage_classes": SOIL_TYPE_BASE_DIR / "soil_drainage_classes.tif",
    "subsoil_bulk_density": SOIL_TYPE_BASE_DIR / "subsoil_bulk_density.tif",
    "subsoil_exchange_capacity": SOIL_TYPE_BASE_DIR / "subsoil_exchange_capacity.tif",
    "subsoil_organic_carbon": SOIL_TYPE_BASE_DIR / "subsoil_organic_carbon.tif",
    "subsoil_ph": SOIL_TYPE_BASE_DIR / "subsoil_pH.tif",
    "subsoil_texture": SOIL_TYPE_BASE_DIR / "subsoil_texture.tif",
    "topsoil_bulk_density": SOIL_TYPE_BASE_DIR / "topsoil_bulk_density.tif",
    "topsoil_exchange_capacity": SOIL_TYPE_BASE_DIR / "topsoil_exchange_capacity.tif",
    "topsoil_organic_carbon": SOIL_TYPE_BASE_DIR / "topsoil_organic_carbon.tif",
    "topsoil_ph": SOIL_TYPE_BASE_DIR / "topsoil_pH.tif",
    "topsoil_texture": SOIL_TYPE_BASE_DIR / "topsoil_texture.tif",
}

ADMIN_BOUNDARY_INPUT_DIR: Path = DATA_DIR / "admin-boundary/input"
ADMIN_BOUNDARY_OUTPUT_DIR: Path = DATA_DIR / "admin-boundary/output"
VILLAGE_BOUNDARIES_DIR: Path = DATA_DIR / "base_layers/village_boundaries"

# ---------------------------------------------------------------------------
# Google Drive IDs
# ---------------------------------------------------------------------------

GDRIVE_ADMIN_BOUNDARY_FILE_ID: str = _base_layer("admin boundaries").get(
    "gdrive_id", ""
)

# MWS boundaries are now sourced from S3 (see config.yaml); no Google Drive
# file ID is configured for it anymore.
_mws_layer = _base_layer("mws", required=False) or {}
GDRIVE_MICROWATERSHED_FILE_ID: str = _mws_layer.get("gdrive_id", "")

# LULC rasters are now sourced from S3 as periodic base layers (see
# config.yaml); no per-year Google Drive file IDs are configured anymore.
LULC_GDRIVE_FILES: list[tuple[str, str]] = []

# ---------------------------------------------------------------------------
# Output base directories
# ---------------------------------------------------------------------------

CHANGE_DETECTION_RASTER_OUTPUT_DIR: Path = _derived_output_dir("change detection")
CHANGE_DETECTION_VECTOR_OUTPUT_DIR: Path = _derived_output_dir(
    "change detection vector"
)
LULC_VECTOR_OUTPUT_DIR: Path = _derived_output_dir("lulc vector")
LULC_V3_OUTPUT_DIR: Path = _derived_output_dir("lulc v3")
LULC_SLOPE_CLUSTER_OUTPUT_DIR: Path = _derived_output_dir("lulc slope clusters")
LULC_PLAIN_CLUSTER_OUTPUT_DIR: Path = _derived_output_dir("lulc plain clusters")
AQUIFER_VECTOR_OUTPUT_DIR: Path = _derived_output_dir("aquifer vector")
SWB_VECTOR_OUTPUT_DIR: Path = _derived_output_dir("generate_swb")
SOIL_TYPE_OUTPUT_DIR: Path = _derived_output_dir("soil type")
HYDROLOGY_LOCAL_OUTPUT_DIR: Path = _derived_output_dir("hydrology_fortnightly")

PAN_INDIA_DRAINAGE_LINES_GPKG_PATH = (
    DATA_DIR / "base_layers/drainage_lines_pan_india.gpkg"
)

PAN_INDIA_DRAINAGE_LINES_PATH = (
    DATA_DIR / "layers/drainage_lines/Pan_India_drainage_lines.gpkg"
)
LOCAL_DRAINAGE_LINES_OUTPUT = DATA_DIR / "layers/drainage_lines/drainage_lines_local"

LOCAL_DRAINAGE_DENSITY_OUTPUT = DATA_DIR / "drainage_density"

PAN_INDIA_CANAL_PATH = _base_layer_path(
    "canal", fallback="{DATA_DIR}/canal/Canal_pan_india.geojson"
)
LOCAL_CANAL_OUTPUT = DATA_DIR / "canal/canal_local"

PAN_INDIA_AGROECOLOGICAL_PATH = _base_layer_path(
    "aez",
    fallback="{DATA_DIR}/base_layers/Pan_India_agroecological_farming.geojson",
)
LOCAL_AGROECOLOGICAL_OUTPUT = DATA_DIR / "layers/agroecological"

PAN_INDIA_LCW_PATH = _base_layer_path(
    "lcw", fallback="{DATA_DIR}/base_layers/Pan_India_lcw_conflict.geojson"
)
LOCAL_LCW_OUTPUT = DATA_DIR / "layers/lcw_conflict"

PAN_INDIA_SOGE_PATH = _base_layer_path(
    "soge", fallback="{DATA_DIR}/base_layers/Pan_India_SOGE_2020.geojson"
)
LOCAL_SOGE_OUTPUT = DATA_DIR / "layers/SOGE_vector"

PAN_INDIA_FACTORY_CSR_PATH = _base_layer_path(
    "factory csr", fallback="{DATA_DIR}/base_layers/Pan_India_factory_csr.geojson"
)
LOCAL_FACTORY_CSR_OUTPUT = DATA_DIR / "layers/factory_csr"

PAN_INDIA_GREEN_CREDIT_PATH = DATA_DIR / "base_layers/Pan_India_green_credit.geojson"
LOCAL_GREEN_CREDIT_OUTPUT = DATA_DIR / "layers/green_credit"

PAN_INDIA_MINING_PATH = _base_layer_path(
    "mining", fallback="{DATA_DIR}/base_layers/Pan_India_mining.geojson"
)
LOCAL_MINING_OUTPUT = DATA_DIR / "layers/mining"

PAN_INDIA_NATURALDEPRESSION_PATH = (
    DATA_DIR / "base_layers/Pan_India_natural_depression.tif"
)
LOCAL_NATURALDEPRESSION_OUTPUT = DATA_DIR / "layers/natural_depression"

PAN_INDIA_DISTANCETONEARESTDRAINAGE_PATH = (
    DATA_DIR / "base_layers/Pan_India_distance_to_nearest_drainage.tif"
)
LOCAL_DISTANCETONEARESTDRAINAGE_OUTPUT = (
    DATA_DIR / "layers/distance_nearest_upstream_DL"
)

PAN_INDIA_FACILITIES_PATH = _base_layer_path(
    "facilities", fallback="{DATA_DIR}/base_layers/Pan_India_facilities_polygon.geojson"
)
LOCAL_FACILITIES_OUTPUT = DATA_DIR / "layers/facilities"
PAN_INDIA_CATCHMENT_AREA_PATH = DATA_DIR / "base_layers/Pan_India_catchment_area.tif"
LOCAL_CATCHMENT_AREA_OUTPUT = DATA_DIR / "layers/catchment_area_singleflow"

PAN_INDIA_SLOPE_PERCENTAGE_PATH = _base_layer_path(
    "slope percentage", fallback="{DATA_DIR}/base_layers/Pan_India_slope_percentage.tif"
)
LOCAL_SLOPE_PERCENTAGE_OUTPUT = DATA_DIR / "layers/slope_percentage"

PAN_INDIA_MWS_CONNECTIVITY_PATH = (
    DATA_DIR / "layers/mws_connectivity/Pan_India_mws_connectivity.geojson"
)
LOCAL_MWS_CONNECTIVITY_OUTPUT = (
    DATA_DIR / "layers/mws_connectivity/mws_connectivity_local"
)

LOCAL_MWS_CENTROID_OUTPUT = DATA_DIR / "layers/mws_centroid"

NREGA_LOCAL_OUTPUT = DATA_DIR / "layers/nrega_assets"
PAN_INDIA_RESTORATION_PATH = _base_layer_path(
    "restoration opportunity",
    fallback="{DATA_DIR}/base_layers/Pan_India_WRI_Restoration.tif",
)
LOCAL_RESTORATION_OUTPUT = DATA_DIR / "layers/restoration_opportunity"

PAN_INDIA_RIVER_PATH = _base_layer_path(
    "river", fallback="{DATA_DIR}/river/River_pan_india.geojson"
)
LOCAL_RIVER_OUTPUT = DATA_DIR / "river/river_local"

PAN_INDIA_FABDEM_PATH = _base_layer_path(
    "dem", fallback="{DATA_DIR}/fabdem/fabdem_pan_india.tif"
)
LOCAL_FABDEM_OUTPUT = DATA_DIR / "fabdem/fabdem_local"

PAN_INDIA_ANTYODAYA_2020 = DATA_DIR / "base_layers/pan_india_antyodaya_2020.gpkg"
LOCAL_ANTYODAYA_2020_OUTPUT = DATA_DIR / "antyodaya/output/antyodaya_local"

PAN_INDIA_LIVESTOCKS = DATA_DIR / "base_layers/pan_india_livestock.gpkg"
LOCAL_LIVESTOCKS_OUTPUT = DATA_DIR / "livestock/output/livestock_local"


PAN_INDIA_FOREST_FRINGE = (
    DATA_DIR / "forest_fringe/input/forest_fringe_degradation.geojson"
)
LOCAL_FOREST_FRINGE_OUTPUT = DATA_DIR / "forest_fringe/output"
PAN_INDIA_TREE_IN_GRASSLAND = (
    DATA_DIR / "tree_in_grassland/input/tree_in_grassland.geojson"
)
LOCAL_TREE_IN_GRASSLAND_OUTPUT = DATA_DIR / "tree_in_grassland/output"
