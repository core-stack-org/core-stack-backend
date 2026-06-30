from pathlib import Path

import yaml

_CONFIG_PATH = Path(__file__).resolve().parent / "config_new.yaml"
_LEGACY_CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f) or {}


_cfg = _load(_CONFIG_PATH)
_legacy_cfg = _load(_LEGACY_CONFIG_PATH)


def _abs(rel_path: str) -> Path:
    base = rel_path.split("{")[0].rstrip("/")
    return PROJECT_ROOT / base


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


def _manifest_base_layers() -> list[dict]:
    base_layers = _cfg.get("base_layers", {})
    layers = []
    layers.extend(base_layers.get("static_layers", []))
    layers.extend(base_layers.get("on_demand_layers", []))
    for layer in base_layers.get("periodic_layers", []):
        layers.extend(_expand_periodic_layer(layer))
    return layers


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
        local_path = Path(layer["local_path"])
        if not allowed_suffixes or local_path.suffix.lower() in allowed_suffixes:
            return PROJECT_ROOT / local_path
    if fallback:
        return PROJECT_ROOT / fallback
    raise KeyError(f"No local_path found in {_CONFIG_PATH.name} for base layer: {name}")


def _find_legacy_input(path_suffix: str) -> dict:
    for item in _legacy_cfg["base_layers"]["inputs"]:
        if item["path"] == path_suffix:
            return item
    raise KeyError(f"No base layer input found in config.yaml for path: {path_suffix}")


def _derived_layer(name: str) -> dict | None:
    key = _layer_key(name)
    for layer in _cfg.get("derived_layers", []):
        if _layer_matches(layer, key) and layer.get("local_path"):
            return layer
    return None


def _derived_output_dir(
    name: str,
    legacy_module: str,
    legacy_index: int = 0,
) -> Path:
    layer = _derived_layer(name)
    if layer:
        return _abs(layer["local_path"])
    return _abs(_legacy_output_entry(legacy_module, legacy_index)["path"])


def _legacy_output_entry(module: str, index: int = 0) -> dict:
    return _legacy_cfg["local_compute_outputs"][module][index]


# ---------------------------------------------------------------------------
# Input paths
# ---------------------------------------------------------------------------

LULC_BASE_DIR: Path = _abs(
    next(
        layer["local_path"]
        for layer in _manifest_base_layers()
        if layer["local_path"].startswith("data/base_layers/lulc/")
    )
).parent

TERRAIN_RASTER_PATH: Path = _base_layer_path("terrain")

AEZ_VECTOR_PATH: Path = _base_layer_path("aez")

PRECOMPUTED_TEHSIL_WATERSHED_DIR: Path = _abs(
    _find_legacy_input("data/base_layers/tehsil_watersheds/")["path"]
)

MICROWATERSHED_PATH: Path = _base_layer_path("mws")

AQUIFER_VECTOR_PATH: Path = _base_layer_path(
    "aquifer",
    fallback="data/base_layers/Aquifer_vector.geojson",
    allowed_suffixes=(".geojson", ".gpkg", ".shp"),
)

SWB_VECTOR_PATH: Path = _base_layer_path(
    "surface water bodies",
    fallback="data/base_layers/pan_india_waterbodies.geojson",
)

SOI_TEHSIL_PATH: Path = PROJECT_ROOT / _find_legacy_input(
    "data/admin-boundary/input/soi_tehsil.geojson"
)["path"]

ADMIN_BOUNDARY_INPUT_DIR: Path = PROJECT_ROOT / "data/admin-boundary/input"
ADMIN_BOUNDARY_OUTPUT_DIR: Path = PROJECT_ROOT / "data/admin-boundary/output"
VILLAGE_BOUNDARIES_DIR: Path = PROJECT_ROOT / "data/base_layers/village_boundaries"

# ---------------------------------------------------------------------------
# Google Drive IDs
# ---------------------------------------------------------------------------

GDRIVE_ADMIN_BOUNDARY_FILE_ID: str = _find_legacy_input(
    "data/admin-boundary/input/"
)["gdrive_id"]
GDRIVE_MICROWATERSHED_FILE_ID: str = _find_legacy_input(
    "data/base_layers/Microwatershed_v2_with_details.geojson"
)["gdrive_id"]

LULC_GDRIVE_FILES: list[tuple[str, str]] = [
    (Path(item["path"]).name, item["gdrive_id"])
    for item in _legacy_cfg["base_layers"]["inputs"]
    if item["path"].startswith("data/base_layers/lulc/")
    and item.get("source") == "google_drive"
]

# ---------------------------------------------------------------------------
# Output base directories
# ---------------------------------------------------------------------------

CHANGE_DETECTION_RASTER_OUTPUT_DIR: Path = _derived_output_dir(
    "change detection", "change_detection", 0
)
CHANGE_DETECTION_VECTOR_OUTPUT_DIR: Path = _derived_output_dir(
    "change detection vector", "change_detection", 1
)
LULC_VECTOR_OUTPUT_DIR: Path = _derived_output_dir("lulc vector", "lulc", 0)
LULC_V3_OUTPUT_DIR: Path = _derived_output_dir("lulc v3", "lulc", 1)
LULC_SLOPE_CLUSTER_OUTPUT_DIR: Path = _derived_output_dir(
    "lulc slope clusters", "lulc_x_terrain", 0
)
LULC_PLAIN_CLUSTER_OUTPUT_DIR: Path = _derived_output_dir(
    "lulc plain clusters", "lulc_x_terrain", 1
)
AQUIFER_VECTOR_OUTPUT_DIR: Path = _derived_output_dir("aquifer vector", "misc", 0)
SWB_VECTOR_OUTPUT_DIR: Path = _abs(
    _legacy_output_entry("surface_water_bodies", 0)["path"]
)


PAN_INDIA_DRAINAGE_LINES_GPKG_PATH = (
    PROJECT_ROOT / "data/base_layers/drainage_lines_pan_india.gpkg"
)

PAN_INDIA_DRAINAGE_LINES_PATH = PROJECT_ROOT / "data/layers/drainage_lines/Pan_India_drainage_lines.gpkg"
LOCAL_DRAINAGE_LINES_OUTPUT = PROJECT_ROOT / "data/layers/drainage_lines/drainage_lines_local"

LOCAL_DRAINAGE_DENSITY_OUTPUT = PROJECT_ROOT / "data/drainage_density"

PAN_INDIA_CANAL_PATH = _base_layer_path(
    "canal", fallback="data/canal/Canal_pan_india.geojson"
)
LOCAL_CANAL_OUTPUT = PROJECT_ROOT / "data/canal/canal_local"

PAN_INDIA_AGROECOLOGICAL_PATH = _base_layer_path(
    "aez",
    fallback="data/base_layers/Pan_India_agroecological_farming.geojson",
)
LOCAL_AGROECOLOGICAL_OUTPUT = PROJECT_ROOT / "data/layers/agroecological"

PAN_INDIA_LCW_PATH = _base_layer_path(
    "lcw", fallback="data/base_layers/Pan_India_lcw_conflict.geojson"
)
LOCAL_LCW_OUTPUT = PROJECT_ROOT / "data/layers/lcw_conflict"

PAN_INDIA_SOGE_PATH = _base_layer_path(
    "soge", fallback="data/base_layers/Pan_India_SOGE_2020.geojson"
)
LOCAL_SOGE_OUTPUT = PROJECT_ROOT / "data/layers/SOGE_vector"

PAN_INDIA_FACTORY_CSR_PATH = _base_layer_path(
    "factory csr", fallback="data/base_layers/Pan_India_factory_csr.geojson"
)
LOCAL_FACTORY_CSR_OUTPUT = PROJECT_ROOT / "data/layers/factory_csr"

PAN_INDIA_GREEN_CREDIT_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_green_credit.geojson"
LOCAL_GREEN_CREDIT_OUTPUT = PROJECT_ROOT / "data/layers/green_credit"

PAN_INDIA_MINING_PATH = _base_layer_path(
    "mining", fallback="data/base_layers/Pan_India_mining.geojson"
)
LOCAL_MINING_OUTPUT = PROJECT_ROOT / "data/layers/mining"

PAN_INDIA_NATURALDEPRESSION_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_natural_depression.tif"
LOCAL_NATURALDEPRESSION_OUTPUT = PROJECT_ROOT / "data/layers/natural_depression"

PAN_INDIA_DISTANCETONEARESTDRAINAGE_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_distance_to_nearest_drainage.tif"
LOCAL_DISTANCETONEARESTDRAINAGE_OUTPUT = PROJECT_ROOT / "data/layers/distance_nearest_upstream_DL"

PAN_INDIA_FACILITIES_PATH = _base_layer_path(
    "facilities", fallback="data/base_layers/Pan_India_facilities_polygon.geojson"
)
LOCAL_FACILITIES_OUTPUT = PROJECT_ROOT / "data/layers/facilities"
PAN_INDIA_CATCHMENT_AREA_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_catchment_area.tif"
LOCAL_CATCHMENT_AREA_OUTPUT = PROJECT_ROOT / "data/layers/catchment_area_singleflow"

PAN_INDIA_SLOPE_PERCENTAGE_PATH = _base_layer_path(
    "slope percentage", fallback="data/base_layers/Pan_India_slope_percentage.tif"
)
LOCAL_SLOPE_PERCENTAGE_OUTPUT = PROJECT_ROOT / "data/layers/slope_percentage"

PAN_INDIA_MWS_CONNECTIVITY_PATH = PROJECT_ROOT / "data/layers/mws_connectivity/Pan_India_mws_connectivity.geojson"
LOCAL_MWS_CONNECTIVITY_OUTPUT = PROJECT_ROOT / "data/layers/mws_connectivity/mws_connectivity_local"

LOCAL_MWS_CENTROID_OUTPUT = PROJECT_ROOT / "data/layers/mws_centroid"

NREGA_LOCAL_OUTPUT = PROJECT_ROOT / "data/layers/nrega_assets"
PAN_INDIA_RESTORATION_PATH = _base_layer_path(
    "restoration opportunity",
    fallback="data/base_layers/Pan_India_WRI_Restoration.tif",
)
LOCAL_RESTORATION_OUTPUT = PROJECT_ROOT / "data/layers/restoration_opportunity"

PAN_INDIA_RIVER_PATH = _base_layer_path(
    "river", fallback="data/river/River_pan_india.geojson"
)
LOCAL_RIVER_OUTPUT = PROJECT_ROOT / "data/river/river_local"

PAN_INDIA_FABDEM_PATH = _base_layer_path("dem", fallback="data/fabdem/fabdem_pan_india.tif")
LOCAL_FABDEM_OUTPUT = PROJECT_ROOT / "data/fabdem/fabdem_local"

PAN_INDIA_ANTYODAYA_2020 = PROJECT_ROOT / "data/base_layers/pan_india_antyodaya_2020.gpkg"
LOCAL_ANTYODAYA_2020_OUTPUT = PROJECT_ROOT / "data/antyodaya/output/antyodaya_local"

PAN_INDIA_LIVESTOCKS = PROJECT_ROOT / "data/base_layers/pan_india_livestock.gpkg"
LOCAL_LIVESTOCKS_OUTPUT = PROJECT_ROOT / "data/livestock/output/livestock_local"
