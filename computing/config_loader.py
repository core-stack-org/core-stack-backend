from pathlib import Path

import yaml

_CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load():
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f)


_cfg = _load()


def _abs(rel_path: str) -> Path:
    base = rel_path.split("{")[0].rstrip("/")
    return PROJECT_ROOT / base


def _find_input(path_suffix: str) -> dict:
    for item in _cfg["base_layers"]["inputs"]:
        if item["path"] == path_suffix:
            return item
    raise KeyError(f"No base layer input found in config.yaml for path: {path_suffix}")


def _output_entry(module: str, index: int = 0) -> dict:
    return _cfg["local_compute_outputs"][module][index]


# ---------------------------------------------------------------------------
# Input paths
# ---------------------------------------------------------------------------

LULC_BASE_DIR: Path = _abs(
    next(
        item["path"]
        for item in _cfg["base_layers"]["inputs"]
        if item["path"].startswith("data/base_layers/lulc/")
    )
).parent

TERRAIN_RASTER_PATH: Path = PROJECT_ROOT / _find_input(
    "data/base_layers/terrain_raster_fabdam_pan_india.tif"
)["path"]

AEZ_VECTOR_PATH: Path = PROJECT_ROOT / _find_input(
    "data/base_layers/AEZs/Agro_Ecological_Regions.shp"
)["path"]

PRECOMPUTED_TEHSIL_WATERSHED_DIR: Path = _abs(
    _find_input("data/base_layers/tehsil_watersheds/")["path"]
)

MICROWATERSHED_PATH: Path = PROJECT_ROOT / _find_input(
    "data/base_layers/Microwatershed_v2_with_details.geojson"
)["path"]

AQUIFER_VECTOR_PATH: Path = PROJECT_ROOT / _find_input(
    "data/base_layers/Aquifer_vector.geojson"
)["path"]

SWB_VECTOR_PATH: Path = PROJECT_ROOT / _find_input(
    "data/base_layers/pan_india_waterbodies.geojson"
)["path"]

SOI_TEHSIL_PATH: Path = PROJECT_ROOT / _find_input(
    "data/admin-boundary/input/soi_tehsil.geojson"
)["path"]

ADMIN_BOUNDARY_INPUT_DIR: Path = PROJECT_ROOT / "data/admin-boundary/input"
ADMIN_BOUNDARY_OUTPUT_DIR: Path = PROJECT_ROOT / "data/admin-boundary/output"
VILLAGE_BOUNDARIES_DIR: Path = PROJECT_ROOT / "data/base_layers/village_boundaries"

# ---------------------------------------------------------------------------
# Google Drive IDs
# ---------------------------------------------------------------------------

GDRIVE_ADMIN_BOUNDARY_FILE_ID: str = _find_input("data/admin-boundary/input/")["gdrive_id"]
GDRIVE_MICROWATERSHED_FILE_ID: str = _find_input(
    "data/base_layers/Microwatershed_v2_with_details.geojson"
)["gdrive_id"]

LULC_GDRIVE_FILES: list[tuple[str, str]] = [
    (Path(item["path"]).name, item["gdrive_id"])
    for item in _cfg["base_layers"]["inputs"]
    if item["path"].startswith("data/base_layers/lulc/") and item.get("source") == "google_drive"
]

# ---------------------------------------------------------------------------
# Output base directories
# ---------------------------------------------------------------------------

CHANGE_DETECTION_RASTER_OUTPUT_DIR: Path = _abs(_output_entry("change_detection", 0)["path"])
CHANGE_DETECTION_VECTOR_OUTPUT_DIR: Path = _abs(_output_entry("change_detection", 1)["path"])
LULC_VECTOR_OUTPUT_DIR: Path = _abs(_output_entry("lulc", 0)["path"])
LULC_V3_OUTPUT_DIR: Path = _abs(_output_entry("lulc", 1)["path"])
LULC_SLOPE_CLUSTER_OUTPUT_DIR: Path = _abs(_output_entry("lulc_x_terrain", 0)["path"])
LULC_PLAIN_CLUSTER_OUTPUT_DIR: Path = _abs(_output_entry("lulc_x_terrain", 1)["path"])
AQUIFER_VECTOR_OUTPUT_DIR: Path = _abs(_output_entry("misc", 0)["path"])
SWB_VECTOR_OUTPUT_DIR: Path = _abs(
    _output_entry("surface_water_bodies", 0)["path"]
)


PAN_INDIA_DRAINAGE_LINES_GPKG_PATH = (
    PROJECT_ROOT / "data/base_layers/drainage_lines_pan_india.gpkg"
)

PAN_INDIA_DRAINAGE_LINES_PATH = PROJECT_ROOT / "data/layers/drainage_lines/Pan_India_drainage_lines.gpkg"
LOCAL_DRAINAGE_LINES_OUTPUT = PROJECT_ROOT / "data/layers/drainage_lines/drainage_lines_local"

LOCAL_DRAINAGE_DENSITY_OUTPUT = PROJECT_ROOT / "data/drainage_density"

PAN_INDIA_CANAL_PATH = PROJECT_ROOT / "data/canal/Canal_pan_india.geojson"
LOCAL_CANAL_OUTPUT = PROJECT_ROOT / "data/canal/canal_local"

PAN_INDIA_AGROECOLOGICAL_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_agroecological_farming.geojson"
LOCAL_AGROECOLOGICAL_OUTPUT = PROJECT_ROOT / "data/layers/agroecological"

PAN_INDIA_LCW_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_lcw_conflict.geojson"
LOCAL_LCW_OUTPUT = PROJECT_ROOT / "data/layers/lcw_conflict"

PAN_INDIA_SOGE_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_SOGE_2020.geojson"
LOCAL_SOGE_OUTPUT = PROJECT_ROOT / "data/layers/SOGE_vector"

PAN_INDIA_FACTORY_CSR_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_factory_csr.geojson"
LOCAL_FACTORY_CSR_OUTPUT = PROJECT_ROOT / "data/layers/factory_csr"

PAN_INDIA_GREEN_CREDIT_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_green_credit.geojson"
LOCAL_GREEN_CREDIT_OUTPUT = PROJECT_ROOT / "data/layers/green_credit"

PAN_INDIA_MINING_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_mining.geojson"
LOCAL_MINING_OUTPUT = PROJECT_ROOT / "data/layers/mining"

PAN_INDIA_NATURALDEPRESSION_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_natural_depression.tif"
LOCAL_NATURALDEPRESSION_OUTPUT = PROJECT_ROOT / "data/layers/natural_depression"

PAN_INDIA_DISTANCETONEARESTDRAINAGE_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_distance_to_nearest_drainage.tif"
LOCAL_DISTANCETONEARESTDRAINAGE_OUTPUT = PROJECT_ROOT / "data/layers/distance_nearest_upstream_DL"

PAN_INDIA_FACILITIES_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_facilities_polygon.geojson"
LOCAL_FACILITIES_OUTPUT = PROJECT_ROOT / "data/layers/facilities"
PAN_INDIA_CATCHMENT_AREA_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_catchment_area.tif"
LOCAL_CATCHMENT_AREA_OUTPUT = PROJECT_ROOT / "data/layers/catchment_area_singleflow"

PAN_INDIA_SLOPE_PERCENTAGE_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_slope_percentage.tif"
LOCAL_SLOPE_PERCENTAGE_OUTPUT = PROJECT_ROOT / "data/layers/slope_percentage"

PAN_INDIA_MWS_CONNECTIVITY_PATH = PROJECT_ROOT / "data/layers/mws_connectivity/Pan_India_mws_connectivity.geojson"
LOCAL_MWS_CONNECTIVITY_OUTPUT = PROJECT_ROOT / "data/layers/mws_connectivity/mws_connectivity_local"

LOCAL_MWS_CENTROID_OUTPUT = PROJECT_ROOT / "data/layers/mws_centroid"

NREGA_LOCAL_OUTPUT = PROJECT_ROOT / "data/layers/nrega_assets"
PAN_INDIA_RESTORATION_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_WRI_Restoration.tif"
LOCAL_RESTORATION_OUTPUT = PROJECT_ROOT / "data/layers/restoration_opportunity"

PAN_INDIA_RIVER_PATH = PROJECT_ROOT / "data/river/River_pan_india.geojson"
LOCAL_RIVER_OUTPUT = PROJECT_ROOT / "data/river/river_local"

PAN_INDIA_FABDEM_PATH = PROJECT_ROOT / "data/fabdem/fabdem_pan_india.tif"
LOCAL_FABDEM_OUTPUT = PROJECT_ROOT / "data/fabdem/fabdem_local"

PAN_INDIA_ANTYODAYA_2020 = PROJECT_ROOT / "data/base_layers/pan_india_antyodaya_2020.gpkg"
LOCAL_ANTYODAYA_2020_OUTPUT = PROJECT_ROOT / "data/antyodaya/output/antyodaya_local"