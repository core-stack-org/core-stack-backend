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
LULC_SLOPE_CLUSTER_OUTPUT_DIR: Path = _abs(_output_entry("lulc_x_terrain", 0)["path"])
AQUIFER_VECTOR_OUTPUT_DIR: Path = _abs(_output_entry("misc", 0)["path"])
