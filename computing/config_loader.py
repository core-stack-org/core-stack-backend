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


PRECOMPUTED_TEHSIL_WATERSHED_DIR: Path = _abs(
    _find_input("data/base_layers/tehsil_watersheds/")["path"]
)

SWB_VECTOR_PATH: Path = PROJECT_ROOT / _find_input(
    "data/base_layers/pan_india_waterbodies.geojson"
)["path"]

SWB_VECTOR_OUTPUT_DIR: Path = _abs(
    _output_entry("surface_water_bodies", 0)["path"]
)
