import os

try:
    from django.conf import settings
except ModuleNotFoundError:
    settings = None

from computing.config_loader import (
    LULC_BASE_DIR,
    PROJECT_ROOT,
    SOIL_RASTER_PATH,
    TERRAIN_RASTER_PATH,
)


def _env_or_setting(name, default=""):
    value = None
    if settings is not None:
        try:
            value = getattr(settings, name, None)
        except Exception:
            value = None
    if value in (None, ""):
        value = os.environ.get(name, default)
    return str(value or "").strip()


DATA_ROOT = PROJECT_ROOT / "data"

# Optional. Do not commit a real project id; use env/settings when needed.
GEE_PROJECT_NAME = _env_or_setting("GEE_PROJECT_NAME")
GEE_SCALE = 30

# API/Celery wrappers set these per request before running hydrology.
BOUNDARY_GEOJSON_PATH = ""
MICROWATERSHEDS_PATH = BOUNDARY_GEOJSON_PATH
DEMFILE_PATH = ""
SOIL_PATH = str(SOIL_RASTER_PATH)
LULC_PATH = str(LULC_BASE_DIR / "lulc_v3_2024_2025.tif")
LULC_SOURCE = "indiasatv3"
INDIASATV3_LULC_PATH = LULC_PATH

RAINFALL_FOLDER = ""
RUNOFFS_FOLDER = ""
TIMESERIES_VECTOR = ""

ARG_START_DATE = "2017-07-01"
ARG_END_DATE = "2025-06-18"
TILE_SIZE = None
