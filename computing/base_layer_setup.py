import logging
import subprocess
from pathlib import Path

import requests
from django.conf import settings

from utilities.constants import GEOSERVER_BASE

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(settings.BASE_DIR)

SOI_TEHSIL_PATH = _PROJECT_ROOT / "data/admin-boundary/input/soi_tehsil.geojson"
ADMIN_BOUNDARY_INPUT_DIR = _PROJECT_ROOT / "data/admin-boundary/input"
ADMIN_BOUNDARY_OUTPUT_DIR = _PROJECT_ROOT / "data/admin-boundary/output"
LULC_DIR = _PROJECT_ROOT / "data/base_layers/lulc"
VILLAGE_BOUNDARIES_DIR = _PROJECT_ROOT / "data/base_layers/village_boundaries"
TEHSIL_WATERSHEDS_DIR = _PROJECT_ROOT / "data/base_layers/tehsil_watersheds"

_GDRIVE_ADMIN_BOUNDARY_FILE_ID = "1VqIhB6HrKFDkDnlk1vedcEHhh5fk4f1d"

# Ordered oldest → newest; each file is ~7-8 GB.
_LULC_GDRIVE_FILES = [
    ("lulc_v3_2017_2018.tif", "1VidwEQqkwtoHqqdUqdwURWyiGd-OteaJ"),
    ("lulc_v3_2018_2019.tif", "1ZeLMAiBfolMrfEJkFnOlvb8OjSqC9vHP"),
    ("lulc_v3_2019_2020.tif", "1gx5VwJCHI-WUDJIwWv48OvbybBe9y0PR"),
    ("lulc_v3_2020_2021.tif", "1xbOt3-t1Ws5olq2Q88Tk32KnUVNKUXqe"),
    ("lulc_v3_2021_2022.tif", "1m8ZnUBbTp-fcH_JcRTUEceRaa8WewQmz"),
    ("lulc_v3_2022_2023.tif", "1_S0VESClg7s-DloAqxrfU8mLHhNSBfp7"),
    ("lulc_v3_2023_2024.tif", "1JVfl67ARRv7TPV5lyLnjoSfWDiXtvXjY"),
    ("lulc_v3_2024_2025.tif", "1CPV03S47s0asEJqdAozbNOT1lkgr0YkG"),
]

_SOI_WFS_PARAMS = {
    "service": "WFS",
    "version": "1.0.0",
    "request": "GetFeature",
    "typeName": "pan_india_asset:SOI_tehsil_pan_india_dataset",
    "outputFormat": "application/json",
}


def _is_dir_populated(path: Path) -> bool:
    return path.is_dir() and any(path.iterdir())


def ensure_soi_tehsil():
    """
    Downloads the SOI tehsil GeoJSON from GeoServer if not already present.
    This is a lightweight bootstrap; the full admin-boundary archive includes
    more data but takes much longer to acquire.
    """
    if SOI_TEHSIL_PATH.exists():
        logger.info("SOI tehsil layer already exists at %s, skipping.", SOI_TEHSIL_PATH)
        return

    SOI_TEHSIL_PATH.parent.mkdir(parents=True, exist_ok=True)

    wfs_url = f"{GEOSERVER_BASE}pan_india_asset/ows"
    logger.info("Downloading SOI tehsil layer from GeoServer...")
    try:
        response = requests.get(
            wfs_url, params=_SOI_WFS_PARAMS, timeout=600, stream=True
        )
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("Failed to download SOI tehsil layer: %s", e)
        return

    with open(SOI_TEHSIL_PATH, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info("SOI tehsil layer saved to %s", SOI_TEHSIL_PATH)


def ensure_admin_boundary_data():
    """
    Downloads and extracts the full admin-boundary archive (~8 GB) from Google Drive.
    Skipped if the input directory is already populated.
    Requires `gdown` and `7z` to be available on PATH.
    """
    if _is_dir_populated(ADMIN_BOUNDARY_INPUT_DIR):
        logger.info("Admin boundary data already exists, skipping.")
        return

    ADMIN_BOUNDARY_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    ADMIN_BOUNDARY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    archive_path = _PROJECT_ROOT / "dataset.7z"
    logger.info("Downloading admin boundary data (~8 GB) from Google Drive...")
    try:
        subprocess.run(
            ["gdown", _GDRIVE_ADMIN_BOUNDARY_FILE_ID, "-O", str(archive_path)],
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.error("Failed to download admin boundary archive: %s", e)
        return

    logger.info("Extracting admin boundary data...")
    try:
        subprocess.run(
            ["7z", "x", str(archive_path), f"-o{_PROJECT_ROOT / 'data/admin-boundary'}"],
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.error("Failed to extract admin boundary archive: %s", e)
        return
    finally:
        if archive_path.exists():
            archive_path.unlink()

    logger.info("Admin boundary data ready at %s", ADMIN_BOUNDARY_INPUT_DIR)


def ensure_lulc_rasters():
    """
    Downloads any missing LULC v3 yearly rasters from Google Drive.
    Files already present on disk are skipped — no re-download.
    Requires `gdown` on PATH. Each file is ~7-8 GB.
    """
    LULC_DIR.mkdir(parents=True, exist_ok=True)

    missing = [
        (filename, file_id)
        for filename, file_id in _LULC_GDRIVE_FILES
        if not (LULC_DIR / filename).exists()
    ]

    if not missing:
        logger.info("All LULC rasters already present at %s, skipping.", LULC_DIR)
        return

    logger.info(
        "%d LULC raster(s) missing, downloading: %s",
        len(missing),
        [f for f, _ in missing],
    )

    for filename, file_id in missing:
        dest = LULC_DIR / filename
        logger.info("Downloading %s (~7-8 GB)...", filename)
        try:
            subprocess.run(
                ["gdown", file_id, "-O", str(dest)],
                check=True,
            )
            logger.info("Saved %s", dest)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.error("Failed to download %s: %s", filename, e)
            if dest.exists():
                dest.unlink()
            raise


def ensure_village_boundaries_dir():
    """
    Ensures the village boundaries directory exists.
    TODO: add download logic once the source is determined.
    """
    VILLAGE_BOUNDARIES_DIR.mkdir(parents=True, exist_ok=True)
    TEHSIL_WATERSHEDS_DIR.mkdir(parents=True, exist_ok=True)


def setup_base_layers():
    ensure_soi_tehsil()
    ensure_admin_boundary_data()
    ensure_lulc_rasters()
    ensure_village_boundaries_dir()
