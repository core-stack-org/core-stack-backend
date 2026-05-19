import logging
import subprocess

import requests
from pathlib import Path
from utilities.constants import GEOSERVER_BASE

from computing.config_loader import (
    ADMIN_BOUNDARY_INPUT_DIR,
    ADMIN_BOUNDARY_OUTPUT_DIR,
    GDRIVE_ADMIN_BOUNDARY_FILE_ID as _GDRIVE_ADMIN_BOUNDARY_FILE_ID,
    GDRIVE_MICROWATERSHED_FILE_ID as _GDRIVE_MICROWATERSHED_FILE_ID,
    LULC_BASE_DIR as LULC_DIR,
    LULC_GDRIVE_FILES as _LULC_GDRIVE_FILES,
    MICROWATERSHED_PATH,
    PRECOMPUTED_TEHSIL_WATERSHED_DIR as TEHSIL_WATERSHEDS_DIR,
    PROJECT_ROOT,
    SOI_TEHSIL_PATH,
    VILLAGE_BOUNDARIES_DIR,
)

logger = logging.getLogger(__name__)

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

    archive_path = PROJECT_ROOT / "dataset.7z"
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
            ["7z", "x", str(archive_path), f"-o{PROJECT_ROOT / 'data/admin-boundary'}"],
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


def ensure_microwatershed():
    """
    Downloads the pan-India microwatershed GeoJSON from Google Drive if not already present.
    Requires `gdown` on PATH.
    Fill in _GDRIVE_MICROWATERSHED_FILE_ID above once the Drive link is available.
    """
    if MICROWATERSHED_PATH.exists():
        logger.info(
            "Microwatershed file already exists at %s, skipping.", MICROWATERSHED_PATH
        )
        return

    if not _GDRIVE_MICROWATERSHED_FILE_ID:
        logger.warning(
            "Microwatershed file not found at %s and no Google Drive file ID is configured. "
            "Set _GDRIVE_MICROWATERSHED_FILE_ID in base_layer_setup.py or place the file manually.",
            MICROWATERSHED_PATH,
        )
        return

    MICROWATERSHED_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger.info(
        "Downloading Microwatershed_v2_with_details.geojson from Google Drive..."
    )
    try:
        subprocess.run(
            ["gdown", _GDRIVE_MICROWATERSHED_FILE_ID, "-O", str(MICROWATERSHED_PATH)],
            check=True,
        )
        logger.info("Saved microwatershed file to %s", MICROWATERSHED_PATH)
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.error("Failed to download microwatershed file: %s", e)
        if MICROWATERSHED_PATH.exists():
            MICROWATERSHED_PATH.unlink()
        raise


def ensure_tehsil_watersheds():
    """
    Generates per-tehsil watershed .gpkg files by spatially intersecting the
    microwatershed dataset against SOI tehsil boundaries.
    Skipped entirely if the output directory is already populated.
    Both source files (SOI tehsil + microwatershed) must exist first.
    """
    if _is_dir_populated(TEHSIL_WATERSHEDS_DIR):
        logger.info(
            "Tehsil watershed files already present at %s, skipping.",
            TEHSIL_WATERSHEDS_DIR,
        )
        return

    if not SOI_TEHSIL_PATH.exists():
        logger.warning(
            "Cannot generate tehsil watersheds: SOI tehsil file missing at %s.",
            SOI_TEHSIL_PATH,
        )
        return

    if not MICROWATERSHED_PATH.exists():
        logger.warning(
            "Cannot generate tehsil watersheds: microwatershed file missing at %s.",
            MICROWATERSHED_PATH,
        )
        return

    TEHSIL_WATERSHEDS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Generating tehsil watershed files (this may take a while)...")

    from computing.terrain_descriptor.store_watersheds_for_tehsils import (
        generate_tehsil_watershed_copies,
    )

    generate_tehsil_watershed_copies(
        microwatershed_path=str(MICROWATERSHED_PATH),
        tehsil_path=str(SOI_TEHSIL_PATH),
        output_dir=str(TEHSIL_WATERSHEDS_DIR),
        output_format="gpkg",
        overwrite=False,
    )
    logger.info("Tehsil watershed files ready at %s", TEHSIL_WATERSHEDS_DIR)


def ensure_village_boundaries_dir():
    """
    Ensures the village boundaries directory exists.
    TODO: add download logic once the source is determined.
    """
    VILLAGE_BOUNDARIES_DIR.mkdir(parents=True, exist_ok=True)


def setup_base_layers():
    ensure_soi_tehsil()
    ensure_admin_boundary_data()
    # ensure_lulc_rasters()
    # ensure_microwatershed()
    # ensure_tehsil_watersheds()
    ensure_village_boundaries_dir()
