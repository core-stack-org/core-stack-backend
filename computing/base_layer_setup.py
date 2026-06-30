import logging
import subprocess
from functools import wraps
from pathlib import Path
from urllib.parse import urlparse

import requests
import yaml

from computing.config_loader import (
    ADMIN_BOUNDARY_INPUT_DIR,
    ADMIN_BOUNDARY_OUTPUT_DIR,
    MICROWATERSHED_PATH,
    PROJECT_ROOT,
    SOI_TEHSIL_PATH,
    VILLAGE_BOUNDARIES_DIR,
)
from computing.config_loader import (
    GDRIVE_ADMIN_BOUNDARY_FILE_ID as _GDRIVE_ADMIN_BOUNDARY_FILE_ID,
)
from computing.config_loader import (
    GDRIVE_MICROWATERSHED_FILE_ID as _GDRIVE_MICROWATERSHED_FILE_ID,
)
from computing.config_loader import (
    LULC_BASE_DIR as LULC_DIR,
)
from computing.config_loader import (
    LULC_GDRIVE_FILES as _LULC_GDRIVE_FILES,
)
from computing.config_loader import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR as TEHSIL_WATERSHEDS_DIR,
)
logger = logging.getLogger(__name__)

CONFIG_NEW_PATH = Path(__file__).resolve().parent / "config_new.yaml"

_SOI_WFS_PARAMS = {
    "service": "WFS",
    "version": "1.0.0",
    "request": "GetFeature",
    "typeName": "pan_india_asset:SOI_tehsil_pan_india_dataset",
    "outputFormat": "application/json",
}


def _is_dir_populated(path: Path) -> bool:
    return path.is_dir() and any(path.iterdir())


def _layer_key(name: str) -> str:
    return name.strip().lower().replace(" ", "_").replace("-", "_")


def _load_new_config() -> dict:
    with open(CONFIG_NEW_PATH) as f:
        return yaml.safe_load(f) or {}


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
        expanded["source"] = _format_periodic_value(
            layer["source"].replace("{filename}", filename), year
        )
        expanded_layers.append(expanded)

    return expanded_layers


def _manifest_layer_groups() -> dict[str, list[dict]]:
    base_layers = _load_new_config().get("base_layers", {})
    groups = {
        "static_layers": list(base_layers.get("static_layers", [])),
        "on_demand_layers": list(base_layers.get("on_demand_layers", [])),
        "periodic_layers": [],
    }

    for layer in base_layers.get("periodic_layers", []):
        groups["periodic_layers"].extend(_expand_periodic_layer(layer))

    return groups


def _manifest_layer_index() -> dict[str, list[dict]]:
    index = {}
    for group_name, layers in _manifest_layer_groups().items():
        index.setdefault(group_name, []).extend(layers)
        for layer in layers:
            index.setdefault(_layer_key(layer["name"]), []).append(layer)
            for alias in layer.get("aliases", []):
                index.setdefault(_layer_key(alias), []).append(layer)

    all_layers = []
    for layers in index.values():
        all_layers.extend(layers)
    index["all"] = list({id(layer): layer for layer in all_layers}.values())
    return index


def _download_s3_file(source: str, destination: Path):
    parsed = urlparse(source)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Invalid S3 source: {source}")

    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required to download base layers from S3") from exc

    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_destination = destination.with_suffix(destination.suffix + ".part")

    logger.info("Downloading %s to %s", source, destination)
    try:
        boto3.client("s3").download_file(
            parsed.netloc,
            parsed.path.lstrip("/"),
            str(temp_destination),
        )
        temp_destination.replace(destination)
    except Exception:
        if temp_destination.exists():
            temp_destination.unlink()
        raise


def ensure_manifest_base_layers(*layers):
    """
    Downloads base layers declared in config_new.yaml.

    Accepted selectors:
    - concrete layer names: terrain, mws, lulc_v3, slope_percentage
    - group names: static_layers, periodic_layers, on_demand_layers
    - all
    """
    selected_layers = layers or ("static_layers", "periodic_layers")
    index = _manifest_layer_index()

    for selected_layer in selected_layers:
        layer_key = _layer_key(selected_layer)
        layer_specs = index.get(layer_key)
        if layer_specs is None:
            available_layers = ", ".join(sorted(index))
            raise ValueError(
                f"Unknown manifest base layer '{selected_layer}'. "
                f"Available layers/groups: {available_layers}"
            )

        for layer in layer_specs:
            source = layer.get("source")
            if not source:
                logger.warning(
                    "Base layer %s has no source in %s; create it manually at %s.",
                    layer["name"],
                    CONFIG_NEW_PATH,
                    PROJECT_ROOT / layer["local_path"],
                )
                continue

            if layer.get("type") != "file":
                raise ValueError(
                    f"Unsupported base layer type for '{layer.get('name')}': "
                    f"{layer.get('type')}"
                )

            local_path = PROJECT_ROOT / layer["local_path"]
            if local_path.exists():
                logger.info(
                    "Base layer %s already exists at %s, skipping.",
                    layer["name"],
                    local_path,
                )
                continue

            if source.startswith("s3://"):
                _download_s3_file(source, local_path)
            else:
                raise ValueError(
                    f"Unsupported source for base layer '{layer['name']}': {source}"
                )


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

    from utilities.constants import GEOSERVER_BASE

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
        clip_to_tehsil=False,
    )
    logger.info("Tehsil watershed files ready at %s", TEHSIL_WATERSHEDS_DIR)


def ensure_village_boundaries_dir():
    """
    Ensures the village boundaries directory exists.
    TODO: add download logic once the source is determined.
    """
    VILLAGE_BOUNDARIES_DIR.mkdir(parents=True, exist_ok=True)


_BASE_LAYER_ENSURERS = {
    "soi_tehsil": ensure_soi_tehsil,
    "admin_boundary": ensure_admin_boundary_data,
    "lulc_rasters": ensure_lulc_rasters,
    "microwatershed": ensure_microwatershed,
    "tehsil_watersheds": ensure_tehsil_watersheds,
    "village_boundaries": ensure_village_boundaries_dir,
}

DEFAULT_BASE_LAYERS = (
    "static_layers",
    "periodic_layers",
)


def setup_base_layers(*layers):
    selected_layers = layers or DEFAULT_BASE_LAYERS
    manifest_index = _manifest_layer_index()

    for layer in selected_layers:
        if layer in _BASE_LAYER_ENSURERS:
            _BASE_LAYER_ENSURERS[layer]()
            continue

        if _layer_key(layer) in manifest_index:
            ensure_manifest_base_layers(layer)
            continue

        legacy_layers = ", ".join(sorted(_BASE_LAYER_ENSURERS))
        manifest_layers = ", ".join(sorted(manifest_index))
        raise ValueError(
            f"Unknown base layer '{layer}'. Legacy layers: {legacy_layers}. "
            f"Manifest layers/groups: {manifest_layers}"
        )


def with_base_layers(*layers):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            setup_base_layers(*layers)
            return func(*args, **kwargs)

        return wrapper

    return decorator


download_base_layers = with_base_layers
