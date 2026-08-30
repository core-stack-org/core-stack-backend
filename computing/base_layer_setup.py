import logging
import subprocess
from functools import wraps
from inspect import signature
from pathlib import Path
from urllib.parse import urlparse

import requests
import yaml

from computing.config_loader import (
    ADMIN_BOUNDARY_INPUT_DIR,
    ADMIN_BOUNDARY_OUTPUT_DIR,
    DATA_DIR,
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

CONFIG_NEW_PATH = Path(__file__).resolve().parent / "config.yaml"

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


def _local_path(rel_path: str) -> Path:
    return Path(rel_path.replace("{DATA_DIR}", str(DATA_DIR)))


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


def _expand_manifest_layers(layers: list[dict]) -> list[dict]:
    expanded_layers = []
    for layer in layers:
        expanded_layers.extend(_expand_periodic_layer(layer))
    return expanded_layers


def _manifest_group_key(path: tuple[str, ...]) -> str:
    return "_".join(path)


def _walk_manifest_groups(node, path: tuple[str, ...] = ()):
    if isinstance(node, list):
        yield _manifest_group_key(path), _expand_manifest_layers(node), path
        return

    if not isinstance(node, dict):
        return

    descendant_layers = []
    for key, value in node.items():
        child_path = (*path, key)
        if isinstance(value, list):
            layers = _expand_manifest_layers(value)
            descendant_layers.extend(layers)
            yield _manifest_group_key(child_path), layers, child_path
        elif isinstance(value, dict):
            child_groups = list(_walk_manifest_groups(value, child_path))
            for group_name, layers, group_path in child_groups:
                descendant_layers.extend(layers)
                yield group_name, layers, group_path

    if path and descendant_layers:
        yield _manifest_group_key(path), descendant_layers, path


def _manifest_layer_groups() -> dict[str, list[dict]]:
    base_layers = _load_new_config().get("base_layers", {})
    groups = {}
    leaf_aliases = {}

    for group_name, layers, group_path in _walk_manifest_groups(base_layers):
        groups[group_name] = layers
        leaf_name = group_path[-1]
        leaf_aliases.setdefault(leaf_name, []).append(group_name)

    for leaf_name, group_names in leaf_aliases.items():
        if len(group_names) == 1:
            groups.setdefault(leaf_name, groups[group_names[0]])

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
        client_kwargs = {}
        try:
            from django.conf import settings

            if settings.S3_ACCESS_KEY and settings.S3_SECRET_KEY:
                client_kwargs.update(
                    aws_access_key_id=settings.S3_ACCESS_KEY,
                    aws_secret_access_key=settings.S3_SECRET_KEY,
                )
            if getattr(settings, "S3_REGION", None):
                client_kwargs["region_name"] = settings.S3_REGION
        except Exception:
            logger.debug(
                "Django S3 settings unavailable; using boto3 credential provider chain.",
                exc_info=True,
            )

        boto3.client("s3", **client_kwargs).download_file(
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
    - group names: static_layers, periodic_layers, tehsil_level
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
                    _local_path(layer["local_path"]),
                )
                continue

            if layer.get("type") != "file":
                raise ValueError(
                    f"Unsupported base layer type for '{layer.get('name')}': "
                    f"{layer.get('type')}"
                )

            local_path = _local_path(layer["local_path"])
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
            ["7z", "x", str(archive_path), f"-o{DATA_DIR / 'admin-boundary'}"],
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


def _active_tehsil_locations():
    from geoadmin.models import TehsilSOI

    return TehsilSOI.objects.filter(
        active_status=True,
        district__active_status=True,
        district__state__active_status=True,
    ).values_list(
        "district__state__state_name",
        "district__district_name",
        "tehsil_name",
    ).order_by(
        "district__state__state_name",
        "district__district_name",
        "tehsil_name",
    )


def _tehsil_watershed_details(state, district, tehsil):
    from utilities.gee_utils import valid_gee_text

    state_slug = valid_gee_text(state.strip().lower())
    district_slug = valid_gee_text(district.strip().lower())
    tehsil_slug = valid_gee_text(tehsil.strip().lower())
    destination = (
        TEHSIL_WATERSHEDS_DIR
        / state_slug
        / district_slug
        / f"{tehsil_slug}.gpkg"
    )
    layer_name = f"mws:mws_{district_slug}_{tehsil_slug}"
    return destination, layer_name


def ensure_tehsil_watershed(state, district, tehsil, force=False):
    import geopandas as gpd

    from utilities.constants import GEOSERVER_BASE

    destination, layer_name = _tehsil_watershed_details(state, district, tehsil)
    if destination.exists() and not force:
        return destination

    wfs_url = f"{GEOSERVER_BASE}mws/ows"
    params = {
        "service": "WFS",
        "version": "1.0.0",
        "request": "GetFeature",
        "typeName": layer_name,
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
    }
    temp_destination = destination.with_suffix(".tmp.gpkg")

    try:
        response = requests.get(wfs_url, params=params, timeout=600)
        response.raise_for_status()
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("type") != "FeatureCollection"
        ):
            raise ValueError("GeoServer did not return a FeatureCollection")

        watersheds = gpd.GeoDataFrame.from_features(
            payload.get("features", []),
            crs="EPSG:4326",
        )
        if watersheds.empty:
            raise ValueError("GeoServer layer is empty")

        destination.parent.mkdir(parents=True, exist_ok=True)
        if temp_destination.exists():
            temp_destination.unlink()
        watersheds.to_file(
            temp_destination,
            layer="watersheds",
            driver="GPKG",
        )
        temp_destination.replace(destination)
    except Exception:
        if temp_destination.exists():
            temp_destination.unlink()
        raise

    logger.info("Saved %s to %s", layer_name, destination)
    return destination


def _download_active_tehsil_watersheds(force=False):
    locations = list(_active_tehsil_locations())
    if not locations:
        logger.warning("No active tehsils found; no watershed files downloaded.")
        return

    written = 0
    skipped = 0
    failures = []

    for state, district, tehsil in locations:
        destination, layer_name = _tehsil_watershed_details(
            state,
            district,
            tehsil,
        )
        if destination.exists() and not force:
            skipped += 1
            continue

        try:
            ensure_tehsil_watershed(
                state=state,
                district=district,
                tehsil=tehsil,
                force=force,
            )
            written += 1
        except Exception as exc:
            failures.append(f"{layer_name}: {exc}")
            logger.error("Failed to download %s: %s", layer_name, exc)

    logger.info(
        "GeoServer tehsil watersheds complete: %d written, %d skipped, %d failed.",
        written,
        skipped,
        len(failures),
    )
    if failures:
        raise RuntimeError(
            "Failed to download watershed layers for active tehsils: "
            + "; ".join(failures)
        )


def with_tehsil_watershed(func):
    func_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        call = func_signature.bind_partial(*args, **kwargs)
        call.apply_defaults()
        compute = str(call.arguments.get("compute", "local")).strip().lower()
        if compute == "local":
            ensure_tehsil_watershed(
                state=call.arguments["state"],
                district=call.arguments["district"],
                tehsil=call.arguments.get("block") or call.arguments.get("tehsil"),
            )
        return func(*args, **kwargs)

    return wrapper


def ensure_tehsil_watersheds(geoserver=False, force=False):
    """
    Generates per-tehsil watershed .gpkg files by spatially intersecting the
    microwatershed dataset against SOI tehsil boundaries.
    Existing files are skipped unless force is true. When geoserver is true,
    only active tehsils are downloaded from the mws workspace.
    Both source files (SOI tehsil + microwatershed) must exist first.
    """
    if geoserver:
        _download_active_tehsil_watersheds(force=force)
        return

    if _is_dir_populated(TEHSIL_WATERSHEDS_DIR) and not force:
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
        overwrite=force,
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


def setup_base_layers(*layers, geoserver=False, force=False):
    selected_layers = layers or DEFAULT_BASE_LAYERS
    manifest_index = _manifest_layer_index()

    for layer in selected_layers:
        if layer in _BASE_LAYER_ENSURERS:
            if layer == "tehsil_watersheds":
                ensure_tehsil_watersheds(geoserver=geoserver, force=force)
            else:
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
