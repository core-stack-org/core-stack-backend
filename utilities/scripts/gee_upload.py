"""General Earth Engine vector upload helpers.

This module stages vector files in Google Cloud Storage, converts GeoJSON-like
inputs into Earth Engine-friendly CSV uploads when needed, starts the Earth
Engine table ingestion, and can optionally wait for completion and make the
result public.

Supported source formats:
- .geojson
- .json
- .geojsonl
- .jsonl
- .ndjson

For very large `.geojson` / `.json` FeatureCollections, install `ijson` so the
file can be streamed instead of loaded fully into memory.

Usage Example:
`single file upload`
python -m utilities.scripts.gee_gen_utils \
  --file data/pan_india_facilities/pan_india_facilities.geojson \
  --service-account-json data/gee_confs/core-stack-learn-818963fa8f26.json \
  --wait \
  --poll-interval 120 \
  --gcs-prefix proximity \
  --asset-id projects/corestack-datasets/assets/datasets/pan_india_facilities \
  --replace-existing
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import tempfile
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

if __package__ in (None, ""):
    REPO_ROOT_FOR_SCRIPT = Path(__file__).resolve().parents[1]
    if str(REPO_ROOT_FOR_SCRIPT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT_FOR_SCRIPT))

from utilities.constants import GCS_BUCKET_NAME

SUPPORTED_VECTOR_SUFFIXES = {".geojson", ".json", ".geojsonl", ".jsonl", ".ndjson"}
DEFAULT_GCS_PREFIX = "gee/vector_uploads"
DEFAULT_SMALL_JSON_THRESHOLD_BYTES = 64 * 1024 * 1024
DEFAULT_GCS_CHUNK_SIZE_MB = 64
DEFAULT_STAGE_CSV_DELIMITER = "\t"
DEFAULT_STAGE_CSV_QUALIFIER = '"'
GEE_OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/earthengine",
    "https://www.googleapis.com/auth/devstorage.full_control",
]

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FACILITIES_POINT_DIR = REPO_ROOT / "data" / "facilities" / "facilities_point_files"
DEFAULT_FACILITIES_ASSET_PARENT = "projects/corestack-datasets/assets/facilities"

TERMINAL_TASK_STATES = {"SUCCEEDED", "COMPLETED", "FAILED", "CANCELLED"}
SUCCESS_TASK_STATES = {"SUCCEEDED", "COMPLETED"}
DEFAULT_PROGRESS_EVERY_FEATURES = 100000
DEFAULT_PROGRESS_EVERY_SECONDS = 30.0


class GEEUploadError(Exception):
    """Raised when vector upload preparation or ingestion fails."""


def log_progress(message: str) -> None:
    """Print a lightweight timestamped progress line for long-running CLI work."""
    timestamp = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def format_bytes(num_bytes: int) -> str:
    """Render a byte count in a compact human-readable format."""
    value = float(num_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{num_bytes} B"


def maybe_log_feature_progress(
    phase: str,
    feature_count: int,
    started_at: float,
    last_logged_at: float,
    every_features: int = DEFAULT_PROGRESS_EVERY_FEATURES,
    every_seconds: float = DEFAULT_PROGRESS_EVERY_SECONDS,
) -> float:
    """Emit periodic feature-count progress logs without flooding the terminal."""
    now = time.monotonic()
    should_log = feature_count == 1 or feature_count % every_features == 0 or (now - last_logged_at) >= every_seconds
    if should_log:
        elapsed = now - started_at
        rate = feature_count / elapsed if elapsed > 0 else 0.0
        log_progress(f"{phase}: processed {feature_count:,} features in {elapsed:.1f}s ({rate:,.0f} features/s)")
        return now
    return last_logged_at


def summarize_status_for_log(status: Dict[str, Any]) -> str:
    """Create a concise one-line task status summary for polling logs."""
    state = status.get("state") or "UNKNOWN"
    metadata = status.get("metadata") or {}
    progress_parts: List[str] = [f"state={state}"]
    for key in ("description", "destinationUris", "progress", "progressPct"):
        value = metadata.get(key)
        if value not in (None, "", [], {}):
            progress_parts.append(f"{key}={value}")
    error_message = status.get("error_message")
    if error_message:
        progress_parts.append(f"error={error_message}")
    return " | ".join(progress_parts)


def make_json_compatible(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, dict):
        return {key: make_json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [make_json_compatible(item) for item in value]
    return value


def sanitize_gee_asset_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", str(name).strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        raise GEEUploadError(f"Could not derive a valid asset name from '{name}'.")
    return cleaned


def normalize_gee_asset_parent(asset_parent: str) -> str:
    normalized = str(asset_parent).strip().rstrip("/")
    if not normalized.startswith("projects/") or "/assets" not in normalized:
        raise GEEUploadError(
            "Earth Engine asset parent must look like "
            "'projects/<project-id>/assets/<folder>'."
        )
    return normalized


def build_gee_asset_id(
    source_path: Path,
    asset_parent: Optional[str] = None,
    asset_id: Optional[str] = None,
    asset_name: Optional[str] = None,
) -> str:
    if asset_id:
        normalized_asset_id = str(asset_id).strip().rstrip("/")
        if not normalized_asset_id.startswith("projects/") or "/assets/" not in normalized_asset_id:
            raise GEEUploadError(
                "Earth Engine asset id must look like "
                "'projects/<project-id>/assets/<folder>/<asset-name>'."
            )
        return normalized_asset_id

    if not asset_parent:
        raise GEEUploadError("Either asset_parent or asset_id must be provided.")

    parent = normalize_gee_asset_parent(asset_parent)
    final_name = sanitize_gee_asset_name(asset_name or source_path.stem)
    return f"{parent}/{final_name}"


def _load_key_dict_from_service_account_file(service_account_json_path: str) -> Dict[str, Any]:
    json_path = Path(service_account_json_path).expanduser().resolve()
    if not json_path.is_file():
        raise GEEUploadError(f"Service-account JSON file was not found: {json_path}")

    with json_path.open("r", encoding="utf-8") as handle:
        key_dict = json.load(handle)

    if not key_dict.get("client_email"):
        raise GEEUploadError(
            f"Service-account JSON is missing client_email: {json_path}"
        )

    return key_dict


def _load_key_dict_from_django_account(gee_account_id: Optional[int]) -> Dict[str, Any]:
    from gee_computing.models import GEEAccount
    from nrm_app.settings import GEE_DEFAULT_ACCOUNT_ID

    account_pk = gee_account_id or GEE_DEFAULT_ACCOUNT_ID
    account = GEEAccount.objects.get(pk=account_pk)
    return json.loads(account.get_credentials().decode("utf-8"))


def load_gee_key_dict(
    gee_account_id: Optional[int] = None,
    service_account_json_path: Optional[str] = None,
) -> Dict[str, Any]:
    if service_account_json_path:
        return _load_key_dict_from_service_account_file(service_account_json_path)
    return _load_key_dict_from_django_account(gee_account_id)


def build_service_account_credentials(
    gee_account_id: Optional[int] = None,
    service_account_json_path: Optional[str] = None,
):
    from google.oauth2 import service_account

    key_dict = load_gee_key_dict(
        gee_account_id=gee_account_id,
        service_account_json_path=service_account_json_path,
    )
    credentials = service_account.Credentials.from_service_account_info(
        key_dict,
        scopes=GEE_OAUTH_SCOPES,
    )
    return credentials, key_dict


def initialize_gee_session(
    gee_account_id: Optional[int] = None,
    service_account_json_path: Optional[str] = None,
):
    import ee

    credentials, key_dict = build_service_account_credentials(
        gee_account_id=gee_account_id,
        service_account_json_path=service_account_json_path,
    )
    ee.Initialize(credentials=credentials, project=key_dict.get("project_id"))
    return ee, credentials, key_dict


def build_gcs_client(credentials, key_dict):
    from google.cloud import storage

    return storage.Client(project=key_dict.get("project_id"), credentials=credentials)


def gee_asset_exists(ee_module, asset_id: str) -> bool:
    try:
        ee_module.data.getAsset(asset_id)
        return True
    except Exception:
        return False


def ensure_gee_folder_path(ee_module, folder_path: str) -> None:
    normalized = normalize_gee_asset_parent(folder_path)
    prefix, suffix = normalized.split("/assets", 1)
    current = f"{prefix}/assets"
    for part in [segment for segment in suffix.split("/") if segment]:
        current = f"{current}/{part}"
        if gee_asset_exists(ee_module, current):
            continue
        ee_module.data.createAsset({"type": "FOLDER"}, current)
        time.sleep(1)


def delete_gee_asset_if_exists(ee_module, asset_id: str) -> bool:
    if not gee_asset_exists(ee_module, asset_id):
        return False
    ee_module.data.deleteAsset(asset_id)
    time.sleep(1)
    return True


def make_gee_asset_public(ee_module, asset_id: str) -> bool:
    acl = ee_module.data.getAssetAcl(asset_id)
    acl["all_users_can_read"] = True
    ee_module.data.setAssetAcl(asset_id, acl)
    updated_acl = ee_module.data.getAssetAcl(asset_id)
    return bool(updated_acl.get("all_users_can_read"))


def delete_gcs_blob(credentials, key_dict, bucket_name: str, blob_name: str) -> None:
    client = build_gcs_client(credentials, key_dict)
    bucket = client.bucket(bucket_name)
    bucket.blob(blob_name).delete()


def verify_gcs_blob_read_access(blob) -> None:
    try:
        blob.reload()
    except Exception as exc:
        raise GEEUploadError(
            f"Uploaded GCS object '{blob.name}' in bucket '{blob.bucket.name}' "
            "is not readable by the current credentials. Earth Engine imports "
            "need storage.objects.get access on the staged object. "
            f"Underlying error: {exc}"
        ) from exc


def preflight_gcs_bucket_access(
    credentials,
    key_dict,
    bucket_name: str,
    gcs_prefix: str,
) -> None:
    client = build_gcs_client(credentials, key_dict)
    bucket = client.bucket(bucket_name)
    probe_blob_name = f"{gcs_prefix.strip('/')}/_codex_access_probe_{uuid.uuid4().hex}.txt"
    probe_blob = bucket.blob(probe_blob_name)
    probe_blob.upload_from_string("gcs access probe")
    try:
        verify_gcs_blob_read_access(probe_blob)
    finally:
        try:
            probe_blob.delete()
        except Exception:
            pass


def upload_file_to_gcs(
    local_file_path: Path,
    destination_blob_name: str,
    credentials,
    key_dict,
    bucket_name: str = GCS_BUCKET_NAME,
    chunk_size_mb: int = DEFAULT_GCS_CHUNK_SIZE_MB,
) -> str:
    client = build_gcs_client(credentials, key_dict)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.chunk_size = max(256 * 1024, chunk_size_mb * 1024 * 1024)
    file_size = local_file_path.stat().st_size
    log_progress(
        f"Uploading staged CSV to gs://{bucket_name}/{destination_blob_name} "
        f"({format_bytes(file_size)}, chunk_size={format_bytes(blob.chunk_size)})"
    )
    started = time.monotonic()
    blob.upload_from_filename(str(local_file_path))
    verify_gcs_blob_read_access(blob)
    elapsed = time.monotonic() - started
    rate = file_size / elapsed if elapsed > 0 else 0.0
    log_progress(
        f"Finished GCS upload in {elapsed:.1f}s "
        f"({format_bytes(int(rate))}/s average)"
    )
    return f"gs://{bucket_name}/{destination_blob_name}"


def normalize_feature(feature: Any, source_label: str) -> Dict[str, Any]:
    if not isinstance(feature, dict):
        raise GEEUploadError(f"Expected feature object in {source_label}, got {type(feature).__name__}.")

    if feature.get("type") == "Feature":
        geometry = feature.get("geometry")
        properties = feature.get("properties") or {}
    elif "geometry" in feature:
        geometry = feature.get("geometry")
        properties = feature.get("properties") or {
            key: value for key, value in feature.items() if key != "geometry"
        }
    elif feature.get("type") in {"Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"}:
        geometry = feature
        properties = {}
    else:
        raise GEEUploadError(
            f"Unsupported feature payload in {source_label}. "
            "Expected a GeoJSON Feature or geometry."
        )

    if properties is None:
        properties = {}
    if not isinstance(properties, dict):
        raise GEEUploadError(f"Feature properties must be an object in {source_label}.")

    return {
        "type": "Feature",
        "geometry": make_json_compatible(geometry),
        "properties": make_json_compatible(properties),
    }


def iter_jsonl_features(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            yield normalize_feature(payload, f"{path}:{line_number}")


def _read_first_non_whitespace_char(path: Path) -> Optional[str]:
    with path.open("r", encoding="utf-8") as handle:
        while True:
            chunk = handle.read(4096)
            if not chunk:
                return None
            for char in chunk:
                if not char.isspace():
                    return char
    return None


def iter_large_json_features_with_ijson(path: Path) -> Iterator[Dict[str, Any]]:
    try:
        import ijson
    except ImportError as exc:
        raise GEEUploadError(
            "Large .geojson/.json uploads need streaming parsing. "
            "Install `ijson` or convert the source file to `.geojsonl`."
        ) from exc

    first_char = _read_first_non_whitespace_char(path)
    if first_char == "[":
        prefix = "item"
    else:
        prefix = "features.item"

    log_progress(
        f"Streaming large JSON input with ijson from {path} "
        f"({format_bytes(path.stat().st_size)}) using prefix '{prefix}'"
    )

    emitted = 0
    with path.open("rb") as handle:
        for feature in ijson.items(handle, prefix):
            emitted += 1
            yield normalize_feature(feature, str(path))

    if emitted == 0:
        raise GEEUploadError(
            f"No features were detected in {path}. Large JSON inputs should be a "
            "GeoJSON FeatureCollection or an array of Feature objects."
        )


def extract_small_json_features(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if isinstance(payload, list):
        for index, feature in enumerate(payload):
            yield normalize_feature(feature, f"{path}[{index}]")
        return

    if not isinstance(payload, dict):
        raise GEEUploadError(f"Unsupported JSON payload in {path}.")

    if payload.get("type") == "FeatureCollection":
        for index, feature in enumerate(payload.get("features", [])):
            yield normalize_feature(feature, f"{path}.features[{index}]")
        return

    if payload.get("type") == "Feature" or "geometry" in payload:
        yield normalize_feature(payload, str(path))
        return

    if isinstance(payload.get("features"), list):
        for index, feature in enumerate(payload["features"]):
            yield normalize_feature(feature, f"{path}.features[{index}]")
        return

    raise GEEUploadError(
        f"Unsupported JSON structure in {path}. Expected a FeatureCollection, "
        "Feature, array of Features, or line-delimited Features."
    )


def iter_source_features(
    source_path: Path,
    small_json_threshold_bytes: int = DEFAULT_SMALL_JSON_THRESHOLD_BYTES,
) -> Iterator[Dict[str, Any]]:
    suffix = source_path.suffix.lower()
    if suffix not in SUPPORTED_VECTOR_SUFFIXES:
        raise GEEUploadError(
            f"Unsupported input format '{source_path.suffix}' for {source_path}."
        )

    if suffix in {".geojsonl", ".jsonl", ".ndjson"}:
        yield from iter_jsonl_features(source_path)
        return

    if source_path.stat().st_size > small_json_threshold_bytes:
        yield from iter_large_json_features_with_ijson(source_path)
        return

    yield from extract_small_json_features(source_path)


def csv_safe_value(value: Any) -> Any:
    if value is None:
        return ""
    value = make_json_compatible(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, str):
        # Keep table rows single-line and delimiter-safe for EE ingestion.
        return value.replace("\r\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")
    return value


def collect_feature_field_names(source_path: Path) -> Tuple[List[str], int]:
    field_names: "OrderedDict[str, None]" = OrderedDict()
    feature_count = 0
    started_at = time.monotonic()
    last_logged_at = started_at
    log_progress(f"Scanning properties to build CSV schema from {source_path}")

    for feature in iter_source_features(source_path):
        feature_count += 1
        last_logged_at = maybe_log_feature_progress(
            "Schema scan",
            feature_count,
            started_at,
            last_logged_at,
        )
        for key in feature["properties"].keys():
            field_names.setdefault(str(key), None)

    if feature_count == 0:
        raise GEEUploadError(f"No features were found in {source_path}.")

    elapsed = time.monotonic() - started_at
    log_progress(
        f"Schema scan complete: {feature_count:,} features, "
        f"{len(field_names):,} property columns discovered in {elapsed:.1f}s"
    )

    return list(field_names.keys()), feature_count


def choose_geometry_column_name(field_names: Iterable[str], preferred_name: str = "geometry") -> str:
    used_names = set(field_names)
    candidate = preferred_name
    index = 1
    while candidate in used_names:
        index += 1
        candidate = f"{preferred_name}_{index}"
    return candidate


def convert_vector_source_to_csv(
    source_path: Path,
    output_csv_path: Path,
    geometry_column_name: str = "geometry",
    csv_delimiter: str = DEFAULT_STAGE_CSV_DELIMITER,
    csv_qualifier: str = DEFAULT_STAGE_CSV_QUALIFIER,
) -> Dict[str, Any]:
    field_names, feature_count = collect_feature_field_names(source_path)
    geometry_column = choose_geometry_column_name(field_names, geometry_column_name)
    csv_columns = list(field_names) + [geometry_column]
    log_progress(
        f"Writing staged CSV to {output_csv_path} with {len(csv_columns):,} columns "
        f"({len(field_names):,} properties + geometry, "
        f"delimiter={repr(csv_delimiter)}, qualifier={repr(csv_qualifier)})"
    )
    started_at = time.monotonic()
    last_logged_at = started_at
    written_count = 0

    with output_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=csv_columns,
            delimiter=csv_delimiter,
            quotechar=csv_qualifier,
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
            doublequote=True,
        )
        writer.writeheader()
        for feature in iter_source_features(source_path):
            written_count += 1
            last_logged_at = maybe_log_feature_progress(
                "CSV write",
                written_count,
                started_at,
                last_logged_at,
            )
            row = {field_name: csv_safe_value(feature["properties"].get(field_name)) for field_name in field_names}
            geometry = feature.get("geometry")
            row[geometry_column] = (
                ""
                if geometry is None
                else json.dumps(
                    make_json_compatible(geometry),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
            writer.writerow(row)

    csv_size = output_csv_path.stat().st_size if output_csv_path.exists() else 0
    elapsed = time.monotonic() - started_at
    log_progress(
        f"CSV staging complete: wrote {written_count:,} rows to {output_csv_path} "
        f"({format_bytes(csv_size)}) in {elapsed:.1f}s"
    )

    return {
        "csv_path": str(output_csv_path),
        "feature_count": feature_count,
        "field_names": field_names,
        "geometry_column": geometry_column,
        "csv_delimiter": csv_delimiter,
        "csv_qualifier": csv_qualifier,
    }


def build_manifest(
    asset_id: str,
    gcs_uri: str,
    geometry_column: str,
    asset_properties: Optional[Dict[str, Any]] = None,
    max_vertices: int = 0,
    max_error_meters: float = 1.0,
    csv_delimiter: str = DEFAULT_STAGE_CSV_DELIMITER,
    csv_qualifier: str = DEFAULT_STAGE_CSV_QUALIFIER,
) -> Dict[str, Any]:
    source: Dict[str, Any] = {
        "uris": [gcs_uri],
        "charset": "UTF-8",
        "primaryGeometryColumn": geometry_column,
        "maxErrorMeters": max_error_meters,
        "csvDelimiter": csv_delimiter,
        "csvQualifier": csv_qualifier,
    }
    if max_vertices:
        source["maxVertices"] = int(max_vertices)

    manifest: Dict[str, Any] = {
        "name": asset_id,
        "sources": [source],
    }
    if asset_properties:
        manifest["properties"] = asset_properties
    return manifest


def start_gee_table_ingestion(
    ee_module,
    manifest: Dict[str, Any],
) -> Dict[str, Any]:
    task_id = None
    operation_name = None

    if hasattr(ee_module.data, "newTaskId"):
        generated = ee_module.data.newTaskId()
        if isinstance(generated, (list, tuple)) and generated:
            task_id = generated[0]
        elif generated:
            task_id = generated

    response = ee_module.data.startTableIngestion(task_id, manifest)

    if isinstance(response, str):
        operation_name = response
    elif isinstance(response, dict):
        operation_name = response.get("name") or response.get("id")

    if not task_id and operation_name:
        task_id = operation_name.rsplit("/", 1)[-1]

    return {
        "task_id": task_id,
        "operation_name": operation_name,
        "raw_response": response,
    }


def fetch_gee_task_status(
    ee_module,
    task_id: Optional[str] = None,
    operation_name: Optional[str] = None,
) -> Dict[str, Any]:
    operations = ee_module.data.listOperations()
    if isinstance(operations, dict) and "operations" in operations:
        operations = operations["operations"]

    operations = operations or []
    for operation in operations:
        name = operation.get("name")
        short_id = name.rsplit("/", 1)[-1] if name else None
        if (operation_name and name == operation_name) or (task_id and short_id == task_id):
            metadata = operation.get("metadata", {})
            return {
                "task_id": short_id,
                "operation_name": name,
                "state": metadata.get("state"),
                "metadata": metadata,
                "error": operation.get("error"),
                "error_message": (operation.get("error") or {}).get("message"),
                "raw": operation,
            }

    if task_id and hasattr(ee_module.data, "getTaskStatus"):
        response = ee_module.data.getTaskStatus(task_id)
        if isinstance(response, list):
            response = response[0] if response else {}
        if isinstance(response, dict):
            return {
                "task_id": response.get("id", task_id),
                "operation_name": operation_name,
                "state": response.get("state"),
                "metadata": response,
                "error": response.get("error_message") or response.get("error"),
                "error_message": response.get("error_message"),
                "raw": response,
            }

    return {
        "task_id": task_id,
        "operation_name": operation_name,
        "state": None,
        "metadata": {},
        "error": None,
        "error_message": None,
        "raw": None,
    }


def wait_for_gee_task(
    ee_module,
    task_id: Optional[str] = None,
    operation_name: Optional[str] = None,
    poll_interval_seconds: int = 30,
    timeout_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    started = time.monotonic()
    attempts = 0

    while True:
        attempts += 1
        status = fetch_gee_task_status(
            ee_module,
            task_id=task_id,
            operation_name=operation_name,
        )
        state = status.get("state")
        elapsed = time.monotonic() - started
        log_progress(
            f"Earth Engine task poll #{attempts} after {elapsed:.1f}s: "
            f"{summarize_status_for_log(status)}"
        )
        if state in TERMINAL_TASK_STATES:
            return status

        if timeout_seconds and (time.monotonic() - started) > timeout_seconds:
            raise GEEUploadError(
                f"Timed out while waiting for task {task_id or operation_name}."
            )

        time.sleep(max(1, poll_interval_seconds))


def normalize_input_paths(
    file_paths: Optional[Sequence[str]] = None,
    input_dir: Optional[str] = None,
) -> List[Path]:
    resolved_paths: List[Path] = []

    for file_path in file_paths or []:
        path = Path(file_path).expanduser().resolve()
        if not path.is_file():
            raise GEEUploadError(f"Input file was not found: {path}")
        resolved_paths.append(path)

    if input_dir:
        directory = Path(input_dir).expanduser().resolve()
        if not directory.is_dir():
            raise GEEUploadError(f"Input directory was not found: {directory}")
        for path in sorted(directory.iterdir()):
            if path.is_file() and path.suffix.lower() in SUPPORTED_VECTOR_SUFFIXES:
                resolved_paths.append(path.resolve())

    deduplicated_paths: List[Path] = []
    seen = set()
    for path in resolved_paths:
        if str(path) in seen:
            continue
        deduplicated_paths.append(path)
        seen.add(str(path))

    if not deduplicated_paths:
        raise GEEUploadError(
            "No input files were found. Provide --file and/or --directory."
        )

    return deduplicated_paths


def upload_vector_file_to_gee(
    source_path: str | Path,
    asset_parent: Optional[str] = None,
    asset_id: Optional[str] = None,
    asset_name: Optional[str] = None,
    gee_account_id: Optional[int] = None,
    service_account_json_path: Optional[str] = None,
    gcs_bucket_name: str = GCS_BUCKET_NAME,
    gcs_prefix: str = DEFAULT_GCS_PREFIX,
    replace_existing: bool = False,
    wait_for_completion: bool = False,
    make_public: bool = False,
    cleanup_gcs: bool = False,
    poll_interval_seconds: int = 30,
    timeout_seconds: Optional[int] = None,
    geometry_column_name: str = "geometry",
    max_vertices: int = 0,
    max_error_meters: float = 1.0,
    chunk_size_mb: int = DEFAULT_GCS_CHUNK_SIZE_MB,
    asset_properties: Optional[Dict[str, Any]] = None,
    csv_delimiter: str = DEFAULT_STAGE_CSV_DELIMITER,
    csv_qualifier: str = DEFAULT_STAGE_CSV_QUALIFIER,
) -> Dict[str, Any]:
    source = Path(source_path).expanduser().resolve()
    if not source.is_file():
        raise GEEUploadError(f"Input file was not found: {source}")

    if source.suffix.lower() not in SUPPORTED_VECTOR_SUFFIXES:
        raise GEEUploadError(
            f"Unsupported input format '{source.suffix}' for {source.name}."
        )

    log_progress(
        f"Starting Earth Engine upload for {source} "
        f"({format_bytes(source.stat().st_size)})"
    )

    ee_module, credentials, key_dict = initialize_gee_session(
        gee_account_id=gee_account_id,
        service_account_json_path=service_account_json_path,
    )

    final_asset_id = build_gee_asset_id(
        source_path=source,
        asset_parent=asset_parent,
        asset_id=asset_id,
        asset_name=asset_name,
    )
    final_asset_parent = final_asset_id.rsplit("/", 1)[0]
    log_progress(f"Resolved target asset: {final_asset_id}")
    log_progress(f"Ensuring Earth Engine folder path exists: {final_asset_parent}")
    ensure_gee_folder_path(ee_module, final_asset_parent)

    deleted_existing = False
    if gee_asset_exists(ee_module, final_asset_id):
        if not replace_existing:
            raise GEEUploadError(
                f"Asset already exists: {final_asset_id}. "
                "Use replace_existing=True to overwrite it."
            )
        log_progress(f"Deleting existing Earth Engine asset before re-upload: {final_asset_id}")
        deleted_existing = delete_gee_asset_if_exists(ee_module, final_asset_id)

    effective_wait = wait_for_completion or make_public or cleanup_gcs
    temp_csv_path: Optional[Path] = None
    gcs_blob_name: Optional[str] = None

    try:
        log_progress(
            f"Running GCS access preflight for bucket '{gcs_bucket_name}' "
            f"under prefix '{gcs_prefix.strip('/')}'"
        )
        preflight_gcs_bucket_access(
            credentials=credentials,
            key_dict=key_dict,
            bucket_name=gcs_bucket_name,
            gcs_prefix=gcs_prefix,
        )

        with tempfile.NamedTemporaryFile(
            prefix=f"{sanitize_gee_asset_name(source.stem)}_",
            suffix=".csv",
            delete=False,
        ) as temp_handle:
            temp_csv_path = Path(temp_handle.name)
        log_progress(f"Temporary staged CSV path: {temp_csv_path}")

        conversion = convert_vector_source_to_csv(
            source,
            temp_csv_path,
            geometry_column_name=geometry_column_name,
            csv_delimiter=csv_delimiter,
            csv_qualifier=csv_qualifier,
        )
        log_progress(
            f"Prepared CSV for Earth Engine import: {conversion['feature_count']:,} features, "
            f"{len(conversion['field_names']):,} property columns, "
            f"geometry column '{conversion['geometry_column']}'"
        )

        asset_metadata = {
            "source_filename": source.name,
            "source_format": source.suffix.lower().lstrip("."),
            "uploaded_via": "utilities.gee_gen_utils",
        }
        if asset_properties:
            asset_metadata.update(asset_properties)

        blob_suffix = f"{sanitize_gee_asset_name(source.stem)}_{uuid.uuid4().hex}.csv"
        gcs_blob_name = f"{gcs_prefix.strip('/')}/{blob_suffix}"
        gcs_uri = upload_file_to_gcs(
            temp_csv_path,
            gcs_blob_name,
            credentials=credentials,
            key_dict=key_dict,
            bucket_name=gcs_bucket_name,
            chunk_size_mb=chunk_size_mb,
        )

        manifest = build_manifest(
            asset_id=final_asset_id,
            gcs_uri=gcs_uri,
            geometry_column=conversion["geometry_column"],
            asset_properties=asset_metadata,
            max_vertices=max_vertices,
            max_error_meters=max_error_meters,
            csv_delimiter=conversion["csv_delimiter"],
            csv_qualifier=conversion["csv_qualifier"],
        )
        log_progress(
            f"Submitting Earth Engine table ingestion for {final_asset_id} "
            f"from {gcs_uri}"
        )
        ingestion = start_gee_table_ingestion(ee_module, manifest)
        log_progress(
            f"Earth Engine ingestion submitted: task_id={ingestion.get('task_id')} "
            f"operation={ingestion.get('operation_name')}"
        )

        result: Dict[str, Any] = {
            "source_path": str(source),
            "asset_id": final_asset_id,
            "asset_parent": final_asset_parent,
            "task_id": ingestion.get("task_id"),
            "operation_name": ingestion.get("operation_name"),
            "gcs_uri": gcs_uri,
            "gcs_blob_name": gcs_blob_name,
            "feature_count": conversion["feature_count"],
            "field_names": conversion["field_names"],
            "geometry_column": conversion["geometry_column"],
            "deleted_existing_asset": deleted_existing,
            "waited_for_completion": effective_wait,
        }

        if effective_wait:
            log_progress(
                f"Waiting for Earth Engine ingestion to finish "
                f"(poll interval {poll_interval_seconds}s)"
            )
            status = wait_for_gee_task(
                ee_module,
                task_id=ingestion.get("task_id"),
                operation_name=ingestion.get("operation_name"),
                poll_interval_seconds=poll_interval_seconds,
                timeout_seconds=timeout_seconds,
            )
            result["status"] = status

            state = status.get("state")
            if state not in SUCCESS_TASK_STATES:
                error_message = status.get("error_message")
                raise GEEUploadError(
                    f"GEE ingestion failed for {final_asset_id} with state={state}."
                    + (f" {error_message}" if error_message else "")
                )

            if make_public:
                log_progress(f"Making Earth Engine asset public: {final_asset_id}")
                result["made_public"] = make_gee_asset_public(ee_module, final_asset_id)

            if cleanup_gcs and gcs_blob_name:
                try:
                    log_progress(f"Deleting staged GCS object: gs://{gcs_bucket_name}/{gcs_blob_name}")
                    delete_gcs_blob(credentials, key_dict, gcs_bucket_name, gcs_blob_name)
                    result["deleted_gcs_blob"] = True
                except Exception as exc:
                    result["deleted_gcs_blob"] = False
                    result["cleanup_warning"] = (
                        "Earth Engine asset upload succeeded, but deleting the staged "
                        f"GCS object failed: {exc}"
                    )
        else:
            result["status"] = {"state": "SUBMITTED"}
            log_progress(
                f"Upload submitted and not waiting for completion. "
                f"Task id: {ingestion.get('task_id')}"
            )

        log_progress(f"Earth Engine upload workflow finished for {final_asset_id}")
        return result
    finally:
        if temp_csv_path and temp_csv_path.exists():
            log_progress(f"Removing temporary staged CSV: {temp_csv_path}")
            temp_csv_path.unlink()


def upload_vector_files_to_gee(
    file_paths: Optional[Sequence[str]] = None,
    input_dir: Optional[str] = None,
    asset_parent: Optional[str] = None,
    gee_account_id: Optional[int] = None,
    service_account_json_path: Optional[str] = None,
    gcs_bucket_name: str = GCS_BUCKET_NAME,
    gcs_prefix: str = DEFAULT_GCS_PREFIX,
    replace_existing: bool = False,
    wait_for_completion: bool = False,
    make_public: bool = False,
    cleanup_gcs: bool = False,
    poll_interval_seconds: int = 30,
    timeout_seconds: Optional[int] = None,
    geometry_column_name: str = "geometry",
    max_vertices: int = 0,
    max_error_meters: float = 1.0,
    chunk_size_mb: int = DEFAULT_GCS_CHUNK_SIZE_MB,
    asset_properties: Optional[Dict[str, Any]] = None,
    csv_delimiter: str = DEFAULT_STAGE_CSV_DELIMITER,
    csv_qualifier: str = DEFAULT_STAGE_CSV_QUALIFIER,
    continue_on_error: bool = True,
) -> List[Dict[str, Any]]:
    paths = normalize_input_paths(file_paths=file_paths, input_dir=input_dir)
    results: List[Dict[str, Any]] = []

    for path in paths:
        try:
            result = upload_vector_file_to_gee(
                source_path=path,
                asset_parent=asset_parent,
                gee_account_id=gee_account_id,
                service_account_json_path=service_account_json_path,
                gcs_bucket_name=gcs_bucket_name,
                gcs_prefix=gcs_prefix,
                replace_existing=replace_existing,
                wait_for_completion=wait_for_completion,
                make_public=make_public,
                cleanup_gcs=cleanup_gcs,
                poll_interval_seconds=poll_interval_seconds,
                timeout_seconds=timeout_seconds,
                geometry_column_name=geometry_column_name,
                max_vertices=max_vertices,
                max_error_meters=max_error_meters,
                chunk_size_mb=chunk_size_mb,
                asset_properties=asset_properties,
                csv_delimiter=csv_delimiter,
                csv_qualifier=csv_qualifier,
            )
            result["ok"] = True
            results.append(result)
        except Exception as exc:
            failure = {
                "source_path": str(path),
                "asset_id": build_gee_asset_id(path, asset_parent=asset_parent)
                if asset_parent
                else None,
                "ok": False,
                "error": str(exc),
            }
            results.append(failure)
            if not continue_on_error:
                raise

    return results


def upload_facilities_point_files(
    gee_account_id: Optional[int] = None,
    service_account_json_path: Optional[str] = None,
    asset_parent: str = DEFAULT_FACILITIES_ASSET_PARENT,
    input_dir: str | Path = DEFAULT_FACILITIES_POINT_DIR,
    **kwargs,
) -> List[Dict[str, Any]]:
    return upload_vector_files_to_gee(
        input_dir=str(Path(input_dir).expanduser().resolve()),
        asset_parent=asset_parent,
        gee_account_id=gee_account_id,
        service_account_json_path=service_account_json_path,
        **kwargs,
    )


def bootstrap_django_for_cli() -> None:
    import os

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")
    from nrm_app.runtime import configure_runtime_environment

    configure_runtime_environment()

    import django

    django.setup()


def parse_cli_properties(property_args: Optional[Sequence[str]]) -> Dict[str, str]:
    parsed: Dict[str, str] = {}
    for raw_value in property_args or []:
        if "=" not in raw_value:
            raise GEEUploadError(
                f"Invalid --property value '{raw_value}'. Expected key=value."
            )
        key, value = raw_value.split("=", 1)
        key = key.strip()
        if not key:
            raise GEEUploadError(
                f"Invalid --property value '{raw_value}'. Property name is empty."
            )
        parsed[key] = value
    return parsed


def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Upload GeoJSON/JSON/GeoJSONL vector files into Earth Engine assets by "
            "staging them in GCS and importing them as table assets."
        )
    )
    parser.add_argument(
        "--file",
        action="append",
        dest="files",
        help="Path to an input file. Repeat the flag to upload multiple files.",
    )
    parser.add_argument(
        "--directory",
        help="Directory containing .geojson/.json/.geojsonl files to upload.",
    )
    parser.add_argument(
        "--asset-parent",
        help="Earth Engine asset folder, e.g. projects/corestack-datasets/assets/facilities",
    )
    parser.add_argument(
        "--asset-id",
        help="Full Earth Engine asset id for a single-file upload.",
    )
    parser.add_argument(
        "--asset-name",
        help="Optional asset name override for a single-file upload.",
    )
    parser.add_argument(
        "--gee-account-id",
        type=int,
        help="Use credentials stored in the Django GEEAccount model.",
    )
    parser.add_argument(
        "--service-account-json",
        help="Path to a service-account JSON file to use instead of Django GEEAccount credentials.",
    )
    parser.add_argument(
        "--gcs-bucket",
        default=GCS_BUCKET_NAME,
        help=f"GCS bucket used as the staging area. Defaults to {GCS_BUCKET_NAME}.",
    )
    parser.add_argument(
        "--gcs-prefix",
        default=DEFAULT_GCS_PREFIX,
        help="Prefix inside the GCS bucket for staged CSV files.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Delete an existing EE asset with the same id before re-uploading.",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for Earth Engine ingestion to finish before exiting.",
    )
    parser.add_argument(
        "--make-public",
        action="store_true",
        help="After a successful upload, set the asset ACL to public if allowed.",
    )
    parser.add_argument(
        "--cleanup-gcs",
        action="store_true",
        help="Delete the staged GCS object after a successful upload.",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Polling interval in seconds when waiting for ingestion tasks.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        help="Optional timeout while waiting for ingestion tasks.",
    )
    parser.add_argument(
        "--max-vertices",
        type=int,
        default=0,
        help="Optional Earth Engine geometry split threshold.",
    )
    parser.add_argument(
        "--max-error-meters",
        type=float,
        default=1.0,
        help="Maximum reprojection error in meters for ingestion.",
    )
    parser.add_argument(
        "--chunk-size-mb",
        type=int,
        default=DEFAULT_GCS_CHUNK_SIZE_MB,
        help="GCS resumable upload chunk size in MB.",
    )
    parser.add_argument(
        "--csv-delimiter",
        default="tab",
        help="Delimiter used in the staged table for Earth Engine. Default: tab.",
    )
    parser.add_argument(
        "--csv-qualifier",
        default=DEFAULT_STAGE_CSV_QUALIFIER,
        help='Quote character used in the staged table. Default: ".',
    )
    parser.add_argument(
        "--property",
        action="append",
        dest="properties",
        help="Asset metadata property in key=value format. Repeat as needed.",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Abort the batch on the first failed file.",
    )
    parser.add_argument(
        "--facilities-defaults",
        action="store_true",
        help=(
            "Shortcut for uploading data/facilities/facilities_point_files to "
            f"{DEFAULT_FACILITIES_ASSET_PARENT}."
        ),
    )
    return parser


def print_cli_result(result: Dict[str, Any]) -> None:
    if result.get("ok") is False:
        print(
            f"FAILED  {result.get('source_path')} -> {result.get('asset_id')}: {result.get('error')}",
            file=sys.stderr,
        )
        return

    state = (result.get("status") or {}).get("state")
    message = (
        f"OK      {result['source_path']} -> {result['asset_id']}"
        f" | task={result.get('task_id')}"
        f" | state={state}"
    )
    if result.get("made_public"):
        message += " | public=yes"
    if result.get("cleanup_warning"):
        message += " | cleanup=warning"
    print(message)
    if result.get("cleanup_warning"):
        print(
            f"WARNING {result['asset_id']}: {result['cleanup_warning']}",
            file=sys.stderr,
        )


def parse_csv_delimiter(raw_value: str) -> str:
    """Map friendly CLI delimiter values to the actual single-character delimiter."""
    normalized = str(raw_value).strip().lower()
    if normalized in {"tab", "\\t", "t"}:
        return "\t"
    if normalized in {"comma", ","}:
        return ","
    if normalized in {"pipe", "|"}:
        return "|"
    if normalized in {"semicolon", ";"}:
        return ";"
    if len(raw_value) == 1:
        return raw_value
    raise GEEUploadError(
        f"Unsupported --csv-delimiter value '{raw_value}'. Use tab, comma, pipe, semicolon, or a single character."
    )


def cli_main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_cli_parser()
    args = parser.parse_args(argv)

    files = args.files or []
    directory = args.directory
    asset_parent = args.asset_parent
    asset_id = args.asset_id
    asset_name = args.asset_name

    if args.gee_account_id and args.service_account_json:
        parser.error("Use either --gee-account-id or --service-account-json, not both.")

    if args.facilities_defaults:
        if not directory:
            directory = str(DEFAULT_FACILITIES_POINT_DIR)
        if not asset_parent and not asset_id:
            asset_parent = DEFAULT_FACILITIES_ASSET_PARENT

    if not files and not directory:
        parser.error("Provide at least one --file or a --directory.")

    if asset_id and (directory or len(files) != 1):
        parser.error("--asset-id can only be used with a single --file upload.")

    if not asset_id and not asset_parent:
        parser.error("Provide --asset-parent for batch uploads or --asset-id for a single file.")

    if args.gee_account_id or not args.service_account_json:
        bootstrap_django_for_cli()

    try:
        asset_properties = parse_cli_properties(args.properties)
        csv_delimiter = parse_csv_delimiter(args.csv_delimiter)
    except GEEUploadError as exc:
        parser.error(str(exc))

    if len(args.csv_qualifier) != 1:
        parser.error("--csv-qualifier must be a single character.")

    effective_wait = bool(args.wait or args.make_public or args.cleanup_gcs)
    common_kwargs = {
        "gee_account_id": args.gee_account_id,
        "service_account_json_path": args.service_account_json,
        "gcs_bucket_name": args.gcs_bucket,
        "gcs_prefix": args.gcs_prefix,
        "replace_existing": args.replace_existing,
        "wait_for_completion": effective_wait,
        "make_public": args.make_public,
        "cleanup_gcs": args.cleanup_gcs,
        "poll_interval_seconds": args.poll_interval,
        "timeout_seconds": args.timeout_seconds,
        "max_vertices": args.max_vertices,
        "max_error_meters": args.max_error_meters,
        "chunk_size_mb": args.chunk_size_mb,
        "asset_properties": asset_properties,
        "csv_delimiter": csv_delimiter,
        "csv_qualifier": args.csv_qualifier,
    }

    if asset_id:
        try:
            result = upload_vector_file_to_gee(
                source_path=files[0],
                asset_id=asset_id,
                asset_name=asset_name,
                **common_kwargs,
            )
        except Exception as exc:
            print(
                f"FAILED  {files[0]} -> {asset_id}: {exc}",
                file=sys.stderr,
            )
            return 1
        print_cli_result(result)
        return 0

    results = upload_vector_files_to_gee(
        file_paths=files,
        input_dir=directory,
        asset_parent=asset_parent,
        continue_on_error=not args.stop_on_error,
        **common_kwargs,
    )

    ok_count = 0
    failed_count = 0
    for result in results:
        print_cli_result(result)
        if result.get("ok"):
            ok_count += 1
        else:
            failed_count += 1

    print(f"Completed uploads: {ok_count} succeeded, {failed_count} failed")
    return 1 if failed_count else 0


if __name__ == "__main__":
    raise SystemExit(cli_main())
