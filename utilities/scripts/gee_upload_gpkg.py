#!/usr/bin/env python3
"""Upload a GeoPackage file to Google Earth Engine as a table asset.

This is a focused path for GPKG assets: it uploads the local file to GCS,
submits an Earth Engine table-ingestion manifest that points at the GPKG, and
optionally waits, cleans up the staged GCS object, and makes the asset public.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
import sys
import time
import uuid
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from nrm_app.settings import GCS_BUCKET_NAME
from utilities.scripts.gee_upload import (
    GEEUploadError,
    build_gcs_client,
    delete_gcs_blob,
    delete_gee_asset_if_exists,
    ensure_gee_folder_path,
    format_bytes,
    gee_asset_exists,
    initialize_gee_session,
    log_progress,
    make_gee_asset_public,
    preflight_gcs_bucket_access,
    sanitize_gee_asset_name,
    start_gee_table_ingestion,
    verify_gcs_blob_read_access,
    wait_for_gee_task,
)


DEFAULT_SERVICE_ACCOUNT_JSON = (
    REPO_ROOT / "data/gee_confs/core-stack-learn-818963fa8f26.json"
)
DEFAULT_INPUT_GPKG = REPO_ROOT / "data/livestock/livestock_asset.gpkg"
DEFAULT_ASSET_ID = "projects/corestack-datasets/assets/datasets/pan_india_livestocks"
DEFAULT_GCS_PREFIX = "gee/corestack_uploads"
DEFAULT_GPKG_GCS_CHUNK_SIZE_MB = 8
DEFAULT_GPKG_COMPOSITE_PART_SIZE_MB = 32
DEFAULT_GPKG_COMPOSITE_WORKERS = 4
DEFAULT_GPKG_GCS_UPLOAD_TIMEOUT = 300
DEFAULT_GPKG_GCS_UPLOAD_RETRY_DEADLINE = 3600


def parse_source_options(values: list[str]) -> dict[str, Any]:
    """Parse KEY=VALUE manifest source options.

    Values are JSON-decoded when possible, so numbers, booleans, arrays, and
    objects can be passed without adding custom flags for every manifest field.
    """

    options: dict[str, Any] = {}
    for value in values:
        if "=" not in value:
            raise argparse.ArgumentTypeError(
                f"Expected KEY=VALUE for --source-option, got {value!r}"
            )
        key, raw = value.split("=", 1)
        key = key.strip()
        if not key:
            raise argparse.ArgumentTypeError("--source-option key cannot be empty")
        try:
            options[key] = json.loads(raw)
        except json.JSONDecodeError:
            options[key] = raw
    return options


def build_manifest(
    *,
    asset_id: str,
    gcs_uri: str,
    primary_geometry_column: str | None = None,
    max_vertices: int | None = None,
    max_error_meters: float | None = None,
    charset: str | None = None,
    source_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source: dict[str, Any] = {"uris": [gcs_uri]}

    if charset:
        source["charset"] = charset
    if primary_geometry_column:
        source["primaryGeometryColumn"] = primary_geometry_column
    if max_vertices is not None:
        source["maxVertices"] = max_vertices
    if max_error_meters is not None:
        source["maxErrorMeters"] = max_error_meters
    if source_options:
        source.update(source_options)

    return {
        "name": asset_id,
        "sources": [source],
        "properties": {
            "source_format": "GPKG",
            "uploaded_by": "utilities/scripts/gee_upload_gpkg.py",
        },
    }


def build_staging_blob_name(input_path: Path, gcs_prefix: str) -> str:
    safe_stem = sanitize_gee_asset_name(input_path.stem)
    suffix = uuid.uuid4().hex[:12]
    prefix = gcs_prefix.strip("/")
    return f"{prefix}/{safe_stem}_{suffix}{input_path.suffix.lower()}"


def upload_gpkg_file_to_gcs(
    *,
    local_file_path: Path,
    destination_blob_name: str,
    credentials,
    key_dict: dict[str, Any],
    bucket_name: str,
    chunk_size_mb: int,
    upload_timeout: int,
    upload_retry_deadline: int,
) -> str:
    from google.cloud.storage.retry import DEFAULT_RETRY

    client = build_gcs_client(credentials, key_dict)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.chunk_size = max(256 * 1024, chunk_size_mb * 1024 * 1024)

    file_size = local_file_path.stat().st_size
    retry = DEFAULT_RETRY.with_deadline(upload_retry_deadline)
    log_progress(
        f"Uploading staged GPKG to gs://{bucket_name}/{destination_blob_name} "
        f"({format_bytes(file_size)}, chunk_size={format_bytes(blob.chunk_size)}, "
        f"timeout={upload_timeout}s, retry_deadline={upload_retry_deadline}s)"
    )

    started = time.monotonic()
    blob.upload_from_filename(
        str(local_file_path),
        timeout=upload_timeout,
        retry=retry,
    )
    verify_gcs_blob_read_access(blob)
    elapsed = time.monotonic() - started
    rate = file_size / elapsed if elapsed > 0 else 0.0
    log_progress(
        f"Finished GCS upload in {elapsed:.1f}s "
        f"({format_bytes(int(rate))}/s average)"
    )
    return f"gs://{bucket_name}/{destination_blob_name}"


def compose_gcs_blobs(bucket, source_blobs: list[Any], destination_blob_name: str):
    """Compose source blobs into destination, respecting GCS's 32-source limit."""

    generation = 0
    current_sources = source_blobs
    intermediate_blobs: list[Any] = []

    while len(current_sources) > 32:
        next_sources: list[Any] = []
        for batch_index in range(0, len(current_sources), 32):
            batch = current_sources[batch_index : batch_index + 32]
            composed_name = (
                f"{destination_blob_name}.compose/"
                f"{uuid.uuid4().hex}/generation-{generation:02d}-"
                f"{batch_index // 32:05d}"
            )
            composed_blob = bucket.blob(composed_name)
            composed_blob.compose(batch)
            intermediate_blobs.append(composed_blob)
            next_sources.append(composed_blob)
        current_sources = next_sources
        generation += 1

    destination_blob = bucket.blob(destination_blob_name)
    destination_blob.compose(current_sources)
    return destination_blob, intermediate_blobs


class OffsetLimitedReader:
    """Read a fixed byte range from a file while presenting a zero-based stream."""

    def __init__(self, path: Path, offset: int, size: int):
        self._path = path
        self._offset = offset
        self._size = size
        self._position = 0
        self._handle = path.open("rb")
        self._handle.seek(offset)

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._position

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            target = offset
        elif whence == 1:
            target = self._position + offset
        elif whence == 2:
            target = self._size + offset
        else:
            raise ValueError(f"Unsupported whence: {whence}")

        target = max(0, min(self._size, target))
        self._handle.seek(self._offset + target)
        self._position = target
        return self._position

    def read(self, size: int = -1) -> bytes:
        remaining = self._size - self._position
        if remaining <= 0:
            return b""
        if size is None or size < 0:
            size = remaining
        else:
            size = min(size, remaining)
        chunk = self._handle.read(size)
        self._position += len(chunk)
        return chunk

    def close(self) -> None:
        self._handle.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


def upload_gpkg_file_to_gcs_composite(
    *,
    local_file_path: Path,
    destination_blob_name: str,
    credentials,
    key_dict: dict[str, Any],
    bucket_name: str,
    part_size_mb: int,
    workers: int,
    upload_timeout: int,
    upload_retry_deadline: int,
) -> str:
    from google.cloud.storage.retry import DEFAULT_RETRY

    client = build_gcs_client(credentials, key_dict)
    bucket = client.bucket(bucket_name)
    file_size = local_file_path.stat().st_size
    part_size = max(256 * 1024, part_size_mb * 1024 * 1024)
    part_count = max(1, math.ceil(file_size / part_size))
    workers = max(1, min(workers, part_count))
    retry = DEFAULT_RETRY.with_deadline(upload_retry_deadline)

    upload_id = uuid.uuid4().hex
    component_prefix = f"{destination_blob_name}.components/{upload_id}"
    component_names = [
        f"{component_prefix}/part-{part_index:05d}" for part_index in range(part_count)
    ]
    component_blobs: list[Any] = []
    intermediate_blobs: list[Any] = []

    log_progress(
        f"Uploading staged GPKG to gs://{bucket_name}/{destination_blob_name} "
        f"with parallel composite upload ({format_bytes(file_size)}, "
        f"{part_count} parts x {format_bytes(part_size)}, workers={workers}, "
        f"timeout={upload_timeout}s, retry_deadline={upload_retry_deadline}s)"
    )

    def upload_part(part_index: int):
        offset = part_index * part_size
        size = min(part_size, file_size - offset)
        blob = bucket.blob(component_names[part_index])
        blob.chunk_size = max(256 * 1024, min(part_size, 8 * 1024 * 1024))
        with OffsetLimitedReader(local_file_path, offset, size) as handle:
            blob.upload_from_file(
                handle,
                size=size,
                rewind=False,
                content_type="application/octet-stream",
                timeout=upload_timeout,
                retry=retry,
            )
        return part_index, blob

    started = time.monotonic()
    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(upload_part, part_index): part_index
                for part_index in range(part_count)
            }
            completed = 0
            for future in as_completed(futures):
                part_index, blob = future.result()
                component_blobs.append(blob)
                completed += 1
                if completed == part_count or completed % max(1, part_count // 10) == 0:
                    log_progress(
                        f"Uploaded GCS component {completed}/{part_count} "
                        f"(latest part {part_index + 1})"
                    )

        component_blobs.sort(key=lambda blob: blob.name)
        log_progress(f"Composing {len(component_blobs)} GCS components")
        destination_blob, intermediate_blobs = compose_gcs_blobs(
            bucket, component_blobs, destination_blob_name
        )
        verify_gcs_blob_read_access(destination_blob)

        elapsed = time.monotonic() - started
        rate = file_size / elapsed if elapsed > 0 else 0.0
        log_progress(
            f"Finished GCS composite upload in {elapsed:.1f}s "
            f"({format_bytes(int(rate))}/s average)"
        )
        return f"gs://{bucket_name}/{destination_blob_name}"
    finally:
        cleanup_blobs = component_blobs + intermediate_blobs
        if cleanup_blobs:
            log_progress(f"Deleting {len(cleanup_blobs)} temporary GCS component objects")
            cleanup_failures = 0
            for blob in cleanup_blobs:
                try:
                    blob.delete()
                except Exception:
                    cleanup_failures += 1
            if cleanup_failures:
                log_progress(
                    f"Could not delete {cleanup_failures}/{len(cleanup_blobs)} "
                    "temporary GCS component objects. The service account may lack "
                    "storage.objects.delete permission."
                )


def delete_gcs_blob_if_permitted(
    credentials,
    key_dict: dict[str, Any],
    bucket_name: str,
    blob_name: str,
) -> bool:
    try:
        delete_gcs_blob(credentials, key_dict, bucket_name, blob_name)
        return True
    except Exception as exc:
        log_progress(
            f"Could not delete staged GCS object gs://{bucket_name}/{blob_name}: {exc}"
        )
        return False


def upload_gpkg_to_gee(args: argparse.Namespace) -> dict[str, Any]:
    input_path = Path(args.file).expanduser().resolve()
    service_account_json = Path(args.service_account_json).expanduser().resolve()
    asset_id = args.asset_id.strip()
    gcs_prefix = args.gcs_prefix.strip("/")

    if not input_path.exists():
        raise FileNotFoundError(f"GPKG file does not exist: {input_path}")
    if input_path.suffix.lower() != ".gpkg":
        raise ValueError(f"Expected a .gpkg file, got: {input_path}")
    if not service_account_json.exists():
        raise FileNotFoundError(
            f"Service account JSON does not exist: {service_account_json}"
        )
    if not asset_id:
        raise ValueError("--asset-id cannot be empty")

    log_progress(
        f"Preparing direct GPKG upload: {input_path} ({format_bytes(input_path.stat().st_size)})"
    )

    ee_module, credentials, key_dict = initialize_gee_session(
        service_account_json_path=str(service_account_json)
    )
    # Construct the client once here so auth problems surface before the long upload starts.
    build_gcs_client(credentials, key_dict)
    preflight_gcs_bucket_access(credentials, key_dict, args.gcs_bucket, gcs_prefix)

    if gee_asset_exists(ee_module, asset_id):
        if not args.replace_existing:
            raise GEEUploadError(
                f"GEE asset already exists: {asset_id}. Pass --replace-existing to overwrite it."
            )
        delete_gee_asset_if_exists(ee_module, asset_id)

    asset_parent = asset_id.rsplit("/", 1)[0]
    ensure_gee_folder_path(ee_module, asset_parent)

    blob_name = build_staging_blob_name(input_path, gcs_prefix)
    if args.gcs_uploader == "composite":
        gcs_uri = upload_gpkg_file_to_gcs_composite(
            local_file_path=input_path,
            destination_blob_name=blob_name,
            credentials=credentials,
            key_dict=key_dict,
            bucket_name=args.gcs_bucket,
            part_size_mb=args.composite_part_size_mb,
            workers=args.composite_workers,
            upload_timeout=args.gcs_upload_timeout,
            upload_retry_deadline=args.gcs_upload_retry_deadline,
        )
    else:
        gcs_uri = upload_gpkg_file_to_gcs(
            local_file_path=input_path,
            destination_blob_name=blob_name,
            credentials=credentials,
            key_dict=key_dict,
            bucket_name=args.gcs_bucket,
            chunk_size_mb=args.chunk_size_mb,
            upload_timeout=args.gcs_upload_timeout,
            upload_retry_deadline=args.gcs_upload_retry_deadline,
        )

    source_options = parse_source_options(args.source_option)
    manifest = build_manifest(
        asset_id=asset_id,
        gcs_uri=gcs_uri,
        primary_geometry_column=args.primary_geometry_column,
        max_vertices=args.max_vertices,
        max_error_meters=args.max_error_meters,
        charset=args.charset,
        source_options=source_options,
    )

    log_progress("Submitting Earth Engine table ingestion manifest")
    ingestion_response = start_gee_table_ingestion(ee_module, manifest)
    operation_name = ingestion_response.get("operation_name")
    task_id = ingestion_response.get("task_id") or operation_name

    result: dict[str, Any] = {
        "asset_id": asset_id,
        "gcs_uri": gcs_uri,
        "manifest": manifest,
        "ingestion_response": ingestion_response,
    }

    should_wait = args.wait or args.cleanup_gcs or args.make_public
    if should_wait:
        task_status = wait_for_gee_task(
            ee_module,
            task_id=task_id,
            operation_name=operation_name,
            poll_interval_seconds=args.poll_interval,
            timeout_seconds=args.timeout,
        )
        terminal_state = task_status.get("state")
        result["terminal_state"] = terminal_state
        result["task_status"] = task_status

        if terminal_state != "SUCCEEDED":
            raise GEEUploadError(
                f"GEE ingestion did not succeed for {asset_id}: {terminal_state}"
            )

        if args.make_public:
            make_gee_asset_public(ee_module, asset_id)
            result["made_public"] = True

        if args.cleanup_gcs:
            if delete_gcs_blob_if_permitted(
                credentials, key_dict, args.gcs_bucket, blob_name
            ):
                result["cleaned_gcs_uri"] = gcs_uri
            else:
                result["cleanup_gcs_failed"] = True
                result["staged_gcs_uri"] = gcs_uri

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload a GeoPackage directly to Google Earth Engine as a table asset.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--file",
        default=str(DEFAULT_INPUT_GPKG),
        help="Local .gpkg file to upload.",
    )
    parser.add_argument(
        "--asset-id",
        default=DEFAULT_ASSET_ID,
        help="Destination Earth Engine asset ID.",
    )
    parser.add_argument(
        "--service-account-json",
        default=str(DEFAULT_SERVICE_ACCOUNT_JSON),
        help="Service account JSON used to authenticate Earth Engine and GCS.",
    )
    parser.add_argument(
        "--gcs-bucket",
        default=GCS_BUCKET_NAME,
        help="GCS bucket used to stage the GPKG before ingestion.",
    )
    parser.add_argument(
        "--gcs-prefix",
        default=DEFAULT_GCS_PREFIX,
        help="GCS prefix for temporary staged uploads.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Delete the destination GEE asset first if it already exists.",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait until the Earth Engine ingestion task finishes.",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Seconds between task-status checks when waiting.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Maximum seconds to wait for ingestion; omit for no timeout.",
    )
    parser.add_argument(
        "--cleanup-gcs",
        action="store_true",
        help="Delete the staged GCS object after successful ingestion. Implies --wait.",
    )
    parser.add_argument(
        "--make-public",
        action="store_true",
        help="Make the GEE asset public after successful ingestion. Implies --wait.",
    )
    parser.add_argument(
        "--gcs-uploader",
        choices=["composite", "resumable"],
        default="composite",
        help="GCS staging upload strategy.",
    )
    parser.add_argument(
        "--chunk-size-mb",
        type=int,
        default=DEFAULT_GPKG_GCS_CHUNK_SIZE_MB,
        help="GCS resumable upload chunk size in MiB when --gcs-uploader=resumable.",
    )
    parser.add_argument(
        "--composite-part-size-mb",
        type=int,
        default=DEFAULT_GPKG_COMPOSITE_PART_SIZE_MB,
        help="Part size in MiB when --gcs-uploader=composite.",
    )
    parser.add_argument(
        "--composite-workers",
        type=int,
        default=DEFAULT_GPKG_COMPOSITE_WORKERS,
        help="Parallel upload workers when --gcs-uploader=composite.",
    )
    parser.add_argument(
        "--gcs-upload-timeout",
        type=int,
        default=DEFAULT_GPKG_GCS_UPLOAD_TIMEOUT,
        help="Per-request timeout in seconds for each GCS upload chunk.",
    )
    parser.add_argument(
        "--gcs-upload-retry-deadline",
        type=int,
        default=DEFAULT_GPKG_GCS_UPLOAD_RETRY_DEADLINE,
        help="Total retry deadline in seconds for the GCS upload.",
    )
    parser.add_argument(
        "--primary-geometry-column",
        default=None,
        help="Optional manifest primaryGeometryColumn, for example geom.",
    )
    parser.add_argument(
        "--max-vertices",
        type=int,
        default=None,
        help="Optional Earth Engine maxVertices source setting.",
    )
    parser.add_argument(
        "--max-error-meters",
        type=float,
        default=None,
        help="Optional Earth Engine maxErrorMeters source setting.",
    )
    parser.add_argument(
        "--charset",
        default=None,
        help="Optional source charset manifest field. Usually unnecessary for GPKG.",
    )
    parser.add_argument(
        "--source-option",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra manifest source option. VALUE is JSON-decoded when possible.",
    )
    parser.add_argument(
        "--summary-json",
        default=None,
        help="Optional path to write the upload summary JSON.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = upload_gpkg_to_gee(args)

    summary = json.dumps(result, indent=2, sort_keys=True, default=str)
    if args.summary_json:
        summary_path = Path(args.summary_json).expanduser().resolve()
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(summary + "\n", encoding="utf-8")
        log_progress(f"Wrote upload summary to {summary_path}")
    print(summary)


if __name__ == "__main__":
    main()
