#!/usr/bin/env python3
"""Upload the merged pan-India GeoPackages to Earth Engine.

This is step 3 of the pan-India asset build, following the ingestion path
proven by ``utilities/scripts/gee/core_stack_gee_ingest.py``:

1. Export a bounded-memory tab-delimited table with one GeoJSON geometry column.
2. Stage the same bytes to GCS with a ``.csv`` object suffix.
3. Start an Earth Engine table ingestion task from a manifest.

Every asset here is a plain GeoPackage feature layer produced by
``merge_outputs.py``, so all columns are ingested unchanged and the Earth
Engine tables keep exactly the structure of the tehsil/district API outputs.

Example:
    uv run --with pyyaml --with shapely \
      python utilities/scripts/build_pan_india_gee_assets/gee_ingest.py build --asset all
    uv run --with earthengine-api --with google-cloud-storage --with pyyaml --with shapely \
      python utilities/scripts/build_pan_india_gee_assets/gee_ingest.py upload --asset all
"""

from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
import math
from pathlib import Path
import resource
import sqlite3
import time
from typing import Any, Sequence
import uuid

import yaml


log = logging.getLogger("pan_india_gee_ingest")

DEFAULT_CONFIG = Path(__file__).with_name("pan_india_assets.yaml")
TERMINAL_TASK_STATES = {"SUCCEEDED", "COMPLETED", "FAILED", "CANCELLED"}
SUCCESS_TASK_STATES = {"SUCCEEDED", "COMPLETED"}
GEE_OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/earthengine",
    "https://www.googleapis.com/auth/devstorage.full_control",
]
DEFAULT_MAX_RSS_MB = 5_000


def setup_logging(debug: bool = False, log_file: Path | None = None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s  [%(levelname)-7s]  %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
    )


def find_repo_root(start: Path) -> Path:
    for candidate in [start, *start.parents]:
        if (candidate / ".git").exists() or (candidate / "manage.py").exists():
            return candidate
    return start


def repo_path(repo_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else repo_root / path


def read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False, allow_unicode=True)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def sql_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    out: list[str] = []
    previous_underscore = False
    for char in text:
        if char.isalnum():
            out.append(char)
            previous_underscore = False
        elif not previous_underscore:
            out.append("_")
            previous_underscore = True
    return "".join(out).strip("_")


def max_rss_mb() -> float:
    # Linux ru_maxrss is KiB.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def enforce_memory(max_rss_mb_limit: int, label: str) -> None:
    rss = max_rss_mb()
    if rss > max_rss_mb_limit:
        raise MemoryError(f"{label} exceeded max RSS {max_rss_mb_limit} MB; current RSS={rss:.1f} MB")


def json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def csv_safe(value: Any) -> Any:
    value = json_safe(value)
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, str):
        return value.replace("\r\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")
    return value


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def with_suffix_token(path: Path, token: str | None) -> Path:
    if not token:
        return path
    clean = token if token.startswith(".") else f".{token}"
    return path.with_name(f"{path.stem}{clean}{path.suffix}")


class DelimitedTableWriter:
    def __init__(self, path: Path, fieldnames: Sequence[str], delimiter: str, quotechar: str):
        self.path = path
        self.fieldnames = list(fieldnames)
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.handle = None
        self.writer = None
        self.rows_written = 0

    def __enter__(self) -> "DelimitedTableWriter":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("w", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(
            self.handle,
            fieldnames=self.fieldnames,
            delimiter=self.delimiter,
            quotechar=self.quotechar,
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
            extrasaction="ignore",
        )
        self.writer.writeheader()
        return self

    def write(self, row: dict[str, Any]) -> None:
        assert self.writer is not None
        self.writer.writerow({field: csv_safe(row.get(field)) for field in self.fieldnames})
        self.rows_written += 1

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.handle is not None:
            self.handle.close()


def maybe_log_progress(label: str, rows: int, started: float, last_logged: float, every_rows: int = 100_000) -> float:
    now = time.monotonic()
    if rows == 1 or rows % every_rows == 0 or (now - last_logged) > 30:
        elapsed = max(now - started, 0.001)
        log.info("%s: %s rows in %.1fs; rss=%.1f MB", label, f"{rows:,}", elapsed, max_rss_mb())
        return now
    return last_logged


def gpkg_geometry_to_geojson(blob: bytes | memoryview | None) -> dict[str, Any] | None:
    if blob is None:
        return None
    from shapely import wkb
    from shapely.geometry import mapping

    data = bytes(blob)
    if data[:2] != b"GP":
        geom = wkb.loads(data)
    else:
        flags = data[3]
        if flags & 0b00010000:
            return None
        envelope_code = (flags >> 1) & 0b00000111
        envelope_bytes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get(envelope_code)
        if envelope_bytes is None:
            raise ValueError(f"Unsupported GeoPackage geometry envelope code: {envelope_code}")
        geom = wkb.loads(data[8 + envelope_bytes :])
    if geom.is_empty:
        return None
    return mapping(geom)


def gpkg_geometry_column(con: sqlite3.Connection, layer: str) -> str:
    row = con.execute(
        "SELECT column_name FROM gpkg_geometry_columns WHERE table_name = ?",
        (layer,),
    ).fetchone()
    return str(row[0]) if row else "geom"


def gpkg_table_columns(con: sqlite3.Connection, layer: str) -> list[str]:
    return [str(row[1]) for row in con.execute(f"PRAGMA table_info({sql_ident(layer)})")]


class IngestConfig:
    """Unified pan-India config resolved for GEE ingestion."""

    def __init__(self, config_path: Path):
        self.config_path = config_path.resolve()
        self.repo_root = find_repo_root(self.config_path)
        config = read_yaml(self.config_path)
        gee_cfg = config.get("gee") or {}
        self.outputs_cfg: dict[str, Any] = gee_cfg.get("outputs") or {}
        self.upload_cfg: dict[str, Any] = gee_cfg.get("upload") or {}
        self.service_account_json = gee_cfg.get("service_account_json")
        merge_dir = repo_path(self.repo_root, (config.get("merge") or {}).get("output_dir", "data/pan_india_gee_assets/outputs"))
        self.assets: dict[str, dict[str, Any]] = {}
        for name, asset_cfg in (config.get("assets") or {}).items():
            self.assets[name] = {
                "asset_id": asset_cfg["asset_id"],
                "source": merge_dir / asset_cfg["gpkg"],
                "layer": asset_cfg["layer"],
                "geometry_column": asset_cfg.get("geometry_column", "geometry"),
            }

    def selected(self, requested: str) -> list[tuple[str, dict[str, Any]]]:
        if requested == "all":
            return list(self.assets.items())
        if requested not in self.assets:
            raise KeyError(f"Unknown asset {requested!r}; choose one of: all, {', '.join(self.assets)}")
        return [(requested, self.assets[requested])]

    def table_path(self, asset_name: str, suffix: str | None) -> Path:
        directory = repo_path(self.repo_root, self.outputs_cfg.get("directory", "data/pan_india_gee_assets/gee"))
        extension = self.outputs_cfg.get("local_extension", ".tsv")
        return with_suffix_token(directory / f"{asset_name}{extension}", suffix)

    def source_path(self, asset_cfg: dict[str, Any], suffix: str | None) -> Path:
        """Prefer a suffixed merged GeoPackage (smoke runs) when it exists."""

        source = Path(asset_cfg["source"])
        if suffix:
            suffixed = with_suffix_token(source, suffix)
            if suffixed.exists():
                return suffixed
        return source

    def summary_path(self, key: str, default: str, override: str | None = None, suffix: str | None = None) -> Path:
        path = repo_path(self.repo_root, override or self.outputs_cfg.get(key, default))
        return with_suffix_token(path, suffix)


def load_config(args: argparse.Namespace) -> IngestConfig:
    return IngestConfig(args.config)


def output_delimiter(upload_cfg: dict[str, Any]) -> str:
    return str(upload_cfg.get("csv_delimiter", "\t"))


def output_quotechar(upload_cfg: dict[str, Any]) -> str:
    return str(upload_cfg.get("csv_qualifier", '"'))


def export_gpkg_feature_layer(
    config: IngestConfig,
    asset_name: str,
    asset_cfg: dict[str, Any],
    *,
    limit: int | None,
    chunk_size: int,
    overwrite: bool,
    output_suffix: str | None,
    max_rss_mb_limit: int,
) -> dict[str, Any]:
    source_path = config.source_path(asset_cfg, output_suffix)
    if not source_path.exists():
        raise FileNotFoundError(f"Merged GeoPackage missing for {asset_name}: {source_path}. Run merge_outputs first.")
    layer = asset_cfg["layer"]
    output = config.table_path(asset_name, output_suffix)
    if output.exists() and not overwrite:
        raise FileExistsError(f"Output exists; pass --overwrite to replace: {output}")

    started = time.monotonic()
    with sqlite3.connect(source_path) as con:
        con.row_factory = sqlite3.Row
        geom_col = gpkg_geometry_column(con, layer)
        selected_columns = [column for column in gpkg_table_columns(con, layer) if column not in {"fid", geom_col}]
        geometry_field = asset_cfg.get("geometry_column", "geometry")
        fieldnames = [*selected_columns, geometry_field]
        select_sql = ", ".join(["fid", *[sql_ident(column) for column in selected_columns], sql_ident(geom_col)])
        total_rows = int(con.execute(f"SELECT COUNT(*) FROM {sql_ident(layer)}").fetchone()[0])
        remaining = limit
        last_fid = 0
        rows_written = 0
        last_logged = started
        with DelimitedTableWriter(output, fieldnames, output_delimiter(config.upload_cfg), output_quotechar(config.upload_cfg)) as writer:
            while True:
                current_chunk = chunk_size if remaining is None else min(chunk_size, remaining)
                if current_chunk <= 0:
                    break
                rows = con.execute(
                    f"""
                    SELECT {select_sql}
                    FROM {sql_ident(layer)}
                    WHERE fid > ?
                    ORDER BY fid
                    LIMIT ?
                    """,
                    (last_fid, current_chunk),
                ).fetchall()
                if not rows:
                    break
                last_fid = int(rows[-1]["fid"])
                for row in rows:
                    geometry = gpkg_geometry_to_geojson(row[geom_col])
                    if geometry is None:
                        continue
                    out = {column: json_safe(row[column]) for column in selected_columns}
                    out[geometry_field] = compact_json(geometry)
                    writer.write(out)
                    rows_written += 1
                    last_logged = maybe_log_progress(asset_name, rows_written, started, last_logged)
                enforce_memory(max_rss_mb_limit, asset_name)
                if remaining is not None:
                    remaining -= len(rows)

    return {
        "asset": asset_name,
        "source": str(source_path),
        "layer": layer,
        "rows_source": total_rows,
        "rows": rows_written,
        "columns": fieldnames,
        "output": str(output),
        "size_bytes": output.stat().st_size,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "max_rss_mb": round(max_rss_mb(), 1),
    }


def build_assets(args: argparse.Namespace) -> dict[str, Any]:
    config = load_config(args)
    assets = config.selected(args.asset)
    summary = {
        "built_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "config": str(config.config_path),
        "max_rss_mb_limit": args.max_rss_mb,
        "assets": {},
    }
    jobs = max(1, int(args.jobs or 1))
    if jobs == 1 or len(assets) <= 1:
        for name, asset_cfg in assets:
            summary["assets"][name] = export_gpkg_feature_layer(
                config,
                name,
                asset_cfg,
                limit=args.limit,
                chunk_size=args.chunk_size,
                overwrite=args.overwrite,
                output_suffix=args.output_suffix,
                max_rss_mb_limit=args.max_rss_mb,
            )
    else:
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            futures = {
                executor.submit(
                    export_gpkg_feature_layer,
                    config,
                    name,
                    asset_cfg,
                    limit=args.limit,
                    chunk_size=args.chunk_size,
                    overwrite=args.overwrite,
                    output_suffix=args.output_suffix,
                    max_rss_mb_limit=args.max_rss_mb,
                ): name
                for name, asset_cfg in assets
            }
            for future in as_completed(futures):
                summary["assets"][futures[future]] = future.result()
    summary_path = config.summary_path(
        "build_summary_yaml",
        "data/pan_india_gee_assets/gee/pan_india_gee_build_summary.yaml",
        suffix=args.output_suffix,
    )
    write_yaml(summary_path, summary)
    log.info("Wrote build summary: %s", summary_path)
    return summary


def initialize_ee(service_account_json: Path) -> tuple[Any, Any, dict[str, Any]]:
    try:
        import ee
        from google.oauth2 import service_account
    except ImportError as exc:
        raise RuntimeError(
            "Upload requires earthengine-api and google-cloud-storage. "
            "Run with `uv run --with earthengine-api --with google-cloud-storage --with pyyaml --with shapely ...`."
        ) from exc
    key = json.loads(service_account_json.read_text(encoding="utf-8"))
    credentials = service_account.Credentials.from_service_account_file(service_account_json, scopes=GEE_OAUTH_SCOPES)
    ee.Initialize(credentials=credentials, project=key.get("project_id"))
    if hasattr(ee.data, "setUserAgent"):
        ee.data.setUserAgent("pan-india-gee-ingest")
    return ee, credentials, key


def gee_asset_exists(ee_module: Any, asset_id: str) -> bool:
    try:
        ee_module.data.getAsset(asset_id)
        return True
    except Exception:
        return False


def ensure_gee_folder_path(ee_module: Any, folder_path: str) -> None:
    if "/assets" not in folder_path:
        raise ValueError(f"Invalid Earth Engine asset path: {folder_path}")
    prefix, suffix = folder_path.rstrip("/").split("/assets", 1)
    current = f"{prefix}/assets"
    for part in [segment for segment in suffix.split("/") if segment]:
        current = f"{current}/{part}"
        if gee_asset_exists(ee_module, current):
            continue
        log.info("Creating Earth Engine folder: %s", current)
        try:
            ee_module.data.createAsset({"type": "FOLDER"}, current)
        except Exception:
            # Parallel uploads can race while creating the shared folder path.
            if not gee_asset_exists(ee_module, current):
                raise
        time.sleep(1)


def upload_file_to_gcs(local_path: Path, credentials: Any, key: dict[str, Any], bucket_name: str, blob_name: str, chunk_size_mb: int) -> str:
    from google.cloud import storage

    client = storage.Client(project=key.get("project_id"), credentials=credentials)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.chunk_size = max(256 * 1024, int(chunk_size_mb) * 1024 * 1024)
    log.info("Uploading %s to gs://%s/%s", local_path, bucket_name, blob_name)
    blob.upload_from_filename(str(local_path))
    blob.reload()
    return f"gs://{bucket_name}/{blob_name}"


def delete_gcs_blob(credentials: Any, key: dict[str, Any], bucket_name: str, blob_name: str) -> bool:
    from google.api_core.exceptions import Forbidden, NotFound
    from google.cloud import storage

    bucket = storage.Client(project=key.get("project_id"), credentials=credentials).bucket(bucket_name)
    try:
        bucket.blob(blob_name).delete()
        return True
    except NotFound:
        return False
    except Forbidden as exc:
        log.warning("Could not delete gs://%s/%s: %s", bucket_name, blob_name, getattr(exc, "message", exc))
        return False


def ingestion_manifest(asset_id: str, gcs_uri: str, geometry_column: str, upload_cfg: dict[str, Any], properties: dict[str, Any]) -> dict[str, Any]:
    source = {
        "uris": [gcs_uri],
        "charset": "UTF-8",
        "primaryGeometryColumn": geometry_column,
        "maxErrorMeters": float(upload_cfg.get("max_error_meters", 1.0)),
        "crs": upload_cfg.get("crs", "EPSG:4326"),
        "geodesic": bool(upload_cfg.get("geodesic", False)),
        "csvDelimiter": upload_cfg.get("csv_delimiter", "\t"),
        "csvQualifier": upload_cfg.get("csv_qualifier", '"'),
    }
    if int(upload_cfg.get("max_vertices", 0) or 0):
        source["maxVertices"] = int(upload_cfg["max_vertices"])
    return {"name": asset_id, "sources": [source], "properties": properties}


def start_table_ingestion(ee_module: Any, manifest: dict[str, Any]) -> dict[str, Any]:
    task_id = None
    if hasattr(ee_module.data, "newTaskId"):
        generated = ee_module.data.newTaskId()
        task_id = generated[0] if isinstance(generated, (list, tuple)) else generated
    response = ee_module.data.startTableIngestion(task_id, manifest)
    operation_name = response if isinstance(response, str) else response.get("name") or response.get("id")
    if not task_id and operation_name:
        task_id = operation_name.rsplit("/", 1)[-1]
    return {"task_id": task_id, "operation_name": operation_name, "raw_response": response}


def fetch_task_status(ee_module: Any, task_id: str | None, operation_name: str | None) -> dict[str, Any]:
    operations = ee_module.data.listOperations()
    if isinstance(operations, dict):
        operations = operations.get("operations", [])
    for operation in operations or []:
        name = operation.get("name")
        short_id = name.rsplit("/", 1)[-1] if name else None
        if (operation_name and name == operation_name) or (task_id and short_id == task_id):
            metadata = operation.get("metadata") or {}
            return {
                "task_id": short_id,
                "operation_name": name,
                "state": metadata.get("state"),
                "metadata": metadata,
                "error": operation.get("error"),
                "error_message": (operation.get("error") or {}).get("message"),
            }
    return {"task_id": task_id, "operation_name": operation_name, "state": None}


def wait_for_task(ee_module: Any, task_id: str | None, operation_name: str | None, poll_interval: int, timeout: int | None) -> dict[str, Any]:
    started = time.monotonic()
    while True:
        status = fetch_task_status(ee_module, task_id, operation_name)
        log.info("GEE task status: %s", status)
        if status.get("state") in TERMINAL_TASK_STATES:
            return status
        if timeout and (time.monotonic() - started) > timeout:
            raise TimeoutError(f"Timed out waiting for GEE task {task_id or operation_name}")
        time.sleep(max(1, poll_interval))


def make_asset_public(ee_module: Any, asset_id: str) -> bool:
    try:
        acl = ee_module.data.getAssetAcl(asset_id)
        acl["all_users_can_read"] = True
        ee_module.data.setAssetAcl(asset_id, acl)
        return bool(ee_module.data.getAssetAcl(asset_id).get("all_users_can_read"))
    except Exception as exc:
        log.warning("Could not make %s public: %s", asset_id, exc)
        return False


def resolve_service_account(config: IngestConfig, args: argparse.Namespace) -> Path:
    value = args.service_account_json or config.service_account_json
    if not value:
        raise ValueError("No service account configured; set gee.service_account_json or pass --service-account-json")
    return repo_path(config.repo_root, value)


def upload_one_asset(
    config: IngestConfig,
    name: str,
    asset_cfg: dict[str, Any],
    service_account_json: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    local_file = config.table_path(name, args.output_suffix)
    if not local_file.exists():
        raise FileNotFoundError(f"Build output missing for {name}: {local_file}")
    upload_cfg = config.upload_cfg
    asset_id = asset_cfg["asset_id"]
    bucket_name = args.gcs_bucket or upload_cfg.get("gcs_bucket", "core_stack")
    gcs_extension = str(upload_cfg.get("gcs_object_extension") or ".csv")
    if not gcs_extension.startswith("."):
        gcs_extension = f".{gcs_extension}"
    blob_name = (
        f"{str(upload_cfg.get('gcs_prefix', 'gee/pan_india_assets')).strip('/')}/"
        f"{slug(name)}_{uuid.uuid4().hex}{gcs_extension}"
    )
    gcs_uri = f"gs://{bucket_name}/{blob_name}"
    manifest = ingestion_manifest(
        asset_id,
        gcs_uri,
        asset_cfg.get("geometry_column", "geometry"),
        upload_cfg,
        {
            "corestack_asset": name,
            "source_file": local_file.name,
            "source_config": str(args.config),
            "built_by": "utilities/scripts/build_pan_india_gee_assets/gee_ingest.py",
        },
    )
    manifest_path = repo_path(
        config.repo_root,
        config.outputs_cfg.get("manifest_dir", "data/pan_india_gee_assets/gee/manifests"),
    ) / f"{name}.manifest.json"
    write_json(manifest_path, manifest)
    if args.dry_run:
        return {"asset_id": asset_id, "source_table": str(local_file), "gcs_uri": gcs_uri, "manifest": str(manifest_path), "dry_run": True}

    ee_module, credentials, key = initialize_ee(service_account_json)
    ensure_gee_folder_path(ee_module, asset_id.rsplit("/", 1)[0])
    if gee_asset_exists(ee_module, asset_id):
        if not args.replace_existing:
            raise RuntimeError(f"GEE asset already exists: {asset_id}. Pass --replace-existing to overwrite.")
        log.info("Deleting existing GEE asset: %s", asset_id)
        ee_module.data.deleteAsset(asset_id)
        time.sleep(1)
    uploaded_gcs_uri = upload_file_to_gcs(
        local_file,
        credentials,
        key,
        bucket_name,
        blob_name,
        int(upload_cfg.get("chunk_size_mb", 64)),
    )
    manifest["sources"][0]["uris"] = [uploaded_gcs_uri]
    write_json(manifest_path, manifest)
    try:
        ingestion = start_table_ingestion(ee_module, manifest)
    except Exception:
        if args.cleanup_gcs:
            delete_gcs_blob(credentials, key, bucket_name, blob_name)
        raise
    result = {
        "asset_id": asset_id,
        "source_table": str(local_file),
        "gcs_uri": uploaded_gcs_uri,
        "manifest": str(manifest_path),
        **ingestion,
    }
    if args.wait or args.make_public or args.cleanup_gcs:
        status = wait_for_task(ee_module, ingestion.get("task_id"), ingestion.get("operation_name"), args.poll_interval, args.timeout)
        result["final_status"] = status
        if status.get("state") not in SUCCESS_TASK_STATES:
            raise RuntimeError(f"GEE ingestion failed for {asset_id}: {status}")
    if args.make_public:
        result["made_public"] = make_asset_public(ee_module, asset_id)
    if args.cleanup_gcs:
        result["cleaned_gcs"] = delete_gcs_blob(credentials, key, bucket_name, blob_name)
    return result


def upload_assets(args: argparse.Namespace) -> dict[str, Any]:
    config = load_config(args)
    service_account_json = resolve_service_account(config, args)
    assets = config.selected(args.asset)
    summary = {
        "uploaded_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "config": str(config.config_path),
        "assets": {},
    }
    jobs = max(1, int(args.jobs or 1))
    if jobs == 1 or len(assets) <= 1:
        for name, asset_cfg in assets:
            summary["assets"][name] = upload_one_asset(config, name, asset_cfg, service_account_json, args)
    else:
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            futures = {
                executor.submit(upload_one_asset, config, name, asset_cfg, service_account_json, args): name
                for name, asset_cfg in assets
            }
            for future in as_completed(futures):
                summary["assets"][futures[future]] = future.result()
    summary_path = config.summary_path(
        "upload_summary_yaml",
        "data/pan_india_gee_assets/gee/pan_india_gee_upload_summary.yaml",
        suffix=args.output_suffix,
    )
    write_yaml(summary_path, summary)
    log.info("Wrote upload summary: %s", summary_path)
    return summary


def task_status(args: argparse.Namespace) -> dict[str, Any]:
    config = load_config(args)
    summary_path = repo_path(
        config.repo_root,
        args.summary
        or config.outputs_cfg.get("upload_summary_yaml", "data/pan_india_gee_assets/gee/pan_india_gee_upload_summary.yaml"),
    )
    service_account_json = resolve_service_account(config, args)
    ee_module, _, _ = initialize_ee(service_account_json)
    summary = read_yaml(summary_path)
    statuses = {"checked_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(), "assets": {}}
    for name, result in (summary.get("assets") or {}).items():
        statuses["assets"][name] = fetch_task_status(ee_module, result.get("task_id"), result.get("operation_name"))
    status_path = config.summary_path(
        "status_summary_yaml",
        "data/pan_india_gee_assets/gee/pan_india_gee_status.yaml",
        override=args.output,
    )
    write_yaml(status_path, statuses)
    log.info("Wrote task status summary: %s", status_path)
    print(yaml.safe_dump(statuses, sort_keys=False))
    return statuses


def publish_assets(args: argparse.Namespace) -> dict[str, Any]:
    config = load_config(args)
    service_account_json = resolve_service_account(config, args)
    ee_module, _, _ = initialize_ee(service_account_json)
    summary = {
        "published_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "config": str(config.config_path),
        "assets": {},
    }
    for name, asset_cfg in config.selected(args.asset):
        asset_id = asset_cfg["asset_id"]
        item = {"asset_id": asset_id, "exists": gee_asset_exists(ee_module, asset_id)}
        if item["exists"]:
            item["made_public"] = make_asset_public(ee_module, asset_id)
        else:
            item["made_public"] = False
            item["error"] = "Asset does not exist yet; wait for ingestion to succeed."
        summary["assets"][name] = item
    summary_path = config.summary_path(
        "public_summary_yaml",
        "data/pan_india_gee_assets/gee/pan_india_gee_public_summary.yaml",
        override=args.output,
    )
    write_yaml(summary_path, summary)
    log.info("Wrote public ACL summary: %s", summary_path)
    print(yaml.safe_dump(summary, sort_keys=False))
    return summary


def verify_assets(args: argparse.Namespace) -> dict[str, Any]:
    config = load_config(args)
    service_account_json = resolve_service_account(config, args)
    ee_module, _, _ = initialize_ee(service_account_json)
    summary = {
        "verified_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "config": str(config.config_path),
        "assets": {},
    }
    for name, asset_cfg in config.selected(args.asset):
        asset_id = asset_cfg["asset_id"]
        item: dict[str, Any] = {"asset_id": asset_id}
        source = Path(asset_cfg["source"])
        if source.exists():
            with sqlite3.connect(source) as con:
                item["local_rows"] = int(
                    con.execute(f"SELECT COUNT(*) FROM {sql_ident(asset_cfg['layer'])}").fetchone()[0]
                )
        try:
            asset = ee_module.data.getAsset(asset_id)
            item["exists"] = True
            item["asset_type"] = asset.get("type")
            item["name"] = asset.get("name")
            collection = ee_module.FeatureCollection(asset_id)
            if not args.skip_size:
                item["feature_count"] = collection.size().getInfo()
                if item.get("local_rows") is not None:
                    item["matches_local_rows"] = item["feature_count"] == item["local_rows"]
            item["property_names"] = collection.first().propertyNames().getInfo()
            item["property_count"] = len(item["property_names"])
        except Exception as exc:
            item["exists"] = False
            item["error"] = str(exc)
        summary["assets"][name] = item
    summary_path = config.summary_path(
        "verify_summary_yaml",
        "data/pan_india_gee_assets/gee/pan_india_gee_verify_summary.yaml",
        override=args.output,
    )
    write_yaml(summary_path, summary)
    log.info("Wrote GEE verify summary: %s", summary_path)
    print(yaml.safe_dump(summary, sort_keys=False))
    return summary


def inspect_assets(args: argparse.Namespace) -> dict[str, Any]:
    config = load_config(args)
    out = {"assets": {}}
    for name, asset_cfg in config.selected(args.asset):
        source = config.source_path(asset_cfg, args.output_suffix)
        output = config.table_path(name, args.output_suffix)
        item = {
            "asset_id": asset_cfg["asset_id"],
            "source": str(source),
            "layer": asset_cfg["layer"],
            "source_exists": source.exists(),
            "source_size_bytes": source.stat().st_size if source.exists() else None,
            "output": str(output),
            "output_exists": output.exists(),
            "output_size_bytes": output.stat().st_size if output.exists() else None,
        }
        if source.exists():
            with sqlite3.connect(source) as con:
                item["layer_rows"] = con.execute(f"SELECT COUNT(*) FROM {sql_ident(asset_cfg['layer'])}").fetchone()[0]
                item["geometry_columns"] = [
                    list(row)
                    for row in con.execute("SELECT table_name, column_name, geometry_type_name, srs_id FROM gpkg_geometry_columns")
                ]
        out["assets"][name] = item
    print(yaml.safe_dump(out, sort_keys=False))
    return out


def add_common_build_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--asset", default="all")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--chunk-size", type=int, default=25_000)
    parser.add_argument("--limit", type=int, default=None, help="Smoke-test limit per asset.")
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--output-suffix", default=None)
    parser.add_argument("--max-rss-mb", type=int, default=DEFAULT_MAX_RSS_MB)


def add_common_upload_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--asset", default="all")
    parser.add_argument("--service-account-json", default=None)
    parser.add_argument("--gcs-bucket", default=None)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--output-suffix", default=None)
    parser.add_argument("--replace-existing", action="store_true")
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--make-public", action="store_true")
    parser.add_argument("--cleanup-gcs", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--poll-interval", type=int, default=60)
    parser.add_argument("--timeout", type=int, default=None)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--debug", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    inspect_cmd = sub.add_parser("inspect", help="Inspect merged sources and local build outputs.")
    inspect_cmd.add_argument("--asset", default="all")
    inspect_cmd.add_argument("--output-suffix", default=None)
    inspect_cmd.set_defaults(func=inspect_assets)

    build_cmd = sub.add_parser("build", help="Build local GEE ingestion tables from merged GeoPackages.")
    add_common_build_args(build_cmd)
    build_cmd.set_defaults(func=build_assets)

    upload_cmd = sub.add_parser("upload", help="Stage local tables to GCS and start GEE table ingestion.")
    add_common_upload_args(upload_cmd)
    upload_cmd.set_defaults(func=upload_assets)

    status_cmd = sub.add_parser("status", help="Check task statuses from an upload summary YAML.")
    status_cmd.add_argument("--summary", default=None)
    status_cmd.add_argument("--service-account-json", default=None)
    status_cmd.add_argument("--output", default=None)
    status_cmd.set_defaults(func=task_status)

    public_cmd = sub.add_parser("make-public", help="Make existing configured GEE assets public.")
    public_cmd.add_argument("--asset", default="all")
    public_cmd.add_argument("--service-account-json", default=None)
    public_cmd.add_argument("--output", default=None)
    public_cmd.set_defaults(func=publish_assets)

    verify_cmd = sub.add_parser("verify", help="Verify existing GEE assets against merged local rows.")
    verify_cmd.add_argument("--asset", default="all")
    verify_cmd.add_argument("--service-account-json", default=None)
    verify_cmd.add_argument("--output", default=None)
    verify_cmd.add_argument("--skip-size", action="store_true")
    verify_cmd.set_defaults(func=verify_assets)

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    config_path = args.config.resolve()
    repo_root = find_repo_root(config_path)
    config = read_yaml(config_path) if config_path.exists() else {}
    log_file = repo_path(repo_root, ((config.get("gee") or {}).get("outputs") or {}).get("log_file", "logs/pan_india_gee_ingest.log"))
    setup_logging(args.debug, log_file=log_file)
    args.func(args)


if __name__ == "__main__":
    main()
