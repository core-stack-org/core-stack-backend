#!/usr/bin/env python3
"""Build and upload GEE-ready facilities v2 table assets.

The local facilities GeoPackages are relational and optimized for CoreStack.
Earth Engine is better served by two flat assets:

- ``pan_india_facilities_v2``: one point per facility-class membership.
- ``village_facility_proximity_v2``: one polygon per village with wide L3/L2
  distance and nearest-facility UID columns.

The builder writes Earth Engine table-ingestion TSV files directly. Each row has
plain properties plus a ``geometry`` column containing GeoJSON geometry.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Sequence

import yaml


log = logging.getLogger("facilities_gee_assets")

DEFAULT_CONFIG = Path(__file__).with_name("facilities_gee_assets.yaml")
TERMINAL_TASK_STATES = {"SUCCEEDED", "COMPLETED", "FAILED", "CANCELLED"}
SUCCESS_TASK_STATES = {"SUCCEEDED", "COMPLETED"}
GEE_OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/earthengine",
    "https://www.googleapis.com/auth/devstorage.full_control",
]


def setup_logging(debug: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s  [%(levelname)-7s]  %(message)s",
        datefmt="%H:%M:%S",
    )


def find_repo_root(start: Path) -> Path:
    for candidate in [start, *start.parents]:
        if (candidate / ".git").exists() or (candidate / "manage.py").exists():
            return candidate
    return start


def resolve_path(repo_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else repo_root / path


def read_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def write_yaml(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False, allow_unicode=True)


def with_output_suffix(path: Path, suffix: Optional[str]) -> Path:
    if not suffix:
        return path
    clean_suffix = suffix if suffix.startswith(".") else f".{suffix}"
    return path.with_name(f"{path.stem}{clean_suffix}{path.suffix}")


def sql_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    out = []
    previous_underscore = False
    for char in text:
        if char.isalnum():
            out.append(char)
            previous_underscore = False
        else:
            if not previous_underscore:
                out.append("_")
            previous_underscore = True
    return "".join(out).strip("_")


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


def gpkg_geometry_to_geojson(blob: bytes | memoryview | None) -> Optional[Dict[str, Any]]:
    if blob is None:
        return None
    from shapely import wkb
    from shapely.geometry import mapping

    data = bytes(blob)
    if data[:2] != b"GP":
        shape = wkb.loads(data)
    else:
        flags = data[3]
        if flags & 0b00010000:
            return None
        envelope_code = (flags >> 1) & 0b00000111
        envelope_bytes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get(envelope_code)
        if envelope_bytes is None:
            raise ValueError(f"Unsupported GeoPackage geometry envelope code: {envelope_code}")
        shape = wkb.loads(data[8 + envelope_bytes :])
    return mapping(shape)


def point_geometry(lon: Any, lat: Any) -> Optional[Dict[str, Any]]:
    if lon is None or lat is None:
        return None
    lon = float(lon)
    lat = float(lat)
    if math.isnan(lon) or math.isnan(lat):
        return None
    return {"type": "Point", "coordinates": [lon, lat]}


class EETsvWriter:
    def __init__(self, path: Path, fieldnames: Sequence[str], delimiter: str = "\t", quotechar: str = '"'):
        self.path = path
        self.fieldnames = list(fieldnames)
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.handle = None
        self.writer = None
        self.rows_written = 0

    def __enter__(self) -> "EETsvWriter":
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

    def write(self, row: Dict[str, Any]) -> None:
        assert self.writer is not None
        self.writer.writerow({field: csv_safe(row.get(field)) for field in self.fieldnames})
        self.rows_written += 1

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.handle is not None:
            self.handle.close()


def maybe_log_progress(label: str, count: int, started_at: float, last_logged_at: float) -> float:
    now = time.monotonic()
    if count == 1 or count % 100_000 == 0 or (now - last_logged_at) > 30:
        elapsed = max(now - started_at, 0.001)
        log.info("%s: %s rows in %.1fs (%.0f rows/s)", label, f"{count:,}", elapsed, count / elapsed)
        return now
    return last_logged_at


def sqlite_rows(con: sqlite3.Connection, sql: str, params: Sequence[Any] = (), arraysize: int = 50_000) -> Iterator[sqlite3.Row]:
    cur = con.execute(sql, params)
    while True:
        rows = cur.fetchmany(arraysize)
        if not rows:
            break
        yield from rows


def export_facility_membership_points(
    repo_root: Path,
    asset_cfg: Dict[str, Any],
    upload_cfg: Dict[str, Any],
    limit: Optional[int] = None,
    overwrite: bool = False,
    output_suffix: Optional[str] = None,
) -> Dict[str, Any]:
    gpkg = resolve_path(repo_root, asset_cfg["source_gpkg"])
    output = with_output_suffix(resolve_path(repo_root, asset_cfg["output_tsv"]), output_suffix)
    if output.exists() and not overwrite:
        raise FileExistsError(f"Output exists; pass --overwrite to replace: {output}")

    facility_cols = asset_cfg["columns"]["facility"]
    membership_cols = asset_cfg["columns"]["membership"]
    generated_cols = asset_cfg["columns"].get("generated", [])
    fieldnames = [*generated_cols, *facility_cols, *membership_cols, asset_cfg.get("geometry_column", "geometry")]
    fieldnames = list(dict.fromkeys(fieldnames))

    facility_select = ", ".join(f"f.{sql_ident(col)} as {sql_ident(col)}" for col in facility_cols)
    membership_select = ", ".join(f"m.{sql_ident(col)} as {sql_ident(col)}" for col in membership_cols)
    sql = f"""
        select {facility_select}, {membership_select}
        from facility_memberships m
        join facilities f on f.facility_uid = m.facility_uid
        where f.latitude is not null
          and f.longitude is not null
    """
    params: list[Any] = []
    if limit:
        sql += " limit ?"
        params.append(int(limit))

    started = time.monotonic()
    last_logged = started
    with sqlite3.connect(gpkg) as con:
        con.row_factory = sqlite3.Row
        with EETsvWriter(
            output,
            fieldnames,
            delimiter=upload_cfg.get("csv_delimiter", "\t"),
            quotechar=upload_cfg.get("csv_qualifier", '"'),
        ) as writer:
            for row in sqlite_rows(con, sql, params):
                props = dict(row)
                geometry = point_geometry(props.get("longitude"), props.get("latitude"))
                if geometry is None:
                    continue
                props["facility_membership_uid"] = "|".join(
                    str(props.get(col) or "")
                    for col in ("facility_uid", "class_l3_facility_class", "class_l4_facility_subtype", "class_k1")
                )
                props[asset_cfg.get("geometry_column", "geometry")] = compact_json(geometry)
                writer.write(props)
                last_logged = maybe_log_progress(asset_cfg["kind"], writer.rows_written, started, last_logged)
            rows = writer.rows_written

    return {
        "asset": "pan_india_facilities_v2",
        "output_tsv": str(output),
        "rows": rows,
        "columns": fieldnames,
        "size_bytes": output.stat().st_size,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def proximity_schema(con: sqlite3.Connection, asset_cfg: Dict[str, Any]) -> Dict[str, Any]:
    class_rows = con.execute(
        """
        select distinct class_l1_domain, class_l2_filter_group, class_l3_facility_class
        from proximity_class_map
        order by class_l1_domain, class_l2_filter_group, class_l3_facility_class
        """
    ).fetchall()
    l3_classes = [row[2] for row in class_rows]
    l2_groups = sorted({(row[0], row[1]) for row in class_rows})
    l1_domains = sorted({row[0] for row in class_rows})

    l3_distance_cols = {
        cls: asset_cfg["include_l3"]["distance_template"].format(class_name=slug(cls))
        for cls in l3_classes
    }
    l3_uid_cols = {
        cls: asset_cfg["include_l3"]["facility_uid_template"].format(class_name=slug(cls))
        for cls in l3_classes
    }
    l2_distance_cols = {
        group: asset_cfg["include_l2"]["distance_template"].format(group_name=slug(group[1]))
        for group in l2_groups
    }
    l2_uid_cols = {
        group: asset_cfg["include_l2"]["facility_uid_template"].format(group_name=slug(group[1]))
        for group in l2_groups
    }
    l2_selected_cols = {
        group: asset_cfg["include_l2"]["selected_l3_template"].format(group_name=slug(group[1]))
        for group in l2_groups
    }
    l1_distance_cols = {}
    l1_group_cols = {}
    l1_cfg = asset_cfg.get("include_l1_summary") or {}
    if l1_cfg.get("enabled", False):
        l1_distance_cols = {
            domain: l1_cfg["nearest_distance_template"].format(domain_name=slug(domain))
            for domain in l1_domains
        }
        l1_group_cols = {
            domain: l1_cfg["nearest_l2_template"].format(domain_name=slug(domain))
            for domain in l1_domains
        }

    return {
        "class_rows": class_rows,
        "l3_classes": l3_classes,
        "l2_groups": l2_groups,
        "l1_domains": l1_domains,
        "l3_distance_cols": l3_distance_cols,
        "l3_uid_cols": l3_uid_cols,
        "l2_distance_cols": l2_distance_cols,
        "l2_uid_cols": l2_uid_cols,
        "l2_selected_cols": l2_selected_cols,
        "l1_distance_cols": l1_distance_cols,
        "l1_group_cols": l1_group_cols,
    }


def create_requested_villages(con: sqlite3.Connection, ids: Sequence[str]) -> None:
    con.execute("drop table if exists temp.requested_villages")
    con.execute("create temp table requested_villages (cs_feature_id text primary key)")
    con.executemany("insert or ignore into requested_villages values (?)", [(str(item),) for item in ids])


def fetch_l3_metrics(con: sqlite3.Connection, schema: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    sql = """
        select cs_feature_id, class_l3_facility_class, nearest_distance_km, nearest_facility_uid
        from proximity_l3
        where cs_feature_id in (select cs_feature_id from requested_villages)
    """
    for village_id, cls, distance, uid in con.execute(sql):
        row = out.setdefault(str(village_id), {})
        row[schema["l3_distance_cols"][cls]] = distance
        row[schema["l3_uid_cols"][cls]] = uid
    return out


def fetch_l2_metrics(con: sqlite3.Connection, schema: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    l1_best: Dict[str, Dict[str, tuple[float, str]]] = {}
    sql = """
        select cs_feature_id, class_l1_domain, class_l2_filter_group,
               logic_distance_km, selected_component_class, nearest_facility_uid
        from proximity_l2_materialized
        where cs_feature_id in (select cs_feature_id from requested_villages)
    """
    for village_id, l1, l2, distance, selected_l3, uid in con.execute(sql):
        village_id = str(village_id)
        group_key = (l1, l2)
        row = out.setdefault(village_id, {})
        row[schema["l2_distance_cols"][group_key]] = distance
        row[schema["l2_uid_cols"][group_key]] = uid
        row[schema["l2_selected_cols"][group_key]] = selected_l3
        if schema["l1_distance_cols"] and distance is not None:
            best_by_domain = l1_best.setdefault(village_id, {})
            current = best_by_domain.get(l1)
            if current is None or float(distance) < current[0]:
                best_by_domain[l1] = (float(distance), l2)

    for village_id, domains in l1_best.items():
        row = out.setdefault(village_id, {})
        for domain, (distance, l2_group) in domains.items():
            row[schema["l1_distance_cols"][domain]] = distance
            row[schema["l1_group_cols"][domain]] = l2_group
    return out


def export_village_proximity_wide(
    repo_root: Path,
    asset_cfg: Dict[str, Any],
    upload_cfg: Dict[str, Any],
    chunk_size: int = 25_000,
    limit: Optional[int] = None,
    overwrite: bool = False,
    output_suffix: Optional[str] = None,
) -> Dict[str, Any]:
    gpkg = resolve_path(repo_root, asset_cfg["source_gpkg"])
    output = with_output_suffix(resolve_path(repo_root, asset_cfg["output_tsv"]), output_suffix)
    if output.exists() and not overwrite:
        raise FileExistsError(f"Output exists; pass --overwrite to replace: {output}")

    started = time.monotonic()
    with sqlite3.connect(gpkg) as con:
        con.row_factory = sqlite3.Row
        con.execute("pragma temp_store = MEMORY")
        schema = proximity_schema(con, asset_cfg)
        admin_map = asset_cfg["admin_columns"]
        admin_fieldnames = list(admin_map.values())
        metric_fieldnames: list[str] = []
        for cls in schema["l3_classes"]:
            metric_fieldnames.extend([schema["l3_distance_cols"][cls], schema["l3_uid_cols"][cls]])
        for group in schema["l2_groups"]:
            metric_fieldnames.extend(
                [
                    schema["l2_distance_cols"][group],
                    schema["l2_uid_cols"][group],
                    schema["l2_selected_cols"][group],
                ]
            )
        for domain in schema["l1_domains"]:
            if domain in schema["l1_distance_cols"]:
                metric_fieldnames.extend([schema["l1_distance_cols"][domain], schema["l1_group_cols"][domain]])
        fieldnames = [*admin_fieldnames, *metric_fieldnames, asset_cfg.get("geometry_column", "geometry")]

        source_admin_cols = list(admin_map.keys())
        select_cols = ", ".join(["fid", *[sql_ident(col) for col in source_admin_cols], "geom"])
        last_fid = 0
        total = 0
        last_logged = started
        remaining = limit

        with EETsvWriter(
            output,
            fieldnames,
            delimiter=upload_cfg.get("csv_delimiter", "\t"),
            quotechar=upload_cfg.get("csv_qualifier", '"'),
        ) as writer:
            while True:
                current_chunk = chunk_size if remaining is None else min(chunk_size, remaining)
                if current_chunk <= 0:
                    break
                rows = con.execute(
                    f"""
                    select {select_cols}
                    from village_shapes
                    where fid > ?
                    order by fid
                    limit ?
                    """,
                    (last_fid, current_chunk),
                ).fetchall()
                if not rows:
                    break
                last_fid = int(rows[-1]["fid"])
                ids = [str(row["cs_feature_id"]) for row in rows]
                create_requested_villages(con, ids)
                l3_metrics = fetch_l3_metrics(con, schema)
                l2_metrics = fetch_l2_metrics(con, schema)

                for row in rows:
                    village_id = str(row["cs_feature_id"])
                    props = {target: json_safe(row[source]) for source, target in admin_map.items()}
                    props.update(l3_metrics.get(village_id, {}))
                    props.update(l2_metrics.get(village_id, {}))
                    geometry = gpkg_geometry_to_geojson(row["geom"])
                    if geometry is None:
                        continue
                    props[asset_cfg.get("geometry_column", "geometry")] = compact_json(geometry)
                    writer.write(props)
                    total += 1
                    last_logged = maybe_log_progress(asset_cfg["kind"], total, started, last_logged)
                if remaining is not None:
                    remaining -= len(rows)

    return {
        "asset": "village_facility_proximity_v2",
        "output_tsv": str(output),
        "rows": total,
        "columns": fieldnames,
        "size_bytes": output.stat().st_size,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def build_assets(args: argparse.Namespace) -> Dict[str, Any]:
    config_path = args.config.resolve()
    repo_root = find_repo_root(config_path)
    config = read_yaml(config_path)
    upload_cfg = config.get("upload") or {}
    assets = config["assets"]
    requested = args.asset
    summary: Dict[str, Any] = {
        "built_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "config": str(config_path),
        "assets": {},
    }

    if requested in {"all", "pan_india_facilities_v2"}:
        summary["assets"]["pan_india_facilities_v2"] = export_facility_membership_points(
            repo_root,
            assets["pan_india_facilities_v2"],
            upload_cfg,
            limit=args.limit_facilities,
            overwrite=args.overwrite,
            output_suffix=args.output_suffix,
        )
    if requested in {"all", "village_facility_proximity_v2"}:
        summary["assets"]["village_facility_proximity_v2"] = export_village_proximity_wide(
            repo_root,
            assets["village_facility_proximity_v2"],
            upload_cfg,
            chunk_size=args.chunk_size,
            limit=args.limit_villages,
            overwrite=args.overwrite,
            output_suffix=args.output_suffix,
        )

    summary_path = with_output_suffix(resolve_path(repo_root, config["outputs"]["summary_yaml"]), args.output_suffix)
    write_yaml(summary_path, summary)
    log.info("Wrote summary: %s", summary_path)
    return summary


def initialize_ee(service_account_json: Path):
    try:
        import geemap
        import ee

        ee.data.setUserAgent(f"geemap/{geemap.__version__} corestack-facilities-v2")
    except ImportError as exc:
        raise RuntimeError("Upload requires geemap/earthengine-api. Run with `uv run --with geemap --with google-cloud-storage`.") from exc

    from google.oauth2 import service_account

    with service_account_json.open("r", encoding="utf-8") as handle:
        key = json.load(handle)
    credentials = service_account.Credentials.from_service_account_file(
        service_account_json,
        scopes=GEE_OAUTH_SCOPES,
    )
    ee.Initialize(credentials=credentials, project=key.get("project_id"))
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
        ee_module.data.createAsset({"type": "FOLDER"}, current)
        time.sleep(1)


def upload_file_to_gcs(local_path: Path, credentials: Any, key: Dict[str, Any], bucket_name: str, blob_name: str, chunk_size_mb: int) -> str:
    from google.cloud import storage

    client = storage.Client(project=key.get("project_id"), credentials=credentials)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.chunk_size = max(256 * 1024, int(chunk_size_mb) * 1024 * 1024)
    log.info("Uploading %s to gs://%s/%s", local_path, bucket_name, blob_name)
    blob.upload_from_filename(str(local_path))
    blob.reload()
    return f"gs://{bucket_name}/{blob_name}"


def start_table_ingestion(
    ee_module: Any,
    asset_id: str,
    gcs_uri: str,
    geometry_column: str,
    upload_cfg: Dict[str, Any],
    properties: Dict[str, Any],
) -> Dict[str, Any]:
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
    manifest = {"name": asset_id, "sources": [source], "properties": properties}

    task_id = None
    if hasattr(ee_module.data, "newTaskId"):
        generated = ee_module.data.newTaskId()
        task_id = generated[0] if isinstance(generated, (list, tuple)) else generated
    response = ee_module.data.startTableIngestion(task_id, manifest)
    operation_name = response if isinstance(response, str) else response.get("name") or response.get("id")
    if not task_id and operation_name:
        task_id = operation_name.rsplit("/", 1)[-1]
    return {"task_id": task_id, "operation_name": operation_name, "raw_response": response}


def fetch_task_status(ee_module: Any, task_id: Optional[str], operation_name: Optional[str]) -> Dict[str, Any]:
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


def wait_for_task(ee_module: Any, task_id: Optional[str], operation_name: Optional[str], poll_interval: int, timeout: Optional[int]) -> Dict[str, Any]:
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
    acl = ee_module.data.getAssetAcl(asset_id)
    acl["all_users_can_read"] = True
    ee_module.data.setAssetAcl(asset_id, acl)
    return bool(ee_module.data.getAssetAcl(asset_id).get("all_users_can_read"))


def upload_assets(args: argparse.Namespace) -> Dict[str, Any]:
    config_path = args.config.resolve()
    repo_root = find_repo_root(config_path)
    config = read_yaml(config_path)
    upload_cfg = config.get("upload") or {}
    service_account_json = resolve_path(repo_root, args.service_account_json or config["service_account_json"])
    ee_module, credentials, key = initialize_ee(service_account_json)

    results: Dict[str, Any] = {
        "uploaded_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "service_account": key.get("client_email"),
        "assets": {},
    }
    for name, asset_cfg in config["assets"].items():
        if args.asset not in {"all", name}:
            continue
        local_file = with_output_suffix(
            resolve_path(repo_root, asset_cfg["output_tsv"]),
            getattr(args, "output_suffix", None),
        )
        if not local_file.exists():
            raise FileNotFoundError(f"Build output missing for {name}: {local_file}")
        asset_id = asset_cfg["asset_id"]
        ensure_gee_folder_path(ee_module, asset_id.rsplit("/", 1)[0])
        if gee_asset_exists(ee_module, asset_id):
            if not args.replace_existing:
                raise RuntimeError(f"GEE asset already exists: {asset_id}. Pass --replace-existing to overwrite.")
            log.info("Deleting existing GEE asset: %s", asset_id)
            ee_module.data.deleteAsset(asset_id)
            time.sleep(1)

        blob_name = f"{upload_cfg.get('gcs_prefix', 'gee/facilities_v2').strip('/')}/{slug(name)}_{uuid.uuid4().hex}.tsv"
        gcs_uri = upload_file_to_gcs(
            local_file,
            credentials,
            key,
            args.gcs_bucket or upload_cfg.get("gcs_bucket", "core_stack"),
            blob_name,
            int(upload_cfg.get("chunk_size_mb", 64)),
        )
        ingestion = start_table_ingestion(
            ee_module,
            asset_id,
            gcs_uri,
            asset_cfg.get("geometry_column", "geometry"),
            upload_cfg,
            {
                "corestack_asset": name,
                "source_file": local_file.name,
                "built_by": "utilities/scripts/facilities_utils/facilities_gee_assets.py",
            },
        )
        result = {
            "asset_id": asset_id,
            "source_tsv": str(local_file),
            "gcs_uri": gcs_uri,
            **ingestion,
        }
        if args.wait or args.make_public or args.cleanup_gcs:
            status = wait_for_task(
                ee_module,
                ingestion.get("task_id"),
                ingestion.get("operation_name"),
                args.poll_interval,
                args.timeout,
            )
            result["final_status"] = status
            if status.get("state") not in SUCCESS_TASK_STATES:
                raise RuntimeError(f"GEE ingestion failed for {asset_id}: {status}")
        if args.make_public:
            result["made_public"] = make_asset_public(ee_module, asset_id)
        if args.cleanup_gcs:
            from google.cloud import storage

            bucket = storage.Client(project=key.get("project_id"), credentials=credentials).bucket(
                args.gcs_bucket or upload_cfg.get("gcs_bucket", "core_stack")
            )
            bucket.blob(blob_name).delete()
            result["cleaned_gcs"] = True
        results["assets"][name] = result

    summary_path = resolve_path(repo_root, config["outputs"]["summary_yaml"]).with_name("facilities_gee_upload_summary.yaml")
    write_yaml(summary_path, results)
    log.info("Wrote upload summary: %s", summary_path)
    return results


def inspect_assets(args: argparse.Namespace) -> Dict[str, Any]:
    config_path = args.config.resolve()
    repo_root = find_repo_root(config_path)
    config = read_yaml(config_path)
    summary = {"assets": {}}
    for name, asset_cfg in config["assets"].items():
        if args.asset not in {"all", name}:
            continue
        path = with_output_suffix(resolve_path(repo_root, asset_cfg["output_tsv"]), args.output_suffix)
        summary["assets"][name] = {
            "asset_id": asset_cfg["asset_id"],
            "kind": asset_cfg["kind"],
            "output_tsv": str(path),
            "exists": path.exists(),
            "size_bytes": path.stat().st_size if path.exists() else None,
        }
    print(yaml.safe_dump(summary, sort_keys=False))
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build/upload facilities v2 Earth Engine table assets.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--debug", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_asset_arg(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--asset",
            choices=["all", "pan_india_facilities_v2", "village_facility_proximity_v2"],
            default="all",
        )

    build = sub.add_parser("build", help="Build local GEE ingestion TSV files.")
    add_asset_arg(build)
    build.add_argument("--overwrite", action="store_true")
    build.add_argument("--chunk-size", type=int, default=25_000, help="Village chunks for proximity export.")
    build.add_argument("--limit-facilities", type=int, default=None, help="Smoke-test limit for facility-membership points.")
    build.add_argument("--limit-villages", type=int, default=None, help="Smoke-test limit for village polygons.")
    build.add_argument("--output-suffix", default=None, help="Append suffix before .tsv/.yaml, useful for smoke outputs such as .sample.")
    build.set_defaults(func=build_assets)

    upload = sub.add_parser("upload", help="Upload built TSV files to GEE table assets.")
    add_asset_arg(upload)
    upload.add_argument("--service-account-json", default=None)
    upload.add_argument("--gcs-bucket", default=None)
    upload.add_argument("--replace-existing", action="store_true")
    upload.add_argument("--wait", action="store_true")
    upload.add_argument("--make-public", action="store_true")
    upload.add_argument("--cleanup-gcs", action="store_true")
    upload.add_argument("--poll-interval", type=int, default=60)
    upload.add_argument("--timeout", type=int, default=None)
    upload.set_defaults(func=upload_assets)

    all_cmd = sub.add_parser("all", help="Build then upload.")
    add_asset_arg(all_cmd)
    all_cmd.add_argument("--overwrite", action="store_true")
    all_cmd.add_argument("--chunk-size", type=int, default=25_000)
    all_cmd.add_argument("--limit-facilities", type=int, default=None)
    all_cmd.add_argument("--limit-villages", type=int, default=None)
    all_cmd.add_argument("--output-suffix", default=None)
    all_cmd.add_argument("--service-account-json", default=None)
    all_cmd.add_argument("--gcs-bucket", default=None)
    all_cmd.add_argument("--replace-existing", action="store_true")
    all_cmd.add_argument("--wait", action="store_true")
    all_cmd.add_argument("--make-public", action="store_true")
    all_cmd.add_argument("--cleanup-gcs", action="store_true")
    all_cmd.add_argument("--poll-interval", type=int, default=60)
    all_cmd.add_argument("--timeout", type=int, default=None)
    all_cmd.set_defaults(func=lambda args: (build_assets(args), upload_assets(args)))

    inspect_cmd = sub.add_parser("inspect", help="Inspect configured local outputs.")
    add_asset_arg(inspect_cmd)
    inspect_cmd.add_argument("--output-suffix", default=None)
    inspect_cmd.set_defaults(func=inspect_assets)

    return parser


def main() -> None:
    args = build_parser().parse_args()
    setup_logging(args.debug)
    args.func(args)


if __name__ == "__main__":
    main()
