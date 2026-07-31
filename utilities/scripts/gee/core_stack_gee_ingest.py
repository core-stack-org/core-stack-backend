#!/usr/bin/env python3
"""Build and upload config-driven Core Stack vector assets to Earth Engine.

The ingestion path deliberately mirrors the reliable facilities upload flow:

1. Export a bounded-memory tab-delimited table with one GeoJSON geometry column.
2. Stage the same bytes to GCS with a ``.csv`` object suffix.
3. Start an Earth Engine table ingestion task from a manifest.

The script is generic for common GeoPackage/GeoJSON feature layers, with
configurable special exporters for assets that are relational locally but should
be flat and native in Earth Engine.
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
from typing import Any, Iterable, Iterator, Sequence
import uuid

import yaml


log = logging.getLogger("core_stack_gee_ingest")

DEFAULT_CONFIG = Path(__file__).with_name("core_stack_gee_assets.yaml")
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


def point_geometry(lon: Any, lat: Any) -> dict[str, Any] | None:
    if lon is None or lat is None:
        return None
    lon = float(lon)
    lat = float(lat)
    if math.isnan(lon) or math.isnan(lat):
        return None
    return {"type": "Point", "coordinates": [lon, lat]}


def gpkg_geometry_column(con: sqlite3.Connection, layer: str) -> str:
    row = con.execute(
        "SELECT column_name FROM gpkg_geometry_columns WHERE table_name = ?",
        (layer,),
    ).fetchone()
    return str(row[0]) if row else "geom"


def gpkg_table_columns(con: sqlite3.Connection, layer: str) -> list[tuple[str, str]]:
    return [(str(row[1]), str(row[2] or "")) for row in con.execute(f"PRAGMA table_info({sql_ident(layer)})")]


def configured_columns(
    available_columns: Sequence[str],
    geometry_column: str,
    asset_cfg: dict[str, Any],
) -> tuple[list[str], dict[str, str]]:
    properties_cfg = asset_cfg.get("properties") or {}
    include = properties_cfg.get("include") or asset_cfg.get("columns") or ["*"]
    exclude = set(properties_cfg.get("exclude") or [])
    rename = dict(properties_cfg.get("rename") or {})
    skip = {"fid", geometry_column}
    if include == ["*"] or include == "*":
        selected = [column for column in available_columns if column not in skip and column not in exclude]
    else:
        selected = [str(column) for column in include if str(column) in available_columns and str(column) not in exclude]
    output_names = {column: str(rename.get(column, column)) for column in selected}
    return selected, output_names


def output_delimiter(upload_cfg: dict[str, Any]) -> str:
    return str(upload_cfg.get("csv_delimiter", "\t"))


def output_quotechar(upload_cfg: dict[str, Any]) -> str:
    return str(upload_cfg.get("csv_qualifier", '"'))


def column_specs(items: Sequence[Any]) -> list[tuple[str, str]]:
    specs: list[tuple[str, str]] = []
    for item in items:
        if isinstance(item, str):
            specs.append((item, item))
        elif isinstance(item, dict):
            if "source" in item:
                specs.append((str(item["source"]), str(item.get("target") or item["source"])))
            elif len(item) == 1:
                source, target = next(iter(item.items()))
                specs.append((str(source), str(target)))
            else:
                raise ValueError(f"Column mapping must have source/target or one source: target pair: {item}")
        else:
            raise TypeError(f"Unsupported column config item: {item!r}")
    return specs


def target_for_source(specs: Sequence[tuple[str, str]], source_name: str) -> str:
    for source, target in specs:
        if source == source_name:
            return target
    return source_name


def asset_output_path(repo_root: Path, asset_name: str, asset_cfg: dict[str, Any], outputs_cfg: dict[str, Any], suffix: str | None) -> Path:
    if asset_cfg.get("output_table"):
        path = repo_path(repo_root, asset_cfg["output_table"])
    else:
        directory = repo_path(repo_root, outputs_cfg.get("directory", "data/gee/core_stack"))
        extension = outputs_cfg.get("local_extension", ".tsv")
        path = directory / f"{asset_name}{extension}"
    return with_suffix_token(path, suffix)


def sqlite_fetchmany(cursor: sqlite3.Cursor, size: int) -> Iterator[sqlite3.Row]:
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        yield from rows


def export_gpkg_feature_layer(
    repo_root: Path,
    asset_name: str,
    asset_cfg: dict[str, Any],
    outputs_cfg: dict[str, Any],
    upload_cfg: dict[str, Any],
    *,
    limit: int | None,
    chunk_size: int,
    overwrite: bool,
    output_suffix: str | None,
    max_rss_mb_limit: int,
) -> dict[str, Any]:
    source_path = repo_path(repo_root, asset_cfg["source"])
    layer = asset_cfg["layer"]
    output = asset_output_path(repo_root, asset_name, asset_cfg, outputs_cfg, output_suffix)
    if output.exists() and not overwrite:
        raise FileExistsError(f"Output exists; pass --overwrite to replace: {output}")

    started = time.monotonic()
    with sqlite3.connect(source_path) as con:
        con.row_factory = sqlite3.Row
        geom_col = gpkg_geometry_column(con, layer)
        all_columns = [column for column, _ in gpkg_table_columns(con, layer)]
        selected_columns, output_names = configured_columns(all_columns, geom_col, asset_cfg)
        property_fields = [output_names[column] for column in selected_columns]
        geometry_field = asset_cfg.get("geometry_column", "geometry")
        fieldnames = [*property_fields, geometry_field]
        select_cols = [sql_ident(column) for column in selected_columns]
        select_sql = ", ".join(["fid", *select_cols, sql_ident(geom_col)])
        total_rows = int(con.execute(f"SELECT COUNT(*) FROM {sql_ident(layer)}").fetchone()[0])
        remaining = limit
        last_fid = 0
        rows_written = 0
        last_logged = started
        with DelimitedTableWriter(output, fieldnames, output_delimiter(upload_cfg), output_quotechar(upload_cfg)) as writer:
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
                    out = {output_names[column]: json_safe(row[column]) for column in selected_columns}
                    out[geometry_field] = compact_json(geometry)
                    writer.write(out)
                    rows_written += 1
                    last_logged = maybe_log_progress(asset_name, rows_written, started, last_logged)
                enforce_memory(max_rss_mb_limit, asset_name)
                if remaining is not None:
                    remaining -= len(rows)

    return {
        "asset": asset_name,
        "kind": "feature_layer",
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


def iter_geojson_features(path: Path, max_in_memory_mb: int) -> Iterator[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix in {".geojsonl", ".jsonl", ".ndjson"}:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)
        return
    try:
        import ijson
    except ImportError:
        if path.stat().st_size > max_in_memory_mb * 1024 * 1024:
            raise RuntimeError(
                f"{path} is too large for in-memory GeoJSON parsing without ijson. "
                "Run with `uv run --with ijson ...` or convert to GeoJSONL."
            )
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        for feature in payload.get("features", []):
            yield feature
        return
    with path.open("rb") as handle:
        yield from ijson.items(handle, "features.item")


def export_geojson_feature_layer(
    repo_root: Path,
    asset_name: str,
    asset_cfg: dict[str, Any],
    outputs_cfg: dict[str, Any],
    upload_cfg: dict[str, Any],
    *,
    limit: int | None,
    overwrite: bool,
    output_suffix: str | None,
    max_rss_mb_limit: int,
) -> dict[str, Any]:
    source_path = repo_path(repo_root, asset_cfg["source"])
    output = asset_output_path(repo_root, asset_name, asset_cfg, outputs_cfg, output_suffix)
    if output.exists() and not overwrite:
        raise FileExistsError(f"Output exists; pass --overwrite to replace: {output}")
    properties_cfg = asset_cfg.get("properties") or {}
    include = properties_cfg.get("include") or ["*"]
    exclude = set(properties_cfg.get("exclude") or [])
    rename = dict(properties_cfg.get("rename") or {})
    geometry_field = asset_cfg.get("geometry_column", "geometry")
    max_json_mb = int(asset_cfg.get("max_in_memory_geojson_mb", 512))

    started = time.monotonic()
    rows_written = 0
    fieldnames: list[str] | None = None
    last_logged = started
    writer_context = None
    writer = None
    try:
        for feature in iter_geojson_features(source_path, max_json_mb):
            props = feature.get("properties") or {}
            if fieldnames is None:
                if include == ["*"] or include == "*":
                    selected = [key for key in props.keys() if key not in exclude]
                else:
                    selected = [key for key in include if key in props and key not in exclude]
                fieldnames = [str(rename.get(column, column)) for column in selected]
                fieldnames.append(geometry_field)
                writer_context = DelimitedTableWriter(output, fieldnames, output_delimiter(upload_cfg), output_quotechar(upload_cfg))
                writer = writer_context.__enter__()
            out = {
                str(rename.get(column, column)): json_safe(props.get(column))
                for column in props.keys()
                if (include == ["*"] or include == "*" or column in include) and column not in exclude
            }
            geometry = feature.get("geometry")
            if not geometry:
                continue
            out[geometry_field] = compact_json(geometry)
            writer.write(out)
            rows_written += 1
            last_logged = maybe_log_progress(asset_name, rows_written, started, last_logged)
            enforce_memory(max_rss_mb_limit, asset_name)
            if limit and rows_written >= limit:
                break
    finally:
        if writer_context is not None:
            writer_context.__exit__(None, None, None)
    if fieldnames is None:
        fieldnames = [geometry_field]
        with DelimitedTableWriter(output, fieldnames, output_delimiter(upload_cfg), output_quotechar(upload_cfg)):
            pass

    return {
        "asset": asset_name,
        "kind": "geojson_feature_layer",
        "source": str(source_path),
        "rows": rows_written,
        "columns": fieldnames,
        "output": str(output),
        "size_bytes": output.stat().st_size,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "max_rss_mb": round(max_rss_mb(), 1),
    }


def export_facility_membership_points(
    repo_root: Path,
    asset_name: str,
    asset_cfg: dict[str, Any],
    outputs_cfg: dict[str, Any],
    upload_cfg: dict[str, Any],
    *,
    limit: int | None,
    chunk_size: int,
    overwrite: bool,
    output_suffix: str | None,
    max_rss_mb_limit: int,
) -> dict[str, Any]:
    gpkg = repo_path(repo_root, asset_cfg["source"])
    output = asset_output_path(repo_root, asset_name, asset_cfg, outputs_cfg, output_suffix)
    if output.exists() and not overwrite:
        raise FileExistsError(f"Output exists; pass --overwrite to replace: {output}")
    columns_cfg = asset_cfg["columns"]
    facility_specs = column_specs(columns_cfg["facility"])
    membership_specs = column_specs(columns_cfg["membership"])
    generated_cols = columns_cfg.get("generated", [])
    geometry_field = asset_cfg.get("geometry_column", "geometry")
    facility_targets = [target for _, target in facility_specs]
    membership_targets = [target for _, target in membership_specs]
    lon_target = target_for_source(facility_specs, "longitude")
    lat_target = target_for_source(facility_specs, "latitude")
    fieldnames = list(dict.fromkeys([*generated_cols, *facility_targets, *membership_targets, geometry_field]))
    facility_select = ", ".join(f"f.{sql_ident(source)} AS {sql_ident(target)}" for source, target in facility_specs)
    membership_select = ", ".join(f"m.{sql_ident(source)} AS {sql_ident(target)}" for source, target in membership_specs)
    sql = f"""
        SELECT {facility_select}, {membership_select}
        FROM facility_memberships m
        JOIN facilities f ON f.facility_uid = m.facility_uid
        WHERE f.latitude IS NOT NULL
          AND f.longitude IS NOT NULL
    """
    if limit:
        sql += f" LIMIT {int(limit)}"

    started = time.monotonic()
    last_logged = started
    rows_written = 0
    with sqlite3.connect(gpkg) as con:
        con.row_factory = sqlite3.Row
        cursor = con.execute(sql)
        with DelimitedTableWriter(output, fieldnames, output_delimiter(upload_cfg), output_quotechar(upload_cfg)) as writer:
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    props = dict(row)
                    geometry = point_geometry(props.get(lon_target), props.get(lat_target))
                    if geometry is None:
                        continue
                    props["facility_membership_uid"] = "|".join(
                        str(props.get(col) or "")
                        for col in ("facility_uid", "class_l3_facility_class", "class_l4_facility_subtype", "class_k1")
                    )
                    props[geometry_field] = compact_json(geometry)
                    writer.write(props)
                    rows_written += 1
                    last_logged = maybe_log_progress(asset_name, rows_written, started, last_logged)
                enforce_memory(max_rss_mb_limit, asset_name)

    return {
        "asset": asset_name,
        "kind": "facility_membership_points",
        "source": str(gpkg),
        "rows": rows_written,
        "columns": fieldnames,
        "output": str(output),
        "size_bytes": output.stat().st_size,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "max_rss_mb": round(max_rss_mb(), 1),
    }


def proximity_schema(con: sqlite3.Connection, asset_cfg: dict[str, Any]) -> dict[str, Any]:
    class_rows = con.execute(
        """
        SELECT DISTINCT class_l1_domain, class_l2_filter_group, class_l3_facility_class
        FROM proximity_class_map
        ORDER BY class_l1_domain, class_l2_filter_group, class_l3_facility_class
        """
    ).fetchall()
    l3_classes = [row[2] for row in class_rows]
    l2_groups = sorted({(row[0], row[1]) for row in class_rows})
    l1_domains = sorted({row[0] for row in class_rows})
    include_l3 = asset_cfg["include_l3"]
    include_l2 = asset_cfg["include_l2"]
    l1_cfg = asset_cfg.get("include_l1_summary") or {}
    l3_distance_cols = {cls: include_l3["distance_template"].format(class_name=slug(cls)) for cls in l3_classes}
    l3_uid_cols = {cls: include_l3["facility_uid_template"].format(class_name=slug(cls)) for cls in l3_classes}
    l2_distance_cols = {group: include_l2["distance_template"].format(group_name=slug(group[1])) for group in l2_groups}
    l2_uid_cols = {group: include_l2["facility_uid_template"].format(group_name=slug(group[1])) for group in l2_groups}
    l2_selected_cols = {group: include_l2["selected_l3_template"].format(group_name=slug(group[1])) for group in l2_groups}
    l1_distance_cols = {}
    l1_group_cols = {}
    if l1_cfg.get("enabled", False):
        l1_distance_cols = {domain: l1_cfg["nearest_distance_template"].format(domain_name=slug(domain)) for domain in l1_domains}
        l1_group_cols = {domain: l1_cfg["nearest_l2_template"].format(domain_name=slug(domain)) for domain in l1_domains}
    return {
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
    con.execute("DROP TABLE IF EXISTS temp.requested_villages")
    con.execute("CREATE TEMP TABLE requested_villages (cs_feature_id TEXT PRIMARY KEY)")
    con.executemany("INSERT OR IGNORE INTO requested_villages VALUES (?)", [(str(item),) for item in ids])


def fetch_l3_metrics(con: sqlite3.Connection, schema: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for village_id, cls, distance, uid in con.execute(
        """
        SELECT cs_feature_id, class_l3_facility_class, nearest_distance_km, nearest_facility_uid
        FROM proximity_l3
        WHERE cs_feature_id IN (SELECT cs_feature_id FROM requested_villages)
        """
    ):
        row = out.setdefault(str(village_id), {})
        row[schema["l3_distance_cols"][cls]] = distance
        row[schema["l3_uid_cols"][cls]] = uid
    return out


def fetch_l2_metrics(con: sqlite3.Connection, schema: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    l1_best: dict[str, dict[str, tuple[float, str]]] = {}
    for village_id, l1, l2, distance, selected_l3, uid in con.execute(
        """
        SELECT cs_feature_id, class_l1_domain, class_l2_filter_group,
               logic_distance_km, selected_component_class, nearest_facility_uid
        FROM proximity_l2_materialized
        WHERE cs_feature_id IN (SELECT cs_feature_id FROM requested_villages)
        """
    ):
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
    asset_name: str,
    asset_cfg: dict[str, Any],
    outputs_cfg: dict[str, Any],
    upload_cfg: dict[str, Any],
    *,
    limit: int | None,
    chunk_size: int,
    overwrite: bool,
    output_suffix: str | None,
    max_rss_mb_limit: int,
) -> dict[str, Any]:
    gpkg = repo_path(repo_root, asset_cfg["source"])
    output = asset_output_path(repo_root, asset_name, asset_cfg, outputs_cfg, output_suffix)
    if output.exists() and not overwrite:
        raise FileExistsError(f"Output exists; pass --overwrite to replace: {output}")
    started = time.monotonic()
    rows_written = 0
    with sqlite3.connect(gpkg) as con:
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA temp_store = MEMORY")
        schema = proximity_schema(con, asset_cfg)
        admin_map = asset_cfg["admin_columns"]
        metric_fields: list[str] = []
        for cls in schema["l3_classes"]:
            metric_fields.extend([schema["l3_distance_cols"][cls], schema["l3_uid_cols"][cls]])
        for group in schema["l2_groups"]:
            metric_fields.extend([schema["l2_distance_cols"][group], schema["l2_uid_cols"][group], schema["l2_selected_cols"][group]])
        for domain in schema["l1_domains"]:
            if domain in schema["l1_distance_cols"]:
                metric_fields.extend([schema["l1_distance_cols"][domain], schema["l1_group_cols"][domain]])
        geometry_field = asset_cfg.get("geometry_column", "geometry")
        fieldnames = [*admin_map.values(), *metric_fields, geometry_field]
        source_cols = list(admin_map.keys())
        select_cols = ", ".join(["fid", *[sql_ident(col) for col in source_cols], "geom"])
        last_fid = 0
        remaining = limit
        last_logged = started
        with DelimitedTableWriter(output, fieldnames, output_delimiter(upload_cfg), output_quotechar(upload_cfg)) as writer:
            while True:
                current_chunk = chunk_size if remaining is None else min(chunk_size, remaining)
                if current_chunk <= 0:
                    break
                rows = con.execute(
                    f"""
                    SELECT {select_cols}
                    FROM village_shapes
                    WHERE fid > ?
                    ORDER BY fid
                    LIMIT ?
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
                    props[geometry_field] = compact_json(geometry)
                    writer.write(props)
                    rows_written += 1
                    last_logged = maybe_log_progress(asset_name, rows_written, started, last_logged)
                enforce_memory(max_rss_mb_limit, asset_name)
                if remaining is not None:
                    remaining -= len(rows)
    return {
        "asset": asset_name,
        "kind": "village_proximity_wide",
        "source": str(gpkg),
        "rows": rows_written,
        "columns": fieldnames,
        "output": str(output),
        "size_bytes": output.stat().st_size,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "max_rss_mb": round(max_rss_mb(), 1),
    }


def export_asset(
    repo_root: Path,
    asset_name: str,
    asset_cfg: dict[str, Any],
    outputs_cfg: dict[str, Any],
    upload_cfg: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    kind = asset_cfg.get("kind", "feature_layer")
    common = {
        "repo_root": repo_root,
        "asset_name": asset_name,
        "asset_cfg": asset_cfg,
        "outputs_cfg": outputs_cfg,
        "upload_cfg": upload_cfg,
        "limit": args.limit,
        "chunk_size": args.chunk_size,
        "overwrite": args.overwrite,
        "output_suffix": args.output_suffix,
        "max_rss_mb_limit": args.max_rss_mb,
    }
    if kind == "feature_layer":
        source_format = str(asset_cfg.get("format", "gpkg")).lower()
        if source_format == "gpkg":
            return export_gpkg_feature_layer(**common)
        if source_format in {"geojson", "geojsonl", "jsonl"}:
            return export_geojson_feature_layer(
                repo_root,
                asset_name,
                asset_cfg,
                outputs_cfg,
                upload_cfg,
                limit=args.limit,
                overwrite=args.overwrite,
                output_suffix=args.output_suffix,
                max_rss_mb_limit=args.max_rss_mb,
            )
    if kind == "facility_membership_points":
        return export_facility_membership_points(**common)
    if kind == "village_proximity_wide":
        return export_village_proximity_wide(**common)
    raise ValueError(f"Unsupported asset kind for {asset_name}: {kind}")


def selected_assets(config: dict[str, Any], requested: str) -> list[tuple[str, dict[str, Any]]]:
    assets = config.get("assets") or {}
    if requested == "all":
        return list(assets.items())
    if requested not in assets:
        raise KeyError(f"Unknown asset {requested!r}; choose one of: all, {', '.join(assets)}")
    return [(requested, assets[requested])]


def load_config(args: argparse.Namespace) -> tuple[Path, Path, dict[str, Any]]:
    config_path = args.config.resolve()
    repo_root = find_repo_root(config_path)
    return config_path, repo_root, read_yaml(config_path)


def build_assets(args: argparse.Namespace) -> dict[str, Any]:
    config_path, repo_root, config = load_config(args)
    outputs_cfg = config.get("outputs") or {}
    upload_cfg = config.get("upload") or {}
    assets = selected_assets(config, args.asset)
    summary = {
        "built_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "config": str(config_path),
        "max_rss_mb_limit": args.max_rss_mb,
        "assets": {},
    }
    jobs = max(1, int(args.jobs or 1))
    if jobs == 1 or len(assets) <= 1:
        for name, asset_cfg in assets:
            summary["assets"][name] = export_asset(repo_root, name, asset_cfg, outputs_cfg, upload_cfg, args)
    else:
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            futures = {
                executor.submit(export_asset, repo_root, name, asset_cfg, outputs_cfg, upload_cfg, args): name
                for name, asset_cfg in assets
            }
            for future in as_completed(futures):
                name = futures[future]
                summary["assets"][name] = future.result()
    summary_path = with_suffix_token(repo_path(repo_root, outputs_cfg.get("build_summary_yaml", "data/gee/core_stack/core_stack_gee_build_summary.yaml")), args.output_suffix)
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
        ee.data.setUserAgent("core-stack-gee-ingest")
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


def local_asset_file(repo_root: Path, asset_name: str, asset_cfg: dict[str, Any], outputs_cfg: dict[str, Any], output_suffix: str | None) -> Path:
    return asset_output_path(repo_root, asset_name, asset_cfg, outputs_cfg, output_suffix)


def upload_one_asset(
    repo_root: Path,
    name: str,
    asset_cfg: dict[str, Any],
    outputs_cfg: dict[str, Any],
    upload_cfg: dict[str, Any],
    service_account_json: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    local_file = local_asset_file(repo_root, name, asset_cfg, outputs_cfg, args.output_suffix)
    if not local_file.exists():
        raise FileNotFoundError(f"Build output missing for {name}: {local_file}")
    asset_id = asset_cfg["asset_id"]
    bucket_name = args.gcs_bucket or upload_cfg.get("gcs_bucket", "core_stack")
    gcs_extension = str(upload_cfg.get("gcs_object_extension") or ".csv")
    if not gcs_extension.startswith("."):
        gcs_extension = f".{gcs_extension}"
    blob_name = (
        f"{str(upload_cfg.get('gcs_prefix', 'gee/core_stack')).strip('/')}/"
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
            "built_by": "utilities/scripts/gee/core_stack_gee_ingest.py",
        },
    )
    manifest_path = repo_path(repo_root, outputs_cfg.get("manifest_dir", "data/gee/core_stack/manifests")) / f"{name}.manifest.json"
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
    config_path, repo_root, config = load_config(args)
    outputs_cfg = config.get("outputs") or {}
    upload_cfg = config.get("upload") or {}
    service_account_json = repo_path(repo_root, args.service_account_json or config["service_account_json"])
    assets = selected_assets(config, args.asset)
    summary = {
        "uploaded_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "config": str(config_path),
        "assets": {},
    }
    jobs = max(1, int(args.jobs or 1))
    if jobs == 1 or len(assets) <= 1:
        for name, asset_cfg in assets:
            summary["assets"][name] = upload_one_asset(repo_root, name, asset_cfg, outputs_cfg, upload_cfg, service_account_json, args)
    else:
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            futures = {
                executor.submit(upload_one_asset, repo_root, name, asset_cfg, outputs_cfg, upload_cfg, service_account_json, args): name
                for name, asset_cfg in assets
            }
            for future in as_completed(futures):
                summary["assets"][futures[future]] = future.result()
    summary_path = with_suffix_token(repo_path(repo_root, outputs_cfg.get("upload_summary_yaml", "data/gee/core_stack/core_stack_gee_upload_summary.yaml")), args.output_suffix)
    write_yaml(summary_path, summary)
    log.info("Wrote upload summary: %s", summary_path)
    return summary


def task_status(args: argparse.Namespace) -> dict[str, Any]:
    config_path, repo_root, config = load_config(args)
    outputs_cfg = config.get("outputs") or {}
    summary_path = repo_path(repo_root, args.summary or outputs_cfg.get("upload_summary_yaml", "data/gee/core_stack/core_stack_gee_upload_summary.yaml"))
    service_account_json = repo_path(repo_root, args.service_account_json or config["service_account_json"])
    ee_module, _, _ = initialize_ee(service_account_json)
    summary = read_yaml(summary_path)
    statuses = {"checked_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(), "assets": {}}
    for name, result in (summary.get("assets") or {}).items():
        statuses["assets"][name] = fetch_task_status(ee_module, result.get("task_id"), result.get("operation_name"))
    status_path = repo_path(
        repo_root,
        args.output or outputs_cfg.get("status_summary_yaml", "data/gee/core_stack/core_stack_gee_status.yaml"),
    )
    write_yaml(status_path, statuses)
    log.info("Wrote task status summary: %s", status_path)
    print(yaml.safe_dump(statuses, sort_keys=False))
    return statuses


def publish_assets(args: argparse.Namespace) -> dict[str, Any]:
    config_path, repo_root, config = load_config(args)
    outputs_cfg = config.get("outputs") or {}
    service_account_json = repo_path(repo_root, args.service_account_json or config["service_account_json"])
    ee_module, _, _ = initialize_ee(service_account_json)
    summary = {
        "published_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "config": str(config_path),
        "assets": {},
    }
    for name, asset_cfg in selected_assets(config, args.asset):
        asset_id = asset_cfg["asset_id"]
        item = {"asset_id": asset_id, "exists": gee_asset_exists(ee_module, asset_id)}
        if item["exists"]:
            item["made_public"] = make_asset_public(ee_module, asset_id)
        else:
            item["made_public"] = False
            item["error"] = "Asset does not exist yet; wait for ingestion to succeed."
        summary["assets"][name] = item
    summary_path = repo_path(
        repo_root,
        args.output or outputs_cfg.get("public_summary_yaml", "data/gee/core_stack/core_stack_gee_public_summary.yaml"),
    )
    write_yaml(summary_path, summary)
    log.info("Wrote public ACL summary: %s", summary_path)
    print(yaml.safe_dump(summary, sort_keys=False))
    return summary


def verify_assets(args: argparse.Namespace) -> dict[str, Any]:
    config_path, repo_root, config = load_config(args)
    outputs_cfg = config.get("outputs") or {}
    service_account_json = repo_path(repo_root, args.service_account_json or config["service_account_json"])
    ee_module, _, _ = initialize_ee(service_account_json)
    summary = {
        "verified_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "config": str(config_path),
        "assets": {},
    }
    for name, asset_cfg in selected_assets(config, args.asset):
        asset_id = asset_cfg["asset_id"]
        item: dict[str, Any] = {"asset_id": asset_id}
        try:
            asset = ee_module.data.getAsset(asset_id)
            item["exists"] = True
            item["asset_type"] = asset.get("type")
            item["name"] = asset.get("name")
            collection = ee_module.FeatureCollection(asset_id)
            if not args.skip_size:
                item["feature_count"] = collection.size().getInfo()
            item["property_names"] = collection.first().propertyNames().getInfo()
            item["property_count"] = len(item["property_names"])
        except Exception as exc:
            item["exists"] = False
            item["error"] = str(exc)
        summary["assets"][name] = item
    summary_path = repo_path(
        repo_root,
        args.output or outputs_cfg.get("verify_summary_yaml", "data/gee/core_stack/core_stack_gee_verify_summary.yaml"),
    )
    write_yaml(summary_path, summary)
    log.info("Wrote GEE verify summary: %s", summary_path)
    print(yaml.safe_dump(summary, sort_keys=False))
    return summary


def inspect_assets(args: argparse.Namespace) -> dict[str, Any]:
    _, repo_root, config = load_config(args)
    outputs_cfg = config.get("outputs") or {}
    out = {"assets": {}}
    for name, asset_cfg in selected_assets(config, args.asset):
        source = repo_path(repo_root, asset_cfg["source"])
        output = local_asset_file(repo_root, name, asset_cfg, outputs_cfg, args.output_suffix)
        item = {
            "asset_id": asset_cfg["asset_id"],
            "kind": asset_cfg.get("kind", "feature_layer"),
            "source": str(source),
            "source_exists": source.exists(),
            "source_size_bytes": source.stat().st_size if source.exists() else None,
            "output": str(output),
            "output_exists": output.exists(),
            "output_size_bytes": output.stat().st_size if output.exists() else None,
        }
        if source.exists() and asset_cfg.get("format", "gpkg") == "gpkg" and asset_cfg.get("layer"):
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--debug", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    inspect_cmd = sub.add_parser("inspect", help="Inspect configured sources and local build outputs.")
    inspect_cmd.add_argument("--asset", default="all")
    inspect_cmd.add_argument("--output-suffix", default=None)
    inspect_cmd.set_defaults(func=inspect_assets)

    build_cmd = sub.add_parser("build", help="Build local GEE ingestion tables.")
    add_common_build_args(build_cmd)
    build_cmd.set_defaults(func=build_assets)

    upload_cmd = sub.add_parser("upload", help="Stage local tables to GCS and start GEE table ingestion.")
    add_common_upload_args(upload_cmd)
    upload_cmd.set_defaults(func=upload_assets)

    all_cmd = sub.add_parser("all", help="Build local tables, then upload them.")
    all_cmd.add_argument("--asset", default="all")
    all_cmd.add_argument("--overwrite", action="store_true")
    all_cmd.add_argument("--chunk-size", type=int, default=25_000)
    all_cmd.add_argument("--limit", type=int, default=None, help="Smoke-test limit per asset.")
    all_cmd.add_argument("--jobs", type=int, default=1)
    all_cmd.add_argument("--output-suffix", default=None)
    all_cmd.add_argument("--max-rss-mb", type=int, default=DEFAULT_MAX_RSS_MB)
    all_cmd.add_argument("--service-account-json", default=None)
    all_cmd.add_argument("--gcs-bucket", default=None)
    all_cmd.add_argument("--replace-existing", action="store_true")
    all_cmd.add_argument("--wait", action="store_true")
    all_cmd.add_argument("--make-public", action="store_true")
    all_cmd.add_argument("--cleanup-gcs", action="store_true")
    all_cmd.add_argument("--dry-run", action="store_true")
    all_cmd.add_argument("--poll-interval", type=int, default=60)
    all_cmd.add_argument("--timeout", type=int, default=None)
    all_cmd.set_defaults(func=lambda args: (build_assets(args), upload_assets(args)))

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

    verify_cmd = sub.add_parser("verify", help="Verify existing GEE assets and sample schemas.")
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
    log_file = repo_path(repo_root, (config.get("outputs") or {}).get("log_file", "logs/core_stack_gee_ingest.log"))
    setup_logging(args.debug, log_file=log_file)
    args.func(args)


if __name__ == "__main__":
    main()
