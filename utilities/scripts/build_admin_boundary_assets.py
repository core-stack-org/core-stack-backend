#!/usr/bin/env python3
"""Build Core Stack village boundary GeoPackages and join village properties.

The script has two independent pieces:

1. Build a cached, sanitised all-India admin boundary GeoPackage from the
   district GeoJSON files under ``data/admin-boundary/input``.
2. Left-join CSV/JSON village properties onto that cached GeoPackage while
   retaining every row from the sanitised admin layer.

Default convenience run:

    python utilities/scripts/build_admin_boundary_assets.py --overwrite

This builds ``data/admin-boundary/cs_admin_sanitised.gpkg`` if needed, then
creates ``data/livestock/livestock.gpkg`` from
``data/livestock/livestock_pan_india.csv``.

Selective livestock output with only admin identity columns and livestock
counts:

    python utilities/scripts/build_admin_boundary_assets.py livestock \
      --overwrite \
      --no-match-status \
      --keep-output-columns state_name,district_name,TEHSIL,pc11_village_id,NAME,pc11_state_id,pc11_district_id,pc11_subdistrict_id,cattle_male,cattle_female,cattle_total,buffalo_male,buffalo_female,buffalo_total,sheep_male,sheep_female,sheep_total,goat_male,goat_female,goat_total,pig_male,pig_female,pig_total

python utilities/scripts/build_admin_boundary_assets.py livestock \
  --overwrite \
  --admin-gpkg data/admin-boundary/cs_admin_sanitised.gpkg \
  --output data/livestock/livestock.gpkg \
  --output-layer livestock \
  --no-match-status \
  --keep-output-columns state_name,district_name,TEHSIL,pc11_village_id,NAME,pc11_state_id,pc11_district_id,pc11_subdistrict_id,cattle_male,cattle_female,cattle_total,buffalo_male,buffalo_female,buffalo_total,sheep_male,sheep_female,sheep_total,goat_male,goat_female,goat_total,pig_male,pig_female,pig_total


The generic join command accepts the same ``--keep-output-columns`` and
``--rename-output-columns old=new,old2=new2`` options.

Export a GeoJSON sister file from an existing livestock GeoPackage:

    python utilities/scripts/build_admin_boundary_assets.py export-layer \
      --input-gpkg data/livestock/livestock_asset.gpkg \
      --input-layer livestock \
      --geojson-output data/livestock/livestock_asset.geojson \
      --overwrite \
      --chunk-size 25000 \
      --reports-dir data/livestock/livestock_geojson_reports \
      --keep-output-columns state_name,district_name,TEHSIL,pc11_village_id,NAME,pc11_state_id,pc11_district_id,pc11_subdistrict_id,cattle_male,cattle_female,cattle_total,buffalo_male,buffalo_female,buffalo_total,sheep_male,sheep_female,sheep_total,goat_male,goat_female,goat_total,pig_male,pig_female,pig_total

Create only GeoJSON directly from the livestock join:

    python utilities/scripts/build_admin_boundary_assets.py livestock \
      --overwrite \
      --admin-gpkg data/admin-boundary/cs_admin_sanitised.gpkg \
      --output data/livestock/livestock_asset.gpkg \
      --output-layer livestock \
      --output-formats geojson \
      --geojson-output data/livestock/livestock_asset.geojson \
      --no-match-status \
      --keep-output-columns state_name,district_name,TEHSIL,pc11_village_id,NAME,pc11_state_id,pc11_district_id,pc11_subdistrict_id,cattle_male,cattle_female,cattle_total,buffalo_male,buffalo_female,buffalo_total,sheep_male,sheep_female,sheep_total,goat_male,goat_female,goat_total,pig_male,pig_female,pig_total

Create both GPKG and GeoJSON from the livestock join:

    python utilities/scripts/build_admin_boundary_assets.py livestock \
      --overwrite \
      --admin-gpkg data/admin-boundary/cs_admin_sanitised.gpkg \
      --output data/livestock/livestock_asset.gpkg \
      --output-layer livestock \
      --output-formats gpkg,geojson \
      --geojson-output data/livestock/livestock_asset.geojson \
      --no-match-status \
      --keep-output-columns state_name,district_name,TEHSIL,pc11_village_id,NAME,pc11_state_id,pc11_district_id,pc11_subdistrict_id,cattle_male,cattle_female,cattle_total,buffalo_male,buffalo_female,buffalo_total,sheep_male,sheep_female,sheep_total,goat_male,goat_female,goat_total,pig_male,pig_female,pig_total
"""

from __future__ import annotations

import argparse
from contextlib import nullcontext
import csv
from collections import Counter
from dataclasses import asdict, dataclass
import fnmatch
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import time
from typing import Any, Iterable, Iterator, Sequence

try:
    import orjson
except Exception:  # pragma: no cover - optional speedup
    orjson = None

try:
    import ijson
except Exception:  # pragma: no cover - pyogrio fallback remains available
    ijson = None

try:
    import geopandas as gpd
    import pandas as pd
    import pyogrio
except Exception as exc:  # pragma: no cover - handled in ensure_dependencies
    gpd = None
    pd = None
    pyogrio = None
    PYOGRIO_STACK_IMPORT_ERROR = exc
else:
    PYOGRIO_STACK_IMPORT_ERROR = None

try:
    from shapely.geometry import MultiPolygon, shape as shapely_shape
    from shapely.ops import unary_union
    from shapely.wkb import loads as load_wkb
except Exception as exc:  # pragma: no cover - handled in ensure_dependencies
    MultiPolygon = None
    shapely_shape = None
    unary_union = None
    load_wkb = None
    SHAPELY_IMPORT_ERROR = exc
else:
    SHAPELY_IMPORT_ERROR = None

try:
    from shapely.validation import make_valid as shapely_make_valid
except Exception:  # pragma: no cover - compatibility fallback
    shapely_make_valid = None


ROOT_DIR = Path(__file__).resolve().parents[2]

DEFAULT_ADMIN_INPUT_DIR = Path("data/admin-boundary/input")
DEFAULT_ADMIN_GPKG = Path("data/admin-boundary/cs_admin_sanitised.gpkg")
DEFAULT_ADMIN_LAYER = "cs_admin_sanitised"
DEFAULT_ADMIN_REPORTS_DIR = Path("data/admin-boundary/cs_admin_sanitised_reports")

DEFAULT_LIVESTOCK_INPUT = Path("data/livestock/livestock_pan_india.csv")
DEFAULT_LIVESTOCK_OUTPUT = Path("data/livestock/livestock.gpkg")
DEFAULT_LIVESTOCK_LAYER = "livestock"
DEFAULT_OUTPUT_FORMATS = "gpkg"
SUPPORTED_OUTPUT_FORMATS = {"gpkg", "geojson"}

ADMIN_COLUMNS = [
    "state_name",
    "district_name",
    "TEHSIL",
    "pc11_village_id",
    "NAME",
    "pc11_state_id",
    "pc11_district_id",
    "pc11_subdistrict_id",
]

ADMIN_KEY_COLUMNS = ["state_name", "district_name", "TEHSIL", "pc11_village_id"]
ID_COLUMNS = {
    "pc11_village_id",
    "pc11_state_id",
    "pc11_district_id",
    "pc11_subdistrict_id",
}
JOIN_OUTPUT_INTEGER_COLUMNS = [
    "pc11_village_id",
    "pc11_state_id",
    "pc11_district_id",
    "pc11_subdistrict_id",
]

DEFAULT_DIAGNOSTIC_COLUMNS = ["No_HH", "TOT_P", "TOT_M", "TOT_F"]
GEOJSON_GEOMETRY_COLUMN = "wkb_geometry"
SQLITE_BATCH_SIZE = 5_000
DEFAULT_GPKG_CHUNK_SIZE = 50_000
DEFAULT_ORJSON_MAX_MB = 512
CSV_JOIN_LIMIT = 12
GEOMETRY_OVERLAP_AREA_EPSILON = 0.0

LIVESTOCK_COLUMNS = [
    "cattle_male",
    "cattle_female",
    "cattle_total",
    "buffalo_male",
    "buffalo_female",
    "buffalo_total",
    "sheep_male",
    "sheep_female",
    "sheep_total",
    "goat_male",
    "goat_female",
    "goat_total",
    "pig_male",
    "pig_female",
    "pig_total",
]


@dataclass(frozen=True, slots=True)
class SourceFileAudit:
    source_file: str
    source_state_slug: str
    source_district_slug: str
    read_status: str
    read_error: str
    feature_count: int | None
    field_count: int | None
    crs: str
    missing_admin_columns: str
    missing_diagnostic_columns: str
    rows_read: int
    valid_village_id_rows: int
    invalid_village_id_rows: int
    elapsed_seconds: float


@dataclass(frozen=True, slots=True)
class JoinTable:
    records_by_key: dict[str, dict[str, Any]]
    selected_columns: list[str]
    output_columns: list[str]
    output_to_source_column: dict[str, str]
    numeric_columns: set[str]
    integer_columns: set[str]
    source_rows: int
    source_rows_with_key: int
    duplicate_key_rows: int
    conflicting_duplicate_key_rows: int


def repo_path(path: Path | str) -> Path:
    path = Path(path)
    return path if path.is_absolute() else ROOT_DIR / path


def normalize_slug(value: Any) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")
    return slug or "unknown"


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def json_dumps_bytes(payload: Any) -> bytes:
    if orjson is not None:
        return orjson.dumps(payload, option=orjson.OPT_SORT_KEYS, default=str)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def json_dumps_text(payload: Any, *, indent: bool = False) -> str:
    if orjson is not None:
        options = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(payload, option=options, default=str).decode("utf-8")
    return json.dumps(payload, indent=2 if indent else None, ensure_ascii=False, default=str)


def json_loads_bytes(payload: bytes) -> Any:
    if orjson is not None:
        return orjson.loads(payload)
    return json.loads(payload.decode("utf-8"))


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return None
    return text


def clean_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value if value != 0 else None
    if isinstance(value, float):
        if math.isnan(value) or value == 0:
            return None
        if value.is_integer():
            return int(value)
    text = str(value).strip()
    if text.lower() in {"", "0", "0.0", "nan", "none", "null", "<na>"}:
        return None
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]
    if re.fullmatch(r"\d+", text):
        number = int(text)
        return number if number != 0 else None
    return None


def normalize_join_id(value: Any) -> str | None:
    number = clean_int(value)
    if number is not None:
        return str(number)
    text = clean_text(value)
    return text


def normalize_property(column: str, value: Any) -> int | str | None:
    if column in ID_COLUMNS:
        return clean_int(value)
    return clean_text(value)


def stable_identity_hash(properties: dict[str, Any], geometry_hash: str | None) -> str:
    payload = {
        "properties": {column: properties.get(column) for column in ADMIN_COLUMNS},
        "geometry_hash": geometry_hash,
    }
    return hashlib.sha256(json_dumps_bytes(payload)).hexdigest()


def stable_shape_hash(geometry: Any) -> str | None:
    if geometry is None or geometry.is_empty:
        return None
    normalised = geometry.normalize() if hasattr(geometry, "normalize") else geometry
    return hashlib.sha256(normalised.wkb).hexdigest()


def geometry_hash_from_wkb(wkb: bytes | None) -> str | None:
    if not wkb:
        return None
    # Keep ingestion fast: GDAL has already encoded the geometry as WKB, so use
    # that byte representation for source identity. Expensive Shapely
    # repair/normalisation only happens later for unique duplicate groups.
    return hashlib.sha256(bytes(wkb)).hexdigest()


def source_group_key_for(properties: dict[str, Any], identity_hash: str) -> str:
    village_id = properties.get("pc11_village_id")
    if village_id is None:
        return f"invalid:{identity_hash[:24]}"
    parts = [normalize_slug(properties.get(column)) for column in ADMIN_KEY_COLUMNS[:-1]]
    return "village:" + ":".join([*parts, str(village_id)])


def output_feature_key(source_group_key: str, part_index: int, part_count: int) -> str:
    if part_count == 1:
        return source_group_key
    return f"{source_group_key}:part:{part_index}"


def list_geojson_files(
    admin_input_dir: Path,
    *,
    state: str | None = None,
    district: str | None = None,
    limit_files: int | None = None,
) -> list[Path]:
    state_target = normalize_slug(state) if state else None
    district_target = normalize_slug(district) if district else None
    files: list[Path] = []
    for state_dir in sorted(path for path in admin_input_dir.iterdir() if path.is_dir()):
        if state_target and normalize_slug(state_dir.name) != state_target:
            continue
        for path in sorted(state_dir.glob("*.geojson")):
            if district_target and normalize_slug(path.stem) != district_target:
                continue
            files.append(path)
            if limit_files is not None and len(files) >= limit_files:
                return files
    return files


def ensure_dependencies() -> None:
    missing = []
    if pyogrio is None or gpd is None or pd is None:
        missing.append(f"geopandas/pyogrio/pandas ({PYOGRIO_STACK_IMPORT_ERROR})")
    if load_wkb is None or shapely_shape is None or unary_union is None:
        missing.append(f"shapely ({SHAPELY_IMPORT_ERROR})")
    if missing:
        raise SystemExit(
            "Missing geospatial dependencies: "
            + ", ".join(missing)
            + "\nActivate the project environment first, e.g. "
            + "`source /home/amitportal/miniconda3/etc/profile.d/conda.sh && conda activate corestack-backend`."
        )


def open_work_connection(sqlite_path: Path, *, overwrite: bool) -> sqlite3.Connection:
    if sqlite_path.exists():
        if not overwrite:
            raise FileExistsError(f"{sqlite_path} already exists. Pass --overwrite to rebuild it.")
        sqlite_path.unlink()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(sqlite_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA temp_store=MEMORY")
    return connection


def create_work_schema(connection: sqlite3.Connection, diagnostic_columns: Sequence[str]) -> None:
    diagnostic_defs = ",\n                ".join(f'"diag_{column}" TEXT' for column in diagnostic_columns)
    diagnostic_defs = f",\n                {diagnostic_defs}" if diagnostic_defs else ""

    connection.executescript(
        f"""
        CREATE TABLE source_rows (
            row_id INTEGER PRIMARY KEY,
            source_file TEXT NOT NULL,
            source_state_slug TEXT NOT NULL,
            source_district_slug TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            source_group_key TEXT NOT NULL,
            identity_hash TEXT NOT NULL,
            geometry_hash TEXT,
            pc11_village_id INTEGER,
            state_name TEXT,
            district_name TEXT,
            TEHSIL TEXT,
            NAME TEXT,
            pc11_state_id INTEGER,
            pc11_district_id INTEGER,
            pc11_subdistrict_id INTEGER
            {diagnostic_defs}
        );

        CREATE INDEX source_rows_group_idx ON source_rows(source_group_key);
        CREATE INDEX source_rows_identity_hash_idx ON source_rows(identity_hash);
        CREATE INDEX source_rows_pc11_village_id_idx ON source_rows(pc11_village_id);

        CREATE TABLE identities (
            identity_hash TEXT PRIMARY KEY,
            source_group_key TEXT NOT NULL,
            geometry_hash TEXT,
            geom_wkb BLOB,
            geom_json BLOB,
            source_row_count INTEGER NOT NULL DEFAULT 0,
            first_source_file TEXT NOT NULL,
            first_source_row_number INTEGER NOT NULL,
            state_name TEXT,
            district_name TEXT,
            TEHSIL TEXT,
            pc11_village_id INTEGER,
            NAME TEXT,
            pc11_state_id INTEGER,
            pc11_district_id INTEGER,
            pc11_subdistrict_id INTEGER
        );

        CREATE INDEX identities_group_idx ON identities(source_group_key);
        CREATE INDEX identities_pc11_village_id_idx ON identities(pc11_village_id);
        """
    )


def drop_feature_schema(connection: sqlite3.Connection) -> None:
    connection.executescript("DROP TABLE IF EXISTS features;")


def create_feature_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE features (
            cs_feature_id TEXT PRIMARY KEY,
            source_group_key TEXT NOT NULL,
            feature_part_index INTEGER NOT NULL,
            feature_part_count INTEGER NOT NULL,
            source_identity_hashes TEXT NOT NULL,
            village_shape_hash TEXT,
            pc11_village_id INTEGER,
            state_name TEXT,
            district_name TEXT,
            TEHSIL TEXT,
            NAME TEXT,
            pc11_state_id INTEGER,
            pc11_district_id INTEGER,
            pc11_subdistrict_id INTEGER,
            geom_wkb BLOB,
            min_lon REAL,
            max_lon REAL,
            min_lat REAL,
            max_lat REAL,
            source_row_count INTEGER NOT NULL,
            source_identity_count INTEGER NOT NULL,
            source_geometry_count INTEGER NOT NULL,
            source_file_count INTEGER NOT NULL,
            resolution_status TEXT NOT NULL
        );

        CREATE INDEX features_group_idx ON features(source_group_key);
        CREATE INDEX features_pc11_village_id_idx ON features(pc11_village_id);
        """
    )


def table_value(table: Any, column: str, row_index: int) -> Any:
    if column not in table.column_names:
        return None
    return table.column(column)[row_index].as_py()


def selected_reader(path: Path, requested_reader: str, *, orjson_max_mb: int) -> str:
    if requested_reader == "orjson":
        if orjson is None:
            raise SystemExit("`--reader orjson` requires the `orjson` package.")
        return "orjson"
    if requested_reader == "pyogrio":
        return "pyogrio"
    if requested_reader == "ijson":
        if ijson is None:
            raise SystemExit("`--reader ijson` requires the `ijson` package.")
        return "ijson"
    if (
        orjson is not None
        and path.suffix.lower() in {".geojson", ".json"}
        and path.stat().st_size <= orjson_max_mb * 1024 * 1024
    ):
        return "orjson"
    if ijson is not None and path.suffix.lower() in {".geojson", ".json"}:
        return "ijson"
    return "pyogrio"


def read_source_file(
    connection: sqlite3.Connection,
    path: Path,
    *,
    diagnostic_columns: Sequence[str],
    reader: str,
    orjson_max_mb: int,
) -> SourceFileAudit:
    chosen_reader = selected_reader(path, reader, orjson_max_mb=orjson_max_mb)
    if chosen_reader == "orjson":
        return read_source_file_orjson(
            connection,
            path,
            diagnostic_columns=diagnostic_columns,
        )
    if chosen_reader == "ijson":
        return read_source_file_ijson(
            connection,
            path,
            diagnostic_columns=diagnostic_columns,
        )
    return read_source_file_pyogrio(
        connection,
        path,
        diagnostic_columns=diagnostic_columns,
    )


def read_source_file_orjson(
    connection: sqlite3.Connection,
    path: Path,
    *,
    diagnostic_columns: Sequence[str],
) -> SourceFileAudit:
    started = time.perf_counter()
    source_file = path.as_posix()
    source_state_slug = path.parent.name
    source_district_slug = path.stem
    rows_read = 0
    valid_village_id_rows = 0
    invalid_village_id_rows = 0
    fields_seen: set[str] = set()

    try:
        payload = orjson.loads(path.read_bytes())
        features = payload.get("features") if isinstance(payload, dict) else None
        if not isinstance(features, list):
            raise ValueError("GeoJSON file does not contain a FeatureCollection features list")

        source_batch = []
        identity_batch = []
        for feature in features:
            if not isinstance(feature, dict):
                continue
            rows_read += 1
            raw_properties = feature.get("properties") or {}
            if not isinstance(raw_properties, dict):
                raw_properties = {}
            fields_seen.update(str(column) for column in raw_properties.keys())
            properties = {
                column: normalize_property(column, raw_properties.get(column))
                for column in ADMIN_COLUMNS
            }

            geometry = feature.get("geometry")
            geom_json = json_dumps_bytes(geometry) if geometry else None
            geom_hash = hashlib.sha256(geom_json).hexdigest() if geom_json else None
            identity_hash = stable_identity_hash(properties, geom_hash)
            source_group_key = source_group_key_for(properties, identity_hash)

            if properties["pc11_village_id"] is None:
                invalid_village_id_rows += 1
            else:
                valid_village_id_rows += 1

            identity_batch.append(
                (
                    identity_hash,
                    source_group_key,
                    geom_hash,
                    None,
                    sqlite3.Binary(geom_json) if geom_json else None,
                    source_file,
                    rows_read,
                    properties["state_name"],
                    properties["district_name"],
                    properties["TEHSIL"],
                    properties["pc11_village_id"],
                    properties["NAME"],
                    properties["pc11_state_id"],
                    properties["pc11_district_id"],
                    properties["pc11_subdistrict_id"],
                )
            )

            diagnostics = [
                clean_text(raw_properties.get(column))
                for column in diagnostic_columns
            ]
            source_batch.append(
                (
                    source_file,
                    source_state_slug,
                    source_district_slug,
                    rows_read,
                    source_group_key,
                    identity_hash,
                    geom_hash,
                    properties["pc11_village_id"],
                    properties["state_name"],
                    properties["district_name"],
                    properties["TEHSIL"],
                    properties["NAME"],
                    properties["pc11_state_id"],
                    properties["pc11_district_id"],
                    properties["pc11_subdistrict_id"],
                    *diagnostics,
                )
            )

            if len(identity_batch) >= SQLITE_BATCH_SIZE:
                flush_source_rows(connection, identity_batch, source_batch, diagnostic_columns)
                identity_batch.clear()
                source_batch.clear()

        flush_source_rows(connection, identity_batch, source_batch, diagnostic_columns)
        connection.commit()
        return SourceFileAudit(
            source_file=source_file,
            source_state_slug=source_state_slug,
            source_district_slug=source_district_slug,
            read_status="ok",
            read_error="",
            feature_count=rows_read,
            field_count=len(fields_seen),
            crs="",
            missing_admin_columns=",".join(column for column in ADMIN_COLUMNS if column not in fields_seen),
            missing_diagnostic_columns=",".join(column for column in diagnostic_columns if column not in fields_seen),
            rows_read=rows_read,
            valid_village_id_rows=valid_village_id_rows,
            invalid_village_id_rows=invalid_village_id_rows,
            elapsed_seconds=round(time.perf_counter() - started, 6),
        )
    except Exception as exc:  # noqa: BLE001 - continue across hundreds of files.
        connection.rollback()
        return SourceFileAudit(
            source_file=source_file,
            source_state_slug=source_state_slug,
            source_district_slug=source_district_slug,
            read_status="error",
            read_error=repr(exc),
            feature_count=None,
            field_count=len(fields_seen) if fields_seen else None,
            crs="",
            missing_admin_columns="",
            missing_diagnostic_columns="",
            rows_read=rows_read,
            valid_village_id_rows=valid_village_id_rows,
            invalid_village_id_rows=invalid_village_id_rows,
            elapsed_seconds=round(time.perf_counter() - started, 6),
        )


def read_source_file_ijson(
    connection: sqlite3.Connection,
    path: Path,
    *,
    diagnostic_columns: Sequence[str],
) -> SourceFileAudit:
    started = time.perf_counter()
    source_file = path.as_posix()
    source_state_slug = path.parent.name
    source_district_slug = path.stem
    rows_read = 0
    valid_village_id_rows = 0
    invalid_village_id_rows = 0
    fields_seen: set[str] = set()

    try:
        source_batch = []
        identity_batch = []
        with path.open("rb") as handle:
            for feature in ijson.items(handle, "features.item", use_float=True):
                rows_read += 1
                raw_properties = feature.get("properties") or {}
                if not isinstance(raw_properties, dict):
                    raw_properties = {}
                fields_seen.update(str(column) for column in raw_properties.keys())
                properties = {
                    column: normalize_property(column, raw_properties.get(column))
                    for column in ADMIN_COLUMNS
                }

                geometry = feature.get("geometry")
                geom_json = json_dumps_bytes(geometry) if geometry else None
                geom_hash = hashlib.sha256(geom_json).hexdigest() if geom_json else None
                identity_hash = stable_identity_hash(properties, geom_hash)
                source_group_key = source_group_key_for(properties, identity_hash)

                if properties["pc11_village_id"] is None:
                    invalid_village_id_rows += 1
                else:
                    valid_village_id_rows += 1

                identity_batch.append(
                    (
                        identity_hash,
                        source_group_key,
                        geom_hash,
                        None,
                        sqlite3.Binary(geom_json) if geom_json else None,
                        source_file,
                        rows_read,
                        properties["state_name"],
                        properties["district_name"],
                        properties["TEHSIL"],
                        properties["pc11_village_id"],
                        properties["NAME"],
                        properties["pc11_state_id"],
                        properties["pc11_district_id"],
                        properties["pc11_subdistrict_id"],
                    )
                )

                diagnostics = [
                    clean_text(raw_properties.get(column))
                    for column in diagnostic_columns
                ]
                source_batch.append(
                    (
                        source_file,
                        source_state_slug,
                        source_district_slug,
                        rows_read,
                        source_group_key,
                        identity_hash,
                        geom_hash,
                        properties["pc11_village_id"],
                        properties["state_name"],
                        properties["district_name"],
                        properties["TEHSIL"],
                        properties["NAME"],
                        properties["pc11_state_id"],
                        properties["pc11_district_id"],
                        properties["pc11_subdistrict_id"],
                        *diagnostics,
                    )
                )

                if len(identity_batch) >= SQLITE_BATCH_SIZE:
                    flush_source_rows(connection, identity_batch, source_batch, diagnostic_columns)
                    identity_batch.clear()
                    source_batch.clear()

        flush_source_rows(connection, identity_batch, source_batch, diagnostic_columns)
        connection.commit()
        return SourceFileAudit(
            source_file=source_file,
            source_state_slug=source_state_slug,
            source_district_slug=source_district_slug,
            read_status="ok",
            read_error="",
            feature_count=rows_read,
            field_count=len(fields_seen),
            crs="",
            missing_admin_columns=",".join(column for column in ADMIN_COLUMNS if column not in fields_seen),
            missing_diagnostic_columns=",".join(column for column in diagnostic_columns if column not in fields_seen),
            rows_read=rows_read,
            valid_village_id_rows=valid_village_id_rows,
            invalid_village_id_rows=invalid_village_id_rows,
            elapsed_seconds=round(time.perf_counter() - started, 6),
        )
    except Exception as exc:  # noqa: BLE001 - continue across hundreds of files.
        connection.rollback()
        return SourceFileAudit(
            source_file=source_file,
            source_state_slug=source_state_slug,
            source_district_slug=source_district_slug,
            read_status="error",
            read_error=repr(exc),
            feature_count=None,
            field_count=len(fields_seen) if fields_seen else None,
            crs="",
            missing_admin_columns="",
            missing_diagnostic_columns="",
            rows_read=rows_read,
            valid_village_id_rows=valid_village_id_rows,
            invalid_village_id_rows=invalid_village_id_rows,
            elapsed_seconds=round(time.perf_counter() - started, 6),
        )


def read_source_file_pyogrio(
    connection: sqlite3.Connection,
    path: Path,
    *,
    diagnostic_columns: Sequence[str],
) -> SourceFileAudit:
    started = time.perf_counter()
    source_file = path.as_posix()
    source_state_slug = path.parent.name
    source_district_slug = path.stem
    rows_read = 0
    valid_village_id_rows = 0
    invalid_village_id_rows = 0

    try:
        info = pyogrio.read_info(path)
        fields = [str(field) for field in info.get("fields", [])]
        field_set = set(fields)
        read_columns = [
            column
            for column in [*ADMIN_COLUMNS, *diagnostic_columns]
            if column in field_set
        ]
        missing_admin_columns = ",".join(column for column in ADMIN_COLUMNS if column not in field_set)
        missing_diagnostic_columns = ",".join(column for column in diagnostic_columns if column not in field_set)
        _, table = pyogrio.read_arrow(path, columns=read_columns, read_geometry=True)
        geometry_column = GEOJSON_GEOMETRY_COLUMN if GEOJSON_GEOMETRY_COLUMN in table.column_names else table.column_names[-1]

        source_batch = []
        identity_batch = []
        for row_index in range(table.num_rows):
            rows_read += 1
            properties = {
                column: normalize_property(column, table_value(table, column, row_index))
                for column in ADMIN_COLUMNS
            }
            wkb = table_value(table, geometry_column, row_index)
            geom_hash = geometry_hash_from_wkb(wkb)
            identity_hash = stable_identity_hash(properties, geom_hash)
            source_group_key = source_group_key_for(properties, identity_hash)

            if properties["pc11_village_id"] is None:
                invalid_village_id_rows += 1
            else:
                valid_village_id_rows += 1

            identity_batch.append(
                (
                    identity_hash,
                    source_group_key,
                    geom_hash,
                    sqlite3.Binary(wkb) if wkb else None,
                    None,
                    source_file,
                    rows_read,
                    properties["state_name"],
                    properties["district_name"],
                    properties["TEHSIL"],
                    properties["pc11_village_id"],
                    properties["NAME"],
                    properties["pc11_state_id"],
                    properties["pc11_district_id"],
                    properties["pc11_subdistrict_id"],
                )
            )

            diagnostics = [
                clean_text(table_value(table, column, row_index))
                for column in diagnostic_columns
            ]
            source_batch.append(
                (
                    source_file,
                    source_state_slug,
                    source_district_slug,
                    rows_read,
                    source_group_key,
                    identity_hash,
                    geom_hash,
                    properties["pc11_village_id"],
                    properties["state_name"],
                    properties["district_name"],
                    properties["TEHSIL"],
                    properties["NAME"],
                    properties["pc11_state_id"],
                    properties["pc11_district_id"],
                    properties["pc11_subdistrict_id"],
                    *diagnostics,
                )
            )

            if len(identity_batch) >= SQLITE_BATCH_SIZE:
                flush_source_rows(connection, identity_batch, source_batch, diagnostic_columns)
                identity_batch.clear()
                source_batch.clear()

        flush_source_rows(connection, identity_batch, source_batch, diagnostic_columns)
        connection.commit()
        return SourceFileAudit(
            source_file=source_file,
            source_state_slug=source_state_slug,
            source_district_slug=source_district_slug,
            read_status="ok",
            read_error="",
            feature_count=info.get("features"),
            field_count=len(fields),
            crs=str(info.get("crs") or ""),
            missing_admin_columns=missing_admin_columns,
            missing_diagnostic_columns=missing_diagnostic_columns,
            rows_read=rows_read,
            valid_village_id_rows=valid_village_id_rows,
            invalid_village_id_rows=invalid_village_id_rows,
            elapsed_seconds=round(time.perf_counter() - started, 6),
        )
    except Exception as exc:  # noqa: BLE001 - keep long all-India scans resilient.
        connection.rollback()
        return SourceFileAudit(
            source_file=source_file,
            source_state_slug=source_state_slug,
            source_district_slug=source_district_slug,
            read_status="error",
            read_error=repr(exc),
            feature_count=None,
            field_count=None,
            crs="",
            missing_admin_columns="",
            missing_diagnostic_columns="",
            rows_read=rows_read,
            valid_village_id_rows=valid_village_id_rows,
            invalid_village_id_rows=invalid_village_id_rows,
            elapsed_seconds=round(time.perf_counter() - started, 6),
        )


def flush_source_rows(
    connection: sqlite3.Connection,
    identity_batch: list[tuple[Any, ...]],
    source_batch: list[tuple[Any, ...]],
    diagnostic_columns: Sequence[str],
) -> None:
    if identity_batch:
        connection.executemany(
            """
            INSERT INTO identities (
                identity_hash,
                source_group_key,
                geometry_hash,
                geom_wkb,
                geom_json,
                source_row_count,
                first_source_file,
                first_source_row_number,
                state_name,
                district_name,
                TEHSIL,
                pc11_village_id,
                NAME,
                pc11_state_id,
                pc11_district_id,
                pc11_subdistrict_id
            ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(identity_hash) DO UPDATE SET
                source_row_count = identities.source_row_count + 1
            """,
            identity_batch,
        )
    if source_batch:
        diagnostic_names = ", ".join(f'"diag_{column}"' for column in diagnostic_columns)
        diagnostic_placeholders = ", ".join("?" for _ in diagnostic_columns)
        diagnostic_sql = f", {diagnostic_names}" if diagnostic_columns else ""
        diagnostic_values = f", {diagnostic_placeholders}" if diagnostic_columns else ""
        connection.executemany(
            f"""
            INSERT INTO source_rows (
                source_file,
                source_state_slug,
                source_district_slug,
                source_row_number,
                source_group_key,
                identity_hash,
                geometry_hash,
                pc11_village_id,
                state_name,
                district_name,
                TEHSIL,
                NAME,
                pc11_state_id,
                pc11_district_id,
                pc11_subdistrict_id
                {diagnostic_sql}
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?{diagnostic_values})
            """,
            source_batch,
        )


def repair_geometry(geometry: Any) -> Any | None:
    if geometry is None or geometry.is_empty:
        return None
    if not geometry.is_valid:
        if shapely_make_valid is not None:
            geometry = shapely_make_valid(geometry)
        else:  # pragma: no cover - compatibility fallback
            geometry = geometry.buffer(0)
    if geometry.is_empty:
        return None
    return polygonal_geometry(geometry)


def polygonal_geometry(geometry: Any) -> Any | None:
    if geometry is None or geometry.is_empty:
        return None
    if geometry.geom_type in {"Polygon", "MultiPolygon"}:
        return geometry
    if geometry.geom_type == "GeometryCollection":
        polygons = []
        for part in geometry.geoms:
            if part.geom_type == "Polygon":
                polygons.append(part)
            elif part.geom_type == "MultiPolygon":
                polygons.extend(part.geoms)
        if not polygons:
            return None
        return MultiPolygon(polygons)
    return None


def promote_polygonal_to_multi(geometry: Any) -> Any | None:
    geometry = repair_geometry(geometry)
    if geometry is None:
        return None
    if geometry.geom_type == "Polygon":
        return MultiPolygon([geometry])
    return geometry


def load_identity_geometry(row: sqlite3.Row) -> Any | None:
    if row["geom_wkb"] is not None:
        return repair_geometry(load_wkb(bytes(row["geom_wkb"])))
    if "geom_json" in row.keys() and row["geom_json"] is not None:
        return repair_geometry(shapely_shape(json_loads_bytes(bytes(row["geom_json"]))))
    return None


def load_group_geometries(rows: Sequence[sqlite3.Row]) -> list[Any]:
    geometries = []
    for row in rows:
        geometry = load_identity_geometry(row)
        if geometry is not None:
            geometries.append(geometry)
    return geometries


def dissolve_geometries(geometries: Sequence[Any]) -> Any | None:
    if not geometries:
        return None
    if len(geometries) == 1:
        return promote_polygonal_to_multi(geometries[0])
    return promote_polygonal_to_multi(unary_union(geometries))


def should_dissolve_geometry_pair(left_geometry: Any, right_geometry: Any) -> bool:
    if left_geometry.equals(right_geometry):
        return True
    if not left_geometry.intersects(right_geometry):
        return False
    intersection = left_geometry.intersection(right_geometry)
    if intersection.is_empty:
        return False
    return float(intersection.area) > GEOMETRY_OVERLAP_AREA_EPSILON


def geometry_components(identity_rows: Sequence[sqlite3.Row]) -> list[list[sqlite3.Row]]:
    """Group identities into non-overlapping output geometry parts."""

    rows_with_geometry: list[tuple[sqlite3.Row, Any]] = []
    rows_without_geometry: list[sqlite3.Row] = []
    for row in identity_rows:
        geometry = load_identity_geometry(row)
        if geometry is None:
            rows_without_geometry.append(row)
            continue
        rows_with_geometry.append((row, geometry))

    if not rows_with_geometry:
        return [list(rows_without_geometry)] if rows_without_geometry else []

    parent = list(range(len(rows_with_geometry)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for left_index, (_, left_geometry) in enumerate(rows_with_geometry):
        for right_index in range(left_index + 1, len(rows_with_geometry)):
            _, right_geometry = rows_with_geometry[right_index]
            if should_dissolve_geometry_pair(left_geometry, right_geometry):
                union(left_index, right_index)

    grouped: dict[int, list[sqlite3.Row]] = {}
    order: list[int] = []
    for index, (row, _) in enumerate(rows_with_geometry):
        root = find(index)
        if root not in grouped:
            grouped[root] = []
            order.append(root)
        grouped[root].append(row)

    if rows_without_geometry:
        grouped[order[0]].extend(rows_without_geometry)

    return [grouped[root] for root in order]


def iter_identity_groups(connection: sqlite3.Connection) -> Iterator[tuple[str, list[sqlite3.Row]]]:
    current_key = None
    current_rows: list[sqlite3.Row] = []
    for row in connection.execute(
        """
        SELECT *
        FROM identities
        ORDER BY source_group_key, first_source_file, first_source_row_number, identity_hash
        """
    ):
        source_group_key = row["source_group_key"]
        if current_key is not None and source_group_key != current_key:
            yield current_key, current_rows
            current_rows = []
        current_key = source_group_key
        current_rows.append(row)
    if current_key is not None:
        yield current_key, current_rows


def ordered_distinct(values: Iterable[Any]) -> list[Any]:
    seen = set()
    output = []
    for value in values:
        if value is None:
            continue
        key = str(value)
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def choose_property(values: Sequence[Any], *, numeric: bool) -> Any:
    distinct = ordered_distinct(values)
    if not distinct:
        return None
    if len(distinct) == 1:
        return distinct[0]
    if numeric:
        return distinct[0]
    if len(distinct) > CSV_JOIN_LIMIT:
        return " | ".join(str(value) for value in distinct[:CSV_JOIN_LIMIT]) + f" | ... (+{len(distinct) - CSV_JOIN_LIMIT})"
    return " | ".join(str(value) for value in distinct)


def classify_resolution(
    row_count: int,
    identity_count: int,
    geometry_count: int,
    *,
    part_count: int,
) -> str:
    if part_count > 1:
        if row_count == 1 and identity_count == 1 and geometry_count == 1:
            return "split_distinct_geometry_part"
        if geometry_count == 1:
            return "split_collapsed_duplicate_geometry_part"
        return "split_dissolved_overlapping_geometry_part"
    if row_count == 1 and identity_count == 1 and geometry_count == 1:
        return "single_source_row"
    if identity_count == 1 and geometry_count == 1:
        return "collapsed_exact_duplicate_rows"
    if geometry_count == 1:
        return "collapsed_attribute_variants_same_geometry"
    return "dissolved_overlapping_geometries"


def build_features(connection: sqlite3.Connection) -> dict[str, Any]:
    started = time.perf_counter()
    drop_feature_schema(connection)
    create_feature_schema(connection)

    insert_rows = []
    skipped_empty_geometry = 0
    source_group_keys = 0
    inserted_feature_rows = 0
    split_source_group_keys = 0
    split_geometry_part_features = 0
    resolution_counts: Counter[str] = Counter()
    multi_geometry_source_groups = 0
    multi_geometry_features = 0

    for source_group_key, identity_rows in iter_identity_groups(connection):
        source_group_keys += 1
        source_geometry_count = len({row["geometry_hash"] for row in identity_rows if row["geometry_hash"]})
        if source_geometry_count > 1:
            multi_geometry_source_groups += 1
        components = geometry_components(identity_rows)
        part_count = len(components)
        if part_count == 0:
            skipped_empty_geometry += 1
            continue
        if part_count > 1:
            split_source_group_keys += 1

        for part_index, component_rows in enumerate(components, start=1):
            geometries = load_group_geometries(component_rows)
            geometry = dissolve_geometries(geometries)
            if geometry is None:
                skipped_empty_geometry += 1
                continue
            min_lon, min_lat, max_lon, max_lat = (float(value) for value in geometry.bounds)

            source_row_count = sum(int(row["source_row_count"]) for row in component_rows)
            identity_count = len(component_rows)
            geometry_count = len({row["geometry_hash"] for row in component_rows if row["geometry_hash"]})
            source_file_count = len({row["first_source_file"] for row in component_rows})
            source_identity_hashes = "|".join(row["identity_hash"] for row in component_rows)

            if geometry_count > 1:
                multi_geometry_features += 1
            if part_count > 1:
                split_geometry_part_features += 1
            resolution_status = classify_resolution(
                source_row_count,
                identity_count,
                geometry_count,
                part_count=part_count,
            )
            resolution_counts[resolution_status] += 1

            values_by_column = {
                column: [row[column] for row in component_rows]
                for column in ADMIN_COLUMNS
            }
            properties = {
                column: choose_property(values_by_column[column], numeric=column in ID_COLUMNS)
                for column in ADMIN_COLUMNS
            }
            cs_feature_id = output_feature_key(source_group_key, part_index, part_count)
            insert_rows.append(
                (
                    cs_feature_id,
                    source_group_key,
                    part_index,
                    part_count,
                    source_identity_hashes,
                    stable_shape_hash(geometry),
                    properties["pc11_village_id"],
                    properties["state_name"],
                    properties["district_name"],
                    properties["TEHSIL"],
                    properties["NAME"],
                    properties["pc11_state_id"],
                    properties["pc11_district_id"],
                    properties["pc11_subdistrict_id"],
                    sqlite3.Binary(geometry.wkb),
                    min_lon,
                    max_lon,
                    min_lat,
                    max_lat,
                    source_row_count,
                    identity_count,
                    geometry_count,
                    source_file_count,
                    resolution_status,
                )
            )
            inserted_feature_rows += 1

            if len(insert_rows) >= SQLITE_BATCH_SIZE:
                flush_features(connection, insert_rows)
                insert_rows.clear()

    flush_features(connection, insert_rows)
    connection.commit()
    return {
        "feature_rows": inserted_feature_rows,
        "source_group_keys": source_group_keys,
        "skipped_empty_geometry_features": skipped_empty_geometry,
        "multi_geometry_source_groups": multi_geometry_source_groups,
        "multi_geometry_features": multi_geometry_features,
        "split_source_group_keys": split_source_group_keys,
        "split_geometry_part_features": split_geometry_part_features,
        "resolution_status_counts": dict(resolution_counts),
        "feature_build_seconds": round(time.perf_counter() - started, 6),
    }


def flush_features(connection: sqlite3.Connection, feature_rows: list[tuple[Any, ...]]) -> None:
    if feature_rows:
        connection.executemany(
            """
            INSERT INTO features (
                cs_feature_id,
                source_group_key,
                feature_part_index,
                feature_part_count,
                source_identity_hashes,
                village_shape_hash,
                pc11_village_id,
                state_name,
                district_name,
                TEHSIL,
                NAME,
                pc11_state_id,
                pc11_district_id,
                pc11_subdistrict_id,
                geom_wkb,
                min_lon,
                max_lon,
                min_lat,
                max_lat,
                source_row_count,
                source_identity_count,
                source_geometry_count,
                source_file_count,
                resolution_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            feature_rows,
        )


def iter_feature_rows(connection: sqlite3.Connection, *, chunk_size: int) -> Iterator[list[sqlite3.Row]]:
    offset = 0
    while True:
        rows = connection.execute(
            """
            SELECT *
            FROM features
            ORDER BY
                pc11_state_id,
                pc11_district_id,
                pc11_subdistrict_id,
                pc11_village_id,
                cs_feature_id
            LIMIT ? OFFSET ?
            """,
            (chunk_size, offset),
        ).fetchall()
        if not rows:
            break
        yield rows
        offset += len(rows)


def feature_rows_to_gdf(rows: Sequence[sqlite3.Row]) -> Any:
    records = []
    for row in rows:
        geometry = promote_polygonal_to_multi(load_wkb(bytes(row["geom_wkb"]))) if row["geom_wkb"] is not None else None
        record = {
            "cs_feature_id": row["cs_feature_id"],
            "source_group_key": row["source_group_key"],
            "feature_part_index": row["feature_part_index"],
            "feature_part_count": row["feature_part_count"],
            "village_shape_hash": row["village_shape_hash"],
            "state_name": row["state_name"],
            "district_name": row["district_name"],
            "TEHSIL": row["TEHSIL"],
            "pc11_village_id": row["pc11_village_id"],
            "NAME": row["NAME"],
            "pc11_state_id": row["pc11_state_id"],
            "pc11_district_id": row["pc11_district_id"],
            "pc11_subdistrict_id": row["pc11_subdistrict_id"],
            "source_row_count": row["source_row_count"],
            "source_identity_count": row["source_identity_count"],
            "source_geometry_count": row["source_geometry_count"],
            "source_file_count": row["source_file_count"],
            "resolution_status": row["resolution_status"],
            "geometry": geometry,
        }
        records.append(record)
    gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
    for column in [
        "feature_part_index",
        "feature_part_count",
        "pc11_village_id",
        "pc11_state_id",
        "pc11_district_id",
        "pc11_subdistrict_id",
        "source_row_count",
        "source_identity_count",
        "source_geometry_count",
        "source_file_count",
    ]:
        gdf[column] = pd.to_numeric(gdf[column], errors="coerce").astype("Int64")
    return gdf


def write_admin_gpkg(
    connection: sqlite3.Connection,
    gpkg_path: Path,
    *,
    layer: str,
    overwrite: bool,
    chunk_size: int,
) -> int:
    if gpkg_path.exists():
        if not overwrite:
            raise SystemExit(f"{gpkg_path} exists. Pass --overwrite to rebuild it.")
        gpkg_path.unlink()
    gpkg_path.parent.mkdir(parents=True, exist_ok=True)

    rows_written = 0
    first_chunk = True
    for rows in iter_feature_rows(connection, chunk_size=chunk_size):
        gdf = feature_rows_to_gdf(rows)
        pyogrio.write_dataframe(
            gdf,
            gpkg_path,
            layer=layer,
            driver="GPKG",
            append=not first_chunk,
            promote_to_multi=True,
        )
        rows_written += len(gdf)
        first_chunk = False
        if rows_written == len(gdf) or rows_written % (chunk_size * 2) == 0:
            print(f"[admin] wrote {rows_written:,} sanitised features to {gpkg_path}", flush=True)

    ensure_gpkg_indexes(
        gpkg_path,
        layer,
        [
            ["pc11_village_id"],
            ["state_name", "district_name", "TEHSIL"],
            ["pc11_state_id", "pc11_district_id", "pc11_subdistrict_id"],
        ],
    )
    return rows_written


def ensure_gpkg_indexes(gpkg_path: Path, layer: str, index_columns: Sequence[Sequence[str]]) -> None:
    if not gpkg_path.exists():
        return
    with sqlite3.connect(gpkg_path) as connection:
        for columns in index_columns:
            suffix = "_".join(normalize_slug(column) for column in columns)
            index_name = f"idx_{normalize_slug(layer)}_{suffix}"
            columns_sql = ", ".join(quote_identifier(column) for column in columns)
            connection.execute(
                f"CREATE INDEX IF NOT EXISTS {quote_identifier(index_name)} "
                f"ON {quote_identifier(layer)} ({columns_sql})"
            )
        connection.commit()


def split_identity_hashes(value: str | None) -> list[str]:
    return [item for item in str(value or "").split("|") if item]


def join_distinct(values: Iterable[Any], *, limit: int = CSV_JOIN_LIMIT) -> str:
    distinct = ordered_distinct(values)
    if len(distinct) > limit:
        return " | ".join(str(value) for value in distinct[:limit]) + f" | ... (+{len(distinct) - limit})"
    return " | ".join(str(value) for value in distinct)


def no_hh_pattern(values: Sequence[str | None]) -> str:
    numbers = []
    for value in values:
        number = clean_int(value)
        if number is not None:
            numbers.append(number)
    distinct = sorted(set(numbers))
    if len(distinct) < 3:
        return "not_enough_distinct_values"
    largest = distinct[-1]
    if largest == sum(distinct[:-1]):
        return "largest_equals_sum_of_other_distinct_values"
    return "multiple_distinct_values"


def fetch_source_rows_for_identity_hashes(
    connection: sqlite3.Connection,
    identity_hashes: Sequence[str],
) -> list[sqlite3.Row]:
    if not identity_hashes:
        return []
    placeholders = ", ".join("?" for _ in identity_hashes)
    return connection.execute(
        f"""
        SELECT *
        FROM source_rows
        WHERE identity_hash IN ({placeholders})
        ORDER BY source_file, source_row_number
        """,
        tuple(identity_hashes),
    ).fetchall()


def write_duplicate_reports(
    connection: sqlite3.Connection,
    reports_dir: Path,
    *,
    diagnostic_columns: Sequence[str],
) -> dict[str, Any]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    duplicate_csv = reports_dir / "duplicate_resolution_groups.csv"
    diagnostic_csv = reports_dir / "diagnostic_duplicate_patterns.csv"
    overlap_csv = reports_dir / "multi_geometry_overlap_pairs.csv"

    duplicate_count = 0
    diagnostic_count = 0
    overlap_count = 0

    with duplicate_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "cs_feature_id",
                "source_group_key",
                "feature_part_index",
                "feature_part_count",
                "pc11_village_id",
                "source_row_count",
                "source_identity_count",
                "source_geometry_count",
                "source_file_count",
                "resolution_status",
                "state_name_values",
                "district_name_values",
                "tehsil_values",
                "name_values",
                "source_files",
            ],
        )
        writer.writeheader()
        for row in connection.execute(
            """
            SELECT *
            FROM features
            WHERE source_row_count > 1 OR source_identity_count > 1 OR source_geometry_count > 1
            ORDER BY source_row_count DESC, source_identity_count DESC, cs_feature_id
            """
        ):
            identity_hashes = split_identity_hashes(row["source_identity_hashes"])
            source_rows = fetch_source_rows_for_identity_hashes(connection, identity_hashes)
            writer.writerow(
                {
                    "cs_feature_id": row["cs_feature_id"],
                    "source_group_key": row["source_group_key"],
                    "feature_part_index": row["feature_part_index"],
                    "feature_part_count": row["feature_part_count"],
                    "pc11_village_id": row["pc11_village_id"],
                    "source_row_count": row["source_row_count"],
                    "source_identity_count": row["source_identity_count"],
                    "source_geometry_count": row["source_geometry_count"],
                    "source_file_count": row["source_file_count"],
                    "resolution_status": row["resolution_status"],
                    "state_name_values": join_distinct([source["state_name"] for source in source_rows]),
                    "district_name_values": join_distinct([source["district_name"] for source in source_rows]),
                    "tehsil_values": join_distinct([source["TEHSIL"] for source in source_rows]),
                    "name_values": join_distinct([source["NAME"] for source in source_rows]),
                    "source_files": join_distinct([source["source_file"] for source in source_rows], limit=5),
                }
            )
            duplicate_count += 1

    if "No_HH" in diagnostic_columns:
        with diagnostic_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "identity_hash",
                    "source_group_key",
                    "pc11_village_id",
                    "geometry_hash",
                    "source_row_count",
                    "No_HH_values",
                    "No_HH_pattern",
                    "state_name",
                    "district_name",
                    "TEHSIL",
                    "NAME",
                    "source_files",
                ],
            )
            writer.writeheader()
            for identity in connection.execute(
                """
                SELECT *
                FROM identities
                WHERE source_row_count > 1
                ORDER BY source_row_count DESC, source_group_key, identity_hash
                """
            ):
                source_rows = connection.execute(
                    "SELECT * FROM source_rows WHERE identity_hash = ? ORDER BY source_file, source_row_number",
                    (identity["identity_hash"],),
                ).fetchall()
                no_hh_values = [row["diag_No_HH"] for row in source_rows]
                distinct_no_hh = ordered_distinct(no_hh_values)
                if len(distinct_no_hh) <= 1:
                    continue
                writer.writerow(
                    {
                        "identity_hash": identity["identity_hash"],
                        "source_group_key": identity["source_group_key"],
                        "pc11_village_id": identity["pc11_village_id"],
                        "geometry_hash": identity["geometry_hash"],
                        "source_row_count": identity["source_row_count"],
                        "No_HH_values": join_distinct(no_hh_values, limit=40),
                        "No_HH_pattern": no_hh_pattern(no_hh_values),
                        "state_name": identity["state_name"],
                        "district_name": identity["district_name"],
                        "TEHSIL": identity["TEHSIL"],
                        "NAME": identity["NAME"],
                        "source_files": join_distinct([row["source_file"] for row in source_rows], limit=5),
                    }
                )
                diagnostic_count += 1

    with overlap_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "source_group_key",
                "left_output_feature_key",
                "right_output_feature_key",
                "pc11_village_id",
                "left_identity_hash",
                "right_identity_hash",
                "left_name",
                "right_name",
                "relation",
                "intersection_area_degrees2",
            ],
        )
        writer.writeheader()
        identity_to_output_feature_key = {}
        for feature in connection.execute("SELECT cs_feature_id, source_identity_hashes FROM features"):
            for identity_hash in split_identity_hashes(feature["source_identity_hashes"]):
                identity_to_output_feature_key[identity_hash] = feature["cs_feature_id"]

        for source_group_key, identity_rows in iter_identity_groups(connection):
            if len({row["geometry_hash"] for row in identity_rows if row["geometry_hash"]}) <= 1:
                continue
            geometries = []
            for row in identity_rows:
                geometry = load_identity_geometry(row)
                if geometry is not None:
                    geometries.append((row, geometry))
            for left_index, (left_row, left_geom) in enumerate(geometries):
                for right_row, right_geom in geometries[left_index + 1 :]:
                    if left_geom.equals(right_geom):
                        relation = "equals"
                        intersection_area = left_geom.area
                    elif left_geom.intersects(right_geom):
                        intersection = left_geom.intersection(right_geom)
                        intersection_area = 0.0 if intersection.is_empty else float(intersection.area)
                        relation = "intersects" if intersection_area > 0 else "touches"
                    else:
                        relation = "disjoint"
                        intersection_area = 0.0
                    writer.writerow(
                        {
                            "source_group_key": source_group_key,
                            "left_output_feature_key": identity_to_output_feature_key.get(left_row["identity_hash"]),
                            "right_output_feature_key": identity_to_output_feature_key.get(right_row["identity_hash"]),
                            "pc11_village_id": left_row["pc11_village_id"],
                            "left_identity_hash": left_row["identity_hash"],
                            "right_identity_hash": right_row["identity_hash"],
                            "left_name": left_row["NAME"],
                            "right_name": right_row["NAME"],
                            "relation": relation,
                            "intersection_area_degrees2": round(intersection_area, 12),
                        }
                    )
                    overlap_count += 1

    return {
        "duplicate_resolution_groups": duplicate_count,
        "diagnostic_duplicate_patterns": diagnostic_count,
        "multi_geometry_overlap_pairs": overlap_count,
    }


def write_file_audit(path: Path, audits: Sequence[SourceFileAudit]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(SourceFileAudit.__annotations__.keys()))
        writer.writeheader()
        for audit in audits:
            writer.writerow(asdict(audit))


def fetch_work_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        "source_rows": connection.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0],
        "identity_rows": connection.execute("SELECT COUNT(*) FROM identities").fetchone()[0],
        "feature_rows": connection.execute("SELECT COUNT(*) FROM features").fetchone()[0],
        "valid_village_id_feature_rows": connection.execute(
            "SELECT COUNT(*) FROM features WHERE pc11_village_id IS NOT NULL"
        ).fetchone()[0],
        "invalid_village_id_feature_rows": connection.execute(
            "SELECT COUNT(*) FROM features WHERE pc11_village_id IS NULL"
        ).fetchone()[0],
    }


def write_admin_summary(
    reports_dir: Path,
    *,
    metadata: dict[str, Any],
    summary: dict[str, Any],
) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "build_summary.json").write_text(
        json_dumps_text({"metadata": metadata, "summary": summary}, indent=True),
        encoding="utf-8",
    )
    resolution_lines = [
        f"- `{key}`: `{value:,}`"
        for key, value in sorted(summary.get("resolution_status_counts", {}).items())
    ] or ["- None"]
    report = f"""# Core Stack Admin Boundary Sanitised Asset

Generated by `utilities/scripts/build_admin_boundary_assets.py build-admin`.

## Outputs

- GeoPackage: `{metadata['admin_gpkg']}`
- Layer: `{metadata['admin_layer']}`
- Source file audit: `{metadata['file_audit_path']}`
- Duplicate resolution groups: `{metadata['duplicate_groups_path']}`
- Diagnostic duplicate patterns: `{metadata['diagnostic_patterns_path']}`
- Multi-geometry overlap pairs: `{metadata['overlap_pairs_path']}`

## Counts

- Source files represented: `{summary['source_file_count']:,}`
- Source rows represented: `{summary['source_rows']:,}`
- Valid `pc11_village_id` source rows: `{summary['valid_village_id_rows']:,}`
- Invalid/zero `pc11_village_id` source rows retained by source identity: `{summary['invalid_village_id_rows']:,}`
- Unique source identities: `{summary['identity_rows']:,}`
- Sanitised feature rows: `{summary['feature_rows']:,}`
- Valid `pc11_village_id` feature rows: `{summary['valid_village_id_feature_rows']:,}`
- Invalid `pc11_village_id` feature rows: `{summary['invalid_village_id_feature_rows']:,}`
- Multi-geometry source groups: `{summary['multi_geometry_source_groups']:,}`
- Split source groups with touching/disjoint geometry parts: `{summary['split_source_group_keys']:,}`
- Output features from split geometry parts: `{summary['split_geometry_part_features']:,}`
- Output features with dissolved overlapping geometries: `{summary['multi_geometry_features']:,}`
- Features skipped for empty geometry: `{summary['skipped_empty_geometry_features']:,}`

## Resolution Status

{chr(10).join(resolution_lines)}

## Notes

- The sanitised feature identity is based on state, district, tehsil,
  `pc11_village_id`, and geometry.
- Exact duplicate rows collapse. Positive-area overlaps dissolve. Touching or
  disjoint duplicate shapes remain separate feature parts.
- `No_HH` duplicate anomalies are reported in
  `diagnostic_duplicate_patterns.csv`.
"""
    (reports_dir / "build_summary.md").write_text(report, encoding="utf-8")


def write_gpkg_metadata(gpkg_path: Path, table_name: str, metadata: dict[str, Any], summary: dict[str, Any]) -> None:
    with sqlite3.connect(gpkg_path) as connection:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {quote_identifier(table_name)} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        rows = [
            ("metadata", json_dumps_text(metadata)),
            ("summary", json_dumps_text(summary)),
        ]
        connection.executemany(
            f"""
            INSERT INTO {quote_identifier(table_name)} (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            rows,
        )
        connection.commit()


def build_admin_asset(args: argparse.Namespace) -> dict[str, Any]:
    ensure_dependencies()
    started = time.perf_counter()
    admin_input_dir = repo_path(args.admin_input_dir)
    admin_gpkg = repo_path(args.admin_gpkg)
    reports_dir = repo_path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    diagnostic_columns = [
        column.strip()
        for column in str(args.diagnostic_columns or "").split(",")
        if column.strip()
    ]

    files = list_geojson_files(
        admin_input_dir,
        state=args.state,
        district=args.district,
        limit_files=args.limit_files,
    )
    if not files:
        raise SystemExit(f"No district GeoJSON files found under {admin_input_dir}")

    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    if args.work_db:
        work_db = repo_path(args.work_db)
    elif args.keep_work_db:
        work_db = reports_dir / "admin_sanitise_work.sqlite3"
    else:
        temp_dir = tempfile.TemporaryDirectory(prefix="cs_admin_sanitise_")
        work_db = Path(temp_dir.name) / "admin_sanitise_work.sqlite3"

    print(f"[admin] scanning {len(files):,} admin GeoJSON files", flush=True)
    connection = open_work_connection(work_db, overwrite=True)
    try:
        create_work_schema(connection, diagnostic_columns)
        audits: list[SourceFileAudit] = []
        for index, path in enumerate(files, start=1):
            audit = read_source_file(
                connection,
                path,
                diagnostic_columns=diagnostic_columns,
                reader=args.reader,
                orjson_max_mb=args.orjson_max_mb,
            )
            audits.append(audit)
            if index == 1 or index % 25 == 0 or index == len(files):
                print(
                    f"[admin] {index:,}/{len(files):,} files scanned; "
                    f"latest={path.parent.name}/{path.name}; rows={audit.rows_read:,}; status={audit.read_status}",
                    flush=True,
                )

        file_audit_path = reports_dir / "source_file_read_audit.csv"
        write_file_audit(file_audit_path, audits)
        source_summary = {
            "source_file_count": len(files),
            "source_rows": sum(audit.rows_read for audit in audits),
            "valid_village_id_rows": sum(audit.valid_village_id_rows for audit in audits),
            "invalid_village_id_rows": sum(audit.invalid_village_id_rows for audit in audits),
            "read_error_files": sum(1 for audit in audits if audit.read_status != "ok"),
        }

        print("[admin] resolving duplicate/non-overlapping village geometries", flush=True)
        feature_summary = build_features(connection)
        counts = fetch_work_counts(connection)
        duplicate_report_summary = write_duplicate_reports(
            connection,
            reports_dir,
            diagnostic_columns=diagnostic_columns,
        )

        print(f"[admin] writing sanitised GeoPackage {admin_gpkg}", flush=True)
        rows_written = write_admin_gpkg(
            connection,
            admin_gpkg,
            layer=args.admin_layer,
            overwrite=args.overwrite,
            chunk_size=args.chunk_size,
        )

        summary = {
            **source_summary,
            **counts,
            **feature_summary,
            **duplicate_report_summary,
            "gpkg_rows_written": rows_written,
            "total_seconds": round(time.perf_counter() - started, 6),
        }
        metadata = {
            "admin_input_dir": admin_input_dir.as_posix(),
            "admin_gpkg": admin_gpkg.as_posix(),
            "admin_layer": args.admin_layer,
            "reports_dir": reports_dir.as_posix(),
            "file_audit_path": file_audit_path.as_posix(),
            "duplicate_groups_path": (reports_dir / "duplicate_resolution_groups.csv").as_posix(),
            "diagnostic_patterns_path": (reports_dir / "diagnostic_duplicate_patterns.csv").as_posix(),
            "overlap_pairs_path": (reports_dir / "multi_geometry_overlap_pairs.csv").as_posix(),
            "admin_columns": ADMIN_COLUMNS,
            "admin_key_columns": ADMIN_KEY_COLUMNS,
            "diagnostic_columns": diagnostic_columns,
            "reader": args.reader,
            "orjson_max_mb": args.orjson_max_mb,
            "state_filter": args.state,
            "district_filter": args.district,
            "limit_files": args.limit_files,
            "work_db": work_db.as_posix() if args.keep_work_db or args.work_db else None,
        }
        write_admin_summary(reports_dir, metadata=metadata, summary=summary)
        write_gpkg_metadata(admin_gpkg, "cs_admin_asset_metadata", metadata, summary)
    finally:
        connection.close()
        if temp_dir is not None:
            temp_dir.cleanup()
        elif not args.keep_work_db and not args.work_db and work_db.exists():
            work_db.unlink()

    print(
        f"[admin] complete: {summary['feature_rows']:,} features from "
        f"{summary['source_rows']:,} source rows in {summary['total_seconds']:.1f}s",
        flush=True,
    )
    return {"metadata": metadata, "summary": summary}


def load_json_records(path: Path) -> list[dict[str, Any]]:
    payload = json_loads_bytes(path.read_bytes())
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        if payload.get("type") == "FeatureCollection" and isinstance(payload.get("features"), list):
            records = []
            for feature in payload["features"]:
                if not isinstance(feature, dict):
                    continue
                properties = feature.get("properties") or {}
                if isinstance(properties, dict):
                    records.append(properties)
            return records
        for key in ["records", "rows", "data", "features"]:
            value = payload.get(key)
            if isinstance(value, list):
                if key == "features":
                    return [
                        feature.get("properties", feature)
                        for feature in value
                        if isinstance(feature, dict)
                    ]
                return [row for row in value if isinstance(row, dict)]
        return [payload]
    raise ValueError(f"Unsupported JSON structure in {path}")


def read_property_header(path: Path) -> Any:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, nrows=0)
    return read_property_dataframe(path)


def read_property_dataframe(path: Path, *, usecols: Sequence[str] | None = None) -> Any:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, usecols=list(usecols) if usecols else None, low_memory=False)
    if suffix in {".json", ".geojson"}:
        dataframe = pd.DataFrame(load_json_records(path))
        return dataframe[list(usecols)] if usecols else dataframe
    if suffix in {".jsonl", ".ndjson"}:
        rows = []
        with path.open("rb") as handle:
            for line in handle:
                if line.strip():
                    rows.append(json_loads_bytes(line))
        dataframe = pd.DataFrame(rows)
        return dataframe[list(usecols)] if usecols else dataframe
    raise ValueError(f"Unsupported join input format: {path}. Expected CSV, JSON, GeoJSON, or JSONL.")


def parse_csv_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_rename_map(value: str | None) -> dict[str, str]:
    if not value:
        return {}
    rename_map: dict[str, str] = {}
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" in item:
            source, target = item.split("=", 1)
        elif ":" in item:
            source, target = item.split(":", 1)
        else:
            raise SystemExit(
                "Invalid rename mapping. Use `old=new` pairs separated by commas."
            )
        source = source.strip()
        target = target.strip()
        if not source or not target:
            raise SystemExit(
                "Invalid rename mapping. Use non-empty `old=new` pairs separated by commas."
            )
        rename_map[source] = target
    return rename_map


def parse_output_formats(value: str | None) -> list[str]:
    formats = [item.strip().lower() for item in (value or DEFAULT_OUTPUT_FORMATS).split(",") if item.strip()]
    if not formats:
        raise SystemExit("At least one output format is required.")
    unknown = [item for item in formats if item not in SUPPORTED_OUTPUT_FORMATS]
    if unknown:
        raise SystemExit(
            "Unsupported output format(s): "
            + ", ".join(unknown)
            + ". Use one or more of: "
            + ", ".join(sorted(SUPPORTED_OUTPUT_FORMATS))
        )
    deduped: list[str] = []
    for item in formats:
        if item not in deduped:
            deduped.append(item)
    return deduped


def default_geojson_path(output_path: Path) -> Path:
    return output_path.with_suffix(".geojson")


class ChunkedGeoJSONWriter:
    """Stream GeoDataFrame chunks into a single GeoJSON FeatureCollection."""

    def __init__(self, path: Path):
        self.path = path
        self.handle = None
        self.first_feature = True
        self.rows_written = 0

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("w", encoding="utf-8")
        self.handle.write('{"type":"FeatureCollection","features":[\n')
        return self

    def write(self, gdf: Any) -> int:
        if gdf.empty:
            return 0
        written = 0
        for feature in gdf.iterfeatures(na="null", drop_id=True):
            if not self.first_feature:
                self.handle.write(",\n")
            self.handle.write(json_dumps_text(feature))
            self.first_feature = False
            written += 1
        self.rows_written += written
        return written

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.handle is not None:
            self.handle.write("\n]}\n")
            self.handle.close()


def project_and_rename_output(
    gdf: Any,
    *,
    keep_columns: Sequence[str],
    rename_map: dict[str, str],
) -> Any:
    geometry_column = gdf.geometry.name
    if keep_columns:
        missing = [column for column in keep_columns if column not in gdf.columns]
        if missing:
            raise SystemExit(
                "Requested output column(s) not found after join: "
                + ", ".join(missing)
            )
        ordered_columns = [column for column in keep_columns if column != geometry_column]
        ordered_columns.append(geometry_column)
        gdf = gdf.loc[:, ordered_columns]

    extra_renames = [column for column in rename_map if column not in gdf.columns]
    if extra_renames:
        raise SystemExit(
            "Requested rename source column(s) not found after projection: "
            + ", ".join(extra_renames)
        )
    if rename_map:
        gdf = gdf.rename(columns=rename_map)
        if geometry_column in rename_map:
            gdf = gdf.set_geometry(rename_map[geometry_column])
    return gdf


def resolve_join_columns(
    dataframe: Any,
    *,
    right_key: str,
    columns: Sequence[str],
    patterns: Sequence[str],
    exclude_columns: Sequence[str],
) -> list[str]:
    available = [str(column) for column in dataframe.columns]
    exclude = {right_key, *exclude_columns}
    selected: list[str] = []

    if columns:
        for column in columns:
            if column not in available:
                raise SystemExit(f"Join column not found in source file: {column}")
            if column not in selected and column not in exclude:
                selected.append(column)

    for pattern in patterns:
        matches = [
            column for column in available
            if fnmatch.fnmatchcase(column, pattern) and column not in exclude
        ]
        for column in matches:
            if column not in selected:
                selected.append(column)

    if not selected and not columns and not patterns:
        selected = [column for column in available if column not in exclude]

    if not selected:
        raise SystemExit("No source columns selected for join.")
    return selected


def prepare_output_column_names(
    selected_columns: Sequence[str],
    *,
    admin_columns: Sequence[str],
    prefix: str,
) -> tuple[list[str], dict[str, str]]:
    admin_column_set = set(admin_columns)
    output_columns = []
    output_to_source: dict[str, str] = {}
    used = set(admin_column_set)
    for source_column in selected_columns:
        output_column = f"{prefix}{source_column}" if prefix else source_column
        if output_column in used:
            output_column = f"joined_{output_column}"
        original = output_column
        counter = 2
        while output_column in used:
            output_column = f"{original}_{counter}"
            counter += 1
        used.add(output_column)
        output_columns.append(output_column)
        output_to_source[output_column] = source_column
    return output_columns, output_to_source


def infer_numeric_columns(dataframe: Any, selected_columns: Sequence[str]) -> tuple[set[str], set[str]]:
    numeric_columns: set[str] = set()
    integer_columns: set[str] = set()
    for column in selected_columns:
        series = dataframe[column]
        numeric = pd.to_numeric(series, errors="coerce")
        non_null = series.notna() & (series.astype(str).str.strip() != "")
        if non_null.any() and numeric[non_null].notna().all():
            numeric_columns.add(column)
            values = numeric[non_null].dropna()
            if not values.empty and (values % 1 == 0).all():
                integer_columns.add(column)
    return numeric_columns, integer_columns


def build_join_table(
    source_path: Path,
    *,
    right_key: str,
    selected_columns: Sequence[str],
    patterns: Sequence[str],
    exclude_columns: Sequence[str],
    output_prefix: str,
    admin_columns: Sequence[str],
) -> JoinTable:
    header = read_property_header(source_path)
    if right_key not in header.columns:
        raise SystemExit(f"Join key `{right_key}` not found in {source_path}")

    selected = resolve_join_columns(
        header,
        right_key=right_key,
        columns=selected_columns,
        patterns=patterns,
        exclude_columns=exclude_columns,
    )
    dataframe = read_property_dataframe(source_path, usecols=[right_key, *selected])
    output_columns, output_to_source = prepare_output_column_names(
        selected,
        admin_columns=admin_columns,
        prefix=output_prefix,
    )
    numeric_columns, integer_columns = infer_numeric_columns(dataframe, selected)

    dataframe = dataframe[[right_key, *selected]].copy()
    dataframe["__join_key"] = dataframe[right_key].map(normalize_join_id)
    source_rows = int(len(dataframe))
    dataframe = dataframe[dataframe["__join_key"].notna()].copy()
    source_rows_with_key = int(len(dataframe))

    duplicate_key_rows = int(dataframe.duplicated("__join_key", keep=False).sum())
    conflicting_duplicate_key_rows = 0
    records_by_key: dict[str, dict[str, Any]] = {}
    source_for_output = {source: output for output, source in output_to_source.items()}
    for join_key, rows in dataframe.groupby("__join_key", sort=False):
        first = rows.iloc[0]
        record = {}
        for source_column in selected:
            output_column = source_for_output[source_column]
            value = first[source_column]
            record[output_column] = None if pd.isna(value) else value
        if len(rows) > 1:
            comparable = rows[selected].fillna("").astype(str)
            if len(comparable.drop_duplicates()) > 1:
                conflicting_duplicate_key_rows += len(rows) - 1
        records_by_key[str(join_key)] = record

    return JoinTable(
        records_by_key=records_by_key,
        selected_columns=list(selected),
        output_columns=output_columns,
        output_to_source_column=output_to_source,
        numeric_columns={source_for_output[column] for column in numeric_columns},
        integer_columns={source_for_output[column] for column in integer_columns},
        source_rows=source_rows,
        source_rows_with_key=source_rows_with_key,
        duplicate_key_rows=duplicate_key_rows,
        conflicting_duplicate_key_rows=conflicting_duplicate_key_rows,
    )


def coerce_join_output_columns(gdf: Any, join_table: JoinTable) -> Any:
    for column in JOIN_OUTPUT_INTEGER_COLUMNS:
        if column in gdf.columns:
            gdf[column] = pd.to_numeric(gdf[column], errors="coerce").astype("Int64")
    for column in join_table.output_columns:
        if column in join_table.numeric_columns:
            numeric = pd.to_numeric(gdf[column], errors="coerce")
            if column in join_table.integer_columns:
                gdf[column] = numeric.astype("Int64")
            else:
                gdf[column] = numeric.astype("Float64")
    return gdf


def write_join_summary(reports_dir: Path, summary: dict[str, Any]) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "join_summary.json").write_text(
        json_dumps_text(summary, indent=True),
        encoding="utf-8",
    )


def validate_output_path(path: Path, *, overwrite: bool, label: str) -> None:
    if path.exists():
        if not overwrite:
            raise SystemExit(f"{label} {path} exists. Pass --overwrite to rebuild it.")
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)


def join_properties_to_admin(args: argparse.Namespace) -> dict[str, Any]:
    ensure_dependencies()
    started = time.perf_counter()
    admin_gpkg = repo_path(args.admin_gpkg)
    source_path = repo_path(args.input)
    output_gpkg = repo_path(args.output)
    output_formats = parse_output_formats(args.output_formats)
    output_geojson = repo_path(args.geojson_output) if args.geojson_output else default_geojson_path(output_gpkg)
    reports_dir = repo_path(args.reports_dir)
    if not admin_gpkg.exists():
        raise SystemExit(f"Admin cache not found: {admin_gpkg}. Run `build-admin` first.")
    if not source_path.exists():
        raise SystemExit(f"Join input not found: {source_path}")
    if "gpkg" in output_formats:
        validate_output_path(output_gpkg, overwrite=args.overwrite, label="GeoPackage")
    if "geojson" in output_formats:
        validate_output_path(output_geojson, overwrite=args.overwrite, label="GeoJSON")

    info = pyogrio.read_info(admin_gpkg, layer=args.admin_layer)
    admin_columns = [str(field) for field in info.get("fields", [])]
    if args.left_key not in admin_columns:
        raise SystemExit(f"Left join key `{args.left_key}` not found in {admin_gpkg}:{args.admin_layer}")
    keep_output_columns = parse_csv_list(args.keep_output_columns)
    rename_output_columns = parse_rename_map(args.rename_output_columns)

    print(f"[join] loading property table {source_path}", flush=True)
    join_table = build_join_table(
        source_path,
        right_key=args.right_key,
        selected_columns=parse_csv_list(args.columns),
        patterns=args.pattern,
        exclude_columns=parse_csv_list(args.exclude_columns),
        output_prefix=args.prefix,
        admin_columns=admin_columns,
    )

    total_features = int(info.get("features") or 0)
    offset = 0
    rows_written = 0
    matched_rows = 0
    first_chunk = True
    last_output_fields: list[str] = []
    print(
        f"[join] joining {len(join_table.records_by_key):,} source keys onto "
        f"{total_features:,} admin features",
        flush=True,
    )

    geojson_writer = ChunkedGeoJSONWriter(output_geojson) if "geojson" in output_formats else None
    with geojson_writer if geojson_writer is not None else nullcontext(None) as writer:
        while True:
            gdf = pyogrio.read_dataframe(
                admin_gpkg,
                layer=args.admin_layer,
                skip_features=offset,
                max_features=args.chunk_size,
            )
            if gdf.empty:
                break
            normalised_keys = gdf[args.left_key].map(normalize_join_id)
            matched = normalised_keys.map(lambda key: key in join_table.records_by_key if key is not None else False)
            matched_rows += int(matched.sum())

            for output_column in join_table.output_columns:
                gdf[output_column] = [
                    join_table.records_by_key.get(key, {}).get(output_column)
                    if key is not None
                    else None
                    for key in normalised_keys
                ]
            if not args.no_match_status:
                gdf[args.match_status_column] = matched.astype("boolean")
            gdf = coerce_join_output_columns(gdf, join_table)
            gdf = project_and_rename_output(
                gdf,
                keep_columns=keep_output_columns,
                rename_map=rename_output_columns,
            )
            last_output_fields = [column for column in gdf.columns if column != gdf.geometry.name]

            if "gpkg" in output_formats:
                pyogrio.write_dataframe(
                    gdf,
                    output_gpkg,
                    layer=args.output_layer,
                    driver="GPKG",
                    append=not first_chunk,
                    promote_to_multi=True,
                )
            if "geojson" in output_formats:
                writer.write(gdf)
            rows_written += len(gdf)
            offset += len(gdf)
            first_chunk = False
            if rows_written == len(gdf) or rows_written % (args.chunk_size * 2) == 0:
                print(f"[join] wrote {rows_written:,}/{total_features:,} joined features", flush=True)

    final_left_key = rename_output_columns.get(args.left_key, args.left_key)
    final_location_columns = [
        rename_output_columns.get(column, column)
        for column in ["state_name", "district_name", "TEHSIL"]
    ]
    index_columns = []
    final_fields: list[str] = []
    if "gpkg" in output_formats:
        final_fields = list(pyogrio.read_info(output_gpkg, layer=args.output_layer).get("fields", []))
        final_fields_set = set(final_fields)
        if final_left_key in final_fields_set:
            index_columns.append([final_left_key])
        if all(column in final_fields_set for column in final_location_columns):
            index_columns.append(final_location_columns)
        ensure_gpkg_indexes(output_gpkg, args.output_layer, index_columns)
    else:
        final_fields = last_output_fields
    summary = {
        "admin_gpkg": admin_gpkg.as_posix(),
        "admin_layer": args.admin_layer,
        "input": source_path.as_posix(),
        "output": output_gpkg.as_posix() if "gpkg" in output_formats else None,
        "geojson_output": output_geojson.as_posix() if "geojson" in output_formats else None,
        "output_formats": output_formats,
        "output_layer": args.output_layer,
        "left_key": args.left_key,
        "right_key": args.right_key,
        "selected_columns": join_table.selected_columns,
        "output_columns": join_table.output_columns,
        "keep_output_columns": keep_output_columns,
        "rename_output_columns": rename_output_columns,
        "final_output_columns": final_fields,
        "source_rows": join_table.source_rows,
        "source_rows_with_key": join_table.source_rows_with_key,
        "source_unique_keys": len(join_table.records_by_key),
        "duplicate_key_rows": join_table.duplicate_key_rows,
        "conflicting_duplicate_key_rows": join_table.conflicting_duplicate_key_rows,
        "admin_rows": rows_written,
        "matched_admin_rows": matched_rows,
        "unmatched_admin_rows": rows_written - matched_rows,
        "match_rate": round(matched_rows / rows_written, 6) if rows_written else 0,
        "total_seconds": round(time.perf_counter() - started, 6),
    }
    write_join_summary(reports_dir, summary)
    if "gpkg" in output_formats:
        write_gpkg_metadata(output_gpkg, f"{normalize_slug(args.output_layer)}_join_metadata", {}, summary)
    print(
        f"[join] complete: {matched_rows:,}/{rows_written:,} admin features matched "
        f"in {summary['total_seconds']:.1f}s",
        flush=True,
    )
    return summary


def export_gpkg_layer_to_geojson(args: argparse.Namespace) -> dict[str, Any]:
    ensure_dependencies()
    started = time.perf_counter()
    input_gpkg = repo_path(args.input_gpkg)
    output_geojson = repo_path(args.geojson_output)
    reports_dir = repo_path(args.reports_dir)
    if not input_gpkg.exists():
        raise SystemExit(f"Input GeoPackage not found: {input_gpkg}")

    keep_output_columns = parse_csv_list(args.keep_output_columns)
    rename_output_columns = parse_rename_map(args.rename_output_columns)
    validate_output_path(output_geojson, overwrite=args.overwrite, label="GeoJSON")

    info = pyogrio.read_info(input_gpkg, layer=args.input_layer)
    total_features = int(info.get("features") or 0)
    rows_written = 0
    offset = 0
    final_fields: list[str] = []
    print(
        f"[export] writing {total_features:,} features from "
        f"{input_gpkg}:{args.input_layer} to {output_geojson}",
        flush=True,
    )
    with ChunkedGeoJSONWriter(output_geojson) as writer:
        while True:
            gdf = pyogrio.read_dataframe(
                input_gpkg,
                layer=args.input_layer,
                skip_features=offset,
                max_features=args.chunk_size,
            )
            if gdf.empty:
                break
            gdf = project_and_rename_output(
                gdf,
                keep_columns=keep_output_columns,
                rename_map=rename_output_columns,
            )
            final_fields = [column for column in gdf.columns if column != gdf.geometry.name]
            writer.write(gdf)
            rows_written += len(gdf)
            offset += len(gdf)
            if rows_written == len(gdf) or rows_written % (args.chunk_size * 2) == 0:
                print(f"[export] wrote {rows_written:,}/{total_features:,} features", flush=True)

    summary = {
        "input_gpkg": input_gpkg.as_posix(),
        "input_layer": args.input_layer,
        "geojson_output": output_geojson.as_posix(),
        "rows_written": rows_written,
        "final_output_columns": final_fields,
        "keep_output_columns": keep_output_columns,
        "rename_output_columns": rename_output_columns,
        "total_seconds": round(time.perf_counter() - started, 6),
    }
    write_join_summary(reports_dir, summary)
    print(
        f"[export] complete: wrote {rows_written:,} features in "
        f"{summary['total_seconds']:.1f}s",
        flush=True,
    )
    return summary


def run_livestock(args: argparse.Namespace) -> dict[str, Any]:
    admin_gpkg = repo_path(args.admin_gpkg)
    if args.rebuild_admin or not admin_gpkg.exists():
        if args.rebuild_admin:
            print("[livestock] rebuilding sanitised admin cache", flush=True)
        else:
            print("[livestock] sanitised admin cache missing; building it first", flush=True)
        admin_args = argparse.Namespace(
            admin_input_dir=args.admin_input_dir,
            admin_gpkg=args.admin_gpkg,
            admin_layer=args.admin_layer,
            reports_dir=args.admin_reports_dir,
            state=args.state,
            district=args.district,
            limit_files=args.limit_files,
            diagnostic_columns=args.diagnostic_columns,
            reader=args.reader,
            orjson_max_mb=args.orjson_max_mb,
            overwrite=args.overwrite or not admin_gpkg.exists(),
            chunk_size=args.admin_chunk_size,
            work_db=args.work_db,
            keep_work_db=args.keep_work_db,
        )
        build_admin_asset(admin_args)
    else:
        print(f"[livestock] using cached admin asset {admin_gpkg}", flush=True)

    join_args = argparse.Namespace(
        admin_gpkg=args.admin_gpkg,
        admin_layer=args.admin_layer,
        input=args.input,
        output=args.output,
        output_layer=args.output_layer,
        left_key=args.left_key,
        right_key=args.right_key,
        columns=args.columns or ",".join(LIVESTOCK_COLUMNS),
        pattern=[],
        exclude_columns="",
        prefix=args.prefix,
        overwrite=args.overwrite,
        reports_dir=args.reports_dir,
        chunk_size=args.join_chunk_size,
        no_match_status=args.no_match_status,
        match_status_column=args.match_status_column,
        keep_output_columns=args.keep_output_columns,
        rename_output_columns=args.rename_output_columns,
        output_formats=args.output_formats,
        geojson_output=args.geojson_output,
    )
    return join_properties_to_admin(join_args)


def add_admin_build_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--admin-input-dir", type=Path, default=DEFAULT_ADMIN_INPUT_DIR)
    parser.add_argument("--admin-gpkg", type=Path, default=DEFAULT_ADMIN_GPKG)
    parser.add_argument("--admin-layer", default=DEFAULT_ADMIN_LAYER)
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_ADMIN_REPORTS_DIR)
    parser.add_argument("--state", help="Optional state slug/name filter, e.g. andhra_pradesh")
    parser.add_argument("--district", help="Optional district slug/name filter, e.g. guntur")
    parser.add_argument("--limit-files", type=int, help="Optional smoke-test file limit")
    parser.add_argument("--diagnostic-columns", default=",".join(DEFAULT_DIAGNOSTIC_COLUMNS))
    parser.add_argument(
        "--reader",
        choices=["auto", "orjson", "ijson", "pyogrio"],
        default="auto",
        help="GeoJSON source reader. auto uses orjson for moderate files, then ijson, then pyogrio.",
    )
    parser.add_argument("--orjson-max-mb", type=int, default=DEFAULT_ORJSON_MAX_MB)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_GPKG_CHUNK_SIZE)
    parser.add_argument("--work-db", type=Path, help="Optional work SQLite path for the sanitisation pass")
    parser.add_argument("--keep-work-db", action="store_true", help="Keep the temporary sanitisation work database")


def add_join_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--admin-gpkg", type=Path, default=DEFAULT_ADMIN_GPKG)
    parser.add_argument("--admin-layer", default=DEFAULT_ADMIN_LAYER)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--output-layer", required=True)
    parser.add_argument(
        "--output-formats",
        default=DEFAULT_OUTPUT_FORMATS,
        help="Comma-separated output formats: gpkg, geojson, or gpkg,geojson.",
    )
    parser.add_argument(
        "--geojson-output",
        type=Path,
        help="Optional GeoJSON output path. Defaults to --output with .geojson suffix.",
    )
    parser.add_argument("--left-key", default="pc11_village_id")
    parser.add_argument("--right-key", default="village_code")
    parser.add_argument("--columns", help="Comma-separated columns to copy from the input table")
    parser.add_argument(
        "--pattern",
        action="append",
        default=[],
        help="Glob pattern for columns to copy; can be repeated, e.g. --pattern 'cattle_*'",
    )
    parser.add_argument("--exclude-columns", help="Comma-separated source columns to exclude")
    parser.add_argument("--prefix", default="", help="Prefix for joined output columns")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--reports-dir", type=Path, default=Path("data/admin-boundary/join_reports"))
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_GPKG_CHUNK_SIZE)
    parser.add_argument("--no-match-status", action="store_true")
    parser.add_argument("--match-status-column", default="property_join_matched")
    parser.add_argument(
        "--keep-output-columns",
        help="Comma-separated final attribute columns to keep. Geometry is always retained.",
    )
    parser.add_argument(
        "--rename-output-columns",
        help="Comma-separated `old=new` final output column renames applied after projection.",
    )


def add_livestock_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--admin-input-dir", type=Path, default=DEFAULT_ADMIN_INPUT_DIR)
    parser.add_argument("--admin-gpkg", type=Path, default=DEFAULT_ADMIN_GPKG)
    parser.add_argument("--admin-layer", default=DEFAULT_ADMIN_LAYER)
    parser.add_argument("--admin-reports-dir", type=Path, default=DEFAULT_ADMIN_REPORTS_DIR)
    parser.add_argument("--input", type=Path, default=DEFAULT_LIVESTOCK_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_LIVESTOCK_OUTPUT)
    parser.add_argument("--output-layer", default=DEFAULT_LIVESTOCK_LAYER)
    parser.add_argument(
        "--output-formats",
        default=DEFAULT_OUTPUT_FORMATS,
        help="Comma-separated output formats: gpkg, geojson, or gpkg,geojson.",
    )
    parser.add_argument(
        "--geojson-output",
        type=Path,
        help="Optional GeoJSON output path. Defaults to --output with .geojson suffix.",
    )
    parser.add_argument("--left-key", default="pc11_village_id")
    parser.add_argument("--right-key", default="village_code")
    parser.add_argument("--columns", help="Comma-separated livestock columns to join")
    parser.add_argument("--prefix", default="", help="Prefix for livestock columns in the output layer")
    parser.add_argument("--reports-dir", type=Path, default=Path("data/livestock/livestock_gpkg_reports"))
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--rebuild-admin", action="store_true")
    parser.add_argument("--state", help="Optional admin build state filter for smoke tests")
    parser.add_argument("--district", help="Optional admin build district filter for smoke tests")
    parser.add_argument("--limit-files", type=int, help="Optional admin build file limit for smoke tests")
    parser.add_argument("--diagnostic-columns", default=",".join(DEFAULT_DIAGNOSTIC_COLUMNS))
    parser.add_argument(
        "--reader",
        choices=["auto", "orjson", "ijson", "pyogrio"],
        default="auto",
        help="Admin GeoJSON source reader. auto uses orjson for moderate files.",
    )
    parser.add_argument("--orjson-max-mb", type=int, default=DEFAULT_ORJSON_MAX_MB)
    parser.add_argument("--admin-chunk-size", type=int, default=DEFAULT_GPKG_CHUNK_SIZE)
    parser.add_argument("--join-chunk-size", type=int, default=DEFAULT_GPKG_CHUNK_SIZE)
    parser.add_argument("--work-db", type=Path, help="Optional work SQLite path for the admin sanitisation pass")
    parser.add_argument("--keep-work-db", action="store_true", help="Keep the temporary admin sanitisation work database")
    parser.add_argument("--no-match-status", action="store_true")
    parser.add_argument("--match-status-column", default="livestock_join_matched")
    parser.add_argument(
        "--keep-output-columns",
        help="Comma-separated final livestock output columns to keep. Geometry is always retained.",
    )
    parser.add_argument(
        "--rename-output-columns",
        help="Comma-separated `old=new` final livestock output column renames applied after projection.",
    )


def add_export_layer_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--input-gpkg", type=Path, required=True)
    parser.add_argument("--input-layer", required=True)
    parser.add_argument("--geojson-output", type=Path, required=True)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_GPKG_CHUNK_SIZE)
    parser.add_argument("--reports-dir", type=Path, default=Path("data/admin-boundary/export_reports"))
    parser.add_argument(
        "--keep-output-columns",
        help="Comma-separated final attribute columns to keep. Geometry is always retained.",
    )
    parser.add_argument(
        "--rename-output-columns",
        help="Comma-separated `old=new` final output column renames applied after projection.",
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    if not raw_argv:
        raw_argv = ["livestock"]
    elif raw_argv[0].startswith("-") and raw_argv[0] not in {"-h", "--help"}:
        raw_argv = ["livestock", *raw_argv]

    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    admin_parser = subparsers.add_parser("build-admin", help="Build the cached sanitised admin GPKG")
    add_admin_build_args(admin_parser)

    join_parser = subparsers.add_parser("join", help="Join CSV/JSON properties onto the cached admin GPKG")
    add_join_args(join_parser)

    livestock_parser = subparsers.add_parser("livestock", help="Build the livestock GPKG from default inputs")
    add_livestock_args(livestock_parser)

    export_parser = subparsers.add_parser("export-layer", help="Export a GPKG layer to GeoJSON")
    add_export_layer_args(export_parser)

    return parser.parse_args(raw_argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "build-admin":
        build_admin_asset(args)
    elif args.command == "join":
        join_properties_to_admin(args)
    elif args.command == "livestock":
        run_livestock(args)
    elif args.command == "export-layer":
        export_gpkg_layer_to_geojson(args)
    else:  # pragma: no cover - argparse prevents this.
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
