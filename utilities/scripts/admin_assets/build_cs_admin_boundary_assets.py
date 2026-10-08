#!/usr/bin/env python3
"""Build village-level data assets from the Core Stack standard admin layer.

This script is the asset-join companion to
``build_cs_admin_boundary_standard.py``. It reads the standardized admin
GeoPackage, joins configured village property tables, and writes reproducible
GeoPackage/GeoJSON siblings for downstream use.

The script intentionally does not import or call ``build_admin_boundary_assets``.
The older script remains useful as implementation history, but this path is
anchored on:

    data/base_resources/cs_admin_standard.gpkg

Examples:

    uv run --with geopandas --with pyogrio --with pandas --with shapely \
      python utilities/scripts/admin_assets/build_cs_admin_boundary_assets.py \
      asset livestock --overwrite

    uv run --with geopandas --with pyogrio --with pandas --with shapely \
      python utilities/scripts/admin_assets/build_cs_admin_boundary_assets.py \
      asset antyodaya --overwrite
"""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import nullcontext
import csv
from dataclasses import dataclass
import fnmatch
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
from typing import Any, Iterable, Sequence

try:
    import geopandas as gpd
    import pandas as pd
    import pyogrio
except Exception as exc:  # pragma: no cover - handled in ensure_dependencies
    gpd = None
    pd = None
    pyogrio = None
    GEOSTACK_IMPORT_ERROR = exc
else:
    GEOSTACK_IMPORT_ERROR = None


ROOT_DIR = Path(__file__).resolve().parents[3]

DEFAULT_ADMIN_GPKG = Path("data/base_resources/cs_admin_standard.gpkg")
DEFAULT_ADMIN_LAYER = "cs_admin_standard"
DEFAULT_ASSET_CONFIG_DIR = Path(__file__).resolve().parent / "asset_configs"
ASSET_CONFIG_SCHEMA_VERSION = 1

SUPPORTED_OUTPUT_FORMATS = {"gpkg", "geojson"}
SUPPORTED_JOIN_STORAGE = {"auto", "memory", "sqlite"}
DEFAULT_OUTPUT_FORMATS = "gpkg,geojson"
DEFAULT_CHUNK_SIZE = 50_000
DEFAULT_SOURCE_CHUNK_SIZE = 50_000
DEFAULT_DISK_JOIN_THRESHOLD_MB = 512
SQLITE_LOOKUP_BATCH_SIZE = 500
JOINED_COLUMNS_TOKEN = "@joined"

STANDARD_IDENTITY_COLUMNS = [
    "cs_feature_id",
    "cs_admin_uid",
    "core_admin_uid",
    "state_name",
    "district_name",
    "TEHSIL",
    "pc11_village_id",
    "village_id",
    "NAME",
    "pc11_state_id",
    "pc11_district_id",
    "pc11_subdistrict_id",
    "feature_part_index",
    "feature_part_count",
]

INTEGER_OUTPUT_COLUMNS = {
    "feature_part_index",
    "feature_part_count",
    "pc11_village_id",
    "village_id",
    "pc11_state_id",
    "pc11_district_id",
    "pc11_subdistrict_id",
    "source_row_count",
    "source_identity_count",
    "source_geometry_count",
    "source_file_count",
}

ASSET_OUTPUT_OVERRIDES = {
    "livestock": {
        "gpkg": "data/livestock/cs_village_livestock_census_20.gpkg",
        "geojson": "data/livestock/cs_village_livestock_census_20.geojson",
        "layer": "cs_village_livestock_census_20",
        "reports_dir": "data/livestock/cs_village_livestock_census_20_reports",
    },
    "antyodaya": {
        "gpkg": "data/antyodaya/output/cs_antyodaya_2020.gpkg",
        "geojson": "data/antyodaya/output/cs_antyodaya_2020.geojson",
        "layer": "cs_antyodaya_2020",
        "reports_dir": "data/antyodaya/output/cs_antyodaya_2020_asset_reports",
    },
}

MISSING_TEXTS = {"", "nan", "none", "null", "<na>"}


@dataclass(slots=True)
class JoinTable:
    records_by_key: dict[str, tuple[Any, ...]] | None
    sqlite_connection: sqlite3.Connection | None
    sqlite_path: Path | None
    remove_sqlite_on_close: bool
    selected_columns: list[str]
    output_columns: list[str]
    output_to_source_column: dict[str, str]
    numeric_columns: set[str]
    integer_columns: set[str]
    source_rows: int
    source_rows_with_key: int
    duplicate_key_rows: int
    conflicting_duplicate_key_rows: int
    source_unique_keys: int
    storage: str

    def lookup(self, keys: Sequence[str | None]) -> dict[str, tuple[Any, ...]]:
        requested = ordered_distinct(key for key in keys if key is not None)
        if not requested:
            return {}
        if self.records_by_key is not None:
            return {
                key: self.records_by_key[key]
                for key in requested
                if key in self.records_by_key
            }
        if self.sqlite_connection is None:
            raise RuntimeError("Join table has no active storage backend.")

        selected_sql = ", ".join(quote_identifier(column) for column in self.output_columns)
        records: dict[str, tuple[Any, ...]] = {}
        for offset in range(0, len(requested), SQLITE_LOOKUP_BATCH_SIZE):
            batch = requested[offset:offset + SQLITE_LOOKUP_BATCH_SIZE]
            placeholders = ", ".join("?" for _ in batch)
            rows = self.sqlite_connection.execute(
                f"SELECT join_key, {selected_sql} FROM join_values "
                f"WHERE join_key IN ({placeholders})",
                batch,
            )
            for row in rows:
                records[str(row[0])] = tuple(row[1:])
        return records

    def iter_keys(self) -> Iterable[str]:
        if self.records_by_key is not None:
            yield from self.records_by_key.keys()
            return
        if self.sqlite_connection is None:
            return
        for row in self.sqlite_connection.execute("SELECT join_key FROM join_values"):
            yield str(row[0])

    def close(self, *, keep_sqlite: bool = False) -> None:
        if self.sqlite_connection is not None:
            self.sqlite_connection.close()
            self.sqlite_connection = None
        if (
            self.sqlite_path is not None
            and self.remove_sqlite_on_close
            and not keep_sqlite
            and self.sqlite_path.exists()
        ):
            self.sqlite_path.unlink()


def repo_path(path: Path | str) -> Path:
    path = Path(path)
    return path if path.is_absolute() else ROOT_DIR / path


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def json_dumps(payload: Any, *, indent: bool = False) -> str:
    return json.dumps(
        payload,
        indent=2 if indent else None,
        ensure_ascii=False,
        sort_keys=not indent,
        default=str,
    )


def ensure_dependencies() -> None:
    if gpd is None or pd is None or pyogrio is None:
        raise SystemExit(
            "Missing geospatial dependencies: "
            f"geopandas/pandas/pyogrio ({GEOSTACK_IMPORT_ERROR}). "
            "Run with `uv run --with geopandas --with pyogrio --with pandas --with shapely`."
        )


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    return None if text.lower() in MISSING_TEXTS else text


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
    text = clean_text(value)
    if text is None:
        return None
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return int(text) if text.isdigit() and int(text) != 0 else None


def normalize_join_id(value: Any) -> str | None:
    number = clean_int(value)
    if number is not None:
        return str(number)
    return clean_text(value)


def ordered_distinct(values: Iterable[str]) -> list[str]:
    seen = set()
    output = []
    for value in values:
        if value not in seen:
            seen.add(value)
            output.append(value)
    return output


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
            raise SystemExit("Invalid rename mapping. Use `old=new` pairs.")
        source = source.strip()
        target = target.strip()
        if not source or not target:
            raise SystemExit("Invalid rename mapping. Use non-empty `old=new` pairs.")
        rename_map[source] = target
    return rename_map


def parse_output_formats(value: str | None) -> list[str]:
    formats = [
        item.strip().lower()
        for item in (value or DEFAULT_OUTPUT_FORMATS).split(",")
        if item.strip()
    ]
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
    return ordered_distinct(formats)


def default_geojson_path(output_gpkg: Path) -> Path:
    return output_gpkg.with_suffix(".geojson")


def config_list(value: Any, *, field: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise SystemExit(f"Asset config `{field}` must be a list of strings.")
    return list(value)


def config_mapping(value: Any, *, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SystemExit(f"Asset config `{field}` must be an object.")
    return value


def resolve_asset_config_path(value: str | Path) -> Path:
    requested = Path(value)
    candidates = [requested]
    if not requested.is_absolute():
        candidates.append(ROOT_DIR / requested)
        candidates.append(DEFAULT_ASSET_CONFIG_DIR / requested)
        if not requested.suffix:
            candidates.append(DEFAULT_ASSET_CONFIG_DIR / f"{requested}.json")
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise SystemExit(
        f"Asset config not found: {value}. "
        f"Use a JSON path or a name from {DEFAULT_ASSET_CONFIG_DIR}."
    )


def load_asset_config(path: Path) -> dict[str, Any]:
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Could not read asset config {path}: {exc}") from exc
    if not isinstance(config, dict):
        raise SystemExit(f"Asset config {path} must contain a JSON object.")
    if config.get("schema_version") != ASSET_CONFIG_SCHEMA_VERSION:
        raise SystemExit(
            f"Asset config {path} must use schema_version {ASSET_CONFIG_SCHEMA_VERSION}."
        )
    for section in ["admin", "source", "output"]:
        config_mapping(config.get(section), field=section)
    return config


def source_header_columns(path: Path) -> list[str]:
    if path.suffix.lower() != ".csv":
        raise SystemExit(f"Unsupported join input format: {path}. Expected CSV.")
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            return [str(column) for column in next(csv.reader(handle))]
    except UnicodeDecodeError:
        with path.open(newline="", encoding="cp1252") as handle:
            return [str(column) for column in next(csv.reader(handle))]


def gpkg_layer_columns(path: Path, layer: str) -> list[str]:
    with sqlite3.connect(path) as connection:
        exists = connection.execute(
            "SELECT 1 FROM gpkg_contents WHERE table_name = ?",
            (layer,),
        ).fetchone()
        if not exists:
            raise SystemExit(f"Layer `{layer}` not found in {path}.")
        return [
            str(row[1])
            for row in connection.execute(
                f"PRAGMA table_info({quote_identifier(layer)})"
            )
            if str(row[1]) not in {"fid", "geom"}
        ]


def resolve_join_columns(
    header_columns: Sequence[str],
    *,
    right_key: str,
    columns: Sequence[str],
    patterns: Sequence[str],
    exclude_columns: Sequence[str],
) -> list[str]:
    available = [str(column) for column in header_columns]
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
    used = set(admin_columns)
    output_columns: list[str] = []
    output_to_source: dict[str, str] = {}
    for source_column in selected_columns:
        output_column = f"{prefix}{source_column}" if prefix else source_column
        if output_column in used:
            output_column = f"joined_{output_column}"
        base = output_column
        counter = 2
        while output_column in used:
            output_column = f"{base}_{counter}"
            counter += 1
        used.add(output_column)
        output_columns.append(output_column)
        output_to_source[output_column] = source_column
    return output_columns, output_to_source


def update_numeric_candidates(
    dataframe: Any,
    selected_columns: Sequence[str],
    *,
    numeric_candidates: set[str],
    integer_candidates: set[str],
    observed_non_null: set[str],
) -> None:
    for column in selected_columns:
        if column not in numeric_candidates:
            continue
        series = dataframe[column]
        non_null = series.notna() & (series.astype(str).str.strip() != "")
        if not non_null.any():
            continue
        observed_non_null.add(column)
        numeric = pd.to_numeric(series[non_null], errors="coerce")
        if numeric.isna().any():
            numeric_candidates.discard(column)
            integer_candidates.discard(column)
            continue
        if column in integer_candidates and not (numeric % 1 == 0).all():
            integer_candidates.discard(column)


def choose_join_storage(source_path: Path, requested: str) -> str:
    storage = requested.strip().lower()
    if storage not in SUPPORTED_JOIN_STORAGE:
        raise SystemExit(
            f"Unsupported join storage `{requested}`. Use one of: "
            + ", ".join(sorted(SUPPORTED_JOIN_STORAGE))
        )
    if storage != "auto":
        return storage
    if source_path.stat().st_size >= DEFAULT_DISK_JOIN_THRESHOLD_MB * 1024 * 1024:
        return "sqlite"
    return "memory"


def read_source_chunks(path: Path, *, usecols: Sequence[str], chunk_size: int) -> Iterable[Any]:
    yield from pd.read_csv(
        path,
        usecols=list(usecols),
        chunksize=chunk_size,
        dtype=str,
        keep_default_na=False,
        low_memory=False,
    )


def cleaned_row_values(values: Sequence[Any]) -> tuple[str | None, ...]:
    return tuple(clean_text(value) for value in values)


def row_signature(values: Sequence[str | None]) -> bytes:
    payload = "\x1f".join("\x00" if value is None else value for value in values)
    return hashlib.sha256(payload.encode("utf-8")).digest()


def source_to_output_map(output_to_source: dict[str, str]) -> dict[str, str]:
    return {source: output for output, source in output_to_source.items()}


def build_memory_join_table(
    source_path: Path,
    *,
    right_key: str,
    selected: Sequence[str],
    output_columns: Sequence[str],
    output_to_source: dict[str, str],
    source_chunk_size: int,
) -> JoinTable:
    records: dict[str, tuple[Any, ...]] = {}
    signatures_by_key: dict[str, set[bytes]] = {}
    numeric_candidates = {column for column in selected if not column.endswith("_unit")}
    integer_candidates = set(numeric_candidates)
    observed_non_null: set[str] = set()
    source_rows = 0
    source_rows_with_key = 0
    duplicate_key_rows = 0

    for chunk in read_source_chunks(
        source_path,
        usecols=[right_key, *selected],
        chunk_size=source_chunk_size,
    ):
        chunk = chunk[[right_key, *selected]]
        source_rows += len(chunk)
        update_numeric_candidates(
            chunk,
            selected,
            numeric_candidates=numeric_candidates,
            integer_candidates=integer_candidates,
            observed_non_null=observed_non_null,
        )
        for row in chunk.itertuples(index=False, name=None):
            join_key = normalize_join_id(row[0])
            if join_key is None:
                continue
            source_rows_with_key += 1
            values = cleaned_row_values(row[1:])
            signature = row_signature(values)
            if join_key in records:
                duplicate_key_rows += 1
                signatures_by_key.setdefault(join_key, set()).add(signature)
                continue
            records[join_key] = values
            signatures_by_key[join_key] = {signature}
        if source_rows == len(chunk) or source_rows % (source_chunk_size * 10) == 0:
            print(f"[asset] indexed {source_rows:,} source rows in memory", flush=True)

    source_to_output = source_to_output_map(output_to_source)
    numeric_columns = numeric_candidates & observed_non_null
    integer_columns = integer_candidates & numeric_columns
    conflicting_duplicates = sum(
        len(signatures) - 1
        for signatures in signatures_by_key.values()
        if len(signatures) > 1
    )
    return JoinTable(
        records_by_key=records,
        sqlite_connection=None,
        sqlite_path=None,
        remove_sqlite_on_close=False,
        selected_columns=list(selected),
        output_columns=list(output_columns),
        output_to_source_column=dict(output_to_source),
        numeric_columns={source_to_output[column] for column in numeric_columns},
        integer_columns={source_to_output[column] for column in integer_columns},
        source_rows=source_rows,
        source_rows_with_key=source_rows_with_key,
        duplicate_key_rows=duplicate_key_rows,
        conflicting_duplicate_key_rows=conflicting_duplicates,
        source_unique_keys=len(records),
        storage="memory",
    )


def sqlite_join_work_path(configured_path: Path | None) -> tuple[Path, bool]:
    if configured_path is not None:
        path = repo_path(configured_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        return path, True
    handle = tempfile.NamedTemporaryFile(
        prefix="cs_admin_asset_join_",
        suffix=".sqlite3",
        delete=False,
    )
    path = Path(handle.name)
    handle.close()
    return path, True


def build_sqlite_join_table(
    source_path: Path,
    *,
    right_key: str,
    selected: Sequence[str],
    output_columns: Sequence[str],
    output_to_source: dict[str, str],
    source_chunk_size: int,
    work_db: Path | None,
) -> JoinTable:
    sqlite_path, remove_on_close = sqlite_join_work_path(work_db)
    connection = sqlite3.connect(sqlite_path)
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("PRAGMA temp_store=MEMORY")
    value_columns_sql = ", ".join(
        f"{quote_identifier(column)} TEXT"
        for column in output_columns
    )
    connection.execute(
        f"CREATE TABLE join_values (join_key TEXT PRIMARY KEY, {value_columns_sql})"
    )
    connection.execute(
        "CREATE TABLE join_key_audit (join_key TEXT NOT NULL, row_signature BLOB NOT NULL)"
    )

    insert_columns_sql = ", ".join(
        ["join_key", *(quote_identifier(column) for column in output_columns)]
    )
    placeholders = ", ".join("?" for _ in range(len(output_columns) + 1))
    insert_values_sql = (
        f"INSERT OR IGNORE INTO join_values ({insert_columns_sql}) "
        f"VALUES ({placeholders})"
    )
    insert_audit_sql = "INSERT INTO join_key_audit (join_key, row_signature) VALUES (?, ?)"

    numeric_candidates = {column for column in selected if not column.endswith("_unit")}
    integer_candidates = set(numeric_candidates)
    observed_non_null: set[str] = set()
    source_rows = 0
    source_rows_with_key = 0

    try:
        for chunk in read_source_chunks(
            source_path,
            usecols=[right_key, *selected],
            chunk_size=source_chunk_size,
        ):
            chunk = chunk[[right_key, *selected]]
            source_rows += len(chunk)
            update_numeric_candidates(
                chunk,
                selected,
                numeric_candidates=numeric_candidates,
                integer_candidates=integer_candidates,
                observed_non_null=observed_non_null,
            )
            value_rows = []
            audit_rows = []
            for row in chunk.itertuples(index=False, name=None):
                join_key = normalize_join_id(row[0])
                if join_key is None:
                    continue
                source_rows_with_key += 1
                values = cleaned_row_values(row[1:])
                value_rows.append((join_key, *values))
                audit_rows.append((join_key, row_signature(values)))
            connection.executemany(insert_values_sql, value_rows)
            connection.executemany(insert_audit_sql, audit_rows)
            connection.commit()
            if source_rows == len(chunk) or source_rows % (source_chunk_size * 10) == 0:
                print(
                    f"[asset] indexed {source_rows:,} source rows in {sqlite_path}",
                    flush=True,
                )

        connection.execute("CREATE INDEX idx_join_key_audit_key ON join_key_audit (join_key)")
        duplicate_stats = connection.execute(
            """
            SELECT
                COALESCE(SUM(row_count), 0),
                COALESCE(SUM(
                    CASE WHEN signature_count > 1 THEN row_count - 1 ELSE 0 END
                ), 0)
            FROM (
                SELECT
                    join_key,
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT row_signature) AS signature_count
                FROM join_key_audit
                GROUP BY join_key
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()
        source_unique_keys = int(
            connection.execute("SELECT COUNT(*) FROM join_values").fetchone()[0]
        )
        connection.execute("DROP TABLE join_key_audit")
        connection.commit()
    except Exception:
        connection.close()
        if remove_on_close and sqlite_path.exists():
            sqlite_path.unlink()
        raise

    source_to_output = source_to_output_map(output_to_source)
    numeric_columns = numeric_candidates & observed_non_null
    integer_columns = integer_candidates & numeric_columns
    return JoinTable(
        records_by_key=None,
        sqlite_connection=connection,
        sqlite_path=sqlite_path,
        remove_sqlite_on_close=remove_on_close,
        selected_columns=list(selected),
        output_columns=list(output_columns),
        output_to_source_column=dict(output_to_source),
        numeric_columns={source_to_output[column] for column in numeric_columns},
        integer_columns={source_to_output[column] for column in integer_columns},
        source_rows=source_rows,
        source_rows_with_key=source_rows_with_key,
        duplicate_key_rows=int(duplicate_stats[0]),
        conflicting_duplicate_key_rows=int(duplicate_stats[1]),
        source_unique_keys=source_unique_keys,
        storage="sqlite",
    )


def build_join_table(
    source_path: Path,
    *,
    right_key: str,
    selected_columns: Sequence[str],
    patterns: Sequence[str],
    exclude_columns: Sequence[str],
    output_prefix: str,
    admin_columns: Sequence[str],
    storage: str,
    source_chunk_size: int,
    work_db: Path | None,
) -> JoinTable:
    header = source_header_columns(source_path)
    if right_key not in header:
        raise SystemExit(f"Join key `{right_key}` not found in {source_path}")

    selected = resolve_join_columns(
        header,
        right_key=right_key,
        columns=selected_columns,
        patterns=patterns,
        exclude_columns=exclude_columns,
    )
    output_columns, output_to_source = prepare_output_column_names(
        selected,
        admin_columns=admin_columns,
        prefix=output_prefix,
    )
    resolved_storage = choose_join_storage(source_path, storage)
    if resolved_storage == "sqlite":
        return build_sqlite_join_table(
            source_path,
            right_key=right_key,
            selected=selected,
            output_columns=output_columns,
            output_to_source=output_to_source,
            source_chunk_size=source_chunk_size,
            work_db=work_db,
        )
    return build_memory_join_table(
        source_path,
        right_key=right_key,
        selected=selected,
        output_columns=output_columns,
        output_to_source=output_to_source,
        source_chunk_size=source_chunk_size,
    )


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
            self.handle.write(json_dumps(feature))
            self.first_feature = False
            written += 1
        self.rows_written += written
        return written

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.handle is not None:
            self.handle.write("\n]}\n")
            self.handle.close()


def as_geodataframe_like(frame: Any, template: Any) -> Any:
    return gpd.GeoDataFrame(
        frame,
        geometry=template.geometry.name,
        crs=template.crs,
    )


def append_join_output_columns(
    gdf: Any,
    *,
    normalised_keys: Any,
    records: dict[str, tuple[Any, ...]],
    matched: Sequence[bool],
    join_table: JoinTable,
    match_status_column: str | None,
) -> Any:
    frames = [gdf]
    overlapping_columns: set[str] = set()

    if join_table.output_columns:
        empty_record = tuple(None for _ in join_table.output_columns)
        joined_records = [
            records.get(key, empty_record) if key is not None else empty_record
            for key in normalised_keys
        ]
        joined_frame = pd.DataFrame.from_records(
            joined_records,
            columns=join_table.output_columns,
            index=gdf.index,
        )
        frames.append(joined_frame)
        overlapping_columns.update(
            column for column in joined_frame.columns if column in gdf.columns
        )

    if match_status_column is not None:
        matched_frame = pd.DataFrame(
            {
                match_status_column: pd.Series(
                    matched,
                    index=gdf.index,
                    dtype="boolean",
                )
            },
            index=gdf.index,
        )
        frames.append(matched_frame)
        if match_status_column in gdf.columns:
            overlapping_columns.add(match_status_column)

    base_gdf = gdf.drop(columns=list(overlapping_columns)) if overlapping_columns else gdf
    frames[0] = base_gdf
    return as_geodataframe_like(pd.concat(frames, axis=1), gdf)


def coerce_output_columns(gdf: Any, join_table: JoinTable) -> Any:
    replacements: dict[str, Any] = {}
    for column in INTEGER_OUTPUT_COLUMNS:
        if column in gdf.columns:
            replacements[column] = pd.to_numeric(gdf[column], errors="coerce").astype("Int64")
    for column in join_table.output_columns:
        if column in join_table.numeric_columns:
            numeric = pd.to_numeric(gdf[column], errors="coerce")
            if column in join_table.integer_columns:
                replacements[column] = numeric.astype("Int64")
            else:
                replacements[column] = numeric.astype("Float64")
    if not replacements:
        return gdf

    replacement_columns = [column for column in gdf.columns if column in replacements]
    replacement_frame = pd.DataFrame(
        {column: replacements[column] for column in replacement_columns},
        index=gdf.index,
    )
    unchanged_frame = gdf.drop(columns=replacement_columns)
    combined = pd.concat([unchanged_frame, replacement_frame], axis=1)
    combined = combined.loc[:, list(gdf.columns)]
    return as_geodataframe_like(combined, gdf)


def resolve_rename_map_for_projection(
    rename_map: dict[str, str],
    projected_columns: Sequence[str],
) -> dict[str, str]:
    projected = set(projected_columns)
    resolved: dict[str, str] = {}
    for source, target in rename_map.items():
        if source == "pc11_village_id" and target == "village_id" and target in projected:
            # The standard admin layer already carries village_id as the PC11
            # village id alias, so this legacy Antyodaya rename is redundant.
            continue
        resolved[source] = target
    final_columns = [resolved.get(column, column) for column in projected_columns]
    duplicates = [
        column for column, count in Counter(final_columns).items()
        if count > 1
    ]
    if duplicates:
        raise SystemExit(
            "Configured output has duplicate final column name(s): "
            + ", ".join(duplicates)
        )
    return resolved


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

    projected = [column for column in gdf.columns if column != geometry_column]
    resolved_renames = resolve_rename_map_for_projection(rename_map, projected)
    extra_renames = [column for column in resolved_renames if column not in gdf.columns]
    if extra_renames:
        raise SystemExit(
            "Requested rename source column(s) not found after projection: "
            + ", ".join(extra_renames)
        )
    if resolved_renames:
        gdf = gdf.rename(columns=resolved_renames)
        if geometry_column in resolved_renames:
            gdf = gdf.set_geometry(resolved_renames[geometry_column])
    return gdf


def expand_keep_output_columns(
    keep_columns: Sequence[str],
    *,
    join_table: JoinTable,
) -> list[str]:
    expanded: list[str] = []
    for column in keep_columns:
        values = join_table.output_columns if column == JOINED_COLUMNS_TOKEN else [column]
        for value in values:
            if value not in expanded:
                expanded.append(value)
    return expanded


def validate_output_path(path: Path, *, overwrite: bool, label: str) -> None:
    if path.exists():
        if not overwrite:
            raise SystemExit(f"{label} {path} exists. Pass --overwrite to rebuild it.")
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)


def ensure_gpkg_indexes(gpkg_path: Path, layer: str, index_columns: Sequence[Sequence[str]]) -> None:
    if not index_columns:
        return
    with sqlite3.connect(gpkg_path) as connection:
        fields = {
            str(row[1])
            for row in connection.execute(f"PRAGMA table_info({quote_identifier(layer)})")
        }
        for columns in index_columns:
            if not all(column in fields for column in columns):
                continue
            suffix = "_".join(columns)
            index_name = f"idx_{layer}_{suffix}".replace("-", "_")
            column_sql = ", ".join(quote_identifier(column) for column in columns)
            connection.execute(
                f"CREATE INDEX IF NOT EXISTS {quote_identifier(index_name)} "
                f"ON {quote_identifier(layer)} ({column_sql})"
            )
        connection.commit()


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
        connection.executemany(
            f"""
            INSERT INTO {quote_identifier(table_name)} (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            [
                ("metadata", json_dumps(metadata)),
                ("summary", json_dumps(summary)),
            ],
        )
        connection.commit()


def write_join_summary(reports_dir: Path, summary: dict[str, Any]) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "join_summary.json").write_text(
        json_dumps(summary, indent=True),
        encoding="utf-8",
    )
    report = f"""# CS Admin Boundary Asset Join

Generated by `utilities/scripts/admin_assets/build_cs_admin_boundary_assets.py`.

## Outputs

- GeoPackage: `{summary.get('output_gpkg')}`
- GeoJSON: `{summary.get('output_geojson')}`
- Layer: `{summary.get('output_layer')}`

## Counts

- Admin rows written: `{summary.get('admin_rows', 0):,}`
- Matched admin rows: `{summary.get('matched_admin_rows', 0):,}`
- Unmatched admin rows: `{summary.get('unmatched_admin_rows', 0):,}`
- Source rows: `{summary.get('source_rows', 0):,}`
- Source rows with join key: `{summary.get('source_rows_with_key', 0):,}`
- Source unique keys: `{summary.get('source_unique_keys', 0):,}`
- Unmatched source keys: `{summary.get('unmatched_source_keys', 0):,}`
- Duplicate source-key rows: `{summary.get('duplicate_key_rows', 0):,}`
- Conflicting duplicate source-key rows: `{summary.get('conflicting_duplicate_key_rows', 0):,}`

## Join

- Admin key: `{summary.get('left_key')}`
- Source key: `{summary.get('right_key')}`
- Storage: `{summary.get('join_storage')}`
"""
    (reports_dir / "join_summary.md").write_text(report, encoding="utf-8")


class UnmatchedAdminWriter:
    def __init__(self, path: Path):
        self.path = path
        self.handle = None
        self.writer = None
        self.count = 0
        self.fieldnames = [
            "match_key",
            "cs_feature_id",
            "cs_admin_uid",
            "pc11_village_id",
            "village_id",
            "state_name",
            "district_name",
            "TEHSIL",
            "NAME",
        ]

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("w", newline="", encoding="utf-8")
        self.writer = csv.DictWriter(self.handle, fieldnames=self.fieldnames)
        self.writer.writeheader()
        return self

    def write_chunk(self, gdf: Any, normalised_keys: Sequence[str | None], matched: Sequence[bool]) -> None:
        if self.writer is None:
            return
        for row_index, is_matched in enumerate(matched):
            if is_matched:
                continue
            row = gdf.iloc[row_index]
            self.writer.writerow(
                {
                    "match_key": normalised_keys[row_index],
                    "cs_feature_id": row.get("cs_feature_id"),
                    "cs_admin_uid": row.get("cs_admin_uid"),
                    "pc11_village_id": row.get("pc11_village_id"),
                    "village_id": row.get("village_id"),
                    "state_name": row.get("state_name"),
                    "district_name": row.get("district_name"),
                    "TEHSIL": row.get("TEHSIL"),
                    "NAME": row.get("NAME"),
                }
            )
            self.count += 1

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.handle is not None:
            self.handle.close()


def write_unmatched_source_keys(path: Path, join_table: JoinTable, matched_source_keys: set[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["join_key"])
        writer.writeheader()
        for key in join_table.iter_keys():
            if key in matched_source_keys:
                continue
            writer.writerow({"join_key": key})
            count += 1
    return count


def asset_config_namespace(
    config: dict[str, Any],
    *,
    config_path: Path,
    overwrite: bool,
    use_config_output: bool,
    admin_gpkg_override: Path | None,
    admin_layer_override: str | None,
    output_gpkg_override: Path | None,
    output_layer_override: str | None,
    output_formats_override: str | None,
    geojson_output_override: Path | None,
    reports_dir_override: Path | None,
    chunk_size_override: int | None,
    source_chunk_size_override: int | None,
    join_storage_override: str | None,
    join_work_db_override: Path | None,
    keep_join_work_db_override: bool,
) -> argparse.Namespace:
    admin = config_mapping(config["admin"], field="admin")
    source = config_mapping(config["source"], field="source")
    output = config_mapping(config["output"], field="output")
    processing = config_mapping(config.get("processing", {}), field="processing")
    asset_name = str(config.get("name") or config_path.stem)
    standard_override = ASSET_OUTPUT_OVERRIDES.get(asset_name, {}) if not use_config_output else {}

    formats = (
        parse_output_formats(output_formats_override)
        if output_formats_override
        else config_list(output.get("formats", ["gpkg", "geojson"]), field="output.formats")
    )
    config_admin_columns = config_list(
        output.get("admin_columns", output.get("keep_columns", [])),
        field="output.admin_columns",
    )
    keep_columns = [
        *STANDARD_IDENTITY_COLUMNS,
        *config_admin_columns,
    ]
    keep_columns = ordered_distinct(keep_columns)
    if output.get("include_joined_columns", False):
        keep_columns.append(JOINED_COLUMNS_TOKEN)

    rename_columns = config_mapping(
        output.get("rename_columns", {}),
        field="output.rename_columns",
    )
    if not all(isinstance(key, str) and isinstance(value, str) for key, value in rename_columns.items()):
        raise SystemExit("Asset config `output.rename_columns` must map strings to strings.")

    required_values = {
        "source.path": source.get("path"),
        "source.right_key": source.get("right_key"),
    }
    missing = [field for field, value in required_values.items() if not value]
    if missing:
        raise SystemExit("Asset config missing required value(s): " + ", ".join(missing))

    output_gpkg = (
        output_gpkg_override
        or Path(standard_override.get("gpkg") or output.get("gpkg"))
    )
    output_layer = str(
        output_layer_override
        or standard_override.get("layer")
        or output.get("layer")
    )
    if not output_gpkg or not output_layer:
        raise SystemExit("Asset config missing output GeoPackage or layer.")

    return argparse.Namespace(
        admin_gpkg=admin_gpkg_override or Path(admin.get("gpkg", DEFAULT_ADMIN_GPKG)),
        admin_layer=admin_layer_override or str(admin.get("layer", DEFAULT_ADMIN_LAYER)),
        input=Path(source["path"]),
        output=output_gpkg,
        output_layer=output_layer,
        output_formats=",".join(formats),
        geojson_output=(
            geojson_output_override
            if geojson_output_override is not None
            else Path(standard_override.get("geojson") or output["geojson"])
            if output.get("geojson") or standard_override.get("geojson")
            else None
        ),
        left_key=str(admin.get("left_key", "pc11_village_id")),
        right_key=str(source["right_key"]),
        columns=",".join(config_list(source.get("columns"), field="source.columns")),
        pattern=config_list(source.get("patterns"), field="source.patterns"),
        exclude_columns=",".join(
            config_list(source.get("exclude_columns"), field="source.exclude_columns")
        ),
        prefix=str(source.get("prefix", "")),
        overwrite=overwrite,
        reports_dir=(
            reports_dir_override
            or Path(standard_override.get("reports_dir") or output.get("reports_dir", "data/admin-boundary/cs_asset_reports"))
        ),
        chunk_size=chunk_size_override or int(processing.get("chunk_size", DEFAULT_CHUNK_SIZE)),
        no_match_status=not bool(output.get("include_match_status", False)),
        match_status_column=str(output.get("match_status_column", "property_join_matched")),
        keep_output_columns=",".join(keep_columns),
        rename_output_columns=",".join(
            f"{source_column}={target_column}"
            for source_column, target_column in rename_columns.items()
        ),
        join_storage=join_storage_override or str(processing.get("join_storage", "auto")),
        source_chunk_size=(
            source_chunk_size_override
            or int(processing.get("source_chunk_size", processing.get("chunk_size", DEFAULT_SOURCE_CHUNK_SIZE)))
        ),
        join_work_db=(
            join_work_db_override
            if join_work_db_override is not None
            else Path(processing["join_work_db"])
            if processing.get("join_work_db")
            else None
        ),
        keep_join_work_db=keep_join_work_db_override or bool(processing.get("keep_join_work_db", False)),
        asset_config=config_path.as_posix(),
        asset_config_sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
        asset_name=asset_name,
    )


def validate_asset_config(args: argparse.Namespace) -> dict[str, Any]:
    admin_gpkg = repo_path(args.admin_gpkg)
    source_path = repo_path(args.input)
    if not admin_gpkg.exists():
        raise SystemExit(f"Standard admin GeoPackage not found: {admin_gpkg}")
    if not source_path.exists():
        raise SystemExit(f"Join input not found: {source_path}")
    if args.chunk_size <= 0 or args.source_chunk_size <= 0:
        raise SystemExit("Configured chunk sizes must be positive integers.")

    admin_columns = gpkg_layer_columns(admin_gpkg, args.admin_layer)
    if args.left_key not in admin_columns:
        raise SystemExit(
            f"Left join key `{args.left_key}` not found in "
            f"{admin_gpkg}:{args.admin_layer}."
        )
    header_columns = source_header_columns(source_path)
    if args.right_key not in header_columns:
        raise SystemExit(f"Join key `{args.right_key}` not found in {source_path}.")
    selected = resolve_join_columns(
        header_columns,
        right_key=args.right_key,
        columns=parse_csv_list(args.columns),
        patterns=args.pattern,
        exclude_columns=parse_csv_list(args.exclude_columns),
    )
    output_columns, _ = prepare_output_column_names(
        selected,
        admin_columns=admin_columns,
        prefix=args.prefix,
    )
    keep_columns = expand_keep_output_columns(
        parse_csv_list(args.keep_output_columns),
        join_table=JoinTable(
            records_by_key={},
            sqlite_connection=None,
            sqlite_path=None,
            remove_sqlite_on_close=False,
            selected_columns=selected,
            output_columns=output_columns,
            output_to_source_column={},
            numeric_columns=set(),
            integer_columns=set(),
            source_rows=0,
            source_rows_with_key=0,
            duplicate_key_rows=0,
            conflicting_duplicate_key_rows=0,
            source_unique_keys=0,
            storage=choose_join_storage(source_path, args.join_storage),
        ),
    )
    available_output_columns = [
        *admin_columns,
        *output_columns,
        *([] if args.no_match_status else [args.match_status_column]),
    ]
    missing_keep = [column for column in keep_columns if column not in available_output_columns]
    if missing_keep:
        raise SystemExit("Configured output column(s) not found: " + ", ".join(missing_keep))
    rename_map = parse_rename_map(args.rename_output_columns)
    resolved_rename_map = resolve_rename_map_for_projection(rename_map, keep_columns or available_output_columns)
    final_columns = [
        resolved_rename_map.get(column, column)
        for column in (keep_columns or available_output_columns)
    ]
    duplicate_final = [
        column for column, count in Counter(final_columns).items()
        if count > 1
    ]
    if duplicate_final:
        raise SystemExit(
            "Configured output has duplicate final column name(s): "
            + ", ".join(duplicate_final)
        )
    output_gpkg = repo_path(args.output)
    output_geojson = (
        repo_path(args.geojson_output)
        if args.geojson_output
        else default_geojson_path(output_gpkg)
    )
    return {
        "asset_name": args.asset_name,
        "asset_config": args.asset_config,
        "asset_config_sha256": args.asset_config_sha256,
        "admin_gpkg": admin_gpkg.as_posix(),
        "admin_layer": args.admin_layer,
        "source": source_path.as_posix(),
        "source_bytes": source_path.stat().st_size,
        "source_columns": len(header_columns),
        "selected_columns": len(selected),
        "selected_first": selected[:10],
        "selected_last": selected[-10:],
        "join_storage": choose_join_storage(source_path, args.join_storage),
        "output_formats": parse_output_formats(args.output_formats),
        "output_gpkg": output_gpkg.as_posix(),
        "output_geojson": output_geojson.as_posix(),
        "output_layer": args.output_layer,
        "final_column_count": len(final_columns),
        "final_columns_first": final_columns[:15],
        "final_columns_last": final_columns[-12:],
    }


def join_properties_to_admin(args: argparse.Namespace) -> dict[str, Any]:
    ensure_dependencies()
    started = time.perf_counter()
    admin_gpkg = repo_path(args.admin_gpkg)
    source_path = repo_path(args.input)
    output_gpkg = repo_path(args.output)
    output_geojson = (
        repo_path(args.geojson_output)
        if args.geojson_output
        else default_geojson_path(output_gpkg)
    )
    reports_dir = repo_path(args.reports_dir)
    output_formats = parse_output_formats(args.output_formats)

    if not admin_gpkg.exists():
        raise SystemExit(f"Standard admin GeoPackage not found: {admin_gpkg}")
    if not source_path.exists():
        raise SystemExit(f"Join input not found: {source_path}")
    if "gpkg" in output_formats:
        validate_output_path(output_gpkg, overwrite=args.overwrite, label="GeoPackage")
    if "geojson" in output_formats:
        validate_output_path(output_geojson, overwrite=args.overwrite, label="GeoJSON")

    info = pyogrio.read_info(admin_gpkg, layer=args.admin_layer)
    admin_columns = [str(field) for field in info.get("fields", [])]
    if args.left_key not in admin_columns:
        raise SystemExit(
            f"Left join key `{args.left_key}` not found in {admin_gpkg}:{args.admin_layer}"
        )
    keep_output_columns = parse_csv_list(args.keep_output_columns)
    rename_output_columns = parse_rename_map(args.rename_output_columns)

    print(f"[asset] loading property table {source_path}", flush=True)
    join_table = build_join_table(
        source_path,
        right_key=args.right_key,
        selected_columns=parse_csv_list(args.columns),
        patterns=args.pattern,
        exclude_columns=parse_csv_list(args.exclude_columns),
        output_prefix=args.prefix,
        admin_columns=admin_columns,
        storage=args.join_storage,
        source_chunk_size=args.source_chunk_size,
        work_db=args.join_work_db,
    )
    keep_output_columns = expand_keep_output_columns(
        keep_output_columns,
        join_table=join_table,
    )

    total_features = int(info.get("features") or 0)
    offset = 0
    rows_written = 0
    matched_rows = 0
    first_chunk = True
    final_fields: list[str] = []
    matched_source_keys: set[str] = set()
    unmatched_admin_path = reports_dir / "unmatched_admin_rows.csv"
    unmatched_source_path = reports_dir / "unmatched_source_keys.csv"

    print(
        f"[asset] joining {join_table.source_unique_keys:,} source keys onto "
        f"{total_features:,} admin features",
        flush=True,
    )

    geojson_writer = ChunkedGeoJSONWriter(output_geojson) if "geojson" in output_formats else None
    try:
        with (
            geojson_writer if geojson_writer is not None else nullcontext(None)
        ) as writer, UnmatchedAdminWriter(unmatched_admin_path) as unmatched_admin_writer:
            while True:
                gdf = pyogrio.read_dataframe(
                    admin_gpkg,
                    layer=args.admin_layer,
                    skip_features=offset,
                    max_features=args.chunk_size,
                )
                if gdf.empty:
                    break
                normalised_keys = gdf[args.left_key].map(normalize_join_id).tolist()
                records = join_table.lookup(normalised_keys)
                matched = [
                    key in records if key is not None else False
                    for key in normalised_keys
                ]
                matched_rows += sum(1 for value in matched if value)
                matched_source_keys.update(
                    key for key, value in zip(normalised_keys, matched) if key and value
                )
                unmatched_admin_writer.write_chunk(gdf, normalised_keys, matched)

                gdf = append_join_output_columns(
                    gdf,
                    normalised_keys=normalised_keys,
                    records=records,
                    matched=matched,
                    join_table=join_table,
                    match_status_column=(
                        None if args.no_match_status else args.match_status_column
                    ),
                )
                gdf = coerce_output_columns(gdf, join_table)
                gdf = project_and_rename_output(
                    gdf,
                    keep_columns=keep_output_columns,
                    rename_map=rename_output_columns,
                )
                final_fields = [column for column in gdf.columns if column != gdf.geometry.name]

                if "gpkg" in output_formats:
                    pyogrio.write_dataframe(
                        gdf,
                        output_gpkg,
                        layer=args.output_layer,
                        driver="GPKG",
                        append=not first_chunk,
                        promote_to_multi=True,
                    )
                if "geojson" in output_formats and writer is not None:
                    writer.write(gdf)
                rows_written += len(gdf)
                offset += len(gdf)
                first_chunk = False
                if rows_written == len(gdf) or rows_written % (args.chunk_size * 2) == 0:
                    denominator = f"/{total_features:,}" if total_features else ""
                    print(f"[asset] wrote {rows_written:,}{denominator} joined features", flush=True)

        unmatched_source_count = write_unmatched_source_keys(
            unmatched_source_path,
            join_table,
            matched_source_keys,
        )
    finally:
        join_table.close(keep_sqlite=args.keep_join_work_db)

    final_left_key = "village_id" if "village_id" in final_fields else args.left_key
    if "gpkg" in output_formats:
        ensure_gpkg_indexes(
            output_gpkg,
            args.output_layer,
            [
                ["cs_feature_id"],
                ["cs_admin_uid"],
                ["core_admin_uid"],
                [final_left_key],
                ["pc11_village_id"],
                ["state_name", "district_name", "TEHSIL"],
            ],
        )

    summary = {
        "asset_name": args.asset_name,
        "asset_config": args.asset_config,
        "asset_config_sha256": args.asset_config_sha256,
        "admin_gpkg": admin_gpkg.as_posix(),
        "admin_layer": args.admin_layer,
        "source": source_path.as_posix(),
        "output_gpkg": output_gpkg.as_posix() if "gpkg" in output_formats else None,
        "output_geojson": output_geojson.as_posix() if "geojson" in output_formats else None,
        "output_formats": output_formats,
        "output_layer": args.output_layer,
        "left_key": args.left_key,
        "right_key": args.right_key,
        "selected_columns": join_table.selected_columns,
        "output_columns": join_table.output_columns,
        "final_output_columns": final_fields,
        "source_rows": join_table.source_rows,
        "source_rows_with_key": join_table.source_rows_with_key,
        "source_unique_keys": join_table.source_unique_keys,
        "duplicate_key_rows": join_table.duplicate_key_rows,
        "conflicting_duplicate_key_rows": join_table.conflicting_duplicate_key_rows,
        "join_storage": join_table.storage,
        "join_work_db": (
            join_table.sqlite_path.as_posix()
            if join_table.sqlite_path is not None and args.keep_join_work_db
            else None
        ),
        "admin_rows": rows_written,
        "matched_admin_rows": matched_rows,
        "unmatched_admin_rows": rows_written - matched_rows,
        "unmatched_source_keys": unmatched_source_count,
        "unmatched_admin_rows_csv": unmatched_admin_path.as_posix(),
        "unmatched_source_keys_csv": unmatched_source_path.as_posix(),
        "match_rate": round(matched_rows / rows_written, 6) if rows_written else 0,
        "total_seconds": round(time.perf_counter() - started, 6),
    }
    write_join_summary(reports_dir, summary)
    if "gpkg" in output_formats:
        write_gpkg_metadata(
            output_gpkg,
            f"{args.output_layer}_asset_metadata",
            {"script": Path(__file__).as_posix()},
            summary,
        )
    print(
        f"[asset] complete: {matched_rows:,}/{rows_written:,} admin features matched "
        f"in {summary['total_seconds']:.1f}s",
        flush=True,
    )
    return summary


def run_configured_asset(args: argparse.Namespace) -> dict[str, Any]:
    config_path = resolve_asset_config_path(args.config)
    config = load_asset_config(config_path)
    join_args = asset_config_namespace(
        config,
        config_path=config_path,
        overwrite=args.overwrite,
        use_config_output=args.use_config_output,
        admin_gpkg_override=args.admin_gpkg,
        admin_layer_override=args.admin_layer,
        output_gpkg_override=args.output_gpkg,
        output_layer_override=args.output_layer,
        output_formats_override=args.output_formats,
        geojson_output_override=args.geojson_output,
        reports_dir_override=args.reports_dir,
        chunk_size_override=args.chunk_size,
        source_chunk_size_override=args.source_chunk_size,
        join_storage_override=args.join_storage,
        join_work_db_override=args.join_work_db,
        keep_join_work_db_override=args.keep_join_work_db,
    )
    validation = validate_asset_config(join_args)
    print(json_dumps(validation, indent=True), flush=True)
    if args.validate_only:
        return validation
    return join_properties_to_admin(join_args)


def add_common_asset_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("config", help="Asset config name or JSON path, e.g. livestock or antyodaya")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument(
        "--use-config-output",
        action="store_true",
        help="Use output paths from the config instead of the new cs_* defaults.",
    )
    parser.add_argument("--admin-gpkg", type=Path)
    parser.add_argument("--admin-layer")
    parser.add_argument("--output-gpkg", type=Path)
    parser.add_argument("--output-layer")
    parser.add_argument(
        "--output-formats",
        help="Comma-separated output formats: gpkg, geojson, or gpkg,geojson.",
    )
    parser.add_argument("--geojson-output", type=Path)
    parser.add_argument("--reports-dir", type=Path)
    parser.add_argument("--chunk-size", type=int)
    parser.add_argument("--source-chunk-size", type=int)
    parser.add_argument("--join-storage", choices=sorted(SUPPORTED_JOIN_STORAGE))
    parser.add_argument("--join-work-db", type=Path)
    parser.add_argument("--keep-join-work-db", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build configured assets from data/base_resources/cs_admin_standard.gpkg."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    asset_parser = subparsers.add_parser("asset", help="Build an asset from a config")
    add_common_asset_args(asset_parser)
    validate_parser = subparsers.add_parser("validate", help="Validate a config without writing outputs")
    add_common_asset_args(validate_parser)
    validate_parser.set_defaults(validate_only=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    if raw_args and raw_args[0] not in {"asset", "validate"} and not raw_args[0].startswith("-"):
        raw_args.insert(0, "asset")
    parser = build_parser()
    args = parser.parse_args(raw_args)
    run_configured_asset(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
