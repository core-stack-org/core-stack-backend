#!/usr/bin/env python3
"""Build the Core Stack standardized village admin boundary asset.

This script is intentionally separate from ``build_admin_boundary_assets.py``.
It reads the district GeoJSON files under ``data/admin-boundary/input`` and
keeps a source-row trace from those raw files into the final standard asset.

Default outputs:

    data/admin-boundary/cs_admin_standard.gpkg
    data/admin-boundary/cs_admin_standard.geojson
    data/admin-boundary/cs_admin_standard.csv
    data/admin-boundary/cs_admin_standard_reports/*

The GeoPackage remains the primary output. The CSV is an attribute-only sibling
for review, joins, and quick QA.
"""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import resource
import sqlite3
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

try:
    from pyproj import Geod
except Exception as exc:  # pragma: no cover - handled in ensure_dependencies
    Geod = None
    PYPROJ_IMPORT_ERROR = exc
else:
    PYPROJ_IMPORT_ERROR = None

try:
    from shapely.geometry import MultiPolygon
    from shapely.ops import unary_union
    from shapely.wkb import loads as load_wkb
except Exception as exc:  # pragma: no cover - handled in ensure_dependencies
    MultiPolygon = None
    unary_union = None
    load_wkb = None
    SHAPELY_IMPORT_ERROR = exc
else:
    SHAPELY_IMPORT_ERROR = None

try:
    from shapely.validation import make_valid as shapely_make_valid
except Exception:  # pragma: no cover - compatibility fallback
    shapely_make_valid = None


ROOT_DIR = Path(__file__).resolve().parents[3]

DEFAULT_ADMIN_INPUT_DIR = Path("data/admin-boundary/input")
DEFAULT_MULTIPART_ANALYSIS = Path("data/admin-boundary/multi_part_village_analysis.csv")
DEFAULT_OUTPUT_GPKG = Path("data/admin-boundary/cs_admin_standard.gpkg")
DEFAULT_OUTPUT_LAYER = "cs_admin_standard"
DEFAULT_OUTPUT_GEOJSON = Path("data/admin-boundary/cs_admin_standard.geojson")
DEFAULT_OUTPUT_CSV = Path("data/admin-boundary/cs_admin_standard.csv")
DEFAULT_REPORTS_DIR = Path("data/admin-boundary/cs_admin_standard_reports")
DEFAULT_CHUNK_SIZE = 50_000
SQLITE_BATCH_SIZE = 5_000
GEOMETRY_OVERLAP_AREA_EPSILON = 0.0

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

ADMIN_INT_COLUMNS = [
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
    "standard_group_part_count",
    "standard_name_count",
]

STANDARD_COLUMNS = [
    "cs_feature_id",
    "cs_admin_uid",
    "core_admin_uid",
    "source_group_key",
    "standard_group_key",
    "feature_part_index",
    "feature_part_count",
    "village_shape_hash",
    "state_name",
    "district_name",
    "TEHSIL",
    "pc11_village_id",
    "village_id",
    "NAME",
    "pc11_state_id",
    "pc11_district_id",
    "pc11_subdistrict_id",
    "source_row_count",
    "source_identity_count",
    "source_geometry_count",
    "source_file_count",
    "resolution_status",
    "standard_action",
    "standard_reason",
    "standard_notes",
    "standard_pattern_flags",
    "standard_group_part_count",
    "standard_part_area_km2",
    "standard_part_area_share",
    "standard_group_area_km2",
    "standard_min_edge_distance_m",
    "standard_min_centroid_distance_km",
    "standard_max_centroid_distance_km",
    "standard_name_count",
]

GEOD = Geod(ellps="WGS84") if Geod is not None else None


def repo_path(path: Path | str) -> Path:
    path = Path(path)
    return path if path.is_absolute() else ROOT_DIR / path


def ensure_dependencies() -> None:
    missing = []
    if gpd is None or pd is None or pyogrio is None:
        missing.append(f"geopandas/pandas/pyogrio ({GEOSTACK_IMPORT_ERROR})")
    if Geod is None:
        missing.append(f"pyproj ({PYPROJ_IMPORT_ERROR})")
    if MultiPolygon is None or unary_union is None or load_wkb is None:
        missing.append(f"shapely ({SHAPELY_IMPORT_ERROR})")
    if missing:
        raise SystemExit(
            "Missing geospatial dependencies: "
            + ", ".join(missing)
            + "\nRun with uv, for example: "
            + "`uv run --with geopandas --with pyogrio --with shapely "
            + "--with pyproj --with pandas python "
            + "utilities/scripts/admin_assets/build_cs_admin_boundary_standard.py --overwrite`."
        )


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
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return int(text) if text.isdigit() else None


def normalize_label(value: Any) -> str:
    text = clean_text(value)
    return "" if text is None else " ".join(text.upper().split())


def normalize_slug(value: Any) -> str:
    text = clean_text(value)
    if text is None:
        return "unknown"
    output = []
    last_was_sep = False
    for char in text.lower():
        if char.isalnum():
            output.append(char)
            last_was_sep = False
        elif not last_was_sep:
            output.append("_")
            last_was_sep = True
    slug = "".join(output).strip("_")
    return slug or "unknown"


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


def stable_shape_hash(geometry: Any) -> str | None:
    if geometry is None or geometry.is_empty:
        return None
    normalised = geometry.normalize() if hasattr(geometry, "normalize") else geometry
    return hashlib.sha256(normalised.wkb).hexdigest()


def normalize_property(column: str, value: Any) -> int | str | None:
    if column in ID_COLUMNS:
        return clean_int(value)
    return clean_text(value)


def stable_identity_hash(properties: dict[str, Any], geometry_hash: str | None) -> str:
    payload = {
        "properties": {column: properties.get(column) for column in ADMIN_COLUMNS},
        "geometry_hash": geometry_hash,
    }
    return hashlib.sha256(json_dumps(payload).encode("utf-8")).hexdigest()


def source_group_key_for(properties: dict[str, Any], identity_hash: str) -> str:
    village_id = properties.get("pc11_village_id")
    if village_id is None:
        return f"invalid:{identity_hash[:24]}"
    parts = [normalize_slug(properties.get(column)) for column in ADMIN_KEY_COLUMNS[:-1]]
    return "village:" + ":".join([*parts, str(village_id)])


def repair_geometry(geometry: Any) -> Any | None:
    if geometry is None or geometry.is_empty:
        return None
    if not geometry.is_valid:
        geometry = shapely_make_valid(geometry) if shapely_make_valid is not None else geometry.buffer(0)
    if geometry is None or geometry.is_empty:
        return None
    if geometry.geom_type == "Polygon":
        return MultiPolygon([geometry])
    if geometry.geom_type == "MultiPolygon":
        return geometry
    if geometry.geom_type == "GeometryCollection":
        polygons = []
        for part in geometry.geoms:
            if part.geom_type == "Polygon":
                polygons.append(part)
            elif part.geom_type == "MultiPolygon":
                polygons.extend(part.geoms)
        return MultiPolygon(polygons) if polygons else None
    return None


def geodesic_area_km2(geometry: Any) -> float:
    if geometry is None or geometry.is_empty or GEOD is None:
        return 0.0
    try:
        area_m2, _ = GEOD.geometry_area_perimeter(geometry)
    except Exception:
        repaired = repair_geometry(geometry)
        if repaired is None:
            return 0.0
        area_m2, _ = GEOD.geometry_area_perimeter(repaired)
    return abs(float(area_m2)) / 1_000_000.0


def local_utm_epsg(geometries: Sequence[Any]) -> int:
    non_empty = [geom for geom in geometries if geom is not None and not geom.is_empty]
    if not non_empty:
        return 32643
    centroid = unary_union(non_empty).centroid
    zone = int((centroid.x + 180) / 6) + 1
    return 32600 + max(1, min(zone, 60))


def group_distance_metrics(geometries: Sequence[Any]) -> dict[str, float | None]:
    if len(geometries) < 2:
        return {
            "min_edge_distance_m": None,
            "min_centroid_distance_km": None,
            "max_centroid_distance_km": None,
        }
    repaired = [repair_geometry(geometry) for geometry in geometries]
    repaired = [geometry for geometry in repaired if geometry is not None]
    if len(repaired) < 2:
        return {
            "min_edge_distance_m": None,
            "min_centroid_distance_km": None,
            "max_centroid_distance_km": None,
        }
    projected = gpd.GeoSeries(repaired, crs="EPSG:4326").to_crs(epsg=local_utm_epsg(repaired))
    edge_distances = []
    centroid_distances = []
    for left_index, left_geometry in enumerate(projected):
        for right_geometry in projected.iloc[left_index + 1 :]:
            edge_distances.append(float(left_geometry.distance(right_geometry)))
            centroid_distances.append(float(left_geometry.centroid.distance(right_geometry.centroid)) / 1000.0)
    return {
        "min_edge_distance_m": min(edge_distances) if edge_distances else None,
        "min_centroid_distance_km": min(centroid_distances) if centroid_distances else None,
        "max_centroid_distance_km": max(centroid_distances) if centroid_distances else None,
    }


def ordered_distinct(values: Iterable[Any]) -> list[str]:
    seen = set()
    output = []
    for value in values:
        text = clean_text(value)
        if text is None or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def read_csv_with_fallback(path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in ["utf-8-sig", "cp1252", "latin1"]:
        try:
            return pd.read_csv(path, dtype=str, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
    raise SystemExit(f"Could not read {path} with UTF-8, cp1252, or latin1: {last_error}")


def load_multipart_notes(path: Path) -> dict[int, dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"Multipart analysis CSV not found: {path}")
    dataframe = read_csv_with_fallback(path)
    notes: dict[int, dict[str, Any]] = {}
    for row in dataframe.to_dict("records"):
        village_id = clean_int(row.get("pc11_village_id"))
        if village_id is None:
            continue
        notes[village_id] = row
    return notes


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


def create_work_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
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
        );

        CREATE INDEX source_rows_identity_hash_idx ON source_rows(identity_hash);
        CREATE INDEX source_rows_source_group_idx ON source_rows(source_group_key);
        CREATE INDEX source_rows_pc11_village_id_idx ON source_rows(pc11_village_id);

        CREATE TABLE identities (
            identity_hash TEXT PRIMARY KEY,
            source_group_key TEXT NOT NULL,
            geometry_hash TEXT,
            geom_wkb BLOB,
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

        CREATE TABLE source_file_audit (
            source_file TEXT PRIMARY KEY,
            source_state_slug TEXT NOT NULL,
            source_district_slug TEXT NOT NULL,
            read_status TEXT NOT NULL,
            read_error TEXT,
            feature_count INTEGER,
            field_count INTEGER,
            crs TEXT,
            rows_read INTEGER NOT NULL,
            valid_village_id_rows INTEGER NOT NULL,
            invalid_village_id_rows INTEGER NOT NULL,
            elapsed_seconds REAL NOT NULL
        );

        CREATE TABLE candidate_features (
            candidate_feature_id TEXT PRIMARY KEY,
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
            geom_wkb BLOB NOT NULL,
            source_row_count INTEGER NOT NULL,
            source_identity_count INTEGER NOT NULL,
            source_geometry_count INTEGER NOT NULL,
            source_file_count INTEGER NOT NULL,
            resolution_status TEXT NOT NULL
        );

        CREATE INDEX candidate_features_pc11_village_id_idx ON candidate_features(pc11_village_id);
        CREATE INDEX candidate_features_source_group_idx ON candidate_features(source_group_key);

        CREATE TABLE standard_trace_map (
            cs_feature_id TEXT,
            trace_status TEXT NOT NULL,
            candidate_feature_id TEXT,
            identity_hash TEXT NOT NULL,
            standard_reason TEXT
        );

        CREATE INDEX standard_trace_map_feature_idx ON standard_trace_map(cs_feature_id);
        CREATE INDEX standard_trace_map_identity_idx ON standard_trace_map(identity_hash);
        """
    )


def insert_source_rows(connection: sqlite3.Connection, rows: list[tuple[Any, ...]]) -> None:
    if rows:
        connection.executemany(
            """
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def insert_identity_rows(connection: sqlite3.Connection, rows: list[tuple[Any, ...]]) -> None:
    if rows:
        connection.executemany(
            """
            INSERT INTO identities (
                identity_hash,
                source_group_key,
                geometry_hash,
                geom_wkb,
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
            ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(identity_hash) DO UPDATE SET
                source_row_count = source_row_count + 1
            """,
            rows,
        )


def ingest_source_file(connection: sqlite3.Connection, path: Path) -> dict[str, Any]:
    started = time.perf_counter()
    source_file = path.relative_to(ROOT_DIR).as_posix() if path.is_relative_to(ROOT_DIR) else path.as_posix()
    state_slug = normalize_slug(path.parent.name)
    district_slug = normalize_slug(path.stem)
    source_batch: list[tuple[Any, ...]] = []
    identity_batch: list[tuple[Any, ...]] = []
    rows_read = 0
    valid_rows = 0
    invalid_rows = 0
    read_status = "ok"
    read_error = None
    feature_count = None
    field_count = None
    crs = ""
    try:
        info = pyogrio.read_info(path)
        feature_count = int(info.get("features") or 0)
        fields = info.get("fields")
        field_count = len(fields) if fields is not None else 0
        crs = str(info.get("crs") or "")
        gdf = pyogrio.read_dataframe(path)
        for row_number, row in enumerate(gdf.itertuples(index=False), start=1):
            rows_read += 1
            row_data = row._asdict()
            properties = {
                column: normalize_property(column, row_data.get(column))
                for column in ADMIN_COLUMNS
            }
            geometry = repair_geometry(row_data.get(gdf.geometry.name))
            geometry_hash = stable_shape_hash(geometry)
            identity_hash = stable_identity_hash(properties, geometry_hash)
            source_group_key = source_group_key_for(properties, identity_hash)
            if properties["pc11_village_id"] is None:
                invalid_rows += 1
            else:
                valid_rows += 1
            source_batch.append(
                (
                    source_file,
                    state_slug,
                    district_slug,
                    row_number,
                    source_group_key,
                    identity_hash,
                    geometry_hash,
                    properties["pc11_village_id"],
                    properties["state_name"],
                    properties["district_name"],
                    properties["TEHSIL"],
                    properties["NAME"],
                    properties["pc11_state_id"],
                    properties["pc11_district_id"],
                    properties["pc11_subdistrict_id"],
                )
            )
            if geometry is not None:
                identity_batch.append(
                    (
                        identity_hash,
                        source_group_key,
                        geometry_hash,
                        sqlite3.Binary(geometry.wkb),
                        source_file,
                        row_number,
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
            if len(source_batch) >= SQLITE_BATCH_SIZE:
                insert_source_rows(connection, source_batch)
                insert_identity_rows(connection, identity_batch)
                source_batch.clear()
                identity_batch.clear()
        insert_source_rows(connection, source_batch)
        insert_identity_rows(connection, identity_batch)
        connection.commit()
    except Exception as exc:
        read_status = "error"
        read_error = str(exc)
    audit = {
        "source_file": source_file,
        "source_state_slug": state_slug,
        "source_district_slug": district_slug,
        "read_status": read_status,
        "read_error": read_error,
        "feature_count": feature_count,
        "field_count": field_count,
        "crs": crs,
        "rows_read": rows_read,
        "valid_village_id_rows": valid_rows,
        "invalid_village_id_rows": invalid_rows,
        "elapsed_seconds": round(time.perf_counter() - started, 6),
    }
    connection.execute(
        """
        INSERT INTO source_file_audit (
            source_file,
            source_state_slug,
            source_district_slug,
            read_status,
            read_error,
            feature_count,
            field_count,
            crs,
            rows_read,
            valid_village_id_rows,
            invalid_village_id_rows,
            elapsed_seconds
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            audit["source_file"],
            audit["source_state_slug"],
            audit["source_district_slug"],
            audit["read_status"],
            audit["read_error"],
            audit["feature_count"],
            audit["field_count"],
            audit["crs"],
            audit["rows_read"],
            audit["valid_village_id_rows"],
            audit["invalid_village_id_rows"],
            audit["elapsed_seconds"],
        ),
    )
    connection.commit()
    return audit


def load_identity_geometry(row: sqlite3.Row) -> Any | None:
    if row["geom_wkb"] is None:
        return None
    return repair_geometry(load_wkb(bytes(row["geom_wkb"])))


def dissolve_geometries(geometries: Sequence[Any]) -> Any | None:
    repaired = [repair_geometry(geometry) for geometry in geometries]
    repaired = [geometry for geometry in repaired if geometry is not None]
    if not repaired:
        return None
    if len(repaired) == 1:
        return repaired[0]
    return repair_geometry(unary_union(repaired))


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
    rows_with_geometry: list[tuple[sqlite3.Row, Any]] = []
    for row in identity_rows:
        geometry = load_identity_geometry(row)
        if geometry is not None:
            rows_with_geometry.append((row, geometry))
    if not rows_with_geometry:
        return []

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
    return [grouped[root] for root in order]


def iter_identity_groups(connection: sqlite3.Connection) -> Iterable[tuple[str, list[sqlite3.Row]]]:
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


def choose_property(values: Sequence[Any], *, numeric: bool) -> Any:
    distinct = ordered_distinct(values)
    if not distinct:
        return None
    if len(distinct) == 1:
        return clean_int(distinct[0]) if numeric else distinct[0]
    return clean_int(distinct[0]) if numeric else " | ".join(str(value) for value in distinct[:12])


def candidate_feature_key(source_group_key: str, part_index: int, part_count: int) -> str:
    if part_count == 1:
        return source_group_key
    return f"{source_group_key}:part:{part_index}"


def classify_candidate_resolution(
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


def flush_candidate_features(connection: sqlite3.Connection, rows: list[tuple[Any, ...]]) -> None:
    if rows:
        connection.executemany(
            """
            INSERT INTO candidate_features (
                candidate_feature_id,
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
                source_row_count,
                source_identity_count,
                source_geometry_count,
                source_file_count,
                resolution_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def build_candidate_features(connection: sqlite3.Connection) -> dict[str, Any]:
    started = time.perf_counter()
    insert_rows: list[tuple[Any, ...]] = []
    resolution_counts: Counter[str] = Counter()
    source_group_count = 0
    output_count = 0
    skipped_empty = 0
    split_source_groups = 0

    for source_group_key, identity_rows in iter_identity_groups(connection):
        source_group_count += 1
        components = geometry_components(identity_rows)
        part_count = len(components)
        if part_count == 0:
            skipped_empty += 1
            continue
        if part_count > 1:
            split_source_groups += 1
        for part_index, component_rows in enumerate(components, start=1):
            geometries = [load_identity_geometry(row) for row in component_rows]
            geometry = dissolve_geometries(geometries)
            if geometry is None:
                skipped_empty += 1
                continue
            source_row_count = sum(int(row["source_row_count"]) for row in component_rows)
            identity_count = len(component_rows)
            geometry_count = len({row["geometry_hash"] for row in component_rows if row["geometry_hash"]})
            source_file_count = len({row["first_source_file"] for row in component_rows})
            source_identity_hashes = "|".join(row["identity_hash"] for row in component_rows)
            properties = {
                column: choose_property(
                    [row[column] for row in component_rows],
                    numeric=column in ID_COLUMNS,
                )
                for column in ADMIN_COLUMNS
            }
            resolution_status = classify_candidate_resolution(
                source_row_count,
                identity_count,
                geometry_count,
                part_count=part_count,
            )
            resolution_counts[resolution_status] += 1
            insert_rows.append(
                (
                    candidate_feature_key(source_group_key, part_index, part_count),
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
                    source_row_count,
                    identity_count,
                    geometry_count,
                    source_file_count,
                    resolution_status,
                )
            )
            output_count += 1
            if len(insert_rows) >= SQLITE_BATCH_SIZE:
                flush_candidate_features(connection, insert_rows)
                insert_rows.clear()
    flush_candidate_features(connection, insert_rows)
    connection.commit()
    return {
        "candidate_source_groups": source_group_count,
        "candidate_features": output_count,
        "candidate_skipped_empty": skipped_empty,
        "candidate_split_source_groups": split_source_groups,
        "candidate_resolution_status_counts": dict(resolution_counts),
        "candidate_build_seconds": round(time.perf_counter() - started, 6),
    }


def candidate_rows_to_gdf(rows: Sequence[sqlite3.Row]) -> Any:
    records = []
    for row in rows:
        geometry = repair_geometry(load_wkb(bytes(row["geom_wkb"]))) if row["geom_wkb"] is not None else None
        records.append(
            {
                "cs_feature_id": row["candidate_feature_id"],
                "source_group_key": row["source_group_key"],
                "feature_part_index": row["feature_part_index"],
                "feature_part_count": row["feature_part_count"],
                "source_identity_hashes": row["source_identity_hashes"],
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
        )
    return gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")


def standard_group_key(village_id: Any, fallback: str) -> str:
    number = clean_int(village_id)
    return f"village:{number}" if number is not None else fallback


def standard_uid(village_id: Any, *, part_index: int, part_count: int, fallback: str) -> str:
    base = standard_group_key(village_id, fallback=fallback)
    if clean_int(village_id) is None:
        return fallback
    return base if part_count == 1 else f"{base}:part:{part_index}"


def note_text(note_row: dict[str, Any] | None) -> str:
    if not note_row:
        return ""
    return clean_text(note_row.get("notes")) or ""


def has_any(text: str, needles: Sequence[str]) -> bool:
    lowered = text.lower()
    return any(needle in lowered for needle in needles)


def classify_note_action(note: str) -> str | None:
    lowered = note.lower()
    if "discard outdated" in lowered and "telangana" in lowered:
        return "prefer_telangana_duplicate"
    explicit_keep = has_any(
        lowered,
        [
            "keep them both",
            "keep them multipart",
            "remain as multiparts",
            "should remain",
            "valid multipart",
            "multipart village",
            "local cluster",
            "probable towns",
            "townships",
            "different village names",
            "no probable fuzzy edge",
            "pass a valid multipart",
        ],
    )
    negative_merge = has_any(
        lowered,
        [
            "not a good merge",
            "should remain",
            "keep them both",
            "remain as multiparts",
            "valid multipart",
        ],
    )
    explicit_merge = has_any(
        lowered,
        [
            "simply be merged",
            "unary",
            "candidate for merging",
            "perfect candiates for merging",
            "perfect candidates for merging",
            "probably just be merged",
            "one row with all parts together",
            "complete surrounding",
            "surrounding another, merge",
            " split_distinct_geometry_part, merge",
        ],
    )
    if explicit_keep:
        return "keep_multipart"
    if explicit_merge and not negative_merge:
        return "merge_parts"
    return None


def all_geometries_equal(geometries: Sequence[Any]) -> bool:
    repaired = [repair_geometry(geometry) for geometry in geometries]
    repaired = [geometry for geometry in repaired if geometry is not None]
    if len(repaired) < 2:
        return False
    first = repaired[0]
    return all(first.equals(other) for other in repaired[1:])


def choose_canonical_index(group: Any, areas: Sequence[float], *, prefer_state: str | None = None) -> Any:
    ranking = group.copy()
    ranking["_standard_area_km2"] = list(areas)
    if prefer_state:
        preferred = ranking[ranking["state_name"].map(normalize_label) == prefer_state]
        if not preferred.empty:
            ranking = preferred
    ranking = ranking.sort_values(
        by=["_standard_area_km2", "source_row_count", "source_identity_count"],
        ascending=[False, False, False],
        kind="mergesort",
    )
    return ranking.index[0]


def geometry_set_signature(group: Any) -> str:
    hashes = []
    for _, row in group.iterrows():
        shape_hash = clean_text(row.get("village_shape_hash"))
        if shape_hash is None:
            shape_hash = stable_shape_hash(repair_geometry(row.geometry))
        hashes.append(shape_hash or "")
    payload = "\n".join(sorted(hashes)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def is_cross_duplicate_instruction(note: str) -> bool:
    lowered = note.lower()
    return has_any(
        lowered,
        [
            "same identical group",
            "identical same parts",
            "repeated pattern",
            "identical feature rows",
            "same local urban",
            "seem to be 3 identical rows",
        ],
    )


def decision_for_group(
    group: Any,
    *,
    note_row: dict[str, Any] | None,
    dominant_share_threshold: float,
    near_distance_m: float,
    far_centroid_km: float,
    many_part_threshold: int,
) -> dict[str, Any]:
    geometries = [repair_geometry(geometry) for geometry in group.geometry.tolist()]
    areas = [geodesic_area_km2(geometry) for geometry in geometries]
    total_area = sum(areas)
    largest_area = max(areas) if areas else 0.0
    largest_share = largest_area / total_area if total_area > 0 else 0.0
    distances = group_distance_metrics(geometries)
    names = [normalize_label(value) for value in group["NAME"].tolist()]
    names = [value for value in names if value]
    name_count = len(set(names))
    states = {normalize_label(value) for value in group["state_name"].tolist() if normalize_label(value)}
    districts = {normalize_label(value) for value in group["district_name"].tolist() if normalize_label(value)}
    tehsils = {normalize_label(value) for value in group["TEHSIL"].tolist() if normalize_label(value)}
    geometry_hashes = set(group["village_shape_hash"].dropna().astype(str))
    note = note_text(note_row)
    note_action = classify_note_action(note)
    identical_geometries = all_geometries_equal(geometries) or (len(geometry_hashes) == 1 and len(group) > 1)

    pattern_flags = []
    if identical_geometries:
        pattern_flags.append("exact_duplicate_geometry")
    if largest_share >= dominant_share_threshold:
        pattern_flags.append("dominant_largest_part")
    if name_count > 1:
        pattern_flags.append("multiple_names")
    if len(states) > 1 or len(districts) > 1 or len(tehsils) > 1:
        pattern_flags.append("admin_variants")
    if distances["min_edge_distance_m"] is not None and distances["min_edge_distance_m"] <= near_distance_m:
        pattern_flags.append("near_or_touching_parts")
    if distances["max_centroid_distance_km"] is not None and distances["max_centroid_distance_km"] >= far_centroid_km:
        pattern_flags.append("far_apart_parts")
    if len(group) >= many_part_threshold:
        pattern_flags.append("many_part_group")
    if note:
        pattern_flags.append("manual_note")

    if note_action == "prefer_telangana_duplicate":
        action = "keep_preferred_duplicate"
        reason = "manual_note_prefers_telangana_over_outdated_duplicate"
        canonical_index = choose_canonical_index(group, areas, prefer_state="TELANGANA")
    elif identical_geometries and len(states) > 1 and "TELANGANA" in states:
        action = "keep_preferred_duplicate"
        reason = "identical_geometry_state_conflict_prefers_telangana"
        canonical_index = choose_canonical_index(group, areas, prefer_state="TELANGANA")
    elif identical_geometries:
        action = "keep_largest_duplicate"
        reason = "identical_geometry_duplicate"
        canonical_index = choose_canonical_index(group, areas)
    elif note_action == "keep_multipart":
        action = "keep_multipart"
        reason = "manual_note_keep_multipart"
        canonical_index = choose_canonical_index(group, areas)
    elif note_action == "merge_parts":
        action = "merge_parts"
        reason = "manual_note_merge"
        canonical_index = choose_canonical_index(group, areas)
    elif largest_share >= dominant_share_threshold:
        action = "merge_parts"
        reason = f"largest_part_share_ge_{dominant_share_threshold:g}"
        canonical_index = choose_canonical_index(group, areas)
    elif (
        distances["max_centroid_distance_km"] is not None
        and distances["max_centroid_distance_km"] >= far_centroid_km
    ):
        action = "keep_multipart"
        reason = f"far_apart_parts_ge_{far_centroid_km:g}km"
        canonical_index = choose_canonical_index(group, areas)
    elif name_count > 1 and len(states) <= 1 and len(districts) <= 1 and len(tehsils) <= 1:
        action = "keep_multipart"
        reason = "multiple_names_same_admin_context"
        canonical_index = choose_canonical_index(group, areas)
    elif len(group) >= many_part_threshold:
        action = "keep_multipart"
        reason = f"many_part_group_ge_{many_part_threshold}"
        canonical_index = choose_canonical_index(group, areas)
    elif (
        distances["min_edge_distance_m"] is not None
        and distances["min_edge_distance_m"] <= near_distance_m
    ):
        action = "merge_parts"
        reason = f"near_or_touching_parts_le_{near_distance_m:g}m"
        canonical_index = choose_canonical_index(group, areas)
    else:
        action = "keep_multipart"
        reason = "conservative_default_keep_multipart"
        canonical_index = choose_canonical_index(group, areas)

    return {
        "action": action,
        "reason": reason,
        "canonical_index": canonical_index,
        "areas_km2": areas,
        "total_area_km2": total_area,
        "largest_area_km2": largest_area,
        "largest_area_share": largest_share,
        "min_edge_distance_m": distances["min_edge_distance_m"],
        "min_centroid_distance_km": distances["min_centroid_distance_km"],
        "max_centroid_distance_km": distances["max_centroid_distance_km"],
        "name_count": name_count,
        "state_count": len(states),
        "district_count": len(districts),
        "tehsil_count": len(tehsils),
        "pattern_flags": pattern_flags,
        "note": note,
    }


def apply_forced_decision(decision: dict[str, Any], *, action: str, reason: str, flag: str) -> dict[str, Any]:
    updated = dict(decision)
    updated["action"] = action
    updated["reason"] = reason
    flags = list(updated.get("pattern_flags") or [])
    if flag not in flags:
        flags.append(flag)
    updated["pattern_flags"] = flags
    return updated


def aggregate_count(values: Any) -> int | None:
    numbers = pd.to_numeric(values, errors="coerce").dropna()
    return int(numbers.sum()) if not numbers.empty else None


def prepare_candidate_standard_columns(gdf: Any, *, action: str, reason: str) -> Any:
    output = gdf.copy()
    source_ids = output["cs_feature_id"].astype(str)
    output["_candidate_feature_ids"] = source_ids
    output["_source_identity_hashes"] = output.get("source_identity_hashes", "")
    output["_trace_status"] = "output"
    output["standard_group_key"] = [
        standard_group_key(village_id, fallback=source_id)
        for village_id, source_id in zip(output["pc11_village_id"], source_ids)
    ]
    output["cs_admin_uid"] = [
        standard_uid(village_id, part_index=1, part_count=1, fallback=source_id)
        for village_id, source_id in zip(output["pc11_village_id"], source_ids)
    ]
    output["cs_feature_id"] = output["cs_admin_uid"]
    output["core_admin_uid"] = output["cs_admin_uid"]
    output["village_id"] = pd.to_numeric(output["pc11_village_id"], errors="coerce").astype("Int64")
    output["standard_action"] = action
    output["standard_reason"] = reason
    output["standard_notes"] = ""
    output["standard_pattern_flags"] = ""
    output["standard_group_part_count"] = 1
    output["standard_part_area_km2"] = float("nan")
    output["standard_part_area_share"] = float("nan")
    output["standard_group_area_km2"] = float("nan")
    output["standard_min_edge_distance_m"] = float("nan")
    output["standard_min_centroid_distance_km"] = float("nan")
    output["standard_max_centroid_distance_km"] = float("nan")
    output["standard_name_count"] = 1
    return coerce_and_order_output(output)


def coerce_and_order_output(gdf: Any) -> Any:
    output = gdf.copy()
    internal_columns = [
        column for column in ["_candidate_feature_ids", "_source_identity_hashes", "_trace_status"]
        if column in output.columns
    ]
    for column in STANDARD_COLUMNS:
        if column not in output.columns:
            output[column] = None
    for column in ADMIN_INT_COLUMNS:
        if column in output.columns:
            output[column] = pd.to_numeric(output[column], errors="coerce").astype("Int64")
    for column in [
        "standard_part_area_km2",
        "standard_part_area_share",
        "standard_group_area_km2",
        "standard_min_edge_distance_m",
        "standard_min_centroid_distance_km",
        "standard_max_centroid_distance_km",
    ]:
        output[column] = pd.to_numeric(output[column], errors="coerce").astype("Float64")
    geometry_column = output.geometry.name
    return output[[*STANDARD_COLUMNS, *internal_columns, geometry_column]]


def public_output_gdf(gdf: Any) -> Any:
    geometry_column = gdf.geometry.name
    columns = [column for column in STANDARD_COLUMNS if column in gdf.columns]
    return gdf[[*columns, geometry_column]]


def make_output_row_from_series(row: Any, geometry: Any) -> dict[str, Any]:
    record = row.to_dict()
    record["geometry"] = geometry
    return record


def transform_haryana_800372_group(
    group: Any,
    *,
    note_row: dict[str, Any] | None,
) -> tuple[Any, list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    note = note_text(note_row)
    village_id = 800372
    target_candidate_feature_id = "village:haryana:kurukshetra:thanesar:800372:part:1"
    output_rows: list[dict[str, Any]] = []
    omission_rows: list[dict[str, Any]] = []
    part_rows: list[dict[str, Any]] = []
    areas = [geodesic_area_km2(repair_geometry(geometry)) for geometry in group.geometry.tolist()]
    total_area = sum(areas)
    distances = group_distance_metrics(group.geometry.tolist())
    canonical_matches = group[group["cs_feature_id"].astype(str) == target_candidate_feature_id]
    canonical_index = canonical_matches.index[0] if not canonical_matches.empty else choose_canonical_index(group, areas)

    for position, (index, source_row) in enumerate(group.iterrows()):
        area = float(areas[position])
        source_id = str(source_row["cs_feature_id"])
        geometry = repair_geometry(source_row.geometry)
        row = source_row.copy()
        row["_candidate_feature_ids"] = source_id
        row["_source_identity_hashes"] = source_row.get("source_identity_hashes", "")
        row["_trace_status"] = "output"
        is_canonical = index == canonical_index
        if is_canonical:
            output_id = standard_uid(village_id, part_index=1, part_count=1, fallback=source_id)
            row["pc11_village_id"] = village_id
            row["village_id"] = village_id
            row["cs_feature_id"] = output_id
            row["cs_admin_uid"] = output_id
            row["core_admin_uid"] = output_id
            row["standard_group_key"] = f"village:{village_id}"
            row["feature_part_index"] = 1
            row["feature_part_count"] = 1
            row["resolution_status"] = "standard_haryana_800372_kept_id_part"
            row["standard_action"] = "keep_selected_part_reclassify_rest"
            row["standard_reason"] = "manual_note_keep_only_kurukshetra_thanesar_part_1_with_village_id"
        else:
            shape_hash = stable_shape_hash(geometry) or hashlib.sha256(source_id.encode("utf-8")).hexdigest()
            output_id = f"invalid:{shape_hash[:24]}"
            row["pc11_village_id"] = pd.NA
            row["village_id"] = pd.NA
            row["cs_feature_id"] = output_id
            row["cs_admin_uid"] = output_id
            row["core_admin_uid"] = output_id
            row["standard_group_key"] = output_id
            row["feature_part_index"] = 1
            row["feature_part_count"] = 1
            row["resolution_status"] = "standard_reclassified_invalid_fragment"
            row["standard_action"] = "reclassify_fragment_without_village_id"
            row["standard_reason"] = "manual_note_remove_800372_from_nonselected_parts"
            omission_rows.append(
                {
                    "pc11_village_id": village_id,
                    "candidate_feature_id": source_id,
                    "omission_reason": "village_id_removed_but_geometry_retained_as_invalid_fragment",
                    "kept_candidate_feature_id": target_candidate_feature_id,
                    "standard_action": "reclassify_fragment_without_village_id",
                    "state_name": source_row["state_name"],
                    "district_name": source_row["district_name"],
                    "TEHSIL": source_row["TEHSIL"],
                    "NAME": source_row["NAME"],
                }
            )
        row["village_shape_hash"] = stable_shape_hash(geometry)
        row["standard_notes"] = note
        row["standard_pattern_flags"] = "manual_note|many_part_group|haryana_800372_reclassification"
        row["standard_group_part_count"] = len(group)
        row["standard_part_area_km2"] = round(area, 6)
        row["standard_part_area_share"] = round(area / total_area, 8) if total_area > 0 else None
        row["standard_group_area_km2"] = round(total_area, 6)
        row["standard_min_edge_distance_m"] = distances["min_edge_distance_m"]
        row["standard_min_centroid_distance_km"] = distances["min_centroid_distance_km"]
        row["standard_max_centroid_distance_km"] = distances["max_centroid_distance_km"]
        row["standard_name_count"] = len(set(normalize_label(value) for value in group["NAME"] if normalize_label(value)))
        output_rows.append(make_output_row_from_series(row, geometry))
        part_rows.append(
            {
                "pc11_village_id": village_id,
                "candidate_feature_id": source_id,
                "source_group_key": source_row["source_group_key"],
                "source_part_index": source_row["feature_part_index"],
                "source_part_count": source_row["feature_part_count"],
                "state_name": source_row["state_name"],
                "district_name": source_row["district_name"],
                "TEHSIL": source_row["TEHSIL"],
                "NAME": source_row["NAME"],
                "area_km2": round(area, 6),
                "area_share": round(area / total_area, 8) if total_area > 0 else None,
                "standard_action": row["standard_action"],
                "standard_reason": row["standard_reason"],
            }
        )

    output_gdf = gpd.GeoDataFrame(output_rows, geometry="geometry", crs=group.crs)
    output_gdf = coerce_and_order_output(output_gdf)
    decision_row = {
        "pc11_village_id": village_id,
        "input_rows": len(group),
        "output_rows": len(output_gdf),
        "omitted_rows": len(omission_rows),
        "standard_action": "keep_selected_part_reclassify_rest",
        "standard_reason": "manual_note_keep_only_kurukshetra_thanesar_part_1_with_village_id",
        "pattern_flags": "manual_note|many_part_group|haryana_800372_reclassification",
        "state_count": len(set(normalize_label(value) for value in group["state_name"] if normalize_label(value))),
        "district_count": len(set(normalize_label(value) for value in group["district_name"] if normalize_label(value))),
        "tehsil_count": len(set(normalize_label(value) for value in group["TEHSIL"] if normalize_label(value))),
        "name_count": len(set(normalize_label(value) for value in group["NAME"] if normalize_label(value))),
        "total_area_km2": round(total_area, 6),
        "largest_area_km2": round(max(areas), 6) if areas else 0,
        "largest_area_share": round(max(areas) / total_area, 8) if total_area > 0 and areas else 0,
        "min_edge_distance_m": distances["min_edge_distance_m"],
        "min_centroid_distance_km": distances["min_centroid_distance_km"],
        "max_centroid_distance_km": distances["max_centroid_distance_km"],
        "note": note,
        "candidate_feature_ids": "|".join(str(value) for value in group["cs_feature_id"].tolist()[:20])
        + (f"|...(+{len(group) - 20})" if len(group) > 20 else ""),
    }
    return output_gdf, omission_rows, part_rows, decision_row


def omitted_cross_duplicate_group_reports(
    group: Any,
    *,
    note_row: dict[str, Any] | None,
    kept_village_id: int | None,
    signature: str,
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    decision = decision_for_group(
        group,
        note_row=note_row,
        dominant_share_threshold=args.dominant_share_threshold,
        near_distance_m=args.near_distance_m,
        far_centroid_km=args.far_centroid_km,
        many_part_threshold=args.many_part_threshold,
    )
    village_id = clean_int(group["pc11_village_id"].iloc[0])
    omission_rows = []
    part_rows = []
    areas = decision["areas_km2"]
    total_area = float(decision["total_area_km2"] or 0.0)
    for position, (_, row) in enumerate(group.iterrows()):
        area = float(areas[position])
        input_id = str(row["cs_feature_id"])
        omission_rows.append(
            {
                "pc11_village_id": village_id,
                "candidate_feature_id": input_id,
                "omission_reason": "cross_village_duplicate_geometry_set",
                "kept_pc11_village_id": kept_village_id,
                "geometry_set_signature": signature,
                "standard_action": "omit_cross_duplicate_group",
                "state_name": row["state_name"],
                "district_name": row["district_name"],
                "TEHSIL": row["TEHSIL"],
                "NAME": row["NAME"],
            }
        )
        part_rows.append(
            {
                "pc11_village_id": village_id,
                "candidate_feature_id": input_id,
                "source_group_key": row["source_group_key"],
                "source_part_index": row["feature_part_index"],
                "source_part_count": row["feature_part_count"],
                "state_name": row["state_name"],
                "district_name": row["district_name"],
                "TEHSIL": row["TEHSIL"],
                "NAME": row["NAME"],
                "area_km2": round(area, 6),
                "area_share": round(area / total_area, 8) if total_area > 0 else None,
                "standard_action": "omit_cross_duplicate_group",
                "standard_reason": "cross_village_duplicate_geometry_set",
            }
        )
    flags = list(decision.get("pattern_flags") or [])
    for flag in ["cross_village_duplicate_geometry_set", "omitted_duplicate_group"]:
        if flag not in flags:
            flags.append(flag)
    decision_row = {
        "pc11_village_id": village_id,
        "input_rows": len(group),
        "output_rows": 0,
        "omitted_rows": len(group),
        "standard_action": "omit_cross_duplicate_group",
        "standard_reason": "cross_village_duplicate_geometry_set",
        "pattern_flags": "|".join(flags),
        "state_count": decision["state_count"],
        "district_count": decision["district_count"],
        "tehsil_count": decision["tehsil_count"],
        "name_count": decision["name_count"],
        "total_area_km2": round(float(decision["total_area_km2"] or 0.0), 6),
        "largest_area_km2": round(float(decision["largest_area_km2"] or 0.0), 6),
        "largest_area_share": round(float(decision["largest_area_share"] or 0.0), 8),
        "min_edge_distance_m": decision["min_edge_distance_m"],
        "min_centroid_distance_km": decision["min_centroid_distance_km"],
        "max_centroid_distance_km": decision["max_centroid_distance_km"],
        "note": decision["note"],
        "geometry_set_signature": signature,
        "kept_pc11_village_id": kept_village_id,
        "candidate_feature_ids": "|".join(str(value) for value in group["cs_feature_id"].tolist()[:20])
        + (f"|...(+{len(group) - 20})" if len(group) > 20 else ""),
    }
    return omission_rows, part_rows, decision_row


def transform_multipart_group(
    group: Any,
    *,
    note_row: dict[str, Any] | None,
    args: argparse.Namespace,
    forced_action: str | None = None,
    forced_reason: str | None = None,
    forced_flag: str | None = None,
) -> tuple[Any, list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    if clean_int(group["pc11_village_id"].iloc[0]) == 800372 or "rest should keep other values" in note_text(note_row).lower():
        output_gdf, omissions, parts, decision = transform_haryana_800372_group(group, note_row=note_row)
        return output_gdf, omissions, parts, decision, []

    decision = decision_for_group(
        group,
        note_row=note_row,
        dominant_share_threshold=args.dominant_share_threshold,
        near_distance_m=args.near_distance_m,
        far_centroid_km=args.far_centroid_km,
        many_part_threshold=args.many_part_threshold,
    )
    if forced_action and forced_reason and forced_flag:
        decision = apply_forced_decision(
            decision,
            action=forced_action,
            reason=forced_reason,
            flag=forced_flag,
        )
    source_ids = [str(value) for value in group["cs_feature_id"].tolist()]
    source_identity_hashes = [
        value
        for value in group.get("source_identity_hashes", pd.Series(dtype=str)).dropna().astype(str).tolist()
        if value
    ]
    village_id = clean_int(group["pc11_village_id"].iloc[0])
    fallback_group = str(group["source_group_key"].iloc[0])
    group_key = standard_group_key(village_id, fallback=fallback_group)
    output_rows: list[dict[str, Any]] = []
    omission_rows: list[dict[str, Any]] = []
    part_rows: list[dict[str, Any]] = []
    areas = decision["areas_km2"]
    total_area = float(decision["total_area_km2"] or 0.0)

    for position, (index, row) in enumerate(group.iterrows()):
        area = float(areas[position])
        part_rows.append(
            {
                "pc11_village_id": village_id,
                "candidate_feature_id": row["cs_feature_id"],
                "source_group_key": row["source_group_key"],
                "source_part_index": row["feature_part_index"],
                "source_part_count": row["feature_part_count"],
                "state_name": row["state_name"],
                "district_name": row["district_name"],
                "TEHSIL": row["TEHSIL"],
                "NAME": row["NAME"],
                "area_km2": round(area, 6),
                "area_share": round(area / total_area, 8) if total_area > 0 else None,
                "standard_action": decision["action"],
                "standard_reason": decision["reason"],
            }
        )

    canonical = group.loc[decision["canonical_index"]].copy()
    canonical_geometry = repair_geometry(canonical.geometry)
    pattern_flags = "|".join(decision["pattern_flags"])

    if decision["action"] == "merge_parts":
        geometries = [repair_geometry(geometry) for geometry in group.geometry.tolist()]
        geometries = [geometry for geometry in geometries if geometry is not None]
        merged_geometry = repair_geometry(unary_union(geometries)) if geometries else canonical_geometry
        output_id = standard_uid(village_id, part_index=1, part_count=1, fallback=str(canonical["cs_feature_id"]))
        canonical["cs_feature_id"] = output_id
        canonical["_candidate_feature_ids"] = "|".join(source_ids)
        canonical["_source_identity_hashes"] = "|".join(source_identity_hashes)
        canonical["_trace_status"] = "output"
        canonical["cs_admin_uid"] = output_id
        canonical["core_admin_uid"] = output_id
        canonical["standard_group_key"] = group_key
        canonical["feature_part_index"] = 1
        canonical["feature_part_count"] = 1
        canonical["village_shape_hash"] = stable_shape_hash(merged_geometry)
        canonical["village_id"] = village_id
        canonical["source_row_count"] = aggregate_count(group["source_row_count"])
        canonical["source_identity_count"] = aggregate_count(group["source_identity_count"])
        canonical["source_geometry_count"] = len(set(group["village_shape_hash"].dropna().astype(str)))
        canonical["source_file_count"] = aggregate_count(group["source_file_count"])
        canonical["resolution_status"] = "standard_merged_parts"
        canonical["standard_action"] = decision["action"]
        canonical["standard_reason"] = decision["reason"]
        canonical["standard_notes"] = decision["note"]
        canonical["standard_pattern_flags"] = pattern_flags
        canonical["standard_group_part_count"] = len(group)
        canonical["standard_part_area_km2"] = round(total_area, 6)
        canonical["standard_part_area_share"] = 1.0
        canonical["standard_group_area_km2"] = round(total_area, 6)
        canonical["standard_min_edge_distance_m"] = decision["min_edge_distance_m"]
        canonical["standard_min_centroid_distance_km"] = decision["min_centroid_distance_km"]
        canonical["standard_max_centroid_distance_km"] = decision["max_centroid_distance_km"]
        canonical["standard_name_count"] = decision["name_count"]
        output_rows.append(make_output_row_from_series(canonical, merged_geometry))
    elif decision["action"] in {"keep_preferred_duplicate", "keep_largest_duplicate"}:
        output_id = standard_uid(village_id, part_index=1, part_count=1, fallback=str(canonical["cs_feature_id"]))
        source_id = str(canonical["cs_feature_id"])
        canonical["cs_feature_id"] = output_id
        canonical["_candidate_feature_ids"] = source_id
        canonical["_source_identity_hashes"] = canonical.get("source_identity_hashes", "")
        canonical["_trace_status"] = "output"
        canonical["cs_admin_uid"] = output_id
        canonical["core_admin_uid"] = output_id
        canonical["standard_group_key"] = group_key
        canonical["feature_part_index"] = 1
        canonical["feature_part_count"] = 1
        canonical["village_shape_hash"] = stable_shape_hash(canonical_geometry)
        canonical["village_id"] = village_id
        canonical["resolution_status"] = "standard_collapsed_duplicate"
        canonical["standard_action"] = decision["action"]
        canonical["standard_reason"] = decision["reason"]
        canonical["standard_notes"] = decision["note"]
        canonical["standard_pattern_flags"] = pattern_flags
        canonical["standard_group_part_count"] = len(group)
        canonical["standard_part_area_km2"] = round(max(areas), 6) if areas else None
        canonical["standard_part_area_share"] = 1.0
        canonical["standard_group_area_km2"] = round(total_area, 6)
        canonical["standard_min_edge_distance_m"] = decision["min_edge_distance_m"]
        canonical["standard_min_centroid_distance_km"] = decision["min_centroid_distance_km"]
        canonical["standard_max_centroid_distance_km"] = decision["max_centroid_distance_km"]
        canonical["standard_name_count"] = decision["name_count"]
        output_rows.append(make_output_row_from_series(canonical, canonical_geometry))
        for _, row in group.drop(index=decision["canonical_index"]).iterrows():
            omission_rows.append(
                {
                    "pc11_village_id": village_id,
                    "candidate_feature_id": row["cs_feature_id"],
                    "omission_reason": decision["reason"],
                    "kept_candidate_feature_id": source_id,
                    "standard_action": decision["action"],
                    "state_name": row["state_name"],
                    "district_name": row["district_name"],
                    "TEHSIL": row["TEHSIL"],
                    "NAME": row["NAME"],
                }
            )
    else:
        ranked = group.copy()
        ranked["_standard_area_km2"] = areas
        ranked = ranked.sort_values(
            by=["_standard_area_km2", "cs_feature_id"],
            ascending=[False, True],
            kind="mergesort",
        )
        part_count = len(ranked)
        for part_index, (_, row) in enumerate(ranked.iterrows(), start=1):
            geometry = repair_geometry(row.geometry)
            source_id = str(row["cs_feature_id"])
            output_id = standard_uid(village_id, part_index=part_index, part_count=part_count, fallback=source_id)
            row = row.copy()
            row["cs_feature_id"] = output_id
            row["_candidate_feature_ids"] = source_id
            row["_source_identity_hashes"] = row.get("source_identity_hashes", "")
            row["_trace_status"] = "output"
            row["cs_admin_uid"] = output_id
            row["core_admin_uid"] = output_id
            row["standard_group_key"] = group_key
            row["feature_part_index"] = part_index
            row["feature_part_count"] = part_count
            row["village_shape_hash"] = stable_shape_hash(geometry)
            row["village_id"] = village_id
            row["resolution_status"] = "standard_kept_multipart"
            row["standard_action"] = "keep_multipart"
            row["standard_reason"] = decision["reason"]
            row["standard_notes"] = decision["note"]
            row["standard_pattern_flags"] = pattern_flags
            row["standard_group_part_count"] = part_count
            area = float(row["_standard_area_km2"])
            row["standard_part_area_km2"] = round(area, 6)
            row["standard_part_area_share"] = round(area / total_area, 8) if total_area > 0 else None
            row["standard_group_area_km2"] = round(total_area, 6)
            row["standard_min_edge_distance_m"] = decision["min_edge_distance_m"]
            row["standard_min_centroid_distance_km"] = decision["min_centroid_distance_km"]
            row["standard_max_centroid_distance_km"] = decision["max_centroid_distance_km"]
            row["standard_name_count"] = decision["name_count"]
            output_rows.append(make_output_row_from_series(row, geometry))

    output_gdf = gpd.GeoDataFrame(output_rows, geometry="geometry", crs=group.crs)
    output_gdf = coerce_and_order_output(output_gdf)
    decision_row = {
        "pc11_village_id": village_id,
        "input_rows": len(group),
        "output_rows": len(output_gdf),
        "omitted_rows": len(omission_rows),
        "standard_action": decision["action"],
        "standard_reason": decision["reason"],
        "pattern_flags": pattern_flags,
        "state_count": decision["state_count"],
        "district_count": decision["district_count"],
        "tehsil_count": decision["tehsil_count"],
        "name_count": decision["name_count"],
        "total_area_km2": round(float(decision["total_area_km2"] or 0.0), 6),
        "largest_area_km2": round(float(decision["largest_area_km2"] or 0.0), 6),
        "largest_area_share": round(float(decision["largest_area_share"] or 0.0), 8),
        "min_edge_distance_m": decision["min_edge_distance_m"],
        "min_centroid_distance_km": decision["min_centroid_distance_km"],
        "max_centroid_distance_km": decision["max_centroid_distance_km"],
        "note": decision["note"],
        "candidate_feature_ids": "|".join(source_ids[:20]) + (f"|...(+{len(source_ids) - 20})" if len(source_ids) > 20 else ""),
    }
    return output_gdf, omission_rows, part_rows, decision_row, []


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


class OutputWriters:
    def __init__(self, args: argparse.Namespace, *, trace_connection: sqlite3.Connection):
        self.args = args
        self.trace_connection = trace_connection
        self.first_gpkg_chunk = True
        self.first_csv_chunk = True
        self.rows_written = 0
        self.geojson_writer = None

    def __enter__(self):
        if not self.args.skip_geojson:
            self.geojson_writer = ChunkedGeoJSONWriter(repo_path(self.args.output_geojson)).__enter__()
        return self

    def write(self, gdf: Any) -> None:
        if gdf.empty:
            return
        gdf = coerce_and_order_output(gdf)
        self.record_trace(gdf)
        public_gdf = public_output_gdf(gdf)
        output_gpkg = repo_path(self.args.output_gpkg)
        pyogrio.write_dataframe(
            public_gdf,
            output_gpkg,
            layer=self.args.output_layer,
            driver="GPKG",
            append=not self.first_gpkg_chunk,
            promote_to_multi=True,
        )
        self.first_gpkg_chunk = False
        if self.geojson_writer is not None:
            self.geojson_writer.write(public_gdf)
        if not self.args.skip_csv:
            csv_path = repo_path(self.args.output_csv)
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            frame = pd.DataFrame(public_gdf.drop(columns=[public_gdf.geometry.name]))
            frame.to_csv(
                csv_path,
                mode="w" if self.first_csv_chunk else "a",
                header=self.first_csv_chunk,
                index=False,
            )
            self.first_csv_chunk = False
        self.rows_written += len(public_gdf)

    def record_trace(self, gdf: Any) -> None:
        rows = []
        for _, row in gdf.iterrows():
            identity_hashes = [
                value
                for value in str(row.get("_source_identity_hashes") or "").split("|")
                if value
            ]
            candidate_ids = clean_text(row.get("_candidate_feature_ids"))
            trace_status = clean_text(row.get("_trace_status")) or "output"
            reason = clean_text(row.get("standard_reason"))
            for identity_hash in identity_hashes:
                rows.append(
                    (
                        row["cs_feature_id"],
                        trace_status,
                        candidate_ids,
                        identity_hash,
                        reason,
                    )
                )
        if rows:
            self.trace_connection.executemany(
                """
                INSERT INTO standard_trace_map (
                    cs_feature_id,
                    trace_status,
                    candidate_feature_id,
                    identity_hash,
                    standard_reason
                ) VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )
            self.trace_connection.commit()

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.geojson_writer is not None:
            self.geojson_writer.__exit__(exc_type, exc, traceback)


def validate_output_path(path: Path, *, overwrite: bool, label: str) -> None:
    if path.exists():
        if not overwrite:
            raise SystemExit(f"{label} already exists: {path}. Pass --overwrite to rebuild it.")
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)


def prepare_outputs(args: argparse.Namespace) -> None:
    validate_output_path(repo_path(args.output_gpkg), overwrite=args.overwrite, label="GeoPackage")
    if not args.skip_geojson:
        validate_output_path(repo_path(args.output_geojson), overwrite=args.overwrite, label="GeoJSON")
    if not args.skip_csv:
        validate_output_path(repo_path(args.output_csv), overwrite=args.overwrite, label="CSV")


def ensure_gpkg_indexes(gpkg_path: Path, layer: str) -> None:
    if not gpkg_path.exists():
        return
    index_columns = [
        ["cs_feature_id"],
        ["cs_admin_uid"],
        ["core_admin_uid"],
        ["pc11_village_id"],
        ["village_id"],
        ["state_name", "district_name", "TEHSIL"],
        ["pc11_state_id", "pc11_district_id", "pc11_subdistrict_id"],
    ]
    with sqlite3.connect(gpkg_path) as connection:
        for columns in index_columns:
            suffix = "_".join(column.lower() for column in columns)
            index_name = f"idx_{layer}_{suffix}"
            columns_sql = ", ".join(quote_identifier(column) for column in columns)
            connection.execute(
                f"CREATE INDEX IF NOT EXISTS {quote_identifier(index_name)} "
                f"ON {quote_identifier(layer)} ({columns_sql})"
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


def insert_review_ids(connection: sqlite3.Connection, review_ids: set[int]) -> None:
    connection.execute("DROP TABLE IF EXISTS standard_review_village_ids")
    connection.execute("CREATE TEMP TABLE standard_review_village_ids (village_id INTEGER PRIMARY KEY)")
    connection.executemany(
        "INSERT INTO standard_review_village_ids (village_id) VALUES (?)",
        [(value,) for value in sorted(review_ids)],
    )
    connection.commit()


def collect_review_village_ids(connection: sqlite3.Connection, notes: dict[int, dict[str, Any]]) -> set[int]:
    review_ids = set(notes)
    for row in connection.execute(
        """
        SELECT pc11_village_id
        FROM candidate_features
        WHERE pc11_village_id IS NOT NULL
        GROUP BY pc11_village_id
        HAVING COUNT(*) > 1
        """
    ):
        village_id = clean_int(row["pc11_village_id"])
        if village_id is not None:
            review_ids.add(village_id)
    return review_ids


def iter_singleton_candidate_chunks(
    connection: sqlite3.Connection,
    *,
    chunk_size: int,
) -> Iterable[Any]:
    offset = 0
    while True:
        rows = connection.execute(
            """
            SELECT *
            FROM candidate_features
            WHERE pc11_village_id IS NULL
               OR pc11_village_id NOT IN (
                   SELECT village_id FROM standard_review_village_ids
               )
            ORDER BY
                pc11_state_id,
                pc11_district_id,
                pc11_subdistrict_id,
                pc11_village_id,
                candidate_feature_id
            LIMIT ? OFFSET ?
            """,
            (chunk_size, offset),
        ).fetchall()
        if not rows:
            break
        yield candidate_rows_to_gdf(rows)
        offset += len(rows)


def load_review_candidate_groups(
    connection: sqlite3.Connection,
    notes: dict[int, dict[str, Any]],
) -> list[tuple[int | None, Any, dict[str, Any] | None, str]]:
    rows = connection.execute(
        """
        SELECT *
        FROM candidate_features
        WHERE pc11_village_id IN (
            SELECT village_id FROM standard_review_village_ids
        )
        ORDER BY pc11_village_id, candidate_feature_id
        """
    ).fetchall()
    gdf = (
        candidate_rows_to_gdf(rows)
        if rows
        else gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs="EPSG:4326")
    )
    groups = []
    if gdf.empty:
        return groups
    gdf["pc11_village_id"] = pd.to_numeric(gdf["pc11_village_id"], errors="coerce").astype("Int64")
    for village_id, group in gdf.groupby("pc11_village_id", dropna=False, sort=True):
        clean_village_id = clean_int(village_id)
        note_row = notes.get(clean_village_id) if clean_village_id is not None else None
        groups.append((clean_village_id, group.copy(), note_row, geometry_set_signature(group)))
    return groups


def duplicate_signature_plan(
    groups: list[tuple[int | None, Any, dict[str, Any] | None, str]]
) -> tuple[dict[str, int | None], set[str], set[str]]:
    by_signature: dict[str, list[tuple[int | None, Any, dict[str, Any] | None, str]]] = {}
    for item in groups:
        by_signature.setdefault(item[3], []).append(item)
    keep_by_signature: dict[str, int | None] = {}
    duplicate_signatures: set[str] = set()
    force_merge_signatures: set[str] = set()
    for signature, items in by_signature.items():
        if len(items) <= 1:
            continue
        notes = [note_text(item[2]) for item in items]
        if not any(is_cross_duplicate_instruction(note) for note in notes):
            continue
        duplicate_signatures.add(signature)
        sorted_items = sorted(items, key=lambda item: (item[0] is None, item[0] or 0))
        keep_by_signature[signature] = sorted_items[0][0]
        if any(
            has_any(
                note.lower(),
                [
                    "keep one row with all parts together",
                    "identical feature rows",
                    "seem to be 3 identical rows",
                ],
            )
            for note in notes
        ):
            force_merge_signatures.add(signature)
    return keep_by_signature, duplicate_signatures, force_merge_signatures


def record_omitted_candidate_trace(
    connection: sqlite3.Connection,
    omission_rows: Sequence[dict[str, Any]],
) -> None:
    rows = []
    for omission in omission_rows:
        if omission.get("standard_action") == "reclassify_fragment_without_village_id":
            continue
        candidate_id = clean_text(omission.get("candidate_feature_id"))
        if candidate_id is None:
            continue
        candidate = connection.execute(
            """
            SELECT source_identity_hashes
            FROM candidate_features
            WHERE candidate_feature_id = ?
            """,
            (candidate_id,),
        ).fetchone()
        if candidate is None:
            continue
        for identity_hash in str(candidate["source_identity_hashes"] or "").split("|"):
            if identity_hash:
                rows.append(
                    (
                        None,
                        "omitted",
                        candidate_id,
                        identity_hash,
                        clean_text(omission.get("omission_reason")),
                    )
                )
    if rows:
        connection.executemany(
            """
            INSERT INTO standard_trace_map (
                cs_feature_id,
                trace_status,
                candidate_feature_id,
                identity_hash,
                standard_reason
            ) VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        connection.commit()


def write_source_trace_table(output_gpkg: Path, work_connection: sqlite3.Connection) -> int:
    trace_rows = work_connection.execute(
        """
        SELECT
            tm.cs_feature_id,
            tm.trace_status,
            tm.candidate_feature_id,
            tm.standard_reason,
            sr.source_file,
            sr.source_row_number,
            sr.source_state_slug,
            sr.source_district_slug,
            sr.identity_hash,
            sr.geometry_hash,
            sr.pc11_village_id,
            sr.state_name,
            sr.district_name,
            sr.TEHSIL,
            sr.NAME,
            sr.pc11_state_id,
            sr.pc11_district_id,
            sr.pc11_subdistrict_id
        FROM standard_trace_map tm
        JOIN source_rows sr ON sr.identity_hash = tm.identity_hash
        ORDER BY tm.cs_feature_id, tm.trace_status, sr.source_file, sr.source_row_number
        """
    ).fetchall()
    with sqlite3.connect(output_gpkg) as output_connection:
        output_connection.execute("DROP TABLE IF EXISTS cs_admin_standard_source_trace")
        output_connection.execute(
            """
            CREATE TABLE cs_admin_standard_source_trace (
                cs_feature_id TEXT,
                trace_status TEXT NOT NULL,
                candidate_feature_id TEXT,
                standard_reason TEXT,
                source_file TEXT NOT NULL,
                source_row_number INTEGER NOT NULL,
                source_state_slug TEXT NOT NULL,
                source_district_slug TEXT NOT NULL,
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
            )
            """
        )
        output_connection.executemany(
            """
            INSERT INTO cs_admin_standard_source_trace (
                cs_feature_id,
                trace_status,
                candidate_feature_id,
                standard_reason,
                source_file,
                source_row_number,
                source_state_slug,
                source_district_slug,
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [tuple(row) for row in trace_rows],
        )
        output_connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_cs_admin_standard_source_trace_feature "
            "ON cs_admin_standard_source_trace (cs_feature_id)"
        )
        output_connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_cs_admin_standard_source_trace_source "
            "ON cs_admin_standard_source_trace (source_file, source_row_number)"
        )
        output_connection.commit()
    return len(trace_rows)


def write_source_file_audit_report(reports_dir: Path, connection: sqlite3.Connection) -> None:
    dataframe = pd.read_sql_query(
        """
        SELECT *
        FROM source_file_audit
        ORDER BY source_file
        """,
        connection,
    )
    dataframe.to_csv(reports_dir / "source_file_read_audit.csv", index=False)


def max_rss_mb() -> float:
    # Linux reports ru_maxrss in KiB.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def write_reports(
    reports_dir: Path,
    *,
    metadata: dict[str, Any],
    summary: dict[str, Any],
    decision_rows: list[dict[str, Any]],
    omission_rows: list[dict[str, Any]],
    part_rows: list[dict[str, Any]],
) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "build_summary.json").write_text(
        json_dumps({"metadata": metadata, "summary": summary}, indent=True),
        encoding="utf-8",
    )
    pd.DataFrame(decision_rows).to_csv(reports_dir / "standard_decision_log.csv", index=False)
    pd.DataFrame(omission_rows).to_csv(reports_dir / "standard_omissions.csv", index=False)
    pd.DataFrame(part_rows).to_csv(reports_dir / "standard_multipart_parts.csv", index=False)

    pattern_counts = Counter()
    for row in decision_rows:
        for flag in str(row.get("pattern_flags") or "").split("|"):
            if flag:
                pattern_counts[flag] += 1
    pd.DataFrame(
        [{"pattern_flag": key, "groups": value} for key, value in sorted(pattern_counts.items())]
    ).to_csv(reports_dir / "standard_pattern_summary.csv", index=False)

    action_lines = [
        f"- `{key}`: `{value:,}`"
        for key, value in sorted(summary.get("standard_action_counts", {}).items())
    ] or ["- None"]
    pattern_lines = [
        f"- `{key}`: `{value:,}`"
        for key, value in sorted(pattern_counts.items())
    ] or ["- None"]
    report = f"""# CS Admin Standard Build Summary

Generated by `utilities/scripts/admin_assets/build_cs_admin_boundary_standard.py`.

## Outputs

- GeoPackage: `{metadata['output_gpkg']}`
- Layer: `{metadata['output_layer']}`
- GeoJSON: `{metadata['output_geojson']}`
- CSV: `{metadata['output_csv']}`

## Counts

- Input features scanned: `{summary['input_features']:,}`
- Raw source identities: `{summary['source_identities']:,}`
- Candidate features after raw duplicate/overlap resolution: `{summary['candidate_features']:,}`
- Village ids held for note/multipart review: `{summary['review_village_ids']:,}`
- Unchanged singleton features written: `{summary['singleton_features_written']:,}`
- Standard output features written: `{summary['output_features']:,}`
- Multipart groups evaluated: `{summary['multipart_groups']:,}`
- Omission/reclassification report rows: `{summary['omitted_features']:,}`
- Raw source trace rows in GeoPackage: `{summary['trace_rows']:,}`
- Peak script RSS: `{summary['max_rss_mb']:.1f} MB`

## Multipart Actions

{chr(10).join(action_lines)}

## Pattern Flags

{chr(10).join(pattern_lines)}

## Decision Rules

- Raw boundary features are read from `{metadata['admin_input_dir']}`.
- Manual notes are read from `{metadata['multipart_analysis_csv']}` with an encoding fallback.
- Exact duplicate geometries collapse to one feature.
- Telangana is preferred when a manual note or identical-geometry state conflict identifies old Andhra/Telangana duplicates.
- Groups with a dominant largest part are dissolved unless manual notes explicitly preserve a valid multipart case.
- Lower-confidence groups remain multipart with part indexes sorted by descending area.
- Raw source trace is stored in `cs_admin_standard_source_trace` inside the GeoPackage.
"""
    (reports_dir / "build_summary.md").write_text(report, encoding="utf-8")


def build_standard_asset(args: argparse.Namespace) -> dict[str, Any]:
    ensure_dependencies()
    started = time.perf_counter()
    admin_input_dir = repo_path(args.admin_input_dir)
    multipart_analysis_csv = repo_path(args.multipart_analysis_csv)
    output_gpkg = repo_path(args.output_gpkg)
    output_geojson = repo_path(args.output_geojson)
    output_csv = repo_path(args.output_csv)
    reports_dir = repo_path(args.reports_dir)

    if not admin_input_dir.exists():
        raise SystemExit(f"Admin input directory not found: {admin_input_dir}")
    notes = load_multipart_notes(multipart_analysis_csv)
    files = list_geojson_files(
        admin_input_dir,
        state=args.state,
        district=args.district,
        limit_files=args.limit_files,
    )
    if not files:
        raise SystemExit(f"No district GeoJSON files found under {admin_input_dir}")

    metadata = {
        "admin_input_dir": admin_input_dir.as_posix(),
        "multipart_analysis_csv": multipart_analysis_csv.as_posix(),
        "output_gpkg": output_gpkg.as_posix(),
        "output_layer": args.output_layer,
        "output_geojson": None if args.skip_geojson else output_geojson.as_posix(),
        "output_csv": None if args.skip_csv else output_csv.as_posix(),
        "reports_dir": reports_dir.as_posix(),
        "source_file_count": len(files),
        "state_filter": args.state,
        "district_filter": args.district,
        "limit_files": args.limit_files,
        "chunk_size": args.chunk_size,
        "dominant_share_threshold": args.dominant_share_threshold,
        "near_distance_m": args.near_distance_m,
        "far_centroid_km": args.far_centroid_km,
        "many_part_threshold": args.many_part_threshold,
        "work_db": repo_path(args.work_db).as_posix() if args.work_db else None,
    }

    if args.validate_only:
        validation = {
            **metadata,
            "multipart_note_rows": len(notes),
            "first_source_files": [
                path.relative_to(ROOT_DIR).as_posix() if path.is_relative_to(ROOT_DIR) else path.as_posix()
                for path in files[:5]
            ],
        }
        print(json_dumps(validation, indent=True), flush=True)
        return validation

    prepare_outputs(args)
    reports_dir.mkdir(parents=True, exist_ok=True)

    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    if args.work_db:
        work_db = repo_path(args.work_db)
    elif args.keep_work_db:
        work_db = reports_dir / "cs_admin_standard_work.sqlite3"
    else:
        temp_dir = tempfile.TemporaryDirectory(prefix="cs_admin_standard_")
        work_db = Path(temp_dir.name) / "cs_admin_standard_work.sqlite3"

    connection = open_work_connection(work_db, overwrite=True)
    try:
        create_work_schema(connection)
        print(f"[standard] ingesting {len(files):,} raw admin GeoJSON files", flush=True)
        audits = []
        for index, path in enumerate(files, start=1):
            audit = ingest_source_file(connection, path)
            audits.append(audit)
            if index == 1 or index % 25 == 0 or index == len(files):
                print(
                    f"[standard] ingested {index:,}/{len(files):,}; "
                    f"latest={path.parent.name}/{path.name}; "
                    f"rows={audit['rows_read']:,}; status={audit['read_status']}; "
                    f"rss={max_rss_mb():.1f} MB",
                    flush=True,
                )

        print("[standard] resolving raw identities into candidate features", flush=True)
        candidate_summary = build_candidate_features(connection)
        print(
            f"[standard] candidate features: {candidate_summary['candidate_features']:,}; "
            f"rss={max_rss_mb():.1f} MB",
            flush=True,
        )

        review_ids = collect_review_village_ids(connection, notes)
        insert_review_ids(connection, review_ids)
        review_groups = load_review_candidate_groups(connection, notes)
        keep_by_signature, duplicate_signatures, force_merge_signatures = duplicate_signature_plan(review_groups)

        decision_rows: list[dict[str, Any]] = []
        omission_rows: list[dict[str, Any]] = []
        part_rows: list[dict[str, Any]] = []
        action_counts: Counter[str] = Counter()
        singleton_features_written = 0
        transformed_rows = 0
        omitted_duplicate_groups = 0
        print(
            f"[standard] writing singleton candidate features; "
            f"{len(review_ids):,} village ids held for note/multipart review",
            flush=True,
        )

        with OutputWriters(args, trace_connection=connection) as writers:
            for chunk_index, singleton in enumerate(
                iter_singleton_candidate_chunks(connection, chunk_size=args.chunk_size),
                start=1,
            ):
                singleton = prepare_candidate_standard_columns(
                    singleton,
                    action="kept_single",
                    reason="single_candidate_from_raw_inputs",
                )
                writers.write(singleton)
                singleton_features_written += len(singleton)
                if chunk_index == 1 or singleton_features_written % (args.chunk_size * 2) == 0:
                    print(
                        f"[standard] singleton features written={singleton_features_written:,}; "
                        f"rss={max_rss_mb():.1f} MB",
                        flush=True,
                    )

            print(f"[standard] applying note/multipart rules to {len(review_groups):,} groups", flush=True)
            for group_index, (clean_village_id, group, note_row, signature) in enumerate(review_groups, start=1):
                forced_action = None
                forced_reason = None
                forced_flag = None
                if signature in duplicate_signatures:
                    keep_village_id = keep_by_signature.get(signature)
                    if clean_village_id != keep_village_id:
                        omissions, parts, decision = omitted_cross_duplicate_group_reports(
                            group.copy(),
                            note_row=note_row,
                            kept_village_id=keep_village_id,
                            signature=signature,
                            args=args,
                        )
                        decision_rows.append(decision)
                        omission_rows.extend(omissions)
                        part_rows.extend(parts)
                        record_omitted_candidate_trace(connection, omissions)
                        action_counts[decision["standard_action"]] += 1
                        omitted_duplicate_groups += 1
                        if group_index == 1 or group_index % 500 == 0 or group_index == len(review_groups):
                            print(
                                f"[standard] review groups {group_index:,}/{len(review_groups):,}; "
                                f"transformed_rows={transformed_rows:,}; omitted_duplicate_groups={omitted_duplicate_groups:,}; "
                                f"rss={max_rss_mb():.1f} MB",
                                flush=True,
                            )
                        continue
                    if signature in force_merge_signatures:
                        forced_action = "merge_parts"
                        forced_reason = "cross_village_duplicate_geometry_set_keep_one_row_with_all_parts"
                        forced_flag = "cross_village_duplicate_canonical"

                output_gdf, omissions, parts, decision, _ = transform_multipart_group(
                    group.copy(),
                    note_row=note_row,
                    args=args,
                    forced_action=forced_action,
                    forced_reason=forced_reason,
                    forced_flag=forced_flag,
                )
                writers.write(output_gdf)
                transformed_rows += len(output_gdf)
                decision_rows.append(decision)
                omission_rows.extend(omissions)
                part_rows.extend(parts)
                record_omitted_candidate_trace(connection, omissions)
                action_counts[decision["standard_action"]] += 1
                if group_index == 1 or group_index % 500 == 0 or group_index == len(review_groups):
                    print(
                        f"[standard] review groups {group_index:,}/{len(review_groups):,}; "
                        f"transformed_rows={transformed_rows:,}; omitted_duplicate_groups={omitted_duplicate_groups:,}; "
                        f"rss={max_rss_mb():.1f} MB",
                        flush=True,
                    )

        print("[standard] creating source trace table inside output GeoPackage", flush=True)
        trace_rows = write_source_trace_table(output_gpkg, connection)
        ensure_gpkg_indexes(output_gpkg, args.output_layer)
        write_source_file_audit_report(reports_dir, connection)
        source_counts = {
            "input_features": connection.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0],
            "source_identities": connection.execute("SELECT COUNT(*) FROM identities").fetchone()[0],
            "candidate_features": connection.execute("SELECT COUNT(*) FROM candidate_features").fetchone()[0],
            "trace_rows": trace_rows,
        }
        summary = {
            **source_counts,
            **candidate_summary,
            "source_file_count": len(files),
            "read_error_files": sum(1 for audit in audits if audit["read_status"] != "ok"),
            "review_village_ids": len(review_ids),
            "multipart_groups": len(review_groups),
            "singleton_features_written": singleton_features_written,
            "multipart_output_features": transformed_rows,
            "omitted_features": len(omission_rows),
            "omitted_duplicate_groups": omitted_duplicate_groups,
            "output_features": singleton_features_written + transformed_rows,
            "standard_action_counts": dict(action_counts),
            "duplicate_geometry_set_signatures": len(duplicate_signatures),
            "output_gpkg_bytes": output_gpkg.stat().st_size if output_gpkg.exists() else None,
            "output_geojson_bytes": output_geojson.stat().st_size if output_geojson.exists() else None,
            "output_csv_bytes": output_csv.stat().st_size if output_csv.exists() else None,
            "max_rss_mb": max_rss_mb(),
            "total_seconds": round(time.perf_counter() - started, 6),
        }
        if args.keep_work_db or args.work_db:
            metadata["work_db"] = work_db.as_posix()
        write_reports(
            reports_dir,
            metadata=metadata,
            summary=summary,
            decision_rows=decision_rows,
            omission_rows=omission_rows,
            part_rows=part_rows,
        )
        write_gpkg_metadata(output_gpkg, "cs_admin_standard_metadata", metadata, summary)
    finally:
        connection.close()
        if temp_dir is not None:
            temp_dir.cleanup()

    print(
        f"[standard] complete: {summary['output_features']:,} features written "
        f"from {summary['input_features']:,} raw source rows in {summary['total_seconds']:.1f}s; "
        f"rss={summary['max_rss_mb']:.1f} MB",
        flush=True,
    )
    return {"metadata": metadata, "summary": summary}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admin-input-dir", type=Path, default=DEFAULT_ADMIN_INPUT_DIR)
    parser.add_argument("--multipart-analysis-csv", type=Path, default=DEFAULT_MULTIPART_ANALYSIS)
    parser.add_argument("--output-gpkg", type=Path, default=DEFAULT_OUTPUT_GPKG)
    parser.add_argument("--output-layer", default=DEFAULT_OUTPUT_LAYER)
    parser.add_argument("--output-geojson", type=Path, default=DEFAULT_OUTPUT_GEOJSON)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR)
    parser.add_argument("--state", help="Optional state slug/name filter for smoke tests")
    parser.add_argument("--district", help="Optional district slug/name filter for smoke tests")
    parser.add_argument("--limit-files", type=int, help="Optional raw file limit for smoke tests")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--dominant-share-threshold", type=float, default=0.9)
    parser.add_argument("--near-distance-m", type=float, default=10.0)
    parser.add_argument("--far-centroid-km", type=float, default=10.0)
    parser.add_argument("--many-part-threshold", type=int, default=20)
    parser.add_argument("--work-db", type=Path, help="Optional SQLite work database path")
    parser.add_argument("--keep-work-db", action="store_true", help="Keep the temporary work database under reports_dir")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--skip-geojson", action="store_true")
    parser.add_argument("--skip-csv", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if args.chunk_size <= 0:
        raise SystemExit("--chunk-size must be positive")
    if not 0 < args.dominant_share_threshold <= 1:
        raise SystemExit("--dominant-share-threshold must be in (0, 1]")
    build_standard_asset(args)


if __name__ == "__main__":
    main()
