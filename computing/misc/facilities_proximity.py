"""Facilities proximity tehsil export from the local pan-India GeoPackage."""

from __future__ import annotations

import re
import shutil
import sqlite3
import time
import zipfile
from functools import lru_cache
from pathlib import Path
from typing import Any

from django.conf import settings
from django.db import DatabaseError

from computing.models import Dataset, LayerType
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from nrm_app.celery import app
from utilities.constants import (
    FACILITIES_DATASET_NAME,
    FACILITIES_GEOSERVER_WORKSPACE,
    FACILITIES_PROXIMITY_GPKG,
)
from utilities.geoserver_utils import Geoserver, GeoserverException


CONFIG_PATH = "utilities/scripts/facilities_utils/config/facilities_master.yaml"
DEFAULT_TABLES = {
    "village_shapes": "village_shapes",
    "l3": "proximity_l3",
    "class_map": "proximity_class_map",
    "nearest_facilities": "proximity_nearest_facilities",
    "l2_materialized": "proximity_l2_materialized",
    "l1_materialized": "proximity_l1_materialized",
}
OUTPUT_DIR = "data/facilities/outputs/tehsil_data"
CACHE_METADATA_TABLE = "proximity_runtime_cache_metadata"
CACHE_VERSION = "2026-07-01-working-cache-1"
LAYER_PREFIX = "facilities"
ALGORITHM = "facilities-proximity"
ALGORITHM_VERSION = "2026-07-01"
EXPORT_LEVELS = ("l3", "l2", "l1")
PRIMARY_LEVEL = "l3"


def _repo_path(path: str | Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else Path(settings.BASE_DIR) / path


@lru_cache(maxsize=1)
def _facilities_config() -> dict[str, Any]:
    try:
        import yaml
    except ImportError:
        return {}

    path = _repo_path(CONFIG_PATH)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as stream:
        return yaml.safe_load(stream) or {}


def _configured_output(key: str, default: str) -> str:
    return str((_facilities_config().get("outputs") or {}).get(key) or default)


@lru_cache(maxsize=1)
def _tables() -> dict[str, str]:
    configured = ((_facilities_config().get("proximity") or {}).get("tables") or {})
    return {**DEFAULT_TABLES, **{key: str(value) for key, value in configured.items()}}


def _table(key: str) -> str:
    return _tables()[key]


def _source_path() -> Path:
    return _repo_path(_configured_output("proximity_gpkg", FACILITIES_PROXIMITY_GPKG))


def _working_path(source_path: Path) -> Path:
    return source_path.parent / "working" / f"{source_path.stem}_working{source_path.suffix}"


def _source_signature(source_path: Path) -> dict[str, str]:
    stat = source_path.stat()
    return {
        "cache_version": CACHE_VERSION,
        "source_path": source_path.as_posix(),
        "source_size": str(stat.st_size),
        "source_mtime_ns": str(stat.st_mtime_ns),
    }


def _metadata_matches(connection: sqlite3.Connection, signature: dict[str, str]) -> bool:
    if not _table_exists(connection, CACHE_METADATA_TABLE):
        return False
    rows = dict(connection.execute(f"select key, value from {CACHE_METADATA_TABLE}").fetchall())
    return all(rows.get(key) == value for key, value in signature.items())


def _working_cache_ready(working_path: Path, signature: dict[str, str]) -> bool:
    if not working_path.exists():
        return False
    try:
        with sqlite3.connect(working_path) as connection:
            return (
                _metadata_matches(connection, signature)
                and _table_exists(connection, _table("village_shapes"))
                and _table_exists(connection, _table("l3"))
                and _table_exists(connection, _table("l2_materialized"))
                and _table_exists(connection, _table("l1_materialized"))
            )
    except sqlite3.Error:
        return False


def _cache_lock_path(working_path: Path) -> Path:
    return working_path.with_name(f"{working_path.name}.lock")


def _acquire_cache_lock(working_path: Path, signature: dict[str, str]) -> bool:
    lock_path = _cache_lock_path(working_path)
    while True:
        if _working_cache_ready(working_path, signature):
            return False
        try:
            lock_path.mkdir(parents=True)
            return True
        except FileExistsError:
            try:
                if time.time() - lock_path.stat().st_mtime > 7200:
                    lock_path.rmdir()
                    continue
            except FileNotFoundError:
                continue
            time.sleep(2)


def _release_cache_lock(working_path: Path) -> None:
    try:
        _cache_lock_path(working_path).rmdir()
    except FileNotFoundError:
        return


def _write_cache_metadata(connection: sqlite3.Connection, signature: dict[str, str]) -> None:
    connection.execute(f"drop table if exists {CACHE_METADATA_TABLE}")
    connection.execute(f"create table {CACHE_METADATA_TABLE} (key text primary key, value text)")
    rows = {**signature, "built_at_epoch": str(time.time())}
    connection.executemany(
        f"insert into {CACHE_METADATA_TABLE} (key, value) values (?, ?)",
        rows.items(),
    )


def _create_runtime_indexes(connection: sqlite3.Connection) -> None:
    village = _quote_ident(_table("village_shapes"))
    l3 = _quote_ident(_table("l3"))
    class_map = _quote_ident(_table("class_map"))
    nearest = _quote_ident(_table("nearest_facilities"))
    l2 = _quote_ident(_table("l2_materialized"))
    l1 = _quote_ident(_table("l1_materialized"))
    connection.executescript(
        f"""
        create index if not exists idx_runtime_village_location
          on {village}(state_name, district_name, TEHSIL);
        create index if not exists idx_runtime_village_id
          on {village}(cs_feature_id);
        create index if not exists idx_runtime_l3_village
          on {l3}(cs_feature_id);
        create index if not exists idx_runtime_l3_class
          on {l3}(class_l3_facility_class);
        create index if not exists idx_runtime_class_map_l3
          on {class_map}(class_l3_facility_class);
        create index if not exists idx_runtime_nearest_uid
          on {nearest}(facility_uid);
        create index if not exists idx_runtime_l2_village
          on {l2}(cs_feature_id);
        create index if not exists idx_runtime_l1_village
          on {l1}(cs_feature_id);
        """
    )


def _materialize_runtime_tables(connection: sqlite3.Connection) -> None:
    l2 = _quote_ident(_table("l2_materialized"))
    l1 = _quote_ident(_table("l1_materialized"))
    l3 = _quote_ident(_table("l3"))
    class_map = _quote_ident(_table("class_map"))
    nearest = _quote_ident(_table("nearest_facilities"))
    connection.executescript(
        f"""
        drop table if exists {l2};
        create table {l2} as
        with ranked as (
          select
            p.cs_feature_id,
            m.class_l1_domain,
            m.class_l2_filter_group,
            case
              when m.filter_logic = 'max'
                then max(p.nearest_distance_km) over (
                  partition by p.cs_feature_id, m.class_l1_domain, m.class_l2_filter_group
                )
              else min(p.nearest_distance_km) over (
                  partition by p.cs_feature_id, m.class_l1_domain, m.class_l2_filter_group
                )
            end as logic_distance_km,
            p.class_l3_facility_class as selected_component_class,
            p.nearest_facility_uid,
            nf.facility_name as nearest_facility_name,
            nf.facility_code as nearest_facility_code,
            nf.latitude as nearest_facility_latitude,
            nf.longitude as nearest_facility_longitude,
            nf.class_l4_facility_subtype as nearest_class_l4_facility_subtype,
            row_number() over (
              partition by p.cs_feature_id, m.class_l1_domain, m.class_l2_filter_group
              order by
                case when m.filter_logic = 'max' then p.nearest_distance_km end desc,
                case when coalesce(m.filter_logic, 'min') != 'max' then p.nearest_distance_km end asc,
                p.class_l3_facility_class
            ) as rn
          from {l3} p
          join {class_map} m
            on m.class_l3_facility_class = p.class_l3_facility_class
          left join {nearest} nf
            on nf.facility_uid = p.nearest_facility_uid
        )
        select
          cs_feature_id,
          class_l1_domain,
          class_l2_filter_group,
          logic_distance_km,
          selected_component_class,
          nearest_facility_uid,
          nearest_facility_name,
          nearest_facility_code,
          nearest_facility_latitude,
          nearest_facility_longitude,
          nearest_class_l4_facility_subtype
        from ranked
        where rn = 1;

        drop table if exists {l1};
        create table {l1} as
        with ranked as (
          select
            p.cs_feature_id,
            m.class_l1_domain,
            p.nearest_distance_km as closest_domain_distance_km,
            m.class_l2_filter_group as selected_filter_group,
            p.class_l3_facility_class as selected_component_class,
            p.nearest_facility_uid,
            nf.facility_name as nearest_facility_name,
            nf.facility_code as nearest_facility_code,
            nf.latitude as nearest_facility_latitude,
            nf.longitude as nearest_facility_longitude,
            nf.class_l4_facility_subtype as nearest_class_l4_facility_subtype,
            row_number() over (
              partition by p.cs_feature_id, m.class_l1_domain
              order by p.nearest_distance_km asc, p.class_l3_facility_class
            ) as rn
          from {l3} p
          join {class_map} m
            on m.class_l3_facility_class = p.class_l3_facility_class
          left join {nearest} nf
            on nf.facility_uid = p.nearest_facility_uid
        )
        select
          cs_feature_id,
          class_l1_domain,
          closest_domain_distance_km,
          selected_filter_group,
          selected_component_class,
          nearest_facility_uid,
          nearest_facility_name,
          nearest_facility_code,
          nearest_facility_latitude,
          nearest_facility_longitude,
          nearest_class_l4_facility_subtype
        from ranked
        where rn = 1;
        """
    )


def _build_working_cache(source_path: Path, working_path: Path, signature: dict[str, str]) -> None:
    working_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = working_path.with_name(f"{working_path.stem}.{time.time_ns()}.tmp{working_path.suffix}")
    if temp_path.exists():
        temp_path.unlink()
    shutil.copy2(source_path, temp_path)
    try:
        with sqlite3.connect(temp_path, timeout=3600) as connection:
            connection.execute("pragma busy_timeout = 3600000")
            connection.execute("pragma temp_store = memory")
            _village_layer(connection)
            _materialize_runtime_tables(connection)
            _create_runtime_indexes(connection)
            _write_cache_metadata(connection, signature)
            connection.commit()
        temp_path.replace(working_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _working_source_path() -> Path:
    source_path = _source_path()
    signature = _source_signature(source_path)
    working_path = _working_path(source_path)
    if _working_cache_ready(working_path, signature):
        return working_path
    has_lock = _acquire_cache_lock(working_path, signature)
    if not has_lock:
        return working_path
    try:
        if not _working_cache_ready(working_path, signature):
            _build_working_cache(source_path, working_path, signature)
    finally:
        _release_cache_lock(working_path)
    return working_path


def _slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")


def _match_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _canonical_asset_name(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9]+", " ", str(value or "")).strip().upper()


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    return (
        connection.execute(
            "select 1 from sqlite_master where type in ('table', 'view') and name = ?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _village_layer(connection: sqlite3.Connection) -> str:
    layer = _table("village_shapes")
    if not _table_exists(connection, layer):
        raise RuntimeError(
            f"Facilities proximity source must contain {layer!r}. "
            "Rebuild data/facilities/outputs/village_facility_proximity.gpkg with the current facilities pipeline."
        )
    return layer


def _geometry_type(connection: sqlite3.Connection, layer: str) -> str:
    row = connection.execute(
        """
        select geometry_type_name
        from gpkg_geometry_columns
        where table_name = ? and column_name = 'geom'
        """,
        (layer,),
    ).fetchone()
    return row[0] if row and row[0] else "POINT"


def _layer_name(district: str, block: str) -> str:
    return f"{LAYER_PREFIX}_{_slug(district)}_{_slug(block)}"


def _output_dir(state: str, district: str, block: str) -> Path:
    return _repo_path(OUTPUT_DIR) / _slug(state) / _slug(district) / _slug(block)


def _location_rows() -> tuple[tuple[str, str, str], ...]:
    with sqlite3.connect(_working_source_path()) as connection:
        layer = _village_layer(connection)
        rows = connection.execute(
            f"""
            select distinct state_name, district_name, TEHSIL
            from {_quote_ident(layer)}
            order by state_name, district_name, TEHSIL
            """
        ).fetchall()
    return tuple((row[0], row[1], row[2]) for row in rows)


def _resolve_location(state: str, district: str, block: str) -> tuple[str, str, str]:
    state_key, district_key, block_key = map(_match_key, (state, district, block))
    state_matches = [row for row in _location_rows() if _match_key(row[0]) == state_key]
    if not state_matches:
        raise ValueError(f"State not found in facilities asset: {state}")

    district_matches = [
        row for row in state_matches if _match_key(row[1]) == district_key
    ]
    if not district_matches:
        available = sorted({row[1] for row in state_matches})[:20]
        raise ValueError(
            f"District not found in facilities asset: {district}. "
            f"Available examples: {available}"
        )

    block_matches = [row for row in district_matches if _match_key(row[2]) == block_key]
    if not block_matches:
        available = sorted({row[2] for row in district_matches})[:30]
        raise ValueError(
            f"TEHSIL/block not found in facilities asset: {block}. "
            f"Available examples: {available}"
        )
    return block_matches[0]


def _create_gpkg_core(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA application_id = 1196444487;
        PRAGMA user_version = 10400;

        CREATE TABLE gpkg_spatial_ref_sys (
            srs_name TEXT NOT NULL,
            srs_id INTEGER NOT NULL PRIMARY KEY,
            organization TEXT NOT NULL,
            organization_coordsys_id INTEGER NOT NULL,
            definition TEXT NOT NULL,
            description TEXT
        );

        INSERT OR REPLACE INTO gpkg_spatial_ref_sys
        (srs_name, srs_id, organization, organization_coordsys_id, definition, description)
        VALUES
        (
            'Undefined Cartesian SRS',
            -1,
            'NONE',
            -1,
            'undefined',
            'undefined Cartesian coordinate reference system'
        ),
        (
            'Undefined geographic SRS',
            0,
            'NONE',
            0,
            'undefined',
            'undefined geographic coordinate reference system'
        ),
        (
            'WGS 84 geodetic',
            4326,
            'EPSG',
            4326,
            'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],
             PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],
             AUTHORITY["EPSG","4326"]]',
            'longitude/latitude coordinates in decimal degrees on the WGS 84 spheroid'
        );

        CREATE TABLE gpkg_contents (
            table_name TEXT NOT NULL PRIMARY KEY,
            data_type TEXT NOT NULL,
            identifier TEXT UNIQUE,
            description TEXT DEFAULT '',
            last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            min_x DOUBLE,
            min_y DOUBLE,
            max_x DOUBLE,
            max_y DOUBLE,
            srs_id INTEGER,
            CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
        );

        CREATE TABLE gpkg_geometry_columns (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            geometry_type_name TEXT NOT NULL,
            srs_id INTEGER NOT NULL,
            z TINYINT NOT NULL,
            m TINYINT NOT NULL,
            PRIMARY KEY (table_name, column_name),
            CONSTRAINT fk_gc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),
            CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
        );
        """
    )


def _register_feature_layer(
    connection: sqlite3.Connection,
    layer: str,
    bounds: dict[str, float | None],
    geometry_type: str,
) -> None:
    connection.execute(
        """
        insert or replace into gpkg_contents
        (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id)
        values (?, 'features', ?, '', strftime('%Y-%m-%dT%H:%M:%fZ','now'), ?, ?, ?, ?, 4326)
        """,
        (
            layer,
            layer,
            bounds.get("min_x"),
            bounds.get("min_y"),
            bounds.get("max_x"),
            bounds.get("max_y"),
        ),
    )
    connection.execute(
        """
        insert or replace into gpkg_geometry_columns
        (table_name, column_name, geometry_type_name, srs_id, z, m)
        values (?, 'geom', ?, 4326, 0, 0)
        """,
        (layer, geometry_type),
    )


def _create_feature_table(
    connection: sqlite3.Connection,
    layer: str,
    columns: list[tuple[str, str]],
    geometry_type: str,
) -> None:
    fields = ["fid INTEGER PRIMARY KEY AUTOINCREMENT", f"geom {geometry_type}"]
    fields.extend(f"{_quote_ident(name)} {sql_type}" for name, sql_type in columns)
    connection.execute(f"create table {_quote_ident(layer)} ({', '.join(fields)})")


def _insert_rows(
    connection: sqlite3.Connection,
    layer: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in ["geom", *columns])
    column_sql = ", ".join(_quote_ident(column) for column in ["geom", *columns])
    connection.executemany(
        f"insert into {_quote_ident(layer)} ({column_sql}) values ({placeholders})",
        rows,
    )


def _selected_village_sql(village_layer: str) -> str:
    return f"""
        select geom, cs_feature_id, state_name, district_name, TEHSIL,
               pc11_village_id, NAME, _village_latitude, _village_longitude
        from {_quote_ident(village_layer)}
        where state_name = ? and district_name = ? and TEHSIL = ?
    """


def _selected_village_count(
    connection: sqlite3.Connection,
    village_layer: str,
    state: str,
    district: str,
    tehsil: str,
) -> int:
    row = connection.execute(
        f"""
        select count(*)
        from {_quote_ident(village_layer)}
        where state_name = ? and district_name = ? and TEHSIL = ?
        """,
        (state, district, tehsil),
    ).fetchone()
    return int(row[0] or 0)


def _layer_bounds(
    connection: sqlite3.Connection,
    village_layer: str,
    state: str,
    district: str,
    tehsil: str,
) -> dict[str, float | None]:
    row = connection.execute(
        f"""
        select
            min(_village_longitude), min(_village_latitude),
            max(_village_longitude), max(_village_latitude)
        from {_quote_ident(village_layer)}
        where state_name = ? and district_name = ? and TEHSIL = ?
        """,
        (state, district, tehsil),
    ).fetchone()
    return {"min_x": row[0], "min_y": row[1], "max_x": row[2], "max_y": row[3]}


def _query_l3(
    connection: sqlite3.Connection,
    village_layer: str,
    state: str,
    district: str,
    tehsil: str,
) -> list[tuple[Any, ...]]:
    return connection.execute(
        f"""
        with selected_villages as ({_selected_village_sql(village_layer)})
        select
            v.geom,
            v.cs_feature_id,
            v.state_name,
            v.district_name,
            v.TEHSIL,
            v.pc11_village_id,
            v.NAME,
            coalesce(nf.facility_name, p.nearest_facility_uid) as title,
            m.class_l1_domain,
            m.class_l2_filter_group,
            p.class_l3_facility_class,
            p.nearest_distance_km,
            p.nearest_facility_uid,
            nf.facility_name as nearest_facility_name,
            nf.facility_code as nearest_facility_code,
            nf.latitude as nearest_facility_latitude,
            nf.longitude as nearest_facility_longitude,
            nf.class_l4_facility_subtype as nearest_class_l4_facility_subtype
        from selected_villages v
        join {_quote_ident(_table("l3"))} p on p.cs_feature_id = v.cs_feature_id
        left join {_quote_ident(_table("class_map"))} m on m.class_l3_facility_class = p.class_l3_facility_class
        left join {_quote_ident(_table("nearest_facilities"))} nf on nf.facility_uid = p.nearest_facility_uid
        order by v.cs_feature_id, p.class_l3_facility_class
        """,
        (state, district, tehsil),
    ).fetchall()


def _query_l2(
    connection: sqlite3.Connection,
    village_layer: str,
    state: str,
    district: str,
    tehsil: str,
) -> list[tuple[Any, ...]]:
    return connection.execute(
        f"""
        with selected_villages as ({_selected_village_sql(village_layer)})
        select
            v.geom,
            v.cs_feature_id,
            v.state_name,
            v.district_name,
            v.TEHSIL,
            v.pc11_village_id,
            v.NAME,
            coalesce(p.nearest_facility_name, p.nearest_facility_uid) as title,
            p.class_l1_domain,
            p.class_l2_filter_group,
            p.logic_distance_km,
            p.selected_component_class,
            p.nearest_facility_uid,
            p.nearest_facility_name,
            p.nearest_facility_code,
            p.nearest_facility_latitude,
            p.nearest_facility_longitude,
            p.nearest_class_l4_facility_subtype
        from selected_villages v
        join {_quote_ident(_table("l2_materialized"))} p
          on cast(p.cs_feature_id as text) = cast(v.cs_feature_id as text)
        order by v.cs_feature_id, p.class_l1_domain, p.class_l2_filter_group
        """,
        (state, district, tehsil),
    ).fetchall()


def _query_l1(
    connection: sqlite3.Connection,
    village_layer: str,
    state: str,
    district: str,
    tehsil: str,
) -> list[tuple[Any, ...]]:
    return connection.execute(
        f"""
        with selected_villages as ({_selected_village_sql(village_layer)})
        select
            v.geom,
            v.cs_feature_id,
            v.state_name,
            v.district_name,
            v.TEHSIL,
            v.pc11_village_id,
            v.NAME,
            coalesce(p.nearest_facility_name, p.nearest_facility_uid) as title,
            p.class_l1_domain,
            p.closest_domain_distance_km,
            p.selected_filter_group,
            p.selected_component_class,
            p.nearest_facility_uid,
            p.nearest_facility_name,
            p.nearest_facility_code,
            p.nearest_facility_latitude,
            p.nearest_facility_longitude,
            p.nearest_class_l4_facility_subtype
        from selected_villages v
        join {_quote_ident(_table("l1_materialized"))} p
          on cast(p.cs_feature_id as text) = cast(v.cs_feature_id as text)
        order by v.cs_feature_id, p.class_l1_domain
        """,
        (state, district, tehsil),
    ).fetchall()


def _query_level(
    connection: sqlite3.Connection,
    level: str,
    village_layer: str,
    state: str,
    district: str,
    tehsil: str,
) -> list[tuple[Any, ...]]:
    if level == "l3":
        return _query_l3(connection, village_layer, state, district, tehsil)
    if level == "l2":
        return _query_l2(connection, village_layer, state, district, tehsil)
    return _query_l1(connection, village_layer, state, district, tehsil)


def _nearest_facility_rows(
    level_rows: dict[str, list[tuple[Any, ...]]],
) -> list[tuple[Any, ...]]:
    selected = level_rows.get(PRIMARY_LEVEL) or []
    seen = set()
    rows = []
    uid_index = _column_row_index(L3_COLUMNS, "nearest_facility_uid")
    for row in selected:
        facility_uid = row[uid_index]
        if not facility_uid or facility_uid in seen:
            continue
        seen.add(facility_uid)
        title = row[uid_index + 1] or facility_uid
        rows.append(
            (
                _facility_point_blob(row[uid_index + 4], row[uid_index + 3]),
                facility_uid,
                title,
                row[uid_index + 1],
                row[uid_index + 2],
                row[uid_index + 3],
                row[uid_index + 4],
                row[uid_index + 5],
            )
        )
    return rows


def _column_row_index(columns: list[tuple[str, str]], column_name: str) -> int:
    for index, (name, _) in enumerate(columns, start=1):
        if name == column_name:
            return index
    raise KeyError(f"Column {column_name!r} is not exported")


def _facility_point_blob(longitude: float | None, latitude: float | None) -> bytes | None:
    if longitude is None or latitude is None:
        return None
    import struct

    return (
        b"GP"
        + bytes([0, 1])
        + struct.pack("<I", 4326)
        + struct.pack("<BI2d", 1, 1, float(longitude), float(latitude))
    )


L3_COLUMNS = [
    ("cs_feature_id", "TEXT"),
    ("state_name", "TEXT"),
    ("district_name", "TEXT"),
    ("TEHSIL", "TEXT"),
    ("pc11_village_id", "TEXT"),
    ("NAME", "TEXT"),
    ("title", "TEXT"),
    ("class_l1_domain", "TEXT"),
    ("class_l2_filter_group", "TEXT"),
    ("class_l3_facility_class", "TEXT"),
    ("nearest_distance_km", "REAL"),
    ("nearest_facility_uid", "TEXT"),
    ("nearest_facility_name", "TEXT"),
    ("nearest_facility_code", "TEXT"),
    ("nearest_facility_latitude", "REAL"),
    ("nearest_facility_longitude", "REAL"),
    ("nearest_class_l4_facility_subtype", "TEXT"),
]

L2_COLUMNS = [
    ("cs_feature_id", "TEXT"),
    ("state_name", "TEXT"),
    ("district_name", "TEXT"),
    ("TEHSIL", "TEXT"),
    ("pc11_village_id", "TEXT"),
    ("NAME", "TEXT"),
    ("title", "TEXT"),
    ("class_l1_domain", "TEXT"),
    ("class_l2_filter_group", "TEXT"),
    ("logic_distance_km", "REAL"),
    ("selected_component_class", "TEXT"),
    ("nearest_facility_uid", "TEXT"),
    ("nearest_facility_name", "TEXT"),
    ("nearest_facility_code", "TEXT"),
    ("nearest_facility_latitude", "REAL"),
    ("nearest_facility_longitude", "REAL"),
    ("nearest_class_l4_facility_subtype", "TEXT"),
]

L1_COLUMNS = [
    ("cs_feature_id", "TEXT"),
    ("state_name", "TEXT"),
    ("district_name", "TEXT"),
    ("TEHSIL", "TEXT"),
    ("pc11_village_id", "TEXT"),
    ("NAME", "TEXT"),
    ("title", "TEXT"),
    ("class_l1_domain", "TEXT"),
    ("closest_domain_distance_km", "REAL"),
    ("selected_filter_group", "TEXT"),
    ("selected_component_class", "TEXT"),
    ("nearest_facility_uid", "TEXT"),
    ("nearest_facility_name", "TEXT"),
    ("nearest_facility_code", "TEXT"),
    ("nearest_facility_latitude", "REAL"),
    ("nearest_facility_longitude", "REAL"),
    ("nearest_class_l4_facility_subtype", "TEXT"),
]

NEAREST_FACILITY_COLUMNS = [
    ("facility_uid", "TEXT"),
    ("title", "TEXT"),
    ("facility_name", "TEXT"),
    ("facility_code", "TEXT"),
    ("latitude", "REAL"),
    ("longitude", "REAL"),
    ("class_l4_facility_subtype", "TEXT"),
]


def _write_output_zip(gpkg_path: Path) -> Path:
    zip_path = gpkg_path.with_suffix(".zip")
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
        archive.write(gpkg_path, arcname=gpkg_path.name)
    return zip_path


def _write_tehsil_gpkg(
    source_path: Path,
    output_dir: Path,
    layer_name: str,
    state: str,
    district: str,
    tehsil: str,
) -> tuple[Path, Path, dict[str, int], str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    gpkg_path = output_dir / f"{layer_name}.gpkg"
    zip_path = gpkg_path.with_suffix(".zip")
    for path in (gpkg_path, zip_path):
        if path.exists():
            path.unlink()

    counts: dict[str, int] = {}
    with sqlite3.connect(source_path) as source, sqlite3.connect(gpkg_path) as output:
        _create_gpkg_core(output)
        village_layer = _village_layer(source)
        village_geometry_type = _geometry_type(source, village_layer)
        village_count = _selected_village_count(
            source,
            village_layer,
            state,
            district,
            tehsil,
        )
        if not village_count:
            raise ValueError(f"No facilities village rows found for {state}/{district}/{tehsil}")
        counts["villages"] = village_count
        bounds = _layer_bounds(source, village_layer, state, district, tehsil)

        level_rows: dict[str, list[tuple[Any, ...]]] = {}
        for level in EXPORT_LEVELS:
            rows = _query_level(source, level, village_layer, state, district, tehsil)
            level_rows[level] = rows
            layer = layer_name if level == PRIMARY_LEVEL else f"{layer_name}_{level}"
            columns = {"l3": L3_COLUMNS, "l2": L2_COLUMNS, "l1": L1_COLUMNS}[level]
            _create_feature_table(output, layer, columns, geometry_type=village_geometry_type)
            _insert_rows(output, layer, [column for column, _ in columns], rows)
            _register_feature_layer(
                output,
                layer,
                bounds,
                geometry_type=village_geometry_type,
            )
            counts[level] = len(rows)

        nearest_rows = _nearest_facility_rows(level_rows)
        facility_layer = f"{layer_name}_nearest_facilities"
        _create_feature_table(
            output,
            facility_layer,
            NEAREST_FACILITY_COLUMNS,
            geometry_type="POINT",
        )
        _insert_rows(
            output,
            facility_layer,
            [column for column, _ in NEAREST_FACILITY_COLUMNS],
            nearest_rows,
        )
        _register_feature_layer(output, facility_layer, bounds, geometry_type="POINT")
        counts["nearest_facilities"] = len(nearest_rows)

        output.commit()

    zip_path = _write_output_zip(gpkg_path)
    return gpkg_path, zip_path, counts, village_layer, village_geometry_type


def _publish_to_geoserver(gpkg_path: Path, zip_path: Path, layer_name: str, overwrite: bool) -> dict[str, Any]:
    try:
        geoserver = Geoserver()
        try:
            geoserver.get_workspace(FACILITIES_GEOSERVER_WORKSPACE)
        except GeoserverException as exc:
            if exc.status != 404:
                raise
            geoserver.create_workspace(FACILITIES_GEOSERVER_WORKSPACE)

        if overwrite:
            geoserver.delete_vector_store(
                workspace=FACILITIES_GEOSERVER_WORKSPACE,
                store=layer_name,
            )

        if not zip_path.exists():
            zip_path = _write_output_zip(gpkg_path)
        response = geoserver.create_shp_datastore(
            path=zip_path.as_posix(),
            store_name=layer_name,
            workspace=FACILITIES_GEOSERVER_WORKSPACE,
            file_extension="gpkg",
        )
        return {"ok": True, "response": response}
    except Exception as exc:
        return {
            "ok": False,
            "error_type": exc.__class__.__name__,
            "error": str(exc)[:500],
        }


def _register_layer_in_db(
    state: str,
    district: str,
    block: str,
    layer_name: str,
    geoserver_url: str,
    source_path: Path,
    gpkg_path: Path,
    zip_path: Path,
    output_dir: Path,
    row_counts: dict[str, int],
    source_village_layer: str,
    source_village_geometry: str,
    overwrite: bool,
) -> tuple[int | None, dict[str, Any]]:
    try:
        Dataset.objects.get_or_create(
            name=FACILITIES_DATASET_NAME,
            defaults={
                "layer_type": LayerType.VECTOR,
                "workspace": FACILITIES_GEOSERVER_WORKSPACE,
            },
        )
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name,
            geoserver_url,
            FACILITIES_DATASET_NAME,
            False,
            "1.0",
            ALGORITHM,
            ALGORITHM_VERSION,
            {
                "is_generated_locally": True,
                "source": source_path.as_posix(),
                "gpkg_path": gpkg_path.as_posix(),
                "zip_path": zip_path.as_posix(),
                "output_dir": output_dir.as_posix(),
                "geoserver_workspace": FACILITIES_GEOSERVER_WORKSPACE,
                "geoserver_layer_name": layer_name,
                "geoserver_url": geoserver_url,
                "row_counts": row_counts,
                "source_village_layer": source_village_layer,
                "source_village_geometry": source_village_geometry,
            },
            _bool(overwrite),
            False,
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
        return layer_id, {"ok": True}
    except DatabaseError as exc:
        return None, {
            "ok": False,
            "error_type": exc.__class__.__name__,
            "error": str(exc)[:500],
        }
    except Exception as exc:
        return None, {
            "ok": False,
            "error_type": exc.__class__.__name__,
            "error": str(exc)[:500],
        }


def generate_facilities_proximity(
    state: str,
    district: str,
    block: str,
    sync_to_geoserver: bool = True,
    overwrite: bool = True,
) -> dict[str, Any]:
    """Export facilities proximity rows for one state/district/TEHSIL."""
    started = time.perf_counter()
    timings: dict[str, float] = {}

    step_started = time.perf_counter()
    canonical_source_path = _source_path()
    source_path = _working_source_path()
    timings["prepare_working_cache_seconds"] = round(time.perf_counter() - step_started, 3)

    step_started = time.perf_counter()
    resolved_state = _canonical_asset_name(state)
    resolved_district = _canonical_asset_name(district)
    resolved_block = _canonical_asset_name(block)
    try:
        resolved_state, resolved_district, resolved_block = _resolve_location(
            resolved_state,
            resolved_district,
            resolved_block,
        )
    except ValueError:
        resolved_state, resolved_district, resolved_block = _resolve_location(
            state,
            district,
            block,
        )
    timings["resolve_location_seconds"] = round(time.perf_counter() - step_started, 3)

    step_started = time.perf_counter()
    layer_name = _layer_name(resolved_district, resolved_block)
    output_dir = _output_dir(resolved_state, resolved_district, resolved_block)
    gpkg_path, zip_path, row_counts, source_village_layer, source_village_geometry = _write_tehsil_gpkg(
        source_path=source_path,
        output_dir=output_dir,
        layer_name=layer_name,
        state=resolved_state,
        district=resolved_district,
        tehsil=resolved_block,
    )
    timings["write_tehsil_gpkg_seconds"] = round(time.perf_counter() - step_started, 3)

    geoserver = None
    layer_id = None
    geoserver_url = None
    db_registration = None
    if _bool(sync_to_geoserver):
        step_started = time.perf_counter()
        geoserver = _publish_to_geoserver(
            gpkg_path,
            zip_path,
            layer_name,
            _bool(overwrite),
        )
        timings["publish_geoserver_seconds"] = round(time.perf_counter() - step_started, 3)
        if geoserver.get("ok"):
            step_started = time.perf_counter()
            geoserver_url = (
                f"{settings.GEOSERVER_URL.rstrip('/')}/{FACILITIES_GEOSERVER_WORKSPACE}/ows"
                "?service=WFS&version=1.0.0&request=GetFeature"
                f"&typeName={FACILITIES_GEOSERVER_WORKSPACE}:{layer_name}"
                "&outputFormat=application/json"
            )
            layer_id, db_registration = _register_layer_in_db(
                state=resolved_state,
                district=resolved_district,
                block=resolved_block,
                layer_name=layer_name,
                geoserver_url=geoserver_url,
                source_path=source_path,
                gpkg_path=gpkg_path,
                zip_path=zip_path,
                output_dir=output_dir,
                row_counts=row_counts,
                source_village_layer=source_village_layer,
                source_village_geometry=source_village_geometry,
                overwrite=_bool(overwrite),
            )
            timings["register_db_seconds"] = round(time.perf_counter() - step_started, 3)

    timings["total_seconds"] = round(time.perf_counter() - started, 3)
    return {
        "status": "success",
        "layer_name": layer_name,
        "row_counts": row_counts,
        "source": canonical_source_path.as_posix(),
        "working_source": source_path.as_posix(),
        "source_village_layer": source_village_layer,
        "source_village_geometry": source_village_geometry,
        "gpkg_path": gpkg_path.as_posix(),
        "zip_path": zip_path.as_posix(),
        "output_dir": output_dir.as_posix(),
        "state_name": resolved_state,
        "district_name": resolved_district,
        "tehsil": resolved_block,
        "sync_to_geoserver": _bool(sync_to_geoserver),
        "geoserver": geoserver,
        "geoserver_url": geoserver_url,
        "db_registration": db_registration,
        "layer_id": layer_id,
        "timings": timings,
        "elapsed_seconds": timings["total_seconds"],
    }


@app.task(bind=True)
def generate_facilities_proximity_task(
    self,
    state: str,
    district: str,
    block: str,
    sync_to_geoserver: bool = True,
    overwrite: bool = True,
) -> dict[str, Any]:
    """Celery task wrapper for local facilities proximity tehsil export."""
    return generate_facilities_proximity(
        state=state,
        district=district,
        block=block,
        sync_to_geoserver=sync_to_geoserver,
        overwrite=overwrite,
    )
