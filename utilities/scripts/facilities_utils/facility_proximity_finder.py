#!/usr/bin/env python3
"""Build lean village-to-facility proximity outputs from facilities and village GPKGs."""

from __future__ import annotations

import argparse
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

from facility_utils import find_repo_root, read_yaml, resolve_path, setup_logging, slugify, taxonomy_rows


EARTH_RADIUS_KM = 6371.0088
log = logging.getLogger("facilities")


def sql_quote(value: Any) -> str:
    if value is None or pd.isna(value):
        return "null"
    return "'" + str(value).replace("'", "''") + "'"


def sql_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def lonlat_to_xyz(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    lat_rad = np.radians(lat)
    lon_rad = np.radians(lon)
    return np.column_stack((np.cos(lat_rad) * np.cos(lon_rad), np.cos(lat_rad) * np.sin(lon_rad), np.sin(lat_rad)))


def chord_to_km(chord: np.ndarray) -> np.ndarray:
    chord = np.clip(chord, 0, 2)
    return EARTH_RADIUS_KM * 2 * np.arcsin(chord / 2)


def load_villages(config: Dict[str, Any], repo_root: Path, sample_villages: Optional[int]) -> Any:
    import geopandas as gpd

    prox = config["proximity"]
    rows = slice(0, sample_villages) if sample_villages else None
    villages = gpd.read_file(resolve_path(prox["village_gpkg"], repo_root), layer=prox["village_layer"], rows=rows, engine="pyogrio")
    if villages.crs is None:
        villages = villages.set_crs(config["schema"]["crs"])
    villages = villages.to_crs(config["schema"]["crs"])
    points = villages.geometry.representative_point()
    villages = villages.copy()
    villages["_village_latitude"] = points.y
    villages["_village_longitude"] = points.x
    return villages


def register_attribute_table(con: sqlite3.Connection, table_name: str) -> None:
    con.execute(
        """
        insert or replace into gpkg_contents
        (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id)
        values (?, 'attributes', ?, '', strftime('%Y-%m-%dT%H:%M:%fZ','now'), null, null, null, null, null)
        """,
        (table_name, table_name),
    )


def drop_derived_outputs(con: sqlite3.Connection, config: Dict[str, Any]) -> None:
    tables = config["proximity"]["tables"]
    con.execute(f"drop view if exists {sql_ident(tables['l2_view'])}")
    con.execute(f"drop view if exists {sql_ident(tables['l1_view'])}")
    con.execute(f"drop table if exists {sql_ident(tables['l2_materialized'])}")
    con.execute(f"drop table if exists {sql_ident(tables['l1_materialized'])}")


def output_village_count(output_gpkg: Path, village_layer: str) -> Optional[int]:
    if not output_gpkg.exists():
        return None
    try:
        with sqlite3.connect(output_gpkg) as con:
            return int(con.execute(f"select count(*) from {sql_ident(village_layer)}").fetchone()[0])
    except sqlite3.Error:
        return None


def village_context_table(config: Dict[str, Any]) -> str:
    tables = config["proximity"]["tables"]
    return tables.get("village_shapes", "village_shapes")


def prepare_output_gpkg(output_gpkg: Path, villages: Any, config: Dict[str, Any], force_full: bool) -> None:
    tables = config["proximity"]["tables"]
    village_layer = village_context_table(config)
    existing_villages = output_village_count(output_gpkg, village_layer)
    if output_gpkg.exists() and not force_full and existing_villages == len(villages):
        log.info("Resuming existing proximity GeoPackage: %s", output_gpkg)
        return
    if output_gpkg.exists():
        output_gpkg.unlink()
    village_cols = [
        config["proximity"]["village_id_col"],
        *(config["proximity"].get("village_context_cols") or []),
        "_village_latitude",
        "_village_longitude",
        "geometry",
    ]
    village_cols = [col for col in village_cols if col in villages.columns]
    villages[village_cols].to_file(output_gpkg, layer=village_layer, driver="GPKG", engine="pyogrio")
    with sqlite3.connect(output_gpkg) as con:
        con.execute(f"drop table if exists {sql_ident(tables['l3'])}")
        con.commit()


def l3_groups(config: Dict[str, Any], facilities_gpkg: Path, classes: Optional[List[str]], sample_classes: Optional[int]) -> List[Dict[str, Any]]:
    requested = set(classes or [])
    df = pd.DataFrame(taxonomy_rows(config))
    df = df[["class_l1_domain", "class_l2_filter_group", "class_l3_facility_class", "filter_logic"]].drop_duplicates()
    if requested:
        df = df[df["class_l3_facility_class"].isin(requested)]
    if sample_classes:
        df = df.head(sample_classes)
    return df.to_dict(orient="records")


def completed_l3_classes(output_gpkg: Path, config: Dict[str, Any], expected_villages: int) -> set[str]:
    if not output_gpkg.exists():
        return set()
    try:
        with sqlite3.connect(output_gpkg) as con:
            rows = con.execute(
                f"""
                select class_l3_facility_class, count(*)
                from {sql_ident(config['proximity']['tables']['l3'])}
                group by class_l3_facility_class
                having count(*) >= ?
                """,
                (expected_villages,),
            ).fetchall()
        return {row[0] for row in rows}
    except sqlite3.Error:
        return set()


def load_l3_facility_points(config: Dict[str, Any], facilities_gpkg: Path, facility_class: str) -> pd.DataFrame:
    sql = f"""
        select distinct
            f.facility_uid,
            f.facility_name,
            f.facility_code,
            f.latitude,
            f.longitude,
            m.class_l4_facility_subtype
        from {config['schema']['facility_layer']} f
        join {config['schema']['membership_table']} m on m.facility_uid = f.facility_uid
        where m.class_l3_facility_class = ?
          and f.latitude is not null
          and f.longitude is not null
    """
    with sqlite3.connect(facilities_gpkg) as con:
        points = pd.read_sql_query(sql, con, params=(facility_class,))
    points["latitude"] = pd.to_numeric(points["latitude"], errors="coerce")
    points["longitude"] = pd.to_numeric(points["longitude"], errors="coerce")
    return points.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)


def append_table(output_gpkg: Path, table_name: str, frame: pd.DataFrame) -> None:
    with sqlite3.connect(output_gpkg) as con:
        frame.to_sql(table_name, con, if_exists="append", index=False)


def nearest_lookup_table(config: Dict[str, Any]) -> str:
    return config["proximity"]["tables"].get("nearest_facilities", "proximity_nearest_facilities")


def ensure_nearest_lookup_table(con: sqlite3.Connection, config: Dict[str, Any]) -> None:
    table = nearest_lookup_table(config)
    con.execute(
        f"""
        create table if not exists {sql_ident(table)} (
          facility_uid text primary key,
          facility_name text,
          facility_code text,
          latitude real,
          longitude real,
          class_l4_facility_subtype text
        )
        """
    )
    register_attribute_table(con, table)


def append_nearest_lookup(output_gpkg: Path, config: Dict[str, Any], nearest: pd.DataFrame) -> None:
    columns = [
        "facility_uid",
        "facility_name",
        "facility_code",
        "latitude",
        "longitude",
        "class_l4_facility_subtype",
    ]
    lookup = nearest[columns].drop_duplicates(subset=["facility_uid"]).where(pd.notna(nearest[columns]), None)
    if lookup.empty:
        return
    table = nearest_lookup_table(config)
    with sqlite3.connect(output_gpkg) as con:
        ensure_nearest_lookup_table(con, config)
        con.executemany(
            f"""
            insert or ignore into {sql_ident(table)}
            (facility_uid, facility_name, facility_code, latitude, longitude, class_l4_facility_subtype)
            values (?, ?, ?, ?, ?, ?)
            """,
            lookup.itertuples(index=False, name=None),
        )
        con.commit()


def nearest_lookup_missing(output_gpkg: Path, config: Dict[str, Any]) -> bool:
    if not output_gpkg.exists():
        return True
    table = nearest_lookup_table(config)
    try:
        with sqlite3.connect(output_gpkg) as con:
            if not table_exists(con, table):
                return True
            return int(con.execute(f"select count(*) from {sql_ident(table)}").fetchone()[0]) == 0
    except sqlite3.Error:
        return True


def rebuild_nearest_lookup(output_gpkg: Path, config: Dict[str, Any], facilities_gpkg: Path, batch_size: int = 900) -> None:
    l3_table = config["proximity"]["tables"]["l3"]
    lookup_table = nearest_lookup_table(config)
    with sqlite3.connect(output_gpkg) as out_con:
        if not table_exists(out_con, l3_table):
            ensure_nearest_lookup_table(out_con, config)
            return
        uids = [
            row[0]
            for row in out_con.execute(
                f"""
                select distinct nearest_facility_uid
                from {sql_ident(l3_table)}
                where nearest_facility_uid is not null and nearest_facility_uid != ''
                """
            ).fetchall()
        ]
        out_con.execute(f"drop table if exists {sql_ident(lookup_table)}")
        ensure_nearest_lookup_table(out_con, config)
        out_con.commit()
    if not uids:
        return
    facility_table = config["schema"]["facility_layer"]
    query_cols = [
        "facility_uid",
        "facility_name",
        "facility_code",
        "latitude",
        "longitude",
        "class_l4_facility_subtype",
    ]
    with sqlite3.connect(facilities_gpkg) as facility_con:
        for start in range(0, len(uids), batch_size):
            batch = uids[start : start + batch_size]
            placeholders = ", ".join("?" for _ in batch)
            sql = (
                f"select {', '.join(query_cols)} "
                f"from {sql_ident(facility_table)} "
                f"where facility_uid in ({placeholders})"
            )
            frame = pd.read_sql_query(sql, facility_con, params=batch)
            append_nearest_lookup(output_gpkg, config, frame)
    log.info("Rebuilt %s with %d nearest facility ids", lookup_table, len(uids))


def table_exists(con: sqlite3.Connection, table_name: str) -> bool:
    return (
        con.execute(
            "select 1 from sqlite_master where type in ('table', 'view') and name = ?",
            (table_name,),
        ).fetchone()
        is not None
    )


def delete_l3_classes(output_gpkg: Path, config: Dict[str, Any], classes: List[str]) -> None:
    if not output_gpkg.exists() or not classes:
        return
    table = config["proximity"]["tables"]["l3"]
    placeholders = ", ".join("?" for _ in classes)
    with sqlite3.connect(output_gpkg) as con:
        drop_derived_outputs(con, config)
        if table_exists(con, table):
            con.execute(
                f"delete from {sql_ident(table)} where class_l3_facility_class in ({placeholders})",
                classes,
            )
            log.info("Deleted existing L3 proximity rows for: %s", ", ".join(classes))
        con.commit()


def build_l3_for_group(
    output_gpkg: Path,
    config: Dict[str, Any],
    villages: Any,
    group: Dict[str, Any],
    points: pd.DataFrame,
    village_chunksize: int,
) -> int:
    tables = config["proximity"]["tables"]
    tree = cKDTree(lonlat_to_xyz(points["latitude"].to_numpy(), points["longitude"].to_numpy()))
    query_xyz = lonlat_to_xyz(villages["_village_latitude"].to_numpy(), villages["_village_longitude"].to_numpy())
    id_col = config["proximity"]["village_id_col"]
    written = 0
    for start in range(0, len(villages), village_chunksize):
        end = min(start + village_chunksize, len(villages))
        chord, index = tree.query(query_xyz[start:end], k=1)
        nearest = points.iloc[index].reset_index(drop=True)
        chunk = villages.iloc[start:end]
        out = pd.DataFrame()
        out[id_col] = chunk[id_col].astype("string").to_numpy()
        out["class_l3_facility_class"] = group["class_l3_facility_class"]
        out["nearest_distance_km"] = np.round(chord_to_km(chord), config["proximity"].get("distance_precision", 6))
        out["nearest_facility_uid"] = nearest["facility_uid"].to_numpy()
        append_table(output_gpkg, tables["l3"], out)
        append_nearest_lookup(output_gpkg, config, nearest)
        written += len(out)
    return written


def class_map_table(config: Dict[str, Any]) -> str:
    return config["proximity"]["tables"].get("class_map", "proximity_class_map")


def class_map_frame(config: Dict[str, Any]) -> pd.DataFrame:
    rows = []
    for row in taxonomy_rows(config):
        rows.append(
            {
                "class_l3_facility_class": row["class_l3_facility_class"],
                "class_l2_filter_group": row["class_l2_filter_group"],
                "class_l1_domain": row["class_l1_domain"],
                "filter_logic": row["filter_logic"],
            }
        )
    return pd.DataFrame(rows).drop_duplicates().sort_values(
        ["class_l1_domain", "class_l2_filter_group", "class_l3_facility_class"]
    )


def write_class_map(output_gpkg: Path, config: Dict[str, Any]) -> None:
    table = class_map_table(config)
    frame = class_map_frame(config)
    with sqlite3.connect(output_gpkg) as con:
        frame.to_sql(table, con, if_exists="replace", index=False)
        register_attribute_table(con, table)
        con.execute(f"create index if not exists idx_{table}_l3 on {sql_ident(table)}(class_l3_facility_class)")
        con.commit()


def context_select_sql(config: Dict[str, Any]) -> str:
    parts = []
    for col in config["proximity"].get("village_context_cols") or []:
        parts.append(f",\n            v.{sql_ident(col)} as {sql_ident(col)}")
    return "".join(parts)


def outer_context_sql(config: Dict[str, Any]) -> str:
    parts = []
    for col in config["proximity"].get("village_context_cols") or []:
        parts.append(f", {sql_ident(col)}")
    return "".join(parts)


def create_l2_view_sql(config: Dict[str, Any]) -> str:
    id_col = config["proximity"]["village_id_col"]
    l3_table = config["proximity"]["tables"]["l3"]
    village_table = village_context_table(config)
    map_table = class_map_table(config)
    lookup_table = nearest_lookup_table(config)
    return f"""
        with ranked as (
          select
            p.{sql_ident(id_col)} as {sql_ident(id_col)}{context_select_sql(config)},
            m.class_l1_domain,
            m.class_l2_filter_group,
            m.filter_logic,
            case
              when m.filter_logic = 'max'
                then max(p.nearest_distance_km) over (
                  partition by p.{sql_ident(id_col)}, m.class_l1_domain, m.class_l2_filter_group
                )
              else min(p.nearest_distance_km) over (
                  partition by p.{sql_ident(id_col)}, m.class_l1_domain, m.class_l2_filter_group
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
              partition by p.{sql_ident(id_col)}, m.class_l1_domain, m.class_l2_filter_group
              order by
                case when m.filter_logic = 'max' then p.nearest_distance_km end desc,
                case when coalesce(m.filter_logic, 'min') != 'max' then p.nearest_distance_km end asc,
                p.class_l3_facility_class
            ) as rn
          from {sql_ident(l3_table)} p
          join {sql_ident(map_table)} m
            on m.class_l3_facility_class = p.class_l3_facility_class
          left join {sql_ident(village_table)} v
            on cast(v.{sql_ident(id_col)} as text) = cast(p.{sql_ident(id_col)} as text)
          left join {sql_ident(lookup_table)} nf
            on nf.facility_uid = p.nearest_facility_uid
        )
        select
          {sql_ident(id_col)}{outer_context_sql(config)},
          class_l1_domain,
          class_l2_filter_group,
          filter_logic,
          logic_distance_km,
          selected_component_class,
          nearest_facility_uid,
          nearest_facility_name,
          nearest_facility_code,
          nearest_facility_latitude,
          nearest_facility_longitude,
          nearest_class_l4_facility_subtype
        from ranked
        where rn = 1
    """


def create_l1_view_sql(config: Dict[str, Any]) -> str:
    id_col = config["proximity"]["village_id_col"]
    l3_table = config["proximity"]["tables"]["l3"]
    village_table = village_context_table(config)
    map_table = class_map_table(config)
    lookup_table = nearest_lookup_table(config)
    return f"""
        with ranked as (
          select
            p.{sql_ident(id_col)} as {sql_ident(id_col)}{context_select_sql(config)},
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
              partition by p.{sql_ident(id_col)}, m.class_l1_domain
              order by p.nearest_distance_km asc, p.class_l3_facility_class
            ) as rn
          from {sql_ident(l3_table)} p
          join {sql_ident(map_table)} m
            on m.class_l3_facility_class = p.class_l3_facility_class
          left join {sql_ident(village_table)} v
            on cast(v.{sql_ident(id_col)} as text) = cast(p.{sql_ident(id_col)} as text)
          left join {sql_ident(lookup_table)} nf
            on nf.facility_uid = p.nearest_facility_uid
        )
        select
          {sql_ident(id_col)}{outer_context_sql(config)},
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
        where rn = 1
    """


def refresh_derived_outputs(output_gpkg: Path, config: Dict[str, Any], materialize: bool) -> None:
    tables = config["proximity"]["tables"]
    write_class_map(output_gpkg, config)
    l2_sql = create_l2_view_sql(config)
    l1_sql = create_l1_view_sql(config)
    with sqlite3.connect(output_gpkg) as con:
        drop_derived_outputs(con, config)
        if config["proximity"].get("create_derived_views", True):
            con.execute(f"create view {sql_ident(tables['l2_view'])} as {l2_sql}")
            con.execute(f"create view {sql_ident(tables['l1_view'])} as {l1_sql}")
        if materialize:
            con.execute(f"create table {sql_ident(tables['l2_materialized'])} as {l2_sql}")
            con.execute(f"create table {sql_ident(tables['l1_materialized'])} as {l1_sql}")
            con.execute(
                f"create index if not exists idx_{tables['l2_materialized']}_village "
                f"on {sql_ident(tables['l2_materialized'])}({sql_ident(config['proximity']['village_id_col'])})"
            )
            con.execute(
                f"create index if not exists idx_{tables['l1_materialized']}_village "
                f"on {sql_ident(tables['l1_materialized'])}({sql_ident(config['proximity']['village_id_col'])})"
            )
            register_attribute_table(con, tables["l2_materialized"])
            register_attribute_table(con, tables["l1_materialized"])
        con.commit()


def write_proximity_metadata(output_gpkg: Path, config: Dict[str, Any], completed: int, total_groups: int) -> None:
    rows = pd.DataFrame(
        [
            {"key": "stored_level", "value": config["proximity"]["stored_level"]},
            {"key": "l2_l1_method", "value": "derived_from_proximity_l3"},
            {"key": "class_map_table", "value": class_map_table(config)},
            {"key": "nearest_facility_lookup_table", "value": nearest_lookup_table(config)},
            {"key": "l3_groups_completed", "value": str(completed)},
            {"key": "l3_groups_total", "value": str(total_groups)},
            {"key": "materialize_derived_tables", "value": str(config["proximity"].get("materialize_derived_tables", False))},
        ]
    )
    with sqlite3.connect(output_gpkg) as con:
        rows.to_sql("proximity_metadata", con, if_exists="replace", index=False)
        register_attribute_table(con, "proximity_metadata")
        con.commit()


def run_proximity(
    config_path: Path,
    classes: Optional[str] = None,
    sample_villages: Optional[int] = None,
    sample_classes: Optional[int] = None,
    village_chunksize: int = 100_000,
    output_gpkg: Optional[Path] = None,
    materialize_derived: bool = False,
    no_derived_views: bool = False,
    refresh_derived_only: bool = False,
    skip_monitor: bool = False,
    force: bool = False,
    debug: bool = False,
) -> None:
    repo_root = find_repo_root(config_path.resolve())
    config = read_yaml(config_path)
    setup_logging(debug, resolve_path(config["pipeline"]["log_path"], repo_root))
    if no_derived_views:
        config["proximity"]["create_derived_views"] = False
    facilities_gpkg = resolve_path(config["outputs"]["facilities_gpkg"], repo_root)
    out_gpkg = output_gpkg or resolve_path(config["outputs"]["proximity_gpkg"], repo_root)
    out_gpkg = out_gpkg if out_gpkg.is_absolute() else repo_root / out_gpkg
    class_filter = [item.strip() for item in classes.split(",") if item.strip()] if classes else None

    if refresh_derived_only:
        expected_villages = output_village_count(out_gpkg, village_context_table(config))
        if expected_villages is None:
            raise RuntimeError(f"Cannot refresh derived proximity outputs because {out_gpkg} has no village layer.")
        all_groups = l3_groups(config, facilities_gpkg, None, None)
        completed_after = completed_l3_classes(out_gpkg, config, expected_villages)
        write_class_map(out_gpkg, config)
        if nearest_lookup_missing(out_gpkg, config):
            rebuild_nearest_lookup(out_gpkg, config, facilities_gpkg)
        if len(completed_after) >= len(all_groups):
            refresh_derived_outputs(
                out_gpkg,
                config,
                materialize=materialize_derived or bool(config["proximity"].get("materialize_derived_tables", False)),
            )
            log.info("Refreshed L1/L2 derived outputs from existing L3 proximity rows.")
        else:
            log.info(
                "Skipping L1/L2 derived views until all L3 classes are complete (%d/%d complete)",
                len(completed_after),
                len(all_groups),
            )
        write_proximity_metadata(out_gpkg, config, len(completed_after), len(all_groups))
        if not skip_monitor:
            from facility_metadata_monitor import write_monitor

            monitor_path = write_monitor(config_path)
            log.info("Updated metadata monitor: %s", monitor_path)
        return

    villages = load_villages(config, repo_root, sample_villages)
    prepare_output_gpkg(out_gpkg, villages, config, force_full=force and not class_filter)
    if force and class_filter:
        delete_l3_classes(out_gpkg, config, class_filter)

    groups = l3_groups(config, facilities_gpkg, class_filter, sample_classes)
    done = completed_l3_classes(out_gpkg, config, len(villages))
    log.info("Prepared %d L3 proximity groups; %d already complete", len(groups), len(done))
    written = 0
    completed_now = 0
    for group in groups:
        facility_class = group["class_l3_facility_class"]
        if facility_class in done:
            continue
        points = load_l3_facility_points(config, facilities_gpkg, facility_class)
        if points.empty:
            log.warning("No facility points for %s", facility_class)
            continue
        rows = build_l3_for_group(out_gpkg, config, villages, group, points, village_chunksize)
        written += rows
        completed_now += 1
        log.info("Wrote L3 proximity for %s (%d facilities, %d rows)", facility_class, len(points), rows)

    with sqlite3.connect(out_gpkg) as con:
        l3_table = config["proximity"]["tables"]["l3"]
        if table_exists(con, l3_table):
            register_attribute_table(con, l3_table)
            con.execute(
                f"create index if not exists idx_proximity_l3_village "
                f"on {sql_ident(l3_table)}({sql_ident(config['proximity']['village_id_col'])})"
            )
            con.execute(
                f"create index if not exists idx_proximity_l3_class "
                f"on {sql_ident(l3_table)}(class_l3_facility_class)"
            )
        con.commit()
    write_class_map(out_gpkg, config)
    all_groups = l3_groups(config, facilities_gpkg, None, None)
    completed_after = completed_l3_classes(out_gpkg, config, len(villages))
    full_l3_complete = len(completed_after) >= len(all_groups)
    if full_l3_complete:
        if nearest_lookup_missing(out_gpkg, config):
            rebuild_nearest_lookup(out_gpkg, config, facilities_gpkg)
        refresh_derived_outputs(
            out_gpkg,
            config,
            materialize=materialize_derived or bool(config["proximity"].get("materialize_derived_tables", False)),
        )
    else:
        log.info(
            "Skipping L1/L2 derived views until all L3 classes are complete (%d/%d complete)",
            len(completed_after),
            len(all_groups),
        )
    write_proximity_metadata(out_gpkg, config, len(completed_after), len(all_groups))
    log.info("Saved proximity GeoPackage: %s (%d new L3 rows)", out_gpkg, written)
    if not skip_monitor:
        from facility_metadata_monitor import write_monitor

        monitor_path = write_monitor(config_path)
        log.info("Updated metadata monitor: %s", monitor_path)


def run_from_args(args: argparse.Namespace) -> None:
    run_proximity(
        config_path=args.config,
        classes=args.classes,
        sample_villages=args.sample_villages,
        sample_classes=args.sample_classes,
        village_chunksize=args.village_chunksize,
        output_gpkg=args.output_gpkg,
        materialize_derived=args.materialize_derived,
        no_derived_views=args.no_derived_views,
        refresh_derived_only=args.refresh_derived_only,
        skip_monitor=args.skip_monitor,
        force=getattr(args, "force", False),
        debug=args.debug,
    )


def build_parser() -> argparse.ArgumentParser:
    repo_root = find_repo_root(Path(__file__).resolve())
    parser = argparse.ArgumentParser(description="Build lean L3-based village-to-facility proximity GPKG.")
    parser.add_argument(
        "--config",
        type=Path,
        default=repo_root / "utilities" / "scripts" / "facilities_utils" / "config" / "facilities_master.yaml",
    )
    parser.add_argument("--classes", type=str, default=None, help="Comma-separated L3 classes to compute.")
    parser.add_argument("--sample-villages", type=int, default=None)
    parser.add_argument("--sample-classes", type=int, default=None)
    parser.add_argument("--village-chunksize", type=int, default=100_000)
    parser.add_argument("--output-gpkg", type=Path, default=None)
    parser.add_argument("--materialize-derived", action="store_true")
    parser.add_argument("--no-derived-views", action="store_true")
    parser.add_argument("--refresh-derived-only", action="store_true", help="Rebuild class-map and L1/L2 derived outputs from existing L3 rows.")
    parser.add_argument("--skip-monitor", action="store_true", help="Skip metadata monitor generation for this run.")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    return parser


def main() -> None:
    run_from_args(build_parser().parse_args())


if __name__ == "__main__":
    main()
