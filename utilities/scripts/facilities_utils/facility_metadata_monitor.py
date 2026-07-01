#!/usr/bin/env python3
"""Generate the nested facilities metadata monitor from config and built assets."""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from facility_cleaners import source_paths
from facility_utils import CLASS_K_COLUMNS, find_repo_root, read_yaml, resolve_path, taxonomy_rows, write_yaml


MAX_CSV_COUNT_BYTES = 50 * 1024 * 1024
TOP_VALUE_LIMIT = 12
L4_VALUE_LIMIT = 250
HIGH_CARDINALITY_COLUMNS = {
    "facility_uid",
    "facility_name",
    "facility_code",
    "village_name",
    "nearest_facility_uid",
    "nearest_facility_name",
}
NUMERIC_COLUMNS = {
    "latitude",
    "longitude",
    "nearest_distance_km",
    "nearest_facility_latitude",
    "nearest_facility_longitude",
    "pincode",
    "establishment_year",
    "district_lgd",
    "membership_count",
}

COLUMN_METADATA: Dict[str, Dict[str, Any]] = {
    "facility_uid": {
        "group": "identity",
        "description": "Stable facility identifier. The facility point layer keeps one row per valid facility uid.",
        "required": True,
    },
    "latitude": {"group": "geometry", "description": "Facility latitude in EPSG:4326.", "required": True},
    "longitude": {"group": "geometry", "description": "Facility longitude in EPSG:4326.", "required": True},
    "facility_name": {"group": "facility_metadata", "description": "Best available display name for the facility."},
    "facility_code": {"group": "facility_metadata", "description": "Useful source code such as school code, IFSC, mandi code, or AISHE code."},
    "class_l1_domain": {"group": "classification", "description": "Broad facility domain.", "required": True},
    "class_l2_filter_group": {"group": "classification", "description": "Access/filter group used for proximity logic.", "required": True},
    "class_l3_facility_class": {"group": "classification", "description": "Canonical facility class. This is the stored proximity search level.", "required": True},
    "class_l4_facility_subtype": {"group": "classification", "description": "Source-derived subtype retained as metadata; it is not traversed for nearest search."},
    "urban_rural": {"group": "facility_metadata", "description": "Urban/rural marker where available."},
    "pincode": {"group": "admin_reference", "description": "Pincode where available and valid."},
    "establishment_year": {"group": "facility_metadata", "description": "Facility establishment year where available and plausible."},
    "district_lgd": {"group": "admin_reference", "description": "Source district LGD reference where available."},
    "village_census11": {"group": "admin_reference", "description": "Source Census 2011 village code where available."},
    "village_name": {"group": "admin_reference", "description": "Source village or local place name where available."},
    "membership_count": {"group": "classification", "description": "Number of L3 memberships attached to the facility."},
}

for class_k in CLASS_K_COLUMNS:
    COLUMN_METADATA[class_k] = {
        "group": "class_k_parameters",
        "description": "Source-specific categorical parameter retained for filtering, audit, or later schema promotion.",
    }


def sql_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def clean_scalar(value: Any) -> Any:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float):
        return round(value, 6)
    return value


def file_info(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    stat = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "size_bytes": int(stat.st_size),
        "size_mb": round(stat.st_size / (1024 * 1024), 3),
        "modified_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat(),
    }


def csv_count(path: Path) -> Optional[int]:
    if not path.exists() or path.stat().st_size > MAX_CSV_COUNT_BYTES:
        return None
    with path.open("rb") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def csv_metadata(path: Path) -> Dict[str, Any]:
    if path.suffix.lower() != ".csv":
        return {}
    if not path.exists():
        return {"csv_rows": None, "csv_rows_status": "missing"}
    rows = csv_count(path)
    if rows is None:
        return {"csv_rows": None, "csv_rows_status": f"not_scanned_larger_than_{MAX_CSV_COUNT_BYTES}_bytes"}
    return {"csv_rows": rows, "csv_rows_status": "counted"}


def table_exists(con: sqlite3.Connection, table_name: str) -> bool:
    return con.execute("select 1 from sqlite_master where type in ('table', 'view') and name = ?", (table_name,)).fetchone() is not None


def table_columns(con: sqlite3.Connection, table_name: str) -> List[str]:
    if not table_exists(con, table_name):
        return []
    return [row[1] for row in con.execute(f"pragma table_info({sql_ident(table_name)})").fetchall()]


def table_count(con: sqlite3.Connection, table_name: str) -> Optional[int]:
    if not table_exists(con, table_name):
        return None
    try:
        return int(con.execute(f"select count(*) from {sql_ident(table_name)}").fetchone()[0])
    except sqlite3.Error:
        return None


def value_filter(column: str) -> str:
    ident = sql_ident(column)
    return f"{ident} is not null and cast({ident} as text) != ''"


def top_values(
    con: sqlite3.Connection,
    table_name: str,
    column: str,
    where_sql: str = "1=1",
    params: Tuple[Any, ...] = (),
    limit: int = TOP_VALUE_LIMIT,
) -> List[Dict[str, Any]]:
    if column not in table_columns(con, table_name):
        return []
    rows = con.execute(
        f"""
        select cast({sql_ident(column)} as text) as value, count(*) as count
        from {sql_ident(table_name)}
        where {where_sql} and {value_filter(column)}
        group by {sql_ident(column)}
        order by count desc, value
        limit {int(limit)}
        """,
        params,
    ).fetchall()
    return [{"value": clean_scalar(value), "count": int(count)} for value, count in rows]


def column_eda(
    con: sqlite3.Connection,
    table_name: str,
    column: str,
    total_rows: Optional[int],
    where_sql: str = "1=1",
    params: Tuple[Any, ...] = (),
    include_distinct: bool = True,
    include_top: bool = True,
    top_limit: int = TOP_VALUE_LIMIT,
) -> Dict[str, Any]:
    columns = table_columns(con, table_name)
    if column not in columns:
        return {"available": False}
    if total_rows is None:
        total_rows = int(con.execute(f"select count(*) from {sql_ident(table_name)} where {where_sql}", params).fetchone()[0])
    non_null = int(
        con.execute(
            f"select count(*) from {sql_ident(table_name)} where {where_sql} and {value_filter(column)}",
            params,
        ).fetchone()[0]
    )
    result: Dict[str, Any] = {
        "available": True,
        "rows_scanned": int(total_rows),
        "non_null_rows": non_null,
        "null_or_blank_rows": int(total_rows) - non_null,
        "coverage": round(non_null / total_rows, 6) if total_rows else 0,
    }
    if include_distinct and column not in HIGH_CARDINALITY_COLUMNS:
        result["unique_values"] = int(
            con.execute(
                f"select count(distinct {sql_ident(column)}) from {sql_ident(table_name)} where {where_sql} and {value_filter(column)}",
                params,
            ).fetchone()[0]
        )
    elif column in HIGH_CARDINALITY_COLUMNS:
        result["unique_values_status"] = "not_counted_high_cardinality"
    if column in NUMERIC_COLUMNS:
        min_value, max_value = con.execute(
            f"select min({sql_ident(column)}), max({sql_ident(column)}) from {sql_ident(table_name)} where {where_sql} and {value_filter(column)}",
            params,
        ).fetchone()
        result["min"] = clean_scalar(min_value)
        result["max"] = clean_scalar(max_value)
    if include_top:
        result["top_values"] = top_values(con, table_name, column, where_sql, params, top_limit)
    return result


def counter_to_eda(counter: Counter, rows_scanned: int, non_null_rows: int, limit: int = TOP_VALUE_LIMIT) -> Dict[str, Any]:
    return {
        "available": True,
        "rows_scanned": int(rows_scanned),
        "non_null_rows": int(non_null_rows),
        "null_or_blank_rows": int(rows_scanned) - int(non_null_rows),
        "coverage": round(non_null_rows / rows_scanned, 6) if rows_scanned else 0,
        "unique_values": len(counter),
        "top_values": [
            {"value": clean_scalar(value), "count": int(count)}
            for value, count in sorted(counter.items(), key=lambda item: (-item[1], str(item[0])))[:limit]
        ],
        "value_list_is_complete": len(counter) <= limit,
    }


def update_counter_from_series(counter: Counter, series: pd.Series) -> int:
    values = series.astype("string").str.strip()
    values = values[values.notna() & (values != "")]
    if values.empty:
        return 0
    counter.update(values.value_counts(dropna=True).to_dict())
    return int(len(values))


def available_csv_columns(path: Path) -> List[str]:
    if not path.exists():
        return []
    return list(pd.read_csv(path, nrows=0).columns)


def scan_membership_metadata(membership_csv: Path, config: Dict[str, Any]) -> Dict[str, Any]:
    configured_k = configured_class_k_columns(config)
    needed_columns = [
        "class_l3_facility_class",
        "class_l4_facility_subtype",
        *configured_k,
    ]
    available = set(available_csv_columns(membership_csv))
    usecols = [column for column in needed_columns if column in available]
    scan = {
        "available": membership_csv.exists(),
        "path": str(membership_csv),
        "configured_class_k_columns": configured_k,
        "total_rows": 0,
        "class_rows": Counter(),
        "l4_by_class": defaultdict(Counter),
        "l4_non_null_by_class": Counter(),
        "class_k_overall": {class_k: Counter() for class_k in configured_k},
        "class_k_non_null_overall": Counter(),
        "class_k_by_class": defaultdict(lambda: {class_k: Counter() for class_k in configured_k}),
        "class_k_non_null_by_class": defaultdict(Counter),
    }
    if not membership_csv.exists() or "class_l3_facility_class" not in usecols:
        return scan
    for chunk in pd.read_csv(membership_csv, usecols=usecols, chunksize=500_000, low_memory=False):
        classes = chunk["class_l3_facility_class"].astype("string").str.strip()
        valid_class = classes.notna() & (classes != "")
        chunk = chunk.loc[valid_class].copy()
        classes = classes.loc[valid_class]
        scan["total_rows"] += len(chunk)
        scan["class_rows"].update(classes.value_counts(dropna=True).to_dict())

        if "class_l4_facility_subtype" in chunk.columns:
            values = chunk["class_l4_facility_subtype"].astype("string").str.strip()
            valid = values.notna() & (values != "")
            if valid.any():
                grouped = pd.DataFrame({"class": classes.loc[valid], "value": values.loc[valid]}).groupby(["class", "value"]).size()
                for (facility_class, value), count in grouped.items():
                    scan["l4_by_class"][str(facility_class)][str(value)] += int(count)
                    scan["l4_non_null_by_class"][str(facility_class)] += int(count)

        for class_k in configured_k:
            if class_k not in chunk.columns:
                continue
            values = chunk[class_k].astype("string").str.strip()
            valid = values.notna() & (values != "")
            if not valid.any():
                continue
            value_counts = values.loc[valid].value_counts(dropna=True)
            scan["class_k_overall"][class_k].update(value_counts.to_dict())
            scan["class_k_non_null_overall"][class_k] += int(valid.sum())
            grouped = pd.DataFrame({"class": classes.loc[valid], "value": values.loc[valid]}).groupby(["class", "value"]).size()
            for (facility_class, value), count in grouped.items():
                scan["class_k_by_class"][str(facility_class)][class_k][str(value)] += int(count)
                scan["class_k_non_null_by_class"][str(facility_class)][class_k] += int(count)
    return scan


def scan_facility_standard_metadata(facility_csv: Path) -> Dict[str, Any]:
    columns = ["urban_rural", "pincode", "establishment_year", "district_lgd", "membership_count"]
    available = set(available_csv_columns(facility_csv))
    usecols = [column for column in columns if column in available]
    counters = {column: Counter() for column in usecols}
    non_null = Counter()
    total_rows = 0
    if not facility_csv.exists() or not usecols:
        return {}
    for chunk in pd.read_csv(facility_csv, usecols=usecols, chunksize=500_000, low_memory=False):
        total_rows += len(chunk)
        for column in usecols:
            non_null[column] += update_counter_from_series(counters[column], chunk[column])
    return {
        column: counter_to_eda(counters[column], total_rows, non_null[column], limit=TOP_VALUE_LIMIT)
        for column in usecols
    }


def gpkg_summary(path: Path) -> Dict[str, Any]:
    summary = file_info(path)
    if not path.exists():
        return summary
    with sqlite3.connect(path) as con:
        summary["layers"] = []
        for table_name, data_type in con.execute("select table_name, data_type from gpkg_contents order by table_name").fetchall():
            summary["layers"].append(
                {
                    "name": table_name,
                    "type": data_type,
                    "rows": table_count(con, table_name),
                    "columns": table_columns(con, table_name),
                }
            )
        summary["views"] = [row[0] for row in con.execute("select name from sqlite_master where type='view' order by name").fetchall()]
    return summary


def load_state(repo_root: Path, config: Dict[str, Any]) -> Dict[str, Any]:
    path = resolve_path(config["pipeline"]["state_path"], repo_root)
    if not path.exists():
        return {"sources": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def output_paths(repo_root: Path, config: Dict[str, Any]) -> Dict[str, Path]:
    return {key: resolve_path(path, repo_root) for key, path in (config.get("outputs") or {}).items()}


def infer_source_classes(config: Dict[str, Any], source_key: str, source: Dict[str, Any]) -> List[str]:
    if source.get("facility_class"):
        return [source["facility_class"]]
    processor = source.get("processor")
    if processor == "health_center_split":
        return sorted(set((source.get("type_to_class") or {}).values()))
    if processor == "school_mhrd":
        classes = set()
        for values in ((config.get("mappings") or {}).get("school") or {}).get("category_to_classes", {}).values():
            classes.update(values or [])
        return sorted(classes)
    if processor == "agri_industry_reclassified":
        return sorted(set(((config.get("mappings") or {}).get("agri_industry") or {}).get("category_to_class", {}).values()))
    return []


def raw_to_standard_columns(source: Dict[str, Any]) -> Dict[str, Any]:
    keys = [
        "id_col",
        "name_col",
        "lat_col",
        "lon_col",
        "subtype_col",
        "facility_code_col",
        "urban_rural_col",
        "pincode_col",
        "establishment_year_col",
        "district_lgd_col",
        "village_census11_col",
        "village_name_col",
        "coord_fallback_col",
        "uid_strategy",
    ]
    return {key: source.get(key) for key in keys if source.get(key)}


def source_catalog(repo_root: Path, config: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    catalog = {}
    for source_key, source in (config.get("sources") or {}).items():
        catalog[source_key] = {
            "active": bool(source.get("active", True)),
            "processor": source.get("processor"),
            "raw_files": [file_info(path) for path in source_paths(repo_root, config, source)],
            "classes_supplied": infer_source_classes(config, source_key, source),
            "raw_to_standard_columns": raw_to_standard_columns(source),
            "class_k_mapping": source.get("class_k") or {},
            "last_run_summary": ((state.get("sources") or {}).get(source_key) or {}).get("summary") or {},
        }
    return catalog


def configured_class_k_columns(config: Dict[str, Any]) -> List[str]:
    configured = set()
    for source in (config.get("sources") or {}).values():
        configured.update((source.get("class_k") or {}).keys())
    return [column for column in CLASS_K_COLUMNS if column in configured]


def class_to_sources(config: Dict[str, Any]) -> Dict[str, List[str]]:
    mapping: Dict[str, List[str]] = {}
    for source_key, source in (config.get("sources") or {}).items():
        for facility_class in infer_source_classes(config, source_key, source):
            mapping.setdefault(facility_class, []).append(source_key)
    return mapping


def class_k_source_details(config: Dict[str, Any], source_keys: Iterable[str], class_k: str) -> Dict[str, Any]:
    labels = []
    source_columns = []
    for source_key in source_keys:
        spec = (((config.get("sources") or {}).get(source_key) or {}).get("class_k") or {}).get(class_k) or {}
        if spec.get("label"):
            labels.append(spec["label"])
        if spec.get("source"):
            source_columns.append(spec["source"])
    return {
        "labels_from_sources": sorted(set(labels)),
        "source_columns": sorted(set(source_columns)),
    }


def membership_counts(con: sqlite3.Connection, membership_table: str) -> Dict[str, Dict[str, int]]:
    if not table_exists(con, membership_table):
        return {}
    rows = con.execute(
        f"""
        select class_l3_facility_class, count(*) as membership_rows, count(distinct facility_uid) as unique_facilities
        from {sql_ident(membership_table)}
        where class_l3_facility_class is not null and class_l3_facility_class != ''
        group by class_l3_facility_class
        """
    ).fetchall()
    return {
        facility_class: {"membership_rows": int(memberships), "unique_facilities": int(facilities)}
        for facility_class, memberships, facilities in rows
    }


def primary_facility_counts(con: sqlite3.Connection, facility_layer: str) -> Dict[str, int]:
    if not table_exists(con, facility_layer):
        return {}
    rows = con.execute(
        f"""
        select class_l3_facility_class, count(*)
        from {sql_ident(facility_layer)}
        where class_l3_facility_class is not null and class_l3_facility_class != ''
        group by class_l3_facility_class
        """
    ).fetchall()
    return {facility_class: int(count) for facility_class, count in rows}


def proximity_status(proximity_gpkg: Path, config: Dict[str, Any]) -> Dict[str, Any]:
    status = {"available": proximity_gpkg.exists(), "path": str(proximity_gpkg)}
    if not proximity_gpkg.exists():
        return status
    tables = config["proximity"]["tables"]
    with sqlite3.connect(proximity_gpkg) as con:
        village_shape_table = tables.get("village_shapes", "village_shapes")
        village_shape_count = table_count(con, village_shape_table) or 0
        village_count = village_shape_count
        l3_count = table_count(con, tables["l3"]) or 0
        class_rows = {}
        if table_exists(con, tables["l3"]):
            for facility_class, rows in con.execute(
                f"""
                select class_l3_facility_class, count(*) as rows
                from {sql_ident(tables['l3'])}
                group by class_l3_facility_class
                order by class_l3_facility_class
                """
            ).fetchall():
                class_rows[facility_class] = {
                    "rows": int(rows),
                    "expected_rows_if_complete": int(village_count),
                    "complete_for_all_villages": int(rows) == village_count,
                }
        status.update(
            {
                "village_shapes": village_shape_count,
                "l3_rows": l3_count,
                "l3_classes_complete": sum(1 for item in class_rows.values() if item["complete_for_all_villages"]),
                "l3_classes_configured": len(taxonomy_rows(config)),
                "nearest_facility_lookup_rows": table_count(con, tables.get("nearest_facilities", "proximity_nearest_facilities")),
                "class_map_rows": table_count(con, tables.get("class_map", "proximity_class_map")),
                "views": [row[0] for row in con.execute("select name from sqlite_master where type='view' order by name").fetchall()],
                "materialized_tables": {
                    key: table_count(con, table)
                    for key, table in {
                        "l2_materialized": tables.get("l2_materialized"),
                        "l1_materialized": tables.get("l1_materialized"),
                    }.items()
                    if table and table_exists(con, table)
                },
                "by_l3_class": class_rows,
            }
        )
    return status


def l4_catalog_for_class(
    con: sqlite3.Connection,
    membership_table: str,
    facility_class: str,
    total_rows: int,
) -> Dict[str, Any]:
    eda = column_eda(
        con,
        membership_table,
        "class_l4_facility_subtype",
        total_rows,
        where_sql="class_l3_facility_class = ?",
        params=(facility_class,),
        include_distinct=True,
        include_top=True,
        top_limit=L4_VALUE_LIMIT,
    )
    eda["value_list_is_complete"] = (eda.get("unique_values") or 0) <= L4_VALUE_LIMIT
    return eda


def l4_eda_from_scan(membership_scan: Dict[str, Any], facility_class: str) -> Dict[str, Any]:
    rows_scanned = int((membership_scan.get("class_rows") or {}).get(facility_class, 0))
    counter = (membership_scan.get("l4_by_class") or {}).get(facility_class, Counter())
    non_null = int((membership_scan.get("l4_non_null_by_class") or {}).get(facility_class, 0))
    return counter_to_eda(counter, rows_scanned, non_null, limit=L4_VALUE_LIMIT)


def class_k_eda_from_scan(membership_scan: Dict[str, Any], facility_class: str, class_k: str) -> Dict[str, Any]:
    rows_scanned = int((membership_scan.get("class_rows") or {}).get(facility_class, 0))
    counter = ((membership_scan.get("class_k_by_class") or {}).get(facility_class) or {}).get(class_k, Counter())
    non_null = int(((membership_scan.get("class_k_non_null_by_class") or {}).get(facility_class) or {}).get(class_k, 0))
    return counter_to_eda(counter, rows_scanned, non_null, limit=TOP_VALUE_LIMIT)


def nested_taxonomy(
    config: Dict[str, Any],
    facilities_gpkg: Path,
    proximity: Dict[str, Any],
    membership_scan: Dict[str, Any],
) -> Dict[str, Any]:
    class_sources = class_to_sources(config)
    rows = taxonomy_rows(config)
    if not facilities_gpkg.exists():
        return {}
    nested: Dict[str, Any] = {}
    with sqlite3.connect(facilities_gpkg) as con:
        membership_table = config["schema"]["membership_table"]
        counts = membership_counts(con, membership_table)
        primary_counts = primary_facility_counts(con, config["schema"]["facility_layer"])
        for row in rows:
            domain = row["class_l1_domain"]
            group = row["class_l2_filter_group"]
            facility_class = row["class_l3_facility_class"]
            domain_node = nested.setdefault(domain, {"summary": {"facility_classes": 0}, "filter_groups": {}})
            group_node = domain_node["filter_groups"].setdefault(
                group,
                {
                    "filter_logic": row["filter_logic"],
                    "summary": {"facility_classes": 0},
                    "classes": {},
                },
            )
            source_keys = class_sources.get(facility_class, [])
            class_k_to_scan = sorted(
                class_k
                for class_k in CLASS_K_COLUMNS
                if class_k_source_details(config, source_keys, class_k)["source_columns"]
            )
            class_counts = counts.get(facility_class, {"membership_rows": 0, "unique_facilities": 0})
            membership_rows = class_counts["membership_rows"]
            class_k_parameters = {}
            for class_k in class_k_to_scan:
                eda = class_k_eda_from_scan(membership_scan, facility_class, class_k)
                if eda.get("non_null_rows", 0) or class_k_source_details(config, source_keys, class_k)["source_columns"]:
                    class_k_parameters[class_k] = {
                        **class_k_source_details(config, source_keys, class_k),
                        "eda": eda,
                    }
            group_node["classes"][facility_class] = {
                "decision": "keep",
                "filter_logic": row["filter_logic"],
                "configured_subtypes": row["configured_subtypes"].split(";") if row["configured_subtypes"] else [],
                "source_trace": {
                    source_key: {
                        "processor": ((config.get("sources") or {}).get(source_key) or {}).get("processor"),
                        "raw_files": ((config.get("sources") or {}).get(source_key) or {}).get("raw_files") or [],
                        "raw_to_standard_columns": raw_to_standard_columns((config.get("sources") or {}).get(source_key) or {}),
                        "class_k_mapping": (((config.get("sources") or {}).get(source_key) or {}).get("class_k") or {}),
                    }
                    for source_key in source_keys
                },
                "counts": {
                    **class_counts,
                    "primary_facility_rows": primary_counts.get(facility_class, 0),
                    "proximity_l3_rows": ((proximity.get("by_l3_class") or {}).get(facility_class) or {}).get("rows"),
                    "proximity_complete_for_all_villages": ((proximity.get("by_l3_class") or {}).get(facility_class) or {}).get(
                        "complete_for_all_villages"
                    ),
                },
                "level_4_subtype": {
                    "output_column": "class_l4_facility_subtype",
                    "role": "deeper source subtype retained as metadata; not used as a proximity traversal level",
                    "actual_values": l4_eda_from_scan(membership_scan, facility_class),
                },
                "class_k_parameters": class_k_parameters,
            }
            domain_node["summary"]["facility_classes"] += 1
            group_node["summary"]["facility_classes"] += 1
            group_node["summary"]["membership_rows"] = group_node["summary"].get("membership_rows", 0) + membership_rows
            domain_node["summary"]["membership_rows"] = domain_node["summary"].get("membership_rows", 0) + membership_rows
    return nested


def column_catalog(config: Dict[str, Any], facilities_gpkg: Path, standard_scan: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    catalog = {}
    facility_columns_available: List[str] = []
    membership_columns_available: List[str] = []
    if not facilities_gpkg.exists():
        facility_columns_available = []
        membership_columns_available = []
    else:
        with sqlite3.connect(facilities_gpkg) as con:
            facility_columns_available = table_columns(con, config["schema"]["facility_layer"])
            membership_columns_available = table_columns(con, config["schema"]["membership_table"])
    configured_k = configured_class_k_columns(config)
    for column in config["schema"]["facility_columns"]:
        meta = COLUMN_METADATA.get(column, {"group": "other", "description": "Configured facility output column."})
        catalog[column] = {
            **meta,
            "include_in_facility_layer": True,
            "include_in_membership_table": column in config["schema"]["membership_columns"],
            "available_in_facility_layer": column in facility_columns_available,
            "available_in_membership_table": column in membership_columns_available,
            "eda_location": (
                "class_k_catalog" if column in CLASS_K_COLUMNS else "l4_subtype_catalog" if column == "class_l4_facility_subtype" else "not_scanned_in_column_catalog"
            ),
        }
        if column in CLASS_K_COLUMNS and column not in configured_k:
            catalog[column]["status"] = "configured_column_but_no_current_source_mapping"
        if standard_scan and column in standard_scan:
            catalog[column]["facility_layer_eda"] = standard_scan[column]
    catalog["geometry"] = {
        "group": "geometry",
        "description": "Point geometry generated from longitude/latitude in EPSG:4326.",
        "include_in_facility_layer": True,
        "available_in_facility_layer": "geom" in facility_columns_available,
    }
    return catalog


def class_k_catalog(config: Dict[str, Any], membership_scan: Dict[str, Any]) -> Dict[str, Any]:
    catalog = {}
    total_rows = int(membership_scan.get("total_rows") or 0)
    for class_k in CLASS_K_COLUMNS:
        labels = []
        source_columns = []
        source_usage = {}
        for source_key, source in (config.get("sources") or {}).items():
            spec = (source.get("class_k") or {}).get(class_k) or {}
            if spec:
                source_usage[source_key] = spec
            if spec.get("label"):
                labels.append(spec["label"])
            if spec.get("source"):
                source_columns.append(spec["source"])
        item = {
            "configured_labels": sorted(set(labels)),
            "configured_source_columns": sorted(set(source_columns)),
            "source_usage": source_usage,
        }
        if source_usage:
            counter = (membership_scan.get("class_k_overall") or {}).get(class_k, Counter())
            non_null = int((membership_scan.get("class_k_non_null_overall") or {}).get(class_k, 0))
            item["membership_table_eda"] = counter_to_eda(counter, total_rows, non_null, limit=TOP_VALUE_LIMIT)
        else:
            item["status"] = "no_current_source_mapping"
        catalog[class_k] = item
    return catalog


def l4_subtype_catalog(config: Dict[str, Any], membership_scan: Dict[str, Any]) -> Dict[str, Any]:
    catalog = {"output_column": "class_l4_facility_subtype", "by_l3_class": {}}
    total_unique = set()
    for row in taxonomy_rows(config):
        facility_class = row["class_l3_facility_class"]
        eda = l4_eda_from_scan(membership_scan, facility_class)
        catalog["by_l3_class"][facility_class] = eda
        for item in eda.get("top_values", []):
            total_unique.add(item["value"])
    catalog["unique_values_seen_in_listed_catalog"] = len(total_unique)
    return catalog


def output_status(repo_root: Path, config: Dict[str, Any]) -> Dict[str, Any]:
    outputs = output_paths(repo_root, config)
    return {
        key: {**file_info(path), **csv_metadata(path)}
        for key, path in outputs.items()
        if key != "metadata_monitor_yaml"
    }


def build_monitor(config_path: Path) -> Dict[str, Any]:
    repo_root = find_repo_root(config_path.resolve())
    config = read_yaml(config_path)
    state = load_state(repo_root, config)
    outputs = output_paths(repo_root, config)
    facilities_gpkg = outputs["facilities_gpkg"]
    proximity_gpkg = outputs["proximity_gpkg"]
    proximity = proximity_status(proximity_gpkg, config)
    membership_scan = scan_membership_metadata(outputs["memberships_csv"], config)
    standard_scan = scan_facility_standard_metadata(outputs["facilities_csv"])
    return {
        "metadata_name": "pan_india_facilities_metadata_monitor",
        "schema_version": "generated_1.0",
        "status": "generated_from_current_config_and_assets",
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source_of_truth": {
            "master_yaml": str(config_path),
            "raw_dir": str(resolve_path(config["pipeline"]["raw_dir"], repo_root)),
            "reference_files": {
                key: file_info(resolve_path(path, repo_root))
                for key, path in (config.get("references") or {}).items()
            },
        },
        "pipeline_design": {
            "facility_asset": "One valid point per facility; multi-class facilities are represented by facility_memberships.",
            "stored_proximity_level": "class_l3_facility_class",
            "l4_policy": "class_l4_facility_subtype is scanned and retained as metadata, but proximity is not traversed at L4.",
            "class_k_policy": "class_k1..class_k8 hold source-specific categorical parameters with source trace and EDA.",
            "derived_levels": "L2 proximity is materialized from L3 rows plus proximity_class_map; L1/domain metrics are optional future derived outputs.",
        },
        "paths": {
            "facilities_gpkg": str(facilities_gpkg),
            "proximity_gpkg": str(proximity_gpkg),
            "metadata_monitor_yaml": str(outputs["metadata_monitor_yaml"]),
        },
        "outputs": output_status(repo_root, config),
        "asset_layers": {
            "facilities_gpkg": gpkg_summary(facilities_gpkg),
            "proximity_gpkg": gpkg_summary(proximity_gpkg),
        },
        "schema": {
            "crs": config["schema"]["crs"],
            "facility_layer": config["schema"]["facility_layer"],
            "membership_table": config["schema"]["membership_table"],
            "facility_columns": config["schema"]["facility_columns"],
            "membership_columns": config["schema"]["membership_columns"],
            "dropped_from_facility_layer": config["schema"].get("dropped_from_facility_layer") or [],
            "column_catalog": column_catalog(config, facilities_gpkg, standard_scan),
            "standard_parameter_catalog": standard_scan,
        },
        "source_catalog": source_catalog(repo_root, config, state),
        "classification_summary": {
            "domain_count": len(config.get("taxonomy") or {}),
            "filter_group_count": len({row["class_l2_filter_group"] for row in taxonomy_rows(config)}),
            "facility_class_count": len(taxonomy_rows(config)),
            "class_k_count": len(CLASS_K_COLUMNS),
        },
        "nested_taxonomy": nested_taxonomy(config, facilities_gpkg, proximity, membership_scan),
        "l4_subtype_catalog": l4_subtype_catalog(config, membership_scan),
        "class_k_catalog": class_k_catalog(config, membership_scan),
        "proximity_status": proximity,
        "editing_guidance": {
            "safe_to_edit": "Edit facilities_master.yaml, not this generated monitor.",
            "regenerate_command": "uv run --with pandas --with numpy --with pyyaml python utilities/scripts/facilities_utils/facility_pipeline.py monitor",
            "new_dataset_template": "utilities/scripts/facilities_utils/config/new_dataset_schema_template.yaml",
        },
    }


def write_monitor(config_path: Path) -> Path:
    repo_root = find_repo_root(config_path.resolve())
    config = read_yaml(config_path)
    output = resolve_path(config["outputs"]["metadata_monitor_yaml"], repo_root)
    write_yaml(output, build_monitor(config_path))
    return output


def build_parser() -> argparse.ArgumentParser:
    repo_root = find_repo_root(Path(__file__).resolve())
    parser = argparse.ArgumentParser(description="Generate nested facilities metadata monitor YAML.")
    parser.add_argument(
        "--config",
        type=Path,
        default=repo_root / "utilities" / "scripts" / "facilities_utils" / "config" / "facilities_master.yaml",
    )
    return parser


def main() -> None:
    output = write_monitor(build_parser().parse_args().config)
    print(output)


if __name__ == "__main__":
    main()
