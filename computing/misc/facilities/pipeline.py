"""Facilities inventory and live proximity pipeline from local GeoPackages."""

from __future__ import annotations

import argparse
import json
import math
import re
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping

import geopandas as gpd
import pandas as pd
from django.conf import settings
from scipy.spatial import cKDTree

from computing.misc.local_pipeline import AdminScope, CSAdminSource, StandardRequest, load_config
from computing.misc.local_pipeline.admin import (
    ADMIN_COLUMN_DESCRIPTIONS,
    admin_output_frame,
    admin_presentation_frame,
)
from computing.misc.local_pipeline.batch import load_request_file
from computing.misc.local_pipeline.schema import (
    STATUS_COMPUTED,
    STATUS_NO_DATA,
    STATUS_NO_VILLAGE_ID,
    OutputOptions,
    resolve_output_options,
    status_column_config,
)
from computing.misc.local_pipeline.gpkg import (
    IndexSpec,
    column_expression,
    connect_gpkg,
    ensure_indexes,
    quote_identifier,
    read_table,
)
from computing.misc.local_pipeline.outputs import (
    OutputBundle,
    column_dictionary,
    frame_profile,
    input_signatures,
    mark_cached_result,
    scope_output_identity,
    slug,
    stable_hash,
    utc_now_text,
)
from computing.misc.local_pipeline.publish import publish_gpkg_layer
from nrm_app.celery import app
from utilities.constants import FACILITIES_GEOSERVER_WORKSPACE


CONFIG_PATH = Path(__file__).with_name("facilities_pipeline.yaml")
ALGORITHM = "local-facilities-live-proximity"
ALGORITHM_VERSION = "1.2"


def _repo_path(path: str | Path) -> Path:
    path = Path(path)
    if path.is_absolute():
        return path
    base_dir = Path(settings.BASE_DIR) if settings.configured else Path.cwd()
    return base_dir / path


def _layer_name(prefix: str, district: str | None, tehsil: str | None) -> str:
    return f"{prefix}_{slug(district)}_{slug(tehsil)}".strip("_")


def _cli_request(state: str, district: str, tehsil: str, sync_to_geoserver: bool = True) -> StandardRequest:
    return StandardRequest.from_mapping(
        {
            "scope": {
                "level": "tehsil",
                "state_name": state,
                "district_name": district,
                "tehsil_name": tehsil,
            },
            "publish": {"sync_to_geoserver": sync_to_geoserver, "overwrite": True},
            "outputs": {"geoserver": sync_to_geoserver},
        }
    )


def _ensure_facility_indexes(config: Mapping[str, Any]) -> list[str]:
    table = config["sources"]["facilities_layer"]
    specs = (
        IndexSpec(
            name=f"idx_{table}_class_l3_lat_lon",
            table=table,
            expressions=(
                column_expression("class_l3_facility_class"),
                column_expression("latitude"),
                column_expression("longitude"),
            ),
        ),
    )
    return ensure_indexes(_repo_path(config["sources"]["facilities_gpkg"]), specs)


def _taxonomy(config: Mapping[str, Any]) -> pd.DataFrame:
    classification_path = config.get("sources", {}).get("classification_yaml")
    if classification_path:
        classification = load_config(_repo_path(classification_path))
        rows: list[dict[str, Any]] = []
        l2_rollups = {
            item["key"]: item.get("rollup", "direct")
            for item in classification.get("l2_groups", [])
        }
        for sort_order, item in enumerate(classification.get("l3_classes", [])):
            rows.append(
                {
                    "class_l1_domain": item.get("class_l1_domain"),
                    "class_l2_filter_group": item.get("class_l2_filter_group"),
                    "class_l3_facility_class": item.get("key"),
                    "class_l3_label": item.get("label"),
                    "configured_subtypes": ";".join(item.get("configured_subtypes", []) or []),
                    "filter_logic": l2_rollups.get(item.get("class_l2_filter_group"), "direct"),
                    "sort_order": sort_order,
                }
            )
        taxonomy = pd.DataFrame(rows)
        if not taxonomy.empty:
            return taxonomy

    taxonomy = read_table(
        _repo_path(config["sources"]["facilities_gpkg"]),
        config["sources"]["taxonomy_table"],
    )
    if "sort_order" in taxonomy.columns:
        taxonomy = taxonomy.sort_values("sort_order")
    return taxonomy


def _classification(config: Mapping[str, Any]) -> dict[str, Any]:
    path = config.get("sources", {}).get("classification_yaml")
    if path:
        return load_config(_repo_path(path))
    taxonomy = _taxonomy(config)
    l2_groups: list[dict[str, Any]] = []
    for group, rows in taxonomy.groupby("class_l2_filter_group", sort=False):
        logic_values = [value for value in rows.get("filter_logic", pd.Series(dtype=str)).dropna().astype(str).unique() if value]
        l2_groups.append(
            {
                "key": group,
                "label": str(group).replace("_", " ").title(),
                "rollup": logic_values[0] if logic_values else "direct",
                "class_l1_domain": rows["class_l1_domain"].dropna().iloc[0] if "class_l1_domain" in rows and not rows["class_l1_domain"].dropna().empty else None,
            }
        )
    return {
        "access_rollup_notes": {},
        "l2_groups": l2_groups,
        "l3_classes": [
            {
                "key": row.class_l3_facility_class,
                "label": str(row.class_l3_facility_class).replace("_", " ").title(),
                "class_l1_domain": getattr(row, "class_l1_domain", None),
                "class_l2_filter_group": getattr(row, "class_l2_filter_group", None),
            }
            for row in taxonomy.itertuples(index=False)
        ],
    }


def _bbox(admin_rows) -> tuple[float, float, float, float]:
    minx, miny, maxx, maxy = admin_rows.total_bounds
    return float(minx), float(miny), float(maxx), float(maxy)


def _read_facilities_bbox(
    config: Mapping[str, Any],
    bbox: tuple[float, float, float, float],
    *,
    class_l3_values: list[str] | None = None,
    expansion_degrees: float = 0,
) -> gpd.GeoDataFrame:
    minx, miny, maxx, maxy = bbox
    minx -= expansion_degrees
    miny -= expansion_degrees
    maxx += expansion_degrees
    maxy += expansion_degrees
    path = _repo_path(config["sources"]["facilities_gpkg"])
    table = config["sources"]["facilities_layer"]
    columns = ["fid", *config["facility_columns"]]
    select_columns = ", ".join(f"f.{quote_identifier(col)}" for col in columns)
    if class_l3_values:
        placeholders = ", ".join(["?"] * len(class_l3_values))
        params: list[Any] = [*class_l3_values, minx, maxx, miny, maxy]
        sql = f"""
            SELECT {select_columns}
            FROM {quote_identifier(table)} f
            WHERE f.class_l3_facility_class IN ({placeholders})
              AND f.longitude BETWEEN ? AND ?
              AND f.latitude BETWEEN ? AND ?
              AND f.latitude IS NOT NULL
              AND f.longitude IS NOT NULL
        """
    else:
        params = [minx, maxx, miny, maxy]
        sql = f"""
            SELECT {select_columns}
            FROM {quote_identifier(table)} f
            JOIN rtree_facilities_geom r ON r.id = f.fid
            WHERE r.maxx >= ?
              AND r.minx <= ?
              AND r.maxy >= ?
              AND r.miny <= ?
              AND f.latitude IS NOT NULL
              AND f.longitude IS NOT NULL
        """
    with connect_gpkg(path, read_only=True) as connection:
        frame = pd.read_sql_query(sql, connection, params=params)
    if frame.empty:
        return gpd.GeoDataFrame(frame, geometry=[], crs="EPSG:4326")
    geometry = gpd.points_from_xy(frame["longitude"], frame["latitude"], crs="EPSG:4326")
    return gpd.GeoDataFrame(frame, geometry=geometry)


def _inventory(candidates: gpd.GeoDataFrame, admin_rows) -> gpd.GeoDataFrame:
    if candidates.empty:
        return candidates
    admin_context = admin_rows[["fid", "pc11_village_id", "village_id", "NAME", "state_name", "district_name", "TEHSIL", "geometry"]].copy()
    admin_context["_admin_key"] = admin_context["fid"]
    joined = gpd.sjoin(candidates, admin_context, how="inner", predicate="intersects")
    if "index_right" in joined.columns:
        joined = joined.drop(columns=["index_right"])
    joined["inside_requested_scope"] = True
    joined["facilities_layer_kind"] = "inventory"
    joined["title"] = joined["facility_name"].fillna(joined["facility_uid"])
    return joined.drop_duplicates(subset=["facility_uid", "_admin_key"])


def _village_points(admin_rows) -> pd.DataFrame:
    rows = admin_rows.copy()
    points = rows.geometry.representative_point()
    frame = pd.DataFrame(rows.drop(columns=["geometry"]))
    frame["_admin_key"] = frame["fid"]
    frame["_village_lon"] = points.x
    frame["_village_lat"] = points.y
    return frame


def _haversine_km(lat1, lon1, lat2, lon2, radius_km: float) -> float:
    lat1 = math.radians(float(lat1))
    lon1 = math.radians(float(lon1))
    lat2 = math.radians(float(lat2))
    lon2 = math.radians(float(lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * radius_km * math.asin(math.sqrt(a))


def _candidate_pool(
    config: Mapping[str, Any],
    bbox: tuple[float, float, float, float],
    taxonomy: pd.DataFrame,
) -> tuple[gpd.GeoDataFrame, dict[str, Any]]:
    search = config["search"]
    base = _read_facilities_bbox(config, bbox, expansion_degrees=float(search["base_expansion_degrees"]))
    required = taxonomy["class_l3_facility_class"].dropna().astype(str).tolist()
    covered = set(base["class_l3_facility_class"].dropna().astype(str)) if not base.empty else set()
    missing = [value for value in required if value not in covered]
    supplemental = gpd.GeoDataFrame(pd.DataFrame(), geometry=[], crs="EPSG:4326")
    if missing:
        supplemental = _read_facilities_bbox(
            config,
            bbox,
            class_l3_values=missing,
            expansion_degrees=float(search["supplemental_expansion_degrees"]),
        )
    frames = [frame.dropna(axis=1, how="all") for frame in (base, supplemental) if not frame.empty]
    pool = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if pool.empty:
        pool = gpd.GeoDataFrame(pool, geometry=[], crs="EPSG:4326")
    else:
        pool = gpd.GeoDataFrame(pool, geometry="geometry", crs="EPSG:4326").drop_duplicates(subset=["facility_uid"])
    metadata = {
        "base_candidates": int(len(base)),
        "supplemental_candidates": int(len(supplemental)),
        "candidate_pool": int(len(pool)),
        "missing_after_supplemental": sorted(set(required) - set(pool["class_l3_facility_class"].dropna().astype(str))) if not pool.empty else required,
    }
    return pool, metadata


def _nearest(
    pool: gpd.GeoDataFrame,
    village_points: pd.DataFrame,
    taxonomy: pd.DataFrame,
    classification: Mapping[str, Any],
    inside_facility_ids: set[Any],
    radius_km: float,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    if pool.empty or village_points.empty:
        return gpd.GeoDataFrame(pd.DataFrame(), geometry=[], crs="EPSG:4326"), pd.DataFrame()
    nearest_rows: list[dict[str, Any]] = []
    point_xy = village_points[["_village_lon", "_village_lat"]].to_numpy()
    for tax in taxonomy.itertuples(index=False):
        class_l3 = str(tax.class_l3_facility_class)
        candidates = pool[pool["class_l3_facility_class"] == class_l3].reset_index(drop=True)
        if candidates.empty:
            continue
        tree = cKDTree(candidates[["longitude", "latitude"]].to_numpy())
        distances, indexes = tree.query(point_xy, k=1)
        for village_idx, candidate_idx in enumerate(indexes):
            village = village_points.iloc[village_idx]
            facility = candidates.iloc[int(candidate_idx)]
            distance_km = _haversine_km(
                village["_village_lat"],
                village["_village_lon"],
                facility["latitude"],
                facility["longitude"],
                radius_km,
            )
            row = {
                "_admin_key": village.get("_admin_key"),
                "fid": village.get("fid"),
                "pc11_village_id": village.get("pc11_village_id"),
                "village_id": village.get("village_id"),
                "village_name": village.get("NAME"),
                "state_name": village.get("state_name"),
                "district_name": village.get("district_name"),
                "TEHSIL": village.get("TEHSIL"),
                "service_level": "l3",
                "class_l1_domain": getattr(tax, "class_l1_domain"),
                "class_l2_filter_group": getattr(tax, "class_l2_filter_group"),
                "class_l3_facility_class": class_l3,
                "class_l3_label": getattr(tax, "class_l3_label", class_l3.replace("_", " ").title()),
                "nearest_distance_km": round(distance_km, 6),
                "inside_requested_scope": bool(facility["facility_uid"] in inside_facility_ids),
                "facilities_layer_kind": "nearest",
                "title": facility.get("facility_name") or facility.get("facility_uid"),
            }
            for column in [
                "facility_uid",
                "facility_name",
                "facility_code",
                "latitude",
                "longitude",
                "class_l4_facility_subtype",
                "urban_rural",
                "pincode",
                "village_census11",
            ]:
                row[column] = facility.get(column)
            row["geometry"] = facility.geometry
            nearest_rows.append(row)
    nearest = gpd.GeoDataFrame(nearest_rows, geometry="geometry", crs="EPSG:4326")
    village_service = _village_service(village_points, nearest, classification)
    return nearest, village_service


def _village_service(village_points: pd.DataFrame, nearest: pd.DataFrame, classification: Mapping[str, Any]) -> pd.DataFrame:
    base = village_points.drop(columns=["_village_lon", "_village_lat"]).copy()
    if nearest.empty:
        return base
    l3 = nearest.copy()
    l3["_slug"] = l3["class_l3_facility_class"].map(slug)
    for metric, suffix in (
        ("nearest_distance_km", "distance_km"),
        ("facility_uid", "facility_uid"),
        ("inside_requested_scope", "inside_scope"),
    ):
        pivot = l3.pivot_table(index="_admin_key", columns="_slug", values=metric, aggfunc="first")
        pivot.columns = [f"l3_{col}_{suffix}" for col in pivot.columns]
        base = base.merge(pivot.reset_index(), on="_admin_key", how="left")
    l2_rollups = {
        item["key"]: str(item.get("rollup") or "direct").lower()
        for item in classification.get("l2_groups", [])
    }
    selected_l2_rows: list[pd.Series] = []
    for _, group in l3.groupby(["_admin_key", "class_l2_filter_group"], dropna=False):
        distances = pd.to_numeric(group["nearest_distance_km"], errors="coerce")
        if distances.dropna().empty:
            continue
        rollup = l2_rollups.get(str(group["class_l2_filter_group"].iloc[0]), "direct")
        selected_index = distances.idxmax() if rollup == "max" else distances.idxmin()
        selected_l2_rows.append(l3.loc[selected_index])
    l2 = pd.DataFrame(selected_l2_rows)
    if l2.empty:
        base["facilities_layer_kind"] = "village_service"
        base["title"] = base["NAME"].fillna(base["fid"])
        return base
    l2["_slug"] = l2["class_l2_filter_group"].map(slug)
    for metric, suffix in (
        ("nearest_distance_km", "distance_km"),
        ("facility_uid", "facility_uid"),
        ("class_l3_facility_class", "selected_l3"),
        ("class_l3_label", "selected_l3_label"),
    ):
        pivot = l2.pivot_table(index="_admin_key", columns="_slug", values=metric, aggfunc="first")
        pivot.columns = [f"l2_{col}_{suffix}" for col in pivot.columns]
        base = base.merge(pivot.reset_index(), on="_admin_key", how="left")
    base["facilities_layer_kind"] = "village_service"
    base["title"] = base["NAME"].fillna(base["fid"])
    return base


def _useful_value(value: Any) -> str | None:
    if value is None or value != value:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "0.0"}:
        return None
    return text


def _clean_facility_text(value: Any) -> str | None:
    """Clean compact labels for report-facing nearest-facility cells."""

    text = _useful_value(value)
    if not text:
        return None
    text = re.sub(r"(?<=[A-Za-z])[.,](?=[A-Za-z])", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _facility_detail(row: pd.Series | None) -> str | None:
    """Build a compact human-readable facility detail for report CSVs."""

    if row is None:
        return None
    parts: list[str] = []
    for column in ("facility_name", "class_l4_facility_subtype", "class_l3_label", "facility_code"):
        value = _clean_facility_text(row.get(column)) if column != "facility_code" else _useful_value(row.get(column))
        if value:
            parts.append(value if column != "facility_code" else f"code {value}")
    scope_value = row.get("inside_requested_scope")
    if scope_value is not None and scope_value == scope_value:
        parts.append("inside scope" if bool(scope_value) else "outside scope")
    return " | ".join(dict.fromkeys(parts)) or None


def _output_contract(config: Mapping[str, Any]) -> Mapping[str, Any]:
    return config.get("output_contract", {})


def _l2_output_column(group: Mapping[str, Any], config: Mapping[str, Any]) -> str:
    key = str(group["key"])
    configured = _output_contract(config).get("l2_distance_columns", {})
    if key in configured:
        return str(configured[key])
    return f"{slug(key)}_cat_distance_km"


def _l3_classes_by_group(classification: Mapping[str, Any]) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for item in classification.get("l3_classes", []):
        grouped.setdefault(item["class_l2_filter_group"], []).append(item)
    return grouped


def _service_output_columns(classification: Mapping[str, Any], config: Mapping[str, Any]) -> list[str]:
    """Report columns derived from the classification structure: for each L2
    group the category distance column, then the nearest-facility detail and
    distance pair for each of its L3 classes."""

    l3_by_l2 = _l3_classes_by_group(classification)
    columns: list[str] = []
    for group in classification.get("l2_groups", []):
        columns.append(_l2_output_column(group, config))
        for item in l3_by_l2.get(group["key"], []):
            l3_slug = slug(item["key"])
            columns.append(f"nearest_{l3_slug}")
            columns.append(f"nearest_{l3_slug}_distance_km")
    return columns


def _column_descriptions(classification: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, str]:
    """Human-readable descriptions for report columns, driven by the
    classification YAML description templates."""

    templates = classification.get("output_column_descriptions", {})
    category_templates = templates.get("category_distance", {})
    descriptions = dict(ADMIN_COLUMN_DESCRIPTIONS)
    status_name, _ = status_column_config(config)
    if status_name and templates.get("status"):
        descriptions[status_name] = str(templates["status"])
    l3_by_l2 = _l3_classes_by_group(classification)
    for group in classification.get("l2_groups", []):
        rollup = str(group.get("rollup") or "direct").lower()
        label = str(group.get("label") or group["key"])
        template = category_templates.get(rollup)
        if template:
            descriptions[_l2_output_column(group, config)] = str(template).format(label=label)
        for item in l3_by_l2.get(group["key"], []):
            l3_label = str(item.get("label") or item["key"])
            l3_slug = slug(item["key"])
            if templates.get("nearest_facility"):
                descriptions[f"nearest_{l3_slug}"] = str(templates["nearest_facility"]).format(label=l3_label)
            if templates.get("nearest_distance"):
                descriptions[f"nearest_{l3_slug}_distance_km"] = str(templates["nearest_distance"]).format(label=l3_label)
    return descriptions


def _machine_column_describer(classification: Mapping[str, Any], config: Mapping[str, Any]):
    """Describe both report columns and the verbose `l2_*`/`l3_*` GPKG columns."""

    descriptions = _column_descriptions(classification, config)
    labels = {slug(item["key"]): str(item.get("label") or item["key"]) for item in classification.get("l3_classes", [])}
    labels.update({slug(group["key"]): str(group.get("label") or group["key"]) for group in classification.get("l2_groups", [])})
    patterns = (
        ("l2_", "_selected_l3_label", "Label of the L3 facility class selected for the {label} group."),
        ("l2_", "_selected_l3", "L3 facility class selected for the {label} group."),
        ("l2_", "_distance_km", "Access distance in km for the {label} group."),
        ("l2_", "_facility_uid", "Identifier of the facility selected for the {label} group."),
        ("l3_", "_distance_km", "Distance in km to the nearest {label}."),
        ("l3_", "_facility_uid", "Identifier of the nearest {label}."),
        ("l3_", "_inside_scope", "Whether the nearest {label} lies inside the requested boundary."),
    )

    def describe(name: str) -> str | None:
        if name in descriptions:
            return descriptions[name]
        for prefix, suffix, template in patterns:
            if name.startswith(prefix) and name.endswith(suffix):
                stem = name[len(prefix) : -len(suffix)]
                if stem in labels:
                    return template.format(label=labels[stem])
        return None

    return describe


def _status_value(village_id: Any, has_candidates: bool) -> str:
    if pd.isna(village_id):
        return STATUS_NO_VILLAGE_ID
    if not has_candidates:
        return STATUS_NO_DATA
    return STATUS_COMPUTED


def _focused_service_frame(
    village_service_gdf: gpd.GeoDataFrame,
    nearest: pd.DataFrame,
    classification: Mapping[str, Any],
    config: Mapping[str, Any],
) -> pd.DataFrame:
    """Return the human-readable village x service matrix for report CSVs:
    admin columns, the status column, then the configured service columns."""

    raw_frame = village_service_gdf.drop(columns=["geometry"], errors="ignore")
    frame = admin_presentation_frame(raw_frame)
    admin_columns = list(frame.columns)
    frame["_admin_key"] = raw_frame["_admin_key"].to_numpy()
    service_rows = raw_frame.set_index("_admin_key", drop=False)
    status_name, _ = status_column_config(config)
    nearest_by_village_l3: dict[tuple[Any, str], pd.Series] = {}
    if not nearest.empty:
        for _, row in nearest.iterrows():
            nearest_by_village_l3[(row.get("_admin_key"), row.get("class_l3_facility_class"))] = row

    rows: list[dict[str, Any]] = []
    l3_by_l2 = _l3_classes_by_group(classification)
    for _, source in frame.iterrows():
        row = source.to_dict()
        admin_key = row.pop("_admin_key", None)
        service = service_rows.loc[admin_key] if admin_key in service_rows.index else pd.Series(dtype=object)
        has_village_id = pd.notna(row.get("village_id"))
        if status_name:
            row[status_name] = _status_value(row.get("village_id"), not nearest.empty)
        if not has_village_id or nearest.empty:
            rows.append(row)
            continue

        for group in classification.get("l2_groups", []):
            group_key = group["key"]
            group_slug = slug(group_key)
            row[_l2_output_column(group, config)] = service.get(f"l2_{group_slug}_distance_km")
            for item in l3_by_l2.get(group_key, []):
                l3_key = item["key"]
                l3_slug = slug(l3_key)
                nearest_row = nearest_by_village_l3.get((admin_key, l3_key))
                row[f"nearest_{l3_slug}"] = _facility_detail(nearest_row)
                row[f"nearest_{l3_slug}_distance_km"] = None if nearest_row is None else nearest_row.get("nearest_distance_km")
        rows.append(row)
    ordered = [*admin_columns]
    if status_name:
        ordered.append(status_name)
    ordered.extend(_service_output_columns(classification, config))
    return pd.DataFrame(rows).reindex(columns=list(dict.fromkeys(ordered)))


def _column_reference_lines(column_entries: list[Mapping[str, Any]]) -> list[str]:
    lines = [
        "## Column Reference",
        "",
        "| Column | Type | Description |",
        "| --- | --- | --- |",
    ]
    for entry in column_entries:
        description = str(entry.get("description") or "").replace("|", "\\|").replace("\n", " ")
        lines.append(f"| `{entry['column']}` | {entry.get('datatype', '')} | {description} |")
    lines.append("")
    return lines


def _readme_lines(
    request: StandardRequest,
    config: Mapping[str, Any],
    result_name: str,
    result: Mapping[str, Any],
    column_entries: list[Mapping[str, Any]] | None = None,
) -> list[str]:
    lines = [
        f"# {result_name}",
        "",
        f"Generated at: `{utc_now_text()}`",
        "",
        "## What This Dataset Is For",
        "",
        "This dataset helps a village, tehsil, or district understand how far people are from important public services and rural infrastructure. It is designed for Core Stack reports, local planning conversations, and quick Excel-based review.",
        "",
        "The source facility points were cleaned into the Core Stack pan-India facilities GeoPackage. At runtime, this pipeline selects only the requested geography from `cs_admin_standard.gpkg`, finds nearby facilities from `cs_pan_india_facilities.gpkg`, and writes the result back with village boundaries so the data can be opened directly in GIS or published to GeoServer.",
        "",
        "## Request",
        "",
        f"- Level: `{request.scope.level}`",
        f"- State: `{request.scope.state_name}`",
        f"- District: `{request.scope.district_name}`",
        f"- Tehsil/block: `{request.scope.tehsil_name}`",
        "",
        "## Data Quality",
        "",
        f"- Village rows: `{result.get('village_rows')}`",
        f"- Inventory facilities: `{result.get('inventory_rows')}`",
        f"- Nearest rows: `{result.get('nearest_rows')}`",
        f"- Candidate pool rows: `{result.get('candidate_pool')}`",
        f"- Focused service rows: `{result.get('focused_service_rows')}`",
        "",
        "## How To Read The Distance Columns",
        "",
        "- `*_cat_distance_km` columns summarize a service category for the village. For essentials bundles (schooling tiers, basic health, financial inclusion) they use the farthest required service, so a low value means the whole baseline bundle is nearby. For opportunity gateways (higher education, advanced health, markets, post-harvest) they use the nearest option, since reaching any one member opens that access. Single-service groups use the nearest point directly.",
        "- `nearest_<facility>` columns hold a compact facility detail (name, subtype, code, inside/outside the requested boundary); the paired `nearest_<facility>_distance_km` column holds the great-circle distance in km from the village locality point.",
        "",
        "## CSV And GIS Outputs",
        "",
        "The CSV holds the human-readable village service columns without geometry so it can be opened directly in Excel or report tooling. The GeoPackage keeps the village geometry plus the full machine columns (`l2_*`, `l3_*`) so the same result can be mapped or re-processed without another join.",
        "",
    ]
    if column_entries:
        lines.extend(_column_reference_lines(column_entries))
    geoserver = result.get("geoserver") or {}
    if geoserver.get("wfs_url") or geoserver.get("wms_url"):
        lines.extend(
            [
                "## GeoServer Layer",
                "",
                f"- WFS GeoJSON: {geoserver.get('wfs_url')}",
                f"- WMS layer: {geoserver.get('wms_url')}",
                "",
            ]
        )
    lines.extend(["## Cautions", ""])
    lines.extend([f"- {item}" for item in config.get("readme", {}).get("cautions", [])])
    return lines


def _stac_fragment(config: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": result["layer_name"],
        "properties": {
            "title": config["title"],
            "description": config["description"],
            "algorithm": ALGORITHM,
            "algorithm_version": ALGORITHM_VERSION,
            "generated_at_utc": utc_now_text(),
            "source_facilities_gpkg": config["sources"]["facilities_gpkg"],
            "source_admin_gpkg": config["sources"]["admin_gpkg"],
        },
        "assets": {
            "data": {"href": result.get("gpkg_path"), "type": "application/geopackage+sqlite3"},
            "readme": {"href": result.get("readme_path"), "type": "text/markdown"},
        },
    }


def _cache_input_signatures(config: Mapping[str, Any], config_path: str | Path) -> dict[str, dict[str, Any]]:
    sources = config.get("sources", {})
    paths: dict[str, str | Path] = {
        "pipeline_config": _repo_path(config_path),
        "admin_gpkg": _repo_path(sources["admin_gpkg"]),
        "facilities_gpkg": _repo_path(sources["facilities_gpkg"]),
    }
    if sources.get("classification_yaml"):
        paths["classification_yaml"] = _repo_path(sources["classification_yaml"])
    return input_signatures(paths)


def _cache_key(request: StandardRequest, outputs: OutputOptions) -> str:
    publish_options = asdict(request.publish)
    publish_options.pop("use_pregenerated", None)
    return stable_hash(
        {
            "algorithm": ALGORITHM,
            "algorithm_version": ALGORITHM_VERSION,
            "scope": asdict(request.scope),
            "outputs": asdict(outputs),
            "publish": publish_options,
        }
    )


def _required_result_paths(outputs: OutputOptions, request: StandardRequest) -> tuple[str, ...]:
    required: list[str] = []
    if outputs.metadata:
        required.append("run_metadata_path")
    if outputs.gpkg or (request.publish.sync_to_geoserver and outputs.geoserver):
        required.append("gpkg_path")
    if outputs.csv:
        required.append("csv_path")
    if outputs.readme:
        required.append("readme_path")
    if outputs.stac:
        required.append("stac_fragment_path")
    if request.publish.sync_to_geoserver and outputs.geoserver:
        required.append("geoserver_links_path")
    return tuple(dict.fromkeys(required))


def run_facilities_pipeline(
    request: StandardRequest,
    *,
    config_path: str | Path = CONFIG_PATH,
) -> dict[str, Any]:
    started = time.perf_counter()
    timings: dict[str, float] = {}
    config = load_config(config_path)
    outputs = resolve_output_options(request, config)
    output_config = config["output"]
    output_parts, layer_name = scope_output_identity(output_config["layer_prefix"], request.scope)
    output_root = _repo_path(output_config["root"]).joinpath(*output_parts)
    bundle = OutputBundle(output_root, layer_name)
    cache_key = _cache_key(request, outputs)
    cache_signatures = _cache_input_signatures(config, config_path)
    required_result_paths = _required_result_paths(outputs, request)
    if request.publish.use_pregenerated:
        cached = bundle.cached_result(
            cache_key=cache_key,
            signatures=cache_signatures,
            required_result_paths=required_result_paths,
        )
        if cached:
            return mark_cached_result(cached, started)

    t0 = time.perf_counter()
    created_facility_indexes = _ensure_facility_indexes(config)
    timings["ensure_facility_indexes_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    admin_source = CSAdminSource(_repo_path(config["sources"]["admin_gpkg"]), table_name=config["sources"]["admin_layer"])
    admin_selection = admin_source.read_scope(AdminScope.from_mapping(asdict(request.scope)), include_geometry=True)
    admin_rows = admin_selection.rows
    bounds = _bbox(admin_rows)
    village_points = _village_points(admin_rows)
    timings["read_admin_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    classification = _classification(config)
    taxonomy = _taxonomy(config)
    inventory_candidates = _read_facilities_bbox(config, bounds, expansion_degrees=0)
    inventory = _inventory(inventory_candidates, admin_rows)
    timings["read_inventory_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    pool, pool_metadata = _candidate_pool(config, bounds, taxonomy)
    nearest, village_service = _nearest(
        pool,
        village_points,
        taxonomy,
        classification,
        set(inventory["facility_uid"]) if not inventory.empty else set(),
        float(config["search"]["earth_radius_km"]),
    )
    timings["build_nearest_seconds"] = round(time.perf_counter() - t0, 3)

    village_service_values = village_service.drop(columns=["fid"], errors="ignore")
    village_service_gdf = admin_rows[["fid", "geometry"]].copy()
    village_service_gdf["_admin_key"] = village_service_gdf["fid"]
    village_service_gdf = village_service_gdf.merge(village_service_values, on="_admin_key", how="left")
    village_service_gdf = gpd.GeoDataFrame(village_service_gdf, geometry="geometry", crs=admin_rows.crs)
    status_name, status_outputs = status_column_config(config)
    if status_name:
        village_service_gdf[status_name] = [
            _status_value(village_id, not nearest.empty)
            for village_id in village_service_gdf["village_id"]
        ]
    focused_services = _focused_service_frame(village_service_gdf, nearest, classification, config)
    descriptions = _column_descriptions(classification, config)
    describe_machine = _machine_column_describer(classification, config)
    report_frame = focused_services
    if status_name and "csv" not in status_outputs:
        report_frame = focused_services.drop(columns=[status_name], errors="ignore")
    gpkg_value_columns = [column for column in village_service_gdf.columns if column not in {"geometry", "_admin_key"}]
    if status_name:
        gpkg_value_columns = [column for column in gpkg_value_columns if column != status_name]
        if {"gpkg", "geoserver"} & status_outputs:
            gpkg_value_columns.insert(0, status_name)
    village_service_output_gdf = gpd.GeoDataFrame(
        admin_output_frame(
            village_service_gdf,
            value_columns=gpkg_value_columns,
            include_geometry=True,
        ),
        geometry="geometry",
        crs=village_service_gdf.crs,
    )

    result: dict[str, Any] = {
        "status": "success",
        "algorithm": ALGORITHM,
        "algorithm_version": ALGORITHM_VERSION,
        "layer_name": layer_name,
        "village_rows": int(len(admin_rows)),
        "inventory_rows": int(len(inventory)),
        "nearest_rows": int(len(nearest)),
        "village_service_rows": int(len(village_service_output_gdf)),
        "focused_service_rows": int(len(focused_services)),
        "created_facility_indexes": created_facility_indexes,
        "admin_created_indexes": admin_selection.created_indexes,
        "state_name": request.scope.state_name,
        "district_name": request.scope.district_name,
        "tehsil": request.scope.tehsil_name,
        "output_dir": bundle.path.as_posix(),
        "sync_to_geoserver": request.publish.sync_to_geoserver,
        **pool_metadata,
    }

    t0 = time.perf_counter()
    paths: dict[str, str] = {}
    if outputs.csv:
        paths["csv_path"] = bundle.write_csv(report_frame, ".csv").as_posix()
    if outputs.gpkg:
        paths["gpkg_path"] = bundle.write_gpkg(
            {output_config["village_service_layer"]: village_service_output_gdf}
        ).as_posix()
    result.update(paths)
    if outputs.stac:
        result["stac_fragment_path"] = bundle.write_json(_stac_fragment(config, result), ".stac_fragment.json").as_posix()
    timings["write_local_outputs_seconds"] = round(time.perf_counter() - t0, 3)

    geoserver = None
    if request.publish.sync_to_geoserver and outputs.geoserver:
        t0 = time.perf_counter()
        gpkg_path = result.get("gpkg_path")
        geoserver_workspace = (
            request.publish.geoserver_workspace
            or output_config.get("geoserver_workspace")
            or FACILITIES_GEOSERVER_WORKSPACE
        )
        if gpkg_path:
            try:
                geoserver_result = publish_gpkg_layer(
                    gpkg_path,
                    workspace=geoserver_workspace,
                    layer_name=layer_name,
                    overwrite=request.publish.overwrite,
                )
                geoserver = asdict(geoserver_result)
                geoserver["ok"] = True
                geoserver["status"] = "published"
                result["geoserver_links_path"] = bundle.write_csv(pd.DataFrame([geoserver]), ".geoserver_links.csv").as_posix()
            except Exception as exc:
                geoserver = {
                    "ok": False,
                    "status": "publish_failed",
                    "workspace": geoserver_workspace,
                    "layer_name": layer_name,
                    "gpkg_path": gpkg_path,
                    "error_type": exc.__class__.__name__,
                    "error": str(exc)[:500],
                }
        else:
            geoserver = {
                "ok": False,
                "status": "missing_gpkg",
                "workspace": geoserver_workspace,
                "layer_name": layer_name,
                "gpkg_path": None,
            }
        timings["publish_geoserver_seconds"] = round(time.perf_counter() - t0, 3)
    result["geoserver"] = geoserver
    if outputs.geoserver and "geoserver_links_path" not in result:
        stale_links = bundle.output_path(".geoserver_links.csv")
        if stale_links.exists():
            stale_links.unlink()
    if outputs.readme:
        result["readme_path"] = bundle.write_readme(
            _readme_lines(request, config, layer_name, result, column_dictionary(report_frame, descriptions))
        ).as_posix()
    result["timings"] = timings
    result["elapsed_seconds"] = round(time.perf_counter() - started, 3)
    if outputs.metadata:
        result["run_metadata_path"] = bundle.write_metadata(
            {
                "request": asdict(request),
                "effective_outputs": asdict(outputs),
                "result": result,
                "config_path": str(config_path),
                "outputs": {
                    "csv": frame_profile(report_frame, descriptions),
                    "gpkg_village_service": frame_profile(
                        pd.DataFrame(village_service_output_gdf.drop(columns=["geometry"], errors="ignore")),
                        describe_machine,
                    ),
                    "inventory": frame_profile(pd.DataFrame(inventory.drop(columns=["geometry"], errors="ignore"))),
                    "nearest": frame_profile(pd.DataFrame(nearest.drop(columns=["geometry"], errors="ignore"))),
                },
            }
        ).as_posix()
    result["cache_manifest_path"] = bundle.write_cache_manifest(
        {
            "cache_key": cache_key,
            "input_signatures": cache_signatures,
            "required_result_paths": required_result_paths,
            "result": result,
        }
    ).as_posix()
    return result


def run_facilities_request(payload: Mapping[str, Any]) -> dict[str, Any]:
    return run_facilities_pipeline(StandardRequest.from_mapping(payload))


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def generate_facilities_proximity_task(self, payload: Mapping[str, Any]):
    return run_facilities_request(payload)


def _run_batch(path: str | Path) -> list[dict[str, Any]]:
    return [run_facilities_pipeline(request) for request in load_request_file(path)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local facilities pipeline.")
    parser.add_argument("--request-file", help="JSON, YAML, CSV, or simple text request file")
    parser.add_argument("--state")
    parser.add_argument("--district")
    parser.add_argument("--tehsil")
    parser.add_argument("--no-geoserver", action="store_true")
    args = parser.parse_args()
    if args.request_file:
        result = _run_batch(args.request_file)
    else:
        if not (args.state and args.district and args.tehsil):
            parser.error("--state, --district, and --tehsil are required without --request-file")
        request = _cli_request(args.state, args.district, args.tehsil, not args.no_geoserver)
        result = run_facilities_pipeline(request)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
