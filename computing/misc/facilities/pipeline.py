"""Facilities inventory and live proximity pipeline from local GeoPackages."""

from __future__ import annotations

import argparse
import json
import math
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping

import geopandas as gpd
import pandas as pd
from django.conf import settings
from scipy.spatial import cKDTree

from computing.misc.local_pipeline import AdminScope, CSAdminSource, StandardRequest, load_config
from computing.misc.local_pipeline.batch import load_request_file
from computing.misc.local_pipeline.gpkg import (
    IndexSpec,
    column_expression,
    connect_gpkg,
    ensure_indexes,
    gpkg_geometry_to_shape,
    quote_identifier,
    read_table,
)
from computing.misc.local_pipeline.outputs import OutputBundle, slug, utc_now_text
from computing.misc.local_pipeline.publish import publish_gpkg_layer
from nrm_app.celery import app


CONFIG_PATH = Path(__file__).with_name("facilities_pipeline.yaml")
ALGORITHM = "local-facilities-live-proximity"
ALGORITHM_VERSION = "1.0"


def _repo_path(path: str | Path) -> Path:
    path = Path(path)
    if path.is_absolute():
        return path
    base_dir = Path(settings.BASE_DIR) if settings.configured else Path.cwd()
    return base_dir / path


def _layer_name(prefix: str, district: str | None, tehsil: str | None) -> str:
    return f"{prefix}_{slug(district)}_{slug(tehsil)}".strip("_")


def _request_from_legacy_args(
    state: str,
    district: str,
    block: str,
    gee_account_id: str | None = None,
    sync_to_geoserver: bool = True,
    overwrite: bool = True,
) -> StandardRequest:
    return StandardRequest.from_mapping(
        {
            "scope": {
                "level": "tehsil",
                "state_name": state,
                "district_name": district,
                "tehsil_name": block,
            },
            "publish": {
                "sync_to_geoserver": sync_to_geoserver,
                "overwrite": overwrite,
                "register_layers": False,
            },
            "outputs": {
                "gpkg": True,
                "csv": True,
                "readme": True,
                "eda": True,
                "stac": True,
                "geoserver": sync_to_geoserver,
                "excel_ready_csv": False,
            },
            "legacy": {"gee_account_id": gee_account_id},
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
    taxonomy = read_table(
        _repo_path(config["sources"]["facilities_gpkg"]),
        config["sources"]["taxonomy_table"],
    )
    if "sort_order" in taxonomy.columns:
        taxonomy = taxonomy.sort_values("sort_order")
    return taxonomy


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
    admin_context = admin_rows[
        ["cs_feature_id", "pc11_village_id", "village_id", "NAME", "state_name", "district_name", "TEHSIL", "geometry"]
    ].copy()
    joined = gpd.sjoin(candidates, admin_context, how="inner", predicate="intersects")
    if "index_right" in joined.columns:
        joined = joined.drop(columns=["index_right"])
    joined["inside_requested_scope"] = True
    joined["facilities_layer_kind"] = "inventory"
    joined["title"] = joined["facility_name"].fillna(joined["facility_uid"])
    return joined.drop_duplicates(subset=["facility_uid", "cs_feature_id"])


def _village_points(admin_rows) -> pd.DataFrame:
    rows = admin_rows.copy()
    points = rows.geometry.representative_point()
    frame = pd.DataFrame(rows.drop(columns=["geometry"]))
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
    admin_rows,
    radius_km: float,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    if pool.empty or village_points.empty:
        return gpd.GeoDataFrame(pd.DataFrame(), geometry=[], crs="EPSG:4326"), pd.DataFrame()
    inside_ids = set(_inventory(pool, admin_rows)["facility_uid"]) if not pool.empty else set()
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
                "cs_feature_id": village.get("cs_feature_id"),
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
                "nearest_distance_km": round(distance_km, 6),
                "inside_requested_scope": bool(facility["facility_uid"] in inside_ids),
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
    village_service = _village_service(village_points, nearest)
    return nearest, village_service


def _village_service(village_points: pd.DataFrame, nearest: pd.DataFrame) -> pd.DataFrame:
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
        pivot = l3.pivot_table(index="cs_feature_id", columns="_slug", values=metric, aggfunc="first")
        pivot.columns = [f"l3_{col}_{suffix}" for col in pivot.columns]
        base = base.merge(pivot.reset_index(), on="cs_feature_id", how="left")
    l2 = l3.sort_values("nearest_distance_km").drop_duplicates(["cs_feature_id", "class_l2_filter_group"])
    l2["_slug"] = l2["class_l2_filter_group"].map(slug)
    for metric, suffix in (
        ("nearest_distance_km", "distance_km"),
        ("facility_uid", "facility_uid"),
        ("class_l3_facility_class", "selected_l3"),
    ):
        pivot = l2.pivot_table(index="cs_feature_id", columns="_slug", values=metric, aggfunc="first")
        pivot.columns = [f"l2_{col}_{suffix}" for col in pivot.columns]
        base = base.merge(pivot.reset_index(), on="cs_feature_id", how="left")
    base["facilities_layer_kind"] = "village_service"
    base["title"] = base["NAME"].fillna(base["cs_feature_id"])
    return base


def _readme_lines(request: StandardRequest, config: Mapping[str, Any], result_name: str, result: Mapping[str, Any]) -> list[str]:
    lines = [
        f"# {result_name}",
        "",
        f"Generated at: `{utc_now_text()}`",
        "",
        "## What This Contains",
        "",
        "This output creates local facilities inventory and nearest-service outputs for the requested Core Stack admin geography.",
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
        "",
        "## Cautions",
        "",
    ]
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


def run_facilities_pipeline(
    request: StandardRequest,
    *,
    config_path: str | Path = CONFIG_PATH,
) -> dict[str, Any]:
    started = time.perf_counter()
    timings: dict[str, float] = {}
    config = load_config(config_path)
    output_config = config["output"]
    layer_name = _layer_name(output_config["layer_prefix"], request.scope.district_name, request.scope.tehsil_name)
    output_root = _repo_path(output_config["root"]) / slug(request.scope.state_name) / slug(request.scope.district_name) / slug(request.scope.tehsil_name)
    bundle = OutputBundle(output_root, layer_name)

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
        admin_rows,
        float(config["search"]["earth_radius_km"]),
    )
    timings["build_nearest_seconds"] = round(time.perf_counter() - t0, 3)

    village_service_gdf = admin_rows[["cs_feature_id", "geometry"]].merge(village_service, on="cs_feature_id", how="left")
    village_service_gdf = gpd.GeoDataFrame(village_service_gdf, geometry="geometry", crs=admin_rows.crs)

    result: dict[str, Any] = {
        "status": "success",
        "algorithm": ALGORITHM,
        "algorithm_version": ALGORITHM_VERSION,
        "layer_name": layer_name,
        "village_rows": int(len(admin_rows)),
        "inventory_rows": int(len(inventory)),
        "nearest_rows": int(len(nearest)),
        "village_service_rows": int(len(village_service_gdf)),
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
    if request.outputs.csv:
        if not inventory.empty:
            paths["inventory_csv_path"] = bundle.write_csv(pd.DataFrame(inventory.drop(columns=["geometry"], errors="ignore")), ".inventory.csv").as_posix()
        if not nearest.empty:
            paths["nearest_csv_path"] = bundle.write_csv(pd.DataFrame(nearest.drop(columns=["geometry"], errors="ignore")), ".nearest.csv").as_posix()
        paths["village_service_csv_path"] = bundle.write_csv(pd.DataFrame(village_service_gdf.drop(columns=["geometry"], errors="ignore")), ".village_service.csv").as_posix()
    if request.outputs.gpkg:
        paths["gpkg_path"] = bundle.write_gpkg(
            {
                output_config["inventory_layer"]: inventory,
                output_config["nearest_layer"]: nearest,
                output_config["village_service_layer"]: village_service_gdf,
            }
        ).as_posix()
    if request.outputs.eda:
        paths["eda_path"] = bundle.write_eda(
            {
                "inventory": pd.DataFrame(inventory.drop(columns=["geometry"], errors="ignore")),
                "nearest": pd.DataFrame(nearest.drop(columns=["geometry"], errors="ignore")),
                "village_service": pd.DataFrame(village_service_gdf.drop(columns=["geometry"], errors="ignore")),
            }
        ).as_posix()
    if request.outputs.readme:
        paths["readme_path"] = bundle.write_readme(_readme_lines(request, config, layer_name, result)).as_posix()
    result.update(paths)
    if request.outputs.stac:
        result["stac_fragment_path"] = bundle.write_json(_stac_fragment(config, result), ".stac_fragment.json").as_posix()
    timings["write_local_outputs_seconds"] = round(time.perf_counter() - t0, 3)

    geoserver = None
    if request.publish.sync_to_geoserver:
        t0 = time.perf_counter()
        gpkg_path = result.get("gpkg_path")
        if gpkg_path:
            try:
                geoserver_result = publish_gpkg_layer(
                    gpkg_path,
                    workspace=output_config["geoserver_workspace"],
                    layer_name=layer_name,
                    overwrite=request.publish.overwrite,
                )
                geoserver = asdict(geoserver_result)
                result["geoserver_links_path"] = bundle.write_csv(pd.DataFrame([geoserver]), ".geoserver_links.csv").as_posix()
            except Exception as exc:
                geoserver = {"ok": False, "error_type": exc.__class__.__name__, "error": str(exc)[:500]}
        timings["publish_geoserver_seconds"] = round(time.perf_counter() - t0, 3)
    result["geoserver"] = geoserver
    result["timings"] = timings
    result["elapsed_seconds"] = round(time.perf_counter() - started, 3)
    result["run_metadata_path"] = bundle.write_metadata(
        {"request": asdict(request), "result": result, "config_path": str(config_path)}
    ).as_posix()
    return result


def run_facilities_request(payload: Mapping[str, Any]) -> dict[str, Any]:
    return run_facilities_pipeline(StandardRequest.from_mapping(payload))


def generate_facilities_proximity(state: str, district: str, block: str, gee_account_id: str | None = None):
    request = _request_from_legacy_args(state, district, block, gee_account_id)
    return run_facilities_pipeline(request)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def generate_facilities_proximity_task(self, state, district, block, gee_account_id=None):
    return generate_facilities_proximity(state, district, block, gee_account_id)


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
        request = _request_from_legacy_args(args.state, args.district, args.tehsil, None, not args.no_geoserver)
        result = run_facilities_pipeline(request)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
