"""Export tehsil-level facilities proximity layers from the local master GPKG."""

from __future__ import annotations

import logging
import re
import sqlite3
import time
import zipfile
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd
from django.conf import settings

from computing.models import Dataset, LayerType
from computing.utils import (
    fix_invalid_geometry_in_gdf,
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from nrm_app.celery import app
from utilities.constants import (
    FACILITIES_DATASET_NAME,
    FACILITIES_GEOSERVER_WORKSPACE,
    FACILITIES_PROXIMITY_GPKG,
)
from utilities.geoserver_utils import Geoserver, GeoserverException


logger = logging.getLogger(__name__)

SOURCE_VILLAGE_LAYER = "village_shapes"
SOURCE_L3_TABLE = "proximity_l3"
SOURCE_L2_TABLE = "proximity_l2_materialized"
SOURCE_CLASS_MAP_TABLE = "proximity_class_map"
SOURCE_NEAREST_TABLE = "proximity_nearest_facilities"
VILLAGE_ID_COL = "cs_feature_id"
OUTPUT_DIR = "data/facilities/outputs/tehsil_data"
LAYER_PREFIX = "facilities"
ALGORITHM = "local-facilities-proximity-gpkg-export"
ALGORITHM_VERSION = "2.0"
VILLAGE_CONTEXT_COLUMNS = [
    "cs_feature_id",
    "state_name",
    "district_name",
    "TEHSIL",
    "pc11_village_id",
    "NAME",
]


def _gpd():
    import geopandas as gpd

    return gpd


def _repo_path(path: str | Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else Path(settings.BASE_DIR) / path


def _source_path() -> Path:
    path = _repo_path(FACILITIES_PROXIMITY_GPKG)
    if not path.exists():
        raise FileNotFoundError(f"Facilities proximity source not found: {path}")
    return path


def _slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")


def _match_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _canonical_asset_name(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9]+", " ", str(value or "")).strip().upper()


def _quote_sql(value: str) -> str:
    return str(value).replace("'", "''")


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _layer_name(district: str, block: str) -> str:
    return f"{LAYER_PREFIX}_{_slug(district)}_{_slug(block)}"


def _output_dir(state: str, district: str, block: str) -> Path:
    return _repo_path(OUTPUT_DIR) / _slug(state) / _slug(district) / _slug(block)


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _require_source_tables(connection: sqlite3.Connection) -> None:
    missing = [
        table
        for table in (
            SOURCE_VILLAGE_LAYER,
            SOURCE_L3_TABLE,
            SOURCE_L2_TABLE,
            SOURCE_CLASS_MAP_TABLE,
            SOURCE_NEAREST_TABLE,
        )
        if not _table_exists(connection, table)
    ]
    if missing:
        raise RuntimeError(
            "Facilities proximity source is missing required table(s): "
            f"{', '.join(missing)}. Rebuild with "
            "`uv run --with pandas --with numpy --with pyyaml --with geopandas "
            "--with shapely --with pyogrio --with scipy python "
            "utilities/scripts/facilities_utils/facility_pipeline.py proximity --force`."
        )


@lru_cache(maxsize=1)
def _location_rows() -> tuple[tuple[str, str, str], ...]:
    source_path = _source_path()
    with sqlite3.connect(source_path) as connection:
        _require_source_tables(connection)
        rows = connection.execute(
            f"""
            SELECT DISTINCT state_name, district_name, TEHSIL
            FROM {SOURCE_VILLAGE_LAYER}
            WHERE state_name IS NOT NULL
              AND district_name IS NOT NULL
              AND TEHSIL IS NOT NULL
            """
        ).fetchall()
    return tuple((str(row[0]), str(row[1]), str(row[2])) for row in rows)


def _resolve_location(state: str, district: str, block: str) -> tuple[str, str, str]:
    state_key, district_key, block_key = map(_match_key, (state, district, block))
    state_matches = [row for row in _location_rows() if _match_key(row[0]) == state_key]
    if not state_matches:
        raise ValueError(f"State not found in facilities proximity asset: {state}")

    district_matches = [row for row in state_matches if _match_key(row[1]) == district_key]
    if not district_matches:
        available = sorted({row[1] for row in state_matches})[:20]
        raise ValueError(
            f"District not found in facilities proximity asset: {district}. "
            f"Available examples: {available}"
        )

    block_matches = [row for row in district_matches if _match_key(row[2]) == block_key]
    if not block_matches:
        available = sorted({row[2] for row in district_matches})[:30]
        raise ValueError(
            f"TEHSIL/block not found in facilities proximity asset: {block}. "
            f"Available examples: {available}"
        )
    return block_matches[0]


def _read_villages(state_name: str, district_name: str, tehsil_name: str):
    source_path = _source_path()
    where = (
        f"state_name = '{_quote_sql(state_name)}' AND "
        f"district_name = '{_quote_sql(district_name)}' AND "
        f"TEHSIL = '{_quote_sql(tehsil_name)}'"
    )
    gdf = _gpd().read_file(source_path, layer=SOURCE_VILLAGE_LAYER, where=where)
    if gdf.empty:
        raise ValueError(f"No village shapes found for {state_name}/{district_name}/{tehsil_name}")
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    return fix_invalid_geometry_in_gdf(gdf)


def _create_requested_villages(connection: sqlite3.Connection, village_ids: list[str]) -> None:
    connection.execute("DROP TABLE IF EXISTS temp.requested_villages")
    connection.execute("CREATE TEMP TABLE requested_villages (cs_feature_id TEXT PRIMARY KEY)")
    connection.executemany(
        "INSERT OR IGNORE INTO requested_villages (cs_feature_id) VALUES (?)",
        [(str(village_id),) for village_id in village_ids],
    )


def _read_l3_attributes(source_path: Path, village_ids: list[str]) -> pd.DataFrame:
    with sqlite3.connect(source_path) as connection:
        _require_source_tables(connection)
        _create_requested_villages(connection, village_ids)
        return pd.read_sql_query(
            f"""
            SELECT
              p.{VILLAGE_ID_COL},
              m.class_l1_domain,
              m.class_l2_filter_group,
              p.class_l3_facility_class,
              p.nearest_distance_km,
              p.nearest_facility_uid,
              nf.facility_name AS nearest_facility_name,
              nf.facility_code AS nearest_facility_code,
              nf.latitude AS nearest_facility_latitude,
              nf.longitude AS nearest_facility_longitude,
              nf.class_l4_facility_subtype AS nearest_class_l4_facility_subtype
            FROM {SOURCE_L3_TABLE} p
            JOIN requested_villages rv
              ON rv.cs_feature_id = CAST(p.{VILLAGE_ID_COL} AS TEXT)
            LEFT JOIN {SOURCE_CLASS_MAP_TABLE} m
              ON m.class_l3_facility_class = p.class_l3_facility_class
            LEFT JOIN {SOURCE_NEAREST_TABLE} nf
              ON nf.facility_uid = p.nearest_facility_uid
            """,
            connection,
        )


def _read_l2_attributes(source_path: Path, village_ids: list[str]) -> pd.DataFrame:
    with sqlite3.connect(source_path) as connection:
        _require_source_tables(connection)
        _create_requested_villages(connection, village_ids)
        frame = pd.read_sql_query(
            f"""
            SELECT
              p.{VILLAGE_ID_COL},
              p.class_l1_domain,
              p.class_l2_filter_group,
              p.logic_distance_km AS nearest_distance_km,
              p.selected_component_class,
              p.nearest_facility_uid,
              nf.facility_name AS nearest_facility_name,
              nf.facility_code AS nearest_facility_code,
              nf.latitude AS nearest_facility_latitude,
              nf.longitude AS nearest_facility_longitude,
              nf.class_l4_facility_subtype AS nearest_class_l4_facility_subtype
            FROM {SOURCE_L2_TABLE} p
            JOIN requested_villages rv
              ON rv.cs_feature_id = CAST(p.{VILLAGE_ID_COL} AS TEXT)
            LEFT JOIN {SOURCE_NEAREST_TABLE} nf
              ON nf.facility_uid = p.nearest_facility_uid
            """,
            connection,
        )
    if frame.empty:
        raise RuntimeError(
            f"{SOURCE_L2_TABLE} has no rows for the requested villages. "
            "Rebuild the facilities proximity GPKG so L2 is materialized in the source."
        )
    return frame


def _read_nearest_facilities(source_path: Path, village_ids: list[str]):
    gpd = _gpd()
    with sqlite3.connect(source_path) as connection:
        _require_source_tables(connection)
        _create_requested_villages(connection, village_ids)
        frame = pd.read_sql_query(
            f"""
            SELECT DISTINCT
              nf.facility_uid,
              nf.facility_name,
              nf.facility_code,
              nf.latitude,
              nf.longitude,
              nf.class_l4_facility_subtype
            FROM {SOURCE_NEAREST_TABLE} nf
            JOIN (
              SELECT p.nearest_facility_uid
              FROM {SOURCE_L3_TABLE} p
              JOIN requested_villages rv
                ON rv.cs_feature_id = CAST(p.{VILLAGE_ID_COL} AS TEXT)
              WHERE p.nearest_facility_uid IS NOT NULL
              UNION
              SELECT p.nearest_facility_uid
              FROM {SOURCE_L2_TABLE} p
              JOIN requested_villages rv
                ON rv.cs_feature_id = CAST(p.{VILLAGE_ID_COL} AS TEXT)
              WHERE p.nearest_facility_uid IS NOT NULL
            ) u
              ON u.nearest_facility_uid = nf.facility_uid
            WHERE nf.latitude IS NOT NULL
              AND nf.longitude IS NOT NULL
            """,
            connection,
        )
    if frame.empty:
        return gpd.GeoDataFrame(frame, geometry=[], crs="EPSG:4326")
    frame["title"] = frame["facility_name"].fillna(frame["facility_uid"])
    geometry = gpd.points_from_xy(frame["longitude"], frame["latitude"], crs="EPSG:4326")
    return gpd.GeoDataFrame(frame, geometry=geometry)


def _attach_village_geometry(villages, attributes: pd.DataFrame, level: str):
    village_cols = [col for col in VILLAGE_CONTEXT_COLUMNS if col in villages.columns]
    base = villages[village_cols + ["geometry"]].copy()
    base[VILLAGE_ID_COL] = base[VILLAGE_ID_COL].astype(str)
    attributes = attributes.copy()
    attributes[VILLAGE_ID_COL] = attributes[VILLAGE_ID_COL].astype(str)
    merged = base.merge(attributes, on=VILLAGE_ID_COL, how="inner")
    if merged.empty:
        raise ValueError(f"No {level} proximity rows found for requested villages")
    merged["proximity_level"] = level
    merged["title"] = merged["nearest_facility_name"].fillna(
        merged.get("class_l3_facility_class", merged.get("class_l2_filter_group"))
    )
    drop_cols = [col for col in ("_village_latitude", "_village_longitude", "filter_logic") if col in merged.columns]
    if drop_cols:
        merged = merged.drop(columns=drop_cols)
    return _gpd().GeoDataFrame(merged, geometry="geometry", crs=villages.crs)


def _write_layer(gdf, gpkg_path: Path, layer: str, mode: str) -> None:
    gdf.to_file(gpkg_path, layer=layer, driver="GPKG", mode=mode)


def _zip_gpkg(gpkg_path: Path) -> Path:
    zip_path = gpkg_path.with_suffix(".zip")
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(gpkg_path, arcname=gpkg_path.name)
    return zip_path


def _write_tehsil_gpkg(
    villages,
    l3,
    l2,
    nearest_facilities,
    output_dir: Path,
    layer_name: str,
) -> tuple[Path, Path, dict[str, int]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    gpkg_path = output_dir / f"{layer_name}.gpkg"
    zip_path = output_dir / f"{layer_name}.zip"
    for path in (gpkg_path, zip_path):
        if path.exists():
            path.unlink()

    clean_villages = villages.drop(columns=[col for col in ("_village_latitude", "_village_longitude") if col in villages.columns])
    _write_layer(l3, gpkg_path, layer_name, "w")
    _write_layer(l2, gpkg_path, "facilities_l2", "a")
    _write_layer(clean_villages, gpkg_path, "villages", "a")
    if not nearest_facilities.empty:
        _write_layer(nearest_facilities, gpkg_path, "nearest_facilities", "a")
    zip_path = _zip_gpkg(gpkg_path)

    return gpkg_path, zip_path, {
        "villages": int(len(clean_villages)),
        "l3": int(len(l3)),
        "l2": int(len(l2)),
        "nearest_facilities": int(len(nearest_facilities)),
    }


def _publish_to_geoserver(gpkg_path: Path, layer_name: str, overwrite: bool) -> dict[str, Any]:
    try:
        geoserver = Geoserver()
        try:
            geoserver.get_workspace(FACILITIES_GEOSERVER_WORKSPACE)
        except GeoserverException as exc:
            if exc.status != 404:
                raise
            geoserver.create_workspace(FACILITIES_GEOSERVER_WORKSPACE)

        response = push_shape_to_geoserver(
            str(gpkg_path.with_suffix("")),
            store_name=layer_name,
            workspace=FACILITIES_GEOSERVER_WORKSPACE,
            layer_name=layer_name if overwrite else None,
            file_type="gpkg",
        )
        return {"ok": True, "response": response}
    except Exception as exc:
        logger.exception("Facilities GeoServer publish failed")
        return {
            "ok": False,
            "error_type": exc.__class__.__name__,
            "error": str(exc)[:500],
        }


def _register_layer(
    state: str,
    district: str,
    block: str,
    layer_name: str,
    geoserver_url: str,
    output: dict[str, Any],
    overwrite: bool,
) -> tuple[int | None, dict[str, Any] | None]:
    try:
        Dataset.objects.get_or_create(
            name=FACILITIES_DATASET_NAME,
            defaults={
                "layer_type": LayerType.VECTOR,
                "workspace": FACILITIES_GEOSERVER_WORKSPACE,
                "is_active": True,
            },
        )
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=geoserver_url,
            dataset_name=FACILITIES_DATASET_NAME,
            algorithm=ALGORITHM,
            algorithm_version=ALGORITHM_VERSION,
            misc=output,
            is_override=_bool(overwrite),
            is_gee_asset=False,
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
        return layer_id, None
    except Exception as exc:
        logger.exception("Facilities DB registration failed")
        return None, {"error_type": exc.__class__.__name__, "error": str(exc)[:500]}


def generate_facilities_proximity(
    state: str,
    district: str,
    block: str,
    sync_to_geoserver: bool = True,
    overwrite: bool = False,
) -> dict[str, Any]:
    started = time.perf_counter()
    timings: dict[str, float] = {}
    layer_name = _layer_name(district, block)
    output_dir = _output_dir(state, district, block)
    source_path = _source_path()

    resolved_state = _canonical_asset_name(state)
    resolved_district = _canonical_asset_name(district)
    resolved_block = _canonical_asset_name(block)
    try:
        t0 = time.perf_counter()
        villages = _read_villages(resolved_state, resolved_district, resolved_block)
    except ValueError:
        resolved_state, resolved_district, resolved_block = _resolve_location(state, district, block)
        villages = _read_villages(resolved_state, resolved_district, resolved_block)
    timings["read_villages_seconds"] = round(time.perf_counter() - t0, 3)

    village_ids = villages[VILLAGE_ID_COL].dropna().astype(str).drop_duplicates().tolist()
    t0 = time.perf_counter()
    l3 = _attach_village_geometry(villages, _read_l3_attributes(source_path, village_ids), "l3")
    l2 = _attach_village_geometry(villages, _read_l2_attributes(source_path, village_ids), "l2")
    nearest_facilities = _read_nearest_facilities(source_path, village_ids)
    timings["read_proximity_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    gpkg_path, zip_path, row_counts = _write_tehsil_gpkg(l3=l3, l2=l2, villages=villages, nearest_facilities=nearest_facilities, output_dir=output_dir, layer_name=layer_name)
    timings["write_gpkg_seconds"] = round(time.perf_counter() - t0, 3)

    geoserver = None
    geoserver_url = None
    layer_id = None
    registration_error = None
    if _bool(sync_to_geoserver):
        t0 = time.perf_counter()
        geoserver = _publish_to_geoserver(gpkg_path, layer_name, _bool(overwrite))
        timings["publish_geoserver_seconds"] = round(time.perf_counter() - t0, 3)
        if geoserver.get("ok"):
            geoserver_url = (
                f"{settings.GEOSERVER_URL.rstrip('/')}/{FACILITIES_GEOSERVER_WORKSPACE}/ows"
                "?service=WFS&version=1.0.0&request=GetFeature"
                f"&typeName={FACILITIES_GEOSERVER_WORKSPACE}:{layer_name}"
                "&outputFormat=application/json"
            )
            layer_id, registration_error = _register_layer(
                resolved_state,
                resolved_district,
                resolved_block,
                layer_name,
                geoserver_url,
                {
                    "is_generated_locally": True,
                    "source": source_path.as_posix(),
                    "gpkg_path": gpkg_path.as_posix(),
                    "zip_path": zip_path.as_posix(),
                    "output_dir": output_dir.as_posix(),
                    "row_counts": row_counts,
                    "geoserver_workspace": FACILITIES_GEOSERVER_WORKSPACE,
                    "geoserver_layer_name": layer_name,
                    "geoserver_url": geoserver_url,
                },
                _bool(overwrite),
            )

    elapsed = round(time.perf_counter() - started, 3)
    timings["total_seconds"] = elapsed
    return {
        "status": "success",
        "layer_name": layer_name,
        "row_counts": row_counts,
        "source": source_path.as_posix(),
        "source_village_layer": SOURCE_VILLAGE_LAYER,
        "source_village_geometry": str(villages.geometry.geom_type.mode().iloc[0]),
        "gpkg_path": gpkg_path.as_posix(),
        "zip_path": zip_path.as_posix(),
        "output_dir": output_dir.as_posix(),
        "state_name": resolved_state,
        "district_name": resolved_district,
        "tehsil": resolved_block,
        "sync_to_geoserver": _bool(sync_to_geoserver),
        "geoserver": geoserver,
        "geoserver_url": geoserver_url,
        "layer_id": layer_id,
        "registration_error": registration_error,
        "timings": timings,
        "elapsed_seconds": elapsed,
    }


@app.task(bind=True)
def generate_facilities_proximity_task(
    self,
    state: str,
    district: str,
    block: str,
    sync_to_geoserver: bool = True,
    overwrite: bool = False,
    **_ignored: Any,
) -> dict[str, Any]:
    """Celery wrapper for the local facilities proximity export."""
    return generate_facilities_proximity(
        state=state,
        district=district,
        block=block,
        sync_to_geoserver=sync_to_geoserver,
        overwrite=overwrite,
    )
