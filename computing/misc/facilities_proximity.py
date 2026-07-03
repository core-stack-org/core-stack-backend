"""Export tehsil-level facilities proximity layers from the local master GPKG."""

from __future__ import annotations

import logging
import re
import shutil
import sqlite3
import time
import tempfile
import zipfile
from functools import lru_cache
from numbers import Integral, Real
from pathlib import Path
from typing import Any

import pandas as pd
from django.conf import settings

from computing.models import Dataset, LayerType
from computing.utils import (
    fix_invalid_geometry_in_gdf,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from nrm_app.celery import app
from utilities.constants import (
    FACILITIES_DATASET_NAME,
    FACILITIES_MASTER_GPKG,
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
SOURCE_FACILITIES_TABLE = "facilities"
VILLAGE_ID_COL = "cs_feature_id"
OUTPUT_DIR = "data/facilities/outputs/tehsil_data"
LAYER_PREFIX = "facilities"
ALGORITHM = "local-facilities-proximity-gpkg-export"
ALGORITHM_VERSION = "2.1"
OUTPUT_INVENTORY = "inventory"
OUTPUT_NEAREST = "nearest"
OUTPUT_VILLAGE_SERVICE = "village_service"
ALL_OUTPUTS = (OUTPUT_INVENTORY, OUTPUT_NEAREST, OUTPUT_VILLAGE_SERVICE)
OUTPUT_ALIASES = {
    "all": "all",
    "facility": OUTPUT_INVENTORY,
    "facilities": OUTPUT_INVENTORY,
    "facility_inventory": OUTPUT_INVENTORY,
    "inventory": OUTPUT_INVENTORY,
    "nearest": OUTPUT_NEAREST,
    "nearest_facilities": OUTPUT_NEAREST,
    "nearest_service": OUTPUT_NEAREST,
    "proximity": OUTPUT_NEAREST,
    "service": OUTPUT_VILLAGE_SERVICE,
    "service_surface": OUTPUT_VILLAGE_SERVICE,
    "village": OUTPUT_VILLAGE_SERVICE,
    "village_service": OUTPUT_VILLAGE_SERVICE,
    "villages": OUTPUT_VILLAGE_SERVICE,
}
LOCAL_GPKG_LAYER_NAMES = {
    OUTPUT_INVENTORY: "facilities_inventory",
    OUTPUT_NEAREST: "facilities_nearest",
    OUTPUT_VILLAGE_SERVICE: "facilities_village_service",
}
INVENTORY_EXPORT_COLUMNS = [
    "facility_uid",
    "facility_name",
    "facility_code",
    "latitude",
    "longitude",
    "class_l1_domain",
    "class_l2_filter_group",
    "class_l3_facility_class",
    "class_l4_facility_subtype",
    "facilities_layer_kind",
    "title",
    "geometry",
]
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


def _facilities_path() -> Path:
    path = _repo_path(FACILITIES_MASTER_GPKG)
    if not path.exists():
        raise FileNotFoundError(f"Facilities master source not found: {path}")
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


def _geoserver_layer_name(output_key: str, district: str, block: str) -> str:
    return f"{LAYER_PREFIX}_{output_key}_{_slug(district)}_{_slug(block)}"


def _output_dir(state: str, district: str, block: str) -> Path:
    return _repo_path(OUTPUT_DIR) / _slug(state) / _slug(district) / _slug(block)


def _bundle_stem(layer_name: str, selected_outputs: tuple[str, ...]) -> str:
    if selected_outputs == ALL_OUTPUTS:
        return layer_name
    return f"{layer_name}_{'_'.join(selected_outputs)}"


def _output_results_template(
    selected_outputs: tuple[str, ...],
    row_counts: dict[str, int],
    district: str,
    block: str,
) -> dict[str, dict[str, Any]]:
    return {
        output_key: {
            "local_layer": LOCAL_GPKG_LAYER_NAMES[output_key],
            "geoserver_layer_name": _geoserver_layer_name(output_key, district, block),
            "row_count": row_counts.get(output_key, 0),
            "geoserver_url": None,
            "layer_id": None,
            "registration_error": None,
        }
        for output_key in selected_outputs
    }


def _parse_outputs(outputs: Any = None) -> tuple[str, ...]:
    if outputs is None or outputs == "":
        return ALL_OUTPUTS
    if isinstance(outputs, str):
        requested = [part.strip() for part in outputs.split(",")]
    elif isinstance(outputs, (list, tuple, set)):
        requested = [str(part).strip() for part in outputs]
    else:
        requested = [str(outputs).strip()]

    parsed: list[str] = []
    for item in requested:
        if not item:
            continue
        key = OUTPUT_ALIASES.get(_slug(item), _slug(item))
        if key == "all":
            return ALL_OUTPUTS
        if key not in ALL_OUTPUTS:
            raise ValueError(
                f"Unknown facilities output {item!r}. "
                f"Choose one of: all, {', '.join(ALL_OUTPUTS)}"
            )
        if key not in parsed:
            parsed.append(key)
    return tuple(parsed or ALL_OUTPUTS)


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _source_connection(source_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(source_path)
    connection.execute("PRAGMA temp_store = MEMORY")
    return connection


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


def _gpkg_geometry_to_shape(blob: bytes | memoryview | None):
    if blob is None:
        return None
    from shapely import wkb

    data = bytes(blob)
    if data[:2] != b"GP":
        return wkb.loads(data)
    flags = data[3]
    if flags & 0b00010000:
        return None
    envelope_code = (flags >> 1) & 0b00000111
    envelope_bytes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get(envelope_code)
    if envelope_bytes is None:
        raise ValueError(f"Unsupported GeoPackage geometry envelope code: {envelope_code}")
    return wkb.loads(data[8 + envelope_bytes :])


def _read_villages(state_name: str, district_name: str, tehsil_name: str):
    gpd = _gpd()
    source_path = _source_path()
    columns = [
        "fid",
        *VILLAGE_CONTEXT_COLUMNS,
        "_village_latitude",
        "_village_longitude",
        "geom",
    ]
    with _source_connection(source_path) as connection:
        frame = pd.read_sql_query(
            f"""
            SELECT {", ".join(columns)}
            FROM {SOURCE_VILLAGE_LAYER}
            WHERE state_name = ?
              AND district_name = ?
              AND TEHSIL = ?
            """,
            connection,
            params=(state_name, district_name, tehsil_name),
        )
    if frame.empty:
        raise ValueError(f"No village shapes found for {state_name}/{district_name}/{tehsil_name}")
    geometry = frame.pop("geom").map(_gpkg_geometry_to_shape)
    gdf = gpd.GeoDataFrame(frame, geometry=geometry, crs="EPSG:4326")
    return fix_invalid_geometry_in_gdf(gdf)


def _read_l3_attributes(source_path: Path, village_ids: list[str]) -> pd.DataFrame:
    if not village_ids:
        return pd.DataFrame()
    placeholders = ", ".join(["?"] * len(village_ids))
    with _source_connection(source_path) as connection:
        _require_source_tables(connection)
        return pd.read_sql_query(
            f"""
            SELECT
              p.{VILLAGE_ID_COL},
              m.class_l1_domain,
              m.class_l2_filter_group,
              p.class_l3_facility_class,
              p.nearest_distance_km,
              p.nearest_facility_uid
            FROM {SOURCE_L3_TABLE} p
            LEFT JOIN {SOURCE_CLASS_MAP_TABLE} m
              ON m.class_l3_facility_class = p.class_l3_facility_class
            WHERE p.{VILLAGE_ID_COL} IN ({placeholders})
            """,
            connection,
            params=village_ids,
        )


def _read_l2_attributes(source_path: Path, village_ids: list[str]) -> pd.DataFrame:
    if not village_ids:
        return pd.DataFrame()
    placeholders = ", ".join(["?"] * len(village_ids))
    with _source_connection(source_path) as connection:
        _require_source_tables(connection)
        frame = pd.read_sql_query(
            f"""
            SELECT
              p.{VILLAGE_ID_COL},
              p.class_l1_domain,
              p.class_l2_filter_group,
              p.logic_distance_km AS nearest_distance_km,
              p.selected_component_class,
              p.nearest_facility_uid
            FROM {SOURCE_L2_TABLE} p
            WHERE p.{VILLAGE_ID_COL} IN ({placeholders})
            """,
            connection,
            params=village_ids,
        )
    if frame.empty:
        raise RuntimeError(
            f"{SOURCE_L2_TABLE} has no rows for the requested villages. "
            "Rebuild the facilities proximity GPKG so L2 is materialized in the source."
        )
    return frame


def _read_nearest_facilities(source_path: Path, facility_uids: list[str]):
    gpd = _gpd()
    if not facility_uids:
        return gpd.GeoDataFrame(pd.DataFrame(), geometry=[], crs="EPSG:4326")
    with _source_connection(source_path) as connection:
        _require_source_tables(connection)
        connection.execute("DROP TABLE IF EXISTS temp.requested_nearest_facilities")
        connection.execute("CREATE TEMP TABLE requested_nearest_facilities (facility_uid TEXT PRIMARY KEY)")
        connection.executemany(
            "INSERT OR IGNORE INTO requested_nearest_facilities (facility_uid) VALUES (?)",
            [(str(facility_uid),) for facility_uid in facility_uids],
        )
        frame = pd.read_sql_query(
            f"""
            SELECT
              nf.facility_uid,
              nf.facility_name,
              nf.facility_code,
              nf.latitude,
              nf.longitude,
              nf.class_l4_facility_subtype
            FROM {SOURCE_NEAREST_TABLE} nf
            WHERE nf.facility_uid IN (
              SELECT facility_uid FROM requested_nearest_facilities
            )
              AND nf.latitude IS NOT NULL
              AND nf.longitude IS NOT NULL
            """,
            connection,
        )
    if frame.empty:
        return gpd.GeoDataFrame(frame, geometry=[], crs="EPSG:4326")
    frame["title"] = frame["facility_name"].fillna(frame["facility_uid"])
    geometry = gpd.points_from_xy(frame["longitude"], frame["latitude"], crs="EPSG:4326")
    return gpd.GeoDataFrame(frame, geometry=geometry)


def _read_inventory_facilities(villages):
    gpd = _gpd()
    facilities_path = _facilities_path()
    minx, miny, maxx, maxy = villages.total_bounds
    with _source_connection(facilities_path) as connection:
        if not _table_exists(connection, SOURCE_FACILITIES_TABLE):
            raise RuntimeError(f"Facilities source is missing table: {SOURCE_FACILITIES_TABLE}")
        if not _table_exists(connection, "rtree_facilities_geom"):
            raise RuntimeError(
                "Facilities source is missing rtree_facilities_geom. "
                "Rebuild the master facilities GPKG with its spatial index."
            )
        frame = pd.read_sql_query(
            f"""
            SELECT
              f.fid AS facility_fid,
              facility_uid,
              facility_name,
              facility_code,
              latitude,
              longitude,
              class_l1_domain,
              class_l2_filter_group,
              class_l3_facility_class,
              class_l4_facility_subtype,
              f.geom
            FROM {SOURCE_FACILITIES_TABLE} f
            JOIN rtree_facilities_geom r
              ON r.id = f.fid
            WHERE r.maxx >= ?
              AND r.minx <= ?
              AND r.maxy >= ?
              AND r.miny <= ?
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
            """,
            connection,
            params=(minx, maxx, miny, maxy),
        )
    if frame.empty:
        return gpd.GeoDataFrame(frame, geometry=[], crs="EPSG:4326")
    if "geom" in frame.columns:
        geometry = frame.pop("geom").map(_gpkg_geometry_to_shape)
    else:
        geometry = gpd.points_from_xy(frame["longitude"], frame["latitude"], crs="EPSG:4326")
    frame["facilities_layer_kind"] = OUTPUT_INVENTORY
    frame["title"] = frame["facility_name"].fillna(frame["facility_uid"])
    facilities = gpd.GeoDataFrame(frame, geometry=geometry, crs="EPSG:4326")
    village_context = villages[["geometry"]]
    joined = gpd.sjoin(facilities, village_context, how="inner", predicate="intersects")
    if "index_right" in joined.columns:
        joined = joined.drop(columns=["index_right"])
    if "facility_fid" in joined.columns:
        joined = joined.drop_duplicates(subset=["facility_fid"])
        joined = joined.drop(columns=["facility_fid"])
    return gpd.GeoDataFrame(joined[INVENTORY_EXPORT_COLUMNS], geometry="geometry", crs="EPSG:4326")


def _village_context_frame(villages) -> pd.DataFrame:
    village_cols = [col for col in VILLAGE_CONTEXT_COLUMNS if col in villages.columns]
    context = villages[village_cols].copy()
    context[VILLAGE_ID_COL] = context[VILLAGE_ID_COL].astype(str)
    return context


def _join_unique(values: pd.Series) -> str | None:
    items: set[str] = set()
    for value in values.dropna():
        if isinstance(value, Integral) and not isinstance(value, bool):
            text = str(int(value))
        elif isinstance(value, Real) and not isinstance(value, bool) and float(value).is_integer():
            text = str(int(value))
        else:
            text = str(value).strip()
        if text:
            items.add(text)
    return "|".join(sorted(items)) if items else None


def _build_nearest_service(
    villages,
    l3_attributes: pd.DataFrame,
    l2_attributes: pd.DataFrame,
    nearest_facilities,
):
    gpd = _gpd()
    frames: list[pd.DataFrame] = []

    l3 = l3_attributes.copy()
    if not l3.empty:
        l3[VILLAGE_ID_COL] = l3[VILLAGE_ID_COL].astype(str)
        l3["service_level"] = "l3"
        l3["service_purpose"] = l3["class_l3_facility_class"]
        l3["selected_l3_facility_class"] = l3["class_l3_facility_class"]
        frames.append(l3)

    l2 = l2_attributes.copy()
    if not l2.empty:
        l2[VILLAGE_ID_COL] = l2[VILLAGE_ID_COL].astype(str)
        l2["service_level"] = "l2"
        l2["service_purpose"] = l2["class_l2_filter_group"]
        l2["class_l3_facility_class"] = l2["selected_component_class"]
        l2["selected_l3_facility_class"] = l2["selected_component_class"]
        frames.append(l2)

    if not frames:
        return gpd.GeoDataFrame(pd.DataFrame(), geometry=[], crs="EPSG:4326")

    frame = pd.concat(frames, ignore_index=True, sort=False)
    frame = frame[frame["nearest_facility_uid"].notna()].copy()
    if frame.empty:
        return gpd.GeoDataFrame(frame, geometry=[], crs="EPSG:4326")
    frame = frame.merge(
        _village_context_frame(villages)[[VILLAGE_ID_COL, "pc11_village_id", "NAME"]],
        on=VILLAGE_ID_COL,
        how="left",
    )

    coverage = frame.groupby("nearest_facility_uid").agg(
        nearest_for_cs_feature_ids=(VILLAGE_ID_COL, _join_unique),
        nearest_for_pc11_village_ids=("pc11_village_id", _join_unique),
        nearest_for_village_names=("NAME", _join_unique),
        nearest_for_l1_domains=("class_l1_domain", _join_unique),
        nearest_for_l2_filter_groups=("class_l2_filter_group", _join_unique),
        nearest_for_l3_facility_classes=("selected_l3_facility_class", _join_unique),
        nearest_for_service_purposes=("service_purpose", _join_unique),
        nearest_distance_min_km=("nearest_distance_km", "min"),
        nearest_distance_mean_km=("nearest_distance_km", "mean"),
        nearest_distance_max_km=("nearest_distance_km", "max"),
    )
    coverage = coverage.reset_index()

    if nearest_facilities.empty:
        return gpd.GeoDataFrame(pd.DataFrame(), geometry=[], crs="EPSG:4326")
    nearest = nearest_facilities.rename(columns={"facility_uid": "nearest_facility_uid"})
    nearest = nearest.merge(coverage, on="nearest_facility_uid", how="inner")
    if nearest.empty:
        return gpd.GeoDataFrame(nearest, geometry=[], crs="EPSG:4326")
    nearest["facility_uid"] = nearest["nearest_facility_uid"]
    nearest["facilities_layer_kind"] = OUTPUT_NEAREST
    nearest["title"] = nearest["facility_name"].fillna(nearest["facility_uid"])
    return gpd.GeoDataFrame(nearest, geometry="geometry", crs="EPSG:4326")


def _wide_metric_frame(
    attributes: pd.DataFrame,
    group_column: str,
    prefix: str,
    value_columns: dict[str, str],
) -> pd.DataFrame:
    if attributes.empty:
        return pd.DataFrame({VILLAGE_ID_COL: []})
    frame = attributes.copy()
    frame[VILLAGE_ID_COL] = frame[VILLAGE_ID_COL].astype(str)
    frame["_metric_slug"] = frame[group_column].map(_slug)
    wide_frames: list[pd.DataFrame] = []
    for source_column, suffix in value_columns.items():
        pivot = frame.pivot_table(
            index=VILLAGE_ID_COL,
            columns="_metric_slug",
            values=source_column,
            aggfunc="first",
        )
        pivot.columns = [f"{prefix}_{column}_{suffix}" for column in pivot.columns]
        wide_frames.append(pivot.reset_index())

    out = wide_frames[0]
    for extra in wide_frames[1:]:
        out = out.merge(extra, on=VILLAGE_ID_COL, how="outer")
    return out


def _l1_summary_frame(l2_attributes: pd.DataFrame) -> pd.DataFrame:
    if l2_attributes.empty:
        return pd.DataFrame({VILLAGE_ID_COL: []})
    frame = l2_attributes.copy()
    frame[VILLAGE_ID_COL] = frame[VILLAGE_ID_COL].astype(str)
    frame = frame[frame["nearest_distance_km"].notna()].copy()
    if frame.empty:
        return pd.DataFrame({VILLAGE_ID_COL: []})
    frame["_rank"] = frame.groupby([VILLAGE_ID_COL, "class_l1_domain"])["nearest_distance_km"].rank(
        method="first"
    )
    best = frame[frame["_rank"] == 1].copy()
    best["_domain_slug"] = best["class_l1_domain"].map(_slug)
    distance = best.pivot_table(
        index=VILLAGE_ID_COL,
        columns="_domain_slug",
        values="nearest_distance_km",
        aggfunc="first",
    )
    distance.columns = [f"l1_{column}_nearest_l2_distance_km" for column in distance.columns]
    group = best.pivot_table(
        index=VILLAGE_ID_COL,
        columns="_domain_slug",
        values="class_l2_filter_group",
        aggfunc="first",
    )
    group.columns = [f"l1_{column}_nearest_l2_group" for column in group.columns]
    return distance.reset_index().merge(group.reset_index(), on=VILLAGE_ID_COL, how="outer")


def _build_village_service(villages, l3_attributes: pd.DataFrame, l2_attributes: pd.DataFrame):
    base = villages.drop(
        columns=[col for col in ("_village_latitude", "_village_longitude") if col in villages.columns]
    ).copy()
    base[VILLAGE_ID_COL] = base[VILLAGE_ID_COL].astype(str)

    l3_wide = _wide_metric_frame(
        l3_attributes,
        "class_l3_facility_class",
        "l3",
        {
            "nearest_distance_km": "distance_km",
            "nearest_facility_uid": "facility_uid",
        },
    )
    l2_wide = _wide_metric_frame(
        l2_attributes,
        "class_l2_filter_group",
        "l2",
        {
            "nearest_distance_km": "distance_km",
            "nearest_facility_uid": "facility_uid",
            "selected_component_class": "selected_l3",
        },
    )
    merged = base.merge(l3_wide, on=VILLAGE_ID_COL, how="left")
    merged = merged.merge(l2_wide, on=VILLAGE_ID_COL, how="left")
    merged = merged.merge(_l1_summary_frame(l2_attributes), on=VILLAGE_ID_COL, how="left")
    merged["facilities_layer_kind"] = OUTPUT_VILLAGE_SERVICE
    merged["title"] = merged["NAME"].fillna(merged[VILLAGE_ID_COL])
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
    output_layers: dict[str, Any],
    output_dir: Path,
    layer_name: str,
    zip_output: bool,
) -> tuple[Path, Path | None, dict[str, int]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    gpkg_path = output_dir / f"{layer_name}.gpkg"
    zip_path = output_dir / f"{layer_name}.zip"
    manifest_path = output_dir / f"{layer_name}.manifest.json"
    for path in (gpkg_path, zip_path, manifest_path):
        if path.exists():
            path.unlink()
    legacy_publish_dir = output_dir / "geoserver_layers"
    if legacy_publish_dir.exists():
        shutil.rmtree(legacy_publish_dir)

    row_counts: dict[str, int] = {}
    first = True
    with tempfile.TemporaryDirectory(prefix="facilities_gpkg_") as temp_dir:
        temp_gpkg_path = Path(temp_dir) / gpkg_path.name
        for output_key in ALL_OUTPUTS:
            gdf = output_layers.get(output_key)
            if gdf is None:
                continue
            row_counts[output_key] = int(len(gdf))
            if gdf.empty:
                continue
            _write_layer(
                gdf,
                temp_gpkg_path,
                LOCAL_GPKG_LAYER_NAMES[output_key],
                "w" if first else "a",
            )
            first = False

        if first:
            raise ValueError("No non-empty facilities outputs were produced")
        shutil.move(str(temp_gpkg_path), gpkg_path)

    if zip_output:
        return gpkg_path, _zip_gpkg(gpkg_path), row_counts
    return gpkg_path, None, row_counts


def _write_single_layer_gpkg(gdf, output_dir: Path, layer_name: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    gpkg_path = output_dir / f"{layer_name}.gpkg"
    zip_path = gpkg_path.with_suffix(".zip")
    for path in (gpkg_path, zip_path):
        if path.exists():
            path.unlink()
    with tempfile.TemporaryDirectory(prefix="facilities_geoserver_gpkg_") as temp_dir:
        temp_gpkg_path = Path(temp_dir) / gpkg_path.name
        _write_layer(gdf, temp_gpkg_path, layer_name, "w")
        shutil.move(str(temp_gpkg_path), gpkg_path)
    _zip_gpkg(gpkg_path)
    return gpkg_path


def _publish_to_geoserver(gpkg_path: Path, layer_name: str, overwrite: bool) -> dict[str, Any]:
    try:
        geoserver = Geoserver()
        try:
            geoserver.get_workspace(FACILITIES_GEOSERVER_WORKSPACE)
        except GeoserverException as exc:
            if exc.status != 404:
                raise
            geoserver.create_workspace(FACILITIES_GEOSERVER_WORKSPACE)

        zip_path = gpkg_path.with_suffix(".zip")
        if not zip_path.exists():
            _zip_gpkg(gpkg_path)
        if overwrite:
            geoserver.delete_vector_store(
                workspace=FACILITIES_GEOSERVER_WORKSPACE,
                store=layer_name,
            )
        response = geoserver.create_shp_datastore(
            path=str(zip_path),
            store_name=layer_name,
            workspace=FACILITIES_GEOSERVER_WORKSPACE,
            file_extension="gpkg",
        )
        return {"ok": True, "response": response}
    except Exception as exc:
        logger.exception("Facilities GeoServer publish failed")
        return {
            "ok": False,
            "error_type": exc.__class__.__name__,
            "error": str(exc)[:500],
        }


def _publish_outputs_to_geoserver(
    output_layers: dict[str, Any],
    district: str,
    block: str,
    overwrite: bool,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    with tempfile.TemporaryDirectory(prefix="facilities_geoserver_layers_") as temp_dir:
        publish_dir = Path(temp_dir)
        for output_key, gdf in output_layers.items():
            layer_name = _geoserver_layer_name(output_key, district, block)
            if gdf.empty:
                results[output_key] = {
                    "ok": False,
                    "layer_name": layer_name,
                    "error": "Output layer is empty",
                }
                continue
            gpkg_path = _write_single_layer_gpkg(gdf, publish_dir, layer_name)
            result = _publish_to_geoserver(gpkg_path, layer_name, overwrite)
            result["layer_name"] = layer_name
            result["publish_artifact"] = "temporary_gpkg_zip"
            results[output_key] = result
    return results


def _create_or_refresh_layer_group(layer_names: list[str], district: str, block: str) -> bool:
    if len(layer_names) <= 1:
        return False
    group_name = _layer_name(district, block)
    geoserver = Geoserver()
    try:
        try:
            geoserver.delete_layergroup(group_name, workspace=FACILITIES_GEOSERVER_WORKSPACE)
        except Exception:
            pass
        geoserver.create_layergroup(
            name=group_name,
            mode="named",
            title=f"Facilities {district} {block}",
            abstract_text="Facilities inventory, nearest-service, and village service layers.",
            layers=layer_names,
            workspace=FACILITIES_GEOSERVER_WORKSPACE,
            formats="json",
            keywords=["facilities", "inventory", "nearest", "village_service"],
        )
        return True
    except Exception:
        logger.exception("Facilities GeoServer layer group creation failed")
        return False


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


def _publish_and_register_outputs(
    *,
    output_layers: dict[str, Any],
    output_dir: Path,
    district: str,
    block: str,
    resolved_state: str,
    resolved_district: str,
    resolved_block: str,
    source_path: Path,
    facilities_source: str | None,
    gpkg_path: Path,
    zip_path: Path | None,
    row_counts: dict[str, int],
    output_results: dict[str, dict[str, Any]],
    overwrite: bool,
) -> tuple[dict[str, dict[str, Any]], bool]:
    geoserver = _publish_outputs_to_geoserver(
        output_layers=output_layers,
        district=district,
        block=block,
        overwrite=_bool(overwrite),
    )
    successful_layers: list[str] = []
    for output_key, publish_result in geoserver.items():
        if not publish_result.get("ok"):
            output_results[output_key]["registration_error"] = publish_result
            continue
        geoserver_layer_name = publish_result["layer_name"]
        geoserver_url = (
            f"{settings.GEOSERVER_URL.rstrip('/')}/{FACILITIES_GEOSERVER_WORKSPACE}/ows"
            "?service=WFS&version=1.0.0&request=GetFeature"
            f"&typeName={FACILITIES_GEOSERVER_WORKSPACE}:{geoserver_layer_name}"
            "&outputFormat=application/json"
        )
        layer_output = {
            "is_generated_locally": True,
            "source": source_path.as_posix(),
            "facilities_source": facilities_source,
            "gpkg_path": gpkg_path.as_posix(),
            "zip_path": zip_path.as_posix() if zip_path else None,
            "output_dir": output_dir.as_posix(),
            "row_counts": row_counts,
            "output_key": output_key,
            "local_gpkg_layer": LOCAL_GPKG_LAYER_NAMES[output_key],
            "geoserver_workspace": FACILITIES_GEOSERVER_WORKSPACE,
            "geoserver_layer_name": geoserver_layer_name,
            "geoserver_url": geoserver_url,
        }
        layer_id, registration_error = _register_layer(
            resolved_state,
            resolved_district,
            resolved_block,
            geoserver_layer_name,
            geoserver_url,
            layer_output,
            _bool(overwrite),
        )
        output_results[output_key].update(
            {
                "geoserver_url": geoserver_url,
                "layer_id": layer_id,
                "registration_error": registration_error,
            }
        )
        successful_layers.append(geoserver_layer_name)
    layer_group_created = _create_or_refresh_layer_group(
        successful_layers,
        district,
        block,
    )
    return geoserver, layer_group_created


def generate_facilities_proximity(
    state: str,
    district: str,
    block: str,
    sync_to_geoserver: bool = True,
    overwrite: bool = False,
    outputs: Any = "all",
    zip_output: bool = False,
) -> dict[str, Any]:
    started = time.perf_counter()
    timings: dict[str, float] = {}
    layer_name = _layer_name(district, block)
    selected_outputs = _parse_outputs(outputs)
    bundle_stem = _bundle_stem(layer_name, selected_outputs)
    output_dir = _output_dir(state, district, block)
    source_path = _source_path()
    logger.info(
        "Facilities proximity started: %s/%s/%s outputs=%s",
        state,
        district,
        block,
        selected_outputs,
    )

    resolved_state = _canonical_asset_name(state)
    resolved_district = _canonical_asset_name(district)
    resolved_block = _canonical_asset_name(block)
    try:
        t0 = time.perf_counter()
        logger.info("Facilities proximity reading villages: %s/%s/%s", resolved_state, resolved_district, resolved_block)
        villages = _read_villages(resolved_state, resolved_district, resolved_block)
    except ValueError:
        resolved_state, resolved_district, resolved_block = _resolve_location(state, district, block)
        logger.info("Facilities proximity resolved location: %s/%s/%s", resolved_state, resolved_district, resolved_block)
        villages = _read_villages(resolved_state, resolved_district, resolved_block)
    timings["read_villages_seconds"] = round(time.perf_counter() - t0, 3)
    logger.info("Facilities proximity read %d villages in %.3fs", len(villages), timings["read_villages_seconds"])

    village_ids = villages[VILLAGE_ID_COL].dropna().astype(str).drop_duplicates().tolist()
    output_layers: dict[str, Any] = {}
    proximity_outputs = {OUTPUT_NEAREST, OUTPUT_VILLAGE_SERVICE}
    l3_attributes = pd.DataFrame()
    l2_attributes = pd.DataFrame()

    if OUTPUT_INVENTORY in selected_outputs:
        t0 = time.perf_counter()
        logger.info("Facilities proximity reading inventory rows for %d villages", len(village_ids))
        output_layers[OUTPUT_INVENTORY] = _read_inventory_facilities(villages)
        timings["read_inventory_seconds"] = round(time.perf_counter() - t0, 3)
        logger.info(
            "Facilities proximity read inventory rows in %.3fs: inventory=%d",
            timings["read_inventory_seconds"],
            len(output_layers[OUTPUT_INVENTORY]),
        )

    if proximity_outputs.intersection(selected_outputs):
        t0 = time.perf_counter()
        logger.info("Facilities proximity reading L3/L2 rows for %d villages", len(village_ids))
        l3_attributes = _read_l3_attributes(source_path, village_ids)
        l2_attributes = _read_l2_attributes(source_path, village_ids)
        timings["read_proximity_seconds"] = round(time.perf_counter() - t0, 3)
        logger.info(
            "Facilities proximity read proximity rows in %.3fs: l3=%d l2=%d",
            timings["read_proximity_seconds"],
            len(l3_attributes),
            len(l2_attributes),
        )

    if OUTPUT_NEAREST in selected_outputs:
        t0 = time.perf_counter()
        nearest_facility_uids = (
            pd.concat(
                [
                    l3_attributes["nearest_facility_uid"],
                    l2_attributes["nearest_facility_uid"],
                ],
                ignore_index=True,
            )
            .dropna()
            .astype(str)
            .drop_duplicates()
            .tolist()
        )
        nearest_facilities = _read_nearest_facilities(source_path, nearest_facility_uids)
        timings["read_nearest_facilities_seconds"] = round(time.perf_counter() - t0, 3)
        logger.info(
            "Facilities proximity read distinct nearest facilities in %.3fs: nearest=%d",
            timings["read_nearest_facilities_seconds"],
            len(nearest_facilities),
        )
        t0 = time.perf_counter()
        output_layers[OUTPUT_NEAREST] = _build_nearest_service(
            villages,
            l3_attributes,
            l2_attributes,
            nearest_facilities,
        )
        timings["build_nearest_seconds"] = round(time.perf_counter() - t0, 3)

    if OUTPUT_VILLAGE_SERVICE in selected_outputs:
        t0 = time.perf_counter()
        output_layers[OUTPUT_VILLAGE_SERVICE] = _build_village_service(
            villages,
            l3_attributes,
            l2_attributes,
        )
        timings["build_village_service_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    logger.info("Facilities proximity writing output GPKG: %s", output_dir / f"{bundle_stem}.gpkg")
    gpkg_path, zip_path, row_counts = _write_tehsil_gpkg(
        output_layers=output_layers,
        output_dir=output_dir,
        layer_name=bundle_stem,
        zip_output=_bool(zip_output),
    )
    timings["write_gpkg_seconds"] = round(time.perf_counter() - t0, 3)
    logger.info("Facilities proximity wrote GPKG in %.3fs: %s", timings["write_gpkg_seconds"], gpkg_path)
    source_village_geometry = str(villages.geometry.geom_type.mode().iloc[0])

    geoserver: dict[str, dict[str, Any]] | None = None
    geoserver_layer_group_created = False
    facilities_source = _facilities_path().as_posix() if OUTPUT_INVENTORY in selected_outputs else None
    output_results = _output_results_template(selected_outputs, row_counts, district, block)
    if _bool(sync_to_geoserver):
        t0 = time.perf_counter()
        logger.info("Facilities proximity publishing selected outputs to GeoServer: %s", selected_outputs)
        geoserver, geoserver_layer_group_created = _publish_and_register_outputs(
            output_layers=output_layers,
            output_dir=output_dir,
            district=district,
            block=block,
            resolved_state=resolved_state,
            resolved_district=resolved_district,
            resolved_block=resolved_block,
            source_path=source_path,
            facilities_source=facilities_source,
            gpkg_path=gpkg_path,
            zip_path=zip_path,
            row_counts=row_counts,
            output_results=output_results,
            overwrite=_bool(overwrite),
        )
        timings["publish_geoserver_seconds"] = round(time.perf_counter() - t0, 3)
        logger.info("Facilities proximity GeoServer publish finished in %.3fs: %s", timings["publish_geoserver_seconds"], geoserver)

    elapsed = round(time.perf_counter() - started, 3)
    timings["total_seconds"] = elapsed
    logger.info("Facilities proximity completed %s in %.3fs", layer_name, elapsed)
    return {
        "status": "success",
        "layer_name": layer_name,
        "bundle_name": bundle_stem,
        "selected_outputs": list(selected_outputs),
        "outputs": output_results,
        "row_counts": row_counts,
        "source": source_path.as_posix(),
        "facilities_source": facilities_source,
        "source_village_layer": SOURCE_VILLAGE_LAYER,
        "source_village_geometry": source_village_geometry,
        "gpkg_path": gpkg_path.as_posix(),
        "zip_path": zip_path.as_posix() if zip_path else None,
        "output_dir": output_dir.as_posix(),
        "state_name": resolved_state,
        "district_name": resolved_district,
        "tehsil": resolved_block,
        "sync_to_geoserver": _bool(sync_to_geoserver),
        "geoserver": geoserver,
        "geoserver_layer_group_created": geoserver_layer_group_created,
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
    outputs: Any = "all",
    zip_output: bool = False,
    **_ignored: Any,
) -> dict[str, Any]:
    """Celery wrapper for the local facilities proximity export."""
    return generate_facilities_proximity(
        state=state,
        district=district,
        block=block,
        sync_to_geoserver=sync_to_geoserver,
        overwrite=overwrite,
        outputs=outputs,
        zip_output=zip_output,
    )
