"""Clip livestock census data by tehsil and optionally publish it.

Names may use spaces or snake_case. The local GPKG is always written first;
successful GeoServer layers are registered in the database by their WFS URL.
"""

from __future__ import annotations

import re
import sqlite3
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

import pyogrio
from django.conf import settings

from computing.models import Dataset, LayerType
from computing.utils import (
    fix_invalid_geometry_in_gdf,
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from nrm_app.celery import app
from utilities.constants import LIVESTOCKS
from utilities.geoserver_utils import Geoserver, GeoserverException


SOURCE_LAYER = "livestock"
LOCATION_INDEX = "idx_livestock_location"
OUTPUT_DIR = "data/livestock/output/tehsil_data"
GEOSERVER_WORKSPACE = "livestocks"
LAYER_PREFIX = "livestocks"
DATASET_NAME = "Livestock Census"
ALGORITHM = "local-pan-india-tehsil-clip"
ALGORITHM_VERSION = "1.0"
INTEGER_COLUMNS = (
    "pc11_village_id",
    "pc11_state_id",
    "pc11_district_id",
    "pc11_subdistrict_id",
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
)


def _repo_path(path: str | Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else Path(settings.BASE_DIR) / path


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


def _ensure_source_index(source_path: Path) -> None:
    """Create the location index once; future clips then use indexed reads."""
    with sqlite3.connect(source_path) as connection:
        connection.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {LOCATION_INDEX}
            ON {SOURCE_LAYER} (state_name, district_name, TEHSIL)
            """
        )
        connection.commit()


@lru_cache(maxsize=1)
def _location_rows() -> tuple[tuple[str, str, str], ...]:
    source_path = _repo_path(LIVESTOCKS)
    _ensure_source_index(source_path)
    rows = pyogrio.read_dataframe(
        source_path,
        sql=(
            "SELECT DISTINCT state_name, district_name, TEHSIL "
            f"FROM {SOURCE_LAYER}"
        ),
        read_geometry=False,
    )
    return tuple(
        (row.state_name, row.district_name, row.TEHSIL)
        for row in rows.itertuples(index=False)
    )


def _resolve_location(state: str, district: str, block: str) -> tuple[str, str, str]:
    state_key, district_key, block_key = map(_match_key, (state, district, block))
    state_matches = [row for row in _location_rows() if _match_key(row[0]) == state_key]
    if not state_matches:
        raise ValueError(f"State not found in livestock asset: {state}")

    district_matches = [
        row for row in state_matches if _match_key(row[1]) == district_key
    ]
    if not district_matches:
        available = sorted({row[1] for row in state_matches})[:20]
        raise ValueError(
            f"District not found in livestock asset: {district}. "
            f"Available examples: {available}"
        )

    block_matches = [row for row in district_matches if _match_key(row[2]) == block_key]
    if not block_matches:
        available = sorted({row[2] for row in district_matches})[:30]
        raise ValueError(
            f"TEHSIL/block not found in livestock asset: {block}. "
            f"Available examples: {available}"
        )
    return block_matches[0]


def _read_clip(state_name: str, district_name: str, tehsil_name: str):
    source_path = _repo_path(LIVESTOCKS)
    _ensure_source_index(source_path)
    where = (
        f"state_name = '{_quote_sql(state_name)}' AND "
        f"district_name = '{_quote_sql(district_name)}' AND "
        f"TEHSIL = '{_quote_sql(tehsil_name)}'"
    )
    gdf = pyogrio.read_dataframe(source_path, layer=SOURCE_LAYER, where=where)
    if gdf.empty:
        raise ValueError(f"No livestock rows found for {state_name}/{district_name}/{tehsil_name}")
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    return _coerce_nullable_integer_columns(fix_invalid_geometry_in_gdf(gdf))


def _coerce_nullable_integer_columns(gdf):
    for column in INTEGER_COLUMNS:
        if column in gdf.columns:
            gdf[column] = gdf[column].astype("Int64")
    return gdf


def _write_clip(gdf, output_dir: Path, layer_name: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    gpkg_path = output_dir / f"{layer_name}.gpkg"
    zip_path = output_dir / f"{layer_name}.zip"
    for path in (gpkg_path, zip_path):
        if path.exists():
            path.unlink()
    pyogrio.write_dataframe(gdf, gpkg_path, layer=layer_name, driver="GPKG")
    return gpkg_path


def _publish_to_geoserver(gpkg_path: Path, layer_name: str, overwrite: bool) -> dict[str, Any]:
    try:
        geoserver = Geoserver()
        try:
            geoserver.get_workspace(GEOSERVER_WORKSPACE)
        except GeoserverException as exc:
            if exc.status != 404:
                raise
            geoserver.create_workspace(GEOSERVER_WORKSPACE)

        response = push_shape_to_geoserver(
            str(gpkg_path.with_suffix("")),
            store_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name if overwrite else None,
            file_type="gpkg",
        )
        return {"ok": True, "response": response}
    except Exception as exc:
        return {
            "ok": False,
            "error_type": exc.__class__.__name__,
            "error": str(exc)[:500],
        }


@app.task(bind=True)
def generate_livestocks_layer_task(
    self,
    state: str,
    district: str,
    block: str,
    sync_to_geoserver: bool = True,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Clip one state/district/TEHSIL from the local pan-India livestock GPKG."""
    started = time.perf_counter()
    layer_name = _layer_name(district, block)
    output_dir = _output_dir(state, district, block)

    resolved_state = _canonical_asset_name(state)
    resolved_district = _canonical_asset_name(district)
    resolved_block = _canonical_asset_name(block)
    try:
        gdf = _read_clip(resolved_state, resolved_district, resolved_block)
    except ValueError:
        resolved_state, resolved_district, resolved_block = _resolve_location(
            state, district, block
        )
        gdf = _read_clip(resolved_state, resolved_district, resolved_block)
    gpkg_path = _write_clip(gdf, output_dir, layer_name)

    geoserver = None
    layer_id = None
    geoserver_url = None
    if _bool(sync_to_geoserver):
        geoserver = _publish_to_geoserver(gpkg_path, layer_name, _bool(overwrite))
        if geoserver.get("ok"):
            geoserver_url = (
                f"{settings.GEOSERVER_URL.rstrip('/')}/{GEOSERVER_WORKSPACE}/ows"
                "?service=WFS&version=1.0.0&request=GetFeature"
                f"&typeName={GEOSERVER_WORKSPACE}:{layer_name}"
                "&outputFormat=application/json"
            )
            Dataset.objects.get_or_create(
                name=DATASET_NAME,
                defaults={
                    "layer_type": LayerType.VECTOR,
                    "workspace": GEOSERVER_WORKSPACE,
                },
            )
            layer_id = save_layer_info_to_db(
                state=resolved_state,
                district=resolved_district,
                block=resolved_block,
                layer_name=layer_name,
                asset_id=geoserver_url,
                dataset_name=DATASET_NAME,
                algorithm=ALGORITHM,
                algorithm_version=ALGORITHM_VERSION,
                misc={
                    "is_generated_locally": True,
                    "source": _repo_path(LIVESTOCKS).as_posix(),
                    "gpkg_path": gpkg_path.as_posix(),
                    "output_dir": output_dir.as_posix(),
                    "geoserver_workspace": GEOSERVER_WORKSPACE,
                    "geoserver_layer_name": layer_name,
                    "geoserver_url": geoserver_url,
                },
                is_override=_bool(overwrite),
                is_gee_asset=False,
            )
            if layer_id:
                update_layer_sync_status(
                    layer_id=layer_id,
                    sync_to_geoserver=True,
                )

    return {
        "status": "success",
        "layer_name": layer_name,
        "rows": int(len(gdf)),
        "source": _repo_path(LIVESTOCKS).as_posix(),
        "gpkg_path": gpkg_path.as_posix(),
        "output_dir": output_dir.as_posix(),
        "state_name": resolved_state,
        "district_name": resolved_district,
        "tehsil": resolved_block,
        "sync_to_geoserver": _bool(sync_to_geoserver),
        "geoserver": geoserver,
        "geoserver_url": geoserver_url,
        "layer_id": layer_id,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
