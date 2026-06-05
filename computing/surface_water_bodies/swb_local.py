import os
from pathlib import Path

import geopandas as gpd

from computing.config_loader import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    SWB_VECTOR_OUTPUT_DIR as LOCAL_OUTPUT_BASE_DIR,
    SWB_VECTOR_PATH,
)
from computing.local_compute_helper import (
    build_output_vector_path,
    load_precomputed_roi,
    push_local_vector_to_geoserver,
    read_validated_vector_file,
    write_vector_output,
)
from computing.surface_water_bodies.clip_swb_local import _clip_gdf, _to_geom
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text

GEOSERVER_WORKSPACE = "swb"
LOCAL_ALGORITHM = "local_surface_water_bodies_clip"
LOCAL_ALGORITHM_VERSION = "local-1.0"
DATASET_NAME = "Surface Water Bodies"


def _slug(value, fallback):
    if value is None:
        return fallback
    return valid_gee_text(str(value).strip().lower()) or fallback


def _resolve_source_path(vector_path):
    src = Path(vector_path).expanduser().resolve()
    if not src.exists():
        raise FileNotFoundError(f"Surface water bodies source not found: {src}")

    prepared = src.with_suffix(".fgb")
    return str(prepared if prepared.exists() else src)


def _resolve_roi_gdf(state, district, block, roi=None, roi_path=None):
    if state and district and block:
        return load_precomputed_roi(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
        )

    if roi is not None:
        geometry = _to_geom(roi)
        return gpd.GeoDataFrame({"geometry": [geometry]}, crs="EPSG:4326")

    if roi_path:
        return read_validated_vector_file(
            roi_path,
            f"ROI file has no valid geometries: {roi_path}",
        )

    raise ValueError(
        "Provide either state/district/block or a valid `roi`/`roi_path` for local SWB clipping."
    )


def _resolve_asset_suffix(state, district, block, asset_suffix):
    if state and district and block:
        return f"{_slug(district, 'unknown_district')}_{_slug(block, 'unknown_block')}"
    if asset_suffix and str(asset_suffix).strip():
        return _slug(asset_suffix, "custom")
    raise ValueError(
        "For non state/district/block runs, `asset_suffix` must be provided."
    )


def _layer_name(asset_suffix):
    return f"surface_waterbodies_{asset_suffix}"


def run_swb_local(
    state=None,
    district=None,
    block=None,
    roi=None,
    roi_path=None,
    asset_suffix=None,
    swb_path=SWB_VECTOR_PATH,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    state = str(state).strip().lower() if state else None
    district = str(district).strip().lower() if district else None
    block = str(block).strip().lower() if block else None

    roi_gdf = _resolve_roi_gdf(
        state=state,
        district=district,
        block=block,
        roi=roi,
        roi_path=roi_path,
    )
    roi_geometry = roi_gdf.union_all()
    if roi_geometry.is_empty:
        raise ValueError("ROI geometry is empty after validation.")

    asset_suffix = _resolve_asset_suffix(state, district, block, asset_suffix)
    layer_name = _layer_name(asset_suffix)
    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        custom_subdir=asset_suffix,
        block_fallback="unknown_block",
    )

    clipped_gdf = _clip_gdf(_resolve_source_path(swb_path), roi_geometry)
    if clipped_gdf.empty:
        raise ValueError("No surface water body features intersect the provided ROI.")

    asset_id = write_vector_output(
        gdf=clipped_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local SWB vector: {asset_id}")

    geoserver_ok = False
    if push_to_geoserver:
        geoserver_response = push_local_vector_to_geoserver(
            path=os.path.splitext(asset_id)[0],
            layer_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            file_type="gpkg",
        )
        geoserver_ok = (
            isinstance(geoserver_response, dict)
            and geoserver_response.get("status_code") in (200, 201)
        )
        print(f"GeoServer response for {layer_name}: {geoserver_response}")

    layer_id = None
    is_admin_run = bool(state and district and block)
    if sync_layer_metadata and is_admin_run:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name=DATASET_NAME,
            misc={
                "is_generated_locally": True,
                "feature_count": int(len(clipped_gdf)),
                "geoserver_available": geoserver_ok,
            },
            algorithm=LOCAL_ALGORITHM,
            algorithm_version=LOCAL_ALGORITHM_VERSION,
        )
        if layer_id and geoserver_ok:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)

    return geoserver_ok if push_to_geoserver else True


def _generate_swb_local_task(
    state=None,
    district=None,
    block=None,
    roi=None,
    roi_path=None,
    asset_suffix=None,
    start_year=None,
    end_year=None,
    gee_account_id=None,
    app_type="MWS",
):
    _ = start_year, end_year, gee_account_id, app_type
    return run_swb_local(
        state=state,
        district=district,
        block=block,
        roi=roi,
        roi_path=roi_path,
        asset_suffix=asset_suffix,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )


@app.task(bind=True)
def generate_swb_layer(
    self,
    state=None,
    district=None,
    block=None,
    roi=None,
    roi_path=None,
    asset_suffix=None,
    start_year=None,
    end_year=None,
    gee_account_id=None,
    app_type="MWS",
):
    _ = self
    return _generate_swb_local_task(
        state=state,
        district=district,
        block=block,
        roi=roi,
        roi_path=roi_path,
        asset_suffix=asset_suffix,
        start_year=start_year,
        end_year=end_year,
        gee_account_id=gee_account_id,
        app_type=app_type,
    )
