import logging
from pathlib import Path

import ee
import geopandas as gpd

from computing.config_loader import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    SWB_VECTOR_OUTPUT_DIR as LOCAL_OUTPUT_BASE_DIR,
    SWB_VECTOR_PATH,
)
from computing.local_compute_helper import (
    build_output_vector_path,
    load_precomputed_roi,
    read_validated_vector_file,
    write_vector_output,
)
from computing.surface_water_bodies.clip_swb_local import _clip_gdf, _to_geom
from computing.surface_water_bodies.swb3 import (
    waterbody_catchment_streamorder_properties,
)
from computing.surface_water_bodies.swb4 import waterbody_wbc_intersection
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    sync_fc_to_geoserver,
    update_layer_sync_status,
)
from nrm_app.celery import app
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    check_task_status,
    create_gee_dir,
    ee_initialize,
    export_vector_asset_to_gee,
    gdf_to_ee_fc,
    get_gee_dir_path,
    is_gee_asset_exists,
    make_asset_public,
    valid_gee_text,
)

LOCAL_ALGORITHM = "local_surface_water_bodies_clip"
LOCAL_ALGORITHM_VERSION = "local-1.0"
DATASET_NAME = "Surface Water Bodies"
GEOSERVER_WORKSPACE = "swb"
logger = logging.getLogger(__name__)
SQM_PER_HECTARE = 10000.0
GEE_EXPORT_CHUNK_SIZE = 1000


def _slug(value, fallback):
    if value is None:
        return fallback
    return valid_gee_text(str(value).strip().lower()) or fallback


def _resolve_source_path(vector_path):
    src = Path(vector_path).expanduser().resolve()
    candidates = [
        src.parent / "pan_india_waterbodies_with_mws.fgb",
        src.with_suffix(".fgb"),
        src,
    ]
    for candidate in candidates:
        if candidate.exists():
            logger.info("Using SWB source path: %s", candidate)
            return str(candidate)

    raise FileNotFoundError(
        "Surface water bodies source not found. Checked: "
        + ", ".join(str(candidate) for candidate in candidates)
    )


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
    return f"surface_waterbodies_{asset_suffix}_local"


def _final_layer_name(asset_suffix):
    return f"surface_waterbodies_{asset_suffix}"


def _gee_description(asset_suffix):
    return f"swb2_{asset_suffix}_local"


def _resolve_asset_folder_list(state, district, block):
    if state and district and block:
        return [state, district, block]
    return None


def _resolve_gee_asset_id(state, district, block, asset_suffix, app_type):
    asset_folder_list = _resolve_asset_folder_list(state, district, block)
    if not asset_folder_list:
        raise ValueError(
            "Local SWB GEE export requires state, district, and block so the asset can be saved alongside the existing swb2 assets."
        )

    description = _gee_description(asset_suffix)
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )
    if "projects//" in asset_id:
        raise ValueError(
            "Invalid GEE asset path resolved. "
            f"asset_id={asset_id}. "
            "Check GEE_STORAGE_PROJECT / GEE_PATHS configuration."
        )
    return description, asset_id, asset_folder_list


def _prepare_gdf_for_gee(gdf):
    prepared = gdf.copy()
    if prepared.crs is None:
        prepared = prepared.set_crs("EPSG:4326")
    elif prepared.crs.to_epsg() != 4326:
        prepared = prepared.to_crs("EPSG:4326")

    for column in prepared.columns:
        if column == "geometry":
            continue
        prepared[column] = prepared[column].where(prepared[column].notna(), None)
    return prepared


def _convert_area_columns_to_hectares(gdf):
    converted = gdf.copy()
    converted_columns = {}

    for column in list(converted.columns):
        if column == "geometry":
            continue

        target_column = None
        if column == "total_area_m2":
            target_column = "total_area"
        elif column.startswith("area_"):
            target_column = column

        if target_column is None:
            continue

        converted[target_column] = converted[column] / SQM_PER_HECTARE
        if target_column != column:
            converted.drop(columns=[column], inplace=True)
        converted_columns[column] = "converted_in_place_to_hectares"

    return converted, converted_columns


def _union_geometry(gdf):
    try:
        return gdf.union_all()
    except AttributeError:
        return gdf.geometry.unary_union


def _chunk_asset_id(base_asset_id, index):
    return f"{base_asset_id}_chunk_{index:04d}"


def _chunk_description(base_description, index):
    return f"{base_description}_chunk_{index:04d}"


def _export_gdf_to_gee_in_chunks(gdf, base_description, base_asset_id):
    chunk_asset_ids = []
    task_ids = []

    for index, start in enumerate(range(0, len(gdf), GEE_EXPORT_CHUNK_SIZE)):
        chunk = gdf.iloc[start : start + GEE_EXPORT_CHUNK_SIZE].copy()
        chunk_description = _chunk_description(base_description, index)
        chunk_asset_id = _chunk_asset_id(base_asset_id, index)
        logger.info(
            "Starting chunked SWB export: chunk_index=%s start=%s end=%s chunk_size=%s chunk_asset_id=%s",
            index,
            start,
            start + len(chunk),
            len(chunk),
            chunk_asset_id,
        )
        chunk_fc = gdf_to_ee_fc(_prepare_gdf_for_gee(chunk))
        task_id = export_vector_asset_to_gee(
            chunk_fc,
            chunk_description,
            chunk_asset_id,
        )
        if not task_id:
            raise RuntimeError(
                f"Failed to start chunk export for {chunk_asset_id}"
            )
        chunk_asset_ids.append(chunk_asset_id)
        task_ids.append(task_id)

    check_task_status(task_ids)
    logger.info(
        "Completed all chunk exports for base_asset_id=%s chunk_count=%s",
        base_asset_id,
        len(chunk_asset_ids),
    )

    merged_fc = ee.FeatureCollection(
        [ee.FeatureCollection(chunk_asset_id) for chunk_asset_id in chunk_asset_ids]
    ).flatten()
    merge_task_id = export_vector_asset_to_gee(merged_fc, base_description, base_asset_id)
    if not merge_task_id:
        raise RuntimeError(f"Failed to start merged export for {base_asset_id}")
    logger.info(
        "Started merged SWB export from chunks: task_id=%s gee_asset_id=%s",
        merge_task_id,
        base_asset_id,
    )
    check_task_status([merge_task_id])
    logger.info("Completed merged SWB export: gee_asset_id=%s", base_asset_id)

    if not is_gee_asset_exists(base_asset_id):
        raise RuntimeError(
            f"Merged SWB asset was not created after chunk merge: {base_asset_id}"
        )

    for chunk_asset_id in chunk_asset_ids:
        try:
            if is_gee_asset_exists(chunk_asset_id):
                ee.data.deleteAsset(chunk_asset_id)
                logger.info("Deleted temporary SWB chunk asset: %s", chunk_asset_id)
        except Exception as exc:
            logger.warning(
                "Failed to delete temporary SWB chunk asset %s: %s",
                chunk_asset_id,
                exc,
            )


def _push_local_swb_to_geoserver(output_path, layer_name):
    geoserver_response = push_shape_to_geoserver(
        str(Path(output_path).with_suffix("")),
        workspace=GEOSERVER_WORKSPACE,
        layer_name=layer_name,
        file_type="gpkg",
    )
    logger.info("GeoServer response for local SWB layer %s: %s", layer_name, geoserver_response)
    return bool(geoserver_response) and geoserver_response.get("status_code") in (200, 201)


def _continue_swb_in_gee(
    state,
    asset_suffix,
    asset_folder_list,
    app_type,
    gee_account_id,
    roi,
):
    swb2_asset_suffix = f"{asset_suffix}_local"
    swb3_task_id, swb3_asset_id = waterbody_catchment_streamorder_properties(
        roi=roi,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        app_type=app_type,
        gee_account_id=gee_account_id,
        supporting_asset_suffix=asset_suffix,
        swb2_asset_suffix=swb2_asset_suffix,
    )
    if swb3_task_id:
        check_task_status([swb3_task_id])
    if not is_gee_asset_exists(swb3_asset_id):
        raise RuntimeError(f"SWB3 GEE asset was not created: {swb3_asset_id}")

    swb4_task_id, swb4_asset_id = waterbody_wbc_intersection(
        roi=roi,
        state=state,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        app_type=app_type,
    )
    if swb4_task_id:
        check_task_status([swb4_task_id])
    if not is_gee_asset_exists(swb4_asset_id):
        raise RuntimeError(f"SWB4 GEE asset was not created: {swb4_asset_id}")

    make_asset_public(swb3_asset_id)
    make_asset_public(swb4_asset_id)
    return asset_suffix, swb3_asset_id


def _sync_final_swb(
    state,
    district,
    block,
    layer_name,
    asset_suffix,
    asset_id,
    start_year,
    end_year,
    push_to_geoserver,
    sync_layer_metadata,
):
    layer_id = None
    if sync_layer_metadata:
        misc = {
            "is_generated_locally": True,
            "source_stage": "swb3_gee_from_swb2_local",
        }
        if start_year is not None:
            misc["start_year"] = start_year
        if end_year is not None:
            misc["end_year"] = end_year
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name=DATASET_NAME,
            misc=misc,
            algorithm=LOCAL_ALGORITHM,
            algorithm_version=LOCAL_ALGORITHM_VERSION,
        )

    if not push_to_geoserver:
        return True

    response = sync_fc_to_geoserver(
        ee.FeatureCollection(asset_id),
        asset_suffix,
        layer_name,
        workspace=GEOSERVER_WORKSPACE,
    )
    synced = bool(response) and response.get("status_code") in (200, 201)
    if synced and layer_id:
        update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
    return synced


def _complete_swb_pipeline(
    state,
    district,
    block,
    asset_suffix,
    asset_folder_list,
    app_type,
    gee_account_id,
    roi,
    start_year,
    end_year,
    push_to_geoserver,
    sync_layer_metadata,
):
    final_asset_suffix, final_asset_id = _continue_swb_in_gee(
        state=state,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        app_type=app_type,
        gee_account_id=gee_account_id,
        roi=roi,
    )
    return _sync_final_swb(
        state=state,
        district=district,
        block=block,
        layer_name=_final_layer_name(asset_suffix),
        asset_suffix=final_asset_suffix,
        asset_id=final_asset_id,
        start_year=start_year,
        end_year=end_year,
        push_to_geoserver=push_to_geoserver,
        sync_layer_metadata=sync_layer_metadata,
    )


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
    gee_account_id=None,
    app_type="MWS",
    start_year=None,
    end_year=None,
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
    logger.info(
        "Starting local SWB generation: state=%s district=%s block=%s asset_suffix=%s app_type=%s",
        state,
        district,
        block,
        asset_suffix,
        app_type,
    )
    logger.info("Resolved ROI rows=%s crs=%s", len(roi_gdf), roi_gdf.crs)
    roi_geometry = _union_geometry(roi_gdf)
    if roi_geometry.is_empty:
        raise ValueError("ROI geometry is empty after validation.")

    asset_suffix = _resolve_asset_suffix(state, district, block, asset_suffix)
    layer_name = _layer_name(asset_suffix)
    gee_description, gee_asset_id, asset_folder_list = _resolve_gee_asset_id(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        app_type=app_type,
    )
    logger.info(
        "Resolved local SWB names: layer_name=%s gee_description=%s gee_asset_id=%s asset_folder_list=%s",
        layer_name,
        gee_description,
        gee_asset_id,
        asset_folder_list,
    )

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        custom_subdir=asset_suffix,
        block_fallback="unknown_block",
    )
    logger.info("Local SWB output path: %s", output_path)

    ee_initialize(gee_account_id)
    gee_roi = gdf_to_ee_fc(_prepare_gdf_for_gee(roi_gdf[["geometry"]]))

    if is_gee_asset_exists(gee_asset_id):
        logger.info("GEE asset already exists, reusing: %s", gee_asset_id)
        layer_id = None
        if sync_layer_metadata:
            layer_id = save_layer_info_to_db(
                state=state,
                district=district,
                block=block,
                layer_name=layer_name,
                asset_id=gee_asset_id,
                dataset_name=DATASET_NAME,
                misc={"is_generated_locally": True, "source_stage": "swb2_local"},
                algorithm=LOCAL_ALGORITHM,
                algorithm_version=LOCAL_ALGORITHM_VERSION,
            )
        make_asset_public(gee_asset_id)

        if push_to_geoserver and output_path.exists():
            layer_at_geoserver = _push_local_swb_to_geoserver(
                output_path=output_path, layer_name=layer_name
            )
            if layer_at_geoserver and layer_id:
                update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
                logger.info("Sync to GeoServer flag updated for existing local SWB layer")
        return _complete_swb_pipeline(
            state=state,
            district=district,
            block=block,
            asset_suffix=asset_suffix,
            asset_folder_list=asset_folder_list,
            app_type=app_type,
            gee_account_id=gee_account_id,
            roi=gee_roi,
            start_year=start_year,
            end_year=end_year,
            push_to_geoserver=push_to_geoserver,
            sync_layer_metadata=sync_layer_metadata,
        )

    clipped_gdf = _clip_gdf(_resolve_source_path(swb_path), roi_geometry)
    if clipped_gdf.empty:
        raise ValueError("No surface water body features intersect the provided ROI.")
    clipped_gdf, converted_area_columns = _convert_area_columns_to_hectares(clipped_gdf)
    logger.info(
        "Clipped SWB features: count=%s columns=%s",
        len(clipped_gdf),
        list(clipped_gdf.columns),
    )
    logger.info(
        "Converted SWB area columns from square meters to hectares: %s",
        converted_area_columns,
    )

    local_asset_path = write_vector_output(
        gdf=clipped_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    logger.info("Saved local SWB vector to disk: %s", local_asset_path)

    logger.info(
        "Initialized Earth Engine for local SWB export: gee_account_id=%s",
        gee_account_id,
    )
    create_gee_dir(asset_folder_list, GEE_PATHS[app_type]["GEE_ASSET_PATH"])
    logger.info(
        "Ensured GEE directory exists under base path=%s for folders=%s",
        GEE_PATHS[app_type]["GEE_ASSET_PATH"],
        asset_folder_list,
    )
    ee_fc = gdf_to_ee_fc(_prepare_gdf_for_gee(clipped_gdf))
    logger.info("Prepared EE FeatureCollection for export: gee_asset_id=%s", gee_asset_id)
    task_id = None
    try:
        task_id = export_vector_asset_to_gee(ee_fc, gee_description, gee_asset_id)
    except Exception as exc:
        logger.warning(
            "Inline GEE export raised an exception for asset_id=%s: %s",
            gee_asset_id,
            exc,
        )

    if task_id:
        logger.info(
            "Started GEE export task: task_id=%s gee_description=%s gee_asset_id=%s",
            task_id,
            gee_description,
            gee_asset_id,
        )
        check_task_status([task_id])
        logger.info(
            "Completed GEE export task: task_id=%s gee_asset_id=%s",
            task_id,
            gee_asset_id,
        )
    else:
        logger.info(
            "Inline GEE export could not be started for asset_id=%s. Falling back to chunked FeatureCollection export.",
            gee_asset_id,
        )
        _export_gdf_to_gee_in_chunks(
            gdf=clipped_gdf,
            base_description=gee_description,
            base_asset_id=gee_asset_id,
        )
        logger.info(
            "Triggered chunked SWB GEE export fallback: gee_asset_id=%s chunk_size=%s",
            gee_asset_id,
            GEE_EXPORT_CHUNK_SIZE,
        )

    if not is_gee_asset_exists(gee_asset_id):
        raise RuntimeError(f"GEE asset was not created: {gee_asset_id}")
    make_asset_public(gee_asset_id)
    logger.info("Marked GEE asset public: %s", gee_asset_id)

    layer_id = None
    if sync_layer_metadata:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=gee_asset_id,
            dataset_name=DATASET_NAME,
            misc={
                "is_generated_locally": True,
                "feature_count": int(len(clipped_gdf)),
                "local_vector_path": local_asset_path,
                "source_stage": "swb2_local",
            },
            algorithm=LOCAL_ALGORITHM,
            algorithm_version=LOCAL_ALGORITHM_VERSION,
        )
        logger.info(
            "Saved local SWB layer metadata: layer_id=%s gee_asset_id=%s layer_name=%s",
            layer_id,
            gee_asset_id,
            layer_name,
        )

    if push_to_geoserver:
        layer_at_geoserver = _push_local_swb_to_geoserver(
            output_path=local_asset_path, layer_name=layer_name
        )
        if layer_at_geoserver and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            logger.info("Sync to GeoServer flag updated for local SWB layer: %s", layer_name)

    return _complete_swb_pipeline(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        app_type=app_type,
        gee_account_id=gee_account_id,
        roi=gee_roi,
        start_year=start_year,
        end_year=end_year,
        push_to_geoserver=push_to_geoserver,
        sync_layer_metadata=sync_layer_metadata,
    )


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
    return run_swb_local(
        state=state,
        district=district,
        block=block,
        roi=roi,
        roi_path=roi_path,
        asset_suffix=asset_suffix,
        push_to_geoserver=True,
        sync_layer_metadata=True,
        gee_account_id=gee_account_id,
        app_type=app_type,
        start_year=start_year,
        end_year=end_year,
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
