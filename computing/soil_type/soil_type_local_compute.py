import logging
from pathlib import Path

from computing.config_loader import (
    INDIA_SUBSOIL_TEXTURE_PATH,
    LOCAL_SOIL_TYPE_RASTER_OUTPUT,
    LOCAL_SOIL_TYPE_VECTOR_OUTPUT,
)
from computing.local_compute_helper import (
    build_output_raster_path,
    build_output_vector_path,
    clip_raster_with_roi,
    compute_categorical_raster_areas_for_watersheds,
    load_precomputed_watersheds,
    push_local_raster_to_geoserver,
    push_local_vector_to_geoserver,
    read_validated_vector_file,
    write_vector_output,
)
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text

logger = logging.getLogger(__name__)

GEOSERVER_WORKSPACE = "soil_type"
LOCAL_ALGORITHM = "local_subsoil_texture"
LOCAL_ALGORITHM_VERSION = "local-1.0"

# HWSD S_USDA_TEX_CLASS codes for subsoil (30-100 cm).
SUBSOIL_TEXTURE_CLASS_DEFINITIONS = (
    {"value": 1, "label": "heavy_clay_area_ha"},
    {"value": 2, "label": "silty_clay_area_ha"},
    {"value": 3, "label": "clay_area_ha"},
    {"value": 4, "label": "silty_clay_loam_area_ha"},
    {"value": 5, "label": "clay_loam_area_ha"},
    {"value": 6, "label": "silt_area_ha"},
    {"value": 7, "label": "silt_loam_area_ha"},
    {"value": 8, "label": "sandy_clay_area_ha"},
    {"value": 9, "label": "loam_area_ha"},
    {"value": 10, "label": "sandy_clay_loam_area_ha"},
    {"value": 11, "label": "sandy_loam_area_ha"},
    {"value": 12, "label": "loamy_sand_area_ha"},
    {"value": 13, "label": "sand_area_ha"},
)


def _slug(value, fallback):
    return valid_gee_text(str(value).strip().lower()) or fallback


def _layer_names(district=None, block=None, asset_suffix=None):
    if district and block:
        prefix = f"{_slug(district, 'unknown_district')}_{_slug(block, 'unknown_block')}"
    else:
        prefix = _slug(asset_suffix, "custom")
    return f"{prefix}_soil_type_raster", f"{prefix}_soil_type_vector"


def run_soil_type_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi_path=None,
    gee_account_id=None,
    soil_type_path=INDIA_SUBSOIL_TEXTURE_PATH,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    _ = gee_account_id
    is_tehsil_run = bool(state and district and block)
    if is_tehsil_run:
        watersheds_gdf, roi_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
    else:
        if not roi_path or not asset_suffix:
            raise ValueError(
                "roi_path and asset_suffix are required for custom soil type runs."
            )
        watersheds_gdf = read_validated_vector_file(
            roi_path,
            f"ROI file has no valid geometries: {roi_path}",
        )
        roi_source = str(roi_path)
    logger.info("Soil type ROI source: %s", roi_source)

    soil_type_path = Path(soil_type_path)
    if not soil_type_path.exists():
        raise FileNotFoundError(
            f"India subsoil texture raster not found: {soil_type_path}"
        )

    raster_layer_name, vector_layer_name = _layer_names(
        district=district,
        block=block,
        asset_suffix=asset_suffix,
    )

    raster_output_path = build_output_raster_path(
        layer_name=raster_layer_name,
        output_base_dir=LOCAL_SOIL_TYPE_RASTER_OUTPUT,
        state=state,
        district=district,
        block=block,
    )
    raster_asset_id = clip_raster_with_roi(
        roi_gdf=watersheds_gdf,
        raster_path=soil_type_path,
        output_path=raster_output_path,
        raster_label="India subsoil texture raster",
    )
    logger.info("Saved clipped soil type raster: %s", raster_asset_id)

    result_gdf = compute_categorical_raster_areas_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        raster_path=soil_type_path,
        class_definitions=SUBSOIL_TEXTURE_CLASS_DEFINITIONS,
    )
    vector_output_path = build_output_vector_path(
        layer_name=vector_layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_SOIL_TYPE_VECTOR_OUTPUT,
    )
    vector_asset_id = write_vector_output(
        result_gdf,
        vector_output_path,
        vector_layer_name,
    )
    logger.info("Saved soil type watershed vector: %s", vector_asset_id)
    logger.info("Soil type vector columns: %s", list(result_gdf.columns))

    raster_geoserver_ok = False
    vector_geoserver_ok = False
    vector_geoserver_response = None
    if push_to_geoserver:
        push_local_raster_to_geoserver(
            file_path=raster_asset_id,
            layer_name=raster_layer_name,
            workspace=GEOSERVER_WORKSPACE,
        )
        raster_geoserver_ok = True
        vector_geoserver_response = push_local_vector_to_geoserver(
            path=vector_asset_id,
            layer_name=vector_layer_name,
            workspace=GEOSERVER_WORKSPACE,
        )
        vector_geoserver_ok = (
            isinstance(vector_geoserver_response, dict)
            and vector_geoserver_response.get("status_code") in (200, 201)
        )

    if sync_layer_metadata and is_tehsil_run:
        common_misc = {
            "is_generated_locally": True,
            "source": "india_subsoil_texture",
        }
        raster_layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=raster_layer_name,
            asset_id=raster_asset_id,
            dataset_name="Soil Type",
            misc={
                **common_misc,
                "geoserver_available": raster_geoserver_ok,
                "output_kind": "raster",
            },
            algorithm=LOCAL_ALGORITHM,
            algorithm_version=LOCAL_ALGORITHM_VERSION,
        )
        if raster_layer_id and raster_geoserver_ok:
            update_layer_sync_status(
                layer_id=raster_layer_id,
                sync_to_geoserver=True,
            )

        vector_layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=vector_layer_name,
            asset_id=vector_asset_id,
            dataset_name="Soil Type",
            misc={
                **common_misc,
                "geoserver_available": vector_geoserver_ok,
                "geoserver_sync_response": vector_geoserver_response,
                "output_kind": "vector",
            },
            algorithm=LOCAL_ALGORITHM,
            algorithm_version=LOCAL_ALGORITHM_VERSION,
        )
        if vector_layer_id and vector_geoserver_ok:
            update_layer_sync_status(
                layer_id=vector_layer_id,
                sync_to_geoserver=True,
            )

    return (
        raster_geoserver_ok and vector_geoserver_ok
        if push_to_geoserver
        else True
    )


@app.task(bind=True)
def generate_soil_type_local(
    self,
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi_path=None,
    gee_account_id=None,
    soil_type_path=INDIA_SUBSOIL_TEXTURE_PATH,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    _ = self
    return run_soil_type_local(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        roi_path=roi_path,
        gee_account_id=gee_account_id,
        soil_type_path=soil_type_path,
        precomputed_roi_dir=precomputed_roi_dir,
        push_to_geoserver=push_to_geoserver,
        sync_layer_metadata=sync_layer_metadata,
    )
