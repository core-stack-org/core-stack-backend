import os
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text
from computing.utils import save_layer_info_to_db
from computing.local_compute_helper import (
    PROJECT_ROOT,
    build_output_raster_path,
    get_roi,
    clip_raster_with_roi,
    queue_local_raster_for_geoserver,
)
from computing.config_loader import (
    PAN_INDIA_CATCHMENT_AREA_PATH,
    LOCAL_CATCHMENT_AREA_OUTPUT,
)

GEOSERVER_WORKSPACE = "catchment_area_singleflow"
CATCHMENT_AREA_STYLE_NAME = "catchment_area_singleflow"


@app.task(bind=True)
def generate_catchment_area_singleflow_local(
    self,
    state=None,
    district=None,
    block=None,
    gee_account_id=None,
    proj_id=None,
    roi_path=None,
    asset_suffix=None,
    asset_folder=None,
    app_type="MWS",
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
    roi=None,
):
    roi = roi or roi_path
    if state and district and block:
        state = str(state).strip().lower()
        district = str(district).strip().lower()
        block = str(block).strip().lower()
    asset_suffix, watersheds_gdf = get_roi(
        asset_suffix,
        block,
        district,
        roi,
        state,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    layer_name_base = f"catchment_area_{asset_suffix}_raster".lower()

    # Raster Processing
    raster_layer_name = f"{layer_name_base}"
    output_raster_path = build_output_raster_path(
        layer_name=raster_layer_name,
        output_base_dir=LOCAL_CATCHMENT_AREA_OUTPUT,
        state=state,
        district=district,
        block=block,
    )

    print("Clipping Catchment Area raster...")
    clipped_raster_path = clip_raster_with_roi(
        roi_gdf=watersheds_gdf,
        raster_path=PAN_INDIA_CATCHMENT_AREA_PATH,
        output_path=output_raster_path,
        raster_label="Catchment Area Raster",
    )
    print(f"Saved clipped Catchment Area raster to: {clipped_raster_path}")

    layer_at_geoserver = False
    raster_layer_id = None
    if sync_layer_metadata and state and district and block:
        raster_layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=raster_layer_name,
            asset_id=str(clipped_raster_path),
            dataset_name="Catchment Area",
            misc={"is_generated_locally": True},
        )

    if push_to_geoserver:
        response = queue_local_raster_for_geoserver(
            file_path=str(clipped_raster_path),
            layer_name=raster_layer_name,
            workspace=GEOSERVER_WORKSPACE,
            style_name=CATCHMENT_AREA_STYLE_NAME,
            layer_id=raster_layer_id,
        )
        layer_at_geoserver = response.get("status_code") == 202
        print(f"Queued raster {raster_layer_name} for GeoServer.")

    return layer_at_geoserver
