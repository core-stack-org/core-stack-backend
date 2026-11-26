from utilities.gee_utils import valid_gee_text

from nrm_app.celery import app

from computing.config_loader import TERRAIN_RASTER_OUTPUT_DIR, TERRAIN_RASTER_PATH
from computing.local_compute_helper import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    build_output_raster_path,
    clip_raster_with_roi,
    get_roi,
    queue_local_raster_for_geoserver,
    read_validated_vector_file,
)


LOCAL_OUTPUT_BASE_DIR = TERRAIN_RASTER_OUTPUT_DIR
GEOSERVER_STYLE = "terrain_raster"
GEOSERVER_WORKSPACE = "terrain"


def run_terrain_raster_fabdem_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=False,
):
    if state and district and block:
        state = str(state).strip().lower()
        district = str(district).strip().lower()
        block = str(block).strip().lower()

    # Uses precomputed tehsil ROI when state/district/block are given,
    # otherwise falls back to the provided `roi` file and `asset_suffix`.
    asset_suffix, roi_gdf = get_roi(
        asset_suffix,
        block,
        district,
        roi,
        state,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    layer_name = f"{valid_gee_text(str(asset_suffix).strip().lower())}_terrain_raster"

    output_raster_path = build_output_raster_path(
        layer_name=layer_name,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        state=state,
        district=district,
        block=block,
    )
    clipped_raster_path = clip_raster_with_roi(
        roi_gdf=roi_gdf,
        raster_path=TERRAIN_RASTER_PATH,
        output_path=output_raster_path,
    )

    print(f"Local clipped FABDEM raster written to: {clipped_raster_path}")

    layer_id = None
    if sync_layer_metadata and state and district and block:
        from computing.utils import save_layer_info_to_db

        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=clipped_raster_path,
            dataset_name="Terrain Raster",
            misc={"is_generated_locally": True},
            algorithm="FABDEM",
            algorithm_version="2.0",
        )

    if push_to_geoserver:
        try:
            geoserver_response = queue_local_raster_for_geoserver(
                file_path=clipped_raster_path,
                layer_name=layer_name,
                workspace=GEOSERVER_WORKSPACE,
                style_name=GEOSERVER_STYLE,
                layer_id=layer_id,
            )
            print(f"GeoServer response: {geoserver_response}")
            if geoserver_response.get("status_code") != 202:
                raise RuntimeError(str(geoserver_response))
        except Exception as error:
            print(f"Failed to queue local FABDEM raster for GeoServer: {error}")
            return False

    return True


def _generate_terrain_raster_clip_task(
    state=None,
    district=None,
    block=None,
    gee_account_id=None,
    asset_suffix=None,
    asset_folder=None,
    proj_id=None,
    roi=None,
    precomputed_roi_dir=None,
    app_type="MWS",
):
    _ = gee_account_id, asset_folder, proj_id, app_type
    return run_terrain_raster_fabdem_local(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        roi=roi,
        precomputed_roi_dir=precomputed_roi_dir,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )


@app.task(bind=True)
def generate_terrain_raster_clip(
    self,
    state=None,
    district=None,
    block=None,
    gee_account_id=None,
    asset_suffix=None,
    asset_folder=None,
    proj_id=None,
    roi=None,
    precomputed_roi_dir=None,
    app_type="MWS",
):
    _ = self
    return _generate_terrain_raster_clip_task(
        state=state,
        district=district,
        block=block,
        gee_account_id=gee_account_id,
        asset_suffix=asset_suffix,
        asset_folder=asset_folder,
        proj_id=proj_id,
        roi=roi,
        precomputed_roi_dir=precomputed_roi_dir,
        app_type=app_type,
    )
