from utilities.gee_utils import valid_gee_text

from nrm_app.celery import app

from computing.local_compute_helper import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    TERRAIN_RASTER_PATH,
    build_output_raster_path,
    clip_raster_with_roi,
    load_precomputed_roi,
    push_local_raster_to_geoserver,
    read_validated_vector_file,
)


LOCAL_OUTPUT_BASE_DIR = "data/terrain/fabdem_local"
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
        layer_name = (
            f"{valid_gee_text(str(district).strip().lower())}_"
            f"{valid_gee_text(str(block).strip().lower())}_terrain_raster"
        )
        roi_gdf = load_precomputed_roi(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
    else:
        if not roi or not asset_suffix:
            raise ValueError(
                "For non state/district/block runs, both `roi` and `asset_suffix` are required."
            )
        layer_name = f"{asset_suffix}_terrain_raster".lower()
        roi_gdf = read_validated_vector_file(
            roi,
            f"ROI file has no valid geometries: {roi}",
        )

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

    if push_to_geoserver:
        try:
            upload_res, style_res = push_local_raster_to_geoserver(
                file_path=clipped_raster_path,
                layer_name=layer_name,
                workspace=GEOSERVER_WORKSPACE,
                style_name=GEOSERVER_STYLE,
            )
            print(f"GeoServer upload response: {upload_res}")
            print(f"GeoServer style response: {style_res}")
        except Exception as error:
            print(f"Failed to sync local FABDEM raster to GeoServer: {error}")
            return False

    if sync_layer_metadata and state and district and block:
        from computing.STAC_specs import generate_STAC_layerwise
        from computing.utils import save_layer_info_to_db, update_layer_sync_status

        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=clipped_raster_path,
            dataset_name="Terrain Raster",
            algorithm="FABDEM",
            algorithm_version="2.0",
        )

        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")

            layer_stac_generated = generate_STAC_layerwise.generate_raster_stac(
                state=state,
                district=district,
                block=block,
                layer_name="terrain_raster",
            )
            update_layer_sync_status(
                layer_id=layer_id,
                is_stac_specs_generated=layer_stac_generated,
            )

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
