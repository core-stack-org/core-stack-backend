import ee
from computing.utils import (
    save_layer_info_to_db,
    update_layer_sync_status,
)

from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    check_task_status,
    export_raster_asset_to_gee,
    make_asset_public,
    is_gee_asset_exists,
    get_gee_asset_path,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    get_gee_dir_path,
)
from nrm_app.celery import app
from utilities.constants import GEE_DATASET_PATH, GEE_PATHS
from computing.STAC_specs import generate_STAC_layerwise


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
    app_type="MWS",
):
    ee_initialize(gee_account_id)
    if state and district and block:
        roi_asset_id = (
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )

        asset_suffix = (
            "terrain_raster_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
        )
        layer_name = (
            valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_terrain_raster"
        )
        asset_id = get_gee_asset_path(state, district, block) + asset_suffix
    else:
        roi_asset_id = roi
        layer_name = f"{asset_suffix}_terrain_raster".lower()
        asset_id = (
            get_gee_dir_path(
                asset_folder, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + f"terrain_raster_{asset_suffix}"
        )
    # Load ROI geometry
    roi = ee.FeatureCollection(roi_asset_id)

    # Load the raster image and clip to ROI
    pan_india_raster = ee.Image(
        f"{GEE_DATASET_PATH}/terrain/pan_india_terrain_raster_fabdem"
    )

    task = export_raster_asset_to_gee(
        image=pan_india_raster.clip(roi.union().geometry()),
        description=asset_suffix,
        asset_id=asset_id,
        scale=30,
        region=roi.geometry(),
    )

    # Check task status
    task_id_list = check_task_status([task])
    print(f"Task completed. Task IDs: {task_id_list}")

    # Check if asset was created
    layer_id = None
    layer_at_geoserver = False

    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)

        task_id = sync_raster_to_gcs(ee.Image(asset_id), 30, layer_name)
        task_id_list = check_task_status([task_id])
        print("task_id_list sync to gcs ", task_id_list)
        if state and district and block:
            layer_id = save_layer_info_to_db(
                state,
                district,
                block,
                layer_name,
                asset_id,
                "Terrain Raster",
                algorithm="FABDEM",
                algorithm_version="2.0",
            )

        res = sync_raster_gcs_to_geoserver(
            "terrain", layer_name, layer_name, "terrain_raster"
        )
        if res and layer_id:
            if state and district and block:
                update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
                print("sync to geoserver flag is updated")

                layer_STAC_generated = False
                layer_STAC_generated = generate_STAC_layerwise.generate_raster_stac(
                    state=state,
                    district=district,
                    block=block,
                    layer_name="terrain_raster",
                )

                update_layer_sync_status(
                    layer_id=layer_id, is_stac_specs_generated=layer_STAC_generated
                )
        layer_at_geoserver = True
    return layer_at_geoserver
