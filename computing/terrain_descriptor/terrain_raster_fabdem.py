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
)
from nrm_app.celery import app
from utilities.constants import GEE_DATASET_PATH


@app.task(bind=True)
def generate_terrain_raster_clip(
    self, state=None, district=None, block=None, gee_account_id=None
):
    ee_initialize(gee_account_id)
    roi_asset_id = (
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    description = (
        "terrain_raster_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )
    asset_id = get_gee_asset_path(state, district, block) + description

    # Load ROI geometry
    roi = ee.FeatureCollection(roi_asset_id)

    # Load the raster image and clip to ROI
    pan_india_raster = ee.Image(
        f"{GEE_DATASET_PATH}/terrain/pan_india_terrain_raster_fabdem"
    )

    task = export_raster_asset_to_gee(
        image=pan_india_raster.clip(roi.geometry()),
        description=description,
        asset_id=asset_id,
        scale=30,
        region=roi.geometry(),
    )

    # Check task status
    task_id_list = check_task_status([task])
    print(f"Task completed. Task IDs: {task_id_list}")

    # Check if asset was created
    layer_id = None

    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)

        task_id = sync_raster_to_gcs(ee.Image(asset_id), 30, description)
        task_id_list = check_task_status([task_id])
        print("task_id_list sync to gcs ", task_id_list)

        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            description,
            asset_id,
            "Terrain Raster",
            layer_version=1.0,
            algorithm="terrain_fabdem",
            algorithm_version=2.0,
        )

        res = sync_raster_gcs_to_geoserver(
            "terrain", description, description, "terrain_raster"
        )
        if res and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
            layer_at_geoserver = True
    return layer_at_geoserver
