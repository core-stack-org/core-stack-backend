import ee
from nrm_app.celery import app
from computing.utils import (
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    export_raster_asset_to_gee,
    make_asset_public,
)
from constants.pan_india_urls import DISTANCE_TO_UPSTREAM_DL


@app.task(bind=True)
def generate_distance_to_nearest_drainage_line(
    self, state, district, block, gee_account_id
):
    ee_initialize(gee_account_id)
    description = (
        "distance_to_drainage_line_"
        + valid_gee_text(district)
        + "_"
        + valid_gee_text(block)
    )

    roi = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    distance_upsream_dl = ee.Image(DISTANCE_TO_UPSTREAM_DL)
    raster = distance_upsream_dl.clip(roi.geometry())

    # Generate raster Layer
    layer_status = distance_to_drainage_line_raster_generation(
        state, district, block, description, roi, raster
    )
    return layer_status


def distance_to_drainage_line_raster_generation(
    state, district, block, description, roi, raster
):
    raster_asset_id = (
        get_gee_asset_path(state, district, block) + description + "_raster"
    )
    if not is_gee_asset_exists(raster_asset_id):
        task_id = export_raster_asset_to_gee(
            image=raster,
            description=description + "_raster",
            asset_id=raster_asset_id,
            scale=30,
            region=roi.geometry(),
        )
        distance_to_nearest_drainage_line__task_id_list = check_task_status([task_id])
        print(
            "Distance to nearest drainage line task_id list",
            distance_to_nearest_drainage_line__task_id_list,
        )

    layer_id = None
    layer_at_geoserver = False
    if is_gee_asset_exists(raster_asset_id):
        """Sync image to google cloud storage and then to geoserver"""
        image = ee.Image(raster_asset_id)
        task_id = sync_raster_to_gcs(image, 30, description + "_raster")

        task_id_list = check_task_status([task_id])
        print("task_id_list sync to gcs ", task_id_list)

        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=description + "_raster",
            asset_id=raster_asset_id,
            dataset_name="Distance to Drainage Line",
        )
        make_asset_public(raster_asset_id)
        res = sync_raster_gcs_to_geoserver(
            "distance_nearest_upstream_DL",
            description + "_raster",
            description + "_raster",
            style_name="distance_nearest_upstream_DL",
        )
        if res and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
            layer_at_geoserver = True
    return layer_at_geoserver
