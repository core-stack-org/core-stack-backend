import ee
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_dir_path,
    get_gee_asset_path,
    check_task_status,
    is_gee_asset_exists,
)
from constants.pan_india_path import CATCHMENT_AREA
from nrm_app.celery import app
from celery import shared_task


@shared_task(bind=True)
def generate_catchment_area_for_block(
    self,
    state=None,
    district=None,
    block=None,
    roi_path=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    start_year=None,
    end_year=None,
    gee_account_id=None,
):
    ee_initialize(gee_account_id)
    if state and district and block:
        description = (
            "catchmenta_area_raster_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
        )
        asset_id = get_gee_asset_path(state, district, block) + description
        roi_boundary = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )
    else:
        description = "catchment_area_raster_" + asset_suffix

        asset_id = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description
        )
        roi_boundary = ee.FeatureCollection(roi_path)
    catchment_area_pan_india_image = ee.Image(CATCHMENT_AREA)

    if not is_gee_asset_exists(raster_asset_id):
        try:
            task_id = export_raster_asset_to_gee(
                image=raster,
                description=description + "_raster",
                asset_id=raster_asset_id,
                scale=30,
                region=roi.geometry(),
            )
            stream_order_task_id_list = check_task_status([task_id])
            print("steam order task_id list", stream_order_task_id_list)

            """ Sync image to google cloud storage and then to geoserver"""
            image = ee.Image(raster_asset_id)
            task_id = sync_raster_to_gcs(image, 30, description + "_raster")

            task_id_list = check_task_status([task_id])
            print("task_id_list sync to gcs ", task_id_list)

            save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=description + "_raster",
                asset_id=raster_asset_id,
                dataset_name="Stream Order",
            )
            make_asset_public(raster_asset_id)
            res = sync_raster_gcs_to_geoserver(
                "stream_order",
                description + "_raster",
                description + "_raster",
                "stream_order",
            )
        except Exception as e:
            print(f"Error occurred in running stream order: {e}")
    return False

    clipped_image = catchment_area_pan_india_image.clip(roi_boundary)
    task = ee.batch.Export.image.toAsset(
        image=clipped_image,
        description=description,
        assetId=asset_id,
        region=roi_boundary.geometry(),
    )
    task.start()
    check_task_status([task])
