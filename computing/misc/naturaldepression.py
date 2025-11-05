import ee
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_dir_path,
    get_gee_asset_path,
    check_task_status,
)
from constants.pan_india_path import NATURAL_DEPRESSION
from nrm_app.celery import app
from celery import shared_task


@shared_task(bind=True)
def generate_natural_dp_for_block(
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
            "natural_depression_raster_"
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
        description = "natural_depression_raster_" + asset_suffix

        asset_id = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description
        )
        roi_boundary = ee.FeatureCollection(roi_path)
    catchment_area_pan_india_image = ee.Image(NATURAL_DEPRESSION)
    clipped_image = catchment_area_pan_india_image.clip(roi_boundary)
    task = ee.batch.Export.image.toAsset(
        image=clipped_image,
        description=description,
        assetId=asset_id,
        region=roi_boundary.geometry(),
    )
    task.start()
    check_task_status([task])
