import ee
from nrm_app.celery import app
from computing.utils import (
    sync_layer_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    export_raster_asset_to_gee,
    make_asset_public,
)
from constants.pan_india_urls import CATCHMETN_AREA, NATURAL_DEPRESSION


@app.task(bind=True)
def generate_natural_depression(self, state, district, block, gee_account_id):
    ee_initialize(gee_account_id)
    description = (
        "natural_depression_" + valid_gee_text(district) + "_" + valid_gee_text(block)
    )

    roi = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    natural_depression = ee.Image(NATURAL_DEPRESSION)
    raster = natural_depression.clip(roi.geometry())

    # Generate raster Layer
    natural_depression_raster_generation(
        state, district, block, description, roi, raster
    )


def natural_depression_raster_generation(
    state, district, block, description, roi, raster
):
    raster_asset_id = (
        get_gee_asset_path(state, district, block) + description + "_raster"
    )
    if not is_gee_asset_exists(raster_asset_id):
        try:
            task_id = export_raster_asset_to_gee(
                image=raster,
                description=description + "_raster",
                asset_id=raster_asset_id,
                scale=30,
                region=roi.geometry(),
            )
            natural_depression_task_id_list = check_task_status([task_id])
            print("natural depression task_id list", natural_depression_task_id_list)

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
                dataset_name="Natural Depression",
            )
            make_asset_public(raster_asset_id)
            res = sync_raster_gcs_to_geoserver(
                "natural_depression",
                description + "_raster",
                description + "_raster",
                style_name="natural_depression",
            )
        except Exception as e:
            print(f"Error occurred in running stream order: {e}")
    return False
