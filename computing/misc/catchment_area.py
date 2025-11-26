import ee
from nrm_app.celery import app
from computing.utils import (
    sync_layer_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from projects.models import Project
from utilities.constants import GEE_PATHS
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
    get_gee_dir_path,
)
from constants.pan_india_urls import CATCHMETN_AREA


@app.task(bind=True)
def generate_catchment_area_singleflow(
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
):
    ee_initialize(gee_account_id)
    if state and district and block:
        description = (
            "catchment_area"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_raster"
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
        roi_boundary = ee.FeatureCollection(roi_path)
        description = "catchment_area_" + asset_suffix

        asset_id = (
            get_gee_dir_path(
                asset_folder, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description
        )

    catchment_area_raster = ee.Image(CATCHMETN_AREA)
    raster = catchment_area_raster.clip(roi_boundary.geometry())

    # Generate raster Layer
    catchment_area_raster_generation(
        asset_id=asset_id,
        state=state,
        district=district,
        block=block,
        asset_suffix=description,
        roi=roi_boundary,
        raster=raster,
        proj_id=proj_id,
    )


def catchment_area_raster_generation(
    raster,
    roi,
    proj_id=None,
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    asset_folder=None,
    app_type=None,
    asset_id=None,
):
    workspacename = "catchment_area_singleflow"
    if proj_id:
        proj_obj = Project.objects.get(pk=proj_id)

    if not is_gee_asset_exists(asset_id):
        try:
            task_id = export_raster_asset_to_gee(
                image=raster,
                description=asset_suffix + "_raster",
                asset_id=asset_id,
                scale=30,
                region=roi.geometry(),
            )
            catchment_area_task_id_list = check_task_status([task_id])
            print("catchmenta area task_id list", catchment_area_task_id_list)

            """ Sync image to google cloud storage and then to geoserver"""
            image = ee.Image(asset_id)
            task_id = sync_raster_to_gcs(image, 30, asset_suffix + "_raster")

            task_id_list = check_task_status([task_id])
            print("task_id_list sync to gcs ", task_id_list)
            if state and district and block:
                save_layer_info_to_db(
                    state,
                    district,
                    block,
                    layer_name=asset_suffix + "_raster",
                    asset_id=asset_id,
                    dataset_name="Catchment Area",
                )
            make_asset_public(asset_id)
            res = sync_raster_gcs_to_geoserver(
                workspacename,
                asset_suffix + "_raster",
                asset_suffix + "_raster",
                "catchment_area_singleflow",
            )
        except Exception as e:
            print(f"Error occurred in running stream order: {e}")
    return False
