import ee

from constants.pan_india_path import PAN_INDIA_SO
from nrm_app.celery import app
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from projects.models import Project
from utilities.constants import GEE_PATHS
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
    get_gee_dir_path,
)


@app.task(bind=True)
def generate_stream_order(
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
            "stream_order_" + valid_gee_text(district) + "_" + valid_gee_text(block)
        )

        roi = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )
        asset_id_raster = (
            get_gee_asset_path(state, district, block) + description + "_raster"
        )
    else:
        proj_obj = Project.objects.get(pk=proj_id)
        description = (
            "stream_order_"
            + valid_gee_text(proj_obj.name)
            + "_"
            + valid_gee_text(str(proj_id))
        )

        roi = ee.FeatureCollection(roi_path)
        asset_id_raster = (
            get_gee_dir_path(
                [proj_obj.name], asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description
            + "_raster"
        )

    stream_order_raster = ee.Image(PAN_INDIA_SO)
    raster = stream_order_raster.clip(roi.geometry())

    # Generate raster Layer
    stream_order_raster_generation(
        raster,
        state,
        district,
        block,
        description=description,
        roi=roi,
        raster_asset_id=asset_id_raster,
    )


def stream_order_raster_generation(
    raster,
    state,
    district,
    block,
    description=None,
    roi=None,
    raster_asset_id=None,
    proj_id=None,
):

    if not is_gee_asset_exists(raster_asset_id):
        task_id = export_raster_asset_to_gee(
            image=raster,
            description=description + "_raster",
            asset_id=raster_asset_id,
            scale=30,
            region=roi.geometry(),
        )
        stream_order_task_id_list = check_task_status([task_id])
        print("steam order task_id list", stream_order_task_id_list)

    layer_id = None
    layer_at_geoserver = False
    if is_gee_asset_exists(raster_asset_id):
        """Sync image to google cloud storage and then to geoserver"""
        image = ee.Image(raster_asset_id)
        task_id = sync_raster_to_gcs(image, 30, description + "_raster")

        task_id_list = check_task_status([task_id])
        print("task_id_list sync to gcs ", task_id_list)
        if state and district and block:
            layer_id = save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=description + "_raster",
                asset_id=raster_asset_id,
                dataset_name="Stream Order",
            )
            workspace_name = "stream_order"

        else:
            proj_obj = Project.objects.get(pk=proj_id)
            workspace_name = proj_obj.app_type
        make_asset_public(raster_asset_id)
        res = sync_raster_gcs_to_geoserver(
            workspace_name,
            description + "_raster",
            description + "_raster",
            "stream_order",
        )
        if res and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
            layer_at_geoserver = True
    return layer_at_geoserver
