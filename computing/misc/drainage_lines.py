import ee

from computing.utils import (
    sync_layer_to_geoserver,
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    check_task_status,
    make_asset_public,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    get_gee_dir_path,
)
from utilities.constants import GEE_DATASET_PATH, GEE_PATHS
from nrm_app.celery import app


@app.task(bind=True)
def clip_drainage_lines(
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
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]
        description = f"drainage_lines_{asset_suffix}"

        asset_id = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description
        )
        roi = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )
    else:
        description = f"drainage_lines_{asset_suffix}"
        asset_id = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description
        )
        roi = ee.FeatureCollection(roi_path)
    pan_india_drainage = ee.FeatureCollection(
        GEE_DATASET_PATH + "/drainage-line/pan_india_drainage_lines"
    )

    clipped_drainage = pan_india_drainage.filterBounds(roi.geometry())

    task = export_vector_asset_to_gee(clipped_drainage, description, asset_id)

    task_id_list = check_task_status([task])
    print("task_id_list", task_id_list)

    layer_at_geoserver = False
    if is_gee_asset_exists(asset_id):
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
            asset_id=asset_id,
            dataset_name="Drainage",
        )

        make_asset_public(asset_id)

        try:
            # Load feature collection from Earth Engine
            fc = ee.FeatureCollection(asset_id)
            res = sync_fc_to_geoserver(
                fc,
                state,
                valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower()),
                "drainage",
            )
            print("Drainage line synced to geoserver:", res)
            if res["status_code"] == 201 and layer_id:
                update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
                print("sync to geoserver flag is updated")
                layer_at_geoserver = True

        except Exception as e:
            print("Exception in syncing Drainage line to geoserver", e)
    return layer_at_geoserver
