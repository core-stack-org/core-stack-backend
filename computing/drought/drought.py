import ee
from computing.utils import (
    sync_layer_to_geoserver,
)
from utilities.constants import GEE_HELPER_PATH, GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_dir_path,
    is_gee_asset_exists,
    make_asset_public,
)
from .generate_layers import generate_drought_layers
from .merge_layers import (
    merge_drought_layers_chunks,
    merge_yearly_layers,
)
from nrm_app.celery import app
from celery import shared_task


@shared_task()
def calculate_drought(
    state=None,
    district=None,
    block=None,
    roi_path=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    start_year=None,
    end_year=None,
):
    ee_initialize()



    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

        roi = ee.FeatureCollection(
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )
    else:
        roi = ee.FeatureCollection(roi_path)
        dst_filename = (
            "drought_"
            + asset_suffix
            + "_"
            + str(start_year)
            + "_"
            + str(end_year)
        )
        asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + dst_filename
    )
        print(f"asset:{asset_id}")

    if not is_gee_asset_exists(asset_id):
        chunk_size = 30  # if shapefile is large, running the script on the complete file will result an error,
        # so divide into chunks and run on the chunks when the chunks are got exported,
        # then the next joining script join the chunks
        current_year = start_year
        merged_tasks = []
        yearly_assets = []
        while current_year <= end_year:
            print("current_year", current_year)
            yearly_drought = (
                get_gee_dir_path(
                    asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_HELPER_PATH"]
                )
                + "drought_"
                + asset_suffix
                + "_"
                + str(current_year)
            )
            yearly_assets.append(yearly_drought)
            if not is_gee_asset_exists(yearly_drought):
                print ("Started Generating drought layer")
                generate_drought_layers(
                    roi,
                    asset_suffix,
                    asset_folder_list,
                    app_type,
                    current_year,
                    start_year,
                    end_year,
                    chunk_size,
                )

                task_id = merge_drought_layers_chunks(
                    roi,
                    asset_suffix,
                    asset_folder_list,
                    app_type,
                    current_year,
                    chunk_size,
                )
                if task_id:
                    merged_tasks.append(task_id)
            current_year += 1

        merged_task_ids = check_task_status(merged_tasks)
        print("All years' asset generated, task id: ", merged_task_ids)

        for asset in yearly_assets:
            make_asset_public(asset)

        task_id = merge_yearly_layers(
            asset_suffix, asset_folder_list, app_type, start_year, end_year
        )
        check_task_status([task_id])
        make_asset_public(asset_id)

    fc = ee.FeatureCollection(asset_id)
    fc = fc.toList(fc.size()).getInfo()
    fc = {"features": fc, "type": "FeatureCollection"}
    res = sync_layer_to_geoserver(
        asset_suffix,
        fc,
        asset_suffix + "_drought",
        "cropping_drought",
    )

    print(res)
