import datetime
from datetime import timedelta
import ee
from dateutil.relativedelta import relativedelta

from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    make_asset_public,
    export_raster_asset_to_gee,
    is_gee_asset_exists,
)
from nrm_app.celery import app
from .cropping_frequency import *
from .misc import clip_lulc_from_river_basin
from computing.utils import save_layer_info_to_db, update_layer_sync_status

from computing.STAC_specs import generate_STAC_layerwise

@app.task(bind=True)
def clip_lulc_v3(self, state, district, block, start_year, end_year, gee_account_id):
    ee_initialize(gee_account_id)
    print("Inside lulc_river_basin")
    roi = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    ).union()

    start_date, end_date = str(start_year) + "-07-01", str(end_year) + "-6-30"

    filename_prefix = (
        valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    )

    loop_start = start_date
    loop_end = end_date
    l1_asset_new = []
    final_output_filename_array_new = []
    final_output_assetid_array_new = []

    scale = 10
    print(loop_start, loop_end)

    while loop_start < loop_end:
        curr_start_date = datetime.strptime(loop_start, "%Y-%m-%d")
        curr_end_date = curr_start_date + relativedelta(years=1) - timedelta(days=1)

        loop_start = (curr_start_date + relativedelta(years=1)).strftime("%Y-%m-%d")

        curr_start_date = curr_start_date.strftime("%Y-%m-%d")
        curr_end_date = curr_end_date.strftime("%Y-%m-%d")
        curr_filename = filename_prefix + "_" + curr_start_date + "_" + curr_end_date

        final_output_filename = curr_filename + "_LULCmap_" + str(scale) + "m"
        final_output_assetid = (
            get_gee_asset_path(state, district, block) + final_output_filename
        )
        final_output_filename_array_new.append(final_output_filename)
        final_output_assetid_array_new.append(final_output_assetid)
        river_basin = ee.FeatureCollection(
            "projects/corestack-datasets/assets/datasets/CGWB_basin"
        )
        l1_asset_new.append(
            clip_lulc_from_river_basin(
                river_basin, roi, scale, curr_start_date, curr_end_date
            )
        )

    task_list = []
    geometry = roi.geometry()
    for i in range(0, len(l1_asset_new)):
        if not is_gee_asset_exists(final_output_assetid_array_new[i]):
            task_id = export_raster_asset_to_gee(
                image=l1_asset_new[i].clip(geometry),
                description=final_output_filename_array_new[i],
                asset_id=final_output_assetid_array_new[i],
                scale=scale,
                region=geometry,
                pyramiding_policy={"predicted_label": "mode"},
            )
            task_list.append(task_id)

    task_id_list = check_task_status(task_list)
    print("LULC task_id_list", task_id_list)

    layer_ids = []
    lulc_workspaces = ["LULC_level_1", "LULC_level_2", "LULC_level_3"]
    for i in range(0, len(l1_asset_new)):
        name_arr = final_output_filename_array_new[i].split("_20")
        s_year = name_arr[1][:2]
        e_year = name_arr[2][:2]
        for workspace in lulc_workspaces:
            suff = workspace.replace("LULC", "")
            layer_name = (
                "LULC_"
                + s_year
                + "_"
                + e_year
                + "_"
                + valid_gee_text(block.lower())
                + suff
            )
            if is_gee_asset_exists(final_output_assetid_array_new[i]):
                layer_id = save_layer_info_to_db(
                    state,
                    district,
                    block,
                    layer_name=layer_name,
                    asset_id=final_output_assetid_array_new[i],
                    dataset_name=workspace,
                    misc={
                        "start_year": start_year,
                        "end_year": end_year,
                    },
                )
                layer_ids.append(layer_id)
                print("saved info to db at the gee level...")
                make_asset_public(final_output_assetid_array_new[i])

    sync_lulc_to_gcs(
        final_output_filename_array_new,
        final_output_assetid_array_new,
        scale,
    )

    layer_at_geoserver = sync_lulc_to_geoserver(
        final_output_filename_array_new,
        l1_asset_new,
        state,
        district,
        block,
        layer_ids,
    )

    return layer_at_geoserver


def sync_lulc_to_gcs(
    final_output_filename_array_new, final_output_assetid_array_new, scale
):
    task_ids = []
    for i in range(0, len(final_output_assetid_array_new)):
        make_asset_public(final_output_assetid_array_new[i])
        image = ee.Image(final_output_assetid_array_new[i])
        name_arr = final_output_filename_array_new[i].split("_20")
        s_year = name_arr[1][:2]
        e_year = name_arr[2][:2]
        layer_name = "LULC_" + s_year + "_" + e_year + "_" + name_arr[0]
        task_ids.append(sync_raster_to_gcs(image, scale, layer_name))

    task_id_list = check_task_status(task_ids)
    print("task_ids sync to gcs ", task_id_list)


def sync_lulc_to_geoserver(
    final_output_filename_array_new,
    l1_asset_new,
    state_name,
    district_name,
    block_name,
    layer_ids,
):
    print("Syncing lulc to geoserver")
    lulc_workspaces = ["LULC_level_1", "LULC_level_2", "LULC_level_3"]
    layer_at_geoserver = False
    for i in range(0, len(l1_asset_new)):
        name_arr = final_output_filename_array_new[i].split("_20")
        s_year = name_arr[1][:2]
        e_year = name_arr[2][:2]
        gcs_file_name = "LULC_" + s_year + "_" + e_year + "_" + name_arr[0]
        print("Syncing " + gcs_file_name + " to geoserver")
        for workspace in lulc_workspaces:
            suff = workspace.replace("LULC", "")
            style = workspace.lower() + "_style"
            layer_name = (
                "LULC_"
                + s_year
                + "_"
                + e_year
                + "_"
                + valid_gee_text(block_name.lower())
                + suff
            )
            res = sync_raster_gcs_to_geoserver(
                workspace, gcs_file_name, layer_name, style
            )
            if res and layer_ids:
                update_layer_sync_status(layer_id=layer_ids[i], sync_to_geoserver=True)

                layer_STAC_generated = False
                layer_STAC_generated = generate_STAC_layerwise.generate_raster_stac(
                    state=state_name,
                    district=district_name,
                    block=block_name,
                    layer_name='land_use_land_cover_raster',
                    start_year=name_arr[1],
                    end_year=(name_arr[1]+1)
                    )
                update_layer_sync_status(layer_id=layer_ids[i],
                                         is_stac_specs_generated=layer_STAC_generated)

                print("geoserver flag is updated")
                layer_at_geoserver = True
    return layer_at_geoserver
