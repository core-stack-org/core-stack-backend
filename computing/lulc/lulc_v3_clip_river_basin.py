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
)
from nrm_app.celery import app
from .cropping_frequency import *
from .misc import clip_lulc_from_river_basin


@app.task(bind=True)
def lulc_river_basin(self, state, district, block, start_year, end_year):
    ee_initialize()
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
        river_basin = ee.FeatureCollection("projects/ee-ankit-mcs/assets/CGWB_basin")
        l1_asset_new.append(
            clip_lulc_from_river_basin(
                river_basin, roi, scale, curr_start_date, curr_end_date
            )
        )

    task_list = []
    geometry = roi.geometry()
    for i in range(0, len(l1_asset_new)):
        image_export_task = ee.batch.Export.image.toAsset(
            image=l1_asset_new[i].clip(geometry),
            description=final_output_filename_array_new[i],
            assetId=final_output_assetid_array_new[i],
            pyramidingPolicy={"predicted_label": "mode"},
            scale=scale,
            maxPixels=1e13,
            crs="EPSG:4326",
        )
        image_export_task.start()
        print("Successfully started the LULC v3", image_export_task.status())
        task_list.append(image_export_task.status()["id"])

    task_id_list = check_task_status(task_list)
    print("LULC task_id_list", task_id_list)

    sync_lulc_to_gcs(
        final_output_filename_array_new,
        final_output_assetid_array_new,
        scale,
    )

    sync_lulc_to_geoserver(final_output_filename_array_new, l1_asset_new, block)


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


def sync_lulc_to_geoserver(final_output_filename_array_new, l1_asset_new, block_name):
    print("Syncing lulc to geoserver")
    lulc_workspaces = ["LULC_level_1", "LULC_level_2", "LULC_level_3"]
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
            sync_raster_gcs_to_geoserver(workspace, gcs_file_name, layer_name, style)
