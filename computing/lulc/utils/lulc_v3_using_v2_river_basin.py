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
from computing.lulc.cropping_frequency import *
from computing.lulc.misc import clip_lulc_from_river_basin


@app.task(bind=True)
def lulc_river_basin(self, state_name, district_name, block_name, start_year, end_year):
    ee_initialize()
    print("Inside lulc_river_basin")
    roi_boundary = ee.FeatureCollection(
        get_gee_asset_path(state_name, district_name, block_name)
        + "filtered_mws_"
        + valid_gee_text(district_name.lower())
        + "_"
        + valid_gee_text(block_name.lower())
        + "_uid"
    )

    start_date, end_date = str(start_year) + "-07-01", str(end_year) + "-6-30"

    filename_prefix = (
        valid_gee_text(district_name.lower()) + "_" + valid_gee_text(block_name.lower())
    )

    loop_start = start_date
    loop_end = end_date
    l1_asset_new = []
    final_output_filename_array_new = []
    final_output_assetid_array_new = []
    crop_freq_array = []

    scale = 10
    print(loop_start, loop_end)

    while loop_start < loop_end:
        curr_start_date = datetime.strptime(loop_start, "%Y-%m-%d")
        curr_end_date = curr_start_date + relativedelta(years=1) - timedelta(days=1)

        loop_start = (curr_start_date + relativedelta(years=1)).strftime("%Y-%m-%d")

        curr_start_date = curr_start_date.strftime("%Y-%m-%d")
        curr_end_date = curr_end_date.strftime("%Y-%m-%d")
        curr_filename = filename_prefix + "_" + curr_start_date + "_" + curr_end_date
        cropping_frequency_img = get_cropping_frequency(
            roi_boundary, curr_start_date, curr_end_date
        )
        final_output_filename = curr_filename + "_LULCmap_" + str(scale) + "m"
        final_output_assetid = (
            get_gee_asset_path(state_name, district_name, block_name)
            + final_output_filename
        )
        final_output_filename_array_new.append(final_output_filename)
        final_output_assetid_array_new.append(final_output_assetid)
        crop_freq_array.append(cropping_frequency_img)
        river_basin = ee.FeatureCollection("projects/ee-ankit-mcs/assets/CGWB_basin")
        l1_asset_new.append(
            clip_lulc_from_river_basin(
                river_basin,
                roi_boundary,
                scale,
                curr_start_date,
                curr_end_date,
                version="v2",
            )
        )

    """
        temporal correction for background pixels
    """
    length = len(l1_asset_new)
    # intermediate years
    for i in range(1, length - 1):

        before = l1_asset_new[i - 1]
        middle = l1_asset_new[i]
        after = l1_asset_new[i + 1]

        # any-bg-any
        cond1 = (before.gte(1)).And(after.gte(1)).And(middle.eq(0))

        if i == 1:
            middle = middle.where(cond1, after)
        elif i == 3:
            middle = middle.where(cond1, before)
        else:
            middle = middle.where(cond1, before)

        l1_asset_new[i] = middle

    # first year
    cond1 = l1_asset_new[0].eq(0).And(l1_asset_new[1].gte(1))
    l1_asset_new[0] = l1_asset_new[0].where(cond1, l1_asset_new[1])

    # last year
    cond2 = l1_asset_new[length - 1].eq(0).And(l1_asset_new[length - 2].gte(1))
    l1_asset_new[length - 1] = l1_asset_new[length - 1].where(
        cond2, l1_asset_new[length - 2]
    )

    """
        temporal correction for Intermediate Years
    """
    zero_image = ee.Image.constant(0).clip(l1_asset_new[0].geometry())

    # Calculating condition count for each pixel
    for i in range(1, length - 1):
        before = l1_asset_new[i - 1]
        middle = l1_asset_new[i]
        after = l1_asset_new[i + 1]

        # shrubs-green-shrubs
        cond1 = (
            before.eq(12)
            .And(after.eq(12))
            .And(
                middle.eq(6)
                .Or(middle.eq(8))
                .Or(middle.eq(9))
                .Or(middle.eq(10))
                .Or(middle.eq(11))
            )
        )

        # water-green-water
        cond2 = (
            before.eq(2)
            .Or(before.eq(3))
            .Or(before.eq(4))
            .And(after.eq(2).Or(after.eq(3)).Or(after.eq(4)))
            .And(
                middle.eq(6)
                .Or(middle.eq(8))
                .Or(middle.eq(9))
                .Or(middle.eq(10))
                .Or(middle.eq(11))
            )
        )

        # tree-shrub-tree
        cond3 = before.eq(6).And(after.eq(6)).And(middle.eq(12))

        # crop-shrub-crop
        cond4 = (
            before.eq(8)
            .Or(before.eq(9))
            .Or(before.eq(10))
            .Or(before.eq(11))
            .And(after.eq(8).Or(after.eq(9)).Or(after.eq(10)).Or(after.eq(11)))
            .And(middle.eq(12))
        )

        # crop-barren-crop
        cond5 = (
            before.eq(8)
            .Or(before.eq(9))
            .Or(before.eq(10))
            .Or(before.eq(11))
            .And(after.eq(8).Or(after.eq(9)).Or(after.eq(10)).Or(after.eq(11)))
            .And(middle.eq(7))
        )

        # tree-farm-tree
        cond6 = (
            before.eq(6)
            .And(after.eq(6))
            .And(middle.eq(8).Or(middle.eq(9)).Or(middle.eq(10)).Or(middle.eq(11)))
        )

        # farm-tree-farm
        cond7 = (
            before.eq(8)
            .Or(before.eq(9))
            .Or(before.eq(10))
            .Or(before.eq(11))
            .And(after.eq(8).Or(after.eq(9)).Or(after.eq(10)).Or(after.eq(11)))
            .And(middle.eq(6))
        )

        # BU-tree-BU
        cond8 = before.eq(1).And(after.eq(1)).And(middle.eq(6))

        # tree-BU-tree
        cond9 = before.eq(6).And(after.eq(6)).And(middle.eq(1))

        # BU-farm-BU
        cond10 = (
            before.eq(1)
            .And(after.eq(1))
            .And(middle.eq(8).Or(middle.eq(9)).Or(middle.eq(10)).Or(middle.eq(11)))
        )

        # barren-green-barren
        cond11 = (
            before.eq(7)
            .And(after.eq(7))
            .And(
                middle.eq(6)
                .Or(middle.eq(8))
                .Or(middle.eq(9))
                .Or(middle.eq(10))
                .Or(middle.eq(11))
            )
        )

        zero_image = (
            zero_image.add(cond1)
            .add(cond2)
            .add(cond3)
            .add(cond4)
            .add(cond5)
            .add(cond6)
            .add(cond7)
            .add(cond8)
            .add(cond9)
            .add(cond10)
            .add(cond11)
        )

    def process_conditions(
        before, middle, after, zero_image, th1, th2, i, length, L1_asset_new
    ):

        cond1 = (
            zero_image.gte(th1)
            .And(zero_image.lte(th2))
            .And(before.eq(12))
            .And(after.eq(12))
            .And(
                middle.eq(6)
                .Or(middle.eq(8))
                .Or(middle.eq(9))
                .Or(middle.eq(10))
                .Or(middle.eq(11))
            )
        )

        cond2 = (
            zero_image.gte(th1)
            .And(zero_image.lte(th2))
            .And(before.eq(2).Or(before.eq(3)).Or(before.eq(4)))
            .And(after.eq(2).Or(after.eq(3)).Or(after.eq(4)))
            .And(
                middle.eq(6)
                .Or(middle.eq(8))
                .Or(middle.eq(9))
                .Or(middle.eq(10))
                .Or(middle.eq(11))
            )
        )

        cond3 = (
            zero_image.gte(th1)
            .And(zero_image.lte(th2))
            .And(before.eq(6))
            .And(after.eq(6))
            .And(middle.eq(12))
        )

        cond4 = (
            zero_image.gte(th1)
            .And(zero_image.lte(th2))
            .And(before.eq(8).Or(before.eq(9)).Or(before.eq(10)).Or(before.eq(11)))
            .And(after.eq(8).Or(after.eq(9)).Or(after.eq(10)).Or(after.eq(11)))
            .And(middle.eq(12))
        )

        cond5 = (
            zero_image.gte(th1)
            .And(zero_image.lte(th2))
            .And(before.eq(8).Or(before.eq(9)).Or(before.eq(10)).Or(before.eq(11)))
            .And(after.eq(8).Or(after.eq(9)).Or(after.eq(10)).Or(after.eq(11)))
            .And(middle.eq(7))
        )

        cond6 = (
            zero_image.gte(th1)
            .And(zero_image.lte(th2))
            .And(before.eq(6))
            .And(after.eq(6))
            .And(middle.eq(8).Or(middle.eq(9)).Or(middle.eq(10)).Or(middle.eq(11)))
        )

        cond7 = (
            zero_image.gte(th1)
            .And(zero_image.lte(th2))
            .And(before.eq(8).Or(before.eq(9)).Or(before.eq(10)).Or(before.eq(11)))
            .And(after.eq(8).Or(after.eq(9)).Or(after.eq(10)).Or(after.eq(11)))
            .And(middle.eq(6))
        )

        cond8 = (
            zero_image.gte(th1)
            .And(zero_image.lte(th2))
            .And(before.eq(1))
            .And(after.eq(1))
            .And(middle.eq(6))
        )

        cond9 = (
            zero_image.gte(th1)
            .And(zero_image.lte(th2))
            .And(before.eq(6))
            .And(after.eq(6))
            .And(middle.eq(1))
        )

        cond10 = (
            zero_image.gte(th1)
            .And(zero_image.lte(th2))
            .And(before.eq(1))
            .And(after.eq(1))
            .And(middle.eq(8).Or(middle.eq(9)).Or(middle.eq(10)).Or(middle.eq(11)))
        )

        cond11 = (
            zero_image.gte(th1)
            .And(zero_image.lte(th2))
            .And(before.eq(7))
            .And(after.eq(7))
            .And(
                middle.eq(6)
                .Or(middle.eq(8))
                .Or(middle.eq(9))
                .Or(middle.eq(10))
                .Or(middle.eq(11))
            )
        )

        if i != 2:
            middle = middle.where(cond1, 12)
            middle = middle.where(cond2, 7)
            middle = middle.where(cond3, 6)
            middle = middle.where(cond6, 6)

            cropping_frequency_img = crop_freq_array[i]
            middle = middle.where(cond7, cropping_frequency_img)
            middle = middle.where(cond4, cropping_frequency_img)
            middle = middle.where(cond5, cropping_frequency_img)

            middle = middle.where(cond9, 6)

            before = before.where(cond10, crop_freq_array[i - 1])
            middle = middle.where(cond10, crop_freq_array[i])
            after = after.where(cond10, crop_freq_array[i + 1])

            middle = middle.where(cond11, 7)

        if i != 1 and i != length - 2:
            cond8 = cond8.And(L1_asset_new[i - 2].eq(1).And(L1_asset_new[i + 2].eq(1)))
            middle = middle.where(cond8, 1)

        return {"before": before, "middle": middle, "after": after}

    # applying condition for each pixel with count 1
    for i in range(1, length - 1):
        before = l1_asset_new[i - 1]
        middle = l1_asset_new[i]
        after = l1_asset_new[i + 1]

        updated_images = process_conditions(
            before, middle, after, zero_image, 1, 1, i, length, l1_asset_new
        )

        l1_asset_new[i - 1] = updated_images["before"]
        l1_asset_new[i] = updated_images["middle"]
        l1_asset_new[i + 1] = updated_images["after"]

    # applying condition for each pixel with count 2
    # first iterate on 2019 windows and do the conditions
    for i in range(1, length - 2):
        before = l1_asset_new[i - 1]
        middle = l1_asset_new[i]
        after = l1_asset_new[i + 1]

        updated_images = process_conditions(
            before, middle, after, zero_image, 2, length - 4, i, length, l1_asset_new
        )

        l1_asset_new[i - 1] = updated_images["before"]
        l1_asset_new[i] = updated_images["middle"]
        l1_asset_new[i + 1] = updated_images["after"]

    # iterate on whole again to do if any remaining conditions
    for i in range(1, length - 1):
        before = l1_asset_new[i - 1]
        middle = l1_asset_new[i]
        after = l1_asset_new[i + 1]

        updated_images = process_conditions(
            before, middle, after, zero_image, 2, length - 4, i, length, l1_asset_new
        )

        l1_asset_new[i - 1] = updated_images["before"]
        l1_asset_new[i] = updated_images["middle"]
        l1_asset_new[i + 1] = updated_images["after"]

    """
        temporal correction for first year 2017
    """
    first_year_image = l1_asset_new[0]

    # BU-farm-farm
    cond4 = (
        l1_asset_new[0]
        .eq(1)
        .And(
            l1_asset_new[1]
            .eq(8)
            .Or(l1_asset_new[1].eq(9))
            .Or(l1_asset_new[1].eq(10))
            .Or(l1_asset_new[1].eq(11))
        )
        .And(
            l1_asset_new[2]
            .eq(8)
            .Or(l1_asset_new[2].eq(9))
            .Or(l1_asset_new[2].eq(10))
            .Or(l1_asset_new[2].eq(11))
        )
    )
    # BU-tree-tree
    cond5 = l1_asset_new[0].eq(1).And(l1_asset_new[1].eq(6)).And(l1_asset_new[2].eq(6))

    first_year_image = first_year_image.where(cond5, 6)

    cropping_frequency_img = crop_freq_array[0]
    first_year_image = first_year_image.where(cond4, cropping_frequency_img)

    l1_asset_new[0] = first_year_image
    task_list = []
    geometry = roi_boundary.geometry()
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

    sync_lulc_to_geoserver(final_output_filename_array_new, l1_asset_new, block_name)


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
