import datetime
from datetime import timedelta

from dateutil.relativedelta import relativedelta

from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
)
from computing.lulc.utils.built_up import *
from computing.lulc.utils.cropland import *
from computing.lulc.cropping_frequency import *
from computing.lulc.utils.water_body import *
from computing.lulc.misc import *
from nrm_app.celery import app


@app.task(bind=True)
def generate_lulc_v2_tehsil(
    self, state_name, district_name, tehsil_name, start_year, end_year, gee_account_id
):
    ee_initialize(gee_account_id)
    print("Inside generate lulc v2")

    start_date, end_date = str(start_year) + "-07-01", str(end_year) + "-6-30"

    roi_boundary_geom = ee.FeatureCollection(
        get_gee_asset_path(state_name, district_name, tehsil_name)
        + "filtered_mws_"
        + valid_gee_text(district_name.lower())
        + "_"
        + valid_gee_text(tehsil_name.lower())
        + "_uid"
    )

    filename_prefix = (
        valid_gee_text(district_name.lower())
        + "_"
        + valid_gee_text(tehsil_name.lower())
    )

    loop_start = start_date
    loop_end = end_date

    print(loop_start, loop_end)

    task_ids = []

    while loop_start < loop_end:

        print("Inside loop", loop_start, loop_end)

        curr_start_date = datetime.strptime(loop_start, "%Y-%m-%d")
        curr_end_date = curr_start_date + relativedelta(years=1) - timedelta(days=1)

        loop_start = (curr_start_date + relativedelta(years=1)).strftime("%Y-%m-%d")

        curr_start_date = curr_start_date.strftime("%Y-%m-%d")
        curr_end_date = curr_end_date.strftime("%Y-%m-%d")

        print(
            "\n EXECUTING L3 LULC PREDICTION FOR ",
            curr_start_date,
            " TO ",
            curr_end_date,
            "\n",
        )

        curr_filename = filename_prefix + "_" + curr_start_date + "_" + curr_end_date

        if datetime.strptime(curr_start_date, "%Y-%m-%d").year < 2017:
            # run all previous code on '2017-07-01' to '2018-06-30'
            bu_image = get_builtup_prediction(
                roi_boundary_geom, "2017-07-01", "2018-06-30"
            )
            water_image = get_water_prediction(
                roi_boundary_geom, "2017-07-01", "2018-06-30"
            )
            combined_water_builtup_img = bu_image.where(
                bu_image.select("predicted_label").eq(0), water_image
            )
            bare_image = get_barrenland_prediction(
                roi_boundary_geom, "2017-07-01", "2018-06-30"
            )
            combined_water_builtup_barren_img = combined_water_builtup_img.where(
                combined_water_builtup_img.select("predicted_label").eq(0), bare_image
            )
            cropland_image = get_cropland_prediction(
                "2017-07-01", "2018-06-30", roi_boundary_geom
            )
            print("cropland done if")
            combined_img = combined_water_builtup_barren_img.where(
                combined_water_builtup_barren_img.select("predicted_label").eq(0),
                cropland_image,
            )

        else:
            # run all previous code on currStartDate and currEndDate
            bu_image = get_builtup_prediction(
                roi_boundary_geom, curr_start_date, curr_end_date
            )
            water_image = get_water_prediction(
                roi_boundary_geom, curr_start_date, curr_end_date
            )
            combined_water_builtup_img = bu_image.where(
                bu_image.select("predicted_label").eq(0), water_image
            )
            bare_image = get_barrenland_prediction(
                roi_boundary_geom, curr_start_date, curr_end_date
            )
            combined_water_builtup_barren_img = combined_water_builtup_img.where(
                combined_water_builtup_img.select("predicted_label").eq(0), bare_image
            )
            cropland_image = get_cropland_prediction(
                curr_start_date, curr_end_date, roi_boundary_geom
            )
            print("cropland done")
            combined_img = combined_water_builtup_barren_img.where(
                combined_water_builtup_barren_img.select("predicted_label").eq(0),
                cropland_image,
            )
        print("combined img created")

        # cropping intensity code runs always on currStartDate and currEndDate
        cropping_frequency_img = get_cropping_frequency(
            roi_boundary_geom, curr_start_date, curr_end_date
        )
        print("cropping freq")
        final_lulc_img = combined_img.where(
            combined_img.select("predicted_label").eq(5), cropping_frequency_img
        )

        if datetime.strptime(curr_start_date, "%Y-%m-%d").year < 2017:
            final_lulc_img = dw_based_shrub_cleaning(
                roi_boundary_geom, final_lulc_img, "2017-07-01", "2018-06-30"
            )
        else:
            final_lulc_img = dw_based_shrub_cleaning(
                roi_boundary_geom, final_lulc_img, curr_start_date, curr_end_date
            )

        scale = 10
        final_output_filename = curr_filename + "_LULCmap_" + str(scale) + "m_v2"
        final_output_assetid = (
            get_gee_asset_path(state_name, district_name, tehsil_name)
            + final_output_filename
        )

        # Setup the task
        image_export_task = ee.batch.Export.image.toAsset(
            image=remap_values(final_lulc_img.clip(roi_boundary_geom.geometry())),
            description=final_output_filename,
            assetId=final_output_assetid,
            pyramidingPolicy={"predicted_label": "mode"},
            scale=scale,
            maxPixels=1e13,
            region=roi_boundary_geom.geometry(),
            crs="EPSG:4326",
        )

        image_export_task.start()
