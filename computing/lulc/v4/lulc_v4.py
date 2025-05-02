import ee
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from computing.lulc.built_up import get_builtup_prediction
from computing.lulc.cropland import get_cropland_prediction
from computing.lulc.misc import get_barrenland_prediction
from computing.lulc.v4.classify_raster import classify_raster
from computing.lulc.v4.cropping_frequency_detection import get_cropping_frequency
from computing.lulc.v4.time_series import time_series
from computing.lulc.water_body import get_water_prediction
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    get_gee_asset_path,
    valid_gee_text,
    is_gee_asset_exists,
)
from nrm_app.celery import app


@app.task(bind=True)
def generate_lulc_v4(self, state, district, block, start_year, end_year):
    ee_initialize()

    task_id = time_series(state, district, block, start_year, end_year)
    if task_id:
        check_task_status([task_id], 500)
    print("Time Series task completed.")

    task_id = classify_raster(state, district, block)
    if task_id:
        check_task_status([task_id], 1000)
    print("Classify raster task completed.")

    generate_final(state, district, block, start_year, end_year)


def generate_final(state, district, block, start_year, end_year):
    filename_prefix = (
        f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
    )

    asset_id = get_gee_asset_path(state, district, block) + filename_prefix + "_v4"

    if is_gee_asset_exists(asset_id):
        return

    roi_boundary = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    all = ee.FeatureCollection(
        get_gee_asset_path(state, district, block) + filename_prefix + "_boundaries"
    )

    farm = all.filter(ee.Filter.eq("class", "farm"))
    scrubland = all.filter(ee.Filter.eq("class", "scrubland"))
    plantation = all.filter(ee.Filter.eq("class", "plantation"))

    start_date = f"{start_year}-07-01"
    end_date = f"{end_year}-07-01"

    # L1_asset_new = []
    # final_output_filename_array_new = []
    # final_output_assetid_array_new = []
    # crop_freq_array = []
    #
    # scale = 10

    loop_start = start_date
    loop_end = (datetime.strptime(end_date, "%Y-%m-%d")).strftime("%Y-%m-%d")

    while loop_start != loop_end:
        curr_start_date = datetime.strptime(loop_start, "%Y-%m-%d")
        curr_end_date = curr_start_date + relativedelta(years=1) - timedelta(days=1)

        loop_start = (curr_start_date + relativedelta(years=1)).strftime("%Y-%m-%d")

        curr_start_date = curr_start_date.strftime("%Y-%m-%d")
        curr_end_date = curr_end_date.strftime("%Y-%m-%d")

        print(
            "\n EXECUTING LULC PREDICTION FOR ",
            curr_start_date,
            " TO ",
            curr_end_date,
            "\n",
        )

        # curr_filename = filename_prefix + "_" + curr_start_date + "_" + curr_end_date

        if datetime.strptime(curr_start_date, "%Y-%m-%d").year < 2017:
            print(
                "To generate LULC output of year ",
                datetime.strptime(curr_start_date, "%Y-%m-%d").year,
                " , go to cell-LULC execution for years before 2017",
            )
            continue

        # LULC prediction code
        bu_image = get_builtup_prediction(roi_boundary, curr_start_date, curr_end_date)
        water_image = get_water_prediction(roi_boundary, curr_start_date, curr_end_date)
        combined_water_builtup_img = bu_image.where(
            bu_image.select("predicted_label").eq(0), water_image
        )
        bare_image = get_barrenland_prediction(
            roi_boundary, curr_start_date, curr_end_date
        )
        combined_water_builtup_barren_img = combined_water_builtup_img.where(
            combined_water_builtup_img.select("predicted_label").eq(0), bare_image
        )

        farm_image = ee.Image(0).rename("predicted_label")
        farm_mask = farm_image.clip(farm).mask()
        farm_image = farm_image.where(farm_mask, 5)

        scrubland_image = ee.Image(0).rename("predicted_label")
        scrubland_mask = scrubland_image.clip(scrubland).mask()
        scrubland_image = scrubland_image.where(scrubland_mask, 12)

        plantation_image = ee.Image(0).rename("predicted_label")
        plantation_mask = plantation_image.clip(plantation).mask()
        plantation_image = plantation_image.where(plantation_mask, 13)

        combined_img = combined_water_builtup_barren_img.where(
            combined_water_builtup_barren_img.select("predicted_label").eq(0),
            farm_image,
        )
        combined_img = combined_img.where(
            combined_img.select("predicted_label").eq(0), scrubland_image
        )
        combined_img = combined_img.where(
            combined_img.select("predicted_label").eq(0), plantation_image
        )

        cropland_image = get_cropland_prediction(
            curr_start_date, curr_end_date, roi_boundary
        )
        tree_image = cropland_image.where(
            cropland_image.select("predicted_label").eq(5), 12
        )
        combined_img = combined_img.where(
            combined_img.select("predicted_label").eq(12), tree_image
        )

        cropping_frequency_img = get_cropping_frequency(
            roi_boundary, curr_start_date, curr_end_date
        )
        final_lulc_img = combined_img.addBands(
            ee.Image.constant(-1).rename(["predicted_cluster"])
        ).where(combined_img.select("predicted_label").eq(5), cropping_frequency_img)

        # final_output_filename = curr_filename + "_LULCmap_" + str(scale) + "m"
        # final_output_assetid = (
        #     "projects/ee-indiasat/assets/LULC_Version2_Outputs_NewHierarchy/"
        #     + final_output_filename
        # )
        #
        # final_output_filename_array_new.append(final_output_filename)
        # final_output_assetid_array_new.append(final_output_assetid)
        # L1_asset_new.append(final_lulc_img)
        # crop_freq_array.append(cropping_frequency_img)

        task = ee.batch.Export.image.toAsset(
            image=final_lulc_img,  # .select("predicted_label"),
            description="lulc_" + filename_prefix + "_v4",
            assetId=asset_id,
            pyramidingPolicy={"predicted_label": "mode"},
            scale=10,
            maxPixels=1e13,
            crs="EPSG:4326",
        )
        task.start()
