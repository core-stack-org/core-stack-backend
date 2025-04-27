import ee
from datetime import datetime, timedelta
import pandas as pd
from dateutil.relativedelta import relativedelta
from computing.lulc.built_up import get_builtup_prediction
from computing.lulc.cropland import get_cropland_prediction
from computing.lulc.misc import get_barrenland_prediction
from computing.lulc.v4.classify_raster import classify_raster
from computing.lulc.v4.cropping_frequency_detection import get_cropping_frequency
from computing.lulc.v4.time_series import time_series
from computing.lulc.water_body import get_water_prediction
from utilities.gee_utils import ee_initialize
from nrm_app.celery import app


@app.task(bind=True)
def generate_lulc_v4(self, state, district, block, start_year, end_year):
    ee_initialize("helper")
    time_series(state, district, block, start_year, end_year)
    classify_raster(state, district, block, start_year, end_year)
    generate_final(state, district, block, start_year, end_year)


def generate_final(state, district, block, start_year, end_year):
    roi_boundary = ee.FeatureCollection("users/mtpictd/india_block_boundaries").filter(
        ee.Filter.eq("block", "Peddapally")
    )
    filename_prefix = "Area_Peddapally"

    suffix = filename_prefix.split("_")[-1]
    all = get_feature_collection(
        "projects/ee-corestack-helper/assets/apps/"
        + filename_prefix
        + "_boundaries_refined"
    )

    farm = all.filter(ee.Filter.eq("class", "farm"))
    scrubland = all.filter(ee.Filter.eq("class", "scrubland"))
    plantation = all.filter(ee.Filter.eq("class", "plantation"))

    startDate = "2023-07-01"
    endDate = "2024-07-01"

    L1_asset_new = []
    final_output_filename_array_new = []
    final_output_assetid_array_new = []
    crop_freq_array = []

    scale = 10

    loopStart = startDate
    loopEnd = (datetime.strptime(endDate, "%Y-%m-%d")).strftime("%Y-%m-%d")

    while loopStart != loopEnd:
        currStartDate = datetime.strptime(loopStart, "%Y-%m-%d")
        currEndDate = currStartDate + relativedelta(years=1) - timedelta(days=1)

        loopStart = (currStartDate + relativedelta(years=1)).strftime("%Y-%m-%d")

        currStartDate = currStartDate.strftime("%Y-%m-%d")
        currEndDate = currEndDate.strftime("%Y-%m-%d")

        print(
            "\n EXECUTING LULC PREDICTION FOR ",
            currStartDate,
            " TO ",
            currEndDate,
            "\n",
        )

        curr_filename = filename_prefix + "_" + currStartDate + "_" + currEndDate

        if datetime.strptime(currStartDate, "%Y-%m-%d").year < 2017:
            print(
                "To generate LULC output of year ",
                datetime.strptime(currStartDate, "%Y-%m-%d").year,
                " , go to cell-LULC execution for years before 2017",
            )
            continue

        # LULC prediction code
        bu_image = get_builtup_prediction(roi_boundary, currStartDate, currEndDate)
        water_image = get_water_prediction(roi_boundary, currStartDate, currEndDate)
        combined_water_builtup_img = bu_image.where(
            bu_image.select("predicted_label").eq(0), water_image
        )
        bare_image = get_barrenland_prediction(roi_boundary, currStartDate, currEndDate)
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
            currStartDate, currEndDate, roi_boundary
        )
        tree_image = cropland_image.where(
            cropland_image.select("predicted_label").eq(5), 12
        )
        combined_img = combined_img.where(
            combined_img.select("predicted_label").eq(12), tree_image
        )

        cropping_frequency_img = get_cropping_frequency(
            roi_boundary, currStartDate, currEndDate
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
            assetId="projects/ee-corestack-helper/assets/apps/"
            + filename_prefix
            + "_v4",
            pyramidingPolicy={"predicted_label": "mode"},
            scale=10,
            maxPixels=1e13,
            crs="EPSG:4326",
        )
        task.start()


def get_feature_collection(asset_id):
    """Check if an asset exists, and load it as a FeatureCollection if it does.
    Otherwise, return an empty FeatureCollection.

    Args:
        asset_id (str): The Earth Engine asset ID.

    Returns:
        ee.FeatureCollection: The loaded FeatureCollection or an empty one.
    """
    try:
        # Get asset information to check existence
        ee.data.getAsset(asset_id)
        print(f"Asset '{asset_id}' exists. Loading FeatureCollection.")
        return ee.FeatureCollection(asset_id)
    except Exception as e:
        print(f"Asset '{asset_id}' does not exist. Returning empty FeatureCollection.")
        return ee.FeatureCollection([])
