import datetime
import ee
from computing.utils import (
    sync_fc_to_geoserver, sync_project_fc_to_geoserver,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    sync_vector_to_gcs,
)
from dateutil.relativedelta import relativedelta

from nrm_app.celery import app
from waterrejuvenation.utils import wait_for_task_completion, WATER_REJ_GEE_ASSET
from .swb2 import calculate_swb2
from .swb3 import calculate_swb3
import logging

logger = logging.getLogger(__name__)


@app.task(bind=True)
def generate_swb_layer(self,  start_year, end_year, state=None, district=None, block=None, roi=None, app_name=None, proj_name = None, force_regenerate = False):
    start_date = f"{start_year}-07-01"
    end_date = f"{end_year}-06-30"

    ee_initialize()
    if state and district and block:
        aoi = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )
        layer_name = (
                "surface_waterbodies_"
                + valid_gee_text(district.lower())
                + "_"
                + valid_gee_text(block.lower())
        )
    else:
        description = "swb1_" + str(app_name) + "_" + str(proj_name)
        aoi = ee.FeatureCollection(

            WATER_REJ_GEE_ASSET + str(proj_name)+ "/"+"filtered_mws_" + str(proj_name)
        )
        layer_name = (
                description
               )


    calculate_swb1(start_date, end_date, aoi, force_regenerate, state, district, block, roi, app_name, proj_name)
    swb2_asset_id = calculate_swb2(aoi, force_regenerate, state, district,  block, roi, app_name, proj_name)

    if not swb2_asset_id:
        swb3_asset_id = calculate_swb3(aoi, state, district, block,roi,app_name,proj_name)
        fc = ee.FeatureCollection(swb3_asset_id)
        res = sync_fc_to_geoserver(fc, app_name, layer_name, workspace="water_rej")
        print(res)
        return swb3_asset_id


    else:
        return swb2_asset_id




def calculate_swb1(start_date, end_date, aoi,  forece_regenerate, state=None, district=None, block=None, roi=None, app_name=None, proj_id =None):

    """
    Calculate surface water bodies analysis version 1.
    Analyzes water body presence and characteristics over time.

    Args:
        aoi: Area of interest
        state, district, block: Geographic location parameters
        start_date, end_date: Analysis period
    Returns:
        Task ID if successful, None if asset already exists
    """
    # Generate description and asset ID for the analysis

    if state and district and block:
        print ("insdie dist flow")
        description = (
        "swb1_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    )

        asset_id = get_gee_asset_path(state, district, block) + description
    else:
        print ("inside else")
        description = "swb1_"+str(app_name) +"_" +str(proj_id)
        asset_id = roi +"/" + description
    if forece_regenerate:
        try:
             ee.data.deleteAsset(asset_id)

             logger.info(f"Deleted existing asset: {asset_id}")
        except Exception as e:
             logger.info(f"No existing asset to delete or error occurred: {e}")
    print("asset_id: "+asset_id)
    # Skip if asset already exists
    if is_gee_asset_exists(asset_id) and not forece_regenerate:
        return

    # Calculate date range for analysis
    loop_start = start_date
    loop_end = (
        datetime.datetime.strptime(end_date, "%Y-%m-%d") + relativedelta(years=1)
    ).strftime("%Y-%m-%d")

    # Initialize collections for analysis
    collec = []  # Binary water presence
    collec2 = []  # Original LULC images

    lulc_bandname = "predicted_label"

    # Process each year in the date range
    while loop_start < loop_end:
        curr_start_date = datetime.datetime.strptime(loop_start, "%Y-%m-%d")
        curr_end_date = (
            curr_start_date + relativedelta(years=1) - datetime.timedelta(days=1)
        )

        loop_start = (curr_start_date + relativedelta(years=1)).strftime("%Y-%m-%d")
        curr_start_date = curr_start_date.strftime("%Y-%m-%d")
        curr_end_date = curr_end_date.strftime("%Y-%m-%d")
        print (curr_start_date)
        print (curr_end_date)
        # Get LULC map for current period
        if state and district and block:
            image = ee.Image(
                get_gee_asset_path(state, district, block)
                + valid_gee_text(district.lower())
                + "_"
                + valid_gee_text(block.lower())
                + "_"
                + curr_start_date
                + "_"
                + curr_end_date
                + "_LULCmap_10m"
                )
        else:
            image = ee.Image(roi+"/"+"clipped_lulc_filtered_mws"+"_"+str(proj_id)+ "_"
                + curr_start_date[:4]
                + "_"
                + curr_end_date[:4])


        # Add water presence masks to collections
        collec.append(image.gte(2).And(image.lte(4)))
        collec2.append(image)

    # Create OR operation string for combining all years
    ored_str = "collec[0]"
    for i in range(1, len(collec) - 1):
        ored_str = ored_str + ".Or(collec[" + str(i) + "])"

    ored = eval(ored_str)

    # Convert to vector polygons
    multi_band_image = ored.addBands(ored)
    vector_polygons = multi_band_image.reduceToVectors(
        reducer=ee.Reducer.anyNonZero(),
        geometry=aoi.geometry(),
        scale=10,
        maxPixels=1e13,
        crs=multi_band_image.projection(),
        geometryType="polygon",
        eightConnected=True,
        labelProperty="water",
    )

    # Filter water features
    features = vector_polygons.filter(ee.Filter.eq("water", 1))

    def calculate_metrics(feature):
        """
        Calculate various metrics for each water body feature
        Including area and seasonal water presence
        """
        # Calculate total area
        fi = ored.clip(feature.geometry())
        total_cnt = fi.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=feature.geometry(),
            scale=10,
            maxPixels=1e13,
        ).getNumber(lulc_bandname)

        # Calculate yearly statistics
        cnt_res = []
        ci = 0
        while ci < len(collec):
            clipped_image = collec[ci].clip(feature.geometry())
            count = clipped_image.reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=feature.geometry(),
                scale=10,
                maxPixels=1e13,
            ).getNumber(lulc_bandname)
            cnt_res.append(count)
            ci += 1

        # Calculate seasonal percentages
        p1, p2, p3, p4 = [], [], [], []
        j = 0
        while j < len(collec2):
            binary_collection = collec2[j]

            # Create binary masks for different classes
            binary_image0 = binary_collection.lte(1)  # Non-water
            binary_image1 = binary_collection.eq(2)  # Kharif
            binary_image2 = binary_collection.eq(3)  # Kharif+Rabi
            binary_image3 = binary_collection.eq(4)  # Kharif+Rabi+Zaid

            # Calculate areas for each class
            counts = []
            for img in [binary_image0, binary_image1, binary_image2, binary_image3]:
                clipped = img.clip(feature.geometry())
                count = clipped.reduceRegion(
                    reducer=ee.Reducer.sum(),
                    geometry=feature.geometry(),
                    scale=10,
                    maxPixels=1e13,
                ).getNumber(lulc_bandname)
                counts.append(count)

            total_count = counts[0].add(counts[1]).add(counts[2]).add(counts[3])

            # Calculate percentages for each season
            count1 = counts[1].add(counts[2]).add(counts[3])
            count2 = counts[2].add(counts[3])

            # Store percentages
            p1.append((counts[0].divide(ored)).multiply(100))
            p2.append((count1.divide(ored)).multiply(100))
            p3.append((count2.divide(ored)).multiply(100))
            p4.append((counts[3].divide(ored)).multiply(100))
            j += 1

        # Set properties for the feature
        properties = {"area_ored": ored.multiply(100)}
        i = 0
        loop_start = start_date
        while loop_start < loop_end:
            curr_start_date = datetime.datetime.strptime(loop_start, "%Y-%m-%d")
            year = f"{curr_start_date.year % 100}-{curr_start_date.year % 100 + 1}"

            # Store area and seasonal percentages
            properties[f"area_{year}"] = cnt_res[i].multiply(100)
            properties[f"k_{year}"] = p2[i]  # Kharif
            properties[f"kr_{year}"] = p3[i]  # Kharif+Rabi
            properties[f"krz_{year}"] = p4[i]  # Kharif+Rabi+Zaid

            i += 1
            loop_start = (curr_start_date + relativedelta(years=1)).strftime("%Y-%m-%d")

        return feature.set(properties)

    # Apply metrics calculation to all features
    feature_collection_with_metrics = features.map(calculate_metrics)

    def updated_feature_collection(feature):
        """
        Categorize water bodies based on area
        """
        area = feature.getNumber("area_ored")

        # Categorize based on area ranges
        new_category = ee.Algorithms.If(
            ee.Number(area).lte(499),
            "0-500",
            ee.Algorithms.If(
                ee.Number(area).lte(999),
                "500-1000",
                ee.Algorithms.If(
                    ee.Number(area).lte(4999), "1000-5000", "5000 and above"
                ),
            ),
        )
        return feature.set("0_category_sq_m", new_category)

    # Apply categorization
    updated_feature_collection_with_metrics = feature_collection_with_metrics.map(
        updated_feature_collection
    )

    # Export results to GEE asset
    try:
        swb_task = ee.batch.Export.table.toAsset(
            collection=updated_feature_collection_with_metrics,
            description=description,
            assetId=asset_id,
        )

        swb_task.start()
        wait_for_task_completion(swb_task)
        print("Successfully started the swb 1", swb_task.status())
        return swb_task.status()["id"]
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error occurred in running swb1 task: {e}")
