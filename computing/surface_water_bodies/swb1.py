import datetime
import ee

from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    get_gee_dir_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
)
from dateutil.relativedelta import relativedelta


def vectorize_water_pixels(
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type=None,
    start_date=None,
    end_date=None,
):
    """
    Analyzes water body presence and characteristics over time.

    Args:
        roi: Area of interest
        asset_suffix: Suffix to be added in layer name e.g.- _{district_name}_{block_name}
        asset_folder_list: folder name in hierarchy in which asset should be saved on GEE
        e.g. [state_name, district_name, block_name] or [org_name, project_name]
        app_type: Type of the App (MWS, PLANTATION, WATER_REJ)
        start_date: Analysis start date
        end_date: Analysis end date
    Returns:
        Task ID if successful, None if asset already exists
    """
    # Generate description and asset ID for the analysis
    description = "swb1_" + asset_suffix
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )

    # Skip if asset already exists
    if is_gee_asset_exists(asset_id):
        return None

    # Initialize collections for analysis
    lulc_water_pixel_collec = []  # Binary water presence
    lulc_collec = []  # Original LULC images

    lulc_bandname = "predicted_label"

    # Calculate date range for analysis
    loop_start = start_date
    loop_end = (
        datetime.datetime.strptime(end_date, "%Y-%m-%d") + relativedelta(years=1)
    ).strftime("%Y-%m-%d")

    # Process each year in the date range
    while loop_start < loop_end:
        curr_start_date = datetime.datetime.strptime(loop_start, "%Y-%m-%d")
        curr_end_date = (
            curr_start_date + relativedelta(years=1) - datetime.timedelta(days=1)
        )

        loop_start = (curr_start_date + relativedelta(years=1)).strftime("%Y-%m-%d")
        curr_start_date = curr_start_date.strftime("%Y-%m-%d")
        curr_end_date = curr_end_date.strftime("%Y-%m-%d")

        # Get LULC map for current period
        print ("issue")
        print (str(get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + asset_suffix
            + "_"
            + curr_start_date
            + "_"
            + curr_end_date
            + "_LULCmap_10m"
        ))

        lulc_image = ee.Image(
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + asset_suffix
            + "_"
            + curr_start_date
            + "_"
            + curr_end_date
            + "_LULCmap_10m"
        )

        # Add water presence masks to collections
        lulc_water_pixel_collec.append(lulc_image.gte(2).And(lulc_image.lte(4)))
        lulc_collec.append(lulc_image)

    # Create OR operation string for combining all years
    ored_str = "lulc_water_pixel_collec[0]"
    for i in range(1, len(lulc_water_pixel_collec) - 1):
        ored_str = ored_str + ".Or(lulc_water_pixel_collec[" + str(i) + "])"

    ored = eval(ored_str)

    multi_band_image = ored.addBands(ored)

    # Convert to vector polygons
    vector_polygons = multi_band_image.reduceToVectors(
        reducer=ee.Reducer.anyNonZero(),
        geometry=roi.geometry(),
        scale=10,
        maxPixels=1e13,
        crs=multi_band_image.projection(),
        geometryType="polygon",
        eightConnected=True,
        labelProperty="water",
    )

    # Filter water features
    water_bodies = vector_polygons.filter(ee.Filter.eq("water", 1))

    def calculate_metrics(feature):
        """
        Calculate various metrics for each water body feature
        Including area and seasonal water presence
        """
        # Calculate total area
        total_ored_area = (
            ored.clip(feature.geometry())
            .reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=feature.geometry(),
                scale=10,
                maxPixels=1e13,
            )
            .getNumber(lulc_bandname)
        )

        # Calculate yearly statistics
        cnt_res = []
        ci = 0
        while ci < len(lulc_water_pixel_collec):
            clipped_image = lulc_water_pixel_collec[ci].clip(feature.geometry())
            count = clipped_image.reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=feature.geometry(),
                scale=10,
                maxPixels=1e13,
            ).getNumber(lulc_bandname)
            cnt_res.append(count)
            ci += 1

        # Calculate seasonal percentages
        kharif_percentage, rabi_percentage, zaid_percentage = [], [], []
        j = 0
        while j < len(lulc_collec):
            binary_collection = lulc_collec[j]

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

            # total_count = counts[0].add(counts[1]).add(counts[2]).add(counts[3])

            # Calculate percentages for each season
            count1 = counts[1].add(counts[2]).add(counts[3])
            count2 = counts[2].add(counts[3])

            # Store percentages
            # p1.append((counts[0].divide(total_ored_area)).multiply(100))
            kharif_percentage.append((count1.divide(total_ored_area)).multiply(100))
            rabi_percentage.append((count2.divide(total_ored_area)).multiply(100))
            zaid_percentage.append((counts[3].divide(total_ored_area)).multiply(100))
            j += 1

        # Set properties for the feature
        area_ored = total_ored_area.multiply(100)
        properties = {"area_ored": area_ored.multiply(0.0001)}

        # Categorize based on area ranges
        wb_category = ee.Algorithms.If(
            ee.Number(area_ored).lte(499),
            "0-500",
            ee.Algorithms.If(
                ee.Number(area_ored).lte(999),
                "500-1000",
                ee.Algorithms.If(
                    ee.Number(area_ored).lte(4999), "1000-5000", "5000 and above"
                ),
            ),
        )

        properties["category_sq_m"] = wb_category

        i = 0
        loop_start = start_date
        while loop_start < loop_end:
            curr_start_date = datetime.datetime.strptime(loop_start, "%Y-%m-%d")
            year = f"{curr_start_date.year % 100}-{curr_start_date.year % 100 + 1}"

            # Store area and seasonal percentages
            properties[f"area_{year}"] = cnt_res[i].multiply(100).multiply(0.0001)
            properties[f"k_{year}"] = kharif_percentage[i]  # Kharif
            properties[f"kr_{year}"] = rabi_percentage[i]  # Kharif+Rabi
            properties[f"krz_{year}"] = zaid_percentage[i]  # Kharif+Rabi+Zaid

            i += 1
            loop_start = (curr_start_date + relativedelta(years=1)).strftime("%Y-%m-%d")

        return feature.set(properties)

    # Apply metrics calculation to all features
    feature_collection_with_metrics = water_bodies.map(calculate_metrics)
    print("feature collection extracted")
    # Export results to GEE asset
    task_id = export_vector_asset_to_gee(
        feature_collection_with_metrics, description, asset_id
    )
    return task_id
