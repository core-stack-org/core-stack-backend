import ee
from computing.plantation.harmonized_ndvi import Get_Padded_NDVI_TS_Image
from utilities.constants import GEE_PATH_PLANTATION
from utilities.gee_utils import (
    check_task_status,
    get_gee_dir_path,
    valid_gee_text,
    is_gee_asset_exists,
)


def get_ndvi_data(suitability_vector, start_year, end_year, organization, project_name):
    """
    Retrieve Normalized Difference Vegetation Index (NDVI) data
    for a given set of features across a specified time range.

    Args:
        suitability_vector: Earth Engine FeatureCollection to annotate with NDVI data
        start_year: Beginning of the temporal analysis range
        end_year: End of the temporal analysis range

    Returns:
        FeatureCollection with NDVI values and dates added as encoded JSON strings
    """
    s_year = start_year
    task_ids = []
    asset_ids = []
    # Construct start and end dates for the entire analysis period
    # Uses July 1st to June 30th to align with agricultural/seasonal cycles
    while s_year <= end_year:
        start_date = f"{s_year}-07-01"
        end_date = f"{s_year+1}-06-30"
        description = (
            "ndvi_"
            + str(s_year)
            + "_"
            + valid_gee_text(organization)
            + "_"
            + valid_gee_text(project_name)
        )
        asset_id = (
            get_gee_dir_path(
                [organization, project_name], asset_path=GEE_PATH_PLANTATION
            )
            + description
        )

        # Remove existing asset if it exists
        if is_gee_asset_exists(asset_id):
            ee.data.deleteAsset(asset_id)

        ndvi = Get_Padded_NDVI_TS_Image(
            start_date, end_date, suitability_vector.bounds()
        )

        def get_ndvi(feature):
            """
            Extract NDVI data for a single feature.

            Args:
                feature: A single geographic feature to analyze

            Returns:
                Feature with NDVI values and dates added as properties
            """
            # Filter NDVI collection to images intersecting the feature's geometry
            ndvi_collection = ndvi.filterBounds(feature.geometry())

            def extract_ndvi(image):
                """
                Extract mean NDVI value for a specific image and feature.

                Args:
                    image: Satellite image to analyze

                Returns:
                    Feature with NDVI value and date
                """
                # Calculate mean NDVI for the feature's geometry
                mean_ndvi = image.reduceRegion(
                    reducer=ee.Reducer.mean(), geometry=feature.geometry(), scale=10
                ).get("gapfilled_NDVI_lsc")

                # Get the date and extract the year
                date = image.date()
                date_str = date.format("YYYY-MM-dd")
                # Extract year as number for proper filtering
                year_num = date.get("year")

                # Handle potential missing NDVI values
                ndvi_value = ee.Algorithms.If(
                    ee.Algorithms.IsEqual(mean_ndvi, None), -9999, mean_ndvi
                )

                return ee.Feature(
                    None,
                    {
                        "date": date_str,
                        "ndvi": ndvi_value,
                        "year": year_num,  # Store year as number for filtering
                    },
                )

            # Extract NDVI for all images intersecting the feature
            ndvi_series = ndvi_collection.map(extract_ndvi)

            # Initialize the feature to be returned (will add year properties to it)
            result_feature = feature

            # Process each year and add it as a separate property
            for year in range(s_year, s_year + 1):
                # Define field name for this year
                field_name = f"NDVI_{year}"

                # Filter features for the current year
                year_features = ndvi_series.filter(ee.Filter.eq("year", year))

                # Get the size of the collection after filtering
                count = year_features.size()

                # Convert to list for processing
                year_ndvi_list = ee.Algorithms.If(
                    ee.Algorithms.IsEqual(count, 0),
                    ee.List([]),  # Empty list if no features
                    year_features.toList(count).map(
                        lambda feature: ee.List(
                            [
                                ee.Feature(feature).get("date"),
                                ee.Feature(feature).get("ndvi"),
                            ]
                        )
                    ),
                )

                # Encode as JSON string
                year_ndvi_json = ee.String.encodeJSON(year_ndvi_list)

                # Add as property to the result feature
                result_feature = result_feature.set(field_name, year_ndvi_json)

            return result_feature

        # Apply NDVI data retrieval to each feature in the suitability vector
        fc = suitability_vector.map(get_ndvi)

        try:
            # Export annotated feature collection to Earth Engine
            task = ee.batch.Export.table.toAsset(
                collection=fc,
                description=description,
                assetId=asset_id,
                project="ee-corestackdev",
            )
            task.start()
            s_year += 1
            print(f"Asset export task started.")
            asset_ids.append(asset_id)
            task_ids.append(task.status()["id"])
        except Exception as e:
            print("Exception in exporting suitability vector", e)

    check_task_status(task_ids)

    return merge_assets_chunked_on_year(asset_ids)


def merge_assets_chunked_on_year(chunk_assets):
    def merge_features(feature):
        # Get the unique ID of the current feature
        uid = feature.get("uid")
        matched_features = []
        for i in range(1, len(chunk_assets)):
            # Find the matching feature in the second collection
            matched_feature = ee.Feature(
                ee.FeatureCollection(chunk_assets[i])
                .filter(ee.Filter.eq("uid", uid))
                .first()
            )
            matched_features.append(matched_feature)

        merged_properties = feature.toDictionary()
        for f in matched_features:
            # Combine properties from both features
            merged_properties = merged_properties.combine(
                f.toDictionary(), overwrite=False
            )

        # Return a new feature with merged properties
        return ee.Feature(feature.geometry(), merged_properties)

    # Map the merge function over the first feature collection
    merged_fc = ee.FeatureCollection(chunk_assets[0]).map(merge_features)
    return merged_fc
