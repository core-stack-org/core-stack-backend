import ee
from computing.plantation.harmonized_ndvi import Get_Padded_NDVI_TS_Image


def get_ndvi_data(suitability_vector, start_year, end_year):
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
    # Sentinel-2 bands for NDVI calculation
    # NIR (Near Infrared): B8, Red: B4
    ndvi_bands = ["B8", "B4"]

    # Cloud cover threshold for image filtering
    cloud_threshold = 20

    # Construct start and end dates for the entire analysis period
    # Uses July 1st to June 30th to align with agricultural/seasonal cycles
    start_date = f"{start_year}-07-01"
    end_date = f"{end_year+1}-06-30"

    ndvi = Get_Padded_NDVI_TS_Image(start_date, end_date, suitability_vector.geometry())

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
                reducer=ee.Reducer.mean(), geometry=feature.geometry(), scale=30
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
        for year in range(start_year, end_year + 1):
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
    return suitability_vector.map(get_ndvi)
