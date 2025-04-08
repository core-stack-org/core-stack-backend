import ee


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

    # Retrieve and process Sentinel-2 imagery
    # Note: Commented out Landsat option suggests flexibility in data source
    # landsat_collection = get_landsat_data(roi, cloud_threshold, start_date, end_date)
    sentinel_collection = get_sentinel_data(cloud_threshold, start_date, end_date)

    # Compute NDVI for the Sentinel-2 image collection
    # NDVI = (NIR - Red) / (NIR + Red)
    ndvi = sentinel_collection.map(
        lambda image: image.normalizedDifference(ndvi_bands)
        .set("system:time_start", image.get("system:time_start"))
        .rename("NDVI")
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
                reducer=ee.Reducer.mean(), geometry=feature.geometry(), scale=30
            ).get("NDVI")

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


def get_sentinel_data(cloud_threshold, start_date, end_date):
    """
    Retrieve and preprocess Sentinel-2 satellite imagery.

    Args:
        cloud_threshold: Maximum acceptable cloud percentage
        start_date: Start of the date range
        end_date: End of the date range

    Returns:
        Processed Sentinel-2 image collection
    """
    sentinel_collection = (
        # Select Harmonized Sentinel-2 surface reflectance collection
        ee.ImageCollection("COPERNICUS/S2_HARMONIZED")
        # Filter by date range
        .filterDate(start_date, end_date)
        # Filter out images with high cloud coverage
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", cloud_threshold))
        # Apply cloud masking
        .map(mask_s2clouds)
        # Add timestamp to each image based on its index
        .map(
            lambda image: image.set(
                "system:time_start",
                ee.Date.parse(
                    "yyyyMMdd", ee.String(image.get("system:index")).slice(0, 8)
                ).millis(),
            )
        )
    )
    return sentinel_collection


def mask_s2clouds(image):
    """
    Remove clouds and cirrus from Sentinel-2 images using QA60 band.

    Args:
        image: Sentinel-2 satellite image

    Returns:
        Cloud-masked and normalized image
    """
    # Select the QA60 quality assessment band
    qa = image.select("QA60")

    # Define bit masks for clouds and cirrus
    # Bits 10 and 11 represent clouds and cirrus, respectively
    cloud_bit_mask = 1 << 10
    cirrus_bit_mask = 1 << 11

    # Create a mask where both cloud and cirrus bits are zero
    # This indicates clear atmospheric conditions
    # Both flags should be set to zero, indicating clear conditions.
    mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(qa.bitwiseAnd(cirrus_bit_mask).eq(0))

    # Apply the mask and normalize pixel values
    # Divide by 10000 to convert to reflectance values
    return image.updateMask(mask).divide(10000)


# def get_landsat_data(roi, cloud_threshold, start_date, end_date):
#     # Landsat ImageCollection
#     landsat_collection = (
#         ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
#         .filterDate(start_date, end_date)
#         .filterBounds(roi.geometry())
#         .filter(ee.Filter.lt("CLOUD_COVER", cloud_threshold))
#         .map(mask_clouds_qa)
#     )
#     return landsat_collection
#
# def mask_clouds_qa(image):
#     qa = image.select("QA_PIXEL")
#     cloud = (
#         qa.bitwiseAnd(1 << 5)
#         .And(qa.bitwiseAnd(1 << 6).Or(qa.bitwiseAnd(1 << 7)))
#         .Or(qa.bitwiseAnd(1 << 4))
#         .Or(qa.bitwiseAnd(1 << 3))
#         .Or(qa.bitwiseAnd(1 << 8).And(qa.bitwiseAnd(1 << 9)))
#     )
#     return image.updateMask(cloud.Not())
