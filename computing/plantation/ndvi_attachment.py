import ee


def get_ndvi_data(suitability_vector, start_year, end_year):
    # Set according to dataset - bands [NIR, Red]
    ndvi_bands = ["B8", "B4"]
    cloud_threshold = 20

    start_date = f"{start_year}-07-01"
    end_date = f"{end_year+1}-06-30"

    # Choose dataset: Sentinel-2 or Landsat
    # landsat_collection = get_landsat_data(roi, cloud_threshold, start_date, end_date)
    sentinel_collection = get_sentinel_data(cloud_threshold, start_date, end_date)

    # Compute NDVI for the chosen dataset
    ndvi = sentinel_collection.map(
        lambda image: image.normalizedDifference(ndvi_bands)
        .set("system:time_start", image.get("system:time_start"))
        .rename("NDVI")
    )

    def get_ndvi(feature):
        """Calculate NDVI for a feature."""
        # Filter NDVI collection to get images within feature geometry
        ndvi_collection = ndvi.filterBounds(feature.geometry())

        def extract_ndvi(image):
            # Reduce region to get the mean NDVI value for the given feature
            mean_ndvi = image.reduceRegion(
                reducer=ee.Reducer.mean(), geometry=feature.geometry(), scale=10
            ).get("NDVI")

            temp_dict = ee.Dictionary(
                {
                    "NDVI": ee.Algorithms.If(
                        ee.Algorithms.IsEqual(mean_ndvi, None), -9999, mean_ndvi
                    ),
                    "date": image.date().format("YYYY-MM-dd"),
                }
            )

            return ee.Feature(None, temp_dict)

        # Map over the NDVI images and get the results in a list of features
        ndvi_series = ndvi_collection.map(extract_ndvi)

        # Aggregate NDVI values and dates
        ndvi_list = ndvi_series.aggregate_array("NDVI")
        date_list = ndvi_series.aggregate_array(
            "date"
        )  # TODO Ask: why are we keeping them as different parameters and not in key-value pairs?

        # Convert lists to strings using JSON encoding
        ndvi_values_str = ee.String.encodeJSON(ndvi_list)
        ndvi_dates_str = ee.String.encodeJSON(date_list)

        # Set results as properties of the feature
        return feature.set(
            {"NDVI_values": ndvi_values_str, "NDVI_dates": ndvi_dates_str}
        )

    return suitability_vector.map(get_ndvi)


def get_sentinel_data(cloud_threshold, start_date, end_date):
    sentinel_collection = (
        ee.ImageCollection("COPERNICUS/S2_HARMONIZED")
        .filterDate(start_date, end_date)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", cloud_threshold))
        .map(mask_s2clouds)
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
    qa = image.select("QA60")

    # Bits 10 and 11 are clouds and cirrus, respectively.
    cloud_bit_mask = 1 << 10
    cirrus_bit_mask = 1 << 11

    # Both flags should be set to zero, indicating clear conditions.
    mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(qa.bitwiseAnd(cirrus_bit_mask).eq(0))

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
