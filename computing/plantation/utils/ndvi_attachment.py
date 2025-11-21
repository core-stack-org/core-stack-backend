import ee
from computing.plantation.utils.harmonized_ndvi import Get_Padded_NDVI_TS_Image
from utilities.gee_utils import (
    check_task_status,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
)


def get_ndvi_data(suitability_vector, start_year, end_year, description, asset_id):
    """
    Extracts and exports NDVI data for a set of features by aggregating NDVI values
    into a per-feature dictionary {date: NDVI} over the specified time range.

    Each feature is stored as a single row, with NDVI values stored as a JSON string
    in a property called NDVI_<year>.

    Args:
        suitability_vector (ee.FeatureCollection): Features to calculate NDVI over.
        start_year (int): Start of the NDVI analysis range.
        end_year (int): End of the NDVI analysis range.
        description (str): Description to use in the export task name.
        asset_id (str): Base asset ID to export the result to.

    Returns:
        ee.FeatureCollection: Merged NDVI time series across years.
    """
    task_ids = []
    asset_ids = []
    # Loop over each year
    while start_year <= end_year:
        start_date = f"{start_year}-07-01"
        end_date = f"{start_year+1}-06-30"

        # Define export task details
        ndvi_description = f"ndvi_{start_year}_{description}"
        ndvi_asset_id = f"{asset_id}_ndvi_{start_year}"

        # Remove previous asset if it exists to avoid overwrite issues
        if is_gee_asset_exists(ndvi_asset_id):
            ee.data.deleteAsset(ndvi_asset_id)

        # Get NDVI image collection (with 'gapfilled_NDVI_lsc' band)
        ndvi = Get_Padded_NDVI_TS_Image(
            start_date, end_date, suitability_vector.bounds()
        )

        def map_image(image):
            date_str = image.date().format("YYYY-MM-dd")

            # Compute mean NDVI for all features at once
            reduced = image.reduceRegions(
                collection=suitability_vector,
                reducer=ee.Reducer.mean(),
                scale=10,
            )

            # Add NDVI value and image date to each feature
            def annotate(feature):
                ndvi_val = ee.Algorithms.If(
                    ee.Algorithms.IsEqual(feature.get("gapfilled_NDVI_lsc"), None),
                    -9999,
                    feature.get("gapfilled_NDVI_lsc"),
                )
                return feature.set("ndvi_date", date_str).set("ndvi", ndvi_val)

            return reduced.map(annotate)

        # Map image-wise extraction and flatten to a single FeatureCollection
        all_ndvi = ndvi.map(map_image).flatten()

        # Extract all unique UIDs from the input feature collection
        uids = suitability_vector.aggregate_array("uid")

        # For each UID, filter NDVI features and aggregate to dict
        def build_feature(uid):
            """
            Reconstruct a single feature by merging its NDVI values across all images
            into one property NDVI_<year> as a JSON dictionary {date: value}.
            """
            # Get the geometry and properties of the original feature
            feature_geom = ee.Feature(
                suitability_vector.filter(ee.Filter.eq("uid", uid)).first()
            )

            # Filter all NDVI records related to this UID
            filtered = all_ndvi.filter(ee.Filter.eq("uid", uid))

            # Create dictionary: {date: ndvi}
            date_ndvi_list = filtered.aggregate_array("ndvi_date").zip(
                filtered.aggregate_array("ndvi")
            )

            # Convert to dictionary and encode as JSON string
            ndvi_dict = ee.Dictionary(date_ndvi_list.flatten())
            ndvi_json = ee.String.encodeJSON(ndvi_dict)

            return feature_geom.set(f"NDVI_{start_year}", ndvi_json)

        # Apply feature-wise aggregation
        merged_fc = ee.FeatureCollection(uids.map(build_feature))

        # Export as single-row-per-feature collection
        try:
            task = export_vector_asset_to_gee(
                merged_fc, ndvi_description, ndvi_asset_id
            )
            print(f"Started export for {start_year}")
            asset_ids.append(ndvi_asset_id)
            task_ids.append(task)
        except Exception as e:
            print("Export error:", e)

        start_year += 1

    check_task_status(task_ids)

    # Merge year-wise outputs into a single collection
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
