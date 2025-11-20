import json

import ee
import datetime

from computing.misc.hls_interpolated_ndvi import get_padded_ndvi_ts_image
from computing.utils import get_layer_object
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_dir_path,
    export_vector_asset_to_gee,
    get_gee_asset_path,
    check_task_status,
    is_gee_asset_exists,
    merge_fc_into_existing_fc,
)


def ndvi_timeseries(
    state="bihar",  # None,
    district="gaya",  # None,
    block="mohanpur",  # None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    start_year=2017,  # None,
    end_year=2021,  # None,
    gee_account_id=1,
):
    ee_initialize(gee_account_id)
    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

        roi = ee.FeatureCollection(
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        ).limit(1)

    description = f"ndvi_timeseries_{asset_suffix}"
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )

    start_date = f"{start_year}-07-01"
    end_date = f"{end_year}-06-30"

    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    if is_gee_asset_exists(asset_id):
        layer_obj = None
        try:
            layer_obj = get_layer_object(
                asset_folder_list[0],
                asset_folder_list[1],
                asset_folder_list[2],
                layer_name=f"{asset_suffix}_ndvi_timeseries",
                dataset_name="NDVI Timeseries",
            )
        except Exception as e:
            print(
                f"layer not found for precipitation. So, reading the column name from asset_id."
            )
        existing_end_date = get_last_date(asset_id, layer_obj)

        # end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        print("existing_end_date", existing_end_date)
        print("end_date", end_date)
        new_start_date = existing_end_date
        last_date = str(existing_end_date.date())

        # new_asset_id = f"{asset_id}_{last_date}_{str(end_date.date())}"
        new_description = f"{description}_{last_date}_{str(end_date.date())}"
        task_id, new_asset_id = _generate_data(
            app_type,
            asset_folder_list,
            asset_id,
            asset_suffix,
            new_description,
            new_start_date,
            end_date,
            roi,
        )
        check_task_status([task_id])

        # Check if data for new year is generated, if yes then merge it in existing asset
        if is_gee_asset_exists(new_asset_id):
            merge_fc_into_existing_fc(asset_id, description, new_asset_id)
    else:
        task_id, new_asset_id = _generate_data(
            app_type,
            asset_folder_list,
            asset_id,
            asset_suffix,
            description,
            start_date,
            end_date,
            roi,
        )


def _generate_data(
    app_type,
    asset_folder_list,
    asset_id,
    asset_suffix,
    description,
    start_date,
    end_date,
    roi,
):
    print("f_start_date>>>", start_date)
    print("end_date>>>", end_date)
    task_ids = []
    asset_ids = []
    f_start_date = start_date
    while f_start_date <= end_date:
        f_end_date = f_start_date + datetime.timedelta(days=364)
        print("f_end_date>>>", f_end_date)
        # if f_end_date > end_date:
        #     break

        f_end_date_str = str(f_end_date.date())
        f_start_date_str = str(f_start_date.date())

        # Define export task details
        ndvi_description = f"{description}_{f_start_date_str}_{f_end_date_str}"
        ndvi_asset_id = f"{asset_id}_{f_start_date_str}_{f_end_date_str}"

        lulc = ee.Image(
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + asset_suffix
            + "_"
            + str(f_start_date.year)
            + "-07-01_"
            + str(f_start_date.year + 1)
            + "-06-30_LULCmap_10m"
        )
        ndvi = get_padded_ndvi_ts_image(f_start_date_str, f_end_date_str, roi, 14)

        # Masks
        crop_mask = lulc.remap([8, 9, 10, 11], [1, 1, 1, 1], 0)
        tree_mask = lulc.remap([6], [1], 0)
        shrub_mask = lulc.remap([12], [1], 0)

        # NDVI masked by LULC class
        ndvi_crop = ndvi.map(lambda img: img.updateMask(crop_mask))
        ndvi_tree = ndvi.map(lambda img: img.updateMask(tree_mask))
        ndvi_shrub = ndvi.map(lambda img: img.updateMask(shrub_mask))

        # Function converts each image to (uid, date, ndvi)
        def extract_ts(image):
            date_str = image.date().format("YYYY-MM-dd")

            # Compute mean NDVI for all features at once
            reduced = image.reduceRegions(
                collection=roi,
                reducer=ee.Reducer.mean(),
                scale=30,
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

        # Extract time-series per category
        fc_crop = ndvi_crop.map(extract_ts).flatten()
        fc_tree = ndvi_tree.map(extract_ts).flatten()
        fc_shrub = ndvi_shrub.map(extract_ts).flatten()

        # Extract all unique UIDs from the input feature collection
        uids = roi.aggregate_array("uid")

        # For each UID, filter NDVI features and aggregate to dict
        def build_feature(uid):
            """
            Reconstruct a single feature by merging its NDVI values across all images
            into one property NDVI_<year> as a JSON dictionary {date: value}.
            """
            # Get the geometry and properties of the original feature
            feature_geom = ee.Feature(roi.filter(ee.Filter.eq("uid", uid)).first())

            # CROPPED
            f1 = fc_crop.filter(ee.Filter.eq("uid", uid))
            list1 = f1.aggregate_array("ndvi_date").zip(f1.aggregate_array("ndvi"))
            ndvi_crop_dict = ee.Dictionary(list1.flatten())
            ndvi_crop_json = ee.String.encodeJSON(ndvi_crop_dict)

            # TREE
            f2 = fc_tree.filter(ee.Filter.eq("uid", uid))
            list2 = f2.aggregate_array("ndvi_date").zip(f2.aggregate_array("ndvi"))
            ndvi_tree_dict = ee.Dictionary(list2.flatten())
            ndvi_tree_json = ee.String.encodeJSON(ndvi_tree_dict)

            # SHRUB
            f3 = fc_shrub.filter(ee.Filter.eq("uid", uid))
            list3 = f3.aggregate_array("ndvi_date").zip(f3.aggregate_array("ndvi"))
            ndvi_shrub_dict = ee.Dictionary(list3.flatten())
            ndvi_shrub_json = ee.String.encodeJSON(ndvi_shrub_dict)

            return (
                feature_geom.set(f"NDVI_crop_{f_start_date.year}", ndvi_crop_json)
                .set(f"NDVI_tree_{f_start_date.year}", ndvi_tree_json)
                .set(f"NDVI_shrub_{f_start_date.year}", ndvi_shrub_json)
            )

        # Apply feature-wise aggregation
        merged_fc = ee.FeatureCollection(uids.map(build_feature))

        # Export as single-row-per-feature collection
        try:
            task = export_vector_asset_to_gee(
                merged_fc, ndvi_description, ndvi_asset_id
            )
            print(f"Started export for {f_start_date.year}")
            asset_ids.append(ndvi_asset_id)
            task_ids.append(task)
        except Exception as e:
            print("Export error:", e)

        f_start_date = f_end_date
    check_task_status(task_ids)
    # Merge year-wise outputs into a single collection
    task_id = export_vector_asset_to_gee(
        merge_assets_chunked_on_year(asset_ids),
        description,
        asset_id,
    )
    return task_id, asset_id


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


def get_last_date(asset_id, layer_obj):
    if layer_obj:
        existing_end_date = layer_obj.misc["end_date"]
        existing_end_date = datetime.datetime.strptime(existing_end_date, "%Y-%m-%d")
    else:
        fc = ee.FeatureCollection(asset_id)
        col_names = fc.first().propertyNames().getInfo()
        filtered_col = [col for col in col_names if col.startswith("NDVI_")]
        filtered_col.sort()

        last_year_col = filtered_col[-1]
        col_data = fc.first().get(last_year_col)
        col_data = json.loads(col_data.getInfo())
        col_data = list(col_data.keys())

        existing_end_date = datetime.datetime.strptime(col_data[-1], "%Y-%m-%d")
        existing_end_date = existing_end_date + datetime.timedelta(days=14)

    return existing_end_date
