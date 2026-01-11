import ee
import datetime
import json
import re
import time

from gee_computing.models import GEEAccount
from nrm_app.celery import app

from computing.misc.hls_interpolated_ndvi import get_padded_ndvi_ts_image
from computing.utils import (
    get_layer_object,
    save_layer_info_to_db,
    sync_layer_to_geoserver,
    update_layer_sync_status,
    sync_fc_to_geoserver,
)
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_dir_path,
    export_vector_asset_to_gee,
    check_task_status,
    is_gee_asset_exists,
    merge_fc_into_existing_fc,
    make_asset_public,
    create_gee_dir,
    build_gee_helper_paths,
)


@app.task(bind=True)
def ndvi_timeseries(
    self,
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    start_year=None,
    end_year=None,
    app_type="MWS",
    gee_account_id=None,
):
    print(f"{gee_account_id=}")
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
        )

    description = f"ndvi_timeseries_{asset_suffix}"
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )

    start_date = f"{start_year}-07-01"
    end_date = f"{end_year+1}-06-30"

    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    layer_at_geoserver = False

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
                f"ndvi_timeseries layer not found in DB. So, reading the column name from asset_id."
            )
        existing_end_date = get_last_date(asset_id, layer_obj)

        print("existing_end_date", existing_end_date)
        print("end_date", end_date)
        new_start_date = existing_end_date
        last_date = str(existing_end_date.date())

        if existing_end_date.year < end_date.year:
            new_asset_id = f"{asset_id}_{last_date}_{str(end_date.date())}"
            new_description = f"{description}_{last_date}_{str(end_date.date())}"
            task_id, new_asset_id, last_date = _generate_data(
                app_type,
                asset_folder_list,
                new_asset_id,
                asset_suffix,
                new_description,
                new_start_date,
                end_date,
                roi,
                gee_account_id,
            )
            check_task_status([task_id])

            clean_asset_columns(description, new_asset_id)

            # Check if data for new year is generated, if yes then merge it in existing asset
            if is_gee_asset_exists(new_asset_id):
                merge_fc_into_existing_fc(
                    asset_id, description, new_asset_id, join_on="uid"
                )
    else:
        task_id, new_asset_id, last_date = _generate_data(
            app_type,
            asset_folder_list,
            asset_id,
            asset_suffix,
            description,
            start_date,
            end_date,
            roi,
            gee_account_id,
        )

        clean_asset_columns(description, asset_id)

    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=description,
            asset_id=asset_id,
            dataset_name="NDVI Timeseries",
            misc={
                "start_date": str(start_date.date()),
                "end_date": last_date,
            },
        )

        fc = ee.FeatureCollection(asset_id)
        res = sync_fc_to_geoserver(
            fc, asset_suffix, description, workspace="ndvi_timeseries"
        )
        print(res)

        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")

            layer_at_geoserver = True
    return layer_at_geoserver


def _generate_data(
    app_type,
    asset_folder_list,
    asset_id,
    asset_suffix,
    description,
    start_date,
    end_date,
    roi,
    gee_account_id,
):
    print("f_start_date>>>", start_date)
    print("end_date>>>", end_date)
    task_ids = []
    asset_ids = []
    f_start_date = start_date
    year_count = end_date.year - start_date.year
    last_date = None

    if year_count > 1:
        gee_obj = GEEAccount.objects.get(pk=gee_account_id)
        ee_initialize(gee_obj.helper_account.id)

    while f_start_date <= end_date:
        f_end_date = f_start_date + datetime.timedelta(days=364)
        print("f_end_date>>>", f_end_date)
        if f_end_date > end_date:
            break

        f_end_date_str = str(f_end_date.date())
        f_start_date_str = str(f_start_date.date())

        # Define export task details
        ndvi_description = f"{description}_{f_start_date_str}_{f_end_date_str}"
        ndvi_asset_id = (
            f"{asset_id}_{f_start_date_str}_{f_end_date_str}"
            if year_count > 1
            else asset_id
        )

        if not is_gee_asset_exists(ndvi_asset_id):

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
            crop_mask = lulc.remap([8, 9, 10, 11], [1, 1, 1, 1], 0)
            tree_mask = lulc.eq(6)
            shrub_mask = lulc.eq(12)

            # NDVI ImageCollection (14-day)
            ndvi = get_padded_ndvi_ts_image(f_start_date_str, f_end_date_str, roi, 14)

            def add_masked_bands(img):
                nd = img.select("gapfilled_NDVI_lsc")
                date = img.date().format("YYYY-MM-dd")

                return ee.Image.cat(
                    [
                        nd.updateMask(crop_mask).rename(ee.String("crop_").cat(date)),
                        nd.updateMask(tree_mask).rename(ee.String("tree_").cat(date)),
                        nd.updateMask(shrub_mask).rename(ee.String("shrub_").cat(date)),
                    ]
                )

            ndvi_masked = ndvi.map(add_masked_bands)

            # Convert time → bands (FAST)
            ndvi_band_stack = ndvi_masked.toBands()

            reduced = ndvi_band_stack.reduceRegions(
                collection=roi.select(["uid"]),
                reducer=ee.Reducer.mean(),
                scale=30,
                tileScale=4,  # helps large polygons
            )
            # Export as single-row-per-feature collection
            try:
                task = export_vector_asset_to_gee(
                    reduced, ndvi_description, ndvi_asset_id
                )
                print(f"Started export for {f_start_date.year}")
                asset_ids.append(ndvi_asset_id)
                task_ids.append(task)
            except Exception as e:
                print("Export error:", e)

        f_start_date = f_end_date
        last_date = str(f_start_date.date())

    check_task_status(task_ids)

    ee_initialize(gee_account_id)
    print(asset_ids)
    if len(asset_ids) > 1:
        # Merge year-wise outputs into a single collection
        task_id = export_vector_asset_to_gee(
            merge_assets_chunked_on_year(asset_ids),
            description,
            asset_id,
        )
        return task_id, asset_id, last_date
    return None, asset_id, last_date


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
        filtered_col = [
            col.split("_")[1] for col in col_names if col.startswith("crop_")
        ]
        filtered_col.sort()

        existing_end_date = datetime.datetime.strptime(filtered_col[-1], "%Y-%m-%d")

    return existing_end_date


def clean_asset_columns(description, asset_id):
    fc = ee.FeatureCollection(asset_id)
    features = fc.getInfo()["features"]

    for f in features:
        props = f["properties"]
        cleaned_props = clean_columns(props)
        # cleaned_features.append(cleaned_props)

        f["properties"] = cleaned_props

    ee_features = [
        ee.Feature(ee.Geometry(f["geometry"]), f["properties"]) for f in features
    ]

    task_id = export_vector_asset_to_gee(
        ee.FeatureCollection(ee_features),
        f"{description}_cleaned",
        f"{asset_id}_cleaned",
    )
    check_task_status([task_id])

    if is_gee_asset_exists(f"{asset_id}_cleaned"):
        # Delete existing asset
        ee.data.deleteAsset(asset_id)
        # Rename new asset with existing asset's name
        ee.data.copyAsset(f"{asset_id}_cleaned", asset_id)
        time.sleep(10)
        # Delete new asset
        ee.data.deleteAsset(f"{asset_id}_cleaned")


def clean_columns(feature):
    cleaned = {}
    for k, v in feature.items():
        new_key = re.sub(r"^\d+_", "", k)
        cleaned[new_key] = v
    return cleaned
