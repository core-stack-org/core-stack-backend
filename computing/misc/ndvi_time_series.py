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

    start_date = f"{start_year}-07-01"
    end_date = f"{end_year+1}-06-30"

    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    layer_at_geoserver = False
    # for cls in ["crop", "tree", "shrub"]:
    description = f"ndvi_timeseries_{asset_suffix}"
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )

    if is_gee_asset_exists(f"{asset_id}_shrub"):  # TODO check for all 3
        layer_obj = None
        try:
            layer_obj = get_layer_object(
                asset_folder_list[0],
                asset_folder_list[1],
                asset_folder_list[2],
                layer_name=f"{description}_shrub",
                dataset_name="NDVI Timeseries",
            )
        except Exception as e:
            print(
                f"ndvi_timeseries layer not found in DB. So, reading the column name from asset_id."
            )
        existing_end_date = get_last_date(f"{asset_id}_shrub", layer_obj)

        print("existing_end_date", existing_end_date)
        print("end_date", end_date)
        new_start_date = existing_end_date
        last_date = str(existing_end_date.date())

        if existing_end_date.year < end_date.year:
            new_asset_ids, last_date = _generate_data(
                app_type,
                asset_folder_list,
                asset_id,
                asset_suffix,
                description,
                new_start_date,
                end_date,
                roi,
                gee_account_id,
            )
            print(new_asset_ids)

            if len(new_asset_ids) > 1:
                ee_initialize(gee_account_id)

            build_final_class_asset(new_asset_ids, asset_id, description)
    else:
        new_asset_ids, last_date = _generate_data(
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

        print(new_asset_ids)
        if len(new_asset_ids) > 1:
            ee_initialize(gee_account_id)

        build_final_class_asset(new_asset_ids, asset_id, description)

    for cls in ["crop", "tree", "shrub"]:
        cls_asset_id = f"{asset_id}_{cls}"
        cls_description = f"{description}_{cls}"
        if is_gee_asset_exists(cls_asset_id):
            make_asset_public(cls_asset_id)
            layer_id = save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=cls_description,
                asset_id=cls_asset_id,
                dataset_name="NDVI Timeseries",
                misc={
                    "start_date": str(start_date.date()),
                    "end_date": last_date,
                },
            )

            fc = ee.FeatureCollection(cls_asset_id)
            res = sync_fc_to_geoserver(
                fc, asset_suffix, cls_description, workspace="ndvi_timeseries"
            )
            print(res)

            if res["status_code"] == 201 and layer_id:
                update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
                print("sync to geoserver flag is updated")

                layer_at_geoserver = True
        return layer_at_geoserver


def extract_class_fc(asset_id, cls_prefix):
    """
    asset_id: yearly NDVI asset
    cls_prefix: 'crop' | 'tree' | 'shrub'
    """

    fc = ee.FeatureCollection(asset_id)

    def filter_props(f):
        props = f.toDictionary()
        keys = props.keys().filter(ee.Filter.stringStartsWith("item", cls_prefix))

        def build_dict(k, acc):
            k = ee.String(k)
            # remove "<number>_<cls_prefix>_"
            new_key = k.split("_").slice(1).join("_")
            return ee.Dictionary(acc).set(new_key, props.get(k))

        new_props = ee.Dictionary(keys.iterate(build_dict, ee.Dictionary({})))
        return ee.Feature(f.geometry(), new_props.set("uid", f.get("uid")))

    return fc.map(filter_props)


def build_final_class_asset(yearly_assets, asset_id, description):
    task_ids = []
    existing_asset_ids = []
    for cls_prefix in ["crop", "tree", "shrub"]:

        cls_asset_id = f"{asset_id}_{cls_prefix}"
        fc_list = [extract_class_fc(asset, cls_prefix) for asset in yearly_assets]

        asset_exists = False
        if is_gee_asset_exists(cls_asset_id):
            asset_exists = True
            merged = ee.FeatureCollection(cls_asset_id)
            ind = 0
        else:
            merged = fc_list[0]
            ind = 1

        for fc in fc_list[ind:]:
            merged = merged.map(
                lambda f: ee.Feature(
                    f.geometry(),
                    f.toDictionary().combine(
                        ee.Feature(
                            fc.filter(ee.Filter.eq("uid", f.get("uid"))).first()
                        ).toDictionary(),
                        overwrite=False,
                    ),
                )
            )
        if asset_exists:
            existing_asset_ids.append(cls_asset_id)
            cls_asset_id = f"{cls_asset_id}_tmp"

        task_id = export_vector_asset_to_gee(
            merged, f"{description}_{cls_prefix}", cls_asset_id
        )
        task_ids.append(task_id)

    check_task_status(task_ids)

    if len(existing_asset_ids) > 0:
        for asset_id in existing_asset_ids:
            ee.data.deleteAsset(asset_id)
            ee.data.copyAsset(f"{asset_id}_tmp", asset_id)
            ee.data.deleteAsset(f"{asset_id}_tmp")


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
        ndvi_asset_id = f"{asset_id}_{f_start_date_str}_{f_end_date_str}"

        print(ndvi_asset_id)
        asset_ids.append(ndvi_asset_id)

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

            def filter_props(f):
                props = f.toDictionary()

                keys = props.keys().filter(
                    ee.Filter.Or(
                        ee.Filter.stringContains("item", "_crop_"),
                        ee.Filter.stringContains("item", "_tree_"),
                        ee.Filter.stringContains("item", "_shrub_"),
                    )
                )

                def build_dict(k, acc):
                    k = ee.String(k)
                    # remove "<number>_"
                    new_key = k.split("_").slice(1).join("_")
                    return ee.Dictionary(acc).set(new_key, props.get(k))

                new_props = ee.Dictionary(keys.iterate(build_dict, ee.Dictionary({})))
                return ee.Feature(f.geometry(), new_props.set("uid", f.get("uid")))

            fc = reduced.map(filter_props)

            # Export as single-row-per-feature collection
            try:
                task = export_vector_asset_to_gee(fc, ndvi_description, ndvi_asset_id)
                print(f"Started export for {f_start_date.year}")
                asset_ids.append(ndvi_asset_id)
                task_ids.append(task)
            except Exception as e:
                print("Export error:", e)

        f_start_date = f_end_date
        last_date = str(f_start_date.date())

    check_task_status(task_ids)

    return asset_ids, last_date


def get_last_date(asset_id, layer_obj):
    if layer_obj:
        existing_end_date = layer_obj.misc["end_date"]
        existing_end_date = datetime.datetime.strptime(existing_end_date, "%Y-%m-%d")
    else:
        fc = ee.FeatureCollection(asset_id)
        col_names = fc.first().propertyNames().getInfo()
        filtered_col = [col for col in col_names if col.startswith("20")]
        filtered_col.sort()
        last_date = filtered_col[-1]
        existing_end_date = datetime.datetime.strptime(last_date, "%Y-%m-%d")

    return existing_end_date
