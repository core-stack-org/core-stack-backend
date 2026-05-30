import ee
import datetime

from computing.ndvi_timeseries.ndvi_helper import (
    export_ndvi_year_spatial_chunks,
    export_ndvi_vector_chunk,
)
from gee_computing.models import GEEAccount
from nrm_app.celery import app

from computing.misc.hls_interpolated_ndvi import get_padded_ndvi_ts_image
from computing.utils import (
    get_layer_object,
    save_layer_info_to_db,
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
    make_asset_public,
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
    mws_count=150,
    chunk_size=100,
):
    """
    It will generate ndvi timeseries layer for given location at tehsil level or region of intrest
    """
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
                mws_count=mws_count,
                chunk_size=chunk_size,
            )

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
            mws_count=mws_count,
            chunk_size=chunk_size,
        )

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
        # keys = props.keys().filter(ee.Filter.stringStartsWith("item", cls_prefix))
        keys = props.keys().filter(
            ee.Filter.stringContains("item", "_" + cls_prefix + "_")
        )

        def build_dict(k, acc):
            k = ee.String(k)
            # remove "<number>_<cls_prefix>_"
            new_key = k.split("_").slice(1).join("_")
            return ee.Dictionary(acc).set(new_key, props.get(k))

        new_props = ee.Dictionary(keys.iterate(build_dict, ee.Dictionary({})))
        return ee.Feature(f.geometry(), new_props.set("uid", f.get("uid")))

    return fc.map(filter_props)


def remove_prefix(fc, cls):
    def rename_feature(f):
        f = ee.Feature(f)
        props = f.toDictionary()
        keys = props.keys()

        def rename_key(k):
            k = ee.String(k)
            return ee.Algorithms.If(
                k.match("^" + cls + "_").size().gt(0), k.replace("^" + cls + "_", ""), k
            )

        new_keys = keys.map(rename_key)
        values = keys.map(lambda k: props.get(k))
        renamed = ee.Dictionary.fromLists(new_keys, values)
        return ee.Feature(f.geometry(), renamed)

    return fc.map(rename_feature)


# Keep only uid + crop* columns
def keep_class_columns(fc, cls):
    props = fc.first().propertyNames()
    cls_cols = props.filter(ee.Filter.stringStartsWith("item", cls))
    keep_cols = ee.List(["uid"]).cat(cls_cols)
    return fc.select(keep_cols)


# Merge function
def merge_fc(fc, current, cls):

    fc = keep_class_columns(ee.FeatureCollection(fc), cls)
    current = ee.FeatureCollection(current)
    join = ee.Join.saveFirst("match")
    join_filter = ee.Filter.equals(leftField="uid", rightField="uid")
    joined = join.apply(current, fc, join_filter)

    def copy_props(f):
        f = ee.Feature(f)
        match = ee.Feature(f.get("match"))

        # Copy properties
        out = ee.Feature(
            f.copyProperties(
                source=match, properties=match.propertyNames().remove("uid")
            )
        )

        # Remove nested feature property
        return out.select(out.propertyNames().remove("match"))

    return ee.FeatureCollection(joined.map(copy_props))


def build_final_class_asset(yearly_assets, asset_id, description):
    task_ids = []

    # Iterate through all collections
    fc_list = [ee.FeatureCollection(asset) for asset in yearly_assets]

    for cls in ["crop", "tree", "shrub"]:
        cls_asset_id = f"{asset_id}_{cls}"
        if is_gee_asset_exists(cls_asset_id):
            ee.data.deleteAsset(cls_asset_id)

        # Base collection
        base = keep_class_columns(fc_list[0], cls)

        # Merge remaining FCs
        merged = ee.FeatureCollection(
            ee.List(fc_list[1:]).iterate(
                lambda fc, current: merge_fc(fc, current, cls), base
            )
        )
        merged = remove_prefix(merged, cls)

        task_id = export_vector_asset_to_gee(
            merged, f"{description}_{cls}", cls_asset_id
        )
        task_ids.append(task_id)

    check_task_status(task_ids)


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
    mws_count,
    chunk_size,
):
    print("f_start_date>>>", start_date)
    print("end_date>>>", end_date)
    asset_ids = []
    f_start_date = start_date
    last_date = None

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

        gee_obj = GEEAccount.objects.get(pk=gee_account_id)
        helper_account_path = build_gee_helper_paths(
            app_type, gee_obj.helper_account.name
        )
        if not is_gee_asset_exists(ndvi_asset_id):
            if roi.size().getInfo() > mws_count:
                export_ndvi_year_spatial_chunks(
                    app_type,
                    asset_folder_list,
                    ndvi_asset_id,
                    asset_suffix,
                    ndvi_description,
                    f_start_date,
                    f_start_date_str,
                    f_end_date_str,
                    roi,
                    helper_account_path,
                    max_features=150,
                    max_area_ha=75000,
                    grid_size_deg=0.75,
                )
            else:
                # task_id = _generate_ndvi(
                #     ndvi_asset_id,
                #     asset_folder_list,
                #     app_type,
                #     asset_suffix,
                #     f_start_date,
                #     f_start_date_str,
                #     f_end_date_str,
                #     roi,
                #     ndvi_description,
                #     f_end_date,
                # )
                task_id = export_ndvi_vector_chunk(
                    roi,
                    ndvi_asset_id,
                    ndvi_description,
                    asset_folder_list,
                    app_type,
                    asset_suffix,
                    f_start_date,
                    f_start_date_str,
                    f_end_date_str,
                )
                if task_id:
                    check_task_status([task_id])
        f_start_date = f_end_date
        last_date = str(f_start_date.date())
    return asset_ids, last_date


# def _generate_ndvi(
#     ndvi_asset_id,
#     asset_folder_list,
#     app_type,
#     asset_suffix,
#     f_start_date,
#     f_start_date_str,
#     f_end_date_str,
#     roi,
#     ndvi_description,
#     f_end_date,
# ):
#     task_id = None
#     if not is_gee_asset_exists(ndvi_asset_id):
#
#         lulc = ee.Image(
#             get_gee_dir_path(
#                 asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
#             )
#             + asset_suffix
#             + "_"
#             + str(f_start_date.year)
#             + "-07-01_"
#             + str(f_start_date.year + 1)
#             + "-06-30_LULCmap_10m"
#         )
#         crop_mask = lulc.remap([8, 9, 10, 11], [1, 1, 1, 1], 0)
#         tree_mask = lulc.eq(6)
#         shrub_mask = lulc.eq(12)
#
#         # NDVI ImageCollection (14-day)
#         ndvi = get_padded_ndvi_ts_image(
#             f_start_date_str, f_end_date_str, roi.union(), 14
#         )
#
#         def add_masked_bands(img):
#             nd = img.select("gapfilled_NDVI_lsc")
#             date = img.date().format("YYYY-MM-dd")
#
#             return ee.Image.cat(
#                 [
#                     nd.updateMask(crop_mask).rename(ee.String("crop_").cat(date)),
#                     nd.updateMask(tree_mask).rename(ee.String("tree_").cat(date)),
#                     nd.updateMask(shrub_mask).rename(ee.String("shrub_").cat(date)),
#                 ]
#             )
#
#         ndvi_masked = ndvi.map(add_masked_bands)
#
#         # Convert time → bands (FAST)
#         ndvi_band_stack = ndvi_masked.toBands()
#
#         reduced = ndvi_band_stack.reduceRegions(
#             collection=roi.select(["uid"]),
#             reducer=ee.Reducer.mean(),
#             scale=30,
#             tileScale=4,  # helps large polygons
#         )
#
#         def filter_props(f):
#             props = f.toDictionary()
#
#             keys = props.keys().filter(
#                 ee.Filter.Or(
#                     ee.Filter.stringContains("item", "_crop_"),
#                     ee.Filter.stringContains("item", "_tree_"),
#                     ee.Filter.stringContains("item", "_shrub_"),
#                 )
#             )
#
#             def build_dict(k, acc):
#                 k = ee.String(k)
#                 # remove "<number>_"
#                 new_key = k.split("_").slice(1).join("_")
#                 return ee.Dictionary(acc).set(new_key, props.get(k))
#
#             new_props = ee.Dictionary(keys.iterate(build_dict, ee.Dictionary({})))
#             return ee.Feature(f.geometry(), new_props.set("uid", f.get("uid")))
#
#         fc = reduced.map(filter_props)
#
#         # Export as single-row-per-feature collection
#         try:
#             task = export_vector_asset_to_gee(fc, ndvi_description, ndvi_asset_id)
#             print(f"Started export for {f_start_date.year}")
#             task_id = task
#         except Exception as e:
#             print("Export error:", e)
#
#     # f_start_date = f_end_date
#     # last_date = str(f_start_date.date())
#     return task_id  # , last_date


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
