import datetime
import time
from datetime import timedelta
import re
import ee
from dateutil.relativedelta import relativedelta

from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    make_asset_public,
    export_raster_asset_to_gee,
    is_gee_asset_exists,
    get_gee_dir_path,
    gcs_file_exists,
    delete_gcs_raster_files,
)
from nrm_app.celery import app
from .cropping_frequency import *
from computing.utils import (
    save_layer_info_to_db,
    update_layer_sync_status,
    get_layer_object,
)

from computing.STAC_specs import generate_STAC_layerwise
from utilities.constants import PAN_INDIA_RIVER_BASIN_LULC_V3_BASE_PATH


def _parse_lulc_filename_for_years(final_output_filename: str):
    """
    final_output_filename format (created in clip_lulc_v3):
      <prefix>_<startYYYY>-<startMM>-<startDD>_<endYYYY>-<endMM>-<endDD>_LULCmap_<scale>

    We use regex instead of split('_20') because the prefix may contain substrings like '_205'
    which break naive year parsing (e.g. producing LULC_5_24_...).
    """
    # Capture prefix + the two YYYY values around the date segments.
    m = re.match(
        r"^(?P<prefix>.*)_(?P<start>\d{4})-\d{2}-\d{2}_(?P<end>\d{4})-\d{2}-\d{2}_",
        final_output_filename,
    )
    if not m:
        raise ValueError(
            f"Unexpected LULC filename format: '{final_output_filename}'. "
            "Expected <prefix>_<YYYY-MM-DD>_<YYYY-MM-DD>_LULCmap_<scale>."
        )
    prefix = m.group("prefix")
    s_year = m.group("start")[2:]
    e_year = m.group("end")[2:]
    return prefix, s_year, e_year


@app.task(bind=True)
def clip_lulc_v3(
    self,
    state=None,
    district=None,
    block=None,
    start_year=None,
    end_year=None,
    gee_account_id=None,
    roi_path=None,
    asset_folder=None,
    asset_suffix=None,
    app_type="MWS",
    force_regenerate=False,
):
    """
    it will generate raster lulc for all three level for given year at tehsil level.
    """
    ee_initialize(gee_account_id)
    print("Inside lulc_river_basin")
    start_date, end_date = str(start_year) + "-07-01", str(end_year + 1) + "-06-30"

    if state and district and block:
        roi = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        ).union()

        filename_prefix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        gee_asset_path = get_gee_asset_path(state, district, block)
    else:
        roi = ee.FeatureCollection(roi_path).union()

        filename_prefix = valid_gee_text(asset_suffix)

        gee_asset_path = get_gee_dir_path(
            asset_folder, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
    loop_start = start_date
    loop_end = end_date
    l1_asset_new = []
    final_output_filename_array_new = []
    final_output_assetid_array_new = []

    layer_obj = None
    if not force_regenerate:
        try:
            layer_obj = get_layer_object(
                state,
                district,
                block,
                layer_name=f"LULC_17_18_{filename_prefix}_level_3",
                dataset_name="LULC_level_3",
            )
        except Exception as e:
            print("DB layer not found for lulc.")

    new_loop_start = None
    if layer_obj:
        existing_end_year = int(layer_obj.misc["end_year"])
        new_loop_start = str(existing_end_year) + "-07-01"
        new_loop_start = datetime.strptime(new_loop_start, "%Y-%m-%d")

    scale = 10

    loop_start = datetime.strptime(loop_start, "%Y-%m-%d")
    loop_end = datetime.strptime(loop_end, "%Y-%m-%d")
    print(loop_start, loop_end)

    while loop_start <= loop_end:
        print("loop_start", loop_start)
        curr_start_date = loop_start  # datetime.strptime(loop_start, "%Y-%m-%d")
        curr_end_date = curr_start_date + relativedelta(years=1) - timedelta(days=1)

        loop_start = curr_start_date + relativedelta(years=1)  # .strftime("%Y-%m-%d")
        curr_start_year = curr_start_date.year
        curr_end_year = curr_end_date.year

        curr_filename = (
            filename_prefix
            + "_"
            + curr_start_date.strftime("%Y-%m-%d")
            + "_"
            + curr_end_date.strftime("%Y-%m-%d")
        )

        final_output_filename = curr_filename + "_LULCmap_" + str(scale) + "m"
        final_output_assetid = gee_asset_path + final_output_filename
        final_output_filename_array_new.append(final_output_filename)
        final_output_assetid_array_new.append(final_output_assetid)
        if not new_loop_start or loop_start >= new_loop_start:
            pan_india = ee.Image(
                f"{PAN_INDIA_RIVER_BASIN_LULC_V3_BASE_PATH}_{curr_start_year}_{curr_end_year}"
            )
            # clipToCollection expects a FeatureCollection; we have geometry here.
            clipped_lulc = pan_india.clip(roi.geometry())
            l1_asset_new.append(clipped_lulc)

    task_list = []
    geometry = roi.geometry()
    if force_regenerate or not is_gee_asset_exists(
        final_output_assetid_array_new[len(l1_asset_new) - 1]
    ):
        for i in range(0, len(l1_asset_new)):
            if force_regenerate and is_gee_asset_exists(final_output_assetid_array_new[i]):
                try:
                    ee.data.deleteAsset(final_output_assetid_array_new[i])
                except Exception as e:
                    print("Error deleting existing LULC asset during force regenerate:", e)
            if (
                is_gee_asset_exists(final_output_assetid_array_new[i])
                and len(l1_asset_new) <= 2
            ):
                ee.data.copyAsset(
                    final_output_assetid_array_new[i],
                    f"{final_output_assetid_array_new[i]}_old",
                )
                time.sleep(10)
                ee.data.deleteAsset(final_output_assetid_array_new[i])
            task_id = export_raster_asset_to_gee(
                image=l1_asset_new[i].clip(geometry),
                description=final_output_filename_array_new[i],
                asset_id=final_output_assetid_array_new[i],
                scale=scale,
                region=geometry,
                pyramiding_policy={"predicted_label": "mode"},
            )
            task_list.append(task_id)

        task_id_list = check_task_status(task_list)
        print("LULC task_id_list", task_id_list)

    layer_ids = []
    lulc_workspaces = ["LULC_level_1", "LULC_level_2", "LULC_level_3"]
    for i in range(0, len(final_output_filename_array_new)):
        name_arr = final_output_filename_array_new[i].split("_20")
        s_year = name_arr[1][:2]
        e_year = name_arr[2][:2]
        for workspace in lulc_workspaces:
            if not roi_path:
                suff = workspace.replace("LULC", "")
                layer_name = (
                    "LULC_" + s_year + "_" + e_year + "_" + filename_prefix + suff
                )
            else:
                suff = workspace.replace("LULC", "")
                layer_name = f"LULC_{s_year}_{e_year}_{asset_suffix}{suff}"
            if is_gee_asset_exists(final_output_assetid_array_new[i]):
                if state and district and block:
                    layer_id = save_layer_info_to_db(
                        state,
                        district,
                        block,
                        layer_name=layer_name,
                        asset_id=final_output_assetid_array_new[i],
                        dataset_name=workspace,
                        misc={
                            "start_year": start_year,
                            "end_year": end_year,
                        },
                    )
                    layer_ids.append(layer_id)
                    print("saved info to db at the gee level...")
                make_asset_public(final_output_assetid_array_new[i])

    sync_lulc_to_gcs(
        final_output_filename_array_new,
        final_output_assetid_array_new,
        scale,
        force_regenerate=force_regenerate,
    )

    layer_at_geoserver = sync_lulc_to_geoserver(
        final_output_filename_array_new,
        state,
        district,
        block,
        layer_ids,
        asset_suffix,
    )

    return layer_at_geoserver


def sync_lulc_to_gcs(
    final_output_filename_array_new,
    final_output_assetid_array_new,
    scale,
    force_regenerate=False,
):
    task_ids = []
    for i in range(0, len(final_output_assetid_array_new)):
        make_asset_public(final_output_assetid_array_new[i])
        image = ee.Image(final_output_assetid_array_new[i])
        prefix, s_year, e_year = _parse_lulc_filename_for_years(
            final_output_filename_array_new[i]
        )
        layer_name = "LULC_" + s_year + "_" + e_year + "_" + prefix
        if force_regenerate:
            delete_gcs_raster_files(layer_name)
        if force_regenerate or not gcs_file_exists(layer_name):
            task_ids.append(sync_raster_to_gcs(image, scale, layer_name))

    task_id_list = check_task_status(task_ids)
    print("task_ids sync to gcs ", task_id_list)


def sync_lulc_to_geoserver(
    final_output_filename_array_new,
    state_name=None,
    district_name=None,
    block_name=None,
    layer_ids=None,
    asset_suffix=None,
):
    print("Syncing lulc to geoserver")
    lulc_workspaces = ["LULC_level_1", "LULC_level_2", "LULC_level_3"]
    layer_at_geoserver = False
    for i in range(0, len(final_output_filename_array_new)):
        prefix, s_year, e_year = _parse_lulc_filename_for_years(
            final_output_filename_array_new[i]
        )
        gcs_file_name = "LULC_" + s_year + "_" + e_year + "_" + prefix
        print("Syncing " + gcs_file_name + " to geoserver")
        for workspace in lulc_workspaces:
            suff = workspace.replace("LULC", "")
            style = workspace.lower() + "_style"
            if block_name:
                layer_name = (
                    "LULC_"
                    + s_year
                    + "_"
                    + e_year
                    + "_"
                    + valid_gee_text(district_name.lower())
                    + "_"
                    + valid_gee_text(block_name.lower())
                    + suff
                )
            else:
                layer_name = f"LULC_{s_year}_{e_year}_{asset_suffix}_{suff}"

            res = sync_raster_gcs_to_geoserver(
                workspace, gcs_file_name, layer_name, style
            )
            if res and layer_ids:
                update_layer_sync_status(layer_id=layer_ids[i], sync_to_geoserver=True)
                print("STAC: Parsed start year", s_year)

                if workspace == "LULC_level_3":
                    start_year_STAC = "20" + str(
                        s_year
                    )  # TODO: these are temp fixes, based on current implementations of the pipelines

                    if state_name and block_name and district_name:
                        layer_STAC_generated = (
                            generate_STAC_layerwise.generate_raster_stac(
                                state=state_name,
                                district=district_name,
                                block=block_name,
                                layer_name="land_use_land_cover_raster",
                                start_year=str(start_year_STAC),
                            )
                        )
                        update_layer_sync_status(
                            layer_id=layer_ids[i],
                            is_stac_specs_generated=layer_STAC_generated,
                        )
                print("geoserver flag is updated")
                layer_at_geoserver = True
    return layer_at_geoserver
