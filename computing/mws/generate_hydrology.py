import ee
from nrm_app.celery import app
from utilities.constants import GEE_PATHS
from .precipitation import precipitation
from .run_off import run_off
from .evapotranspiration import evapotranspiration
from .delta_g import delta_g
from .net_value import net_value
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_dir_path,
    make_asset_public,
    is_gee_asset_exists,
)
from .well_depth import well_depth
from .calculateG import calculate_g
import sys
from computing.utils import (
    save_layer_info_to_db,
    sync_layer_to_geoserver,
    update_layer_sync_status,
)


@app.task(bind=True)
def generate_hydrology(
    self,
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    start_year=None,
    end_year=None,
    is_annual=False,
    gee_account_id=None,
):
    ee_initialize(gee_account_id)

    sys.setrecursionlimit(6000)

    end_year = end_year if is_annual else end_year + 1
    task_list = []
    start_date = f"{start_year}-07-01"
    end_date = f"{end_year}-06-30"

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

    layer_name_suffix = "annual" if is_annual else "fortnight"
    ppt_task_id, ppt_asset_id, ppt_last_date = precipitation(
        roi=roi,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        app_type=app_type,
        start_date=start_date,
        end_date=end_date,
        is_annual=is_annual,
    )
    if ppt_task_id:
        task_list.append(ppt_task_id)

    et_task_id, et_asset_id, et_last_date = evapotranspiration(
        roi=roi,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        app_type=app_type,
        start_year=start_year,
        end_year=end_year,
        is_annual=is_annual,
    )
    if et_task_id:
        task_list.append(et_task_id)

    ro_task_id, ro_asset_id, ro_last_date = run_off(
        gee_account_id=gee_account_id,
        roi=roi,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        app_type=app_type,
        start_date=start_date,
        end_date=end_date,
        is_annual=is_annual,
    )
    if ro_task_id:
        task_list.append(ro_task_id)

    task_id_list = check_task_status(task_list) if len(task_list) > 0 else []
    print("task_id_list", task_id_list)

    if state and district and block:
        if is_gee_asset_exists(ppt_asset_id):
            make_asset_public(ppt_asset_id)
            save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=f"{asset_suffix}_precipitation_{layer_name_suffix}",
                asset_id=ppt_asset_id,
                dataset_name="Hydrology Precipitation",
                algorithm_version="1.1",
                misc={"start_date": start_date, "end_date": ppt_last_date},
            )
            print("save Precipitation info at the gee level...")

        if is_gee_asset_exists(et_asset_id):
            make_asset_public(et_asset_id)
            save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=f"{asset_suffix}_evapotranspiration_{layer_name_suffix}",
                asset_id=et_asset_id,
                dataset_name="Hydrology Evapotranspiration",
                algorithm="fldas",
                algorithm_version="1.1",
                misc={"start_date": start_date, "end_date": et_last_date},
            )
            print("save Evapotranspiration info at the gee level...")

        if is_gee_asset_exists(ro_asset_id):
            make_asset_public(ro_asset_id)
            save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=f"{asset_suffix}_run_off_{layer_name_suffix}",
                asset_id=ro_asset_id,
                dataset_name="Hydrology Run Off",
                algorithm_version="1.1",
                misc={"start_date": start_date, "end_date": ro_last_date},
            )
            print("save Run Off info at the gee level...")

    dg_task_id, delta_g_asset_id, g_last_date = delta_g(
        roi=roi,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        app_type=app_type,
        start_date=start_date,
        end_date=end_date,
        is_annual=is_annual,
    )

    task_id_list = check_task_status([dg_task_id]) if dg_task_id else []
    print("dg task_id_list", task_id_list)

    layer_name = "deltaG_fortnight_" + asset_suffix

    if is_annual:
        wd_task_id, wd_asset_id, g_last_date = well_depth(
            asset_suffix=asset_suffix,
            asset_folder_list=asset_folder_list,
            app_type=app_type,
            start_date=start_date,
            end_date=end_date,
        )

        task_id_list = check_task_status([wd_task_id]) if wd_task_id else []
        print("wd task_id_list", task_id_list)

        wd_task_id, delta_g_asset_id = net_value(
            asset_suffix=asset_suffix,
            asset_folder_list=asset_folder_list,
            app_type=app_type,
            start_date=start_date,
            end_date=end_date,
        )
        task_id_list = check_task_status([wd_task_id]) if wd_task_id else []
        print("wdn task_id_list", task_id_list)

        layer_name = "deltaG_well_depth_" + asset_suffix

    asset_id = calculate_g(
        delta_g_asset_id,
        asset_folder_list,
        asset_suffix,
        app_type,
        start_date,
        end_date,
        is_annual,
        gee_account_id,
    )
    layer_at_geoserver = False
    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Hydrology",
            algorithm_version="1.1",
            misc={
                "start_date": start_date,
                "end_date": g_last_date,
            },
        )

        fc = ee.FeatureCollection(asset_id).getInfo()
        fc = {"features": fc["features"], "type": fc["type"]}
        res = sync_layer_to_geoserver(asset_suffix, fc, layer_name, "mws_layers")
        print(res)

        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
            layer_at_geoserver = True
    return layer_at_geoserver
