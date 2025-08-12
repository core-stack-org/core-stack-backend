import ee
import datetime
from dateutil.relativedelta import relativedelta
import json

from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    get_gee_dir_path,
    export_vector_asset_to_gee,
    is_gee_asset_exists,
)


def calculate_g(
    delta_g_asset_id,
    asset_folder_list,
    layer_name,
    app_type,
    start_date,
    end_date,
    is_annual,
):
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + layer_name
    )

    db_end_date = "2023-06-30"
    if is_gee_asset_exists(asset_id):
        if db_end_date < end_date:
            ee.data.deleteAsset(asset_id)
        else:
            return None, asset_id

    fc = ee.FeatureCollection(delta_g_asset_id).getInfo()
    features = fc["features"]
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    for f in features:
        properties = f["properties"]
        n_start_date = start_date
        f_start_date = datetime.datetime.strptime(n_start_date, "%Y-%m-%d")
        l_start_date = None
        fn_index = 0

        while f_start_date <= end_date:
            if is_annual:
                f_end_date = f_start_date + relativedelta(years=1)
            else:
                if fn_index == 25:
                    f_end_date = f_start_date + relativedelta(months=1, day=1)
                    fn_index = 0
                else:
                    f_end_date = f_start_date + datetime.timedelta(days=14)
                    fn_index += 1

            col_date = (
                str(f_start_date.year) + "_" + str(f_start_date.year + 1)
                if is_annual
                else f_start_date.strftime("%Y-%m-%d")
            )

            curr_prop = json.loads(properties[col_date])
            prev_g = 0
            if l_start_date:
                l_col_date = (
                    str(l_start_date.year) + "_" + str(l_start_date.year + 1)
                    if is_annual
                    else l_start_date.strftime("%Y-%m-%d")
                )
                last_prop = json.loads(properties[l_col_date])
                prev_g = last_prop.get("G", 0)

            curr_prop["G"] = curr_prop["DeltaG"] + prev_g

            # Store the full dict as a string
            properties[col_date] = json.dumps(curr_prop)

            l_start_date = f_start_date
            f_start_date = f_end_date
            n_start_date = f_start_date.strftime("%Y-%m-%d")

        f["properties"] = properties

    # Rewrap into ee.FeatureCollection with valid geometry
    ee_features = [
        ee.Feature(ee.Geometry(f["geometry"]), f["properties"]) for f in features
    ]
    fc = ee.FeatureCollection(ee_features)
    task_id = export_vector_asset_to_gee(fc, layer_name, asset_id)
    return asset_id, task_id
