import ee
import datetime
from dateutil.relativedelta import relativedelta
from computing.utils import sync_layer_to_geoserver
from utilities.gee_utils import valid_gee_text
import json


def calculate_g(asset_id, layer_name, shp_folder, start_date, end_date, is_annual):
    fc = ee.FeatureCollection(asset_id).getInfo()
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
                    # Setting date to 1st July if index==25
                    f_end_date = f_start_date + relativedelta(months=1, day=1)
                    fn_index = 0
                else:
                    f_end_date = f_start_date + datetime.timedelta(days=14)
                    fn_index += 1
            col_date = (
                str(f_start_date.year) + "_" + str(f_start_date.year + 1)
                if is_annual
                else n_start_date
            )
            curr_prop = json.loads(properties[col_date])
            prev_g = 0
            if l_start_date:
                l_col_date = (
                    str(l_start_date.year) + "_" + str(l_start_date.year + 1)
                    if is_annual
                    else l_start_date.date()
                )
                last_prop = properties[str(l_col_date)]
                prev_g = last_prop["G"]
            curr_prop["G"] = curr_prop["DeltaG"] + prev_g
            properties[col_date] = curr_prop

            l_start_date = f_start_date
            f_start_date = f_end_date
            n_start_date = str(f_start_date.date())

        f["properties"] = properties
    fc["features"] = features
    res = sync_layer_to_geoserver(shp_folder, fc, layer_name, "mws_layers")
    print(res)
