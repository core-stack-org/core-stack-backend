import ee
import datetime

from dateutil.relativedelta import relativedelta
from utilities.gee_utils import valid_gee_text, get_gee_asset_path, is_gee_asset_exists


def delta_g(state, district, block, start_date, end_date, is_annual):
    description = (
        "filtered_delta_g_"
        + ("annual_" if is_annual else "fortnight_")
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )
    asset_id = get_gee_asset_path(state, district, block) + description

    if is_gee_asset_exists(asset_id):
        return None, asset_id

    prec = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "Prec_"
        + ("annual_" if is_annual else "fortnight_")
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )  # Prec feature collection
    runoff = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "Runoff_"
        + ("annual_" if is_annual else "fortnight_")
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )  # RO feature collection
    et = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "ET_"
        + ("annual_" if is_annual else "fortnight_")
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )  # et feature collection
    shape = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )  # mws feature collection

    keys = ["Precipitation", "RunOff", "ET", "DeltaG"]

    f_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
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

        def res(feat):
            uid = feat.get("uid")
            p = ee.Feature(prec.filter(ee.Filter.eq("uid", uid)).first())
            q = ee.Feature(runoff.filter(ee.Filter.eq("uid", uid)).first())
            e = ee.Feature(et.filter(ee.Filter.eq("uid", uid)).first())

            p = ee.Number(p.get(start_date))
            q = ee.Number(q.get(start_date))
            e = ee.Number(e.get(start_date))
            g = p.subtract(q).subtract(e)
            values = [p, q, e, g]
            d = ee.Dictionary.fromLists(keys, values)
            col_date = (
                str(f_start_date.year) + "_" + str(f_start_date.year + 1)
                if is_annual
                else start_date
            )
            feat = feat.set(ee.String(col_date), ee.String.encodeJSON(d))
            return feat

        shape = shape.map(res)
        f_start_date = f_end_date
        start_date = str(f_start_date.date())

    try:

        task = ee.batch.Export.table.toAsset(
            **{
                "collection": shape,
                "description": description,
                "assetId": asset_id,
            }
        )
        task.start()
        print("Successfully started the task deltaG", task.status())

        return task.status()["id"], asset_id
    except Exception as e:
        print(f"Error occurred in running delta_G task: {e}")
