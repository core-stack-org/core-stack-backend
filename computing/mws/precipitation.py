import ee
import datetime

from dateutil.relativedelta import relativedelta
from utilities.gee_utils import valid_gee_text, get_gee_asset_path, is_gee_asset_exists


def precipitation(state, district, block, start_date, end_date, is_annual):
    description = (
        ("Prec_annual_" if is_annual else "Prec_fortnight_")
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )

    asset_id = get_gee_asset_path(state, district, block) + description
    if is_gee_asset_exists(asset_id):
        return

    roi = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    size = ee.Number(roi.size())
    size1 = size.subtract(ee.Number(1))
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
        dataset = ee.ImageCollection("JAXA/GPM_L3/GSMaP/v6/operational").filter(
            ee.Filter.date(f_start_date, f_end_date)
        )
        total = dataset.reduce(ee.Reducer.sum())
        total = total.clip(roi)

        stats2 = total.reduceRegions(
            reducer=ee.Reducer.mean(),
            collection=roi,
            scale=11132,
        )
        statsl = ee.List(stats2.toList(size))

        def res(m):
            feat = ee.Feature(statsl.get(m))
            uid = feat.get("uid")
            f = ee.Feature(roi.filter(ee.Filter.eq("uid", uid)).first())
            val = feat.get("hourlyPrecipRate_sum")
            val = ee.Algorithms.If(val, val, 0)
            return f.set(start_date, val)

        mws = ee.List.sequence(0, size1)
        l = mws.map(res)
        roi = ee.FeatureCollection(l)
        f_start_date = f_end_date
        start_date = str(f_start_date.date())

    try:
        task = ee.batch.Export.table.toAsset(
            **{
                "collection": roi,
                "description": description,
                "assetId": asset_id,
            }
        )

        task.start()
        print("Successfully started the task fortnight_precipitation", task.status())
        return task.status()["id"]
    except Exception as e:
        print(f"Error occurred in running precipitation task: {e}")
