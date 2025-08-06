import ee
import datetime

from dateutil.relativedelta import relativedelta

from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    get_gee_dir_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
)


def precipitation(
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type=None,
    start_year=None,
    end_year=None,
    is_annual=False,
):

    description = ("Prec_annual_" if is_annual else "Prec_fortnight_") + asset_suffix

    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )

    start_date = f"{start_year}-07-01"
    end_date = f"{end_year}-06-30"

    if is_gee_asset_exists(asset_id):
        print("Asset exists")
        roi = ee.FeatureCollection(asset_id)
        col_names = roi.first().propertyNames().getInfo()
        filtered_col = [col for col in col_names if col.startswith("20")]
        filtered_col.sort()

        start_year = int(filtered_col[-1].split("-")[0]) + 1
        start_date = f"{start_year-1}-07-01"

        if (is_annual and start_year < end_year) or (
            not is_annual and start_date < end_date
        ):
            print("Need to merge")
            pass
        else:
            print("Full data")
            return None, asset_id

    size = ee.Number(roi.size())
    size1 = size.subtract(ee.Number(1))
    f_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    fn_index = 0
    print("f_start_date", f_start_date)
    print("end_date", end_date)
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

    # Export feature collection to GEE
    task_id = export_vector_asset_to_gee(roi, description, asset_id)
    return task_id, asset_id
