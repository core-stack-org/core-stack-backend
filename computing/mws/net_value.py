import ee
import datetime
from dateutil.relativedelta import relativedelta
from utilities.gee_utils import (
    get_gee_dir_path,
    is_gee_asset_exists,
    export_vector_to_gee,
)


def net_value(
    asset_suffix=None,
    asset_folder_list=None,
    start_date=None,
    end_date=None,
):
    description = "well_depth_net_value_" + asset_suffix
    asset_id = get_gee_dir_path(asset_folder_list) + description

    if is_gee_asset_exists(asset_id):
        return None, asset_id

    well_depth_fc = (
        get_gee_dir_path(asset_folder_list) + "well_depth_annual_" + asset_suffix
    )
    shape = ee.FeatureCollection(well_depth_fc)

    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    while start_date.year + 4 < end_date.year:
        f_start_date = start_date
        curr_date = f_start_date + relativedelta(years=5)
        years = []
        f_year = f_start_date.year
        while f_year < curr_date.year:
            year = str(f_year) + "_" + str(f_year + 1)
            years.append(year)
            f_year += 1
        s = "Net" + str(f_start_date.year) + "_" + str(curr_date.year)[-2:]

        def feat(f):
            # lr = ee.List([])
            base = ee.Number(0)
            for i in range(len(years)):
                g = ee.Dictionary(ee.String(f.get(years[i])).decodeJSON())
                base = base.add(ee.Number(g.get("WellDepth")))
            f = f.set(s, base)
            return f

        start_date = start_date + relativedelta(years=1)
        shape = shape.map(feat)

    # Export feature collection to GEE
    task_id = export_vector_to_gee(shape, description, asset_id)
    return task_id, asset_id
