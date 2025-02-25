import ee
import datetime
from dateutil.relativedelta import relativedelta
from utilities.gee_utils import valid_gee_text, get_gee_asset_path, is_gee_asset_exists


def net_value(state, district, block, start_date, end_date):
    description = (
        "well_depth_net_value_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )
    asset_id = get_gee_asset_path(state, district, block) + description

    if is_gee_asset_exists(asset_id):
        return None, asset_id

    well_depth_fc = (
        get_gee_asset_path(state, district, block)
        + "well_depth_annual_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
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

    try:
        task = ee.batch.Export.table.toAsset(
            **{
                "collection": shape,
                "description": description,
                "assetId": asset_id,
                "scale": 30,
                "maxPixels": 1e13,
            }
        )
        task.start()
        print("Successfully started the task well_depth_net_value ", task.status())
        return task.status()["id"], asset_id
    except Exception as e:
        print(f"Error occurred in running well_depth_net_value task: {e}")
