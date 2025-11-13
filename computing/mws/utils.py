import ee
import datetime


def get_last_date(asset_id, is_annual, layer_obj):
    if layer_obj:
        existing_end_date = layer_obj.misc["end_date"]
        existing_end_date = datetime.datetime.strptime(existing_end_date, "%Y-%m-%d")
    else:
        fc = ee.FeatureCollection(asset_id)
        col_names = fc.first().propertyNames().getInfo()
        filtered_col = [col for col in col_names if col.startswith("20")]
        filtered_col.sort()
        existing_end_date = datetime.datetime.strptime(filtered_col[-1], "%Y-%m-%d")

        if is_annual:
            existing_end_date = existing_end_date + datetime.timedelta(days=364)
        else:
            existing_end_date = existing_end_date + datetime.timedelta(days=14)

    return existing_end_date
