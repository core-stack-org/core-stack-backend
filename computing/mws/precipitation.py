import ee
import datetime

from dateutil.relativedelta import relativedelta

from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    get_gee_dir_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    check_task_status,
    merge_fc_into_existing_fc,
)
from computing.models import Layer, Dataset


def precipitation(
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type=None,
    start_date=None,
    end_date=None,
    is_annual=False,
):

    description = ("Prec_annual_" if is_annual else "Prec_fortnight_") + asset_suffix

    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )
    if is_gee_asset_exists(asset_id):
        layer_obj = None
        try:
            layer_name_suffix = "annual" if is_annual else "fortnight"
            dataset = Dataset.objects.get(name="Hydrology Precipitation")
            layer_obj = Layer.objects.get(
                dataset=dataset,
                layer_name=f"{asset_suffix}_precipitation_{layer_name_suffix}",
            )
        except Exception as e:
            print(
                f"layer not found for precipitation. So, reading the column name from asset_id."
            )
        if layer_obj:
            existing_end_date = layer_obj.misc["end_year"]
        else:
            fc = ee.FeatureCollection(asset_id)
            col_names = fc.first().propertyNames().getInfo()
            filtered_col = [col for col in col_names if col.startswith("20")]
            filtered_col.sort()
            existing_end_date = (int(filtered_col[-1].split("-")[0]) + 1 if is_annual else filtered_col[-1].split("-")[0])
        existing_end_date = f"{existing_end_date}-06-30"
        existing_end_date = datetime.datetime.strptime(existing_end_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        print("existing_end_date", existing_end_date)
        print("end_date", end_date)
        if existing_end_date < end_date:
            new_start_date = existing_end_date + relativedelta(months=1, day=1)
            new_start_date = new_start_date.strftime("%Y-%m-%d")
            end_date = end_date.strftime("%Y-%m-%d")
            new_asset_id = f"{asset_id}_{new_start_date}_{end_date}"
            new_description = f"{description}_{new_start_date}_{end_date}"
            if not is_gee_asset_exists(new_asset_id):
                task_id, new_asset_id = _generate_data(
                    roi,
                    new_asset_id,
                    new_description,
                    new_start_date,
                    end_date,
                    is_annual,
                )
                check_task_status([task_id])
                print("Prec new year data generated.")

            # Check if data for new year is generated, if yes then merge it in existing asset
            if is_gee_asset_exists(new_asset_id):
                merge_fc_into_existing_fc(asset_id, description, new_asset_id)
        return None, asset_id
    else:
        return _generate_data(
            roi, asset_id, description, start_date, end_date, is_annual
        )


def _generate_data(roi, asset_id, description, start_date, end_date, is_annual):
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
    # Export feature collection to GEE
    task_id = export_vector_asset_to_gee(roi, description, asset_id)
    return task_id, asset_id
