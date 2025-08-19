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


def delta_g(
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type=None,
    start_date=None,
    end_date=None,
    is_annual=False,
):
    description = (
        "filtered_delta_g_"
        + ("annual_" if is_annual else "fortnight_")
        + asset_suffix
        + "_uid"
    )

    asset_path = get_gee_dir_path(
        asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    asset_id = asset_path + description

    if is_gee_asset_exists(asset_id):
        print("DeltaG asset already exists")
        dataset = Dataset.objects.get(name="Hydrology")
        # TODO instead of here, pass in arguments from main file
        layer_name = (
            "deltaG_well_depth_" if is_annual else "deltaG_fortnight_"
        ) + asset_suffix

        layer_obj = Layer.objects.get(
            dataset=dataset,
            layer_name=layer_name,
        )
        db_end_date = f"{layer_obj.misc["end_year"]}-06-30"
        if db_end_date < end_date:
            new_start_date = datetime.datetime.strptime(db_end_date, "%Y-%m-%d")
            new_start_date = new_start_date + relativedelta(months=1, day=1)
            new_start_date = new_start_date.strftime("%Y-%m-%d")

            new_asset_id = f"{asset_id}_{new_start_date}_{end_date}"
            new_description = f"{description}_{new_start_date}_{end_date}"
            if not is_gee_asset_exists(new_asset_id):
                task_id, new_asset_id = _generate_data(
                    roi,
                    new_asset_id,
                    asset_path,
                    asset_suffix,
                    new_description,
                    new_start_date,
                    end_date,
                    is_annual,
                )
                check_task_status([task_id])
                print("DeltaG new year data generated.")

            merge_fc_into_existing_fc(asset_id, description, new_asset_id)
        return None, asset_id

    return _generate_data(
        roi,
        asset_id,
        asset_path,
        asset_suffix,
        description,
        start_date,
        end_date,
        is_annual,
    )


def _generate_data(
    roi,
    asset_id,
    asset_path,
    asset_suffix,
    description,
    start_date,
    end_date,
    is_annual,
):
    prec = ee.FeatureCollection(
        asset_path + "Prec_" + ("annual_" if is_annual else "fortnight_") + asset_suffix
    )  # Precipitation feature collection
    runoff = ee.FeatureCollection(
        asset_path
        + "Runoff_"
        + ("annual_" if is_annual else "fortnight_")
        + asset_suffix
    )  # RO feature collection
    et = ee.FeatureCollection(
        asset_path + "ET_" + ("annual_" if is_annual else "fortnight_") + asset_suffix
    )  # et feature collection
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

        roi = roi.map(res)
        f_start_date = f_end_date
        start_date = str(f_start_date.date())
    # Export feature collection to GEE
    task_id = export_vector_asset_to_gee(roi, description, asset_id)
    return task_id, asset_id
