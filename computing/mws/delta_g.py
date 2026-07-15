import ee
import datetime
from computing.mws.utils import (
    hydrology_period_columns,
    hydrology_period_end,
    hydrology_period_label,
    parse_hydrology_period_start,
)
from computing.utils import get_layer_object
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    get_gee_dir_path,
    is_gee_asset_exists,
    load_gee_asset,
    export_vector_asset_to_gee,
    check_task_status,
    merge_fc_into_existing_fc,)


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
        layer_obj = None
        try:
            layer_name = (
                "deltaG_well_depth_" if is_annual else "deltaG_fortnight_"
            ) + asset_suffix
            layer_obj = get_layer_object(
                asset_folder_list[0],
                asset_folder_list[1],
                asset_folder_list[2],
                layer_name=layer_name,
                dataset_name="Hydrology",
            )
        except Exception as e:
            print(
                "layer not found for deltaG. So, reading the column name from asset_id"
            )

        # existing_end_date = get_last_date(asset_id, is_annual, layer_obj)

        if layer_obj:
            existing_end_date = layer_obj.misc["end_date"]
            existing_end_date = datetime.datetime.strptime(
                existing_end_date, "%Y-%m-%d"
            )
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
            last_date = str(existing_end_date.date())

            if existing_end_date.year < end_date.year:
                new_start_date = existing_end_date
                new_start_date = new_start_date.strftime("%Y-%m-%d")
                end_date = end_date.strftime("%Y-%m-%d")
                new_asset_id = f"{asset_id}_{new_start_date}_{end_date}"
                new_description = f"{description}_{new_start_date}_{end_date}"

                if not is_gee_asset_exists(new_asset_id):
                    task_id, new_asset_id, last_date = _generate_data(
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

                # Check if data for new year is generated, if yes then merge it in existing asset
                if is_gee_asset_exists(new_asset_id):
                    merge_fc_into_existing_fc(asset_id, description, new_asset_id)
            return None, asset_id, last_date
        else:
            ee.data.deleteAsset(asset_id)

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
    prec = load_gee_asset(
        asset_path + "Prec_" + ("annual_" if is_annual else "fortnight_") + asset_suffix
    )  # Precipitation feature collection

    runoff = load_gee_asset(
        asset_path
        + "Runoff_"
        + ("annual_" if is_annual else "fortnight_")
        + asset_suffix
    )  # RO feature collection

    et = load_gee_asset(
        asset_path + "ET_" + ("annual_" if is_annual else "fortnight_") + asset_suffix
    )  # et feature collection

    col_names = hydrology_period_columns(prec.first().propertyNames().getInfo())

    if start_date in col_names and col_names[0] != start_date:
        col_names = col_names[col_names.index(start_date) :]

    keys = ["Precipitation", "RunOff", "ET", "DeltaG"]

    for col_date in col_names:

        def get_delta_g(feat):
            uid = feat.get("uid")
            p = ee.Feature(prec.filter(ee.Filter.eq("uid", uid)).first())
            q = ee.Feature(runoff.filter(ee.Filter.eq("uid", uid)).first())
            e = ee.Feature(et.filter(ee.Filter.eq("uid", uid)).first())

            p = ee.Number(p.get(col_date))
            q = ee.Number(q.get(col_date))
            e = ee.Number(e.get(col_date))
            g = p.subtract(q).subtract(e)
            values = [p, q, e, g]
            d = ee.Dictionary.fromLists(keys, values)
            g_col_date = hydrology_period_label(col_date, is_annual)
            feat = feat.set(ee.String(g_col_date), ee.String.encodeJSON(d))
            return feat

        roi = roi.map(get_delta_g)
        start_date = col_date

    period_start = parse_hydrology_period_start(start_date)
    last_date = hydrology_period_end(period_start, is_annual)

    # Export feature collection to GEE
    task_id = export_vector_asset_to_gee(roi, description, asset_id)
    return task_id, asset_id, str(last_date.date())
