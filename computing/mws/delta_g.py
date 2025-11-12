import ee
import datetime
from computing.utils import get_layer_object
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    get_gee_dir_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    check_task_status,
    merge_fc_into_existing_fc,
)


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
            last_date = str(end_date.date())

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

    col_names = prec.first().propertyNames().getInfo()
    col_names = [col for col in col_names if col.startswith("20")]
    col_names.sort()

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
            g_col_date = datetime.datetime.strptime(col_date, "%Y-%m-%d")
            g_col_date = (
                str(g_col_date.year) + "_" + str(g_col_date.year + 1)
                if is_annual
                else col_date
            )
            feat = feat.set(ee.String(g_col_date), ee.String.encodeJSON(d))
            return feat

        roi = roi.map(get_delta_g)
        start_date = col_date

    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    if is_annual:
        last_date = start_date + datetime.timedelta(days=364)
    else:
        last_date = start_date + datetime.timedelta(days=14)

    # Export feature collection to GEE
    task_id = export_vector_asset_to_gee(roi, description, asset_id)
    return task_id, asset_id, last_date
