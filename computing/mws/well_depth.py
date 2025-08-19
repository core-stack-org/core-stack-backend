import ee
import datetime

from dateutil.relativedelta import relativedelta

from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    get_gee_dir_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
)
from computing.models import Layer, Dataset


def well_depth(
    asset_suffix=None,
    asset_folder_list=None,
    app_type=None,
    start_date=None,
    end_date=None,
):
    print("Inside well depth script")
    description = "well_depth_annual_" + asset_suffix
    asset_path = get_gee_dir_path(
        asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    asset_id = asset_path + description

    if is_gee_asset_exists(asset_id):
        print("Well depth asset already exists")
        dataset = Dataset.objects.get(name="Hydrology")
        layer_obj = Layer.objects.get(
            dataset=dataset,
            layer_name=f"deltaG_well_depth_{asset_suffix}",
        )
        db_end_date = layer_obj.misc["end_year"]
        db_end_date = f"{db_end_date}-06-30"
        db_end_date = datetime.datetime.strptime(db_end_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

        if db_end_date < end_date:
            end_date = end_date.strftime("%Y-%m-%d")
            ee.data.deleteAsset(asset_id)
        else:
            return None, asset_id

    return _generate_data(
        asset_id, asset_path, description, asset_suffix, start_date, end_date
    )


def _generate_data(
    asset_id, asset_path, description, asset_suffix, start_date, end_date
):
    principal_aquifers = ee.FeatureCollection(
        "projects/ee-anz208490/assets/principalAquifer"
    )
    slopes = ee.FeatureCollection(
        asset_path + "filtered_delta_g_annual_" + asset_suffix + "_uid"
    )
    yeild__ = ee.List(principal_aquifers.aggregate_array("yeild__"))
    distinct = yeild__.distinct()
    diction = ee.Dictionary(
        {
            "": "NA",
            "-": "NA",
            "Upto 2%": 0.02,
            "1-2%": 0.02,
            "Upto 1.5%": 0.015,
            "Upto 3%": 0.03,
            "Upto 2.5%": 0.025,
            "6 - 8%": 0.08,
            "1-1.5%": 0.015,
            "2-3%": 0.03,
            "Upto 4%": 0.04,
            "Upto 5%": 0.05,
            "Upto -3.5%": 0.035,
            "Upto 3 %": 0.03,
            "Upto 9%": 0.09,
            "1-2.5": 0.025,
            "Upto 1.2%": 0.012,
            "Upto 5-2%": 0.05,
            "Upto 1%": 0.01,
            "Up to 1.5%": 0.015,
            "Upto 8%": 0.08,
            "Upto 6%": 0.06,
            "0.08": 0.08,
            "8 - 16%": 0.16,
            "Not Explored": "NA",
            "8 - 15%": 0.15,
            "6 - 10%": 0.1,
            "6 - 15%": 0.15,
            "8 - 20%": 0.2,
            "8 - 10%": 0.1,
            "6 - 12%": 0.12,
            "6 - 16%": 0.16,
            "8 - 12%": 0.12,
            "8 - 18%": 0.18,
            "Upto 3.5%": 0.035,
            "Upto 15%": 0.15,
            "1.5-2%": 0.02,
        }
    )

    def fun(aquifer):
        yeild = ee.String(aquifer.get("yeild__"))
        return aquifer.set("y_value", diction.get(yeild))

    mapped = principal_aquifers.map(fun)
    mapped = ee.FeatureCollection(mapped)
    aquifers_with_yield_value = mapped.filter(ee.Filter.neq("y_value", "NA"))

    def fun2(mws):
        filtered_aquifers = aquifers_with_yield_value.filterBounds(mws.geometry())

        def fun3(aquifer):
            polygon_intersection = aquifer.intersection(
                mws.geometry(), ee.ErrorMargin(1)
            )

            polygon_area = polygon_intersection.area(ee.ErrorMargin(1))

            mws_area = mws.geometry().area(ee.ErrorMargin(1))

            fraction = polygon_area.divide(mws_area)
            weighted_yeild = fraction.multiply(aquifer.get("y_value"))
            return aquifer.set("weighted_yeild", weighted_yeild)

        mapped_filtered_aquifers = filtered_aquifers.map(fun3)
        weighted_avg_yeild = mapped_filtered_aquifers.aggregate_sum("weighted_yeild")
        return mws.set("weighted_avg_yeild", weighted_avg_yeild)

    shape = slopes.map(fun2)
    keys = ["Precipitation", "RunOff", "ET", "DeltaG", "WellDepth"]
    # year of interest
    f_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    while f_start_date <= end_date:
        f_end_date = f_start_date + relativedelta(years=1)

        def res(n):
            col_date = str(f_start_date.year) + "_" + str(f_start_date.year + 1)
            d = ee.Dictionary(ee.String(n.get(str(col_date))).decodeJSON())
            p = d.get("Precipitation")
            q = d.get("RunOff")
            et = d.get("ET")
            dg = d.get("DeltaG")
            y = n.get("weighted_avg_yeild")

            wd = ee.Number(dg).divide(ee.Number(y).multiply(ee.Number(1000)))
            values = [p, q, et, dg, wd]
            d = ee.Dictionary.fromLists(keys, values)
            n = n.set(ee.String(str(col_date)), ee.String.encodeJSON(d))

            return n

        shape = shape.map(res)
        f_start_date = f_end_date
        start_date = f_start_date
    # Export feature collection to GEE
    task_id = export_vector_asset_to_gee(shape, description, asset_id)
    return task_id, asset_id
