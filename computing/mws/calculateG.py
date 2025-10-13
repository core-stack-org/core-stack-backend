import ee
import datetime
from dateutil.relativedelta import relativedelta
import json
import os
from utilities.constants import GEE_PATHS, MERGE_MWS_PATH
from utilities.gee_utils import (
    get_gee_dir_path,
    is_gee_asset_exists,
    upload_shp_to_gee,
    check_task_status,
)
import geopandas as gpd
from computing.models import Layer, Dataset


def calculate_g(
    delta_g_asset_id,
    asset_folder_list,
    asset_suffix,
    app_type,
    start_date,
    end_date,
    is_annual,
    gee_account_id,
):
    layer_name = (
        "deltaG_well_depth_" if is_annual else "deltaG_fortnight_"
    ) + asset_suffix
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + layer_name
    )

    if is_gee_asset_exists(asset_id):
        dataset = Dataset.objects.get(name="Hydrology")
        layer_obj = None
        try:
            layer_obj = Layer.objects.get(
                dataset=dataset,
                layer_name=layer_name,
            )
        except Exception as e:
            print("layer not found. So, reading the column name from asset_id.")

        if layer_obj:
            db_end_date = layer_obj.misc["end_year"]
        else:
            roi = ee.FeatureCollection(asset_id)
            col_names = roi.first().propertyNames().getInfo()
            filtered_col = [col for col in col_names if col.startswith("20")]
            filtered_col.sort()
            db_end_date = filtered_col[-1].split("-")[0].split("_")[-1]

        db_end_date = f"{db_end_date}-06-30"
        db_end_date = datetime.datetime.strptime(db_end_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

        if db_end_date < end_date:
            end_date = end_date.strftime("%Y-%m-%d")
            ee.data.deleteAsset(asset_id)
        else:
            return asset_id

    fc = ee.FeatureCollection(delta_g_asset_id).getInfo()
    features = fc["features"]
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    for f in features:
        properties = f["properties"]
        n_start_date = start_date
        f_start_date = datetime.datetime.strptime(n_start_date, "%Y-%m-%d")
        l_start_date = None
        fn_index = 0

        while f_start_date <= end_date:
            if is_annual:
                f_end_date = f_start_date + relativedelta(years=1)
            else:
                if fn_index == 25:
                    f_end_date = f_start_date + relativedelta(months=1, day=1)
                    fn_index = 0
                else:
                    f_end_date = f_start_date + datetime.timedelta(days=14)
                    fn_index += 1

            col_date = (
                str(f_start_date.year) + "_" + str(f_start_date.year + 1)
                if is_annual
                else f_start_date.strftime("%Y-%m-%d")
            )

            curr_prop = json.loads(properties[col_date])
            prev_g = 0
            if l_start_date:
                l_col_date = (
                    str(l_start_date.year) + "_" + str(l_start_date.year + 1)
                    if is_annual
                    else l_start_date.strftime("%Y-%m-%d")
                )
                last_prop = json.loads(properties[l_col_date])
                prev_g = last_prop.get("G", 0)

            curr_prop["G"] = curr_prop["DeltaG"] + prev_g

            # Store the full dict as a string
            properties[col_date] = json.dumps(curr_prop)

            l_start_date = f_start_date
            f_start_date = f_end_date
            n_start_date = f_start_date.strftime("%Y-%m-%d")

        f["properties"] = properties

    try:
        # Rewrap into ee.FeatureCollection with valid geometry
        ee_features = [
            ee.Feature(ee.Geometry(f["geometry"]), f["properties"]) for f in features
        ]
        # task_id = export_vector_asset_to_gee(fc, layer_name, asset_id)
        task = ee.batch.Export.table.toAsset(
            collection=ee.FeatureCollection(ee_features),
            description=layer_name,
            assetId=asset_id,
        )

        task.start()
        if task.status()["id"]:
            task_id_list = check_task_status([task.status()["id"]])
            print("task_id_list", task_id_list)
    except Exception as e:
        print("Error in exporting deltaG:", e)

        if "Request payload size exceeds the limit" in str(e):
            print("Uploading asset with shp file using CLI command.")

            fc["features"] = features
            gdf = gpd.GeoDataFrame.from_features(fc)
            gdf.crs = "EPSG:4326"

            path = os.path.join(MERGE_MWS_PATH, asset_folder_list[0])
            if not os.path.exists(path):
                os.mkdir(path)

            path = os.path.join(str(path), asset_suffix)
            if not os.path.exists(path):
                os.mkdir(path)

            path = f"{path}/{layer_name}.shp"
            gdf.to_file(path, driver="ESRI Shapefile", encoding="UTF-8")
            print("path", path)
            upload_shp_to_gee(path, layer_name, asset_id, gee_account_id)

    return asset_id
