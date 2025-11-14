import ee
import datetime
import json
import os

from computing.utils import get_layer_object
from utilities.constants import GEE_PATHS, MERGE_MWS_PATH
from utilities.gee_utils import (
    get_gee_dir_path,
    is_gee_asset_exists,
    upload_shp_to_gee,
    check_task_status,
)
import geopandas as gpd


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
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    if is_gee_asset_exists(asset_id):
        layer_obj = None
        try:
            layer_obj = get_layer_object(
                asset_folder_list[0],
                asset_folder_list[1],
                asset_folder_list[2],
                layer_name=layer_name,
                dataset_name="Hydrology",
            )
        except Exception as e:
            print("layer not found. So, reading the column name from asset_id.")

        if layer_obj:
            db_end_date = layer_obj.misc["end_date"]
        else:
            roi = ee.FeatureCollection(asset_id)
            col_names = roi.first().propertyNames().getInfo()
            filtered_col = [col for col in col_names if col.startswith("20")]
            filtered_col.sort()
            db_end_date = filtered_col[-1]  # .split("-")[0].split("_")[-1]

        db_end_date = datetime.datetime.strptime(db_end_date, "%Y-%m-%d")
        if db_end_date.year < end_date.year:
            ee.data.deleteAsset(asset_id)
        else:
            return asset_id

    fc = ee.FeatureCollection(delta_g_asset_id)
    deltaG_col_names = fc.first().propertyNames().getInfo()
    deltaG_col_names = [col for col in deltaG_col_names if col.startswith("20")]
    deltaG_col_names.sort()
    fc = fc.getInfo()
    features = fc["features"]

    for f in features:
        properties = f["properties"]
        l_col_date = None

        for col_date in deltaG_col_names:
            curr_prop = json.loads(properties[col_date])
            prev_g = 0
            if l_col_date:
                last_prop = json.loads(properties[l_col_date])
                prev_g = last_prop.get("G", 0)

            curr_prop["G"] = curr_prop["DeltaG"] + prev_g

            # Store the full dict as a string
            properties[col_date] = json.dumps(curr_prop)
            l_col_date = col_date

        f["properties"] = properties

    try:
        # Rewrap into ee.FeatureCollection with valid geometry
        ee_features = [
            ee.Feature(ee.Geometry(f["geometry"]), f["properties"]) for f in features
        ]

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
