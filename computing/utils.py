import os

import geopandas as gpd
import fiona

from utilities.gee_utils import (
    ee_initialize,
    sync_vector_to_gcs,
    check_task_status,
    get_geojson_from_gcs,
    is_gee_asset_exists,
    valid_gee_text,
    get_gee_asset_path,
)
from utilities.geoserver_utils import Geoserver
import shutil
from utilities.constants import (
    ADMIN_BOUNDARY_OUTPUT_DIR,
    SHAPEFILE_DIR,
    GEE_HELPER_PATH,
)
import ee
import json


def generate_shape_files(path):
    gdf = gpd.read_file(path + ".json")
    os.remove(path + ".json")

    gdf.to_file(
        path,
        driver="ESRI Shapefile",
    )
    return path


def convert_to_zip(dir_name):
    return shutil.make_archive(dir_name, "zip", dir_name + "/")


def push_shape_to_geoserver(shape_path_dir, store_name=None, workspace=None):
    geo = Geoserver()
    zip_path = convert_to_zip(shape_path_dir)
    response = geo.create_shp_datastore(
        path=zip_path, store_name=store_name, workspace=workspace
    )
    # if response["status_code"] in [200, 201, 202]:
    #     os.remove(zip_path)
    #     shutil.rmtree(shape_path_dir)
    return response["response_text"]


def kml_to_geojson(state_name, district_name, block_name, kml_path):
    fiona.drvsupport.supported_drivers["kml"] = (
        "rw"  # enable KML support which is disabled by default
    )
    fiona.drvsupport.supported_drivers["KML"] = (
        "rw"  # enable KML support which is disabled by default
    )
    gdf = gpd.read_file(kml_path)
    geometry_types = gdf.geometry.geometry.type.unique()
    state_dir = os.path.join(ADMIN_BOUNDARY_OUTPUT_DIR, state_name)

    for gtype in geometry_types:
        df = gdf.loc[gdf.geometry.geometry.type == gtype]
        path = os.path.join(state_dir, f"{district_name}_{block_name}_{gtype}")
        df.to_file(path + ".json", driver="GeoJSON")
        generate_shape_files(path)
        push_shape_to_geoserver(path, workspace="test_workspace")


def convert_kml_to_shapefile(kml_path, output_dir, shapefile_name):
    if not os.path.exists(output_dir + "/" + shapefile_name):
        os.makedirs(output_dir + "/" + shapefile_name)

    shapefile_path = os.path.join(
        output_dir + "/" + shapefile_name, shapefile_name + ".shp"
    )
    print("path path", shapefile_path)
    cmd = f"ogr2ogr -f 'ESRI Shapefile' {shapefile_path} {kml_path}"  # output.shp input.kml
    os.system(command=cmd)

    return output_dir + "/" + shapefile_name


def kml_to_shp(state_name, district_name, block_name, kml_path):
    shapefile_name = f"{district_name}_{block_name}"
    shapefile_layer_path = convert_kml_to_shapefile(
        kml_path, SHAPEFILE_DIR, shapefile_name
    )

    push_shape_to_geoserver(shapefile_layer_path, workspace="customkml")

    # os.remove(kml_path)
    # shutil.rmtree(shapefile_layer_path)
    os.remove(shapefile_layer_path + ".zip")


def sync_layer_to_geoserver(state_name, fc, layer_name, workspace):
    state_dir = os.path.join("data/fc_to_shape", state_name)
    if not os.path.exists(state_dir):
        os.mkdir(state_dir)
    path = os.path.join(state_dir, f"{layer_name}")
    # Write the feature collection into json file
    with open(path + ".json", "w") as f:
        try:
            f.write(f"{json.dumps(fc)}")
        except Exception as e:
            print(e)

    path = generate_shape_files(path)
    return push_shape_to_geoserver(path, workspace=workspace)


def sync_fc_to_geoserver(fc, state_name, layer_name, workspace):
    try:
        geojson_fc = fc.getInfo()
    except Exception as e:
        print("Exception in getInfo()", e)
        task_id = sync_vector_to_gcs(fc, layer_name, "GeoJSON")
        check_task_status([task_id])

        geojson_fc = get_geojson_from_gcs(layer_name)

    if len(geojson_fc["features"]) > 0:
        new_fc = {"features": geojson_fc["features"], "type": geojson_fc["type"]}

        state_dir = os.path.join("data/fc_to_shape", state_name)
        if not os.path.exists(state_dir):
            os.mkdir(state_dir)
        path = os.path.join(state_dir, f"{layer_name}")
        # Write the feature collection into json file
        with open(path + ".json", "w") as f:
            try:
                f.write(f"{json.dumps(new_fc)}")
            except Exception as e:
                print(e)

        path = generate_shape_files(path)
        return push_shape_to_geoserver(path, workspace=workspace)


def to_camelcase(text):
    words = text.split()
    camelcase = words[0].lower()
    for word in words[1:]:
        camelcase += word.capitalize()
    return camelcase


def create_chunk(aoi, description, chunk_size):
    size = aoi.size().getInfo()
    parts = size // chunk_size
    # task_ids = []
    rois = []
    descs = []
    for part in range(parts + 1):
        start = part * chunk_size
        end = start + chunk_size
        block_name_for_parts = description + "_" + str(start) + "-" + str(end)
        roi = ee.FeatureCollection(aoi.toList(aoi.size()).slice(start, end))
        if roi.size().getInfo() > 0:
            descs.append(block_name_for_parts)
            rois.append(roi)

    return rois, descs


def merge_chunks(aoi, state, district, block, description, chunk_size):
    print("Merge Chunk task initiated")
    ee_initialize()
    size = aoi.size().getInfo()
    parts = size // chunk_size
    assets = []
    for part in range(parts + 1):
        start = part * chunk_size
        end = start + chunk_size
        block_name_for_parts = description + "_" + str(start) + "-" + str(end)
        src_asset_id = (
                get_gee_asset_path(state, district, block, GEE_HELPER_PATH)
                + block_name_for_parts
        )
        if is_gee_asset_exists(src_asset_id):
            assets.append(ee.FeatureCollection(src_asset_id))

    asset = ee.FeatureCollection(assets).flatten()

    asset_id = get_gee_asset_path(state, district, block) + description
    try:
        # Export an ee.FeatureCollection as an Earth Engine asset.
        task = ee.batch.Export.table.toAsset(
            **{
                "collection": asset,
                "description": description,
                "assetId": asset_id,
            }
        )

        task.start()
        print("Successfully started the merge chunk", task.status())
        return task.status()["id"]
    except Exception as e:
        print(f"Error occurred in running merge task: {e}")
