import os

import geopandas as gpd
import fiona
import copy

from projects.models import Project
from utilities.gee_utils import (
    ee_initialize,
    sync_vector_to_gcs,
    check_task_status,
    get_geojson_from_gcs,
    is_gee_asset_exists,
    valid_gee_text,
    get_gee_asset_path,
    get_gee_dir_path,
)
from utilities.geoserver_utils import Geoserver
import shutil
from utilities.constants import (
    ADMIN_BOUNDARY_OUTPUT_DIR,
    SHAPEFILE_DIR,
    GEE_HELPER_PATH,
    GEE_ASSET_PATH, GEE_PATHS,
)
import ee
import json
from shapely.geometry import shape
from shapely.validation import explain_validity
import zipfile
from datetime import datetime, timedelta


def generate_shape_files(path):
    gdf = gpd.read_file(path + ".json")
    os.remove(path + ".json")

    gdf.to_file(
        path,
        driver="ESRI Shapefile",
    )
    return path


def convert_to_zip(dir_name, file_type):
    if file_type == "gpkg":
        with zipfile.ZipFile(dir_name + ".zip", "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(dir_name + ".gpkg", arcname=os.path.basename(dir_name + ".gpkg"))
        return dir_name + ".zip"
    else:
        return shutil.make_archive(dir_name, "zip", dir_name + "/")


def push_shape_to_geoserver(path, store_name=None, workspace=None, file_type="shp"):
    geo = Geoserver()

    zip_path = convert_to_zip(path, file_type)

    response = geo.create_shp_datastore(
        path=zip_path,
        store_name=store_name,
        workspace=workspace,
        file_extension=file_type,
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
        state_dir = os.path.join("data/fc_to_shape", state_name)
        if not os.path.exists(state_dir):
            os.mkdir(state_dir)
        path = os.path.join(state_dir, f"{layer_name}")

        # Convert to GeoDataFrame
        gdf = gpd.GeoDataFrame.from_features(geojson_fc["features"])

        # Set CRS (Earth Engine uses EPSG:4326 by default)
        gdf.crs = "EPSG:4326"

        gdf = fix_invalid_geometry_in_gdf(gdf)

        # Save as GeoPackage
        gdf.to_file(path + ".gpkg", driver="GPKG")

        return push_shape_to_geoserver(path, workspace=workspace, file_type="gpkg")
        # new_fc = {"features": geojson_fc["features"], "type": geojson_fc["type"]}
        #
        # state_dir = os.path.join("data/fc_to_shape", state_name)
        # if not os.path.exists(state_dir):
        #     os.mkdir(state_dir)
        # path = os.path.join(state_dir, f"{layer_name}")
        # # Write the feature collection into json file
        # with open(path + ".json", "w") as f:
        #     try:
        #         f.write(f"{json.dumps(new_fc)}")
        #     except Exception as e:
        #         print(e)
        #
        # path = generate_shape_files(path)
        # return push_shape_to_geoserver(path, workspace=workspace)
def sync_project_fc_to_geoserver(fc, project_name, layer_name, workspace):
    try:
        geojson_fc = fc.getInfo()
    except Exception as e:
        print("Exception in getInfo()", e)
        task_id = sync_vector_to_gcs(fc, layer_name, "GeoJSON")
        check_task_status([task_id])

        geojson_fc = get_geojson_from_gcs(layer_name)

    if len(geojson_fc["features"]) > 0:
        state_dir = os.path.join("data/fc_to_shape", project_name)
        if not os.path.exists(state_dir):
            os.mkdir(state_dir)
        path = os.path.join(state_dir, f"{layer_name}")

        # Convert to GeoDataFrame
        gdf = gpd.GeoDataFrame.from_features(geojson_fc["features"])

        # Set CRS (Earth Engine uses EPSG:4326 by default)
        gdf.crs = "EPSG:4326"

        gdf = fix_invalid_geometry_in_gdf(gdf)

        # Save as GeoPackage
        gdf.to_file(path + ".gpkg", driver="GPKG")

        return push_shape_to_geoserver(path, workspace=workspace, file_type="gpkg")

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


def merge_chunks(
    aoi,
    folder_list,
    description,
    chunk_size,
    chunk_asset_path=GEE_HELPER_PATH,
    merge_asset_path=GEE_ASSET_PATH,
):
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
            get_gee_dir_path(folder_list, chunk_asset_path) + block_name_for_parts
        )
        if is_gee_asset_exists(src_asset_id):
            assets.append(ee.FeatureCollection(src_asset_id))

    asset = ee.FeatureCollection(assets).flatten()

    asset_id = get_gee_dir_path(folder_list, merge_asset_path) + description
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


def fix_invalid_geometry_in_gdf(gdf):
    invalid = gdf[~gdf.is_valid]
    if not invalid.empty:
        print("Invalid geometries found:")
        for idx, geom in invalid.geometry.items():
            print(f"Index {idx}: {explain_validity(geom)}")
            gdf.loc[idx, "geometry"] = gdf.loc[idx, "geometry"].buffer(0)

    return gdf


def get_season_key(date):
    """Return season key like 'rabi_2017-2018' based on Indian cropping seasons."""
    month = date.month
    year = date.year
    next_year = year + 1

    if month in [1, 2]:
        return f"rabi_{year - 1}-{year}"  # Jan–Feb → Rabi of previous year
    elif month in [11, 12]:
        return f"rabi_{year}-{next_year}"  # Nov–Dec → Rabi starting this year
    elif month in [3, 4, 5, 6]:
        return f"zaid_{year}-{next_year}"
    elif month in [7, 8, 9, 10]:
        return f"kharif_{year}-{next_year}"
    else:
        return None

def get_agri_year_key(season_key):
    """Convert a season key to agricultural year key (e.g., rabi_2017-2018 → 2017-2018)."""
    season, years = season_key.split("_")
    start_year, end_year = map(int, years.split("-"))

    if season in ["kharif", "rabi"]:
        return f"{start_year}-{end_year}"
    elif season == "zaid":
        return f"{start_year - 1}-{start_year}"  # Zaid 2018-2019 → Agri year 2017-2018
    else:
        return None

def calculate_precipitation_season(geojson_filepath, start_year=2017, end_year=2024):
    # Load the GeoJSON file
    with open(geojson_filepath, "r") as f:
        feature_collection = json.load(f)

    features_ee = []

    for feature in feature_collection["features"]:
        original_props = feature["properties"]
        new_props = {}

        # Copy UID if available
        if "uid" in original_props:
            new_props["uid"] = original_props["uid"]

        agri_year_totals = {}

        for key, val in original_props.items():
            try:
                date = datetime.strptime(key, "%Y-%m-%d")
                season_key = get_season_key(date)
                if not season_key:
                    continue

                agri_key = get_agri_year_key(season_key)
                if not agri_key:
                    continue

                # Filter by agri year range
                agri_start = int(agri_key.split("-")[0])
                if not (start_year <= agri_start <= end_year):
                    continue

                season = season_key.split("_")[0]  # e.g., 'kharif'
                full_key = f"{season}_{agri_key}"  # e.g., '2017-2018_kharif'

                agri_year_totals[full_key] = agri_year_totals.get(full_key, 0) + float(val)

            except Exception:
                continue  # Skip bad keys/values

        # Add precipitation totals per agri year and season
        for agri_key, total in agri_year_totals.items():
            new_props[f"precipitation_{agri_key}"] = total

        # Optional debug
        # print(new_props)

        # Create Earth Engine Feature
        geom_ee = ee.Geometry(feature["geometry"])
        feature_ee = ee.Feature(geom_ee, new_props)
        features_ee.append(feature_ee)

    # Return as FeatureCollection
    return ee.FeatureCollection(features_ee)


def generate_geojson_with_ci_and_ndvi(zoi_asset, ci_asset, ndvi_asset, proj_id):
    # Load project
    proj_obj = Project.objects.get(pk=proj_id)

    # Build CI and NDVI asset paths
    asset_path_ci = get_gee_dir_path(
        [proj_obj.name], asset_path=GEE_PATHS['WATER_REJ']["GEE_ASSET_PATH"]
    ) + ci_asset

    asset_path_ndvi = get_gee_dir_path(
        [proj_obj.name], asset_path=GEE_PATHS['NDVI']["GEE_ASSET_PATH"]
    ) + ndvi_asset

    # Load FeatureCollections
    zoi = ee.FeatureCollection(zoi_asset)
    ci = ee.FeatureCollection(asset_path_ci)
    ndvi = ee.FeatureCollection(asset_path_ndvi)

    # -------------------------
    # STEP 1: Join ZOI with Cropping Intensity
    # -------------------------
    join = ee.Join.inner()
    filter = ee.Filter.intersects(leftField='.geo', rightField='.geo')
    zoi_ci_joined = join.apply(zoi, ci, filter)

    def merge_zoi_ci(pair):
        zoi_feat = ee.Feature(pair.get('primary'))
        ci_feat = ee.Feature(pair.get('secondary'))
        merged_props = zoi_feat.toDictionary().combine(ci_feat.toDictionary(), True)
        return ee.Feature(zoi_feat.geometry(), merged_props)

    zoi_with_ci = ee.FeatureCollection(zoi_ci_joined.map(merge_zoi_ci))

    # -------------------------
    # STEP 2: Join ZOI+CI with NDVI
    # -------------------------
    zoi_ndvi_joined = join.apply(zoi_with_ci, ndvi, filter)

    def merge_zoi_ci_ndvi(pair):
        ci_feat = ee.Feature(pair.get('primary'))
        ndvi_feat = ee.Feature(pair.get('secondary'))
        merged_props = ci_feat.toDictionary().combine(ndvi_feat.toDictionary(), True)
        return ee.Feature(ci_feat.geometry(), merged_props)

    final_merged = ee.FeatureCollection(zoi_ndvi_joined.map(merge_zoi_ci_ndvi))

    # -------------------------
    # STEP 3: Export or Push to GeoServer
    # -------------------------
    layer_name = f'WaterRejapp_zoi_{proj_obj.name}_{proj_obj.id}'
    sync_project_fc_to_geoserver(final_merged, proj_obj.name, layer_name, 'waterrej')
