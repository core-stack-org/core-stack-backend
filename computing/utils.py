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
    get_gee_dir_path,
    export_vector_asset_to_gee,
    is_asset_public,
)
from utilities.geoserver_utils import Geoserver
import shutil
from utilities.constants import (
    ADMIN_BOUNDARY_OUTPUT_DIR,
    SHAPEFILE_DIR,
    GEE_HELPER_PATH,
    GEE_ASSET_PATH,
)
import ee
import json
from shapely.geometry import shape
from shapely.validation import explain_validity
import zipfile
from computing.models import Dataset, Layer, LayerType
from geoadmin.models import StateSOI, DistrictSOI, TehsilSOI


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
    return response


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


def sync_layer_to_geoserver(shp_folder, fc, layer_name, workspace):
    state_dir = os.path.join("data/fc_to_shape", shp_folder)
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


def sync_fc_to_geoserver(fc, shp_folder, layer_name, workspace):
    try:
        geojson_fc = fc.getInfo()
    except Exception as e:
        print("Exception in getInfo()", e)
        task_id = sync_vector_to_gcs(fc, layer_name, "GeoJSON")
        check_task_status([task_id])

        geojson_fc = get_geojson_from_gcs(layer_name)

    if len(geojson_fc["features"]) > 0:
        state_dir = os.path.join("data/fc_to_shape", shp_folder)
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
    else:
        return "No features in FeatureCollection"
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
    task_id = export_vector_asset_to_gee(asset, description, asset_id)
    return task_id


def fix_invalid_geometry_in_gdf(gdf):
    invalid = gdf[~gdf.is_valid]
    if not invalid.empty:
        print("Invalid geometries found:")
        for idx, geom in invalid.geometry.items():
            print(f"Index {idx}: {explain_validity(geom)}")
            gdf.loc[idx, "geometry"] = gdf.loc[idx, "geometry"].buffer(0)

    return gdf


def get_directory_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            if os.path.isfile(file_path):
                total_size += os.path.getsize(file_path)
    return total_size


def save_layer_info_to_db(
    state,
    district,
    block,
    layer_name,
    asset_id,
    dataset_name,
    sync_to_geoserver=False,
    layer_version=1.0,
    misc=None,
    is_override=False,
):
    print("inside the save_layer_info_to_db function ")
    dataset = Dataset.objects.get(name=dataset_name, layer_version=layer_version)
    state = state.upper()
    district = district.upper()
    block = block.upper()

    try:
        state_obj = StateSOI.objects.get(state_name__iexact=state)
        district_obj = DistrictSOI.objects.get(
            district_name__iexact=district, state=state_obj
        )
        block_obj = TehsilSOI.objects.get(
            tehsil_name__iexact=block, district=district_obj
        )
    except Exception as e:
        print("Error fetching in state district block:", e)
        return
    is_public = is_asset_public(asset_id)

    layer_obj, created = Layer.objects.update_or_create(
        dataset=dataset,
        layer_name=layer_name.lower(),
        state=state_obj,
        district=district_obj,
        block=block_obj,
        gee_asset_path=asset_id,
        defaults={
            "is_sync_to_geoserver": sync_to_geoserver,
            "is_public_gee_asset": is_public,
            "is_override": is_override,
            "misc": misc,
        },
    )
    if layer_obj:
        print("found layer object and updated")
    else:
        print("layer object not found so, created new one")
