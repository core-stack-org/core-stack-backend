import os

import requests
import numpy as np

from nrm_app.settings import (
    EARTH_DATA_USER,
    EARTH_DATA_PASSWORD,
    GEE_SERVICE_ACCOUNT_KEY_PATH,
    GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH,
)
from utilities.constants import (
    GEE_ASSET_PATH,
    GCS_BUCKET_NAME,
)
import ee, geetools
import time
import re
import json
from google.cloud import storage
from google.api_core import retry

from utilities.geoserver_utils import Geoserver


def ee_initialize(project=None):
    try:
        if project == "helper":
            service_account = "corestack-helper@ee-corestack-helper.iam.gserviceaccount.com"
            credentials = ee.ServiceAccountCredentials(
                service_account, GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH
            )
        else:
            service_account = "core-stack-dev@ee-corestackdev.iam.gserviceaccount.com"
            credentials = ee.ServiceAccountCredentials(
                service_account, GEE_SERVICE_ACCOUNT_KEY_PATH
            )
        ee.Initialize(credentials)
        print("ee initialized", project)
    except Exception as e:
        print("Exception in gee connection", e)


def gcs_config():
    from google.oauth2 import service_account

    # Authenticate Earth Engine
    ee_initialize()

    # Authenticate Google Cloud Storage
    credentials = service_account.Credentials.from_service_account_file(
        GEE_SERVICE_ACCOUNT_KEY_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    # Create Storage Client
    storage_client = storage.Client(credentials=credentials)

    # Verify access
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    return bucket

    # print(list(bucket.list_blobs()))


def download_gee_layer(state, district, block):
    ee_initialize()
    fc = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + district
        + "_"
        + block
        + "_uid"
    )
    features = fc.getInfo()["features"]

    for feature in features:
        print("properties", feature["properties"])
        print("")


def check_gee_task_status(task_id):
    ee_initialize()
    try:
        gee_tasks = ee.data.getTaskStatus(task_id)
        print(gee_tasks)
        # gee_tasks = ee.data.listOperations()
        # print("check_gee_task_status>> ", gee_tasks)
        return gee_tasks
    except Exception as e:
        print("Exception in check_gee_task_status", e)


def check_task_status(task_id_list, sleep_time=60):
    print (len(task_id_list))
    if isinstance(task_id_list, str):
        return task_id_list
    if len(task_id_list) > 0:
        time.sleep(sleep_time)
        tasks = ee.data.listOperations()
        # tasks = check_gee_task_status(task_id_list[0])
        # print("tasks>>>", tasks)
        if tasks:
            for task in tasks:
                task_id = task["name"].split("/")[-1]
                if task_id in task_id_list and task["metadata"]["state"] in (
                        "SUCCEEDED",
                        "COMPLETED",
                        "FAILED",
                        "CANCELLED",
                ):
                    task_id_list.remove(task_id)
        print("task_id_list after", task_id_list)

        if len(task_id_list) > 0:
            print("Tasks not completed yet...")
            check_task_status(task_id_list)
    return task_id_list


def valid_gee_text(description):
    description = re.sub(r"[^a-zA-Z0-9 .,:;_-]", "", description)
    return description.replace(" ", "_")


def earthdata_auth(file_name, path):
    # url = "https://n5eil01u.ecs.nsidc.org/MOST/MOD10A1.006/2016.12.31/"
    # url = "https://e4ftl01.cr.usgs.gov/MOTA/MCD43A2.006/2017.09.04/"
    url = "https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/2000.02.11/"

    filename = path + "/" + file_name
    with requests.Session() as session:
        session.auth = (EARTH_DATA_USER, EARTH_DATA_PASSWORD)

        r1 = session.request("get", url + file_name)

        r = session.get(r1.url, auth=(EARTH_DATA_USER, EARTH_DATA_PASSWORD))
        print(r)
        if r.ok:
            with open(filename, "wb") as fd:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    fd.write(chunk)
    return filename


def gdf_to_ee_fc(gdf):
    features = []
    for i, row in gdf.iterrows():
        properties = row.drop("geometry").to_dict()
        geometry = ee.Geometry(row.geometry.__geo_interface__)
        feature = ee.Feature(geometry, properties)
        features.append(feature)
    return ee.FeatureCollection(features)


def create_gee_folder(folder_path, gee_project_path=GEE_ASSET_PATH):
    try:
        res = ee.data.createAsset(
            {"type": "Folder"},
            gee_project_path + folder_path,
        )
        print(res)
        time.sleep(10)
    except Exception as e:
        print("Error:", e)


def create_gee_directory(state, district, block, gee_project_path=GEE_ASSET_PATH):
    folder_path = valid_gee_text(state.lower()) + "/" + valid_gee_text(district.lower())
    create_gee_folder(folder_path, gee_project_path)

    folder_path = (
            valid_gee_text(state.lower())
            + "/"
            + valid_gee_text(district.lower())
            + "/"
            + valid_gee_text(block.lower())
    )
    create_gee_folder(folder_path, gee_project_path)


def get_gee_asset_path(state, district=None, block=None, asset_path=GEE_ASSET_PATH):
    gee_path = asset_path + valid_gee_text(state.lower()) + "/"
    if district:
        gee_path += valid_gee_text(district.lower()) + "/"
    if block:
        gee_path += valid_gee_text(block.lower()) + "/"
    return gee_path


def create_gee_dir(folder_list, gee_project_path=GEE_ASSET_PATH):
    folder_path = ""
    for folder in folder_list:
        folder_path += valid_gee_text(folder.lower())
        create_gee_folder(folder_path, gee_project_path)
        folder_path = folder_path + "/"


def get_gee_dir_path(folder_list, asset_path=GEE_ASSET_PATH):
    gee_path = asset_path
    for folder in folder_list:
        gee_path += valid_gee_text(folder.lower()) + "/"
    return gee_path


def geojson_to_ee_featurecollection(geojson_data):
    """
    Convert a GeoJSON FeatureCollection to an Earth Engine FeatureCollection
    """
    # # Read the GeoJSON file
    # with open(geojson_path, "r") as f:
    #     geojson_data = json.load(f)

    # Convert GeoJSON features to Earth Engine features
    ee_features = []
    for feature in geojson_data["features"]:
        # Convert the feature to a GeoJSON string
        feature_geojson = json.dumps(feature)

        # Create an Earth Engine Feature using ee.Geometry.coordinates()
        geometry = ee.Geometry(feature["geometry"])
        ee_feature = ee.Feature(geometry)

        # Add properties from the original feature
        if "properties" in feature:
            ee_feature = ee_feature.set(feature["properties"])

        ee_features.append(ee_feature)

    # Create an Earth Engine FeatureCollection
    return ee.FeatureCollection(ee_features)


def is_gee_asset_exists(path):
    asset = ee.Asset(path)
    flag = asset.exists()
    if flag:
        print(f"{path} already exists.")
    return flag


def move_asset_to_another_folder(src_folder, dest_folder):
    ee_initialize()
    # folder from where to copy
    # src_folder = "projects/df-project-iit/assets/core-stack/andhra_pradesh/ananthapur/nallacheruvu"
    # # folder where to copy
    # dest_folder = "projects/df-project-iit/assets/core-stack/andhra_pradesh/anantapur/nallacheruvu"

    # get all assets in the folder
    assets = ee.data.listAssets({"parent": src_folder})

    # loop through assets and copy them one by one to the new destination
    for asset in assets["assets"]:
        # construct destination path
        new_asset = dest_folder + "/" + asset["id"].split("/")[-1]
        # copy to destination
        ee.data.copyAsset(asset["id"], new_asset, True)
        # delete source asset
        # ee.data.deleteAsset(asset["id"])


def make_asset_public(asset_id):
    try:
        # Get the ACL of the asset
        acl = ee.data.getAssetAcl(asset_id)

        # Add 'all_users' to readers
        acl["all_users_can_read"] = True

        # Update the ACL
        @retry.Retry()
        def update_acl():
            ee.data.setAssetAcl(asset_id, acl)

        update_acl()

        # Verify the change
        updated_acl = ee.data.getAssetAcl(asset_id)
        if updated_acl.get("all_users_can_read"):
            print(f"Successfully made asset {asset_id} public")
            return True
        else:
            print(f"Failed to make asset {asset_id} public")
            return False
    except Exception as e:
        print(f"Error making asset public: {str(e)}")
        return False


def sync_raster_to_gcs(image, scale, layer_name):
    print("inside sync_raster_to_gcs")
    export_task = ee.batch.Export.image.toCloudStorage(
        image=image,
        description="gcs_" + layer_name,
        bucket=GCS_BUCKET_NAME,
        fileNamePrefix="nrm_raster/" + layer_name,
        scale=scale,
        fileFormat="GeoTIFF",
        crs="EPSG:4326",
        maxPixels=1e13,
    )

    export_task.start()
    print("Successfully started the sync_raster_to_gcs", export_task.status())
    return export_task.status()["id"]


def sync_raster_gcs_to_geoserver(workspace, gcs_file_name, layer_name, style_name):
    print("inside sync_raster_to_geoserver")
    bucket = gcs_config()

    blob = bucket.blob("nrm_raster/" + gcs_file_name + ".tif")
    tif_content = blob.download_as_bytes()

    geo = Geoserver()
    file_upload_res = geo.upload_raster(tif_content, workspace, layer_name)
    print("File response:", file_upload_res)
    if style_name:
        style_res = geo.publish_style(
            layer_name=layer_name, style_name=style_name, workspace=workspace
        )
        print("Style response:", style_res)


def upload_tif_to_gcs(gcs_file_name, local_file_path):
    bucket = gcs_config()
    blob_name = "nrm_raster/" + gcs_file_name
    blob = bucket.blob(blob_name)
    out_path = (
            "/".join(local_file_path.split("/")[:-1])
            + "/"
            + gcs_file_name.split(".")[0]
            + "_comp.tif"
    )
    print(out_path)
    cmd = f"gdal_translate {local_file_path} {out_path} -co TILED=YES -co COPY_SRC_OVERVIEWS=YES -co COMPRESS=LZW"
    os.system(command=cmd)

    blob.upload_from_filename(out_path)

    print(f"File {out_path} uploaded to {blob_name} in bucket {GCS_BUCKET_NAME}")
    time.sleep(10)
    return f"gs://{GCS_BUCKET_NAME}/{blob_name}"


def upload_tif_from_gcs_to_gee(gcs_path, asset_id, scale):
    # Read the image
    image = ee.Image.loadGeoTIFF(gcs_path)
    image = image.reproject(crs=image.projection())
    image = image.select(["B0"]).rename(["b1"])
    # Create an export task
    task = ee.batch.Export.image.toAsset(
        image=image,
        description=asset_id.split("/")[-1],
        assetId=asset_id,
        scale=scale,
        region=image.geometry(),
        crs="EPSG:4326",
        maxPixels=1e13,
    )

    # Start the upload task
    task.start()
    print("Successfully started the upload_tif_from_gcs_to_gee", task.status())
    return task.status()["id"]


def sync_vector_to_gcs(fc, layer_name, file_type="SHP"):
    print("inside sync_vector_to_gcs")
    export_task = ee.batch.Export.table.toCloudStorage(
        collection=fc,
        description="gcs_" + layer_name,
        bucket=GCS_BUCKET_NAME,
        fileNamePrefix="nrm_vector/" + layer_name,
        fileFormat=file_type,
    )

    export_task.start()
    print("Successfully started the sync_vector_to_gcs", export_task.status())
    return export_task.status()["id"]


def get_geojson_from_gcs(gcs_file_name):
    """
    Fetch a GeoJSON file from Google Cloud Storage and return it as a Python dictionary.
    """
    # Initialize a storage client
    bucket = gcs_config()
    blob_name = "nrm_vector/" + gcs_file_name + ".geojson"
    blob = bucket.blob(blob_name)

    # Download the content as string
    geojson_str = blob.download_as_text()

    # Parse string as JSON
    geojson_data = json.loads(geojson_str)

    return geojson_data


def harmonize_band_types(image, target_type="Float"):
    """
    Harmonize all bands in an image to the same data type.

    Args:
        image (ee.Image): Input image with mixed band types
        target_type (str): Target data type ('Float', 'Byte', 'Int' etc.)

    Returns:
        ee.Image: Image with harmonized band types
    """
    # Get list of band names
    band_names = image.bandNames()

    # Function to cast each band to target type
    def cast_band(band_name):
        band = image.select(band_name)
        if target_type == "Float":
            return band.toFloat()
        elif target_type == "Byte":
            return band.toByte()
        elif target_type == "Int":
            return band.toInt()
        elif target_type == "Double":
            return band.toDouble()
        else:
            raise ValueError(f"Unsupported target type: {target_type}")

    # Cast all bands and combine back into single image
    harmonized_bands = band_names.map(lambda name: cast_band(ee.String(name)))
    return ee.ImageCollection(harmonized_bands).toBands().rename(band_names)

def get_distance_between_two_lan_long(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    r = 6371
    return c * r * 1000