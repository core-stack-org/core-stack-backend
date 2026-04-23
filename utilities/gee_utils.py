import os
import shutil

import requests

from nrm_app.settings import (
    BASE_DIR,
    EARTH_DATA_USER,
    EARTH_DATA_PASSWORD,
    GEE_DEFAULT_ACCOUNT_ID,
    GEE_HELPER_ACCOUNT_ID,
    FERNET_KEY,
    GCS_BUCKET_NAME,
)
from utilities.constants import GEE_ASSET_PATH
import ee, geetools
import time
import re
import json
import subprocess
from google.cloud import storage
from google.api_core import retry
from utilities.geoserver_utils import Geoserver
from gee_computing.models import GEEAccount
from google.oauth2 import service_account
import numpy as np
import tempfile
from cryptography.fernet import Fernet


class GEEInitializationError(RuntimeError):
    """Raised when Earth Engine credentials cannot be initialized safely."""


def _normalize_gee_account_id(account_id=None, project=None):
    if project == "helper":
        account_id = GEE_HELPER_ACCOUNT_ID

    if isinstance(account_id, str):
        lowered = account_id.strip().lower()
        if lowered == "helper":
            account_id = GEE_HELPER_ACCOUNT_ID
        elif lowered == "default":
            account_id = GEE_DEFAULT_ACCOUNT_ID
        elif lowered == "datasets":
            raise GEEInitializationError(
                "The 'datasets' Earth Engine alias is not configured for this installation path."
            )
        else:
            account_id = account_id.strip()

    if account_id in (None, ""):
        raise GEEInitializationError(
            "GEE account id is blank. Set GEE_DEFAULT_ACCOUNT_ID/GEE_HELPER_ACCOUNT_ID in .env."
        )

    try:
        return int(account_id)
    except (TypeError, ValueError) as exc:
        raise GEEInitializationError(
            f"GEE account id must be an integer, got {account_id!r}."
        ) from exc


def _get_gee_account(account_id=None, project=None):
    normalized_account_id = _normalize_gee_account_id(account_id, project=project)

    try:
        account = GEEAccount.objects.get(pk=normalized_account_id)
    except GEEAccount.DoesNotExist as exc:
        raise GEEInitializationError(
            f"GEEAccount with id={normalized_account_id} was not found."
        ) from exc

    return normalized_account_id, account


def ee_initialize(
    account_id=GEE_DEFAULT_ACCOUNT_ID,
    strict=False,
    log_failure=True,
    project=None,
):
    try:
        normalized_account_id, account = _get_gee_account(account_id, project=project)

        credentials_blob = account.get_credentials()
        if not credentials_blob:
            raise GEEInitializationError(
                f"GEEAccount id={normalized_account_id} does not have stored credentials."
            )

        key_dict = json.loads(credentials_blob.decode("utf-8"))
        credentials = service_account.Credentials.from_service_account_info(
            key_dict,
            scopes=[
                "https://www.googleapis.com/auth/earthengine",
                "https://www.googleapis.com/auth/devstorage.full_control",
            ],
        )
        ee.Initialize(credentials)

        return True
    except Exception as exc:
        if strict:
            raise
        if log_failure:
            print(f"Skipping Earth Engine initialization: {exc}")
        return False


def ee_initialize_safe(account_id=GEE_DEFAULT_ACCOUNT_ID):
    return ee_initialize(account_id=account_id, strict=False, log_failure=True)


def probe_gee_connection(account_id=GEE_DEFAULT_ACCOUNT_ID, project=None):
    ee_initialize(account_id=account_id, strict=True, project=project)
    return ee.Number(1).getInfo() == 1


def probe_gcs_upload_access(gee_account_id=GEE_DEFAULT_ACCOUNT_ID, cleanup=True):
    normalized_account_id, account = _get_gee_account(gee_account_id)
    bucket = gcs_config(gee_account_id=normalized_account_id)
    blob_name = (
        f"core-stack-initialisation-probe/account-{normalized_account_id}/"
        f"{int(time.time())}-{os.getpid()}.txt"
    )
    blob = bucket.blob(blob_name)

    try:
        blob.upload_from_string(
            "core-stack gcs upload probe\n",
            content_type="text/plain",
        )
    except Exception as exc:
        raise GEEInitializationError(
            f"GCS upload probe failed for bucket '{GCS_BUCKET_NAME}' with "
            f"account id={normalized_account_id} "
            f"({account.service_account_email}): {exc}"
        ) from exc

    cleanup_detail = "Upload succeeded."
    if cleanup:
        try:
            blob.delete()
            cleanup_detail = "Upload and cleanup succeeded."
        except Exception as exc:
            cleanup_detail = (
                "Upload succeeded, but probe cleanup could not delete the temporary "
                f"object: {exc}"
            )

    return {
        "account_id": normalized_account_id,
        "service_account_email": account.service_account_email,
        "bucket_name": GCS_BUCKET_NAME,
        "blob_name": blob_name,
        "detail": cleanup_detail,
    }


def copy_gee_credentials_into_repo(
    credentials_path,
    destination_dir="data/gee_confs",
    destination_name=None,
):
    source_path = os.path.abspath(credentials_path)
    if not os.path.isfile(source_path):
        raise GEEInitializationError(
            f"GEE credentials file was not found: {source_path}"
        )

    if os.path.isabs(destination_dir):
        repo_directory = destination_dir
    else:
        repo_directory = os.path.join(BASE_DIR, destination_dir)

    os.makedirs(repo_directory, exist_ok=True)

    file_name = destination_name or os.path.basename(source_path)
    destination_path = os.path.join(repo_directory, file_name)

    if os.path.abspath(source_path) != os.path.abspath(destination_path):
        shutil.copy2(source_path, destination_path)

    os.chmod(destination_path, 0o640)

    return {
        "absolute_path": destination_path,
        "relative_path": os.path.relpath(destination_path, BASE_DIR),
    }


def upsert_gee_account_from_json(
    credentials_path, account_name=None, helper_account_id=None
):
    credentials_path = os.path.abspath(credentials_path)
    if not os.path.isfile(credentials_path):
        raise GEEInitializationError(
            f"GEE credentials file was not found: {credentials_path}"
        )

    with open(credentials_path, "rb") as credentials_file:
        credentials_payload = credentials_file.read()

    key_dict = json.loads(credentials_payload.decode("utf-8"))
    service_account_email = key_dict.get("client_email")
    if not service_account_email:
        raise GEEInitializationError(
            "The provided credentials JSON does not contain client_email."
        )

    account_name = (
        account_name or os.path.splitext(os.path.basename(credentials_path))[0]
    )
    account = (
        GEEAccount.objects.filter(service_account_email=service_account_email).first()
        or GEEAccount.objects.filter(name=account_name).first()
        or GEEAccount(name=account_name)
    )

    account.name = account_name
    account.service_account_email = service_account_email
    account.credentials_encrypted = Fernet(FERNET_KEY).encrypt(credentials_payload)
    account.is_visible = True
    account.save()

    if helper_account_id:
        helper_id = _normalize_gee_account_id(helper_account_id)
        if helper_id != account.id:
            account.helper_account = GEEAccount.objects.get(pk=helper_id)
    elif account.helper_account_id is None:
        account.helper_account = account

    account.save()
    return account


# def ee_initialize(project=None):
#     try:
#         if project == "helper":
#             service_account = (
#                 "corestack-helper@ee-corestack-helper.iam.gserviceaccount.com"
#             )
#             conf_path = os.path.join(BASE_DIR, GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH)
#             credentials = ee.ServiceAccountCredentials(service_account, str(conf_path))
#         elif project == "datasets":
#             service_account = (
#                 "corestack-datasets@corestack-datasets.iam.gserviceaccount.com"
#             )
#             conf_path = os.path.join(BASE_DIR, GEE_DATASETS_SERVICE_ACCOUNT_KEY_PATH)
#             credentials = ee.ServiceAccountCredentials(service_account, str(conf_path))
#         else:
#             service_account = "core-stack-dev@ee-corestackdev.iam.gserviceaccount.com"
#             conf_path = os.path.join(BASE_DIR, GEE_SERVICE_ACCOUNT_KEY_PATH)
#             credentials = ee.ServiceAccountCredentials(service_account, str(conf_path))
#         ee.Initialize(credentials)
#         print("ee initialized", project)
#     except Exception as e:
#         print("Exception in gee connection", e)


def gcs_config(gee_account_id=GEE_DEFAULT_ACCOUNT_ID):
    from google.oauth2 import service_account

    # # Authenticate Earth Engine
    # ee_initialize()

    # Authenticate Google Cloud Storage
    _, account = _get_gee_account(gee_account_id)
    key_dict = json.loads(account.get_credentials().decode("utf-8"))
    credentials = service_account.Credentials.from_service_account_info(
        key_dict,
        scopes=[
            "https://www.googleapis.com/auth/earthengine",
            "https://www.googleapis.com/auth/devstorage.full_control",
        ],
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
    ee_initialize(1)
    try:
        gee_tasks = ee.data.getTaskStatus(task_id)
        print(gee_tasks)
        # gee_tasks = ee.data.listOperations()
        # print("check_gee_task_status>> ", gee_tasks)
        return gee_tasks
    except Exception as e:
        print("Exception in check_gee_task_status", e)


def check_task_status(task_id_list, sleep_time=60):
    task_id_list = list(filter(None, task_id_list))
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
    description = re.sub(r"[^a-zA-Z0-9 ,:;_-]", "", description)
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


def create_gee_folder(folder_path, gee_project_path):
    full_path = gee_project_path + folder_path
    parts = full_path.split("/")
    for i in range(1, len(parts) + 1):
        sub_path = "/".join(parts[:i])
        try:
            ee.data.getAsset(sub_path)
            print(f"Exists: {sub_path}")
        except:
            try:
                ee.data.createAsset({"type": "Folder"}, sub_path)
                print(f"Created: {sub_path}")
                time.sleep(1)
            except Exception as e:
                print(f"Failed: {sub_path} -> {e}")


def create_gee_directory(
    state=None,
    district=None,
    block=None,
    folder_path=None,
    gee_project_path=GEE_ASSET_PATH,
):
    if state and district and block:
        folder_path = (
            valid_gee_text(state.lower()) + "/" + valid_gee_text(district.lower())
        )
        create_gee_folder(folder_path, gee_project_path)

        folder_path = (
            valid_gee_text(state.lower())
            + "/"
            + valid_gee_text(district.lower())
            + "/"
            + valid_gee_text(block.lower())
        )
        create_gee_folder(folder_path, gee_project_path)
    else:
        print("inside else")
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


def export_vector_asset_to_gee(fc, description, asset_id):
    try:
        task = ee.batch.Export.table.toAsset(
            collection=fc,
            description=description,
            assetId=asset_id,
        )

        task.start()
        print(
            f"Successfully started the task for {description}, task id:{task.status()}"
        )
        return task.status()["id"]
    except Exception as e:
        print(f"Error occurred in running {description} task: {e}")
        return None


def export_raster_asset_to_gee(
    image,
    description,
    asset_id,
    scale,
    region,
    pyramiding_policy=None,
    max_pixel=1e13,
    crs="EPSG:4326",
):
    try:
        export_params = {
            "image": image,
            "description": description,
            "assetId": asset_id,
            "scale": scale,
            "region": region,
            "maxPixels": max_pixel,
            "crs": crs,
        }
        if pyramiding_policy:
            export_params["pyramidingPolicy"] = pyramiding_policy

        task = ee.batch.Export.image.toAsset(**export_params)

        task.start()
        print(
            f"Successfully started the task for {description}, task id:{task.status()}"
        )
        return task.status()["id"]
    except Exception as e:
        print(f"Error occurred in running {description} task: {e}")
        return None


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


def is_asset_public(asset_id):
    try:
        acl = ee.data.getAssetAcl(asset_id)
        if acl.get("all_users_can_read"):
            return True
        else:
            return False
    except Exception as e:
        print(f"Error in checking asset public : {e}")
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
    geo = Geoserver()
    geo.delete_raster_store(workspace=workspace, store=layer_name)
    bucket = gcs_config()

    blob = bucket.blob("nrm_raster/" + gcs_file_name + ".tif")
    tif_content = blob.download_as_bytes()

    file_upload_res = geo.upload_raster(tif_content, workspace, layer_name)
    print("File response:", file_upload_res)
    if style_name:
        style_res = geo.publish_style(
            layer_name=layer_name, style_name=style_name, workspace=workspace
        )
        print("Style response:", style_res)
    return f"File response: {file_upload_res}"


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


def gcs_file_exists(layer_name):
    bucket = gcs_config()
    blob = bucket.blob(f"nrm_raster/{layer_name}.tif")
    return blob.exists()


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


def download_csv_from_gcs(bucket_name, blob_name, destination_file_name):
    try:
        bucket = gcs_config()
        blob = bucket.blob(bucket_name + "/" + blob_name)
        if blob.exists():
            blob.download_to_filename(destination_file_name)
            print(
                f"Downloaded {blob_name} from bucket {bucket_name} to {destination_file_name}"
            )
        else:
            print(
                f"Blob '{blob_name}' does not exist in bucket '{bucket_name}'. No file downloaded."
            )
    except Exception as e:
        print(
            f"Exception in downloading csv {blob_name} from GCS bucket {bucket_name}", e
        )


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


def upload_file_to_gcs(local_file_path, destination_blob_name):
    """Upload a file to a Google Cloud Storage bucket"""
    bucket = gcs_config()
    print(local_file_path)
    blob = bucket.blob(destination_blob_name)

    # Set the chunk size to 100 MB (must be a multiple of 256 KB)
    blob.chunk_size = 100 * 1024 * 1024  # 100 MB

    # Upload the file using a resumable upload
    blob.upload_from_filename(local_file_path)

    print(f"File {local_file_path} uploaded to {destination_blob_name}.")


def extract_task_id(command_output):
    """
    Extract the Earth Engine task ID from command output.

    Args:
        command_output (str): The stdout from the earthengine command

    Returns:
        str or None: The task ID if found, otherwise None
    """
    # Looking for patterns like:
    # "Started upload task with ID: abcdef1234567890"
    # or "Task ID: abcdef1234567890"

    import re

    # Try different possible patterns
    patterns = [
        r"Started upload task with ID: ([a-zA-Z0-9_]+)",
        r"Task ID: ([a-zA-Z0-9_]+)",
        r"ID: ([a-zA-Z0-9_]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, command_output)
        if match:
            return match.group(1)

    return None


def gcs_to_gee_asset_cli(gcs_uri, asset_id, gee_account_id):
    account = GEEAccount.objects.get(pk=gee_account_id)
    key_dict = json.loads(account.get_credentials().decode("utf-8"))

    # Write credentials to a temp JSON file
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as f:
        json.dump(key_dict, f)
        service_account_file = f.name

    """Use earthengine CLI to upload from GCS to GEE asset"""
    command = [
        "earthengine",
        f"--service_account_file={service_account_file}",
        "upload",
        "table",
        f"--asset_id={asset_id}",
        gcs_uri,
    ]

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print("Upload initiated successfully.")
        print("Output:", result.stdout)
        if result.returncode == 0:
            return extract_task_id(result.stdout)
        return None
    except subprocess.CalledProcessError as e:
        print("An error occurred during the upload.")
        print("Command:", " ".join(command))
        print("Return Code:", e.returncode)
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return None


def upload_shp_to_gee(
    shapefile_path, file_name, asset_id, gee_account_id=GEE_DEFAULT_ACCOUNT_ID
):
    """
    Upload a shapefile to GEE asset from GCS using CLI commands
    Args:
        shapefile_path:
        file_name:
        asset_id:
        gee_account_id:

    Returns:

    """
    gcs_blob_name = f"shapefiles/{file_name}/{file_name}.shp"

    # Make sure all shapefile components (.shp, .dbf, .shx, .prj) are uploaded
    components = [".shp", ".dbf", ".shx", ".prj"]
    for component in components:
        base_name = os.path.splitext(shapefile_path)[0]
        component_path = base_name + component
        if os.path.exists(component_path):
            dest_blob = gcs_blob_name.replace(".shp", component)
            upload_file_to_gcs(component_path, dest_blob)

    # GCS URI to the shapefile
    gcs_uri = f"gs://{GCS_BUCKET_NAME}/{gcs_blob_name}"

    # Upload from GCS to GEE
    task_id = gcs_to_gee_asset_cli(gcs_uri, asset_id, gee_account_id)
    if task_id:
        check_task_status([task_id], 100)


def merge_fc_into_existing_fc(asset_id, description, new_asset_id, join_on="id"):
    print("Asset ID:", asset_id)
    print("New Asset ID:", new_asset_id)
    # Join on 'id'
    joined = ee.Join.inner().apply(
        primary=ee.FeatureCollection(asset_id),
        secondary=ee.FeatureCollection(new_asset_id),
        condition=ee.Filter.equals(leftField=join_on, rightField=join_on),
    )

    # Merge properties from both collections
    def merge_properties(f):
        f1 = ee.Feature(f.get("primary"))
        f2 = ee.Feature(f.get("secondary"))
        return f1.copyProperties(f2)

    merged = joined.map(merge_properties)
    task_id = export_vector_asset_to_gee(
        merged, f"{description}_merge", f"{asset_id}_merge"
    )
    task_list = check_task_status([task_id])
    print("merge task completed.", task_list)

    if is_gee_asset_exists(f"{asset_id}_merge"):
        # Delete existing asset
        ee.data.deleteAsset(asset_id)
        ee.data.deleteAsset(new_asset_id)
        # Rename new asset with existing asset's name
        ee.data.copyAsset(f"{asset_id}_merge", asset_id)
        time.sleep(10)
        # Delete new asset
        ee.data.deleteAsset(f"{asset_id}_merge")


def build_gee_helper_paths(app_type, helper_project):
    gee_helper_base_path = f"projects/{helper_project}/assets/apps"
    return f"{gee_helper_base_path}/{app_type.lower()}/"


def get_distance_between_two_lan_long(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    r = 6371
    return c * r * 1000
