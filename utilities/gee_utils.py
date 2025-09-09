import os
import tempfile
from contextlib import ExitStack
import time
import requests
from nrm_app.settings import (
    EARTH_DATA_USER,
    EARTH_DATA_PASSWORD,
    GEE_SERVICE_ACCOUNT_KEY_PATH,
    GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH, GEE_TEST_SERVICE_ACCOUNT_KEY_PATH,
)
from utilities.constants import (
    GEE_ASSET_PATH,
    GCS_BUCKET_NAME,
)
import ee, geetools, os, tempfile
import re
import json
import subprocess
from google.cloud import storage
from google.api_core import retry
from utilities.geoserver_utils import Geoserver
import numpy as np

import rasterio
from rasterio.merge import merge


def ee_initialize(project='ee-corestack-dev'):
    try:
        if project == "helper":
            service_account = "corestack-helper@ee-corestack-helper.iam.gserviceaccount.com"
            credentials = ee.ServiceAccountCredentials(
                service_account, GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH
            )
        elif project == "ee-kapil-test":
            print ("self account")
            service_account = "ee-kapil-test@ee-kapil-test.iam.gserviceaccount.com"
            credentials = ee.ServiceAccountCredentials(
                service_account, GEE_TEST_SERVICE_ACCOUNT_KEY_PATH
            )
        else:
            print ("core-stack")
            service_account = "core-stack-dev@ee-corestackdev.iam.gserviceaccount.com"
            credentials = ee.ServiceAccountCredentials(
                service_account, GEE_SERVICE_ACCOUNT_KEY_PATH
            )
        ee.Initialize(credentials)
        print("Earth Engine initialized for project:", project)
    except Exception as e:
        print("Exception in GEE connection:", e)


def gcs_config():
    from google.oauth2 import service_account

    # # Authenticate Earth Engine
    # ee_initialize()

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
    geometry,
    pyramiding_policy=None,
    max_pixel=1e13,
    crs="EPSG:4326",
):
    try:
        task = ee.batch.Export.image.toAsset(
            image=image,
            description=description,
            assetId=asset_id,
            pyramidingPolicy=pyramiding_policy,
            scale=scale,
            region=geometry,
            maxPixels=max_pixel,
            crs=crs,
        )
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


def sync_raster_to_gcs(image, scale, layer_name):
    print("inside sync_raster_to_gcs")
    export_task = ee.batch.Export.image.toCloudStorage(
        image=image,
        description="gcs_" + layer_name,
        bucket=GCS_BUCKET_NAME,
        fileNamePrefix="nrm_raster/" + layer_name,
        scale=scale,
        crs="EPSG:4326",
        maxPixels=1e13,
        fileFormat="GeoTIFF",
        formatOptions={
            "cloudOptimized": True
        }
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

def extract_water_pixels(tif_bytes, water_values={2, 3, 4}, nodata_value=0):
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp_in:
        tmp_in.write(tif_bytes)
        input_path = tmp_in.name

    output_path = tempfile.mktemp(suffix="_water_only.tif")

    with rasterio.open(input_path) as src:
        data = src.read(1)  # Assuming single-band raster
        profile = src.profile

        # Create mask for water values
        water_mask = np.isin(data, list(water_values))
        masked_data = np.where(water_mask, data, nodata_value)

        profile.update({
            "dtype": masked_data.dtype,
            "nodata": nodata_value,
            "compress": "LZW"  # Optional: compress output
        })

        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(masked_data, 1)

    # Read the masked raster back as bytes
    with open(output_path, "rb") as f:
        water_only_bytes = f.read()

    os.remove(input_path)
    os.remove(output_path)

    return water_only_bytes

def sync_multiple_raster_gcs_to_geoserver(workspace, gcs_file_name, layer_name, style_name=None):
    print("Inside sync_multiple_raster_gcs_to_geoserver")

    try:
        bucket = gcs_config()
        prefix = f"nrm_raster/{gcs_file_name}"
        print(f"Searching for blobs with prefix: {prefix}")

        blobs = list(bucket.list_blobs(prefix=prefix))
        tif_blobs = [blob for blob in blobs if blob.name.endswith(".tif")]

        print(f"Found {len(tif_blobs)} TIFF file(s).")

        if not tif_blobs:
            return {"success": False, "error": "No raster files found."}

        # Case 1: Single raster file
        if len(tif_blobs) == 1:
            print("Single raster found. Downloading and filtering...")
            original_bytes = tif_blobs[0].download_as_bytes()
            tif_content = extract_water_pixels(original_bytes, water_values={2, 3, 4})

        # Case 2: Multiple rasters – filter each, then merge
        else:
            print(f"{len(tif_blobs)} rasters found. Downloading, filtering, and merging...")
            temp_files = []
            merged_path = None

            try:
                for blob in tif_blobs:
                    print(f"Downloading and filtering: {blob.name}")
                    original_bytes = blob.download_as_bytes()
                    water_only_bytes = extract_water_pixels(original_bytes, water_values={2, 3, 4})

                    _, temp_path = tempfile.mkstemp(suffix=".tif")
                    with open(temp_path, "wb") as f:
                        f.write(water_only_bytes)
                    temp_files.append(temp_path)

                print("All rasters filtered. Proceeding to merge...")

                with ExitStack() as stack:
                    src_files_to_mosaic = [stack.enter_context(rasterio.open(fp)) for fp in temp_files]
                    mosaic, out_trans = merge(src_files_to_mosaic)
                    print("Mosaic shape:", mosaic.shape)
                    print("Mosaic dtype:", mosaic.dtype)

                    if mosaic.shape[0] != 1:
                        raise ValueError("Merged raster should be single-band.")

                    # Convert to uint8 to save space
                    mosaic_data = mosaic[0].astype(np.uint8)

                    out_meta = src_files_to_mosaic[0].meta.copy()
                    out_meta.update({
                        "driver": "GTiff",
                        "height": mosaic_data.shape[0],
                        "width": mosaic_data.shape[1],
                        "transform": out_trans,
                        "compress": "LZW",  # Efficient compression
                        "dtype": "uint8",
                        "count": 1,
                        "nodata": 0
                    })

                _, merged_path = tempfile.mkstemp(suffix=".tif")
                with rasterio.open(merged_path, "w", **out_meta) as dest:
                    dest.write(mosaic_data, 1)

                with open(merged_path, "rb") as f:
                    tif_content = f.read()

            finally:
                for fpath in temp_files:
                    try:
                        os.remove(fpath)
                    except Exception as cleanup_err:
                        print(f"Warning: Could not remove temp file {fpath}: {cleanup_err}")

                if merged_path:
                    try:
                        print (merged_path)
                        os.remove(merged_path)
                    except Exception as cleanup_err:
                        print(f"Warning: Could not remove merged file {merged_path}: {cleanup_err}")

    except Exception as e:
        print(f"Error preparing raster: {e}")
        return {"success": False, "error": str(e)}

    # Upload to GeoServer
    try:
        geo = Geoserver()
        print("Uploading water-only raster to GeoServer...")
        file_upload_res = geo.upload_raster(tif_content, workspace, layer_name)
        print("File upload response:", file_upload_res)
    except Exception as e:
        print(f"Error uploading raster to GeoServer: {e}")
        return {"success": False, "error": str(e)}

    # Optionally publish style
    if style_name:
        try:
            style_res = geo.publish_style(
                layer_name=layer_name,
                style_name=style_name,
                workspace=workspace
            )
            print("Style response:", style_res)
        except Exception as e:
            print(f"Error applying style to layer: {e}")
            return {"success": False, "error": str(e)}

    return {"success": True, "message": "Raster uploaded and styled successfully."}



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


def gcs_to_gee_asset_cli(gcs_uri, asset_id):
    """Use earthengine CLI to upload from GCS to GEE asset"""
    command = [
        "earthengine",
        f"--service_account_file={GEE_SERVICE_ACCOUNT_KEY_PATH}",
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


def upload_shp_to_gee(shapefile_path, file_name, asset_id):
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
    gcs_uri = f"gs://core_stack/{gcs_blob_name}"

    # Upload from GCS to GEE
    task_id = gcs_to_gee_asset_cli(gcs_uri, asset_id)
    if task_id:
        check_task_status([task_id], 600)

def get_distance_between_two_lan_long(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    r = 6371
    return c * r * 1000