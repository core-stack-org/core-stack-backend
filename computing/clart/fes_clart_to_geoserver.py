import os
import ee
import subprocess
import shutil
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path, is_gee_asset_exists,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    gcs_config,
)
from utilities.constants import (
    GCS_BUCKET_NAME,
)
from nrm_app.settings import BASE_DIR
from nrm_app.celery import app

@app.task(bind=True)
def generate_fes_clart_layer(self, state, district, block, file_path):
    print("Inside generate_fes_clart_layer")
    ee_initialize()

    try:
        upload_dir = os.path.join(BASE_DIR, 'data', 'fes_clart_file', state, district)
        os.makedirs(upload_dir, exist_ok=True)

        filename = os.path.basename(file_path)
        ext = os.path.splitext(filename)[1].lower()
        new_filename = f"{district}_{block}_clart{ext}"
        new_file_path = os.path.join(upload_dir, new_filename)
        shutil.copy(file_path, new_file_path)

        #GEE asset naming
        description = f"{valid_gee_text(district)}_{valid_gee_text(block)}_clart"
        asset_id = get_gee_asset_path(state, district, block) + description

        if is_gee_asset_exists(asset_id):
            return {
                "success": f"Asset already exists: {asset_id}",
                "asset_id": asset_id
            }

        #Convert to Cloud Optimized GeoTIFF (COG)
        cog_file_path = os.path.join(upload_dir, f"cog_{new_filename}")
        gdal_cmd = [
            "gdal_translate", 
            "-of", "COG", 
            "-co", "COMPRESS=DEFLATE",
            "-co", "PREDICTOR=2",
            "-co", "BIGTIFF=YES",
            new_file_path, 
            cog_file_path
        ]

        subprocess.run(gdal_cmd, check=True)

        #Upload COG to GCS
        temp_gcs_path = f"nrm_raster/{new_filename}"
        blob = gcs_config().blob(temp_gcs_path)
        blob.upload_from_filename(cog_file_path)
        gcs_url = f"gs://{GCS_BUCKET_NAME}/{temp_gcs_path}"

        #Ingest into GEE
        image = ee.Image.loadGeoTIFF(gcs_url)
        task = ee.batch.Export.image.toAsset(
            image=image,
            description=description,
            assetId=asset_id,
            pyramidingPolicy={"predicted_label": "mode"},
            scale=30,
            maxPixels=1e13,
            crs="EPSG:4326",
        )
        task.start()
        check_task_status([task.status()["id"]])

        #Export to GCS and sync with GeoServer
        image = ee.Image(asset_id)
        task_id = sync_raster_to_gcs(image, 30, description)
        check_task_status([task_id])
        sync_raster_gcs_to_geoserver("clart", description, description, "testClart")

        #Clean up temp COG
        os.remove(cog_file_path)

        return {
            "success": f"File '{new_filename}' uploaded to GEE and published to GeoServer.",
            "asset_id": asset_id,
            "location": {"state": state, "district": district, "block": block}
        }

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"GDAL conversion failed: {e}")
    except Exception as e:
        raise e
