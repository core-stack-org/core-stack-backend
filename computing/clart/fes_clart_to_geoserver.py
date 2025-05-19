import os
import ee
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path, is_gee_asset_exists,
    upload_tif_to_gcs,
    upload_tif_from_gcs_to_gee,
    sync_raster_gcs_to_geoserver,
)
from nrm_app.settings import BASE_DIR
from nrm_app.celery import app

@app.task(bind=True)
def generate_fes_clart_layer(self, state, district, block, file_path, clart_filename):
    print("Inside generate_fes_clart_layer")
    ee_initialize()
    try:
        description = f"{valid_gee_text(district)}_{valid_gee_text(block)}_clart"
        asset_id = get_gee_asset_path(state, district, block) + description + "_fes"

        if is_gee_asset_exists(asset_id):
            return {
                "success": f"Asset already exists: {asset_id}",
                "asset_id": asset_id
            }

        gcs_path = upload_tif_to_gcs(clart_filename, file_path)
        task_id = upload_tif_from_gcs_to_gee(gcs_path, asset_id, 30)
        check_task_status([task_id])
        return sync_raster_gcs_to_geoserver("clart", description + "_fes", description, "testClart")

    except Exception as e:
        raise e
