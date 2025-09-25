import os
import ee
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    upload_tif_to_gcs,
    upload_tif_from_gcs_to_gee,
    sync_raster_gcs_to_geoserver,
    make_asset_public,
)
from nrm_app.settings import BASE_DIR
from nrm_app.celery import app
from computing.utils import save_layer_info_to_db, update_layer_sync_status


@app.task(bind=True)
def generate_fes_clart_layer(self, state, district, block, file_path, clart_filename, gee_account_id):
    print("Inside generate_fes_clart_layer")
    ee_initialize(gee_account_id)
    try:
        description = (
            f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_clart"
        )
        asset_id = get_gee_asset_path(state, district, block) + description + "_fes"

        if not is_gee_asset_exists(asset_id):
            gcs_path = upload_tif_to_gcs(clart_filename, file_path)
            task_id = upload_tif_from_gcs_to_gee(gcs_path, asset_id, 30)
            check_task_status([task_id])

        if is_gee_asset_exists(asset_id):
            layer_id = save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=description,
                asset_id=asset_id,
                dataset_name="CLART",
            )
            print("saving fes clart layer info at the gee level...")
            make_asset_public(asset_id)

            res = sync_raster_gcs_to_geoserver(
                "clart", description + "_fes", description, "testClart"
            )
            if res:
                save_layer_info_to_db(
                    state,
                    district,
                    block,
                    layer_name=description,
                    asset_id=asset_id,
                    dataset_name="CLART",
                    sync_to_geoserver=True,
                    misc={"override_name": f"{description}_fes"},
                    is_override=True,
                )
                print("saving fes clart layer info at the geoserver level...")
            return res
    except Exception as e:
        raise e
