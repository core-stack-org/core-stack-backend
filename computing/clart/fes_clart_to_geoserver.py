import ee
import os
from nrm_app.settings import BASE_DIR
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
from nrm_app.celery import app
from computing.utils import (
    get_layer_object,
)
from computing.models import *
from computing.STAC_specs import generate_STAC_layerwise
from computing.utils import update_layer_sync_status



@app.task(bind=True)
def generate_fes_clart_layer(self, state, district, block, file_path, gee_account_id):
    print("Inside generate_fes_clart_layer")
    ee_initialize(gee_account_id)

    try:
        clart_filename = os.path.basename(file_path)

        description = (
            valid_gee_text(district) + "_" + valid_gee_text(block) + "_clart"
        )
        asset_id = get_gee_asset_path(state, district, block) + description + "_fes"

        if not is_gee_asset_exists(asset_id):
            gcs_path = upload_tif_to_gcs(clart_filename, file_path)
            task_id = upload_tif_from_gcs_to_gee(gcs_path, asset_id, 30)
            check_task_status([task_id])

        if is_gee_asset_exists(asset_id):
            layer_obj = get_layer_object(state, district, block, description, "CLART")
            make_asset_public(asset_id)

            res = sync_raster_gcs_to_geoserver(
                "clart", description + "_fes", description, "testClart"
            )
            if res:
                Layer.objects.filter(id=layer_obj.pk).update(
                    is_sync_to_geoserver=True,
                    misc={"override_asset_id": asset_id},
                    is_override=True,
                )

                generate_STAC_layerwise.generate_raster_stac(state=state,district=district,block=block,layer_name='clart_raster')
                update_layer_sync_status(layer_id=Layer.id, is_stac_specs_generated=True)
                print("Stac Specs generated and updated")
            return res
    except Exception as e:
        raise e
