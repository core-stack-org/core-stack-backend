import ee

from computing.utils import (
    sync_layer_to_geoserver,
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from projects.models import Project
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    check_task_status,
    make_asset_public,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    get_gee_dir_path,
)
from utilities.constants import GEE_DATASET_PATH, GEE_PATHS
from nrm_app.celery import app
from computing.STAC_specs import generate_STAC_layerwise


@app.task(bind=True)
def clip_drainage_lines(
    self,
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    asset_folder=None,
    gee_account_id=None,
    roi_path=None,
    app_type="MWS",
    proj_id=None,
):
    print("started drainage line")
    ee_initialize(gee_account_id)
    pan_india_drainage = ee.FeatureCollection(
        GEE_DATASET_PATH + "/drainage-line/pan_india_drainage_lines"
    )
    description = ""
    if state and district and block:
        description = f"drainage_lines_{district}_{block}"
        roi = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )
        asset_suffix = (
            f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
        )
        state_name = state
        asset_id = get_gee_asset_path(state, district, block) + description

    else:
        description = f"drainage_lines_{asset_suffix}"
        proj_obj = Project.objects.get(pk=proj_id)
        state_name = proj_obj.name
        roi = ee.FeatureCollection(roi_path)
        state = proj_obj.name
        asset_id = (
            get_gee_dir_path(
                [proj_obj.name], asset_path=GEE_PATHS["WATER_REJ"]["GEE_ASSET_PATH"]
            )
            + asset_suffix
        )

    print(asset_id)
    clipped_drainage = pan_india_drainage.filterBounds(roi.geometry())

    task = export_vector_asset_to_gee(clipped_drainage, description, asset_id)

    task_id_list = check_task_status([task])
    print("task_id_list", task_id_list)
    make_asset_public(asset_id)
    layer_at_geoserver = False
    if is_gee_asset_exists(asset_id):
        if state and district and block:
            layer_id = save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=asset_suffix,
                asset_id=asset_id,
                dataset_name="Drainage",
            )

        try:
            # Load feature collection from Earth Engine
            fc = ee.FeatureCollection(asset_id)
            res = sync_fc_to_geoserver(fc, state_name, asset_suffix, "drainage")
            print("Drainage line synced to geoserver:", res)
            if res["status_code"] == 201 and layer_id:
                update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
                print("sync to geoserver flag is updated")

                layer_STAC_generated = False
                if state and district and block:
                    layer_STAC_generated = generate_STAC_layerwise.generate_vector_stac(
                        state=state,
                        district=district,
                        block=block,
                        layer_name="drainage_lines_vector",
                    )
                    update_layer_sync_status(
                        layer_id=layer_id, is_stac_specs_generated=layer_STAC_generated
                    )

                    layer_at_geoserver = True

        except Exception as e:
            print("Exception in syncing Drainage line to geoserver", e)
    return layer_at_geoserver
