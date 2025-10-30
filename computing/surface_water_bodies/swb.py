import ee
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    make_asset_public,
    get_gee_dir_path,
    is_gee_asset_exists,
)

from nrm_app.celery import app
from .swb1 import vectorize_water_pixels
from .swb2 import waterbody_mws_intersection
from .swb3 import waterbody_wbc_intersection
from computing.STAC_specs import generate_STAC_layerwise


@app.task(bind=True)
def generate_swb_layer(
    self,
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    start_year=None,
    end_year=None,
    gee_account_id=None,
):
    ee_initialize(gee_account_id)
    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

        roi = ee.FeatureCollection(
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )

    start_date = f"{start_year}-07-01"
    end_date = f"{end_year}-06-30"

    # SWB1: Vectorize LULC water pixels for our ROI
    swb1 = vectorize_water_pixels(
        roi=roi,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        app_type=app_type,
        start_date=start_date,
        end_date=end_date,
    )
    if swb1:
        task_id_list = check_task_status([swb1])

        print("SWB1 task completed - task_id_list", task_id_list)

    # SWB2: Intersect water bodies with micro-watershed to get unique ids for water bodies per micro-watershed
    layer_name = "surface_waterbodies_" + asset_suffix

    swb2, asset_id = waterbody_mws_intersection(
        roi=roi,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        app_type=app_type,
    )
    if swb2:
        task_id_list = check_task_status([swb2])
        print("SWB2 task completed - task_id_list:", task_id_list)

    layer_at_geoserver = sync_asset_to_db_and_geoserver(
        asset_id, layer_name, asset_suffix, start_date, end_date, state, district, block
    )

    # SWB3: Intersect water bodies with WBC (Water Body Census) to get more data on intersecting water bodies
    swb3, asset_id = waterbody_wbc_intersection(
        roi=roi,
        state=state,  # Mandatory
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        app_type=app_type,
    )
    if swb3:
        task_id_list = check_task_status([swb3])
        print("SWB task completed - swb3_task_id_list:", task_id_list)

    layer_at_geoserver = sync_asset_to_db_and_geoserver(
        asset_id,
        layer_name,
        asset_suffix,
        start_date,
        end_date,
        state,
        district,
        block,
    )
    return layer_at_geoserver


def sync_asset_to_db_and_geoserver(
    asset_id,
    layer_name,
    asset_suffix,
    start_date,
    end_date,
    state=None,
    district=None,
    block=None,
    dataset_name="Surface Water Bodies",
    workspace="swb",
):
    layer_at_geoserver = False
    if is_gee_asset_exists(asset_id):
        layer_id = None
        if state and district and block:
            layer_id = save_layer_info_to_db(
                state=state,
                district=district,
                block=block,
                layer_name=layer_name,
                asset_id=asset_id,
                dataset_name=dataset_name,
                misc={
                    "start_year": start_date.split("-")[0],
                    "end_year": end_date.split("-")[0],
                },
            )
            print(f"layer id returned i.e. {layer_id}")

        make_asset_public(asset_id)

        fc = ee.FeatureCollection(asset_id)
        res = sync_fc_to_geoserver(fc, asset_suffix, layer_name, workspace=workspace)
        print(res)

        if res.get("status_code") == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag updated")

            generate_STAC_layerwise.generate_vector_stac(state=state,district=district,block=block,layer_name='surface_water_bodies_vector')
            update_layer_sync_status(layer_id=layer_id, is_stac_specs_generated=True)
            print("Stac Specs generated and updated")
            layer_at_geoserver = True
    return layer_at_geoserver
