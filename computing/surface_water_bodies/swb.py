import ee
from computing.utils import (
    sync_fc_to_geoserver,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    make_asset_public,
)

from nrm_app.celery import app
from .swb1 import vectorize_water_pixels
from .swb2 import waterbody_mws_intersection
from .swb3 import waterbody_wbc_intersection


@app.task(bind=True)
def generate_swb_layer(
    self,
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    start_year=None,
    end_year=None,
):
    ee_initialize()
    if state and district and block:
        roi = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )

        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

    start_date = f"{start_year}-07-01"
    end_date = f"{end_year}-06-30"

    # SWB1: Vectorize LULC water pixels for our ROI
    swb1 = vectorize_water_pixels(
        roi=roi,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        start_date=start_date,
        end_date=end_date,
    )
    if swb1:
        task_id_list = check_task_status([swb1])
        print("SWB1 task completed - task_id_list", task_id_list)

    # SWB2: Intersect water bodies with micro-watershed to get unique ids for water bodies per micro-watershed
    swb2, asset_id = waterbody_mws_intersection(
        roi=roi,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
    )
    if swb2:
        task_id_list = check_task_status([swb2])
        print("SWB2 task completed - task_id_list:", task_id_list)
        make_asset_public(asset_id)
    layer_name = (
        "surface_waterbodies_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )
    fc = ee.FeatureCollection(asset_id)
    res = sync_fc_to_geoserver(fc, asset_suffix, layer_name, workspace="water_bodies")
    print(res)

    # SWB3: Intersect water bodies with WBC (Water Body Census) to get more data on intersecting water bodies
    swb3, asset_id = waterbody_wbc_intersection(
        roi=roi,
        state=state,  # Mandatory
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
    )
    if swb3:
        task_id_list = check_task_status([swb3])
        print("SWB task completed - swb3_task_id_list:", task_id_list)
        make_asset_public(asset_id)

    fc = ee.FeatureCollection(asset_id)
    res = sync_fc_to_geoserver(fc, asset_suffix, layer_name, workspace="water_bodies")
    print(res)
