import ee
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
    get_layer_object,
)
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_dir_path,
    is_gee_asset_exists,
    make_asset_public,
)
from .generate_layers import generate_drought_layers
from .merge_layers import (
    merge_drought_layers_chunks,
    merge_yearly_layers,
)
from nrm_app.celery import app
from computing.STAC_specs import generate_STAC_layerwise


@app.task(bind=True)
def calculate_drought(
    self,
    state=None,
    district=None,
    block=None,
    roi_path=None,
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

        roi_path = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + f"filtered_mws_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_uid"
        )

    dst_filename = "drought_" + asset_suffix

    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + dst_filename
    )
    roi = ee.FeatureCollection(roi_path)
    layer_name = asset_suffix + "_drought"

    if is_gee_asset_exists(asset_id):
        layer_obj = None
        try:
            layer_obj = get_layer_object(
                state,
                district,
                block,
                layer_name=layer_name,
                dataset_name="Drought",
            )
        except Exception as e:
            print("DB layer not found for drought.")

        existing_end_year = get_last_date(asset_id, layer_obj)

        if existing_end_year < end_year:
            generate_drought_yearly(
                app_type,
                asset_folder_list,
                asset_suffix,
                existing_end_year,
                end_year,
                gee_account_id,
                roi,
            )
    else:
        generate_drought_yearly(
            app_type,
            asset_folder_list,
            asset_suffix,
            start_year,
            end_year,
            gee_account_id,
            roi,
        )
    task_id = merge_yearly_layers(
        asset_suffix,
        asset_folder_list,
        app_type,
        start_year,
        end_year,
        gee_account_id,
    )
    check_task_status([task_id])

    return push_to_geoserver_db_stc(
        asset_id, block, district, end_year, layer_name, start_year, state
    )


def generate_drought_yearly(
    app_type, asset_folder_list, asset_suffix, start_year, end_year, gee_account_id, roi
):
    chunk_size = 30  # if shapefile is large, running the script on the complete file will result an error,
    # so divide into chunks and run on the chunks when the chunks are got exported,
    # then the next joining script join the chunks
    current_year = start_year
    merged_tasks = []
    yearly_assets = []
    while current_year <= end_year:
        print("current_year", current_year)
        yearly_drought = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + f"drought_{asset_suffix}_{current_year}_v2"
        )
        yearly_assets.append(yearly_drought)
        if not is_gee_asset_exists(yearly_drought):
            generate_drought_layers(
                roi,
                asset_suffix,
                asset_folder_list,
                app_type,
                current_year,
                start_year,
                end_year,
                chunk_size,
                gee_account_id,
            )

            task_id = merge_drought_layers_chunks(
                roi,
                asset_suffix,
                asset_folder_list,
                app_type,
                current_year,
                chunk_size,
                gee_account_id,
            )
            if task_id:
                merged_tasks.append(task_id)
        current_year += 1
    merged_task_ids = check_task_status(merged_tasks)
    print("All years' asset generated, task id: ", merged_task_ids)
    for asset in yearly_assets:
        make_asset_public(asset)


def push_to_geoserver_db_stc(
    asset_id, block, district, end_year, layer_name, start_year, state
):
    print("Inside push_to_geoserver_db_stc")
    layer_at_geoserver = False
    if is_gee_asset_exists(asset_id):
        layer_id = None
        if state and district and block:
            layer_id = save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=layer_name,
                asset_id=asset_id,
                dataset_name="Drought",
                algorithm="MOD09A1-NDVI/NDWI",
                algorithm_version="2.0",
                misc={"start_year": start_year, "end_year": end_year},
            )

        make_asset_public(asset_id)

        fc = ee.FeatureCollection(asset_id)

        res = sync_fc_to_geoserver(fc, state, layer_name, "drought")
        print(res)
        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag updated")
            layer_at_geoserver = True

            layer_STAC_generated = False
            layer_STAC_generated = generate_STAC_layerwise.generate_vector_stac(
                state=state,
                district=district,
                block=block,
                layer_name="drought_frequency_vector",
            )  # we only intend to run
            # stac for drought frequency as it has style file. so please double check
            # if this data is only of drought frequency and not additionally containing
            # drought causalty data
    return layer_at_geoserver


def get_last_date(asset_id, layer_obj):
    if layer_obj:
        existing_end_year = int(layer_obj.misc["end_year"])
    else:
        fc = ee.FeatureCollection(asset_id)
        col_names = fc.first().propertyNames().getInfo()
        filtered_col = [
            col.split("_")[1] for col in col_names if col.startswith("drlb_")
        ]
        filtered_col.sort()
        existing_end_year = filtered_col[-1]

    return int(existing_end_year)
