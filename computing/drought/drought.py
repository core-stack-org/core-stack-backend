import ee
from utilities.constants import GEE_HELPER_PATH
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    make_asset_public,
)
from .generate_layers import generate_drought_layers
from .merge_layers import (
    merge_drought_layers_chunks,
    merge_yearly_layers,
)
from nrm_app.celery import app
from computing.utils import sync_fc_to_geoserver


@app.task(bind=True)
def calculate_drought(self, state, district, block, start_year, end_year):
    ee_initialize()

    dst_filename = (
        "drought_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_"
        + str(start_year)
        + "_"
        + str(end_year)
    )

    asset_id = get_gee_asset_path(state, district, block) + dst_filename

    if not is_gee_asset_exists(asset_id):
        aoi = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )

        chunk_size_ = 30  # if shapefile is large, running the script on the complete file will result an error,
        # so divide into chunks and run on the chunks when the chunks are got exported,
        # then the next joining script join the chunks
        current_year = start_year
        merged_tasks = []
        yearly_assets = []
        while current_year <= end_year:
            print("current_year", current_year)
            yearly_drought = (
                get_gee_asset_path(state, district, block, GEE_HELPER_PATH)
                + "drought_"
                + valid_gee_text(district.lower())
                + "_"
                + valid_gee_text(block.lower())
                + "_"
                + str(current_year)
            )
            yearly_assets.append(yearly_drought)
            if not is_gee_asset_exists(yearly_drought):
                generate_drought_layers(
                    aoi,
                    state,
                    district,
                    block,
                    current_year,
                    start_year,
                    end_year,
                    chunk_size_,
                )

                task_id = merge_drought_layers_chunks(
                    aoi, state, district, block, current_year, chunk_size_
                )
                if task_id:
                    merged_tasks.append(task_id)
            current_year += 1

        merged_task_ids = check_task_status(merged_tasks)
        print("All years' asset generated, task id: ", merged_task_ids)

        for asset in yearly_assets:
            make_asset_public(asset)

        merge_yearly_layers(state, district, block, start_year, end_year)

    fc = ee.FeatureCollection(asset_id)
    description = valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower()) + "_drought"
    res = sync_fc_to_geoserver(fc, state, description, 'cropping_drought')
    print(res)
