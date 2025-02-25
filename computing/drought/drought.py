import ee
from computing.utils import (
    sync_layer_to_geoserver,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
)
from .generate_layers import generate_drought_layers
from .merge_layers import (
    merge_drought_layers_chunks,
    merge_yearly_layers,
)
from nrm_app.celery import app


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

        while current_year <= end_year:
            print("current_year", current_year)
            if not is_gee_asset_exists(
                get_gee_asset_path(state, district, block)
                + "drought_"
                + valid_gee_text(district.lower())
                + "_"
                + valid_gee_text(block.lower())
                + "_"
                + str(current_year)
            ):
                task_ids = generate_drought_layers(
                    aoi,
                    state,
                    district,
                    block,
                    current_year,
                    start_year,
                    end_year,
                    chunk_size_,
                )
                task_id_list = check_task_status(task_ids)
                print("All chunks' asset generated, task id: ", task_id_list)
                task_id = merge_drought_layers_chunks(
                    aoi, state, district, block, current_year, chunk_size_
                )
                if task_id:
                    merged_tasks.append(task_id)
            current_year += 1

        merged_task_ids = check_task_status(merged_tasks)
        print("All years' asset generated, task id: ", merged_task_ids)

        task_id = merge_yearly_layers(state, district, block, start_year, end_year)
        if task_id:
            check_task_status([task_id])

    fc = ee.FeatureCollection(asset_id)
    fc = fc.toList(fc.size()).getInfo()
    fc = {"features": fc, "type": "FeatureCollection"}
    res = sync_layer_to_geoserver(
        state,
        fc,
        valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_drought",
        "cropping_drought",
    )

    print(res)
