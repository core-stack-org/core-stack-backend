import ee
from nrm_app.celery import app
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    make_asset_public,
)

from computing.mws.evapotranspiration import merge_assets_chunked_on_year


@app.task(bind=True)
def tree_health_ch_vector(
    self, state, district, block, start_year, end_year, gee_account_id
):
    ee_initialize(gee_account_id)

    roi = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    yearly_assets = []

    for year in range(start_year, end_year + 1):
        print(f"Processing year {year}")

        task_list = generate_vector(roi, state, district, block, year)
        task_id_list = check_task_status([task_list])
        print("task_id_list ", task_id_list)

        year_asset_id = (
            get_gee_asset_path(state, district, block)
            + f"tree_health_ch_vector_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_{year}"
        )

        if is_gee_asset_exists(year_asset_id):
            yearly_assets.append(year_asset_id)

    description = (
        "tree_health_ch_vector_"
        + valid_gee_text(district)
        + "_"
        + valid_gee_text(block)
        + "_"
        + str(start_year)
        + "_"
        + str(end_year)
    )

    asset_id = get_gee_asset_path(state, district, block) + description
    if not is_gee_asset_exists(asset_id):
        task = merge_assets_chunked_on_year(yearly_assets, description, asset_id)
        task_id_list = check_task_status([task])

    merged_fc = ee.FeatureCollection(asset_id)
    sync_res = sync_fc_to_geoserver(merged_fc, state, description, "canopy_height")

    # layer_id = None
    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=description,
            asset_id=asset_id,
            dataset_name="Canopy Height Vector",
            misc={"start_year": start_year, "end_year": end_year},
        )

    try:
        layer_at_geoserver = False
        merged_fc = ee.FeatureCollection(asset_id)
        sync_res = sync_fc_to_geoserver(merged_fc, state, description, "canopy_height")
        if sync_res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
            layer_at_geoserver = True

    except Exception as e:
        print(f"Error syncing combined data to GeoServer: {e}")
        raise

    return layer_at_geoserver


def generate_vector(roi, state, district, block, year):
    """Generate vector data for a specific year based on raster data."""

    args = [
        {"value": 0, "label": "Short_Trees"},
        {"value": 1, "label": "Medium_Height_Trees"},
        {"value": 2, "label": "Tall_Trees"},
        {"value": 3, "label": "Missing_Data"},
    ]

    raster = ee.Image(
        get_gee_asset_path(state, district, block)
        + "tree_health_ch_raster_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_"
        + str(year)
    )

    fc = roi
    for arg in args:
        raster = raster.select(["ch_class"])
        mask = raster.eq(ee.Number(arg["value"]))
        pixel_area = ee.Image.pixelArea()
        forest_area = pixel_area.updateMask(mask)

        fc = forest_area.reduceRegions(
            collection=fc, reducer=ee.Reducer.sum(), scale=25, crs=raster.projection()
        )

        def process_feature(feature):
            value = feature.get("sum")
            value = ee.Number(value).multiply(0.0001)
            column_name = f"{arg['label']}_{year}"
            return feature.set(column_name, value)

        fc = fc.map(process_feature)

    description = (
        "tree_health_ch_vector_"
        + valid_gee_text(district)
        + "_"
        + valid_gee_text(block)
        + "_"
        + str(year)
    )
    task = export_vector_asset_to_gee(
        fc, description, get_gee_asset_path(state, district, block) + description
    )
    return task
