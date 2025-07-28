import ee
from computing.utils import sync_layer_to_geoserver, save_layer_info_to_db
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
)
from nrm_app.celery import app


@app.task(bind=True)
def vectorise_lulc(self, state, district, block, start_year, end_year):
    ee_initialize()
    fc = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    lulc_list = []
    s_year = start_year  # START_YEAR
    while s_year <= end_year:
        lulc_list.append(
            ee.Image(
                get_gee_asset_path(state, district, block)
                + valid_gee_text(district.lower())
                + "_"
                + valid_gee_text(block.lower())
                + "_"
                + str(s_year)
                + "-07-01_"
                + str(s_year + 1)
                + "-06-30_LULCmap_10m"
            )
        )
        s_year += 1

    lulc = ee.List(lulc_list)

    # 0 - Background
    # 1 - Built-up
    # 2 - Water in Kharif
    # 3 - Water in Kharif+Rabi
    # 4 - Water in Kharif+Rabi+Zaid
    # 6 - Tree/Forests
    # 7 - Barrenlands
    # 8 - Single cropping cropland
    # 9 - Single Non-Kharif cropping cropland
    # 10 - Double cropping cropland
    # 11 - Triple cropping cropland
    # 12 - Shrub_Scrub

    args = [
        {"label": 1, "txt": "built-up_area_"},
        {"label": 2, "txt": "k_water_area_"},
        {"label": 3, "txt": "kr_water_area_"},
        {"label": 4, "txt": "krz_water_area_"},
        {"label": 5, "txt": "cropland_area_"},
        {"label": 6, "txt": "tree_forest_area_"},
        {"label": 7, "txt": "barrenlands_area_"},
        {"label": 8, "txt": "single_kharif_cropped_area_"},
        {"label": 9, "txt": "single_non_kharif_cropped_area_"},
        {"label": 10, "txt": "doubly_cropped_area_"},
        {"label": 11, "txt": "triply_cropped_area_"},
        {"label": 12, "txt": "shrub_scrub_area_"},
    ]

    def res(feature):
        value = feature.get("sum")
        value = ee.Number(value).divide(10000)
        return feature.set(arg["txt"] + str(sy), value)

    for arg in args:
        s_year = start_year
        while s_year <= end_year:
            sy = s_year
            image = ee.Image(lulc.get(sy - start_year)).select(["predicted_label"])
            mask = image.eq(ee.Number(arg["label"]))
            pixel_area = ee.Image.pixelArea()
            forest_area = pixel_area.updateMask(mask)
            fc = forest_area.reduceRegions(fc, ee.Reducer.sum(), 10, image.projection())
            s_year += 1
            fc = fc.map(res)

    fc = ee.FeatureCollection(fc)

    description = (
        "lulc_vector_" + valid_gee_text(district) + "_" + valid_gee_text(block)
    )
    asset_id = get_gee_asset_path(state, district, block) + description
    task = export_vector_asset_to_gee(fc, description, asset_id)
    task_status = check_task_status([task])
    print("Task completed - ", task_status)
    if is_gee_asset_exists(asset_id):
        save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=description,
            asset_id=asset_id,
            dataset_name="LULC",
        )

    fc = ee.FeatureCollection(asset_id).getInfo()

    fc = {"features": fc["features"], "type": fc["type"]}
    res = sync_layer_to_geoserver(
        state,
        fc,
        "lulc_vector_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower()),
        "lulc_vector",
    )
    print(res)
    if res["status_code"] == 201:
        save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=description,
            asset_id=asset_id,
            dataset_name="LULC",
            sync_to_geoserver=True,
        )
