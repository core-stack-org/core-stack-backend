import ee
from computing.utils import (
    sync_layer_to_geoserver,
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
    merge_fc_into_existing_fc,
)
from nrm_app.celery import app
from computing.models import *


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
    description = (
        "lulc_vector_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )
    asset_id = get_gee_asset_path(state, district, block) + description
    if is_gee_asset_exists(asset_id):
        layer_obj = None
        try:
            dataset = Dataset.objects.get(name="LULC")
            layer_obj = Layer.objects.get(
                dataset=dataset,
                layer_name=description,
            )
        except Exception as e:
            print(
                "layer not found for lulc vector. So, reading the column name from asset_id."
            )
        if layer_obj:
            existing_end_date = int(layer_obj.misc["end_year"])
        else:
            roi = ee.FeatureCollection(asset_id)
            col_names = roi.first().propertyNames().getInfo()
            filtered_col = [col for col in col_names if col.startswith("tree")]
            filtered_col.sort()
            existing_end_date = int(filtered_col[-1].split("_")[-1])
        print("existing_end_date", existing_end_date)
        print("end_year", end_year)
        if existing_end_date < end_year:
            new_start_year = existing_end_date
            new_asset_id = f"{asset_id}_{new_start_year}_{end_year}"
            new_description = f"{description}_{new_start_year}_{end_year}"
            if not is_gee_asset_exists(new_asset_id):
                generate_vector(
                    start_year=new_start_year,
                    end_year=end_year,
                    state=state,
                    district=district,
                    block=block,
                    description=new_description,
                    asset_id=new_asset_id,
                    fc=fc,
                )

                if is_gee_asset_exists(new_asset_id):
                    merge_fc_into_existing_fc(asset_id, description, new_asset_id)

                sync_to_db_and_geoserver(
                    asset_id=asset_id,
                    state=state,
                    district=district,
                    block=block,
                    description=description,
                    start_year=start_year,
                    end_year=end_year,
                )
                return "merging completed..."
        return "already upto date "
    else:
        generate_vector(
            start_year=start_year,
            end_year=end_year,
            state=state,
            district=district,
            block=block,
            description=description,
            asset_id=asset_id,
            fc=fc,
        )
        sync_to_db_and_geoserver(
            asset_id=asset_id,
            state=state,
            district=district,
            block=block,
            description=description,
            start_year=start_year,
            end_year=end_year,
        )
        return "generated new layer"


def generate_vector(
    start_year, end_year, state, district, block, description, asset_id, fc
):
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
    task = export_vector_asset_to_gee(fc, description, asset_id)
    task_status = check_task_status([task])
    print("Task completed - ", task_status)


def sync_to_db_and_geoserver(
    asset_id, state, district, block, description, start_year, end_year
):
    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)

        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=description,
            asset_id=asset_id,
            dataset_name="LULC",
            misc={
                "start_year": start_year,
                "end_year": end_year,
            },
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
        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag updated")
