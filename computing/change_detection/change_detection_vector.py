import ee
from computing.utils import (
    sync_layer_to_geoserver,
    save_layer_info_to_db
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
)
from nrm_app.celery import app


@app.task(bind=True)
def vectorise_change_detection(self, state, district, block):
    ee_initialize()
    roi = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    task_list = [
        afforestation_vector(roi, state, district, block),
        deforestation_vector(roi, state, district, block),
        degradation_vector(roi, state, district, block),
        urbanization_vector(roi, state, district, block),
        crop_intensity_vector(roi, state, district, block),
    ]

    print(task_list)
    task_id_list = check_task_status(task_list)
    print("Change vector task completed - task_id_list:", task_id_list)

    sync_change_to_geoserver(block, district, state)


def afforestation_vector(roi, state, district, block):
    args = [
        {"value": 1, "label": "fo_fo"},
        {"value": 2, "label": "bu_fo"},
        {"value": 3, "label": "fa_fo"},
        {"value": 4, "label": "ba_fo"},
        {"value": 5, "label": "sc_fo"},
        {"value": [2, 3, 4, 5], "label": "total_aff"},
    ]  # Classes in afforestation raster layer

    return generate_vector(roi, args, state, district, block, "Afforestation")


def deforestation_vector(roi, state, district, block):
    args = [
        {"value": 1, "label": "fo_fo"},
        {"value": 2, "label": "fo_bu"},
        {"value": 3, "label": "fo_fa"},
        {"value": 4, "label": "fo_ba"},
        {"value": 5, "label": "fo_sc"},
        {"value": [2, 3, 4, 5], "label": "total_def"},
    ]  # Classes in deforestation raster layer

    return generate_vector(roi, args, state, district, block, "Deforestation")


def degradation_vector(roi, state, district, block):

    args = [
        {"value": 1, "label": "f_f"},
        {"value": 2, "label": "f_bu"},
        {"value": 3, "label": "f_ba"},
        {"value": 4, "label": "f_sc"},
        {"value": [2, 3, 4], "label": "total_deg"},
    ]  # Classes in degradation raster layer

    return generate_vector(roi, args, state, district, block, "Degradation")


def urbanization_vector(roi, state, district, block):
    args = [
        {"value": 1, "label": "bu_bu"},
        {"value": 2, "label": "w_bu"},
        {"value": 3, "label": "tr_bu"},
        {"value": 4, "label": "b_bu"},
        {"value": [2, 3, 4], "label": "total_urb"},
    ]  # Classes in urbanization raster layer

    return generate_vector(roi, args, state, district, block, "Urbanization")


def crop_intensity_vector(roi, state, district, block):

    args = [
        {"value": 1, "label": "do_si"},
        {"value": 2, "label": "tr_si"},
        {"value": 3, "label": "tr_do"},
        {"value": 4, "label": "si_do"},
        {"value": 5, "label": "si_tr"},
        {"value": 6, "label": "do_tr"},
        {"value": 7, "label": "same"},
        {"value": [1, 2, 3, 4, 5, 6], "label": "total_change"},
    ]  # Classes in crop_intensity raster layer

    return generate_vector(roi, args, state, district, block, "CropIntensity")


def generate_vector(roi, args, state, district, block, layer_name):
    raster = ee.Image(
        get_gee_asset_path(state, district, block)
        + "change_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_"
        + layer_name
    )  # Change detection raster layer

    fc = roi
    for arg in args:
        raster = raster.select(["constant"])
        if isinstance(arg["value"], list) and len(arg["value"]) > 1:
            ored_str = "raster.eq(ee.Number(" + str(arg["value"][0]) + "))"
            for i in range(1, len(arg["value"])):
                ored_str = (
                    ored_str + ".Or(raster.eq(ee.Number(" + str(arg["value"][i]) + ")))"
                )
            print(ored_str)
            mask = eval(ored_str)
        else:
            mask = raster.eq(ee.Number(arg["value"]))

        pixel_area = ee.Image.pixelArea()
        forest_area = pixel_area.updateMask(mask)

        fc = forest_area.reduceRegions(
            collection=fc, reducer=ee.Reducer.sum(), scale=10, crs=raster.projection()
        )

        def remove_property(feat, prop):
            properties = feat.propertyNames()
            select_properties = properties.filter(ee.Filter.neq("item", prop))
            return feat.select(select_properties)

        def process_feature(feature):
            value = feature.get("sum")
            value = ee.Number(value).multiply(0.0001)
            feature = feature.set(arg["label"], value)
            feature = remove_property(feature, "sum")
            return feature

        fc = fc.map(process_feature)

    fc = ee.FeatureCollection(fc)

    description = (
        "change_vector_"
        + valid_gee_text(district)
        + "_"
        + valid_gee_text(block)
        + "_"
        + layer_name
    )

    task = ee.batch.Export.table.toAsset(
        **{
            "collection": fc,
            "description": description,
            "assetId": get_gee_asset_path(state, district, block) + description,
        }
    )
    task.start()
    return task.status()["id"]
    save_layer_info_to_db(
        state, 
        district, 
        block, 
        f"change_vector_{district.title()}_{block.title()}", 
        asset_id=f"{get_gee_asset_path(state, district, block) + description}", 
        workspace_name='Change Detection Vector'
        )


def sync_change_to_geoserver(block, district, state):
    param_list = [
        "Urbanization",
        "Degradation",
        "Deforestation",
        "Afforestation",
        "CropIntensity",
    ]
    for param in param_list:
        asset_id = (
            get_gee_asset_path(state, district, block)
            + "change_vector_"
            + valid_gee_text(district)
            + "_"
            + valid_gee_text(block)
            + "_"
            + param
        )
        fc = ee.FeatureCollection(asset_id).getInfo()
        fc = {"features": fc["features"], "type": fc["type"]}
        res = sync_layer_to_geoserver(
            state,
            fc,
            "change_vector_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_"
            + param,
            "change_detection",
        )
        print(res)
