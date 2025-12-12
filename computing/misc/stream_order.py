import ee

from constants.pan_india_path import PAN_INDIA_SO
from nrm_app.celery import app
from computing.utils import (
    save_layer_info_to_db,
    update_layer_sync_status,
    sync_layer_to_geoserver,
)
from projects.models import Project
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    export_raster_asset_to_gee,
    make_asset_public,
    get_gee_dir_path,
    export_vector_asset_to_gee,
)


@app.task(bind=True)
def generate_stream_order(
    self,
    state=None,
    district=None,
    block=None,
    gee_account_id=None,
    proj_id=None,
    roi_path=None,
    asset_suffix=None,
    asset_folder=None,
    app_type="MWS",
):

    ee_initialize(gee_account_id)
    proj_obj = None
    if state and district and block:
        description = (
            "stream_order_" + valid_gee_text(district) + "_" + valid_gee_text(block)
        )

        roi = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )
        asset_id_raster = (
            get_gee_asset_path(state, district, block) + description + "_raster"
        )
    else:
        proj_obj = Project.objects.get(pk=proj_id)
        description = (
            "stream_order_"
            + valid_gee_text(proj_obj.name)
            + "_"
            + valid_gee_text(str(proj_id))
        ).lower()

        roi = ee.FeatureCollection(roi_path)
        asset_id_raster = (
            get_gee_dir_path(
                [proj_obj.name], asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description
            + "_raster"
        )

    stream_order_raster = ee.Image(PAN_INDIA_SO)
    raster = stream_order_raster.clip(roi.geometry())

    # Generate raster Layer
    stream_order_raster_generation(
        raster,
        state,
        district,
        block,
        description=description,
        roi=roi,
        raster_asset_id=asset_id_raster,
        proj_id=proj_id,
    )
    args = [
        {"value": 1, "label": "1"},
        {"value": 2, "label": "2"},
        {"value": 3, "label": "3"},
        {"value": 4, "label": "4"},
        {"value": 5, "label": "5"},
        {"value": 6, "label": "6"},
        {"value": 7, "label": "7"},
        {"value": 8, "label": "8"},
        {"value": 9, "label": "9"},
        {"value": 10, "label": "10"},
        {"value": 11, "label": "11"},
    ]

    fc = calculate_pixel_area_percentage(args, roi, raster)
    # Generate vector Layer
    layer_at_geoserver = stream_order_vector_generation(
        state, district, block, description, fc
    )
    return layer_at_geoserver


def stream_order_raster_generation(
    raster,
    state,
    district,
    block,
    description=None,
    roi=None,
    raster_asset_id=None,
    proj_id=None,
):

    if not is_gee_asset_exists(raster_asset_id):
        task_id = export_raster_asset_to_gee(
            image=raster,
            description=description + "_raster",
            asset_id=raster_asset_id,
            scale=30,
            region=roi.geometry(),
        )
        stream_order_task_id_list = check_task_status([task_id])
        print("steam order task_id list", stream_order_task_id_list)

    layer_id = None
    layer_at_geoserver = False
    if is_gee_asset_exists(raster_asset_id):
        """Sync image to google cloud storage and then to geoserver"""
        image = ee.Image(raster_asset_id)
        task_id = sync_raster_to_gcs(image, 30, description + "_raster")

        task_id_list = check_task_status([task_id])
        print("task_id_list sync to gcs ", task_id_list)
        if state and district and block:
            layer_id = save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=description + "_raster",
                asset_id=raster_asset_id,
                dataset_name="Stream Order",
            )
            workspace_name = "stream_order"

        else:
            proj_obj = Project.objects.get(pk=proj_id)
            workspace_name = "stream_order"
        make_asset_public(raster_asset_id)
        res = sync_raster_gcs_to_geoserver(
            workspace_name,
            description + "_raster",
            description + "_raster",
            "stream_order",
        )
        if res and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
            layer_at_geoserver = True
    return layer_at_geoserver


def stream_order_vector_generation(state, district, block, description, fc):
    vector_asset_id = (
        get_gee_asset_path(state, district, block) + description + "_vector"
    )
    if not is_gee_asset_exists(vector_asset_id):
        task = export_vector_asset_to_gee(fc, description + "_vector", vector_asset_id)
        check_task_status([task])
    if is_gee_asset_exists(vector_asset_id):
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=description + "_vector",
            asset_id=vector_asset_id,
            dataset_name="Stream Order",
        )
        make_asset_public(vector_asset_id)

        # Sync to geoserver
        fc = ee.FeatureCollection(vector_asset_id).getInfo()
        fc = {"features": fc["features"], "type": fc["type"]}
        res = res = sync_layer_to_geoserver(
            state, fc, description + "_vector", "stream_order"
        )
        print(res)
        layer_at_geoserver = False
        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
            layer_at_geoserver = True
        return layer_at_geoserver
    return False


def calculate_pixel_area_percentage(class_labels, fc, raster):
    pixel_area = ee.Image.pixelArea()
    fc_with_total = pixel_area.reduceRegions(
        collection=fc,
        reducer=ee.Reducer.sum(),
        scale=10,
        crs=raster.projection(),
    )

    def add_total_area(feature):
        total_area = ee.Number(feature.get("sum"))
        return feature.set("total_area", total_area)

    fc_with_total = fc_with_total.map(add_total_area)

    for arg in class_labels:
        raster_band = raster.select(["b1"])
        mask = raster_band.eq(ee.Number(arg["value"]))
        class_area = pixel_area.updateMask(mask)

        fc_with_total = class_area.reduceRegions(
            collection=fc_with_total,
            reducer=ee.Reducer.sum(),
            scale=30,
            crs=raster.projection(),
        )

        def process_feature(feature):
            class_area_sum = ee.Number(feature.get("sum"))
            total_area = ee.Number(feature.get("total_area"))
            percentage = class_area_sum.divide(total_area).multiply(100)
            percentage = ee.Algorithms.If(total_area.gt(0), percentage, 0)
            feature = feature.set(arg["label"], percentage)
            return feature

        fc_with_total = fc_with_total.map(process_feature)

    def clean_properties(feature):
        return (
            feature.select(propertySelectors=[".*"], retainGeometry=True)
            .set("sum", None)
            .set("total_area", None)
        )

    fc_final = ee.FeatureCollection(fc_with_total).map(clean_properties)

    return fc_final
