import ee
from nrm_app.celery import app
from computing.utils import (
    save_layer_info_to_db,
    update_layer_sync_status,
)
from projects.models import Project
from utilities.constants import GEE_PATHS, FABDEM
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    export_raster_asset_to_gee,
    export_vector_asset_to_gee,
    make_asset_public,
    get_gee_dir_path,
)

from computing.utils import (
    sync_layer_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)


@app.task(bind=True)
def generate_dem_layer(
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
    """
    It will generate dem raster layer for given location at tehsil level
    """
    print(f"Generating dem layer for {state} - {district} - {block}")

    ee_initialize(gee_account_id)
    if state and district and block:
        description = (
            valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_dem_raster"
        )
        asset_id = get_gee_asset_path(state, district, block) + description
        roi_boundary = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )

    else:
        roi_boundary = ee.FeatureCollection(roi_path)
        description = asset_suffix + "_dem_raster"

        asset_id = (
            get_gee_dir_path(
                asset_folder, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description
        )

    fabdem_img = ee.ImageCollection(FABDEM)
    dem_raster = (
        fabdem_img.mosaic()
        .setDefaultProjection("EPSG:3857", None, 30)
        .rename("elevation")
    )
    raster = dem_raster.clip(roi_boundary.geometry())

    # Generate raster Layer
    layer_status = dem_raster_generation(
        asset_id=asset_id,
        state=state,
        district=district,
        block=block,
        description=description,
        roi=roi_boundary,
        raster=raster,
        proj_id=proj_id,
    )

    vectorize_fabdem(roi_boundary, asset_id, state, district, block)

    return layer_status


def dem_raster_generation(
    raster,
    roi,
    proj_id=None,
    state=None,
    district=None,
    block=None,
    description=None,
    asset_id=None,
):
    workspacename = "dem"
    if proj_id:
        proj_obj = Project.objects.get(pk=proj_id)

    if not is_gee_asset_exists(asset_id):
        task_id = export_raster_asset_to_gee(
            image=raster,
            description=description,
            asset_id=asset_id,
            scale=30,
            region=roi.geometry(),
        )
        dem_task_id_list = check_task_status([task_id])
        print("DEM raster task_id list", dem_task_id_list)

    layer_id = None
    layer_at_geoserver = False
    if is_gee_asset_exists(asset_id):
        """Sync image to google cloud storage and then to geoserver"""
        image = ee.Image(asset_id)
        task_id = sync_raster_to_gcs(image, 30, description)

        task_id_list = check_task_status([task_id])
        print("task_id_list sync to gcs ", task_id_list)
        if state and district and block:
            layer_id = save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=description,
                asset_id=asset_id,
                dataset_name="DEM Raster",
            )
        make_asset_public(asset_id)
        res = sync_raster_gcs_to_geoserver(
            workspacename,
            description,
            description,
            "dem",
        )
        if res and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print(f"sync to geoserver flag is updated for {block}")
            layer_at_geoserver = True
    return layer_at_geoserver


def vectorize_fabdem(mws_fc, raster_asset_id, state, district, block):
    description = (
        valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_dem_vector"
    )

    dem = ee.Image(raster_asset_id).select("elevation")

    pixel_area = ee.Image.pixelArea()
    area_image = pixel_area.updateMask(dem.mask()).rename("area")

    combined = dem.addBands(area_image)

    reducer = (
        ee.Reducer.min()
        .setOutputs(["min"])
        .combine(ee.Reducer.max().setOutputs(["max"]), sharedInputs=True)
        .combine(ee.Reducer.mean().setOutputs(["mean"]), sharedInputs=True)
        .combine(ee.Reducer.sum().setOutputs(["sum"]), sharedInputs=True)
    )

    fc = combined.reduceRegions(
        collection=mws_fc,
        reducer=reducer,
        scale=dem.projection().nominalScale(),
        tileScale=4,
    )

    def process(feature):
        return feature.set(
            {
                "uid": feature.get("uid"),
                "area_in_ha": feature.get("area_in_ha"),
                "min_elevation": feature.get("elevation_min"),
                "max_elevation": feature.get("elevation_max"),
                "mean_elevation": feature.get("elevation_mean"),
            }
        ).select(
            ["uid", "area_in_ha", "min_elevation", "max_elevation", "mean_elevation"]
        )

    fc = fc.map(process)

    asset_id = get_gee_asset_path(state, district, block) + description

    if not is_gee_asset_exists(asset_id):
        task_id = export_vector_asset_to_gee(fc, description, asset_id=asset_id)
        check_task_status([task_id])

    layer_at_geoserver = False

    if is_gee_asset_exists(asset_id):
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=description,
            asset_id=asset_id,
            dataset_name="Dem Vector",
        )

        fc_geojson = fc.getInfo()
        fc_geojson = {"features": fc_geojson["features"], "type": fc_geojson["type"]}

        res = sync_layer_to_geoserver(
            state, fc_geojson, description, "digital_elevation_model"
        )

        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            layer_at_geoserver = True

    return layer_at_geoserver
