"""
Clean Forest Fire pipeline.

Uses MODIS Terra and Aqua active fire products to compute fire incident
metrics for each micro-watershed.
"""

import ee

from computing.utils import (
    save_layer_info_to_db,
    sync_fc_to_geoserver,
    update_layer_sync_status,
)
from nrm_app.celery import app
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    check_task_status,
    ee_initialize,
    export_vector_asset_to_gee,
    get_gee_dir_path,
    is_gee_asset_exists,
    make_asset_public,
    valid_gee_text,
)

SCALE = 1000
TILE_SCALE = 4

TERRA_FIRE_PATH = "MODIS/061/MOD14A1"
AQUA_FIRE_PATH = "MODIS/061/MYD14A1"
FIRE_BAND = "MaxFRP"

METRIC_FIELDS = [
    "uid",
    "fire_frp_sum_per_year",
    "fire_frp_mean",
    "fire_frp_max",
    "fire_count_per_year",
]


@app.task(bind=True)
def generate_forest_fire_layer_updated(
        self,
        state,
        district,
        block,
        start_year=2001,
        end_year=2022,
        gee_account_id=None,
        app_type="MWS",
):
    """
    Generate MODIS fire metrics as a vector layer for each MWS feature.
    """

    ee_initialize(gee_account_id)

    start_year = int(start_year)
    end_year = int(end_year)
    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    n_years = end_year - start_year + 1
    asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    )
    asset_folder_list = [state, district, block]

    gee_base_path = get_gee_dir_path(
        asset_folder_list,
        asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
    )
    description = f"forest_fire_{asset_suffix}_{start_year}_{end_year}"
    layer_name = f"{asset_suffix}_forest_fire"
    asset_id = gee_base_path + description

    print(f"Forest Fire updated pipeline started: {asset_id=}")

    if not is_gee_asset_exists(asset_id):
        roi_path = (
                gee_base_path
                + f"filtered_mws_{valid_gee_text(district.lower())}"
                + f"_{valid_gee_text(block.lower())}_uid"
        )
        mws_fc = _prepare_mws_features(roi_path)

        fire_collection = _load_fire_collection(start_year, end_year)
        fire_count = fire_collection.size().getInfo()
        print("Forest Fire MODIS image count:", fire_count)
        if fire_count == 0:
            raise ValueError(
                "No MODIS fire images found for "
                f"{start_year}-01-01 to {end_year}-12-31"
            )

        projection = ee.Image(fire_collection.first()).select(FIRE_BAND).projection()
        metric_images = _build_metric_images(fire_collection, n_years, projection)

        fc = _add_fire_metrics(mws_fc, metric_images, projection)
        fc = fc.select(METRIC_FIELDS)

        task_id = export_vector_asset_to_gee(fc, description, asset_id)
        if task_id:
            check_task_status([task_id])
            print("Forest Fire updated layer exported to GEE.")

    return _save_to_db_and_sync_to_geoserver(
        layer_name=layer_name,
        asset_id=asset_id,
        start_year=start_year,
        end_year=end_year,
        asset_suffix=asset_suffix,
        state=state,
        district=district,
        block=block,
    )


def _load_fire_collection(start_year, end_year):
    start_date = f"{start_year}-07-01"
    end_date = f"{end_year}-07-01"

    terra = ee.ImageCollection(TERRA_FIRE_PATH)
    aqua = ee.ImageCollection(AQUA_FIRE_PATH)

    return terra.merge(aqua).filterDate(start_date, end_date).select(FIRE_BAND)


def _prepare_mws_features(roi_path):
    fc = ee.FeatureCollection(roi_path).filter(ee.Filter.notNull(["uid"]))

    def repair_and_measure(feature):
        geom = feature.geometry().buffer(0).simplify(10)
        return feature.setGeometry(geom).set("area_m2", geom.area(1))

    return fc.map(repair_and_measure).filter(ee.Filter.gt("area_m2", 0))


def _build_metric_images(fire_collection, n_years, projection):
    def mask_fire_pixels(image):
        image = ee.Image(image)
        return image.updateMask(image.gt(0))

    def fire_incident_pixel(image):
        image = ee.Image(image)
        return image.gt(0).unmask(0)

    fire_only = fire_collection.map(mask_fire_pixels)
    fire_incidents = fire_collection.map(fire_incident_pixel)

    def prepare(image, metric_name):
        return ee.Image(image).rename(metric_name).setDefaultProjection(projection)

    return {
        "fire_frp_sum_per_year": prepare(
            fire_only.sum().divide(n_years),
            "fire_frp_sum_per_year",
        ),
        "fire_frp_mean": prepare(
            fire_only.mean(),
            "fire_frp_mean",
        ),
        "fire_frp_max": prepare(
            fire_only.max(),
            "fire_frp_max",
        ),
        "fire_count_per_year": prepare(
            fire_incidents.sum().divide(n_years),
            "fire_count_per_year",
        ),
    }


def _add_fire_metrics(mws_fc, metric_images, projection):
    fc = _reduce_metric(
        mws_fc,
        metric_images["fire_frp_sum_per_year"],
        ee.Reducer.sum(),
        "fire_frp_sum_per_year",
        projection,
    )
    fc = _reduce_metric(
        fc,
        metric_images["fire_frp_mean"],
        ee.Reducer.mean(),
        "fire_frp_mean",
        projection,
    )
    fc = _reduce_metric(
        fc,
        metric_images["fire_frp_max"],
        ee.Reducer.mean(),
        "fire_frp_max",
        projection,
    )
    return _reduce_metric(
        fc,
        metric_images["fire_count_per_year"],
        ee.Reducer.sum(),
        "fire_count_per_year",
        projection,
    )


def _reduce_metric(fc, image, reducer, metric_name, projection):
    reduced = image.reduceRegions(
        collection=fc,
        reducer=reducer,
        scale=SCALE,
        crs=projection,
        tileScale=TILE_SCALE,
    )

    def fill_null(feature):
        value = feature.get(metric_name)
        value = ee.Algorithms.If(ee.Algorithms.IsEqual(value, None), 0, value)
        return feature.set(metric_name, ee.Number(value))

    return reduced.map(fill_null)


def _save_to_db_and_sync_to_geoserver(
        layer_name=None,
        asset_id=None,
        start_year=None,
        end_year=None,
        asset_suffix=None,
        state=None,
        district=None,
        block=None,
):
    print("Forest Fire updated: save_to_db_and_sync_to_geoserver")

    layer_id = None
    if state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Forest Fire",
            misc={
                "start_year": start_year,
                "end_year": end_year,
            },
        )

    make_asset_public(asset_id)

    fc = ee.FeatureCollection(asset_id)
    res = sync_fc_to_geoserver(fc, asset_suffix, layer_name, "forest_fire")
    print(res)

    layer_at_geoserver = False
    if res["status_code"] == 201 and layer_id:
        update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
        print("Forest Fire updated: sync to geoserver flag updated")
        layer_at_geoserver = True

    return layer_at_geoserver
