import ee

# import geemap

from .forest_fire_utils import (
    SCALE,
    MAXPIX,
    load_fire_collections,
    prepare_frp_images,
)
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    export_vector_asset_to_gee,
    get_gee_dir_path,
    check_task_status,
    valid_gee_text,
    is_gee_asset_exists,
    make_asset_public,
)
from computing.utils import (
    sync_fc_to_geoserver,
    update_layer_sync_status,
    save_layer_info_to_db,
)
from nrm_app.celery import app


@app.task(bind=True)
def generate_forest_fire_layer(
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

    # ------------------------------------------------------------------
    # STEP 2: Set up ROI (MWS boundaries from GEE)
    # ------------------------------------------------------------------
    roi_path = (
        get_gee_dir_path(
            asset_folder_list,
            asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
        )
        + f"filtered_mws_{valid_gee_text(district.lower())}"
        + f"_{valid_gee_text(block.lower())}_uid"
    )
    mws_fc = ee.FeatureCollection(roi_path)

    print("Total features:", mws_fc.size().getInfo())

    def flag_bad_geom(f):
        area = f.geometry().area(1)
        return f.set("area_m2", area)

    debug_fc = mws_fc.map(flag_bad_geom)
    bad = debug_fc.filter(ee.Filter.eq("area_m2", 0))

    print("Bad geometry count:", bad.size().getInfo())
    print("Bad UIDs:", bad.aggregate_array("uid").getInfo())

    # ------------------------------------------------------------------
    # STEP 3: Compute fire metrics
    # ------------------------------------------------------------------
    if not is_gee_asset_exists(asset_id):
        frp_collection = load_fire_collections(start_year, end_year)
        fire_images = prepare_frp_images(frp_collection, n_years)

        frp_sum_img = fire_images["sum"]
        frp_mean_img = fire_images["mean"]
        frp_max_img = fire_images["max"]
        fire_count_img = fire_images["count"]

        def compute_fire_metrics(f):
            geom = f.geometry()

            def reduce(img, reducer, band):
                val = img.reduceRegion(
                    reducer=reducer,
                    geometry=geom,
                    scale=SCALE,
                    maxPixels=MAXPIX,
                    bestEffort=True,
                ).get(band)

                return ee.Number(
                    ee.Algorithms.If(
                        ee.Algorithms.IsEqual(val, None),
                        0,
                        val,
                    )
                )

            fire_frp_sum_per_year = reduce(
                frp_sum_img,
                ee.Reducer.sum(),
                "MaxFRP",
            )
            fire_frp_mean = reduce(
                frp_mean_img,
                ee.Reducer.mean(),
                "MaxFRP",
            )
            fire_frp_max = reduce(
                frp_max_img,
                ee.Reducer.mean(),
                "MaxFRP",
            )
            fire_count_per_year = reduce(
                fire_count_img,
                ee.Reducer.sum(),
                "fire",
            )

            return (
                f.set("fire_frp_sum_per_year", fire_frp_sum_per_year)
                .set("fire_frp_mean", fire_frp_mean)
                .set("fire_frp_max", fire_frp_max)
                .set("fire_count_per_year", fire_count_per_year)
            )

        def repair_geometry(f):
            return f.setGeometry(f.geometry().buffer(0).simplify(10))

        def validate_feature(f):
            geom = f.geometry()

            return f.set(
                {
                    "geom_type": geom.type(),
                    "area_m2": geom.area(1),
                }
            )

        validated = mws_fc.map(validate_feature)

        mws_fc = validated.filter(ee.Filter.gt("area_m2", 0))

        mws_fc = mws_fc.map(repair_geometry)

        fc = mws_fc.map(compute_fire_metrics)

        fc = fc.select(
            [
                "uid",
                "fire_frp_sum_per_year",
                "fire_frp_mean",
                "fire_frp_max",
                "fire_count_per_year",
            ]
        )

        # ------------------------------------------------------------------
        # STEP 4
        # ------------------------------------------------------------------

        # print("Exporting locally...")
        #
        # geemap.ee_export_vector(
        #     fc,
        #     filename=f"{description}.geojson",
        # )

        task_id = export_vector_asset_to_gee(fc, description, asset_id)
        if task_id:
            check_task_status([task_id])
            print("Forest Fire layer exported to GEE.")

    # ------------------------------------------------------------------
    # STEP 5: Publish to GeoServer and save metadata to DB
    # ------------------------------------------------------------------
    layer_at_geoserver = _save_to_db_and_sync_to_geoserver(
        layer_name=layer_name,
        asset_id=asset_id,
        start_year=start_year,
        end_year=end_year,
        asset_suffix=asset_suffix,
        state=state,
        district=district,
        block=block,
    )
    return layer_at_geoserver


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
    """Publish asset to GeoServer and persist metadata to the database."""
    print("Forest Fire: save_to_db_and_sync_to_geoserver")

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
        print("Forest Fire: sync to geoserver flag updated")
        layer_at_geoserver = True

    return layer_at_geoserver
