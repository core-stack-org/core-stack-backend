"""
Forest Fire pipeline.

"""

import ee
import geemap

from computing.utils import (
    save_layer_info_to_db,
    sync_fc_to_geoserver,
    update_layer_sync_status,
)
from .forest_fire_utils import (
    SCALE,
    MAXPIX,
    load_fire_image,
    prepare_frp_images,
)
from gee_computing.models import GEEAccount
from utilities.constants import GEE_PATHS, AEZ, MWS_DATASET
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_dir_path,
    export_vector_asset_to_gee,
    check_task_status,
    make_asset_public,
)
from nrm_app.celery import app


def forest_fringes_on_AEZ(aez_no, gee_account_id=7):
    ee_initialize(gee_account_id)
    aez = ee.FeatureCollection(AEZ)
    mwses = ee.FeatureCollection(MWS_DATASET)

    filter_aez = aez.filter(ee.Filter.eq("ae_regcode", aez_no)).geometry()

    roi = mwses.filterBounds(filter_aez)

    asset_suffix = f"AEZ_{aez_no}"
    asset_folder_list = ["forest_fire"]
    generate_forest_fire_layer(
        roi=roi,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        gee_account_id=gee_account_id,
        app_type="forest_fire",
        sync_to_db=False,
        sync_to_geoserver=False,
    )


@app.task(bind=True)
def generate_forest_fire_layer(
        self,
        state=None,
        district=None,
        block=None,
        roi=None,
        asset_suffix=None,
        asset_folder_list=None,
        start_year=2004,
        end_year=2022,
        gee_account_id=None,
        app_type="MWS",
        sync_to_db=True,
        sync_to_geoserver=True,
):
    """
    Generate MODIS fire metrics for a FeatureCollection.
    """
    ee_initialize(gee_account_id)

    start_year = int(start_year)
    end_year = int(end_year)
    n_years = end_year - start_year + 1

    print("Forest Fire pipeline started")

    if state and district and block:
        asset_suffix = (
                valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

        roi = ee.FeatureCollection(
            get_gee_dir_path(
                asset_folder_list,
                asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
            )
            + f"filtered_mws_{valid_gee_text(district.lower())}"
            + f"_{valid_gee_text(block.lower())}_uid"
        )

    description = f"forest_fire_{asset_suffix}"
    if app_type in GEE_PATHS:
        asset_path = GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    else:
        gee_obj = GEEAccount.objects.get(pk=gee_account_id)
        asset_path = f"projects/{gee_obj.name}/assets/"

    asset_id = get_gee_dir_path(asset_folder_list, asset_path=asset_path) + description

    print("Total features:", roi.size().getInfo())

    def flag_bad_geom(f):
        area = f.geometry().area(1)
        return f.set("area_m2", area)

    debug_fc = roi.map(flag_bad_geom)
    bad = debug_fc.filter(ee.Filter.eq("area_m2", 0))

    print("Bad geometry count:", bad.size().getInfo())
    print("Bad UIDs:", bad.aggregate_array("uid").getInfo())

    fire_image = load_fire_image()

    print(fire_image.bandNames().getInfo())

    fire_images = prepare_frp_images(
        fire_image,
        start_year,
        end_year,
    )

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

        return ee.Feature(f.geometry()).set(
            {
                "uid": f.get("uid"),
                "fire_frp_sum_per_year": reduce(
                    frp_sum_img,
                    ee.Reducer.sum(),
                    "sum",
                ),
                "fire_frp_mean": reduce(
                    frp_mean_img,
                    ee.Reducer.mean(),
                    "mean",
                ),
                "fire_frp_max": reduce(
                    frp_max_img,
                    ee.Reducer.mean(),
                    "max",
                ),
                "fire_count_per_year": reduce(
                    fire_count_img,
                    ee.Reducer.sum(),
                    "sum",
                ),
            }
        )

    # roi = roi.filter(ee.Filter.notNull(["uid"]))

    # def repair_geometry(f):
    #     return f.setGeometry(f.geometry().buffer(0).simplify(10))
    #
    # def validate_feature(f):
    #     geom = f.geometry()
    #
    #     return f.set(
    #         {
    #             "geom_type": geom.type(),
    #             "area_m2": geom.area(1),
    #         }
    #     )
    #
    # validated = roi.map(validate_feature)
    #
    # roi = validated.filter(ee.Filter.gt("area_m2", 0))
    #
    # roi = roi.map(repair_geometry)

    fc = roi.map(compute_fire_metrics)

    fc = fc.select(
        [
            "uid",
            "fire_frp_sum_per_year",
            "fire_frp_mean",
            "fire_frp_max",
            "fire_count_per_year",
        ]
    )

    # print("Exporting locally...")
    #
    # geemap.ee_export_vector(
    #     fc,
    #     filename="forest_fire.geojson",
    # )
    #
    # print("Done.")
    #
    # return fc

    # --------------------------------------------------------------
    # Export to GEE
    # --------------------------------------------------------------
    task_id = export_vector_asset_to_gee(fc, description, asset_id)

    if task_id:
        check_task_status([task_id])
        print("Forest Fire layer exported to GEE.")

    # ------------------------------------------------------------------
    # Publish to GeoServer and save metadata to DB
    # ------------------------------------------------------------------
    layer_at_geoserver = _save_to_db_and_sync_to_geoserver(
        layer_name=description,
        asset_id=asset_id,
        asset_suffix=asset_suffix,
        state=state,
        district=district,
        block=block,
        sync_to_db=sync_to_db,
        sync_to_geoserver=sync_to_geoserver,
    )
    return layer_at_geoserver


def _save_to_db_and_sync_to_geoserver(
        layer_name=None,
        asset_id=None,
        asset_suffix=None,
        state=None,
        district=None,
        block=None,
        sync_to_db=True,
        sync_to_geoserver=True,
):
    """Publish asset to GeoServer and persist metadata to the database."""
    print("Forest Fire: save_to_db_and_sync_to_geoserver")

    layer_id = None
    if sync_to_db and state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Forest Fire",
        )

    make_asset_public(asset_id)
    if sync_to_geoserver:
        fc = ee.FeatureCollection(asset_id)
        res = sync_fc_to_geoserver(fc, asset_suffix, layer_name, "forest_fire")
        print(res)

        layer_at_geoserver = False
        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Forest Fire: sync to geoserver flag updated")
            layer_at_geoserver = True

        return layer_at_geoserver
    return False
