"""
Forest Fire pipeline.

Generates a vector layer with MODIS-based fire metrics per micro-watershed.
Uses MODIS Terra (MOD14A1) and Aqua (MYD14A1) active fire products to
quantify fire radiative power and fire frequency across a user-defined
time window.

For each MWS the pipeline computes four metrics:
  - fire_frp_sum_per_year   – yearly-normalised total Fire Radiative Power
  - fire_frp_mean           – temporal mean FRP
  - fire_frp_max            – peak FRP observed
  - fire_count_per_year     – yearly-normalised fire pixel count
"""

import ee
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_dir_path,
    is_gee_asset_exists,
    make_asset_public,
    export_vector_asset_to_gee,
)
from nrm_app.celery import app
from .forest_fire_utils import (
    SCALE,
    MAXPIX,
    load_fire_collections,
    prepare_frp_images,
)


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
    Generate MODIS fire-risk metrics as a vector layer.

    For each micro-watershed the task computes four fire metrics from
    merged MODIS Terra + Aqua active fire products, exports the result
    as a vector asset to GEE, syncs to GeoServer, and saves metadata.

    Args:
        state:          str – state name.
        district:       str – district name.
        block:          str – block / tehsil name.
        start_year:     int – first year of the analysis window (default 2001).
        end_year:       int – last year of the analysis window  (default 2022).
        gee_account_id: int – GEE service-account ID for authentication.
        app_type:       str – application type key in GEE_PATHS (default "MWS").
    """

    # ------------------------------------------------------------------
    # STEP 1: Initialize GEE and set up paths
    # ------------------------------------------------------------------
    ee_initialize(gee_account_id)

    start_year = int(start_year)
    end_year = int(end_year)
    n_years = end_year - start_year + 1

    asset_suffix = (
        valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    )
    asset_folder_list = [state, district, block]

    description = f"forest_fire_{asset_suffix}_{start_year}_{end_year}"
    layer_name = f"{asset_suffix}_forest_fire"

    asset_id = (
        get_gee_dir_path(
            asset_folder_list,
            asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
        )
        + description
    )

    print(f"Forest Fire pipeline started: {asset_id=}")

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

    # Add this debug block before the map to identify bad features
    print("Total features:", mws_fc.size().getInfo())

    # Check for features whose geometry area is 0
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

        # Prepare temporally-aggregated fire images
        frp_collection = load_fire_collections(start_year, end_year)
        fire_images = prepare_frp_images(frp_collection, n_years)

        frp_sum_img = fire_images["sum"]
        frp_mean_img = fire_images["mean"]
        frp_max_img = fire_images["max"]
        fire_count_img = fire_images["count"]

        # ---- per-MWS compute function (closure over EE objects) ------


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
                return ee.Number(ee.Algorithms.If(
                    ee.Algorithms.IsEqual(val, None), 0, val
                ))

            return ee.Feature(f.geometry()).set({
                "uid":                   f.get("uid"),
                "fire_frp_sum_per_year": reduce(frp_sum_img,    ee.Reducer.sum(),  "MaxFRP"),
                "fire_frp_mean":         reduce(frp_mean_img,   ee.Reducer.mean(), "MaxFRP"),
                "fire_frp_max":          reduce(frp_max_img,    ee.Reducer.mean(), "MaxFRP"),  # ← mean not max
                "fire_count_per_year":   reduce(fire_count_img, ee.Reducer.sum(),  "fire"),
            })

        # ---- map compute over all MWS features ----
        mws_fc = mws_fc.filter(ee.Filter.notNull(['uid']))

        # After your area filter, add geometry repair
        def repair_geometry(f):
            return f.setGeometry(f.geometry().buffer(0).simplify(10))

        def validate_feature(f):
            geom = f.geometry()

            return f.set({
                "geom_type": geom.type(),
                "area_m2": geom.area(1)
            })

        validated = mws_fc.map(validate_feature)

        mws_fc = validated.filter(ee.Filter.gt("area_m2", 0))
        mws_fc = mws_fc.map(repair_geometry)           

        fc = mws_fc.map(compute_fire_metrics)

        fc = fc.select([
            "uid",
            "fire_frp_sum_per_year",
            "fire_frp_mean",
            "fire_frp_max",
            "fire_count_per_year",
        ])

        # --------------------------------------------------------------
        # STEP 4: Export to GEE
        # --------------------------------------------------------------
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


# ------------------------------------------------------------------
# Private helpers (publish / persist)
# ------------------------------------------------------------------


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
    res = sync_fc_to_geoserver(
        fc, asset_suffix, layer_name, "forest_fire"
    )
    print(res)

    layer_at_geoserver = False
    if res["status_code"] == 201 and layer_id:
        update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
        print("Forest Fire: sync to geoserver flag updated")
        layer_at_geoserver = True

    return layer_at_geoserver
