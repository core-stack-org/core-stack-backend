"""
Forest Fringe pipeline.

Generates a vector layer with forest-fringe metrics per micro-watershed.
Analyses forest edge (fringe) zones to quantify deforestation and
degradation pressure on forest boundaries.

For each MWS the pipeline computes:
  - forest_fringe_area_m2       – area of the 50 m fringe ring
  - forest_fringe_ratio         – fringe area / MWS area
  - tree_degradation_mws_area_m2
  - tree_degradation_fringe_area_m2
  - tree_degradation_fringe_ratio
  - tree_deforestation_mws_area_m2
  - tree_deforestation_fringe_area_m2
  - tree_deforestation_fringe_ratio
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
from .forest_fringe_utils import (
    SCALE,
    MAXPIX,
    OUTER_BUFFER,
    FRINGE_WIDTH,
    load_tree_mode,
    load_ltp_change,
    load_overall_change,
    make_fringe,
)


@app.task(bind=True)
def generate_forest_fringe_layer(
    self,
    state,
    district,
    block,
    gee_account_id=None,
    app_type="MWS",
):
    """
    Generate forest-fringe metrics as a vector layer.

    For each micro-watershed the task:
      1. Expands the MWS boundary by OUTER_BUFFER metres.
      2. Identifies forest patches (tree-mode ≥ 3 years) in the expanded area.
      3. Filters to large tree patches (LTPs ≥ 1 ha).
      4. Builds a 50 m inward fringe ring around each LTP and clips it
         back to the original MWS.
      5. Computes deforestation & degradation areas inside the MWS and
         inside the fringe, plus their ratios.

    Results are exported as a vector asset to GEE, synced to GeoServer,
    and metadata is saved to the database.

    Args:
        state:          str – state name.
        district:       str – district name.
        block:          str – block / tehsil name.
        gee_account_id: int – GEE service-account ID for authentication.
        app_type:       str – application type key in GEE_PATHS (default "MWS").
    """

    # ------------------------------------------------------------------
    # STEP 1: Initialize GEE and set up paths
    # ------------------------------------------------------------------
    ee_initialize(gee_account_id)

    asset_suffix = (
        valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    )
    asset_folder_list = [state, district, block]

    description = f"forest_fringe_{asset_suffix}"
    layer_name = f"{asset_suffix}_forest_fringe"

    asset_id = (
        get_gee_dir_path(
            asset_folder_list,
            asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
        )
        + description
    )

    print(f"Forest Fringe pipeline started: {asset_id=}")

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

    # ------------------------------------------------------------------
    # STEP 3: Compute forest-fringe metrics
    # ------------------------------------------------------------------
    if not is_gee_asset_exists(asset_id):

        # Prepare global images
        tree_mode = load_tree_mode()
        ltp_change = load_ltp_change()
        overall_change = load_overall_change()
        pixel_area = ee.Image.pixelArea()

        # ---- per-MWS compute function (closure over EE objects) ------

        def compute_metrics_per_mws(f):
            """Compute all forest-fringe metrics for a single MWS."""
            mws_geom = f.geometry()
            mws_area = mws_geom.area(1)

            # 1) Expand MWS by OUTER_BUFFER
            expanded_mws = mws_geom.buffer(OUTER_BUFFER, 1)

            # 2) Forest inside expanded MWS
            forest = tree_mode.clip(expanded_mws)

            # 3) Vectorize forest patches
            forest_patches = forest.reduceToVectors(
                geometry=expanded_mws,
                scale=SCALE,
                geometryType="polygon",
                eightConnected=True,
                maxPixels=MAXPIX,
            )

            # 4) Keep only LTPs (>= 1 ha)
            ltps = forest_patches.filter(ee.Filter.area(10000, 1e13))

            # 5) Build fringe rings and clip to original MWS
            fringes = ltps.map(make_fringe)
            fringes_clipped = fringes.map(
                lambda fr: ee.Feature(
                    fr.geometry().intersection(mws_geom, 1)
                )
            )

            fringe_img = (
                ee.Image.constant(1)
                .paint(fringes_clipped, 1)
                .selfMask()
                .clip(mws_geom)
            )

            # ---- Fringe area (geometry-based) ----
            fringe_geom = fringes_clipped.geometry()

            fringe_area = ee.Number(
                ee.Algorithms.If(
                    fringe_geom.area(1).eq(0),
                    0,
                    fringe_geom.area(1),
                )
            )

            fringe_is_empty = fringe_area.eq(0)

            # ---- Deforestation & degradation masks ----
            deforestation = (
                ltp_change.eq(6)
                .Or(ltp_change.eq(7))
                .updateMask(tree_mode)
                .clip(mws_geom)
            )

            degradation = (
                overall_change.eq(-1)
                .updateMask(tree_mode)
                .clip(mws_geom)
            )

            # ---- Areas in MWS ----
            defo_mws_area = (
                pixel_area.updateMask(deforestation)
                .reduceRegion(
                    ee.Reducer.sum(), mws_geom, SCALE, maxPixels=MAXPIX
                )
                .get("area")
            )

            degr_mws_area = (
                pixel_area.updateMask(degradation)
                .reduceRegion(
                    ee.Reducer.sum(), mws_geom, SCALE, maxPixels=MAXPIX
                )
                .get("area")
            )

            # ---- Areas in forest fringe (guard against empty fringe) ----
            deforestation_fringe = ee.Image(
                ee.Algorithms.If(
                    fringe_is_empty,
                    ee.Image(0).updateMask(ee.Image(0)),
                    deforestation.clip(fringe_geom),
                )
            )

            degradation_fringe = ee.Image(
                ee.Algorithms.If(
                    fringe_is_empty,
                    ee.Image(0).updateMask(ee.Image(0)),
                    degradation.clip(fringe_geom),
                )
            )

            defo_fringe_area = (
                pixel_area.updateMask(deforestation_fringe)
                .reduceRegion(
                    ee.Reducer.sum(), fringe_geom, SCALE, maxPixels=MAXPIX
                )
                .get("area")
            )

            degr_fringe_area = (
                pixel_area.updateMask(degradation_fringe)
                .reduceRegion(
                    ee.Reducer.sum(), fringe_geom, SCALE, maxPixels=MAXPIX
                )
                .get("area")
            )

            # ---- Ratios ----
            fringe_to_mws_ratio = ee.Number(fringe_area).divide(mws_area)
            degr_fringe_ratio = ee.Number(degr_fringe_area).divide(
                fringe_area
            )
            defo_fringe_ratio = ee.Number(defo_fringe_area).divide(
                fringe_area
            )

            return ee.Feature(f.geometry()).set({
                "uid": f.get("uid"),
                "mws_area_m2": mws_area,
                "forest_fringe_area_m2": fringe_area,
                "forest_fringe_ratio": fringe_to_mws_ratio,
                "tree_degradation_mws_area_m2": degr_mws_area,
                "tree_degradation_fringe_area_m2": degr_fringe_area,
                "tree_degradation_fringe_ratio": degr_fringe_ratio,
                "tree_deforestation_mws_area_m2": defo_mws_area,
                "tree_deforestation_fringe_area_m2": defo_fringe_area,
                "tree_deforestation_fringe_ratio": defo_fringe_ratio,
            })

        # ---- map compute over all MWS features ----
        results_fc = mws_fc.map(compute_metrics_per_mws)

        fc = results_fc.select([
            "uid",
            "mws_area_m2",
            "forest_fringe_area_m2",
            "forest_fringe_ratio",
            "tree_degradation_mws_area_m2",
            "tree_degradation_fringe_area_m2",
            "tree_degradation_fringe_ratio",
            "tree_deforestation_mws_area_m2",
            "tree_deforestation_fringe_area_m2",
            "tree_deforestation_fringe_ratio",
        ])

        # --------------------------------------------------------------
        # STEP 4: Export to GEE
        # --------------------------------------------------------------
        task_id = export_vector_asset_to_gee(fc, description, asset_id)
        
        if task_id:
            check_task_status([task_id])
            print("Forest Fringe layer exported to GEE.")

    # ------------------------------------------------------------------
    # STEP 5: Publish to GeoServer and save metadata to DB
    # ------------------------------------------------------------------
    layer_at_geoserver = _save_to_db_and_sync_to_geoserver(
        layer_name=layer_name,
        asset_id=asset_id,
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
    asset_suffix=None,
    state=None,
    district=None,
    block=None,
):
    """Publish asset to GeoServer and persist metadata to the database."""
    print("Forest Fringe: save_to_db_and_sync_to_geoserver")

    layer_id = None
    if state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Forest Fringe",
        )

    make_asset_public(asset_id)

    fc = ee.FeatureCollection(asset_id)
    res = sync_fc_to_geoserver(
        fc, asset_suffix, layer_name, "forest_fringe"
    )
    print(res)

    layer_at_geoserver = False
    if res["status_code"] == 201 and layer_id:
        update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
        print("Forest Fringe: sync to geoserver flag updated")
        layer_at_geoserver = True

    return layer_at_geoserver
