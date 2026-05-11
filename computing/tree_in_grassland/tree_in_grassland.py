"""
Tree in Grassland pipeline.

Generates a vector layer with tree-shrub context metrics per micro-watershed.
Analyses tree-shrub spatial context using multi-year LULC data to quantify
grassland area, tree loss, and land-use transition patterns.

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
from .tree_in_grassland_utils import (
    SCALE,
    MAXPIX,
    load_pan_india_lulc,
    temporal_context,
)


@app.task(bind=True)
def generate_tree_in_grassland_layer(
    self,
    state,
    district,
    block,
    start_year=2017,
    end_year=2019,
    gee_account_id=None,
    app_type="MWS",
):
    """
    Generate tree-in-grassland context metrics as a vector layer.

    Computes tree-shrub spatial context analysis using pan-India LULC v3 data
    across a multi-year window. For each micro-watershed, calculates:

      Metrics A – grassland area, tree-in-shrub area, isolated shrub area,
                  shrubland area, tree loss, tree-loss-to-grassland ratio,
                  tree-loss-to-tree-in-shrub ratio, tree-shrub-to-barren area.

      Metrics B – tree-shrub transitions to built-up, kharif water,
                  kharif-rabi water.

      Metrics C – tree-shrub transitions to kharif-rabi-zaid water, crops.

    The temporal analysis works with two overlapping 3-year windows:
        start period = [start_year, start_year+1, start_year+2]
        end   period = [end_year-1, end_year-2, end_year]

    Results are exported as a vector asset to GEE, synced to GeoServer,
    and metadata is saved to the database.

    Args:
        state:          str – state name.
        district:       str – district name.
        block:          str – block / tehsil name.
        start_year:     int – first year of the analysis window (default 2018).
        end_year:       int – last year of the analysis window  (default 2021).
        gee_account_id: int – GEE service-account ID for authentication.
        app_type:       str – application type key in GEE_PATHS (default "MWS").
    """

    # ------------------------------------------------------------------
    # STEP 1: Initialize GEE and set up paths
    # ------------------------------------------------------------------
    ee_initialize(gee_account_id)

    start_year = int(start_year)
    end_year = int(end_year)

    asset_suffix = (
        valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    )
    asset_folder_list = [state, district, block]

    description = f"tree_in_grassland_{asset_suffix}_{start_year}_{end_year}"
    layer_name = f"{asset_suffix}_tree_in_grassland"

    asset_id = (
        get_gee_dir_path(
            asset_folder_list,
            asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
        )
        + description
    )

    print(f"Tree in Grassland pipeline started: {asset_id=}")

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
    # STEP 3: Compute tree-in-grassland metrics
    # ------------------------------------------------------------------
    if not is_gee_asset_exists(asset_id):

        # Load pan-India LULC images for the required year range
        lulc_by_year = {
            year: load_pan_india_lulc(year)
            for year in range(start_year-1, end_year + 2)
        }

        pixel_area = ee.Image.pixelArea()

        # Temporal windows (overlapping 3-year periods)
        start_years = [start_year-1, start_year, start_year + 1]
        end_years = [end_year - 1, end_year, end_year+1] #need to change

        # ---- inner compute functions (closures over EE objects) ------

        def compute_A(f):
            """Grassland area, tree loss, and degradation metrics."""
            aoi = f.geometry()

            lulc_start = (
                ee.ImageCollection([lulc_by_year[y] for y in start_years])
                .reduce(ee.Reducer.mode())
                .clip(aoi)
            )
            lulc_end = (
                ee.ImageCollection([lulc_by_year[y] for y in end_years])
                .reduce(ee.Reducer.mode())
                .clip(aoi)
            )

            context_start, context_end = temporal_context(
                lulc_by_year, aoi, start_years, end_years
            )

            grassland_mask = (context_start.eq(1)).Or(context_start.eq(2))
            tree_loss = grassland_mask.And(context_end.eq(0))
            tree_to_barren = grassland_mask.And(lulc_end.eq(7))

            def area(mask):
                return (
                    pixel_area.updateMask(mask)
                    .reduceRegion(
                        ee.Reducer.sum(), aoi, SCALE, maxPixels=MAXPIX
                    )
                    .get("area")
                )

            grassland_area = area(grassland_mask)
            tree_in_shrub_area = area(context_start.eq(1))
            isolated_shrub_area = area(
                lulc_start.eq(12).And(context_start.eq(0))
            )
            shrubland_area = area(lulc_start.eq(12))
            tree_loss_area = area(tree_loss)
            barren_area = area(tree_to_barren)

            return f.set(
                {
                    "grassland_area_m2": grassland_area,
                    "tree_in_shrub_area_m2": tree_in_shrub_area,
                    "isolated_shrub_area_m2": isolated_shrub_area,
                    "shrubland_area_m2": shrubland_area,
                    "tree_loss_area_m2": tree_loss_area,
                    "tree_loss_to_grassland_ratio": ee.Number(
                        tree_loss_area
                    ).divide(grassland_area),
                    "tree_loss_to_tree_in_shrub_ratio": ee.Number(
                        tree_loss_area
                    ).divide(tree_in_shrub_area),
                    "tree_shrub_to_barren_area_m2": barren_area,
                }
            )

        def compute_B(f):
            """Tree-shrub to built-up and water transition metrics."""
            aoi = f.geometry()

            lulc_end = (
                ee.ImageCollection([lulc_by_year[y] for y in end_years])
                .reduce(ee.Reducer.mode())
                .clip(aoi)
            )

            context_start, _ = temporal_context(
                lulc_by_year, aoi, start_years, end_years
            )

            grassland_mask = (context_start.eq(1)).Or(context_start.eq(2))
            to_built = grassland_mask.And(lulc_end.eq(1))
            to_kharif = grassland_mask.And(lulc_end.eq(2))
            to_kharif_rabi = grassland_mask.And(lulc_end.eq(3))

            def area(mask):
                return (
                    pixel_area.updateMask(mask)
                    .reduceRegion(
                        ee.Reducer.sum(), aoi, SCALE, maxPixels=MAXPIX
                    )
                    .get("area")
                )

            return f.set(
                {
                    "tree_shrub_to_built_area_m2": area(to_built),
                    "tree_shrub_to_kharif_water_area_m2": area(to_kharif),
                    "tree_shrub_to_kharif_rabi_water_area_m2": area(
                        to_kharif_rabi
                    ),
                }
            )

        def compute_C(f):
            """Tree-shrub to crop and zaid-water transition metrics."""
            aoi = f.geometry()

            lulc_end = (
                ee.ImageCollection([lulc_by_year[y] for y in end_years])
                .reduce(ee.Reducer.mode())
                .clip(aoi)
            )

            context_start, _ = temporal_context(
                lulc_by_year, aoi, start_years, end_years
            )

            grassland_mask = (context_start.eq(1)).Or(context_start.eq(2))
            to_zaid = grassland_mask.And(lulc_end.eq(4))
            to_crops = grassland_mask.And(
                lulc_end.eq(5)
                .Or(lulc_end.eq(8))
                .Or(lulc_end.eq(9))
                .Or(lulc_end.eq(10))
                .Or(lulc_end.eq(11))
            )

            def area(mask):
                return (
                    pixel_area.updateMask(mask)
                    .reduceRegion(
                        ee.Reducer.sum(), aoi, SCALE, maxPixels=MAXPIX
                    )
                    .get("area")
                )

            return f.set(
                {
                    "tree_shrub_to_kharif_rabi_zaid_water_area_m2": area(
                        to_zaid
                    ),
                    "tree_shrub_to_crops_area_m2": area(to_crops),
                }
            )

        # ---- map each compute group over MWS features ----------------
        # results_A = mws_fc.map(compute_A)
        # results_B = mws_fc.map(compute_B)
        # results_C = mws_fc.map(compute_C)

        # # ---- merge A, B, C into a single FeatureCollection -----------
        # join = ee.Join.inner()
        # index_filter = ee.Filter.equals(
        #     leftField="system:index", rightField="system:index"
        # )

        # # Join A + B
        # ab_joined = join.apply(results_A, results_B, index_filter)

        # def _merge_ab(pair):
        #     feat_a = ee.Feature(pair.get("primary"))
        #     feat_b = ee.Feature(pair.get("secondary"))
        #     return feat_a.copyProperties(feat_b)

        # results_AB = ee.FeatureCollection(ab_joined.map(_merge_ab))

        # # Join AB + C
        # abc_joined = join.apply(results_AB, results_C, index_filter)

        # def _merge_abc(pair):
        #     feat_ab = ee.Feature(pair.get("primary"))
        #     feat_c = ee.Feature(pair.get("secondary"))
        #     return feat_ab.copyProperties(feat_c)

        # fc = ee.FeatureCollection(abc_joined.map(_merge_abc))
        def compute_all(f):
            f = compute_A(f)
            f = compute_B(f)
            f = compute_C(f)
            return f

        fc = mws_fc.map(compute_all)

        # --------------------------------------------------------------
        # STEP 4: Export to GEE
        # --------------------------------------------------------------
        task_id = export_vector_asset_to_gee(fc, description, asset_id)
        if task_id:
            check_task_status([task_id])
            print("Tree in Grassland layer exported to GEE.")

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
    print("Tree in Grassland: save_to_db_and_sync_to_geoserver")

    layer_id = None
    if state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Tree in Grassland",
            misc={
                "start_year": start_year,
                "end_year": end_year,
            },
        )

    make_asset_public(asset_id)

    fc = ee.FeatureCollection(asset_id)
    res = sync_fc_to_geoserver(
        fc, state, layer_name, "tree_in_grassland"
    )
    print(res)

    layer_at_geoserver = False
    if res["status_code"] == 201 and layer_id:
        update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
        print("Tree in Grassland: sync to geoserver flag updated")
        layer_at_geoserver = True

    return layer_at_geoserver
