import ee
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.constants import (
    GEE_PATHS,
    RIVER_PAN_INDIA_ASSET,
)
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    check_task_status,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    make_asset_public,
    get_gee_dir_path,
)
from nrm_app.celery import app


@app.task(bind=True)
def river_vector(
    self,
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
):
    ee_initialize(gee_account_id)
    print(f"Inside process river_vector for {state} - {district} - {block}")

    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

        roi_asset_id = (
            get_gee_dir_path(
                asset_folder_list,
                asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
            )
            + f"filtered_mws_{asset_suffix}_uid"
        )
        roi = ee.FeatureCollection(roi_asset_id)

    else:
        roi = ee.FeatureCollection(roi)

    # ------------------------------------------------------------------
    # Asset details
    # ------------------------------------------------------------------
    description = f"{asset_suffix}_river_vector"

    asset_id = (
        get_gee_dir_path(
            asset_folder_list,
            asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
        )
        + description
    )

    print(f"Asset ID: {asset_id}")
    if not is_gee_asset_exists(asset_id):
        pan_india_data = ee.FeatureCollection(RIVER_PAN_INDIA_ASSET)
        outer_boundary = roi.geometry().dissolve(maxError=1)
        rivers_in_roi = pan_india_data.filterBounds(outer_boundary)

        # ── Step 2: For each river, collect every watershed it touches ──
        # river = left, ROI polygon = right
        # A river crossing N watersheds → N entries in roi_matches
        spatial_filter = ee.Filter.intersects(
            leftField=".geo",
            rightField=".geo",
            maxError=1,
        )
        join = ee.Join.saveAll(matchesKey="roi_matches")
        joined_data = join.apply(rivers_in_roi, roi, spatial_filter)

        # rivers that DID match at least one watershed
        matched_rivers = joined_data.filter(
            ee.Filter.listContains("roi_matches", None).Not()
        )

        # rivers inside outer boundary but with ZERO watershed matches
        # → these are the "gap" rivers visible in the red area
        matched_river_ids = matched_rivers.aggregate_array("objectid")
        gap_rivers = rivers_in_roi.filter(
            ee.Filter.inList("objectid", matched_river_ids).Not()
        )

        # ── Step 3: Expand matched rivers → one feature per watershed ───
        def expand_river(river_feat, acc):
            river_feat = ee.Feature(river_feat)
            roi_matches = ee.List(river_feat.get("roi_matches"))

            def clip_to_watershed(roi_feat):
                roi_feat = ee.Feature(roi_feat)
                clipped_geom = river_feat.geometry().intersection(
                    roi_feat.geometry(),
                    ee.ErrorMargin(1),
                )
                return (
                    ee.Feature(clipped_geom)
                    .copyProperties(river_feat)
                    .set("uid", roi_feat.get("uid"))
                    .set("area_in_ha", roi_feat.get("area_in_ha"))
                    .set("roi_matches", None)
                )

            return ee.List(acc).cat(roi_matches.map(clip_to_watershed))

        matched_list = ee.List(matched_rivers.iterate(expand_river, ee.List([])))
        matched_fc = ee.FeatureCollection(matched_list)

        # ── Step 4: Handle gap rivers ────────────────────────────────────
        # Clip to the outer dissolved boundary so geometry stays inside
        # the study area, but uid/area_in_ha are left blank
        def make_gap_feature(river_feat):
            river_feat = ee.Feature(river_feat)
            clipped_geom = river_feat.geometry().intersection(
                outer_boundary,
                ee.ErrorMargin(1),
            )
            return (
                ee.Feature(clipped_geom)
                .copyProperties(river_feat)
                .set("uid", ee.String(""))
                .set("area_in_ha", ee.String(""))
                .set("roi_matches", None)
            )

        gap_fc = gap_rivers.map(make_gap_feature)

        # ── Step 5: Merge matched + gap features ─────────────────────────
        result_fc = matched_fc.merge(gap_fc)
        task = export_vector_asset_to_gee(result_fc, description, asset_id)
        check_task_status([task])

    # ------------------------------------------------------------------
    # Publish & Sync
    # ------------------------------------------------------------------
    layer_at_geoserver = False

    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            description,
            asset_id,
            "River Vector",
        )

        fc = ee.FeatureCollection(asset_id)

        print("Syncing to GeoServer...")
        sync_res = sync_fc_to_geoserver(fc, state, description, "river")

        if sync_res and sync_res.get("status_code") == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated")
            layer_at_geoserver = True

    return layer_at_geoserver
