import ee
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.constants import GEE_PATHS, CANAL_PAN_INDIA_ASSET
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
def canal_vector(
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
    """
    Generate canal vector layer — mirrors local_compute canal logic.

    Final output per feature:
    - Geometry   : Canal boundary line clipped to watershed (or outer ROI
                   boundary if no individual watershed matched)
    - uid        : Watershed UID  ("" if gap canal)
    - area_in_ha : Watershed area ("" if gap canal)
    - All original canal attributes (canname, cancode, prjname, etc.)

    Cases handled:
    1. Canal intersects 1 watershed  → 1 clipped feature, uid filled
    2. Canal intersects N watersheds → N clipped features, uid filled
    3. Canal inside outer ROI but no → 1 feature clipped to outer ROI,
       individual watershed match      uid = "", area_in_ha = ""
    4. Canal outside ROI entirely    → dropped
    """

    # ------------------------------------------------------------------
    # Initialize Earth Engine
    # ------------------------------------------------------------------
    ee_initialize(gee_account_id)

    print(f"Inside process canal_vector for {state} - {district} - {block}")

    # ------------------------------------------------------------------
    # Prepare ROI
    # ------------------------------------------------------------------
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
            + f"filtered_mws_{asset_suffix}_uid"
        )
    else:
        roi = ee.FeatureCollection(roi)

    # ------------------------------------------------------------------
    # Asset details
    # ------------------------------------------------------------------
    description = f"{asset_suffix}_canal_vector"

    asset_id = (
        get_gee_dir_path(
            asset_folder_list,
            asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
        )
        + description
    )

    print(f"Asset ID: {asset_id}")

    # ------------------------------------------------------------------
    # Create asset
    # ------------------------------------------------------------------
    if not is_gee_asset_exists(asset_id):

        print("Loading canal dataset...")
        pan_india_data = ee.FeatureCollection(CANAL_PAN_INDIA_ASSET)

        # ── Outer dissolved boundary of entire ROI ─────────────────────
        # Used to:
        #   (a) coarse-filter canals to study area
        #   (b) clip gap canals that fall between watershed polygons
        outer_boundary = roi.geometry().dissolve(maxError=1)

        # ── Step 1: Coarse filter to study area ────────────────────────
        canals_in_roi = pan_india_data.filterBounds(outer_boundary)

        print("Checking canal geometry types and converting to lines...")

        # ── Step 2: Convert canal polygons → boundary lines ────────────
        # Canal pan-India dataset may store canals as Polygon/MultiPolygon
        # (buffered representations). Extract boundary so intersection with
        # watershed polygons produces lines, not polygons — mirrors the
        # local compute .boundary conversion.
        def to_boundary_line(feature):
            feature = ee.Feature(feature)
            geom = feature.geometry()
            geom_type = geom.type()

            # If polygon type → convert to boundary line
            line_geom = ee.Algorithms.If(
                ee.List(["Polygon", "MultiPolygon"]).contains(geom_type),
                geom.boundary(),  # LinearRing / MultiLineString
                geom,  # already a line, keep as-is
            )
            return feature.setGeometry(line_geom)

        canals_as_lines = canals_in_roi.map(to_boundary_line)

        # ── Step 3: For each canal, find every watershed it intersects ──
        # Canal = left, ROI watershed = right
        # A canal crossing N watersheds → N entries in roi_matches
        spatial_filter = ee.Filter.intersects(
            leftField=".geo",
            rightField=".geo",
            maxError=1,
        )
        join = ee.Join.saveAll(matchesKey="roi_matches")
        joined_data = join.apply(canals_as_lines, roi, spatial_filter)

        # Canals with at least one watershed match
        matched_canals = joined_data.filter(
            ee.Filter.listContains("roi_matches", None).Not()
        )

        # Gap canals — inside outer boundary but no watershed match
        matched_ids = matched_canals.aggregate_array("objectid")
        gap_canals = canals_as_lines.filter(
            ee.Filter.inList("objectid", matched_ids).Not()
        )

        print("Creating clipped canal segments per watershed...")

        # ── Step 4: Expand matched canals → one feature per watershed ───
        # iterate() required because one canal → multiple output features
        def expand_canal(canal_feat, acc):
            canal_feat = ee.Feature(canal_feat)
            roi_matches = ee.List(canal_feat.get("roi_matches"))

            def clip_to_watershed(roi_feat):
                roi_feat = ee.Feature(roi_feat)

                # Clip canal line to this watershed's boundary
                clipped_geom = canal_feat.geometry().intersection(
                    roi_feat.geometry(),
                    ee.ErrorMargin(1),
                )

                return (
                    ee.Feature(clipped_geom)
                    .copyProperties(canal_feat)
                    .set("uid", roi_feat.get("uid"))
                    .set("area_in_ha", roi_feat.get("area_in_ha"))
                    .set("roi_matches", None)
                )

            return ee.List(acc).cat(roi_matches.map(clip_to_watershed))

        matched_list = ee.List(matched_canals.iterate(expand_canal, ee.List([])))
        matched_fc = ee.FeatureCollection(matched_list)

        # ── Step 5: Handle gap canals ──────────────────────────────────
        # Clip to outer dissolved boundary, uid and area_in_ha left blank
        def make_gap_feature(canal_feat):
            canal_feat = ee.Feature(canal_feat)
            clipped_geom = canal_feat.geometry().intersection(
                outer_boundary,
                ee.ErrorMargin(1),
            )
            return (
                ee.Feature(clipped_geom)
                .copyProperties(canal_feat)
                .set("uid", ee.String(""))
                .set("area_in_ha", ee.String(""))
                .set("roi_matches", None)
            )

        gap_fc = gap_canals.map(make_gap_feature)

        # ── Step 6: Merge matched + gap feature collections ────────────
        result_fc = matched_fc.merge(gap_fc)

        print(f"Matched canal segments : {matched_fc.size().getInfo()}")
        print(f"Gap canal segments     : {gap_fc.size().getInfo()}")
        print(f"Total canal segments   : {result_fc.size().getInfo()}")

        # ── Step 7: Export ─────────────────────────────────────────────
        print("Exporting canal vector asset...")
        task = export_vector_asset_to_gee(result_fc, description, asset_id)
        check_task_status([task])

    # ------------------------------------------------------------------
    # Publish & Sync
    # ------------------------------------------------------------------
    layer_at_geoserver = False

    if is_gee_asset_exists(asset_id):

        print("Asset exists. Publishing...")
        make_asset_public(asset_id)

        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            description,
            asset_id,
            "Canal Vector",
        )

        fc = ee.FeatureCollection(asset_id)

        print("Syncing to GeoServer...")
        sync_res = sync_fc_to_geoserver(fc, state, description, "canal")

        if sync_res and sync_res.get("status_code") == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated")
            layer_at_geoserver = True

    return layer_at_geoserver
