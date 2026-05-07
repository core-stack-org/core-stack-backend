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


def build_canal_json(f):
    """
    Build a single canal feature as a JSON object string: {...}
    Runs entirely server-side as a GEE expression — safe inside .map()
    """

    def kv(key, val):
        """String property → "key":"value" or "key":"" if null"""
        v = ee.Algorithms.If(
            val,
            ee.String('"').cat(ee.String(val)).cat('"'),
            ee.String('""'),
        )
        return ee.String('"').cat(key).cat('":').cat(ee.String(v))

    def kv_num(key, val):
        """Numeric property → "key":123 or "key":null if null"""
        v = ee.Algorithms.If(
            val,
            ee.String(val),
            ee.String("null"),
        )
        return ee.String('"').cat(key).cat('":').cat(ee.String(v))

    pairs = ee.List(
        [
            kv("canname", f.get("canname")),
            kv("cancode", f.get("cancode")),
            kv("prjname", f.get("prjname")),
            kv("prjcode", f.get("prjcode")),
            kv("state", f.get("state")),
            kv("cn_purp", f.get("cn_purp")),
            kv("cn_ss", f.get("cn_ss")),
            kv("cn_st", f.get("cn_st")),
            kv("cn_type", f.get("cn_type")),
            kv("status_yr", f.get("status_yr")),
            kv_num("can_type", f.get("can_type")),
            kv_num("objectid", f.get("objectid")),
            kv_num("st_length", f.get("st_length(")),
        ]
    )

    return ee.String("{").cat(ee.String(pairs.join(","))).cat("}")


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
    # ── Initialize Earth Engine ───────────────────────────────────────────
    ee_initialize(gee_account_id)

    print(f"Inside process canal_vector for {state} - {district} - {block}")

    # ── Prepare ROI and asset path ────────────────────────────────────────
    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

        roi = ee.FeatureCollection(
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + f"filtered_mws_{asset_suffix}_uid"
        )

    description = f"{asset_suffix}_canal_vector"

    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )

    # ── Create asset if it does not exist ─────────────────────────────────
    if not is_gee_asset_exists(asset_id):
        roi = ee.FeatureCollection(roi)
        pan_india_data = ee.FeatureCollection(CANAL_PAN_INDIA_ASSET)

        # STEP 1 ── Clip canals to ROI bounding box
        clipped_canals = pan_india_data.filterBounds(roi.geometry())

        # STEP 2 ── For each CANAL find every ROI polygon it intersects
        #           Canal = left, ROI = right
        #           → a canal touching N polygons produces N roi_matches
        spatial_filter = ee.Filter.intersects(
            leftField=".geo", rightField=".geo", maxError=1
        )
        save_all_join = ee.Join.saveAll(matchesKey="roi_matches")
        canals_with_roi = save_all_join.apply(clipped_canals, roi, spatial_filter)

        # STEP 3 ── Flatten to one feature per (canal × ROI polygon) pair
        def canal_to_roi_features(canal_feat):
            canal_feat = ee.Feature(canal_feat)
            roi_matches = ee.List(canal_feat.get("roi_matches"))
            canal_json = build_canal_json(canal_feat)

            def make_pair(roi_feat):
                roi_feat = ee.Feature(roi_feat)
                uid = roi_feat.get("uid")
                area_in_ha = roi_feat.get("area_in_ha")

                return ee.Feature(roi_feat.geometry()).set(
                    {
                        "uid": uid,
                        "area_in_ha": area_in_ha,
                        "canal_available": True,
                        "misc": ee.String("[").cat(canal_json).cat("]"),
                    }
                )

            return roi_matches.map(make_pair)

        paired_fc = ee.FeatureCollection(
            canals_with_roi.toList(canals_with_roi.size())
            .map(canal_to_roi_features)
            .flatten()
        )

        # STEP 4 ── Group by uid → merge multiple canal JSON strings per polygon
        uid_filter = ee.Filter.equals(leftField="uid", rightField="uid")
        group_join = ee.Join.saveAll(matchesKey="same_uid")
        grouped = group_join.apply(
            paired_fc.distinct("uid"),  # one representative feature per uid
            paired_fc,
            uid_filter,
        )

        def merge_canals_for_uid(feature):
            feature = ee.Feature(feature)
            uid = feature.get("uid")
            area_in_ha = feature.get("area_in_ha")
            matches = ee.List(feature.get("same_uid"))

            # misc per pair is "[{...}]" — strip brackets to get "{...}"
            def extract_inner(f):
                f = ee.Feature(f)
                raw = ee.String(f.get("misc"))
                return raw.slice(1, raw.length().subtract(1))

            canal_parts = matches.map(extract_inner)
            misc_json = ee.String("[").cat(ee.String(canal_parts.join(","))).cat("]")

            return ee.Feature(feature.geometry()).set(
                {
                    "uid": uid,
                    "area_in_ha": area_in_ha,
                    "canal_available": True,
                    "misc": misc_json,
                    "same_uid": None,
                }
            )

        canal_fc = ee.FeatureCollection(grouped.map(merge_canals_for_uid))

        # STEP 5 ── Find ROI polygons with NO intersecting canal
        joined_uids = canal_fc.aggregate_array("uid")
        no_canal_roi = roi.filter(ee.Filter.inList("uid", joined_uids).Not())

        def make_blank(f):
            f = ee.Feature(f)
            return ee.Feature(f.geometry()).set(
                {
                    "uid": f.get("uid"),
                    "area_in_ha": f.get("area_in_ha"),
                    "canal_available": False,
                    "misc": ee.String("[]"),
                }
            )

        blank_fc = no_canal_roi.map(make_blank)

        # STEP 6 ── Merge canal + no-canal feature collections
        result_fc = canal_fc.merge(blank_fc)

        print(f"Total features with canal   : {canal_fc.size().getInfo()}")
        print(f"Total features without canal: {blank_fc.size().getInfo()}")
        print(f"Total result features       : {result_fc.size().getInfo()}")

        task = export_vector_asset_to_gee(result_fc, description, asset_id)
        task_id_list = check_task_status([task])
        print(f"Task completed. Task IDs: {task_id_list}")

    # ── Publish and sync if asset exists ──────────────────────────────────
    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)

        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            description,
            asset_id,
            "Canal Vector",
        )

        layer_at_geoserver = False
        merged_fc = ee.FeatureCollection(asset_id)

        sync_res = sync_fc_to_geoserver(merged_fc, state, description, "canal")

        if sync_res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            layer_at_geoserver = True

        return layer_at_geoserver

    return None
