import ee

from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    get_gee_dir_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
)

DEFAULT_PAN_INDIA_RIVER_ASSET = (
    "projects/ext-datasets/assets/datasets/River_pan_india"
)
DEFAULT_PAN_INDIA_CANAL_ASSET = (
    "projects/ext-datasets/assets/datasets/Canal_pan_india"
)
DEFAULT_WATERBODY_TYPE_BUFFER_M = 500
DEFAULT_PAN_INDIA_VILLAGE_ASSET = (
    "projects/ext-datasets/assets/datasets/Village_pan_india"
)
DEFAULT_PAN_INDIA_VILLAGE_NAME_FIELD = "Village Na"


def _safe_feature_collection(asset_id):
    if not asset_id:
        return None
    try:
        return ee.FeatureCollection(asset_id)
    except Exception:
        print(f"[SWB5] Asset not found or inaccessible: {asset_id}")
        return None


def add_waterbody_type_flag(
    swb_fc,
    river_asset_id=DEFAULT_PAN_INDIA_RIVER_ASSET,
    canal_asset_id=DEFAULT_PAN_INDIA_CANAL_ASSET,
    buffer_m=DEFAULT_WATERBODY_TYPE_BUFFER_M,
):
    river_fc = _safe_feature_collection(river_asset_id)
    canal_fc = _safe_feature_collection(canal_asset_id)

    if river_fc is None and canal_fc is None:
        return swb_fc.map(lambda f: ee.Feature(f).set("waterbody_type", "unknown"))

    if river_fc is not None:
        river_fc = river_fc.select(["rivname", "objectid", "ripcode"])
    if canal_fc is not None:
        canal_fc = canal_fc.select(["canname", "cancode", "prjname"])

    swb_extent = swb_fc.geometry().bounds().buffer(max(buffer_m * 5, 5000))
    if river_fc is not None:
        river_fc = river_fc.filterBounds(swb_extent)
    if canal_fc is not None:
        canal_fc = canal_fc.filterBounds(swb_extent)

    spatial_filter = ee.Filter.Or(
        ee.Filter.intersects(leftField=".geo", rightField=".geo", maxError=1),
        ee.Filter.withinDistance(
            distance=buffer_m, leftField=".geo", rightField=".geo", maxError=1
        ),
    )

    if river_fc is not None:
        river_join = ee.Join.saveFirst("river_match", outer=True)
        with_river = ee.FeatureCollection(
            river_join.apply(primary=swb_fc, secondary=river_fc, condition=spatial_filter)
        )
    else:
        with_river = swb_fc.map(lambda f: ee.Feature(f).set("river_match", None))

    if canal_fc is not None:
        canal_join = ee.Join.saveFirst("canal_match", outer=True)
        with_both = ee.FeatureCollection(
            canal_join.apply(primary=with_river, secondary=canal_fc, condition=spatial_filter)
        )
    else:
        with_both = with_river.map(lambda f: ee.Feature(f).set("canal_match", None))

    def classify(feature):
        feature = ee.Feature(feature)
        river_match = feature.get("river_match")
        canal_match = feature.get("canal_match")

        river_is_null = ee.Algorithms.IsEqual(river_match, None)
        canal_is_null = ee.Algorithms.IsEqual(canal_match, None)

        waterbody_type = ee.Algorithms.If(
            river_is_null,
            ee.Algorithms.If(canal_is_null, "individual", "canal"),
            "river",
        )
        waterbody_type_name = ee.Algorithms.If(
            river_is_null,
            ee.Algorithms.If(
                canal_is_null, None, ee.Feature(canal_match).get("canname")
            ),
            ee.Feature(river_match).get("rivname"),
        )

        river_objectid = ee.Algorithms.If(
            river_is_null, None, ee.Feature(river_match).get("objectid")
        )
        rip_code = ee.Algorithms.If(
            river_is_null, None, ee.Feature(river_match).get("ripcode")
        )
        canal_condition_fail = ee.Algorithms.If(river_is_null, canal_is_null, True)
        canal_code = ee.Algorithms.If(
            canal_condition_fail, None, ee.Feature(canal_match).get("cancode")
        )
        project_name = ee.Algorithms.If(
            canal_condition_fail, None, ee.Feature(canal_match).get("prjname")
        )

        return (
            feature.set(
                {
                    "waterbody_type": waterbody_type,
                    "waterbody_type_name": waterbody_type_name,
                    "river_objectid": river_objectid,
                    "rip_code": rip_code,
                    "canal_code": canal_code,
                    "project_name": project_name,
                    "river_asset_loaded": river_fc is not None,
                    "canal_asset_loaded": canal_fc is not None,
                }
            )
            .set("river_match", None)
            .set("canal_match", None)
        )

    return with_both.map(classify)


def add_village_name_flag(
    swb_fc,
    village_asset_id=DEFAULT_PAN_INDIA_VILLAGE_ASSET,
    village_name_field=DEFAULT_PAN_INDIA_VILLAGE_NAME_FIELD,
    clip_buffer_m=500,
):
    village_fc = _safe_feature_collection(village_asset_id)

    if village_fc is None:
        return swb_fc.map(
            lambda f: ee.Feature(f).set(
                {
                    "intersecting_villages": "[]",
                    "intersecting_villages_count": 0,
                    "covering_village_names": None,
                    "village_name": None,
                    "village_asset_loaded": False,
                }
            )
        )

    village_fc = village_fc.filterBounds(
        swb_fc.geometry().bounds().buffer(clip_buffer_m)
    )

    condition = ee.Filter.intersects(
        leftField=".geo", rightField=".geo", maxError=1
    )
    join = ee.Join.saveAll("village_matches", outer=True)
    with_village = ee.FeatureCollection(
        join.apply(primary=swb_fc, secondary=village_fc, condition=condition)
    )

    def classify(feature):
        feature = ee.Feature(feature)
        matches = ee.List(
            ee.Algorithms.If(
                feature.get("village_matches"),
                feature.get("village_matches"),
                ee.List([]),
            )
        )
        has_matches = matches.size().gt(0)

        names = ee.List(
            matches.map(lambda m: ee.Feature(m).get(village_name_field))
        ).removeAll([None]).distinct().sort()

        intersecting_villages = ee.Algorithms.If(
            has_matches,
            ee.String.encodeJSON(names),
            "[]",
        )
        intersecting_villages_count = ee.Algorithms.If(has_matches, names.size(), 0)
        covering_village_names = ee.Algorithms.If(
            has_matches, ee.String(names.join(", ")), None
        )

        return (
            feature.set(
                {
                    "intersecting_villages": intersecting_villages,
                    "intersecting_villages_count": intersecting_villages_count,
                    "covering_village_names": covering_village_names,
                    "village_name": covering_village_names,
                    "village_asset_loaded": True,
                }
            )
            .set("village_matches", None)
        )

    return with_village.map(classify)


def waterbody_pan_india_enrichment(
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type=None,
    gee_account_id=None,
    river_asset_id=None,
    canal_asset_id=None,
    waterbody_type_buffer_m=DEFAULT_WATERBODY_TYPE_BUFFER_M,
):
    """
    Enrich SWB features with pan-India river/canal type and village intersections.
    Input: SWB4 (WBC-enriched) asset if it exists, otherwise SWB3 (catchment/stream order).
    Output: SWB5 asset.
    """
    base = get_gee_dir_path(
        asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    swb3_asset = base + "swb3_" + asset_suffix
    swb4_asset = base + "swb4_" + asset_suffix
    description = "swb5_" + asset_suffix
    asset_id = base + description

    if is_gee_asset_exists(asset_id):
        print(f"[SWB5] Asset already exists, skipping export: {asset_id}")
        return None, asset_id

    use_swb4 = is_gee_asset_exists(swb4_asset)
    input_asset = swb4_asset if use_swb4 else swb3_asset
    if not is_gee_asset_exists(input_asset):
        print(f"[SWB5] No input asset at {input_asset}; cannot build SWB5.")
        return None, asset_id

    print(
        f"[SWB5] Using input {'swb4' if use_swb4 else 'swb3'}: {input_asset}"
    )
    river = river_asset_id or DEFAULT_PAN_INDIA_RIVER_ASSET
    canal = canal_asset_id or DEFAULT_PAN_INDIA_CANAL_ASSET
    print(f"[SWB5] river_asset_id: {river}, canal_asset_id: {canal}")

    water_bodies = ee.FeatureCollection(input_asset)
    typed = add_waterbody_type_flag(
        water_bodies,
        river_asset_id=river,
        canal_asset_id=canal,
        buffer_m=waterbody_type_buffer_m,
    )
    enriched = add_village_name_flag(
        typed,
        village_asset_id=DEFAULT_PAN_INDIA_VILLAGE_ASSET,
        village_name_field=DEFAULT_PAN_INDIA_VILLAGE_NAME_FIELD,
        clip_buffer_m=waterbody_type_buffer_m,
    )

    try:
        print(
            f"[SWB5] feature count: {enriched.size().getInfo()}"
        )
    except Exception as debug_err:
        print(f"[SWB5] debug logging failed: {debug_err}")

    task_id = export_vector_asset_to_gee(enriched, description, asset_id)
    return task_id, asset_id
