from computing.utils import generate_swb_layer_with_max_so_catchment
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    valid_gee_text,
    get_gee_dir_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
)
import ee

from waterrejuvenation.utils import add_on_drainage_flag

# Update these to your exact pan-India assets.
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
# Common field names across village datasets are often `villname` or `village`.
# If your export shows blank, we can switch this field.
DEFAULT_PAN_INDIA_VILLAGE_NAME_FIELD = "Village Na"


def _safe_feature_collection(asset_id):
    if not asset_id:
        return None
    try:
        # For public datasets, metadata fetch can fail in some environments
        # even when the collection is readable in computations.
        return ee.FeatureCollection(asset_id)
    except Exception:
        print(f"[SWB4] Asset not found or inaccessible: {asset_id}")
        return None


def add_waterbody_type_flag(
    swb_fc,
    river_asset_id=DEFAULT_PAN_INDIA_RIVER_ASSET,
    canal_asset_id=DEFAULT_PAN_INDIA_CANAL_ASSET,
    buffer_m=DEFAULT_WATERBODY_TYPE_BUFFER_M,
):
    river_fc = _safe_feature_collection(river_asset_id)
    canal_fc = _safe_feature_collection(canal_asset_id)

    # If both datasets are unavailable, mark everything as unknown.
    if river_fc is None and canal_fc is None:
        return swb_fc.map(lambda f: ee.Feature(f).set("waterbody_type", "unknown"))

    # Keep only required fields on reference layers
    if river_fc is not None:
        river_fc = river_fc.select(["rivname", "objectid", "ripcode"])
    if canal_fc is not None:
        canal_fc = canal_fc.select(["canname", "cancode", "prjname"])

    # Critical optimization: clip massive pan-India layers to SWB extent first.
    # This dramatically reduces join graph size and avoids EE memory overflow.
    swb_extent = swb_fc.geometry().bounds().buffer(max(buffer_m * 5, 5000))
    if river_fc is not None:
        river_fc = river_fc.filterBounds(swb_extent)
    if canal_fc is not None:
        canal_fc = canal_fc.filterBounds(swb_extent)

    # Match on either direct intersect OR within configurable distance.
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
        # Canal info should be populated only when:
        # - river is null (so no river match)
        # - canal is NOT null
        canal_condition_fail = ee.Algorithms.If(river_is_null, canal_is_null, True)
        canal_code = ee.Algorithms.If(
            canal_condition_fail, None, ee.Feature(canal_match).get("cancode")
        )
        project_name = ee.Algorithms.If(
            canal_condition_fail, None, ee.Feature(canal_match).get("prjname")
        )

        # Drop temporary join payload fields to keep export graph light.
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
                    "covering_village_names": None,
                    "village_name": None,
                    "village_asset_loaded": False,
                }
            )
        )

    # Clip to relevant area.
    # Avoid `.select(...)` here because field names may vary (and using a wrong
    # field during select can drop properties or fail the whole map).
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
        matches = ee.List(ee.Algorithms.If(feature.get("village_matches"), feature.get("village_matches"), ee.List([])))
        has_matches = matches.size().gt(0)

        names = ee.List(
            matches.map(lambda m: ee.Feature(m).get(village_name_field))
        ).removeAll([None]).distinct().sort()

        covering_village_names = ee.Algorithms.If(
            has_matches, ee.String(names.join(", ")), None
        )

        return (
            feature.set(
                {
                    "covering_village_names": covering_village_names,
                    # Backward compatibility for existing consumers.
                    "village_name": covering_village_names,
                    "village_asset_loaded": True,
                }
            )
            .set("village_matches", None)
        )

    return with_village.map(classify)


def waterbody_catchment_streamorder_properties(
    roi=None,
    state=None,
    district=None,
    block=None,
    project_id=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type=None,
    gee_account_id=None,
    river_asset_id=DEFAULT_PAN_INDIA_RIVER_ASSET,
    canal_asset_id=DEFAULT_PAN_INDIA_CANAL_ASSET,
    waterbody_type_buffer_m=DEFAULT_WATERBODY_TYPE_BUFFER_M,
):
    print(f"asset suffix swb4: {asset_suffix}")
    print(f"[SWB4] river_asset_id: {river_asset_id}")
    print(f"[SWB4] canal_asset_id: {canal_asset_id}")
    print(f"[SWB4] waterbody_type_buffer_m: {waterbody_type_buffer_m}")
    description = "swb4_" + asset_suffix
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )
    asset_suffix_swb4 = "swb4_" + asset_suffix
    swb3_asset = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + "swb3_"
        + asset_suffix
    )

    swb2_asset = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + "swb2_"
        + asset_suffix
    )
    try:
        ee.data.getAsset(swb3_asset)
        water_bodies = ee.FeatureCollection(swb3_asset)
    except Exception as e:
        print("SWB3 does not exist")
        water_bodies = ee.FeatureCollection(swb2_asset)

    print(f"asset_i{water_bodies}")
    swb4_fs = generate_swb_layer_with_max_so_catchment(
        roi=water_bodies,
        asset_suffix=asset_suffix,
        asset_folder=asset_folder_list,
        app_type=app_type,
        gee_account_id=gee_account_id,
    )
    asset_id_dl = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + "drainage_lines_"
        + asset_suffix
    )
    swb4_fs_on_drainage = add_on_drainage_flag(swb4_fs, asset_id_dl)
    swb4_fc_with_waterbody_type = add_waterbody_type_flag(
        swb4_fs_on_drainage,
        river_asset_id=river_asset_id,
        canal_asset_id=canal_asset_id,
        buffer_m=waterbody_type_buffer_m,
    )
    swb4_fc_with_village = add_village_name_flag(
        swb4_fc_with_waterbody_type,
        village_asset_id=DEFAULT_PAN_INDIA_VILLAGE_ASSET,
        village_name_field=DEFAULT_PAN_INDIA_VILLAGE_NAME_FIELD,
        clip_buffer_m=waterbody_type_buffer_m,
    )

    # Lightweight debug logs only (avoid heavy getInfo on full collections).
    try:
        swb_count = swb4_fs_on_drainage.size().getInfo()
        typed_count = swb4_fc_with_waterbody_type.size().getInfo()
        print(f"[SWB4] swb_count_before_type: {swb_count}")
        print(f"[SWB4] swb_count_after_type: {typed_count}")
    except Exception as debug_err:
        print(f"[SWB4] debug logging failed: {debug_err}")

    task_id = export_vector_asset_to_gee(
        swb4_fc_with_village, description, asset_id
    )
    return task_id, asset_id
