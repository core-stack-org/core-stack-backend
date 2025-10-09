import ee

from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    is_gee_asset_exists,
    get_gee_dir_path,
    export_vector_asset_to_gee,
)


def waterbody_mws_intersection(
        roi=None,
        asset_suffix=None,
        asset_folder_list=None,
        app_type=None,
):
    description = "swb2_" + asset_suffix
    asset_id = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description
    )

    if is_gee_asset_exists(asset_id):
        return None, asset_id

    water_bodies = ee.FeatureCollection(
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + "swb1_"
        + asset_suffix
    )

    def intersect_with_mws(feature):
        mws = roi.filterBounds(feature.geometry())
        uid_list = mws.aggregate_array("uid")
        mws_uid = uid_list.join("_")
        return feature.set("MWS_UID", mws_uid)

    intersected_features = water_bodies.map(intersect_with_mws)

    size = intersected_features.size()
    intersected_features = intersected_features.toList(size)

    def get_waterbody_index(n):
        f = ee.Feature(intersected_features.get(n))
        i = ee.Number(n).toInt()
        s = ee.String("_").cat(ee.Number(i).format())
        # s = ee.String("_").cat(ee.String(i))
        return f.set("index", s)

    size = ee.Number(size).subtract(ee.Number(1))
    nl = ee.List.sequence(0, size)

    f = nl.map(get_waterbody_index)
    f = ee.FeatureCollection(f)

    def generate_waterbody_uid(feature):
        id1 = feature.get("MWS_UID")
        id2 = feature.get("index")
        s = ee.String(id1).cat(ee.String(id2))
        return feature.set("UID", s)

    fc = f.map(generate_waterbody_uid)

    columns_to_remove = ["index", "ID"]
    all_columns = fc.first().toDictionary().keys()
    columns_to_keep = all_columns.filter(
        ee.Filter.inList("item", columns_to_remove).Not()
    )
    fc_without_columns = fc.select(columns_to_keep)

    # Export results to GEE asset
    task_id = export_vector_asset_to_gee(fc_without_columns, description, asset_id)
    return task_id, asset_id
