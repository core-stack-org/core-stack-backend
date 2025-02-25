import ee
from utilities.gee_utils import valid_gee_text, get_gee_asset_path, is_gee_asset_exists


def calculate_swb2(aoi, state, district, block):
    description = (
        "swb2_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    )
    asset_id = get_gee_asset_path(state, district, block) + description

    if is_gee_asset_exists(asset_id):
        return None, asset_id

    waterbodies = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "swb1_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )

    def check_intersection(feature):
        mws = aoi.filterBounds(feature.geometry())
        uid_list = mws.aggregate_array("uid")
        mws_uid = uid_list.join("_")
        return feature.set("MWS_UID", mws_uid)

    intersected_features = waterbodies.map(check_intersection)

    size = intersected_features.size()
    intersected_features = intersected_features.toList(size)

    def ui(n):
        f = ee.Feature(intersected_features.get(n))
        i = ee.Number(n).toInt()
        s = ee.String("_").cat(ee.Number(i).format())
        # s = ee.String("_").cat(ee.String(i))
        return f.set("index", s)

    size = ee.Number(size).subtract(ee.Number(1))
    nl = ee.List.sequence(0, size)

    f = nl.map(ui)
    f = ee.FeatureCollection(f)

    def final(feature):
        id1 = feature.get("MWS_UID")
        id2 = feature.get("index")
        s = ee.String(id1).cat(ee.String(id2))
        return feature.set("UID", s)

    collec = f.map(final)
    fc = collec

    columns_to_remove = ["index", "ID"]
    all_columns = fc.first().toDictionary().keys()
    columns_to_keep = all_columns.filter(
        ee.Filter.inList("item", columns_to_remove).Not()
    )
    fc_without_columns = fc.select(columns_to_keep)
    try:

        swb_task = ee.batch.Export.table.toAsset(
            **{
                "collection": fc_without_columns,
                "description": description,
                "assetId": asset_id,
            }
        )

        swb_task.start()
        print("Successfully started the swb2 task", swb_task.status())
        return swb_task.status()["id"], asset_id
    except Exception as e:
        print(f"Error occurred in running swb2 task: {e}")
