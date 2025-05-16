import ee
from utilities.gee_utils import valid_gee_text, get_gee_asset_path, is_gee_asset_exists
from waterrejuvenation.utils import wait_for_task_completion
import logging

logger = logging.getLogger(__name__)

def calculate_swb2(aoi, force_regenerate, state=None, district=None, block=None, roi=None, app_name =None, proj_id=None):
    if state and block and district:
        description = (
        "swb2_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    )
        asset_id_swb1 = get_gee_asset_path(state, district, block) + description
        waterbodies = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "swb1_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
        )
    else:
        description = (
                "swb2_" +str(app_name) +"_" +str(proj_id)
        )
        asset_id_swb2 = "projects/ee-corestackdev/assets/apps/waterrej/" + str(proj_id) + "/" + description
        print ("projects/ee-corestackdev/assets/apps/waterrej/"+str(proj_id)+"/"+ "swb1_"+str(app_name)+"_"+str(proj_id))
        waterbodies = ee.FeatureCollection("projects/ee-corestackdev/assets/apps/waterrej/"+str(proj_id)+"/"+ "swb1_"+str(app_name)+"_"+str(proj_id))
    print ("completed")
    if is_gee_asset_exists(asset_id_swb2) and not force_regenerate:
        return None, asset_id_swb2



    def check_intersection(feature):
        mws = aoi.filterBounds(feature.geometry())
        uid_list = mws.aggregate_array("uid")
        mws_uid = uid_list.join("_")
        return feature.set("MWS_UID", mws_uid)

    intersected_features = waterbodies.map(check_intersection)
    print("intersection completed")
    print(asset_id_swb2)
    if force_regenerate:
        try:
             ee.data.deleteAsset(asset_id_swb2)

             logger.info(f"Deleted existing asset: {asset_id_swb2}")
        except Exception as e:
             logger.info(f"No existing asset to delete or error occurred: {e}")
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
    print ("checkpoint 1")
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
                "assetId": asset_id_swb2,
            }
        )

        swb_task.start()
        wait_for_task_completion(swb_task)

        print("Successfully started the swb2 task", swb_task.status())
        return  asset_id_swb2
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error occurred in running swb2 task: {e}")
