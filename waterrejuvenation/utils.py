from projects.models import Project
from utilities.gee_utils import (
    ee_initialize,
    get_distance_between_two_lan_long, get_gee_asset_path, check_task_status, gdf_to_ee_fc, sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver
)
WATER_REJ_GEE_ASSET='projects/ee-corestackdev/assets/apps/waterrej/'
import time
import ee
import logging
logger = logging.getLogger(__name__)



years = ['2017_2018', '2018_2019', '2019_2020', '2020_2021', '2021_2022', '2022_2023', '2023_2024']
def get_filtered_mws_layer_name(project_name, layer_name):
    return  WATER_REJ_GEE_ASSET+str(project_name)+'/'+str(layer_name)+'_'+str(project_name)

def gen_proj_roi(project_name):
    return WATER_REJ_GEE_ASSET+str(project_name)

def wait_for_task_completion(task):
    print(f"Waiting for task '{task.config.get('description')}' to finish...")
    while task.active():
        print(f"  Task state: {task.state}")
        time.sleep(30)  # Wait 30 seconds before checking again
    print(f"  Final task state: {task.state}")
    return task.state == 'COMPLETED'


def get_nearest_waterbody(lat, lon, waterbodies_asset_id):
    """
    Find the nearest waterbody to a given lat/lon point using a GEE asset.

    Parameters:
    - lat (float): Latitude of the point.
    - lon (float): Longitude of the point.
    - waterbodies_asset_id (str): GEE Asset ID for the waterbodies FeatureCollection.

    Returns:
    - dict: Properties of the nearest waterbody feature.
    """
    # Load waterbodies FeatureCollection from GEE asset

    waterbodies_fc = ee.FeatureCollection(waterbodies_asset_id)

    # Create a point geometry from the provided lat/lon
    point = ee.Geometry.Point([lon, lat])

    # Add distance to each waterbody polygon feature
    waterbodies_with_dist = waterbodies_fc.map(
        lambda f: f.set('distance', f.geometry().distance(point))
    )

    # Sort by distance and get the closest feature
    nearest_waterbody = waterbodies_with_dist.sort('distance').first()

    # Get the properties of the nearest waterbody
    nearest_info = nearest_waterbody.getInfo()

    # Return the properties of the nearest waterbody
    return nearest_info['properties']

def get_waterbody_id_for_lat_long(excel_hash, water_body_asset_id):
    ee_initialize()
    from waterrejuvenation.models import WaterbodiesDesiltingLog

    desilt_log = WaterbodiesDesiltingLog.objects.filter(excel_hash=excel_hash)
    for d in desilt_log:
        prop = get_nearest_waterbody(d.closest_wb_lat, d.closest_wb_long, water_body_asset_id)
        d.waterbody_id = prop['MWS_UID']
        d.save()

    return 'Success'

def get_water_mask(year):
    # Define your water classes
    water_classes = [2, 3, 5]



    image = ee.Image('projects/ee-corestackdev/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3_' + year)
    water_mask = image.select('predicted_label') \
            .remap(water_classes, [1] * len(water_classes), 0) \
            .rename('water') \
            .set('year', year)
    return water_mask

def fine_closest_wb_pixel(lon, lat):
    ee_initialize()
    reference_point = ee.Geometry.Point([lon, lat])
    final_water_mask = generate_water_mask_lulc()
    distance_image = final_water_mask.fastDistanceTransform(30).sqrt() \
        .rename('distance')

    # Step 3: Get the distance at the input point
    distance_result = distance_image.reduceRegion(
        reducer=ee.Reducer.first(),
        geometry=reference_point,
        scale=30,
        maxPixels=1e9
    )

    # Extract the result using correct band name
    closest_distance = distance_result.get('distance')

    if closest_distance is None:
        print("No water pixel found near this point.")
    else:
        print("Distance (in meters) to nearest water pixel:", closest_distance.getInfo())

def generate_water_mask_lulc():
    years = ['2017_2018', '2018_2019', '2019_2020', '2020_2021', '2021_2022', '2022_2023', '2023_2024']
    water_masks = [get_water_mask(year) for year in years]

    # Convert to image collection
    water_collection = ee.ImageCollection(water_masks)

    # Sum across years
    water_sum = water_collection.reduce(ee.Reducer.sum())

    # Final mask: where water was present in at least 2 years
    final_water_mask = water_sum.gte(2)
    return final_water_mask
    # merged_df = pd.merge(gdf_points, waterbodies[['latitude', 'longitude', 'waterbody_name', 'matched']],
    #                      left_on=['closest_wb_long', 'closest_wb_lat'],
    #                      right_on=['longitude', 'latitude'],
    #                      how='left')

    # for _, row in merged_df.iterrows():
    #     lat = row['closest_wb_lat']
    #     lon = row['closest_wb_long']
    #     matched = row.get('matched', False)
    #
    #     if pd.notna(matched) and matched:
    #         try:
    #
    #             data_dict = json.loads(matched)
    #             wb_id = data_dict['properties']['MWS_UID']
    #             print(wb_id)
    #             # Get the waterbody instance by its coordinates (or ID if available)
    #             desilt_log = WaterbodiesDesiltingLog.objects.filter(closest_wb_lat=row['closest_wb_lat'], closest_wb_long=row['closest_wb_long'])
    #             desilt_log_obj = desilt_log[0]
    #             desilt_log_obj.waterbody_id = wb_id
    #             desilt_log_obj.save()
    #             print (desilt_log)
    #             try:
    #                 desilt_log[1].delete()
    #             except Exception as e:
    #                 print ("desilt ")
    #
    #             # Update the matching desilting log record
    #             #desilt_log = WaterbodiesDesiltingLog.objects.get(closest_wb_lat=lat, closest_wb_long=lon)
    #             #desilt_log.waterbody_id = waterbody
    #             #desilt_log.save()
    #         except WaterbodiesDesiltingLog.DoesNotExist:
    #             print(f"Waterbody not found for lat={row['latitude']}, lon={row['longitude']}")
    #


def clip_lulc_output(mws_asset_id,  proj_id):
    ee_initialize()
    mws = ee.FeatureCollection(mws_asset_id)

    print(proj_id)
    proj_obj = Project.objects.get(pk = proj_id)
    lulc_asset_id_base = get_filtered_mws_layer_name(proj_obj.name, 'clipped_lulc_filtered_mws')
    PAN_INDIA_LULC_BASE_PATH = "projects/ee-corestackdev/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3"
    for year in years:
        asset_id = f"{lulc_asset_id_base}_{year}"
        delete_asset_on_GEE(asset_id)
        lulc_path = f'{PAN_INDIA_LULC_BASE_PATH}_{year}'  # Adjust this according to your naming convention
        lulc = ee.Image(lulc_path)

        # Clip the LULC image to the MWS boundary
        lulc_clipped = lulc.clip(mws)

        # Define export task
        description = f"lulc_clipped_task_{year}"
        asset_id = f"{lulc_asset_id_base}_{year}"  # Adjust destination asset ID accordingly
        print(asset_id)
        task = ee.batch.Export.image.toAsset(
            image=lulc_clipped,
            description=description,
            assetId=asset_id,
            region=mws.geometry(),
            scale=10,
            maxPixels=1e13
        )

        task.start()
        wait_for_task_completion(task)
        layer_name = 'clipped_lulc_filtered_mws_'+str(proj_obj.name)+'_'+str(proj_obj.id)+'_'+str(year)
        image = ee.Image(asset_id)
        task_id = sync_raster_to_gcs(image, 10, layer_name)

        task_id_list = check_task_status([task_id])
        print("task_id_list sync to gcs ", task_id_list)

        sync_raster_gcs_to_geoserver("waterrej", layer_name, layer_name,  "lulc_level_1_style")


def delete_asset_on_GEE(asset_id):
    ee_initialize()
    try:
        ee.data.deleteAsset(asset_id)
        logger.info(f"Deleted existing asset: {asset_id}")
    except Exception as e:
        logger.info(f"No existing asset to delete or error occurred: {e}")
