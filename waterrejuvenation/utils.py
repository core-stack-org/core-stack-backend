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
from datetime import datetime
from nrm_app.settings import  lulc_years, water_classes



years = ['2017_2018','2018_2019','2019_2020', '2020_2021', '2021_2022', '2022_2023', '2023_2024']
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
    water_classes = [2, 3, 4]
    image = ee.Image('projects/ee-corestackdev/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3_' + year)
    water_mask = image.select('predicted_label') \
            .remap(water_classes, [1] * len(water_classes), 0) \
            .rename('water') \
            .set('year', year)
    return water_mask

def fine_closest_wb_pixel(lon, lat):
    ee_initialize()


def find_nearest_water_pixel(lat, lon, distance_threshold):
    """
    Finds the nearest water pixel within a given threshold.

    Parameters:
        lat (float): Latitude of the input location.
        lon (float): Longitude of the input location.
        lulc_years (list of str): List of LULC year identifiers (e.g., '2017_2018').
        water_classes (list of int): List of LULC class codes considered as water.
        distance_threshold (float): Maximum distance (in meters) to consider.

    Returns:
        dict: {
            'success': bool,
            'latitude': float (if success),
            'longitude': float (if success),
            'distance_m': float (if success)
        }
    """
    ee_initialize()
    point = ee.Geometry.Point([lon, lat])

    # Create water masks from each LULC year
    water_masks = []
    for year in lulc_years:
        image = ee.Image(f'projects/ee-corestackdev/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3_{year}')
        water_mask = image.select('predicted_label') \
            .remap(water_classes, [1] * len(water_classes), 0) \
            .rename('water')
        water_masks.append(water_mask)

    # Combine water masks across years to get union of water pixels
    water_union = ee.ImageCollection(water_masks).sum()
    water_binary = water_union.gte(2).selfMask()



    # Compute distance to nearest water pixel (in meters)
    distance_img = water_binary.fastDistanceTransform(30).sqrt().multiply(30).rename('distance')
    distance_to_water = distance_img.reduceRegion(
        reducer=ee.Reducer.min(),
        geometry=point,
        scale=10,
        maxPixels=1e9
    ).get('distance')

    # Convert EE object to native value
    distance_value = ee.Number(distance_to_water).getInfo()
    print (distance_value)
    if distance_value is None or distance_value > distance_threshold:
        return {'success': False}

    # Buffer the point to extract nearby water pixels
    buffer = point.buffer(distance_value + 30)

    # Get coordinates of the closest water pixel
    water_vector = water_binary.reduceToVectors(
        geometry=buffer,
        geometryType='centroid',
        scale=10,
        labelProperty='water',
        maxPixels=1e8
    ).geometry().coordinates().get(0)

    coords = ee.List(water_vector).getInfo()

    return {
        'success': True,
        'latitude': coords[1],
        'longitude': coords[0],
        'distance_m': distance_value
    }
def generate_water_mask_lulc(years):
    water_masks = [get_water_mask(year) for year in years]

    # Convert to image collection
    water_collection = ee.ImageCollection(water_masks)

    # Sum across years
    water_sum = water_collection.reduce(ee.Reducer.sum())

    # Final mask: where water was present in at least 2 years
    final_water_mask = water_sum.gte(2)
    return final_water_mask


def clip_lulc_output(mws_asset_id,  proj_id):
    ee_initialize()
    mws = ee.FeatureCollection(mws_asset_id)

    print(proj_id)
    proj_obj = Project.objects.get(pk = proj_id)
    lulc_asset_id_base = get_filtered_mws_layer_name(proj_obj.name, 'clipped_lulc_filtered_mws')
    PAN_INDIA_LULC_BASE_PATH = "projects/ee-corestackdev/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3"
    for year in years:
        current_date_time = int(datetime.now().timestamp())
        asset_id = f"{lulc_asset_id_base}_{year}"
        delete_asset_on_GEE(asset_id)
        lulc_path = f'{PAN_INDIA_LULC_BASE_PATH}_{year}'  # Adjust this according to your naming convention
        lulc = ee.Image(lulc_path)

        # Clip the LULC image to the MWS boundary
        lulc_clipped = lulc.clip(mws.geometry())

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
        gcs_file_name = 'raster_' + str(proj_obj.name) + '_' + str(proj_obj.id) + '_' + str(year)
        layer_name = 'clipped_lulc_filtered_mws_'+str(proj_obj.name)+'_'+str(proj_obj.id)+'_'+str(year)
        image = ee.Image(asset_id)
        task_id = sync_raster_to_gcs(image, 10, gcs_file_name)

        task_id_list = check_task_status([task_id])
        print("task_id_list sync to gcs ", task_id_list)

        sync_raster_gcs_to_geoserver("waterrej", gcs_file_name, layer_name,  "lulc_level_1_style")


def delete_asset_on_GEE(asset_id):
    ee_initialize()
    try:
        ee.data.deleteAsset(asset_id)
        logger.info(f"Deleted existing asset: {asset_id}")
    except Exception as e:
        logger.info(f"No existing asset to delete or error occurred: {e}")
