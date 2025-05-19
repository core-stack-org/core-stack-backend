import ee
import numpy as np
from nrm_app.celery import  app
from utilities.gee_utils import (
    ee_initialize,
    get_distance_between_two_lan_long
)
from nrm_app.settings import PAN_INDIA_LULC_PATH, PAN_INDIA_MWS_PATH



def get_lulc_class(ref_lat, ref_long):
    ee_initialize()

    # CHANGE DE-SILTING LOCATION HERE
    reference_point = ee.Geometry.Point([ref_long, ref_lat])

    final_lulc_img = ee.Image(PAN_INDIA_LULC_PATH).select('predicted_label')
    point_value = final_lulc_img.reduceRegion(
        reducer=ee.Reducer.first(),
        geometry=reference_point,
        scale=10,
        maxPixels=1e9
    )
    lulc_class = point_value.getInfo().get('predicted_label')
    return lulc_class



def find_closest_water_pixel(lon, lat, lulc_class):
    reference_point = ee.Geometry.Point([lon, lat])
    final_lulc_img = ee.Image(PAN_INDIA_LULC_PATH).select('predicted_label')

    # STEP 1: CHECK IF THE REFERENCE POINT IS ALREADY A WATER PIXEL
    if lulc_class in [2, 3, 4]:
        print(f"The reference point is already a water pixel (class {lulc_class}).")
        return True, lat, lon, 0

    if lulc_class is None:
        print("Reference point has no LULC value. This could be outside the image bounds.")
        lulc_class = -1
    print(f"Reference point is not a water pixel (class {lulc_class}). Finding closest water pixel...")

    # STEP 2: CREATE A WATER MASK AROUND THE REFERENCE POINT
    water_mask = final_lulc_img.eq(2).Or(final_lulc_img.eq(3)).Or(final_lulc_img.eq(4))
    water_pixels = water_mask.selfMask()
    buffer_distance = 1500
    search_region = reference_point.buffer(buffer_distance)

    # STEP 3: CHECK IF WATER PIXELS EXIST IN THE SEARCH REGION
    water_pixel_count = water_pixels.reduceRegion(
        reducer=ee.Reducer.count(),
        geometry=search_region,
        scale=10,
        maxPixels=1e9
    ).getInfo().get('predicted_label', 0)

    if water_pixel_count == 0:
        print("WARNING: No water pixels found within 1 km of the reference point.")
        return False, None, None, -1

    # STEP 4: COMPUTE DISTANCE FROM REFERENCE POINT TO ALL WATER PIXELS
    point_image = ee.Image.constant(1).mask(
        ee.Image.constant(1).clip(reference_point.buffer(1))
    )
    distance_image = point_image.fastDistanceTransform(5000).sqrt()
    water_distance = distance_image.updateMask(water_mask)
    min_distance_result = water_distance.reduceRegion(
        reducer=ee.Reducer.min(),
        geometry=search_region,
        scale=10,
        maxPixels=1e9
    )
    min_dist_value = min_distance_result.getInfo().get('distance')

    # STEP 5: IDENTIFY ALL PIXELS AT THE MINIMUM DISTANCE
    epsilon = 0.1
    min_dist_pixels = water_distance.lte(ee.Image.constant(min_dist_value + epsilon)).And(
        water_distance.gte(ee.Image.constant(min_dist_value - epsilon))
    ).And(water_mask)
    min_dist_vectors = min_dist_pixels.selfMask().reduceToVectors(
        geometry=search_region,
        scale=10,
        geometryType='centroid',
        eightConnected=True,
        maxPixels=1e9
    )
    min_dist_count = min_dist_vectors.size().getInfo()

    # STEP 6: JUST TAKE ANY ONE CLOSEST PIXEL WITHOUT COMPLEX VERIFICATION
    if min_dist_count > 1:
        print(f"Multiple pixels found at the same minimum distance. Taking the first one.")

    # STEP 7: GET COORDINATES OF THE CLOSEST PIXEL AND VISUALIZE
    closest_point = min_dist_vectors.first()
    closest_coords = closest_point.geometry().coordinates().getInfo()
    closest_lon, closest_lat = closest_coords[0], closest_coords[1]

    # Calculate distance
    distance = get_distance_between_two_lan_long(lon, lat, closest_lon, closest_lat)
    print(f"Closest water pixel found at: [{closest_lat}, {closest_lon}]")
    print(f"Distance to closest water pixel: {distance:.2f} meters")

    return True, closest_lat, closest_lon, distance

def find_watersheds_for_point_with_buffer(latitude, longitude, buffer_distance=1500):

    point = ee.Geometry.Point([longitude, latitude])
    buffered_point = point.buffer(buffer_distance)
    WaterSheds = ee.FeatureCollection(PAN_INDIA_MWS_PATH)
    intersecting_watersheds = WaterSheds.filter(ee.Filter.intersects('.geo', buffered_point))
    return intersecting_watersheds, buffered_point

def generate_lulc_raster_for_intersecting_mws():
    ee_initialize()
    asset_id = 'projects/ee-corestackdev/assets/apps/waterrej/proj1'
    image = ee.Image(PAN_INDIA_LULC_PATH)

    # Load or define a vector region (e.g., a country)
    roi = ee.FeatureCollection(asset_id)


    # Clip the raster using the vector geometry
    clipped_image = image.clip(roi.geometry())

    # (Optional) Export the clipped image to Google Drive
    task = ee.batch.Export.image.toAsset(
        image=clipped_image,
        description='Export_Clipped_Image',
        assetId='projects/ee-corestackdev/assets/apps/waterrej/lulcfrom',
        region=roi.geometry(),
        scale=30,
        maxPixels=1e13
    )

    # Start the export task
    task.start()
    print("Export to asset started. Check Tasks tab in GEE.")

