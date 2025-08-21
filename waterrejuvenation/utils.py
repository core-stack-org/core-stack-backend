from computing.plantation.harmonized_ndvi import Get_Padded_NDVI_TS_Image
from computing.utils import sync_project_fc_to_geoserver
from projects.models import Project
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    get_distance_between_two_lan_long, get_gee_asset_path, check_task_status, gdf_to_ee_fc, sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver, sync_multiple_raster_gcs_to_geoserver, make_asset_public, is_gee_asset_exists,
    get_gee_dir_path
)


WATER_REJ_GEE_ASSET='projects/ee-corestackdev/assets/apps/waterbody/'
WATER_REJ_TEST_GEE_ASSET='projects/ee-kapil-test/assets/apps/waterbody/'
import time
import ee
import logging
logger = logging.getLogger(__name__)
from datetime import datetime
from nrm_app.settings import  lulc_years, water_classes
import pandas as pd
import math

years = ['2017_2018', '2018_2019','2019_2020', '2020_2021', '2021_2022', '2022_2023', '2023_2024']

def get_filtered_mws_layer_name(project_name, layer_name, project_id = 'ee-corestackdev'):
    WATER_REJ_GEE_ASSET = f"projects/{project_id}/assets/apps/waterbody/"
    return  WATER_REJ_GEE_ASSET+str(project_name.lower())+'/'+str(layer_name)+'_'+str(project_name.lower())

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
        print (d.closest_wb_lat, d.closest_wb_long)
        if (d.closest_wb_long and d.closest_wb_lat):
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

    print ("Given lat long")
    print (lat,lon)
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
    vector_fc = water_binary.reduceToVectors(
        geometry=buffer,
        geometryType='centroid',
        scale=10,
        labelProperty='water',
        maxPixels=1e8
    )

    first_feature = ee.Feature(vector_fc.first())
    coords = first_feature.geometry().coordinates()
    coords_list = coords.getInfo()

    # Convert EE List to Python list

    print(coords_list)
    return {
        'success': True,
        'latitude': coords_list[1],
        'longitude': coords_list[0],
        'distance_m': distance_value
    }

def clean(val):
    return 'N/A' if pd.isna(val) or str(val).strip() == '' else val


def generate_water_mask_lulc(years):
    water_masks = [get_water_mask(year) for year in years]

    # Convert to image collection
    water_collection = ee.ImageCollection(water_masks)

    # Sum across years
    water_sum = water_collection.reduce(ee.Reducer.sum())

    # Final mask: where water was present in at least 2 years
    final_water_mask = water_sum.gte(2)
    return final_water_mask


def clip_lulc_output(mws_asset_id,  proj_id, gee_project_id):

    mws = ee.FeatureCollection(mws_asset_id)
    try:

        clipped_geom = mws.simplify(100)
        print(f"Geometry simplified with tolerance {100} meters.")
    except Exception as e:
        print(f"Geometry simplification failed: {e}")
        clipped_geom = mws.bounds().buffer(1000)
        print(f"Using buffered bounding box with buffer {100} meters.")

    proj_obj = Project.objects.get(pk = proj_id)
    lulc_asset_id_base = get_filtered_mws_layer_name(proj_obj.name, 'clipped_lulc_filtered_mws', gee_project_id)
    PAN_INDIA_LULC_BASE_PATH = "projects/ee-corestackdev/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3"
    for year in years:
        start, end = year.split('_')[0], year.split('_')[1]
        start_date = f"{start}-07-01"
        end_date = f"{end}-06-30"
        asset_id = f"{lulc_asset_id_base}_{start_date}_{end_date}_LULCmap_10m"
        delete_asset_on_GEE(asset_id)
        lulc_path = f'{PAN_INDIA_LULC_BASE_PATH}_{year}'  # Adjust this according to your naming convention
        lulc = ee.Image(lulc_path)

        #Clip the LULC image to the MWS boundary
        lulc_clipped = lulc.clipToCollection(mws)

        # Define export task
        description = f"lulc_clipped_task_{year}"

        print(asset_id)
        task = ee.batch.Export.image.toAsset(
               image=lulc_clipped,
               description=description,
               assetId=asset_id,
               region=clipped_geom,
               scale=10,
               maxPixels=1e13
           )
        #  #
        task.start()
        wait_for_task_completion(task)
        make_asset_public(asset_id)
        gcs_file_name = 'raster_' + str(proj_obj.name) + '_' + str(proj_obj.id) + '_' + str(year)
        layer_name = 'clipped_lulc_filtered_mws_'+str(proj_obj.name)+'_'+str(proj_obj.id)+'_'+str(year)
        image = ee.Image(asset_id)
        task_id = sync_raster_to_gcs(image, 10, gcs_file_name)

        task_id_list = check_task_status([task_id])
        print("task_id_list sync to gcs ", task_id_list)

        sync_raster_gcs_to_geoserver("waterrej", gcs_file_name, layer_name,  "lulc_level_1_style")


def delete_asset_on_GEE(asset_id):
    try:
        ee.data.deleteAsset(asset_id)
        logger.info(f"Deleted existing asset: {asset_id}")
    except Exception as e:
        logger.info(f"No existing asset to delete or error occurred: {e}")

def create_ring(feature):
        point = feature.geometry()
        zoi = ee.Number(feature.get('zoi'))
        waterbody_name = feature.get('waterbody_name')
        impactfull = feature.get('impactful')
        uid = feature.get('UID')
        outer = point.buffer(zoi)
        inner = point.buffer(0)  # Optional, but keeps the logic clear
        ring = outer.difference(inner, 1)
        zoi_area = calculate_zoi_area(zoi)

        return ee.Feature(ring).set({
            'zoi': zoi,
            'waterbody_name':waterbody_name,
            'impactfull':impactfull,
            'UID':uid,
            'lon': point.coordinates().get(0),
            'lat': point.coordinates().get(1),
            'zoi_area': zoi_area

        })


def get_all_asset_name(project_id):
    proj_obj = Project.objects.get(pk = project_id)


def extract_feature_info(fc):
    """
    Extracts a list of (ndmis, lat, lon, uid, waterbody_name) tuples from a FeatureCollection.
    Handles:
    - Geometry-based lat/lon
    - Properties UID and waterbody_name
    - NDMI values from '100m_NDMI' to '1500m_NDMI'
    """
    distances = list(range(100, 1600, 50))
    ndmi_keys = [f"{d}m_NDMI" for d in distances]

    def parse_feature(feature):
        coords = ee.Feature(feature).geometry().coordinates()
        lon = coords.get(0)
        lat = coords.get(1)
        ndmi_values = [feature.get(k) for k in ndmi_keys]

        return ee.Feature(feature).set({
            'lon': lon,
            'lat': lat,
            'ndmis': ee.List(ndmi_values)
        })

    # Apply parsing function
    parsed_fc = fc.map(parse_feature)

    # Extract final arrays
    ndmis = parsed_fc.aggregate_array('ndmis').getInfo()
    lats = parsed_fc.aggregate_array('lat').getInfo()
    lons = parsed_fc.aggregate_array('lon').getInfo()
    uids = parsed_fc.aggregate_array('UID').getInfo()
    names = parsed_fc.aggregate_array('waterbody_name').getInfo()

    return ndmis, lats, lons, uids, names


def calculate_zoi_area(zoi):
    area_sqm = ee.Number(zoi).pow(2).multiply(math.pi)  # area in square meters
    area_hectares = area_sqm.divide(10_000)
    return ee.Number.parse(area_hectares.format('%.2f'))




def get_ndvi_data(suitability_vector, start_year, end_year, description, asset_id):
    """
    Extracts and exports NDVI data for a set of features by aggregating NDVI values
    into a per-feature dictionary {date: NDVI} over the specified time range.

    Each feature is stored as a single row, with NDVI values stored as a JSON string
    in a property called NDVI_<year>.

    Args:
        suitability_vector (ee.FeatureCollection): Features to calculate NDVI over.
        start_year (int): Start of the NDVI analysis range.
        end_year (int): End of the NDVI analysis range.
        description (str): Description to use in the export task name.
        asset_id (str): Base asset ID to export the result to.

    Returns:
        ee.FeatureCollection: Merged NDVI time series across years.
    """
    task_ids = []
    asset_ids = []
    # Loop over each year
    while start_year <= end_year:
        start_date = f"{start_year}-07-01"
        end_date = f"{start_year+1}-06-30"

        # Define export task details
        ndvi_description = f"ndvi_{start_year}_{description}"
        ndvi_asset_id = f"{asset_id}_ndvi_{start_year}"[:100]

        # Remove previous asset if it exists to avoid overwrite issues
        if is_gee_asset_exists(ndvi_asset_id):
            ee.data.deleteAsset(ndvi_asset_id)

        # Get NDVI image collection (with 'gapfilled_NDVI_lsc' band)
        ndvi = Get_Padded_NDVI_TS_Image(
            start_date, end_date, suitability_vector.bounds()
        )

        def map_image(image):
            date_str = image.date().format("YYYY-MM-dd")

            # Compute mean NDVI for all features at once
            reduced = image.reduceRegions(
                collection=suitability_vector,
                reducer=ee.Reducer.mean(),
                scale=10,
            )

            # Add NDVI value and image date to each feature
            def annotate(feature):
                ndvi_val = ee.Algorithms.If(
                    ee.Algorithms.IsEqual(feature.get("gapfilled_NDVI_lsc"), None),
                    -9999,
                    feature.get("gapfilled_NDVI_lsc"),
                )
                return feature.set("ndvi_date", date_str).set("ndvi", ndvi_val)

            return reduced.map(annotate)

        # Map image-wise extraction and flatten to a single FeatureCollection
        all_ndvi = ndvi.map(map_image).flatten()

        # Extract all unique UIDs from the input feature collection
        uids = suitability_vector.aggregate_array("UID")
        count = uids.size()  # Server-side count
        print ("total")
        print(count.getInfo())

        # For each UID, filter NDVI features and aggregate to dict
        def build_feature(uid):
            """
            Reconstruct a single feature by merging its NDVI values across all images
            into one property NDVI_<year> as a JSON dictionary {date: value}.
            """
            # Get the geometry and properties of the original feature
            feature_geom = ee.Feature(
                suitability_vector.filter(ee.Filter.eq("UID", uid)).first()
            )

            # Filter all NDVI records related to this UID
            filtered = all_ndvi.filter(ee.Filter.eq("UID", uid))

            # Create dictionary: {date: ndvi}
            date_ndvi_list = filtered.aggregate_array("ndvi_date").zip(
                filtered.aggregate_array("ndvi")
            )

            # Convert to dictionary and encode as JSON string
            ndvi_dict = ee.Dictionary(date_ndvi_list.flatten())
            ndvi_json = ee.String.encodeJSON(ndvi_dict)

            return feature_geom.set(f"NDVI_{start_year}", ndvi_json)

        # Apply feature-wise aggregation
        merged_fc = ee.FeatureCollection(uids.map(build_feature))

        # Export as single-row-per-feature collection
        try:
            task = ee.batch.Export.table.toAsset(
                collection=merged_fc,
                description=ndvi_description,
                assetId=ndvi_asset_id,
            )
            task.start()
            print(f"Started export for {start_year}")
            asset_ids.append(ndvi_asset_id)
            task_ids.append(task.status()["id"])
        except Exception as e:
            print("Export error:", e)

        start_year += 1

    check_task_status(task_ids)

    # Merge year-wise outputs into a single collection
    return merge_assets_chunked_on_year(asset_ids)


def merge_assets_chunked_on_year(chunk_assets):
    def merge_features(feature):
        # Get the unique ID of the current feature
        uid = feature.get("UID")
        matched_features = []
        for i in range(1, len(chunk_assets)):
            # Find the matching feature in the second collection
            matched_feature = ee.Feature(
                ee.FeatureCollection(chunk_assets[i])
                .filter(ee.Filter.eq("UID", uid))
                .first()
            )
            matched_features.append(matched_feature)

        merged_properties = feature.toDictionary()
        for f in matched_features:
            # Combine properties from both features
            merged_properties = merged_properties.combine(
                f.toDictionary(), overwrite=False
            )

        # Return a new feature with merged properties
        return ee.Feature(feature.geometry(), merged_properties)

    # Map the merge function over the first feature collection
    merged_fc = ee.FeatureCollection(chunk_assets[0]).map(merge_features)
    return merged_fc


def get_ndvi_for_zoi(zoi_asset_path, asset_suffix, asset_folder, app_type='WATER_REJ'):
    ndvi_asset_path = get_gee_dir_path(
        asset_folder, asset_path=GEE_PATHS['WATER_REJ']["GEE_ASSET_PATH"]) + "ndmi"

    zoi_collections = ee.FeatureCollection(zoi_asset_path)


    fc = get_ndvi_data(zoi_collections, 2017, 2024, 'ndmi', ndvi_asset_path)
    task = ee.batch.Export.table.toAsset(collection = fc, description = 'export_ndvi_waterrej_task',
            assetId = ndvi_asset_path)
    task.start()
    wait_for_task_completion(task)
    return ndvi_asset_path



def generate_draught_with_mws(draught_asset_id, mws_fc, proj_id):
    proj_obj = Project.objects.get(pk = proj_id)
    ee_initialize('')


    # Load FeatureCollections
    mw_fc = ee.FeatureCollection(mws_asset_id)
    draught_fc = ee.FeatureCollection(draught_asset_id)

    # Define spatial filter (intersects)
    spatial_filter = ee.Filter.intersects(
        leftField=".geo",  # geometry field of left collection
        rightField=".geo",
        maxError=1
    )

    # Define the join
    join = ee.Join.inner()

    # Apply the join
    joined = join.apply(mw_fc, draught_fc, spatial_filter)

    # Convert joined result into a proper FeatureCollection
    # by merging properties from both
    def merge_features(feature):
        left = ee.Feature(feature.get("primary"))
        right = ee.Feature(feature.get("secondary"))
        return left.copyProperties(right)

    final_fc = ee.FeatureCollection(joined.map(merge_features))
    layer_name = 'WaterRejapp_mws_' + str(proj_obj.name) + '_' + str(proj_obj.id)
    sync_project_fc_to_geoserver(final_fc, proj_obj.name, layer_name, 'waterrej')
