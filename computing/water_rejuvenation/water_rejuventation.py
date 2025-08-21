import ee
import numpy as np

from computing.utils import sync_project_fc_to_geoserver
from gee_computing.utils import extract_ndmis, classify_checkdams, mask_landsat_clouds
from nrm_app.celery import  app
from projects.models import Project
from utilities.gee_utils import (
    ee_initialize,
    get_distance_between_two_lan_long
)
from nrm_app.settings import PAN_INDIA_LULC_PATH, PAN_INDIA_MWS_PATH
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

import geemap.colormaps as cm
from rasterio.plot import show

from waterrejuvenation.utils import get_filtered_mws_layer_name, wait_for_task_completion, create_ring, \
    delete_asset_on_GEE, extract_feature_info


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

def compute_metrics(feature, elevation, ndmi_image, cropping_mask, scale = 30):
    buffer_radii = [i for i in range(100, 1501, 50)]  # 100m to 1500m with a step of 50m
    point_geom = feature.geometry()
    results = {}

    # Get elevation at the center point
    center_elevation = elevation.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=point_geom,
        scale=scale,
        maxPixels=1e9
    ).get('elevation')

    # Convert center_elevation to an Image for comparison
    center_elevation_img = ee.Image.constant(center_elevation)

    for radius in buffer_radii:
        # Create buffer and ring area
        buffer = point_geom.buffer(radius)
        buffer_area = buffer.difference(point_geom.buffer(radius - 50))

        # Apply cropping mask
        ndmi_masked = ndmi_image.updateMask(cropping_mask)

        # Apply elevation mask (downstream pixels only)
        elevation_mask = elevation.lt(center_elevation_img)
        ndmi_masked_downstream = ndmi_masked.updateMask(elevation_mask)

        # Compute NDMI mean and pixel count within the ring
        ndmi_mean = ndmi_masked_downstream.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=buffer_area,
            scale=scale,
            maxPixels=1e9
        ).get('NDMI')

        pixel_count = ndmi_masked_downstream.updateMask(ndmi_masked_downstream.neq(0)).reduceRegion(
            reducer=ee.Reducer.count(),
            geometry=buffer_area,
            scale=scale,
            maxPixels=1e9
        ).get('NDMI')

        # Handle null values
        ndmi_mean = ee.Algorithms.If(ee.Algorithms.IsEqual(ndmi_mean, None), -1, ndmi_mean)
        pixel_count = ee.Algorithms.If(ee.Algorithms.IsEqual(pixel_count, None), 0, pixel_count)

        # Store results
        results[f'{radius}m_NDMI'] = ndmi_mean
        results[f'{radius}m_pixel_count'] = pixel_count
    return feature.set(results)

def wrap_compute_metrics(elevation, ndmi_image, cropping_mask, scale=30):
    def wrapped(feature):
        return compute_metrics(feature, elevation, ndmi_image, cropping_mask, scale)
    return wrapped

def calculate_elevation(landsat_collection, lulc_asset_id):
    ndmi_image = landsat_collection.mean().normalizedDifference(['B5', 'B6']).rename('NDMI')
    lulc = ee.Image(lulc_asset_id)
    cropping_mask = lulc.eq(8).Or(lulc.eq(9)).Or(lulc.eq(10)).Or(lulc.eq(11))
    # Load elevation dataset
    elevation = ee.Image('USGS/SRTMGL1_003')
    return elevation, cropping_mask, ndmi_image

def get_year_value_pairs(feature, prefix):
    years = ['17-18', '18-19', '19-20', '20-21', '21-22', '22-23', '23-24']

    return ee.List([
        ee.Dictionary({'year': y, 'value': ee.Number(feature.get(f'{prefix}_{y}'))})
        for y in years
    ])

# Function to find max year & value from a given prefix
def get_max_year_value(pairs):
    def reduce_fn(a, b):
        a_val = ee.Number(ee.Dictionary(a).get('value'))
        b_val = ee.Number(ee.Dictionary(b).get('value'))
        return ee.Algorithms.If(b_val.gt(a_val), b, a)
    return pairs.iterate(reduce_fn, pairs.get(0))

# Main scoring function
def compute_water_score(feature):
    # Get year-value pairs
    krz_pairs = get_year_value_pairs(feature, 'krz')
    kr_pairs = get_year_value_pairs(feature, 'kr')
    k_pairs = get_year_value_pairs(feature, 'k')

    # Max values
    krz_max = get_max_year_value(krz_pairs)
    kr_max = get_max_year_value(kr_pairs)
    k_max = get_max_year_value(k_pairs)

    # Extract value from dicts
    krz_value = ee.Number(ee.Dictionary(krz_max).get('value'))
    kr_value = ee.Number(ee.Dictionary(kr_max).get('value'))
    k_value = ee.Number(ee.Dictionary(k_max).get('value'))

    # Logic: choose best season group
    best_info = ee.Algorithms.If(
        krz_value.gt(0), krz_max,
        ee.Algorithms.If(
            kr_value.gt(0), kr_max,
            k_max
        )
    )

    best_dict = ee.Dictionary(best_info)
    best_year = best_dict.get('year')
    best_score = best_dict.get('value')

    return feature.set({
        'water_score': best_score,
        'water_year': best_year
    })

def get_lulc_asset_from_year(year, proj_name):
    lulc_asset_id_base = get_filtered_mws_layer_name(proj_name, 'clipped_lulc_filtered_mws')
    start_date_ndmi, end_date_ndmi = '20'+year.split('-')[0]+'-07-01', '20'+year.split('-')[0]+'-10-30'
    start_date, end_date = '20' + year.split('-')[0] + '-07-01', '20' + year.split('-')[1] + '-06-30'
    asset_id = f"{lulc_asset_id_base}_{start_date}_{end_date}_LULCmap_10m"
    return str(asset_id), start_date_ndmi, end_date_ndmi


def get_farthest_point(feature):
    geometry = feature.geometry()
    centroid = geometry.centroid()

    def extract_coords(geom):
        geom_type = geom.type()
        coords = ee.Algorithms.If(
            geom_type.compareTo('Polygon').eq(0),
            ee.List(geom.coordinates().get(0)),  # Polygon
            ee.List(geom.coordinates().get(0)).get(0)  # MultiPolygon
        )
        return ee.List(coords)

    coords = extract_coords(geometry)

    # Compute distances and keep track of the farthest point
    def compute_distance(coord):
        coord = ee.List(coord)
        point = ee.Geometry.Point(coord)
        distance = point.distance(centroid)
        return ee.Dictionary({'coord': coord, 'distance': distance})

    coord_with_distances = ee.List(coords.map(compute_distance))

    def reducer(el, prev):
        el = ee.Dictionary(el)
        prev = ee.Dictionary(prev)
        return ee.Algorithms.If(
            el.getNumber('distance').gt(prev.getNumber('distance')),
            el,
            prev
        )

    max_coord = ee.Dictionary(coord_with_distances.iterate(
        reducer,
        ee.Dictionary({'coord': [0, 0], 'distance': 0})
    ))

    farthest_point = ee.Geometry.Point(ee.List(max_coord.get('coord')))
    return ee.Feature(farthest_point).copyProperties(feature).set('farthest_distance', max_coord.get('distance'))


def get_centroid_point(feature):
    geometry = feature.geometry()
    centroid = geometry.centroid()

    # Return centroid as feature, copying properties
    return ee.Feature(centroid).copyProperties(feature).set('type', 'centroid')

def generate_zoi_asset_on_gee(swb_asset_id, proj_id):
    ee_initialize()
    proj_obj = Project.objects.get(pk = proj_id)
    swb_feature_collection = ee.FeatureCollection(swb_asset_id)
    scored_fc = swb_feature_collection.map(compute_water_score)
    top_feature = scored_fc.sort('water_score', False).first()
    lulc_year = top_feature.getInfo()['properties']['water_year']
    lulc_asset_id, start_date, end_date = get_lulc_asset_from_year(lulc_year, proj_obj.name)
    landsat_collection = ee.ImageCollection('LANDSAT/LC08/C02/T1_TOA') \
        .filterDate(start_date, end_date) \
        .map(mask_landsat_clouds)
    elevation, cropping_mask, ndmi_image = calculate_elevation(landsat_collection, lulc_asset_id)
    # swb_centroids = swb_feature_collection.map(lambda feature:
    #                                    ee.Feature(feature.geometry().centroid()).copyProperties(feature)
    #                                    )
    swb_centroids = swb_feature_collection.map(get_farthest_point)
    asset_asset_ndmi = get_filtered_mws_layer_name(proj_obj.name, 'ndmi_layer')
    try:
        delete_asset_on_GEE(asset_asset_ndmi)
    except Exception as e:
        print ("asset not present")

    computed_collection = swb_centroids.map(wrap_compute_metrics(elevation, ndmi_image, cropping_mask))

    task = ee.batch.Export.table.toAsset(
        collection=computed_collection,
        description='waterej_ndmi_calc',
        assetId=asset_asset_ndmi,
        # fileFormat='KML'
    )
    task.start()
    wait_for_task_completion(task)
    ndmi_fc = ee.FeatureCollection(asset_asset_ndmi)
    ndmi_list = ndmi_fc.map(extract_ndmis)
    ndmi_lists = ndmi_list.aggregate_array('ndmis')
    lat_list = ndmi_list.aggregate_array('lat')
    lon_list = ndmi_list.aggregate_array('lon')
    wb_id_list = ndmi_list.aggregate_array('uid')
    wb_name_list = ndmi_list.aggregate_array('waterbody_name')
    lat_list_python = lat_list.getInfo()  # Convert latitudes to Python list
    lon_list_python = lon_list.getInfo()
    ndmi_lists_python = ndmi_lists.getInfo()
    wb_id_list_python = wb_id_list.getInfo()
    wb_name_list_python = wb_name_list.getInfo()
    filtered_ndmis = []
    filtered_lats = []
    filtered_lons = []
    filtered_wb_ids = []
    filtered_wb_names = []
    indices = []

    for i, ndmi_list in enumerate(ndmi_lists_python):
        if not any(val == -1 for val in ndmi_list):  # Check for -1 values
            indices.append(i)
            filtered_ndmis.append(ndmi_list)
            filtered_lats.append(lat_list_python[i])  # Now it's a Python list
            filtered_lons.append(lon_list_python[i])  # Now it's a Python list
            filtered_wb_ids.append(wb_id_list_python[i])
            filtered_wb_names.append(wb_name_list_python[i])

    ndmis_all = []
    lats_all = []
    lons_all = []
    wb_id_all = []
    wb_name_all = []
    for i in range(len(filtered_ndmis)):
        ndmis_all.append(np.array(filtered_ndmis[i]))
        lats_all.append(filtered_lats[i])
        lons_all.append(filtered_lons[i])
        wb_id_all.append(filtered_wb_ids[i])
        wb_name_all.append(filtered_wb_names[i])
    impactful, non_impactful, zoi_res = classify_checkdams(ndmis_all, poly_degree=8, threshold=0.010)
    features = []
    for i in range(len(lons_all)):
        print(i)
        if i in zoi_res:
            point = ee.Geometry.Point([lons_all[i], lats_all[i]])
            if i in non_impactful:
                impactful = False
            else:
                impactful = True

            feature = ee.Feature(point, {
                'zoi': zoi_res[i],
                'UID': wb_id_all[i],
                'waterbody_name': wb_name_all[i],
                'impactfull' : impactful
            })
            features.append(feature)

    zoi_fc = ee.FeatureCollection(features)
    asset_id_zoi = get_filtered_mws_layer_name(proj_obj.name, 'zoi_layer')
    try:
        delete_asset_on_GEE(asset_id_zoi)
    except Exception as e:
        print("asset not present")
    task = ee.batch.Export.table.toAsset(
        collection=zoi_fc,
        description='zoi_ndmi_export',
        assetId=asset_id_zoi
        # change to your path
    )

    task.start()
    wait_for_task_completion(task)
    input_fc = ee.FeatureCollection(asset_id_zoi)
    valid_features = input_fc.filter(ee.Filter.gt('zoi', 0))
    zoi_rings = valid_features.map(create_ring)
    asset_id_zoi_ring =  get_filtered_mws_layer_name(proj_obj.name, 'swb_zoi_ring')
    delete_asset_on_GEE(asset_id_zoi_ring)
    task = ee.batch.Export.table.toAsset(
        collection=zoi_rings,
        description='zoi_single_ring_export',
        assetId=asset_id_zoi_ring
    )
    print (asset_id_zoi_ring)

    task.start()
    wait_for_task_completion(task)
    zoi = ee.FeatureCollection(asset_id_zoi_ring)
    spatial_filter = ee.Filter.intersects(
        leftField='.geo',
        rightField='.geo'
    )

    # Define the join
    spatial_join = ee.Join.inner()

    # Apply the join
    joined = spatial_join.apply(swb_feature_collection, zoi, spatial_filter)

    # Map over joined pairs and create new features with ZOI geometry + merged attributes
    def merge_features(pair):
        water_feat = ee.Feature(pair.get('primary'))
        zoi_feat = ee.Feature(pair.get('secondary'))

        merged_props = water_feat.toDictionary().combine(zoi_feat.toDictionary(), True)
        return ee.Feature(zoi_feat.geometry(), merged_props)

    # Create the merged FeatureCollection
    zoi_with_water_props = ee.FeatureCollection(joined.map(merge_features))
    layer_name_swb = 'WaterRejapp-' + str(proj_obj.name) + '_' + str(proj_obj.id)
    sync_project_fc_to_geoserver(zoi_with_water_props, proj_obj.name, layer_name_swb, 'waterrej')
    wait_for_task_completion(task)

    layer_name = 'WaterRejapp_zoi_' + str(proj_obj.name) + '_' + str(proj_obj.id)
    return asset_id_zoi

def process_waterrej_zoi(swb_asset_id, proj_id):
    ee_initialize()
    proj_obj = Project.objects.get(pk=proj_id)

    # Step 1: Load SWB Features and Compute Water Score
    swb_fc = ee.FeatureCollection(swb_asset_id)
    scored_fc = swb_fc.map(compute_water_score)
    top_feature = scored_fc.sort('water_score', False).first()
    lulc_year = top_feature.getInfo()['properties']['water_year']

    # Step 2: Prepare Input Layers
    lulc_asset_id, start_date, end_date = get_lulc_asset_from_year(lulc_year, proj_obj.name)
    landsat = ee.ImageCollection('LANDSAT/LC08/C02/T1_TOA') \
        .filterDate(start_date, end_date) \
        .map(mask_landsat_clouds)
    elevation, cropping_mask, ndmi_img = calculate_elevation(landsat, lulc_asset_id)

    # Step 3: NDMI Computation at Centroid of waterbody
    swb_centroids = swb_fc.map(get_centroid_point)
    ndmi_asset = get_filtered_mws_layer_name(proj_obj.name, 'ndmi_layer')
    try:
        delete_asset_on_GEE(ndmi_asset)
    except Exception:
        print("NDMI asset not present, skipping delete.")

    ndmi_computed_fc = swb_centroids.map(wrap_compute_metrics(elevation, ndmi_img, cropping_mask))
    export_and_wait(ndmi_computed_fc, 'waterej_ndmi_calc', ndmi_asset)

    # Step 4: Extract NDMI Data and Filter Invalid Entries
    ndmi_fc = ee.FeatureCollection(ndmi_asset)

    ndmi_list, lat_list, lon_list, uid_list, name_list = extract_feature_info(ndmi_fc)


    #
    # Step 5: Filter Valid Features
    # filtered = [
    #     (i, ndmis, lat_list[i], lon_list[i], uid_list[i], name_list[i])
    #     for i, ndmis in enumerate(ndmi_list)
    #     if not any(val == -1 for val in ndmis)
    # ]

    filtered = [
        (i, ndmis, lat_list[i], lon_list[i], uid_list[i], name_list[i])
        for i, ndmis in enumerate(ndmi_list)
    ]

    ndmis_all, lats_all, lons_all, uids_all, names_all = zip(*[
        (np.array(n), lat, lon, uid, name)
        for _, n, lat, lon, uid, name in filtered
    ])

    # Step 6: Classify Check Dams by ZOI
    impactful, non_impactful, zoi_res = classify_checkdams(ndmis_all, poly_degree=8, threshold=0.010)
    features = []

    for i, (lon, lat, uid, name) in enumerate(zip(lons_all, lats_all, uids_all, names_all)):
        if i in zoi_res:
            point = ee.Geometry.Point([lon, lat])
            is_impactful = i not in non_impactful
            feature = ee.Feature(point, {
                'zoi': zoi_res[i]['zoi'],
                'UID': uid,
                'waterbody_name': name,
                'impactful': zoi_res[i]['impactful'],
                **zoi_res[i]['cumlative_score_array']
            })
            features.append(feature)

    zoi_fc = ee.FeatureCollection(features)
    zoi_asset = get_filtered_mws_layer_name(proj_obj.name, 'zoi_layer')
    try:
        delete_asset_on_GEE(zoi_asset)
    except Exception:
        print("ZOI asset not present, skipping delete.")
    export_and_wait(zoi_fc, 'zoi_ndmi_export', zoi_asset)

    # Step 7: Create ZOI Rings
    zoi_rings = ee.FeatureCollection(zoi_asset).filter(ee.Filter.gt('zoi', 0)).map(create_ring)
    zoi_ring_asset = get_filtered_mws_layer_name(proj_obj.name, 'swb_zoi_ring')
    delete_asset_on_GEE(zoi_ring_asset)
    export_and_wait(zoi_rings, 'zoi_single_ring_export', zoi_ring_asset)

    # Step 8: Join ZOI with SWB Properties
    spatial_filter = ee.Filter.intersects(leftField='.geo', rightField='.geo')
    joined = ee.Join.inner().apply(swb_fc, ee.FeatureCollection(zoi_ring_asset), spatial_filter)

    def merge_features(pair):
        primary = ee.Feature(pair.get('primary'))
        secondary = ee.Feature(pair.get('secondary'))
        combined = primary.toDictionary().combine(secondary.toDictionary(), True)
        return ee.Feature(secondary.geometry(), combined)

    merged_fc = ee.FeatureCollection(joined.map(merge_features))
    # Define the filter to match features where UID is equal
    join_filter = ee.Filter.equals(leftField='UID', rightField='UID')

    # Perform inner join
    inner_join = ee.Join.inner()
    joined = inner_join.apply(ndmi_fc, merged_fc, join_filter)

    # Merge properties and use geometry from merged_fc (secondary)
    def merge_ndmi_zoi_features(f):
        primary = ee.Feature(f.get('primary'))  # from ndmi_fc
        secondary = ee.Feature(f.get('secondary'))  # from merged_fc

        # Merge all properties from both features (primary first, then overwrite with secondary)
        merged_props = primary.toDictionary().combine(secondary.toDictionary(), overwrite=True)

        # Return a new feature with geometry from merged_fc (secondary)
        return ee.Feature(secondary.geometry(), merged_props)

    # Final merged FeatureCollection
    final_fc = ee.FeatureCollection(joined.map(merge_ndmi_zoi_features))
    # Step 9: Sync to GeoServer
    layer_name = f'WaterRejapp_zoi_{proj_obj.name}_{proj_obj.id}'
    asset_id_zoi = get_filtered_mws_layer_name(proj_obj.name, layer_name)
    delete_asset_on_GEE(asset_id_zoi)
    task = ee.batch.Export.table.toAsset(collection = final_fc,
                                         description = 'export zoi with ci',
                                         assetId = asset_id_zoi)
    task.start()
    wait_for_task_completion(task)
    sync_project_fc_to_geoserver(final_fc, proj_obj.name, layer_name, 'waterrej')

    return asset_id_zoi


def export_and_wait(collection, description, asset_id):
    """Helper to export EE collection and wait for task."""
    task = ee.batch.Export.table.toAsset(
        collection=collection,
        description=description,
        assetId=asset_id
    )
    task.start()
    wait_for_task_completion(task)


