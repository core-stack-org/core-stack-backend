"""
Core GEE computation logic for temperature and humidity layer generation
"""
import ee
import json
from datetime import datetime, timedelta
from utilities.gee_utils import (
    ee_initialize,
    get_gee_dir_path,
    export_raster_asset_to_gee,
    export_vector_asset_to_gee,
    check_task_status,
    make_asset_public,
    valid_gee_text
)
from utilities.constants import GEE_PATHS


def get_aoi_geometry(state_obj, district_obj=None, block_obj=None):
    """
    Get Area of Interest geometry from administrative boundaries

    Args:
        state_obj: StateSOI model instance
        district_obj: DistrictSOI model instance (optional)
        block_obj: TehsilSOI model instance (optional)

    Returns:
        ee.Geometry: Area of interest
    """
    if block_obj and block_obj.geom:
        # Use block boundary
        geojson = json.loads(block_obj.geom.geojson)
        return ee.Geometry(geojson['geometry'])
    elif district_obj and district_obj.geom:
        # Use district boundary
        geojson = json.loads(district_obj.geom.geojson)
        return ee.Geometry(geojson['geometry'])
    elif state_obj and state_obj.geom:
        # Use state boundary
        geojson = json.loads(state_obj.geom.geojson)
        return ee.Geometry(geojson['geometry'])
    else:
        raise ValueError("No valid geometry found for the specified administrative level")


def get_date_range(start_date=None, end_date=None, temporal_range="monthly"):
    """
    Determine date range for analysis

    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        temporal_range: "monthly", "seasonal", or "annual"

    Returns:
        tuple: (start_date, end_date) as strings
    """
    if start_date and end_date:
        return start_date, end_date

    # Default to last complete period
    today = datetime.now()

    if temporal_range == "monthly":
        # Last complete month
        end_date = datetime(today.year, today.month, 1) - timedelta(days=1)
        start_date = datetime(end_date.year, end_date.month, 1)
    elif temporal_range == "seasonal":
        # Last complete season (3 months)
        month = today.month
        if month in [3, 4, 5]:  # Spring (Dec-Feb data)
            start_date = datetime(today.year - 1, 12, 1)
            end_date = datetime(today.year, 3, 1) - timedelta(days=1)
        elif month in [6, 7, 8]:  # Summer (Mar-May data)
            start_date = datetime(today.year, 3, 1)
            end_date = datetime(today.year, 6, 1) - timedelta(days=1)
        elif month in [9, 10, 11]:  # Monsoon (Jun-Aug data)
            start_date = datetime(today.year, 6, 1)
            end_date = datetime(today.year, 9, 1) - timedelta(days=1)
        else:  # Winter (Sep-Nov data)
            start_date = datetime(today.year, 9, 1)
            end_date = datetime(today.year, 12, 1) - timedelta(days=1)
    else:  # annual
        # Last complete year
        start_date = datetime(today.year - 1, 1, 1)
        end_date = datetime(today.year - 1, 12, 31)

    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def generate_temperature_layer(
    state_obj,
    district_obj=None,
    block_obj=None,
    start_date=None,
    end_date=None,
    temporal_range="monthly",
    gee_account_id=1
):
    """
    Generate temperature raster layer at 5km resolution using MODIS LST

    Args:
        state_obj: StateSOI model instance
        district_obj: DistrictSOI model instance (optional)
        block_obj: TehsilSOI model instance (optional)
        start_date: Start date for analysis
        end_date: End date for analysis
        temporal_range: Temporal aggregation level
        gee_account_id: GEE account ID to use

    Returns:
        dict: Asset path and statistics
    """
    # Initialize EE
    ee_initialize(gee_account_id)

    # Get AoI
    aoi = get_aoi_geometry(state_obj, district_obj, block_obj)

    # Get date range
    start_date, end_date = get_date_range(start_date, end_date, temporal_range)

    # Fetch MODIS Land Surface Temperature data
    modis_lst = ee.ImageCollection('MODIS/061/MOD11A1') \
        .filterBounds(aoi) \
        .filterDate(start_date, end_date) \
        .select(['LST_Day_1km', 'LST_Night_1km'])

    # Convert from Kelvin to Celsius and calculate mean
    def kelvin_to_celsius(image):
        day_temp = image.select('LST_Day_1km').multiply(0.02).subtract(273.15)
        night_temp = image.select('LST_Night_1km').multiply(0.02).subtract(273.15)
        mean_temp = day_temp.add(night_temp).divide(2)
        return mean_temp \
            .rename('temperature') \
            .copyProperties(image, ['system:time_start'])

    temperature_celsius = modis_lst.map(kelvin_to_celsius)

    # Calculate temporal mean
    mean_temperature = temperature_celsius.mean() \
        .clip(aoi) \
        .rename('mean_temperature')

    # Resample to 5km resolution - FIXED with proper projection
    projection = ee.Projection('EPSG:4326').atScale(5000)

    # Set default projection first
    temperature_with_projection = mean_temperature.setDefaultProjection(projection)

    # Now resample
    temperature_5km = temperature_with_projection \
        .reduceResolution(
            reducer=ee.Reducer.mean(),
            maxPixels=65536
        ) \
        .reproject(crs=projection, scale=5000)

    # Calculate statistics - FIXED reducer combination
    stats = temperature_5km.reduceRegion(
        reducer=ee.Reducer.mean()
            .combine(ee.Reducer.minMax(), '', True)
            .combine(ee.Reducer.stdDev(), '', True),
        geometry=aoi,
        scale=5000,
        maxPixels=1e9
    )

    # Build asset path
    path_components = []
    if state_obj:
        path_components.append(valid_gee_text(state_obj.name.lower()))
    if district_obj:
        path_components.append(valid_gee_text(district_obj.name.lower()))
    if block_obj:
        path_components.append(valid_gee_text(block_obj.name.lower()))

    base_path = GEE_PATHS.get('TEMPERATURE_HUMIDITY', {}).get(
        'GEE_ASSET_PATH',
        'projects/ee-corestackdev/assets/apps/temperature_humidity/'
    )

    asset_dir = get_gee_dir_path(path_components, asset_path=base_path)
    asset_name = f"temperature_5km_{'_'.join(path_components)}_{temporal_range}"
    asset_path = f"{asset_dir}{asset_name}"

    # Set metadata
    metadata = {
        'system:description': f'Temperature at 5km resolution for {" ".join(path_components)}',
        'data_source': 'MODIS LST',
        'temporal_range': f'{start_date} to {end_date}',
        'temporal_aggregation': temporal_range,
        'spatial_resolution': '5000 meters',
        'processing_date': datetime.now().isoformat(),
        'units': 'degrees Celsius',
        'projection': 'EPSG:4326'
    }

    temperature_5km = temperature_5km.set(metadata)

    # Export to GEE asset
    task_id = export_raster_asset_to_gee(
        image=temperature_5km,
        asset_id=asset_path,
        region=aoi,
        scale=5000,
        maxPixels=1e13
    )

    # Wait for export to complete
    check_task_status([task_id])

    # Make asset public
    make_asset_public(asset_path)

    return {
        'asset_path': asset_path,
        'statistics': stats.getInfo() if stats else {},
        'metadata': metadata
    }


def generate_humidity_layer(
    state_obj,
    district_obj=None,
    block_obj=None,
    start_date=None,
    end_date=None,
    temporal_range="monthly",
    gee_account_id=1
):
    """
    Generate humidity raster layer at 5km resolution using ERA5 data

    Args:
        state_obj: StateSOI model instance
        district_obj: DistrictSOI model instance (optional)
        block_obj: TehsilSOI model instance (optional)
        start_date: Start date for analysis
        end_date: End date for analysis
        temporal_range: Temporal aggregation level
        gee_account_id: GEE account ID to use

    Returns:
        dict: Asset path and statistics
    """
    # Initialize EE
    ee_initialize(gee_account_id)

    # Get AoI
    aoi = get_aoi_geometry(state_obj, district_obj, block_obj)

    # Get date range
    start_date, end_date = get_date_range(start_date, end_date, temporal_range)

    # Fetch ERA5-Land hourly data
    era5 = ee.ImageCollection('ECMWF/ERA5_LAND/HOURLY') \
        .filterBounds(aoi) \
        .filterDate(start_date, end_date) \
        .select(['temperature_2m', 'dewpoint_temperature_2m'])

    # Calculate relative humidity from temperature and dewpoint
    def calculate_relative_humidity(image):
        temp = image.select('temperature_2m').subtract(273.15)  # Convert to Celsius
        dewpoint = image.select('dewpoint_temperature_2m').subtract(273.15)

        # Magnus formula for relative humidity
        e_dewpoint = ee.Image.constant(6.112).multiply(
            ee.Image.constant(17.67).multiply(dewpoint)
            .divide(ee.Image.constant(243.5).add(dewpoint))
            .exp()
        )

        e_temp = ee.Image.constant(6.112).multiply(
            ee.Image.constant(17.67).multiply(temp)
            .divide(ee.Image.constant(243.5).add(temp))
            .exp()
        )

        humidity = e_dewpoint.divide(e_temp).multiply(100) \
            .rename('humidity') \
            .copyProperties(image, ['system:time_start'])

        return humidity

    relative_humidity = era5.map(calculate_relative_humidity)

    # Calculate temporal mean
    mean_humidity = relative_humidity.mean() \
        .clip(aoi) \
        .rename('mean_humidity')

    # Resample to 5km resolution - FIXED with proper projection
    projection = ee.Projection('EPSG:4326').atScale(5000)

    # Set default projection first
    humidity_with_projection = mean_humidity.setDefaultProjection(projection)

    # Now resample
    humidity_5km = humidity_with_projection \
        .reduceResolution(
            reducer=ee.Reducer.mean(),
            maxPixels=65536
        ) \
        .reproject(crs=projection, scale=5000)

    # Calculate statistics - FIXED reducer combination
    stats = humidity_5km.reduceRegion(
        reducer=ee.Reducer.mean()
            .combine(ee.Reducer.minMax(), '', True)
            .combine(ee.Reducer.stdDev(), '', True),
        geometry=aoi,
        scale=5000,
        maxPixels=1e9
    )

    # Build asset path
    path_components = []
    if state_obj:
        path_components.append(valid_gee_text(state_obj.name.lower()))
    if district_obj:
        path_components.append(valid_gee_text(district_obj.name.lower()))
    if block_obj:
        path_components.append(valid_gee_text(block_obj.name.lower()))

    base_path = GEE_PATHS.get('TEMPERATURE_HUMIDITY', {}).get(
        'GEE_ASSET_PATH',
        'projects/ee-corestackdev/assets/apps/temperature_humidity/'
    )

    asset_dir = get_gee_dir_path(path_components, asset_path=base_path)
    asset_name = f"humidity_5km_{'_'.join(path_components)}_{temporal_range}"
    asset_path = f"{asset_dir}{asset_name}"

    # Set metadata
    metadata = {
        'system:description': f'Humidity at 5km resolution for {" ".join(path_components)}',
        'data_source': 'ERA5-Land',
        'temporal_range': f'{start_date} to {end_date}',
        'temporal_aggregation': temporal_range,
        'spatial_resolution': '5000 meters',
        'processing_date': datetime.now().isoformat(),
        'units': 'percentage',
        'projection': 'EPSG:4326'
    }

    humidity_5km = humidity_5km.set(metadata)

    # Export to GEE asset
    task_id = export_raster_asset_to_gee(
        image=humidity_5km,
        asset_id=asset_path,
        region=aoi,
        scale=5000,
        maxPixels=1e13
    )

    # Wait for export to complete
    check_task_status([task_id])

    # Make asset public
    make_asset_public(asset_path)

    return {
        'asset_path': asset_path,
        'statistics': stats.getInfo() if stats else {},
        'metadata': metadata
    }


def generate_climate_vectors(
    temperature_asset,
    humidity_asset,
    state_obj,
    district_obj=None,
    block_obj=None,
    gee_account_id=1
):
    """
    Generate vector polygons from temperature and humidity rasters

    Args:
        temperature_asset: GEE asset path for temperature raster
        humidity_asset: GEE asset path for humidity raster
        state_obj: StateSOI model instance
        district_obj: DistrictSOI model instance (optional)
        block_obj: TehsilSOI model instance (optional)
        gee_account_id: GEE account ID to use

    Returns:
        dict: Asset path and polygon count
    """
    # Initialize EE
    ee_initialize(gee_account_id)

    # Get AoI
    aoi = get_aoi_geometry(state_obj, district_obj, block_obj)

    # Load rasters
    temperature = ee.Image(temperature_asset)
    humidity = ee.Image(humidity_asset)

    # Stack bands
    combined = temperature.addBands(humidity)

    # Create zones for vectorization (using temperature ranges)
    # Normalize temperature to 0-100 scale for zone creation
    temp_min = 15  # Typical minimum temperature
    temp_max = 40  # Typical maximum temperature
    zones = temperature.unitScale(temp_min, temp_max).multiply(100).int()

    # Convert to vectors
    vectors = zones.addBands(combined).reduceToVectors(
        geometry=aoi,
        scale=5000,
        geometryType='polygon',
        eightConnected=False,
        labelProperty='zone',
        reducer=ee.Reducer.mean(),
        maxPixels=1e13
    )

    # Calculate statistics for each polygon
    def add_statistics(feature):
        geometry = feature.geometry()

        # Calculate mean temperature
        mean_temp = temperature.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geometry,
            scale=5000,
            maxPixels=1e9
        )

        # Calculate mean humidity
        mean_hum = humidity.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geometry,
            scale=5000,
            maxPixels=1e9
        )

        # Calculate area in km²
        area = geometry.area().divide(1000000)

        return feature \
            .set('mean_temperature', mean_temp.get('mean_temperature')) \
            .set('mean_humidity', mean_hum.get('mean_humidity')) \
            .set('area_km2', area)

    vectors_with_stats = vectors.map(add_statistics)

    # Build asset path
    path_components = []
    if state_obj:
        path_components.append(valid_gee_text(state_obj.name.lower()))
    if district_obj:
        path_components.append(valid_gee_text(district_obj.name.lower()))
    if block_obj:
        path_components.append(valid_gee_text(block_obj.name.lower()))

    base_path = GEE_PATHS.get('TEMPERATURE_HUMIDITY', {}).get(
        'GEE_ASSET_PATH',
        'projects/ee-corestackdev/assets/apps/temperature_humidity/'
    )

    asset_dir = get_gee_dir_path(path_components, asset_path=base_path)
    asset_name = f"climate_vectors_5km_{'_'.join(path_components)}"
    asset_path = f"{asset_dir}{asset_name}"

    # Export to GEE asset
    task_id = export_vector_asset_to_gee(
        collection=vectors_with_stats,
        asset_id=asset_path
    )

    # Wait for export to complete
    check_task_status([task_id])

    # Make asset public
    make_asset_public(asset_path)

    # Get polygon count
    polygon_count = vectors_with_stats.size().getInfo()

    return {
        'asset_path': asset_path,
        'polygon_count': polygon_count
    }