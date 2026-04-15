import ee
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_dir_path,
    is_gee_asset_exists,
    make_asset_public,
    check_task_status
)
from computing.models import Dataset
from nrm_app.celery import app

@app.task(bind=True)
def calculate_temperature_humidity(
    self,
    state=None,
    district=None,
    block=None,
    app_type="MWS",
    year=2023,
    gee_account_id=None,
):
    """
    Generate coarse field level (~5km) temperature and humidity 
    raster layers and vectorize them onto the MWS ROI.
    """
    ee_initialize(gee_account_id)

    asset_suffix = valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    asset_folder_list = [state, district, block]

    roi_path = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + f"filtered_mws_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_uid"
    )

    roi = ee.FeatureCollection(roi_path)
    
    # MOD11C3 provides monthly LST at 0.05 degrees (~5km)
    modis_lst = ee.ImageCollection('MODIS/061/MOD11C3') \
        .filterDate(f'{year}-01-01', f'{year}-12-31') \
        .select('LST_Day_CMG') \
        .mean()
    
    # Convert LST from Kelvin to Celsius (scale factor is 0.02)
    # Formula: (DN * 0.02) - 273.15
    temperature = modis_lst.multiply(0.02).subtract(273.15).rename('mean_temperature')

    # ERA5 Land Monthly Aggregated for Humidity
    era5 = ee.ImageCollection('ECMWF/ERA5_LAND/MONTHLY_AGGR') \
        .filterDate(f'{year}-01-01', f'{year}-12-31') \
        .mean()
    
    t = era5.select('temperature_2m').subtract(273.15)
    td = era5.select('dewpoint_temperature_2m').subtract(273.15)
    
    # August-Roche-Magnus approximation for Relative Humidity
    # RH = 100 * (exp((17.625 * Td) / (243.04 + Td)) / exp((17.625 * T) / (243.04 + T)))
    expr = '100 * (exp((17.625 * Td) / (243.04 + Td)) / exp((17.625 * T) / (243.04 + T)))'
    humidity = era5.expression(expr, {
        'T': t, 'Td': td
    }).rename('mean_humidity')

    composite = temperature.addBands(humidity).clip(roi.geometry())
    
    # Calculate Mean Temperature and Humidity per ROI polygon
    reduced = composite.reduceRegions(**{
        'collection': roi,
        'reducer': ee.Reducer.mean(),
        'scale': 5000, 
    })

    # Add Area
    def add_area(f):
        area_km2 = f.geometry().area().divide(1e6)
        return f.set('Area_km2', area_km2)
    
    final_fc = reduced.map(add_area)

    dst_filename = f"temp_humid_{asset_suffix}_{year}"
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + dst_filename
    )
    
    if not is_gee_asset_exists(asset_id):
        task = ee.batch.Export.table.toAsset(**{
            'collection': final_fc,
            'description': f'TempHumid_{asset_suffix}_{year}',
            'assetId': asset_id
        })
        task.start()
        print(f"Started Export Table Task: {task.id}")
        check_task_status([task.id])

    make_asset_public(asset_id)
    
    layer_name = f"{asset_suffix}_temp_humid_{year}"
    
    dataset, _ = Dataset.objects.get_or_create(
        name="Temperature_Humidity",
        defaults={'layer_type': 'vector', 'workspace': 'climate'}
    )
    
    layer_id = save_layer_info_to_db(
        state, district, block,
        layer_name=layer_name,
        asset_id=asset_id,
        dataset_name=dataset.name,
        algorithm="TEMP_HUMID_ALGORITHM",
    )
    
    res = sync_fc_to_geoserver(ee.FeatureCollection(asset_id), state, layer_name, dataset.workspace)
    if res and type(res) == dict and res.get("status_code") == 201 and layer_id:
        update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
    
    return True
