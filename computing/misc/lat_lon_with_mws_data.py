import ee
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
)
from datetime import datetime
from nrm_app.settings import EXCEL_PATH
import json
import pandas as pd
import numpy as np
from stats_generator.mws_indicators import get_generate_filter_mws_data


def get_location_info_by_lat_lon(lon, lat):
    ee_initialize()
    point = ee.Geometry.Point([lon, lat])
    feature_collection = ee.FeatureCollection("projects/ee-corestackdev/assets/datasets/SOI_tehsil_vector")
    intersected = feature_collection.filterBounds(point)

    features = intersected.toList(intersected.size())

    for i in range(intersected.size().getInfo()):
        feature = ee.Feature(features.get(i))
        return feature.getInfo()['properties']



def get_mws_id_by_lat_lon(lon, lat):
    ee_initialize()
    data_dict = get_location_info_by_lat_lon(lon, lat)
    print("data_dict", data_dict)

    state = data_dict['STATE']
    district = data_dict['District']
    block = data_dict['TEHSIL']
    print(state, district, block)

    asset_path = get_gee_asset_path(state, district, block)
    mws_asset_id = asset_path + f'filtered_mws_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_uid'
    mws_fc = ee.FeatureCollection(mws_asset_id)
    point = ee.Geometry.Point([lon, lat])
    matching_feature = mws_fc.filterBounds(point).first()
    uid = ee.String(matching_feature.get('uid')).getInfo()

    return {
        'uid': uid,
        'state': state,
        'district': district,
        'block': block
    }



def get_mws_json_from_stats_excel(state, district, block, mws_id):
    state_folder = state.replace(" ", "_").upper()
    district_folder = district.replace(" ", "_").upper() 
    file_xl_path = EXCEL_PATH + 'data/stats_excel_files/' + state_folder + '/' + district_folder + '/' + district + '_' + block
    xlsx_file = file_xl_path + '.xlsx'

    sheets = {
        'hydrological_annual': -1,
        'terrain': -1,
        'croppingIntensity_annual': -1,
        'surfaceWaterBodies_annual': -1,
        'croppingDrought_kharif': -1,
        'nrega_annual': -1,
        'mws_intersect_villages': -1,
        'change_detection_degradation': -1,
        'change_detection_afforestation': -1,
        'change_detection_deforestation': -1,
        'change_detection_urbanization': -1,
        'change_detection_cropintensity': -1,
        'terrain_lulc_slope': -1,
        'terrain_lulc_plain': -1,
        'restoration_vector': -1,
        'aquifer_vector': -1,
        'soge_vector': -1,
    }

    result = {}

    try:
        xls = pd.ExcelFile(xlsx_file)
    except Exception as e:
        return {"error": f"Failed to read Excel file: {str(e)}"}

    for sheet_name in sheets:
        if sheet_name not in xls.sheet_names:
            print(sheet_name, "is not available for this location")
            continue

        try:
            df = xls.parse(sheet_name)
            df.columns = [col.strip().lower() for col in df.columns]
            if sheet_name=='nrega_annual':
                filtered_df = df[df['mws_id'] == mws_id]
            elif sheet_name=='mws_intersect_villages':
                filtered_df = df[df['mws uid'] == mws_id]
            else:
                filtered_df = df[df['uid'] == mws_id]

            if not filtered_df.empty:
                result[sheet_name] = filtered_df.to_dict(orient='records')

        except Exception as e:
            result[sheet_name] = {"error": f"Error processing sheet: {str(e)}"}

    return result


def get_mws_json_from_kyl_indicator(state, district, block, mws_id):
    get_generate_filter_mws_data(state, district, block, 'xlsx')
    state_folder = state.replace(" ", "_").upper()
    district_folder = district.replace(" ", "_").upper()
    file_xl_path = EXCEL_PATH + 'data/stats_excel_files/' + state_folder + '/' + district_folder + '/' + district + '_' + block
    xlsx_file = file_xl_path + '_KYL_filter_data.xlsx'

    try:
        df = pd.read_excel(xlsx_file)
        df.columns = [col.strip().lower() for col in df.columns]

        if 'mws_id' not in df.columns:
            return {"error": "'mws_id' column not found in Excel file."}

        filtered_df = df[df['mws_id'] == mws_id]
        filtered_df = filtered_df.replace([np.inf, -np.inf], np.nan)

        # Convert to dict with null-safe serialization
        json_compatible = json.loads(
            filtered_df.to_json(orient='records', default_handler=str)
        )

        return json_compatible

    except Exception as e:
        return {"error": f"Error reading or processing file: {str(e)}"}

