import os
import requests, json
from rest_framework.response import Response
import pandas as pd
import pymannkendall as mk
import numpy as np
from shapely.geometry import Point, shape
import ast

path_to_add =''

def create_geojson_for_all_mws(existing_geojson_path, df, new_geojson_path):
    with open(existing_geojson_path) as f:
        existing_data = json.load(f)

    features = []

    for _, row in df.iterrows():
        uid = row['mws_id']
        geometry = None

        for feature in existing_data['features']:
            if feature['properties'].get('uid') == uid:
                geometry = feature['geometry']
                break

        if geometry is None:
            print(f"No geometry found for uid: {uid}. Using default geometry (e.g., None).")
            geometry = {"type": "Point", "coordinates": [0, 0]}

        properties = row.to_dict()

        new_feature = {
            'type': 'Feature',
            'geometry': geometry,
            'properties': properties
        }
        features.append(new_feature)

    new_feature_collection = {
        'type': 'FeatureCollection',
        'features': features
    }

    with open(new_geojson_path, 'w') as f:
        json.dump(new_feature_collection, f)


def get_generate_filter_mws_data(state, district, block, file_type):
    state_folder = state.replace(" ", "_").upper()
    district_folder = district.replace(" ", "_").upper() 
    file_xl_path = path_to_add + 'data/stats_excel_files/' + state_folder + '/' + district_folder + '/' + district + '_' + block
    xlsx_file = file_xl_path + '.xlsx'
    df_hydrological_annual = pd.read_excel(xlsx_file, sheet_name='hydrological_annual')

    results = []
    for specific_mws_id in df_hydrological_annual['UID'].unique():
        hydro_annual_mws_data = df_hydrological_annual[df_hydrological_annual['UID'] == specific_mws_id]
        precipitation_columns = hydro_annual_mws_data.filter(like='Precipitation')          # Avg_precipitation
        total_percipitation_column = precipitation_columns.shape[1]
        sum_precipitation = precipitation_columns.sum(axis=1).sum()
        avg_percipitation = round(sum_precipitation / total_percipitation_column, 4) 

        try:
            df_terrain_vector = pd.read_excel(xlsx_file, sheet_name='terrain')
            terrain_vector_mws_data = df_terrain_vector[df_terrain_vector['UID'] == specific_mws_id]
            terrainCluster_ID = terrain_vector_mws_data.get('terrainCluster_ID', None).iloc[0]        # terrain
        except:
            terrainCluster_ID = ''

        try:
            df_crp_intensity = pd.read_excel(xlsx_file, sheet_name='croppingIntensity_annual')
            df_crp_intensity_mws_data = df_crp_intensity[df_crp_intensity['UID'] == specific_mws_id]
            df_crp_intensity_mws_data = df_crp_intensity_mws_data.fillna(0)

            crp_Intensity_columns = df_crp_intensity_mws_data.filter(like='cropping_intensity')  # cropping_intensity_avg
            total_crp_Intensity_column = crp_Intensity_columns.shape[1]
            sum_crp_Intensity = crp_Intensity_columns.sum(axis=1).sum()
            cropping_intensity_avg = round(sum_crp_Intensity / total_crp_Intensity_column if total_crp_Intensity_column > 0 else 0, 4)

            ######### Cropping Intensity Trend  #################
            crp_intensity_T = df_crp_intensity_mws_data.filter(like='cropping_intensity').dropna()  # Drop rows with NaN for trend calculation
            crp_intensity_T = crp_intensity_T.squeeze().tolist()[:-3]
            result = mk.original_test(crp_intensity_T)

            def sens_slope(data):
                slopes = []
                for i in range(len(data) - 1):
                    for j in range(i + 1, len(data)):
                        s = (data[j] - data[i]) / (j - i)
                        slopes.append(s)
                return np.median(slopes)

            cropping_intensity_trend_value = sens_slope(crp_intensity_T)
            cropping_intensity_trend = None
            if result.trend == "no trend":
                cropping_intensity_trend = '0'
            elif result.trend == "increasing":
                cropping_intensity_trend = '1'
            else:
                cropping_intensity_trend = '-1'

            # Total cropped area, replace NaN with 0 for these columns as well
            total_cropped_area = df_crp_intensity_mws_data.iloc[0]['sum']

            # Handle single-cropped area calculation
            single_crop_columns = df_crp_intensity_mws_data.filter(like='single_cropped_area')  # avg_single_cropped
            total_single_crop_column = single_crop_columns.shape[1]
            sum_single_crop = single_crop_columns.sum(axis=1).sum()
            percent_single_crop = sum_single_crop * 100 / total_cropped_area if total_cropped_area > 0 else 0
            avg_single_cropped = round(percent_single_crop / total_single_crop_column if total_single_crop_column > 0 else 0, 4)

            # Handle doubly-cropped area calculation
            double_crop_columns = df_crp_intensity_mws_data.filter(like='doubly_cropped_area')  # avg_double_cropped
            total_double_crop_column = double_crop_columns.shape[1]
            sum_double_crop = double_crop_columns.sum(axis=1).sum()
            percent_double_crop = sum_double_crop * 100 / total_cropped_area if total_cropped_area > 0 else 0
            avg_double_cropped = round(percent_double_crop / total_double_crop_column if total_double_crop_column > 0 else 0, 4)

            # Handle triply-cropped area calculation
            triply_crop_columns = df_crp_intensity_mws_data.filter(like='triply_cropped_area')  # avg_triply_cropped
            total_triply_crop_column = triply_crop_columns.shape[1]
            sum_triply_crop = triply_crop_columns.sum(axis=1).sum()
            percent_triply_crop = sum_triply_crop * 100 / total_cropped_area if total_cropped_area > 0 else 0
            avg_triply_cropped = round(percent_triply_crop / total_triply_crop_column if total_triply_crop_column > 0 else 0, 4)

        except Exception as e:
            # Handle exception and ensure all variables are set
            cropping_intensity_avg = 0
            cropping_intensity_trend = ''
            avg_single_cropped = 0
            avg_double_cropped = 0
            avg_triply_cropped = 0
            print(f"Error occurred: {e}")

        try:
            df_swb_annual = pd.read_excel(xlsx_file, sheet_name='surfaceWaterBodies_annual')
            df_crp_intensity = pd.read_excel(xlsx_file, sheet_name='croppingIntensity_annual')
            df_crp_intensity_mws_data = df_crp_intensity[df_crp_intensity['UID'] == specific_mws_id]
            df_swb_annual_mws_data = df_swb_annual[df_swb_annual['UID'] == specific_mws_id]

            df_crp_intensity_mws_data = df_crp_intensity_mws_data.fillna(0)
            df_swb_annual_mws_data = df_swb_annual_mws_data.fillna(0)
            swb_area_kharif_columns = df_swb_annual_mws_data.filter(like='kharif_area')
            single_kharif_crop_columns = df_crp_intensity_mws_data.filter(like='single_kharif_cropped_area')
            double_crop_columns = df_crp_intensity_mws_data.filter(like='doubly_cropped_area')
            triply_crop_columns = df_crp_intensity_mws_data.filter(like='triply_cropped_area')

            combined_columns_kharif = single_kharif_crop_columns.add(double_crop_columns, fill_value=0)
            combined_columns_kharif = combined_columns_kharif.add(triply_crop_columns, fill_value=0)
            total_cropped_area_kharif = combined_columns_kharif.sum(axis=1).sum()
            total_swb_area_kharif_column = swb_area_kharif_columns.shape[1]
            sum_swb_area_kharif = swb_area_kharif_columns.sum(axis=1).sum()

            avg_wsr_ratio_kharif = sum_swb_area_kharif / total_cropped_area_kharif if total_cropped_area_kharif > 0 else 0
            avg_wsr_ratio_kharif = round(avg_wsr_ratio_kharif * 100 / total_swb_area_kharif_column, 4)
            swb_area_rabi_columns = df_swb_annual_mws_data.filter(like='rabi_area')
            single_non_kharif_crop_columns = df_crp_intensity_mws_data.filter(like='single_non_kharif_cropped_area')

            # Combine the cropping areas and calculate total cropped area for Rabi
            combined_columns_rabi = single_non_kharif_crop_columns.add(double_crop_columns, fill_value=0)
            combined_columns_rabi = combined_columns_rabi.add(triply_crop_columns, fill_value=0)
            total_cropped_area_rabi = combined_columns_rabi.sum(axis=1).sum()

            total_swb_rabi_column = swb_area_rabi_columns.shape[1]
            sum_swb_area_rabi = swb_area_rabi_columns.sum(axis=1).sum()
            
            # Average WSR ratio for Rabi
            avg_wsr_ratio_rabi = sum_swb_area_rabi / total_cropped_area_rabi if total_cropped_area_rabi > 0 else 0
            avg_wsr_ratio_rabi = round(avg_wsr_ratio_rabi * 100 / total_swb_rabi_column, 4)
            swb_area_zaid_columns = df_swb_annual_mws_data.filter(like='zaid_area')
            total_cropped_area_zaid = triply_crop_columns.sum(axis=1).sum()

            total_swb_zaid_column = swb_area_zaid_columns.shape[1]
            sum_swb_area_zaid = swb_area_zaid_columns.sum(axis=1).sum()
            avg_wsr_ratio_zaid = sum_swb_area_zaid / total_cropped_area_zaid if total_cropped_area_zaid > 0 else 0
            avg_wsr_ratio_zaid = round(avg_wsr_ratio_zaid * 100 / total_swb_zaid_column, 4)

        except Exception as e:
            avg_wsr_ratio_kharif = 0
            avg_wsr_ratio_rabi = 0
            avg_wsr_ratio_zaid = 0
            print(f"Error occurred: {e}")


        ############ Swb_average
        avg_kharif_surface_water_mws = 0
        avg_rabi_surface_water_mws  = 0
        avg_zaid_surface_water_mws = 0
        df_swb_annual = pd.read_excel(xlsx_file, sheet_name='surfaceWaterBodies_annual')
        df_swb_annual_mws_data = df_swb_annual[df_swb_annual['UID'] == specific_mws_id]
        if not df_swb_annual_mws_data.empty:
            total_swb_area = df_swb_annual_mws_data.iloc[0]['total_swb_area']
            
            if total_swb_area != 0:  # Check if total_swb_area is not zero
                swb_area_kharif_columns = df_swb_annual_mws_data.filter(like='kharif_area')
                total_swb_area_kharif_column = swb_area_kharif_columns.shape[1]
                sum_swb_area_kharif = swb_area_kharif_columns.sum(axis=1).sum()/ total_swb_area
                avg_kharif_surface_water_mws = round(sum_swb_area_kharif * 100 / total_swb_area_kharif_column if total_swb_area_kharif_column > 0 else 0, 4)

                swb_rabi_area_columns = df_swb_annual_mws_data.filter(like='rabi_area')
                total_swb_rabi_area_column = swb_rabi_area_columns.shape[1]
                sum_swb_rabi_area = swb_rabi_area_columns.sum(axis=1).sum()/ total_swb_area
                avg_rabi_surface_water_mws = round(sum_swb_rabi_area * 100 / total_swb_rabi_area_column if total_swb_rabi_area_column > 0 else 0, 4)

                swb_zaid_area_columns = df_swb_annual_mws_data.filter(like='zaid_area')
                total_swb_zaid_area_column = swb_zaid_area_columns.shape[1]
                sum_swb_zaid_area = swb_zaid_area_columns.sum(axis=1).sum()/ total_swb_area
                avg_zaid_surface_water_mws = round(sum_swb_zaid_area * 100 / total_swb_zaid_area_column if total_swb_zaid_area_column > 0 else 0, 4)
            else:
                avg_perc_kharif_surface_water_mws = avg_perc_rabi_surface_water_mws = avg_perc_zaid_surface_water_mws = 0
        else:
            print("DataFrame is empty. No data to process.")
            avg_perc_kharif_surface_water_mws = avg_perc_rabi_surface_water_mws = avg_perc_zaid_surface_water_mws = 0


        ######### G Trend  #################
        G_Trend = hydro_annual_mws_data.filter(like='G').drop(columns=hydro_annual_mws_data.filter(like='DeltaG').columns).dropna()
        G_Trend = G_Trend.squeeze().tolist()
        result = mk.original_test(G_Trend)
        def sens_slope(data):
            slopes = []
            for i in range(len(data) - 1):
                for j in range(i + 1, len(data)):
                    s = (data[j] - data[i]) / (j - i)
                    slopes.append(s)
            return np.median(slopes)

        trend_g_value = sens_slope(G_Trend)
        trend_g = None
        if result.trend=="no trend":
            trend_g = '0'
        elif result.trend=="increasing":
            trend_g = '1'
        else:
            trend_g = '-1'


        #########  drought_category  ##############
        try:
            df_croppingDrought_kharif = pd.read_excel(xlsx_file, sheet_name='croppingDrought_kharif')
            df_crpDrought_mws_data = df_croppingDrought_kharif[df_croppingDrought_kharif['UID'] == specific_mws_id]
            years = ['2017', '2018', '2019', '2020', '2021', '2022']
            sum_moderate_severe = {year: 1 if (df_crpDrought_mws_data.iloc[0][f'Moderate_{year}'] + df_crpDrought_mws_data.iloc[0][f'Severe_{year}']) > 5 else 0 for year in years}
            sum_of_values = sum(sum_moderate_severe.values())
            drought_category = None
            if sum_of_values>=2:
                drought_category = 2
            else:
                drought_category = sum_of_values


            ########   avg_dry_spell_in_weeks 
            dryspell_columns = df_crpDrought_mws_data.filter(like='drysp')   #avg_dry_spell_in_weeks
            total_dryspell_column = dryspell_columns.shape[1]
            sum_dryspell = dryspell_columns.sum(axis=1).sum()
            avg_dry_spell_in_weeks = round(sum_dryspell / total_dryspell_column if total_dryspell_column > 0 else 0, 4)
        except:
            drought_category = 0
            avg_dry_spell_in_weeks = 0


        ################# avg_runoff
        runoff_columns = hydro_annual_mws_data.filter(like='RunOff')          # avg_runoff
        total_runoff_column = runoff_columns.shape[1]
        sum_runoff = runoff_columns.sum(axis=1).sum()
        avg_runoff = sum_runoff / total_runoff_column 


        ############## Nrega Asset ##########################
        try:
            df_nrega_annual = pd.read_excel(xlsx_file, sheet_name='nrega_annual')
            df_nrega_assets_mws_data = df_nrega_annual[df_nrega_annual['mws_id'] == specific_mws_id]
            nrega_assets_sum = df_nrega_assets_mws_data.iloc[:, 1:].select_dtypes(include='number').sum().sum()
        except:
            nrega_assets_sum = 0


        ############ MWS Intersect Villages  ######################## 
        try:
            df_mws_inters_villages = pd.read_excel(xlsx_file, sheet_name='mws_intersect_villages')
            df_mws_inters_villages_mws_data = df_mws_inters_villages[df_mws_inters_villages['MWS UID'] == specific_mws_id]
            mws_intersect_villages = df_mws_inters_villages_mws_data.get('Village IDs', None).iloc[0]
            mws_intersect_villages = ast.literal_eval(mws_intersect_villages)
        except:
            mws_intersect_villages = ''


        ############  Change Detection Degradation  ###################
        try:
            df_mws_change_detection = pd.read_excel(xlsx_file, sheet_name='change_detection_degradation')
            df_change_detection_mws_data = df_mws_change_detection[df_mws_change_detection['UID'] == specific_mws_id]
            degradation_column = ['DEGR_Farm-Barren', 'DEGR_Farm-Built_Up', 'DEGR_Farm-Scrub_Land']
            degradation_land_area = df_change_detection_mws_data.get('DEGR_Total', None).iloc[0]
        except:
            degradation_land_area = 0

        
        ############  Change Detection Afforestation  ###################
        try:
            df_mws_change_detection = pd.read_excel(xlsx_file, sheet_name='change_detection_afforestation')
            df_change_detection_mws_data = df_mws_change_detection[df_mws_change_detection['UID'] == specific_mws_id]
            afforestation_column = ['AFFO_Barren-Forest', 'AFFO_Farm-Forest']
            afforestation_land_area = df_change_detection_mws_data.get('AFFO_total', None).iloc[0]
        except:
            afforestation_land_area = 0


        ############  Change Detection Deforestation  ###################
        try:
            df_mws_change_detection = pd.read_excel(xlsx_file, sheet_name='change_detection_deforestation')
            df_change_detection_mws_data = df_mws_change_detection[df_mws_change_detection['UID'] == specific_mws_id]
            deforestation_column = ['DEFO_Forest-Scrub_land', 'DEFO_Forest-Barren', 'DEFO_Forest-Built_Up', 'DEFO_Forest-Farm']
            deforestation_land_area = df_change_detection_mws_data.get('DEFO_total', None).iloc[0]
        except:
            deforestation_land_area = 0


        ############  Change Detection Urbanization  ###################
        try:
            df_mws_change_detection = pd.read_excel(xlsx_file, sheet_name='change_detection_urbanization')
            df_change_detection_mws_data = df_mws_change_detection[df_mws_change_detection['UID'] == specific_mws_id]
            urbanization_column = ['URBA_Barren/Shrub-Built_Up', 'URBA_Built_Up-Built_Up', 'URBA_Tree/Farm-Built_Up', 'URBA_Water-Built_Up']
            urbanization_land_area = df_change_detection_mws_data.get('URBA_Total', None).iloc[0]
        except:
            urbanization_land_area = 0


        ############# Terrain lulc slope / plain  #####################
        try:
            df_mws_terrain_lulc_slope = pd.read_excel(xlsx_file, sheet_name='terrain_lulc_slope')
            df_lulc_slope_mws_data = df_mws_terrain_lulc_slope[df_mws_terrain_lulc_slope['UID'] == specific_mws_id]
            lulc_slope_category = df_lulc_slope_mws_data.get('cluster_name', pd.NA).iloc[0] if not df_lulc_slope_mws_data.empty else None
        except:
            lulc_slope_category=''

        try:
            df_mws_terrain_lulc_plain = pd.read_excel(xlsx_file, sheet_name='terrain_lulc_plain')
            df_lulc_plain_mws_data = df_mws_terrain_lulc_plain[df_mws_terrain_lulc_plain['UID'] == specific_mws_id]
            lulc_plain_category = df_lulc_plain_mws_data.get('cluster_name', pd.NA).iloc[0] if not df_lulc_plain_mws_data.empty else None
        except:
            lulc_plain_category = ''



        results.append({
            'mws_id': specific_mws_id,
            'terrainCluster_ID': terrainCluster_ID,
            'avg_precipitation': avg_percipitation,
            'cropping_intensity_trend': cropping_intensity_trend,
            'cropping_intensity_avg': cropping_intensity_avg,
            'avg_single_cropped': avg_single_cropped,
            'avg_double_cropped': avg_double_cropped,
            'avg_triple_cropped': avg_triply_cropped,
            'avg_wsr_ratio_kharif': avg_wsr_ratio_kharif,
            'avg_wsr_ratio_rabi': avg_wsr_ratio_rabi,
            'avg_wsr_ratio_zaid': avg_wsr_ratio_zaid,
            'avg_kharif_surface_water_mws': avg_kharif_surface_water_mws,
            'avg_rabi_surface_water_mws': avg_rabi_surface_water_mws,
            'avg_zaid_surface_water_mws': avg_zaid_surface_water_mws,
            'trend_g': trend_g,
            'drought_category': drought_category,
            'avg_number_dry_spell': avg_dry_spell_in_weeks,
            'avg_runoff': round(avg_runoff,4),
            'total_nrega_assets': nrega_assets_sum,
            'mws_intersect_villages': mws_intersect_villages,
            'degradation_land_area': round(degradation_land_area,4),
            'afforestation_land_area': round(afforestation_land_area,4),
            'deforestation_land_area': round(deforestation_land_area,4),
            'urbanization_land_area': round(urbanization_land_area,4),
            'lulc_slope_category': lulc_slope_category,
            'lulc_plain_category': lulc_plain_category
        })

    results_df = pd.DataFrame(results)
    if file_type=='xlsx':
        results_df.to_excel(file_xl_path + '_KYL_filter_data.xlsx', index=False)
    elif file_type=='json':
        results_list = results_df.to_dict(orient='records')
        with open(file_xl_path + '_KYL_filter_data.json', 'w') as json_file:
            json.dump(results_list, json_file, indent=4)
    elif file_type=='geojson':
        layer_name = 'deltaG_well_depth_' + district + '_' + block
        mws_annual_geojson = get_url('mws_layers', layer_name)
        response = requests.get(mws_annual_geojson)
        response.raise_for_status()

        # Check if response has content
        if response.content:
            geojson_data = response.json()
            deltaG_geojson = file_xl_path + '_deltaG_annual.geojson'
            
            with open(deltaG_geojson, 'w') as f:
                json.dump(geojson_data, f)
        create_geojson_for_all_mws(deltaG_geojson, results_df, file_xl_path + '_KYL_filter_data.geojson')


def download_KYL_filter_data(state, district, block, file_type):
    state_folder = state.replace(" ", "_").upper()
    district_folder = district.replace(" ", "_").upper() 
    file_xl_path = path_to_add + 'data/stats_excel_files/' + state_folder + '/' + district_folder + '/' + district + '_' + block
    filename = None
    file_path = None
    if file_type=='xlsx':
        file_path = os.path.join(file_xl_path + '_KYL_filter_data.xlsx')
    elif file_type=='json':
        file_path = os.path.join(file_xl_path + '_KYL_filter_data.json')
    elif file_type=='geojson':
        file_path = os.path.join(file_xl_path + '_KYL_filter_data.geojson')

    if os.path.exists(file_path):
        return file_path
    else:
        return None
