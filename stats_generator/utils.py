from django.http import HttpResponse, Http404
import os
import requests, json
from rest_framework.response import Response
from rest_framework import status
import pandas as pd
import geopandas as gpd
from collections import defaultdict
from datetime import datetime
from nrm_app.settings import GEOSERVER_URL
import pymannkendall as mk
import numpy as np
from shapely.geometry import Point, shape


path_to_add = '/home/ubuntu/prod_dir/nrm-app/'


workspace_list = ['terrain', 'nrega_assets', 'water_bodies', 'cropping_intensity', 'cropping_drought', 'mws_layers', 'mws_layers_fort', 'panchayat_boundaries', 'terrain_lulc_plain', 'change_detection']
#workspace_list = ['nrega_assets', 'mws_layers']

def get_url(workspace, layer_name):
    print("get_url",  workspace, layer_name)
    """Construct the GeoServer WFS request URL for fetching GeoJSON data."""
    geojson_url = f"{GEOSERVER_URL}/{workspace}/ows?service=WFS&version=1.0.0&request=GetFeature&typeName={workspace}:{layer_name}&outputFormat=application/json"
    print("layer_url",  geojson_url)
    return geojson_url

def get_vector_layer_geoserver(state, district, block):
    """Fetch vector layer data from GeoServer and save it as a beautified GeoJSON file."""
    state_folder = state.replace(" ", "_").upper()
    district_folder = district.replace(" ", "_").upper()
    base_path = path_to_add + 'data/stats_excel_files/'
    
    state_path = os.path.join(base_path, state_folder)
    if not os.path.exists(state_path):
        os.makedirs(state_path)
    
    district_path = os.path.join(state_path, district_folder)
    if not os.path.exists(district_path):
        os.makedirs(district_path)
    
    xlsx_file = os.path.join(district_path, f"{district}_{block}.xlsx")
    with pd.ExcelWriter(xlsx_file, engine='openpyxl') as writer:
        for workspace in workspace_list:
            print("workspace_name", workspace)
            layer_name=''
            if workspace=='terrain':
                layer_name = district + '_' + block + '_cluster' 
            elif workspace=='water_bodies':
                layer_name= 'surface_waterbodies_'  + district + '_' + block
            elif workspace=='nrega_assets':
                layer_name = district + '_' + block
            elif workspace=='cropping_intensity':
                layer_name = district + '_' + block + '_intensity'
            elif workspace=='cropping_drought':
                layer_name = district + '_' + block + '_drought'
            elif workspace=='mws_layers':
                layer_name = 'deltaG_well_depth_' + district + '_' + block
            elif workspace=='mws_layers_fort':
                layer_name = 'deltaG_fortnight_' + district + '_' + block
            elif workspace=='panchayat_boundaries':
                layer_name = district + '_' + block
            elif workspace=='change_detection':
                create_excel_chan_detection(xlsx_file, writer, district, block)
                continue
            elif workspace=='drought_causality':
                layer_name = district + '_' + block + '_drought_causality'
            elif workspace=='ccd':
                layer_name = district + '_' + block + "_tree_health_ccd_vector_2017_2022"
            elif workspace=='canopy_height':
                layer_name = district + '_' + block + "_tree_health_ch_vector_2017_2022"
            elif workspace=='tree_overall_ch':
                layer_name = 'tree_health_overall_change_vector_' + district + '_' + block



            if workspace=='mws_layers_fort': 
                url = get_url('mws_layers', layer_name)
            elif workspace=='terrain_lulc_slope':
                layer_name = district + '_' + block + '_lulc_slope'
                url = get_url('terrain_lulc', layer_name)
            elif workspace=='terrain_lulc_plain':
                layer_name = district + '_' + block + '_lulc_plain'
                url = get_url('terrain_lulc', layer_name)
            else:
                url = get_url(workspace, layer_name)
            
            # try:
            response = requests.get(url)
            response.raise_for_status()

            if response.content:
                geojson_data = response.json()
                if workspace=='terrain':
                    create_excel_for_terrain(geojson_data, xlsx_file, writer)
                elif workspace=='terrain_lulc_slope':
                    create_excel_for_terrain_lulc_slope(geojson_data, xlsx_file, writer)
                elif workspace=='terrain_lulc_plain':
                    create_excel_for_terrain_lulc_plain(geojson_data, xlsx_file, writer)
                elif workspace=='water_bodies':
                    create_excel_for_swb(geojson_data, xlsx_file, writer)
                elif workspace == 'nrega_assets':
                    mws_file_geojson = os.path.join(district_path, 'mws_annual.geojson')
                    mws_lay_name = 'deltaG_well_depth_' + district + '_' + block
                    mws_file_url = get_url('mws_layers', mws_lay_name)

                    response = requests.get(mws_file_url)
                    if response.status_code != 200:
                        print(f"Error fetching data: {response.status_code}")
                        return

                    mws_geojson_datas = response.json()

                    create_excel_for_nrega_assets(geojson_data, mws_geojson_datas, xlsx_file, writer)
                    fetch_village_asset_count(state, district, block, writer, xlsx_file)
                    create_excel_mws_inters_villages(mws_geojson_datas, xlsx_file, writer, district, block)
                    create_excel_village_inters_mwss(mws_geojson_datas, xlsx_file, writer, district, block)

                elif workspace=='cropping_intensity':
                    create_excel_crop_inten(geojson_data, xlsx_file, writer)
                elif workspace=='cropping_drought':
                    create_excel_crop_drou(geojson_data, xlsx_file, writer)
                elif workspace=='mws_layers':
                    parsed_data_annual_mws = parse_geojson_annnual_mws(geojson_data)
                    create_excel_annual_mws(parsed_data_annual_mws, xlsx_file, writer)
                elif workspace=='mws_layers_fort':
                    processed_data = [process_feature(feature) for feature in geojson_data['features']]
                    create_excel_seas_mws(processed_data, xlsx_file, writer)
                elif workspace=='panchayat_boundaries':
                    create_excel_for_village_boun(geojson_data, writer)
                elif workspace=='drought_causality':
                    create_excel_for_drought_causality(geojson_data, xlsx_file, writer)
                elif workspace=='ccd':
                    create_excel_for_ccd(geojson_data, xlsx_file, writer)
                elif workspace=='canopy_height':
                    create_excel_for_ch(geojson_data, xlsx_file, writer)
                elif workspace=='tree_overall_ch':
                    create_excel_for_overall_tree_change(geojson_data, xlsx_file, writer)

            else:
                print(f"Empty response for layer .")
                return {"Error": "Empty response"}
        return {"success": "url received"}



def create_excel_for_overall_tree_change(data, xlsx_file, writer):
    df_data = []
    features = data['features']
    
    for feature in features:  
        properties = feature['properties']      
        row = {
            'UID': properties['uid'],  
            'area_in_ha': properties['area_in_ha'],  
            'Afforestation': properties['Afforestat'],
            'Deforestation': properties['Deforestat'],
            'Degradation': properties['Degradatio'],
            'Improvement': properties['Improvemen'],
            'Missing_Data': properties['Missing Da'],
            'No_Change': properties['No_Change'],
            'Partially_Degraded': properties['Partially_'],
        }
        
        df_data.append(row)
    df = pd.DataFrame(df_data) 
    df = df.sort_values(['UID'])  
    df.to_excel(writer, sheet_name='overall_tree_change', index=False)
    print(f"Excel file created for overall_tree_change")



def create_excel_for_ccd(data, xlsx_file, writer):
    df_data = []
    features = data['features']
    for feature in features:  
        properties = feature['properties']
        
        row = {
            'UID': properties['uid'],
            'area_in_ha': properties['area_in_ha'],
        }

        for year in range(2017, 2023):
            row['High_Density_' + str(year)] = properties.get('hi_de_' + str(year), None)
            row['Low_Density_' + str(year)] = properties.get('lo_de_' + str(year), None)
            row['Missing_Data_' + str(year)] = properties.get('mi_da_' + str(year), None)
        
        df_data.append(row)

    df = pd.DataFrame(df_data)
    df = df.sort_values(['UID'])  
    df.to_excel(writer, sheet_name='Canopy_Cover_Density', index=False)
    print(f"Excel file created for Canopy_Cover_Density")


def create_excel_for_ch(data, xlsx_file, writer):
    df_data = []
    features = data['features']
    for feature in features:  
        properties = feature['properties']
        
        row = {
            'UID': properties['uid'],
            'area_in_ha': properties['area_in_ha'],
        }

        for year in range(2017, 2023):
            row['Short_Trees_' + str(year)] = properties.get('sh_tr_' + str(year), None)
            row['Medium_Trees_' + str(year)] = properties.get('md_tr_' + str(year), None)
            row['Tall_Trees_' + str(year)] = properties.get('tl_tr_' + str(year), None)
            row['Missing_Data_' + str(year)] = properties.get('mi_da_' + str(year), None)
        
        df_data.append(row)

    df = pd.DataFrame(df_data)
    df = df.sort_values(['UID'])  
    df.to_excel(writer, sheet_name='Canopy_height', index=False)
    print(f"Excel file created for Canopy_height")



def create_excel_for_drought_causality(data, xlsx_file, writer):
    df_data = []
    features = data['features']
    for feature in features:  
        properties = feature['properties']
        
        row = {
            'UID': properties['uid'],
        }

        for year in range(2017, 2023):
            row['severe_moderate_drought_causality_' + str(year)] = properties.get('se_mo_' + str(year), None)
            row['mild_drought_causality_' + str(year)] = properties.get('mild_' + str(year), None)
        
        df_data.append(row)

    df = pd.DataFrame(df_data) 
    df = df.sort_values(['UID'])  
    df.to_excel(writer, sheet_name='drought_causality', index=False)
    print(f"Excel file created for drought_causality")



def create_excel_chan_detection(xlsx_file, writer, district, block):
    change_detection_list = ["Afforestation", "CropIntensity", "Deforestation", "Degradation", "Urbanization"]
    afforestation_data = {}
    cropintensity_data = {}
    deforestation_data = {}
    degradation_data = {}
    urbanization_data = {}

    for per_cd in change_detection_list:
        layer_name = 'change_vector_' + district + '_' + block + "_" + per_cd
        layer_url = get_url('change_detection', layer_name)

        response = requests.get(layer_url)
        if response.status_code != 200:
            print(f"Error fetching data: {response.status_code}")
            return

        layer_geojson = response.json()
        features = layer_geojson['features']
        
        for feature in features:
            properties = feature['properties']
            uid = properties['uid']

            if per_cd == "Afforestation":
                afforestation_data[uid] = {
                    'UID': uid,
                    'AFFO_area_in_hac': properties.get('area_in_ha', None),
                    'AFFO_Barren-Forest': properties.get('ba_fo', None),
                    'AFFO_Built_Up-Forest': properties.get('bu_fo', None),
                    'AFFO_Farm-Forest': properties.get('fa_fo', None),
                    'AFFO_Forest-Forest': properties.get('fo_fo', None),
                    'AFFO_Scrub_Land-Forest': properties.get('sc_fo', None),
                    'AFFO_total': properties.get('total_aff', None),

                }
            elif per_cd == "CropIntensity":
                cropintensity_data[uid] = {
                    'UID': uid,
                    'CRP_INT_area_in_hac': properties.get('area_in_ha', None),
                    'CRP_INT_Double-Single': properties.get('do_si', None),
                    'CRP_INT_Double-Triple': properties.get('do_tr', None),
                    'CRP_INT_Single-Double': properties.get('si_do', None),
                    'CRP_INT_Single-Triple': properties.get('si_tr', None),
                    'CRP_INT_Triple-Double': properties.get('tr_do', None),
                    'CRP_INT_Triple-Single': properties.get('tr_si', None),
                    'CRP_INT_No_Change': properties.get('same', None),
                    'CRP_INT_Total_Change': properties.get('total_chan', None),
                }
            elif per_cd == "Deforestation":
                deforestation_data[uid] = {
                    'UID': uid,
                    'DEFO_area_in_hac': properties.get('area_in_ha', None),
                    'DEFO_Forest-Barren': properties.get('fo_ba', None),
                    'DEFO_Forest-Built_Up': properties.get('fo_bu', None),
                    'DEFO_Forest-Farm': properties.get('fo_fa', None),
                    'DEFO_Forest-Forest': properties.get('fo_fo', None),
                    'DEFO_Forest-Scrub_land': properties.get('fo_sc', None),
                    'DEFO_total': properties.get('total_def', None),
                }
            elif per_cd == "Degradation":
                degradation_data[uid] = {
                    'UID': uid,
                    'DEGR_area_in_hac': properties.get('area_in_ha', None),
                    'DEGR_Farm-Barren': properties.get('f_ba', None),
                    'DEGR_Farm-Built_Up': properties.get('f_bu', None),
                    'DEGR_Farm-Farm': properties.get('f_f', None),
                    'DEGR_Farm-Scrub_Land': properties.get('f_sc', None),
                    'DEGR_Total': properties.get('total_deg', None),
                }
            elif per_cd == "Urbanization":
                urbanization_data[uid] = {
                    'UID': uid,
                    'URBA_area_in_hac': properties.get('area_in_ha', None),
                    'URBA_Barren/Shrub-Built_Up': properties.get('b_bu', None),
                    'URBA_Built_Up-Built_Up': properties.get('bu_bu', None),
                    'URBA_Tree/Farm-Built_Up': properties.get('tr_bu', None),
                    'URBA_Water-Built_Up': properties.get('w_bu', None),
                    'URBA_Total': properties.get('total_urb', None),
                }

    merged_data = []
    all_uids = set(afforestation_data.keys()).union(set(cropintensity_data.keys()), set(deforestation_data.keys()), 
                                                      set(degradation_data.keys()), set(urbanization_data.keys()))

    for uid in all_uids:
        merged_row = {
            'UID': uid,
            **afforestation_data.get(uid, {}),  # Add afforestation data
            **cropintensity_data.get(uid, {}),  # Add crop intensity data
            **deforestation_data.get(uid, {}),  # Add deforestation data
            **degradation_data.get(uid, {}),  # Add degradation data
            **urbanization_data.get(uid, {}),  # Add urbanization data
        }
        merged_data.append(merged_row)
    df = pd.DataFrame(merged_data)
    df = df.sort_values(['UID'])  
    df.to_excel(writer, sheet_name='change_detection', index=False)
    print("Excel file created with unique UID and merged data.")


def create_excel_mws_inters_villages(mws_geojson, xlsx_file, writer, district, block):
    print("Inside create_excel_mws_inters_villages")
    admin_layer_name = district + '_' + block
    admin_file_url = get_url('panchayat_boundaries', admin_layer_name)

    response = requests.get(admin_file_url)
    if response.status_code != 200:
        print(f"Error fetching data: {response.status_code}")
        return

    village_geojson = response.json()

    def calculate_intersection_area(village_geom, mws_geom):
        if village_geom.intersects(mws_geom):
            intersection = village_geom.intersection(mws_geom)
            return intersection.area
        return 0

    mws_villages_dict = {}

    for mws_feature in mws_geojson['features']:
        mws_uid = mws_feature['properties']['uid']
        mws_geom = shape(mws_feature['geometry'])
        village_ids = set()
        
        for village_feature in village_geojson['features']:
            village_id = village_feature['properties']['vill_ID']
            if village_id == 0:
                continue
                
            village_geom = shape(village_feature['geometry'])
            area_intersected = calculate_intersection_area(village_geom, mws_geom)
            if area_intersected > 0:
                village_ids.add(village_id)
        if village_ids:
            mws_villages_dict[mws_uid] = list(village_ids)

    data = [{'MWS UID': mws_uid, 'Village IDs': village_ids} for mws_uid, village_ids in mws_villages_dict.items()]

    df = pd.DataFrame(data)
    df.to_excel(writer, sheet_name='mws_intersect_villages', index=False)
    print("The data has been saved to mws_intersect_villages.xlsx")




def create_excel_village_inters_mwss(mws_geojson, xlsx_file, writer, district, block):
    print("Inside create_excel_village_inters_mwss")
    admin_layer_name = district + '_' + block
    admin_file_url = get_url('panchayat_boundaries', admin_layer_name)

    response = requests.get(admin_file_url)
    if response.status_code != 200:
        print(f"Error fetching data: {response.status_code}")
        return
    village_geojson = response.json()

    def calculate_intersection_area(village_geom, mws_geom):
        if village_geom.intersects(mws_geom):
            intersection = village_geom.intersection(mws_geom)
            return intersection.area
        return 0

    data = []

    processed_villages = set()

    for village_feature in village_geojson['features']:
        village_id = village_feature['properties']['vill_ID']
        village_name = village_feature['properties']['vill_name']
        
        village_key = (village_id, village_name)
        
        if village_key in processed_villages:
            continue
        processed_villages.add(village_key)
        
        village_geom = shape(village_feature['geometry'])
        
        mws_uids = []
        intersection_areas = []
        
        for mws_feature in mws_geojson['features']:
            mws_geom = shape(mws_feature['geometry'])
            area_intersected = calculate_intersection_area(village_geom, mws_geom)
            if area_intersected > 0:
                mws_uids.append(mws_feature['properties']['uid'])
                intersection_areas.append(area_intersected)
        
        data.append({
            'Village ID': village_id,
            'Village Name': village_name,
            'MWS UIDs': mws_uids,
        })

    df = pd.DataFrame(data)
    df.to_excel(writer, sheet_name='village_intersect_mwss', index=False)
    print("The data has been saved to village_intersect_mwss.")


def create_excel_for_terrain(data, output_file, writer):
    print("Inside create_excel_for_terrain function")
    df_data = []
    
    terrain_description = {
            0: 'Broad Sloppy and Hilly',
            1: 'Mostly Plains',
            2: 'Mostly Hills and Valleys',
            3: 'Broad Plains and Slopes'
        }

    features = data['features']
    
    for feature in features:
        properties = feature['properties']        
        row = {
            'UID': properties['uid'],  
            'area_in_hac': properties['area_in_ha'],
            'terrainCluster_ID': properties['terrainClu'],
            'Terrain_Description': terrain_description.get(properties['terrainClu']),
            '% of area hill_slope': properties['hill_slope'],
            '% of area plain_area': properties['plain_area'],
            '% of area ridge_area': properties['ridge_area'],
            '% of area slopy_area': properties['slopy_area'],
            '% of area valley_area': properties['valley_are'],
        }
        
        df_data.append(row)

    df = pd.DataFrame(df_data) 
    df = df.sort_values(['UID'])  
    df.to_excel(writer, sheet_name='terrain', index=False)
    print(f"Excel file created: {output_file}")


def create_excel_for_terrain_lulc_slope(data, output_file, writer):
    df_data = []
    
    terrain_description = {
            0: 'Broad Sloppy and Hilly',
            1: 'Mostly Plains',
            2: 'Mostly Hills and Valleys',
            3: 'Broad Plains and Slopes'
        }

    features = data['features']
    
    for feature in features:
        properties = feature['properties']
        row = {
            'UID': properties['uid'],  
            'area_in_hac': properties['area_in_ha'],
            'terrainCluster_ID': properties['terrain_cl'],
            'Terrain_Description': terrain_description.get(properties['terrain_cl']),
            'cluster_name': properties['clust_name'],
            '% of area barren': properties['barren'],
            '% of area forests': properties['forests'],
            '% of area shrub_scrubs': properties['shrub_scru'],
            '% of area single_kharif': properties['sing_khari'],
            '% of area single_non_kharif': properties['sing_non_k'],
            '% of area double cropping': properties['double'],
            '% of area triple cropping': properties['triple'],
        }
        
        df_data.append(row)
    df = pd.DataFrame(df_data) 
    df = df.sort_values(['UID'])  
    df.to_excel(writer, sheet_name='terrain_lulc_slope', index=False)
    print(f"Excel file created: {output_file}")


def create_excel_for_terrain_lulc_plain(data, output_file, writer):
    df_data = []
    
    terrain_description = {
            0: 'Broad Sloppy and Hilly',
            1: 'Mostly Plains',
            2: 'Mostly Hills and Valleys',
            3: 'Broad Plains and Slopes'
        }

    features = data['features']
    
    for feature in features:
        properties = feature['properties']
        row = {
            'UID': properties['uid'],  
            'area_in_hac': properties['area_in_ha'],
            'terrainCluster_ID': properties['terrain_cl'],
            'Terrain_Description': terrain_description.get(properties['terrain_cl']),
            'cluster_name': properties['clust_name'],
            '% of area barren': properties['barren'],
            '% of area forests': properties['forest'],
            '% of area shrub_scrubs': properties['shrubs_scr'],
            '% of area single_non_kharif': properties['sing_non_k'],
            '% of area single_cropping': properties['sing_crop'],
            '% of area double cropping': properties['double_cro'],
            '% of area triple cropping': properties['triple_cro'],
        }
        
        df_data.append(row)

    df = pd.DataFrame(df_data) 
    df = df.sort_values(['UID'])  
    df.to_excel(writer, sheet_name='terrain_lulc_plain', index=False)
    print(f"Excel file created: {output_file}")



def create_excel_for_swb(data, output_file, writer):
    df_data = []

    features = data.get('features', [])
    for feature in features:
        properties = feature.get('properties', {})
        uid = properties.get('MWS_UID', 'Unknown')
        area_17_18 = properties.get('area_17-18', 0)

        def calculate_area(base_area, percentage):
            base_area = base_area * (percentage / 100)
            return base_area / 10000

        parts = uid.split('_')
        num_uid_parts_is = [f"{parts[i]}_{parts[i+1]}" for i in range(0, len(parts) - 1, 2)]
        if len(parts) % 2 == 1:  # Check for an unpaired last part
            num_uid_parts_is.append(parts[-1])

        for num_uid_part in num_uid_parts_is:
            row = {
                'UID': num_uid_part,

                # Calculate areas based on number of UID parts
                'total_area_2017-2018': properties.get('area_17-18', 0) / 10000 / len(num_uid_parts_is),
                'kharif_area_2017-2018': calculate_area(area_17_18, properties.get('k_17-18', 0)) / len(num_uid_parts_is),
                'rabi_area_2017-2018': calculate_area(area_17_18, properties.get('kr_17-18', 0)) / len(num_uid_parts_is),
                'zaid_area_2017-2018': calculate_area(area_17_18, properties.get('krz_17-18', 0)) / len(num_uid_parts_is),

                'total_area_2018-2019': properties.get('area_18-19', 0) / 10000 / len(num_uid_parts_is),
                'kharif_area_2018-2019': calculate_area(properties.get('area_18-19', 0), properties.get('k_18-19', 0)) / len(num_uid_parts_is),
                'rabi_area_2018-2019': calculate_area(properties.get('area_18-19', 0), properties.get('kr_18-19', 0)) / len(num_uid_parts_is),
                'zaid_area_2018-2019': calculate_area(properties.get('area_18-19', 0), properties.get('krz_18-19', 0)) / len(num_uid_parts_is),

                'total_area_2019-2020': properties.get('area_19-20', 0) / 10000 / len(num_uid_parts_is),
                'kharif_area_2019-2020': calculate_area(properties.get('area_19-20', 0), properties.get('k_19-20', 0)) / len(num_uid_parts_is),
                'rabi_area_2019-2020': calculate_area(properties.get('area_19-20', 0), properties.get('kr_19-20', 0)) / len(num_uid_parts_is),
                'zaid_area_2019-2020': calculate_area(properties.get('area_19-20', 0), properties.get('krz_19-20', 0)) / len(num_uid_parts_is),

                'total_area_2020-2021': properties.get('area_20-21', 0) / 10000 / len(num_uid_parts_is),
                'kharif_area_2020-2021': calculate_area(properties.get('area_20-21', 0), properties.get('k_20-21', 0)) / len(num_uid_parts_is),
                'rabi_area_2020-2021': calculate_area(properties.get('area_20-21', 0), properties.get('kr_20-21', 0)) / len(num_uid_parts_is),
                'zaid_area_2020-2021': calculate_area(properties.get('area_20-21', 0), properties.get('krz_20-21', 0)) / len(num_uid_parts_is),

                'total_area_2021-2022': properties.get('area_21-22', 0) / 10000 / len(num_uid_parts_is),
                'kharif_area_2021-2022': calculate_area(properties.get('area_21-22', 0), properties.get('k_21-22', 0)) / len(num_uid_parts_is),
                'rabi_area_2021-2022': calculate_area(properties.get('area_21-22', 0), properties.get('kr_21-22', 0)) / len(num_uid_parts_is),
                'zaid_area_2021-2022': calculate_area(properties.get('area_21-22', 0), properties.get('krz_21-22', 0)) / len(num_uid_parts_is),

                # 'total_area_2022-2023': properties.get('area_22-23', 0) / 10000 / len(num_uid_parts_is),
                # 'kharif_area_2022-2023': calculate_area(properties.get('area_22-23', 0), properties.get('k_22-23', 0)) / len(num_uid_parts_is),
                # 'rabi_area_2022-2023': calculate_area(properties.get('area_22-23', 0), properties.get('kr_22-23', 0)) / len(num_uid_parts_is),
                # 'zaid_area_2022-2023': calculate_area(properties.get('area_22-23', 0), properties.get('krz_22-23', 0)) / len(num_uid_parts_is),

                'total_swb_area': properties.get('area_ored', 0) / 10000 / len(num_uid_parts_is),
            }
            df_data.append(row)

    df = pd.DataFrame(df_data)

    grouped_df = df.groupby('UID').agg({
        'total_area_2017-2018': 'sum',
        'kharif_area_2017-2018': 'sum',
        'rabi_area_2017-2018': 'sum',
        'zaid_area_2017-2018': 'sum',

        'total_area_2018-2019': 'sum',
        'kharif_area_2018-2019': 'sum',
        'rabi_area_2018-2019': 'sum',
        'zaid_area_2018-2019': 'sum',

        'total_area_2019-2020': 'sum',
        'kharif_area_2019-2020': 'sum',
        'rabi_area_2019-2020': 'sum',
        'zaid_area_2019-2020': 'sum',

        'total_area_2020-2021': 'sum',
        'kharif_area_2020-2021': 'sum',
        'rabi_area_2020-2021': 'sum',
        'zaid_area_2020-2021': 'sum',

        'total_area_2021-2022': 'sum',
        'kharif_area_2021-2022': 'sum',
        'rabi_area_2021-2022': 'sum',
        'zaid_area_2021-2022': 'sum',

        # 'total_area_2022-2023': 'sum',
        # 'kharif_area_2022-2023': 'sum',
        # 'rabi_area_2022-2023': 'sum',
        # 'zaid_area_2022-2023': 'sum',

        'total_swb_area': 'sum'
    }).reset_index()

    grouped_df = grouped_df.sort_values(['UID'])
    grouped_df.to_excel(writer, sheet_name='surfaceWaterBodies_annual', index=False)


def create_excel_for_nrega_assets(nrega_data, mws_data, output_file, writer):
    workCategoryMapping = {
        "SWC - Landscape level impact": "Soil and water conservation",
        "Agri Impact - HH, Community": "Land restoration",
        "Plantation": "Plantations",
        "Irrigation - Site level impact": "Irrigation on farms",
        "Irrigation Site level - Non RWH": "Other farm works",
        "Household Livelihood": "Off-farm livelihood assets",
        "Others - HH, Community": "Community assets",
    }

    mws = gpd.GeoDataFrame.from_features(mws_data['features'])
    nrega = gpd.GeoDataFrame.from_features(nrega_data['features'])

    # Set CRS if available in JSON
    if 'crs' in mws_data:
        mws.set_crs(mws_data['crs']['properties']['name'], inplace=True)
    if 'crs' in nrega_data:
        nrega.set_crs(nrega_data['crs']['properties']['name'], inplace=True)

    joined = gpd.sjoin(nrega, mws, how="inner", predicate="within")
    counts = {}

    df_data = []
    valid_years = range(2017, 2023)

    date_formats = [
        "%d-%b-%y %H:%M:%S.%f",
        "%d-%b-%y %H:%M:%S",
        "%d-%m-%y %H:%M:%S.%f",
        "%d-%m-%y %H:%M:%S",
        "%d-%b-%Y %H:%M:%S.%f",
        "%d-%b-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M:%S.%f",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ]

    for _, row in joined.iterrows():
        creation_t = row['creation_t']
        work_category = row['WorkCatego']
        mws_id = row['uid']

        if isinstance(creation_t, pd.Timestamp):
            creation_t = creation_t.strftime('%d-%m-%Y %H:%M:%S')

        date_obj = None
        for date_format in date_formats:
            try:
                date_obj = datetime.strptime(creation_t, date_format)
                break
            except ValueError:
                continue

        if date_obj is None:
            continue

        year = date_obj.year
        if year < 100:
            year += 2000

        if year not in valid_years:
            continue

        category = workCategoryMapping.get(work_category, 'Others - HH, Community')

        if mws_id not in counts:
            counts[mws_id] = {year: {cat: 0 for cat in workCategoryMapping.values()} for year in range(2017, 2025)}

        if category not in counts[mws_id][year]:
            counts[mws_id][year][category] = 0 
        counts[mws_id][year][category] += 1

    for mws_id, year_data in counts.items():
        row = {'mws_id': mws_id}
        for year, categories in year_data.items():
            for category in workCategoryMapping.values():
                count = categories.get(category, 0)
                row[f"{category}_{year}"] = count
        df_data.append(row)

    if not df_data:
        print("No data was collected for the DataFrame.")
    else:
        print(f"Collected {len(df_data)} rows of data for the DataFrame.")

    if df_data:
        df = pd.DataFrame(df_data)
        df.to_excel(writer, sheet_name='nrega_annual', index=False)
    else:
        print("No data available to write to Excel.")




def create_excel_village_nrega_assets(result_df, output_file, writer):
    workCategoryMapping = {
        "SWC - Landscape level impact": "Soil and water conservation",
        "Agri Impact - HH,  Community": "Land restoration",
        "Plantation": "Plantations",
        "Irrigation - Site level impact": "Irrigation on farms",
        "Irrigation Site level - Non RWH": "Other farm works",
        "Household Livelihood": "Off-farm livelihood assets",
        "Others - HH, Community": "Community assets",
    }

    # Initialize dictionary for counts
    counts = {}

    # Process each row from the result_df
    for _, row in result_df.iterrows():
        date_obj = None
        creation_t = row['creation_t']
        
        # Parse the creation date
        date_formats = [
            "%d-%b-%y %H:%M:%S.%f",
            "%d-%b-%y %H:%M:%S",
            "%d-%m-%y %H:%M:%S.%f",
            "%d-%m-%y %H:%M:%S",
            "%d-%b-%Y %H:%M:%S.%f",
            "%d-%b-%Y %H:%M:%S",
            "%d-%m-%Y %H:%M:%S.%f",
            "%d-%m-%Y %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
        ]

        for date_format in date_formats:
            try:
                date_obj = datetime.strptime(creation_t, date_format)
                break
            except (ValueError, TypeError):
                continue

        if date_obj is None:
            continue

        year = date_obj.year
        if year not in range(2017, 2023):
            continue

        village_name = row['vill_name']
        village_id = row['vill_ID']
        category = workCategoryMapping.get(row['WorkCatego'])

        if category is None:
            continue

        # Initialize village data if it doesn't exist
        if village_name not in counts:
            counts[village_name] = {
                'vill_id': village_id,
                **{y: {cat: 0 for cat in workCategoryMapping.values()} for y in range(2017, 2025)}
            }

        # Increment count for the specific category and year
        counts[village_name][year][category] += 1

    # Prepare data for DataFrame
    df_data = []
    for village_name, year_data in counts.items():
        row = {
            'vill_id': year_data['vill_id'],
            'vill_name': village_name
        }
        
        # Add counts for each category and year
        for category in workCategoryMapping.values():
            for year in range(2017, 2023):
                column_name = f"{category}_{year}"
                row[column_name] = year_data[year][category]
        
        df_data.append(row)

    # Create final DataFrame and save to Excel
    df = pd.DataFrame(df_data)
    
    # Sort columns to ensure consistent order
    id_name_cols = ['vill_id', 'vill_name']
    other_cols = [col for col in df.columns if col not in id_name_cols]
    other_cols.sort()
    df = df[id_name_cols + other_cols]

    # Save to Excel
    df.to_excel(writer, sheet_name='nrega_assets_village', index=False)
    print(f"Excel file created successfully")

    # return df



def fetch_village_asset_count(state, district, block, writer, output_file):
    village_gdf = gpd.read_file(get_url('panchayat_boundaries', f'{district}_{block}'))
    nrega_json = requests.get(get_url('nrega_assets', f'{district}_{block}')).json()
    
    # Extract points and properties from NREGA assets
    points_data = []
    for feature in nrega_json['features']:
        point = Point(feature['geometry']['coordinates'])
        properties = feature['properties']
        points_data.append({
            'geometry': point,
            'Asset ID': properties['Asset ID'],
            'creation_t': properties['creation_t'],
            'WorkCatego': properties['WorkCatego']
        })
    
    points_gdf = gpd.GeoDataFrame(points_data, geometry='geometry')
    
    if village_gdf.crs != points_gdf.crs:
        points_gdf.set_crs(village_gdf.crs, inplace=True)
    
    joined_gdf = gpd.sjoin(
        points_gdf,
        village_gdf[['vill_ID', 'vill_name', 'geometry']],
        how='inner',
        predicate='within'
    )
    
    village_asset_count = joined_gdf.groupby('vill_ID').size().reset_index(name='asset_count')
    
    result_df = joined_gdf[[
        'vill_ID', 
        'vill_name', 
        'Asset ID', 
        'creation_t', 
        'WorkCatego'
    ]].copy()

    create_excel_village_nrega_assets(result_df, output_file, writer)
    return village_asset_count

def analyze_results(village_asset_count, village_gdf):
    villages_with_counts = village_gdf.merge(
        village_asset_count,
        on='vill_ID',
        how='left'
    )
    
    villages_with_counts['asset_count'] = villages_with_counts['asset_count'].fillna(0)
    return villages_with_counts


def create_excel_crop_inten(data, output_file, writer):
    df_data = []

    features = data['features']
    for feature in features:
        properties = feature['properties']
        uid = properties.get('uid', 'Unknown')
        
        row = {
            'UID': uid,
            'area_in_ha': properties['area_in_ha'],
            'cropping_intensity_2017-2018': properties['cropping_1'],
            'single_cropped_area_2017-2018': properties['single_c_1']/10000,
            'single_kharif_cropped_area_2017-2018': properties['single_k_1']/10000,
            'single_non_kharif_cropped_area_2017-2018': properties['single_n_1']/10000,
            'doubly_cropped_area_2017-2018': properties['doubly_c_1']/10000,
            'triply_cropped_area_2017-2018': properties['triply_c_1']/10000,
            'cropping_intensity_2018-2019': properties['cropping_2'],
            'single_cropped_area_2018-2019': properties['single_c_2']/10000,
            'single_kharif_cropped_area_2018-2019': properties['single_k_2']/10000,
            'single_non_kharif_cropped_area_2018-2019': properties['single_n_2']/10000,
            'doubly_cropped_area_2018-2019': properties['doubly_c_2']/10000,
            'triply_cropped_area_2018-2019': properties['triply_c_2']/10000,
            'cropping_intensity_2019-2020': properties['cropping_3'],
            'single_cropped_area_2019-2020': properties['single_c_3']/10000,
            'single_kharif_cropped_area_2019-2020': properties['single_k_3']/10000,
            'single_non_kharif_cropped_area_2019-2020': properties['single_n_3']/10000,
            'doubly_cropped_area_2019-2020': properties['doubly_c_3']/10000,
            'triply_cropped_area_2019-2020': properties['triply_c_3']/10000,
            'cropping_intensity_2020-2021': properties['cropping_4'],
            'single_cropped_area_2020-2021': properties['single_c_4']/10000,
            'single_kharif_cropped_area_2020-2021': properties['single_k_4']/10000,
            'single_non_kharif_cropped_area_2020-2021': properties['single_n_4']/10000,
            'doubly_cropped_area_2020-2021': properties['doubly_c_4']/10000,
            'triply_cropped_area_2020-2021': properties['triply_c_4']/10000,
            'cropping_intensity_2021-2022': properties['cropping_5'],
            'single_cropped_area_2021-2022': properties['single_c_5']/10000,
            'single_kharif_cropped_area_2021-2022': properties['single_k_5']/10000,
            'single_non_kharif_cropped_area_2021-2022': properties['single_n_5']/10000,
            'doubly_cropped_area_2021-2022': properties['doubly_c_5']/10000,
            'triply_cropped_area_2021-2022': properties['triply_c_5']/10000,
            # 'cropping_intensity_2022': properties['cropping_6'],
            # 'single_cropped_area_2022': properties['single_c_6']/10000,
            # 'single_kharif_cropped_area_2022': properties['single_k_6']/10000,
            # 'single_non_kharif_cropped_area_2022': properties['single_n_6']/10000,
            # 'doubly_cropped_area_2022': properties['doubly_c_6']/10000,
            # 'triply_cropped_area_2022': properties['triply_c_6']/10000,
            'sum': properties['sum']/10000
        }
        df_data.append(row)

    df = pd.DataFrame(df_data)
    df = df.sort_values(['UID'])
    df.to_excel(writer, sheet_name='croppingIntensity_annual', index=False)
    print(f"Excel file created: {output_file}")


def create_excel_crop_drou(data, output_file, writer):
    df_data = []
    
    features = data['features']
    for feature in features:
        feature_id = feature.get('id', 'Unknown ID')
        properties = feature['properties']
        row = {
                'UID': properties.get('uid', 'Unknown ID'),
                'No_Drought_2017': properties.get('drlb_2017', None).count('0'),
                'Mild_2017': properties.get('drlb_2017', None).count('1'),
                'Moderate_2017': properties.get('drlb_2017', None).count('2'),
                'Severe_2017': properties.get('drlb_2017', None).count('3'),
                'drysp_2017': properties.get('drysp_2017', '0'),
                'kharif_cropped_sqkm_2017': properties.get('kh_cr_2017', '0'),
                'monsoon_onset_2017': properties.get('m_ons_2017', '0'),
                '%_area_cropped_kharif_2017': properties.get('pcr_k_2017', '0'),
                'total_weeks_2017': properties.get('t_wks_2017', '0'),

                'No_Drought_2018': properties.get('drlb_2018', None).count('0'),
                'Mild_2018': properties.get('drlb_2018', None).count('1'),
                'Moderate_2018': properties.get('drlb_2018', None).count('2'),
                'Severe_2018': properties.get('drlb_2018', None).count('3'),
                'drysp_2018': properties.get('drysp_2018', '0'),
                'kharif_cropped_sqkm_2018': properties.get('kh_cr_2018', '0'),
                'monsoon_onset_2018': properties.get('m_ons_2018', '0'),
                '%_area_cropped_kharif_2018': properties.get('pcr_k_2018', '0'),
                'total_weeks_2018': properties.get('t_wks_2018', '0'),

                'No_Drought_2019': properties.get('drlb_2019', None).count('0'),
                'Mild_2019': properties.get('drlb_2019', None).count('1'),
                'Moderate_2019': properties.get('drlb_2019', None).count('2'),
                'Severe_2019': properties.get('drlb_2019', None).count('3'),
                'drysp_2019': properties.get('drysp_2019', '0'),
                'kharif_cropped_sqkm_2019': properties.get('kh_cr_2019', '0'),
                'monsoon_onset_2019': properties.get('m_ons_2019', '0'),
                '%_area_cropped_kharif_2019': properties.get('pcr_k_2019', '0'),
                'total_weeks_2019': properties.get('t_wks_2019', '0'),

                'No_Drought_2020': properties.get('drlb_2020', None).count('0'),
                'Mild_2020': properties.get('drlb_2020', None).count('1'),
                'Moderate_2020': properties.get('drlb_2020', None).count('2'),
                'Severe_2020': properties.get('drlb_2020', None).count('3'),
                'drysp_2020': properties.get('drysp_2020', '0'),
                'kharif_cropped_sqkm_2020': properties.get('kh_cr_2020', '0'),
                'monsoon_onset_2020': properties.get('m_ons_2020', '0'),
                '%_area_cropped_kharif_2020': properties.get('pcr_k_2020', '0'),
                'total_weeks_2020': properties.get('t_wks_2020', '0'),

                'No_Drought_2021': properties.get('drlb_2021', None).count('0'),
                'Mild_2021': properties.get('drlb_2021', None).count('1'),
                'Moderate_2021': properties.get('drlb_2021', None).count('2'),
                'Severe_2021': properties.get('drlb_2021', None).count('3'),
                'drysp_2021': properties.get('drysp_2021', '0'),
                'kharif_cropped_sqkm_2021': properties.get('kh_cr_2021', '0'),
                'monsoon_onset_2021': properties.get('m_ons_2021', '0'),
                '%_area_cropped_kharif_2021': properties.get('pcr_k_2021', '0'),
                'total_weeks_2021': properties.get('t_wks_2021', '0'),

                'No_Drought_2022': properties.get('drlb_2022', None).count('0'),
                'Mild_2022': properties.get('drlb_2022', None).count('1'),
                'Moderate_2022': properties.get('drlb_2022', None).count('2'),
                'Severe_2022': properties.get('drlb_2022', None).count('3'),
                'drysp_2022': properties.get('drysp_2022', '0'),
                'kharif_cropped_sqkm_2022': properties.get('kh_cr_2022', '0'),
                'monsoon_onset_2022': properties.get('m_ons_2022', '0'),
                '%_area_cropped_kharif_2022': properties.get('pcr_k_2022', '0'),
                'total_weeks_2022': properties.get('t_wks_2022', '0')

                # 'No_Drought_2023': properties.get('drlb_2023', None).count('0') if properties.get('drlb_2023', None) else 0,
                # 'Mild_2023': properties.get('drlb_2023', None).count('1') if properties.get('drlb_2023', None) else 0,
                # 'Moderate_2023': properties.get('drlb_2023', None).count('2') if properties.get('drlb_2023', None) else 0,
                # 'Severe_2023': properties.get('drlb_2023', None).count('3') if properties.get('drlb_2023', None) else 0,
                # 'drysp_2023': properties.get('drysp_2023', '0')
                # 'kharif_cropped_sqkm_2023': properties.get('kh_cr_2023', '0'),
                # 'monsoon_onset_2023': properties.get('m_ons_2023', '0'),
                # '%_area_cropped_kharif_2023': properties.get('pcr_k_2023', '0'),
                # 'total_weeks_2023': properties.get('t_wks_2023', '0'),
            }
        df_data.append(row)
    df = pd.DataFrame(df_data)
    df = df.sort_values(['UID'])
    df.to_excel(writer, sheet_name='croppingDrought_kharif', index=False)
    print("Excel file created for cropping Drought")



def parse_geojson_annnual_mws(data):
    features = data['features']

    all_data = defaultdict(lambda: defaultdict(dict))

    for feature in features:
        properties = feature['properties']
        uid = properties.get('uid', 'Unknown')

        for key, value in properties.items():
            if isinstance(key, str) and isinstance(value, str):
                if key.startswith('20') and len(key) == 9:
                    year = key
                    try:
                        # Attempt to parse the value as JSON
                        year_data = json.loads(value.replace("'", '"'))
                        all_data[uid][year] = year_data
                    except Exception as e:
                        print(f"Couldn't parse data for {uid}, {key}: {e}")
            else:
                print(f"Skipping non-string key or value for {uid}: {key} -> {value}")

    return all_data


def create_excel_annual_mws(data, output_file, writer):
    df_data = []
    year_columns = ['ET', 'RunOff', 'G', 'DeltaG', 'Precipitation', 'WellDepth']
    
    for uid, years in data.items():
        row = {'UID': uid}
        
        for year, metrics in years.items():
            start_year = year[:4]
            end_year = str(int(start_year) + 1)
            formatted_year = f"{start_year}-{end_year}"
            
            for col in year_columns:
                column_name = f"{col}_{formatted_year}"
                row[column_name] = metrics.get(col, 'N/A')

        df_data.append(row)

    df = pd.DataFrame(df_data)
    df = df.sort_values(['UID'])
    df.to_excel(writer, sheet_name='hydrological_annual', index=False)
    print(f"Excel file created: {output_file}")



def parse_json_seas_mws(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def get_season(month):
    if month in (3, 4, 5, 6):
        return 'zaid'
    elif month in (7, 8, 9, 10):
        return 'kharif'
    elif month in (11, 12, 1, 2):
        return 'rabi'

def process_feature(feature):
    uid = feature['properties']['uid']
    results = {
        'UID': uid,
        'precipitation': {'kharif': {}, 'rabi': {}, 'zaid': {}},
        'et': {'kharif': {}, 'rabi': {}, 'zaid': {}},
        'runoff': {'kharif': {}, 'rabi': {}, 'zaid': {}},
        'delta g': {'kharif': {}, 'rabi': {}, 'zaid': {}},
        'g': {'kharif': {}, 'rabi': {}, 'zaid': {}}
    }

    variable_mapping = {
        'Precipitation': 'precipitation',
        'ET': 'et',
        'RunOff': 'runoff',
        'DeltaG': 'delta g',
        'G': 'g'
    }

    for key, value in feature['properties'].items():
        if key.startswith('20'):
            try:
                date = datetime.strptime(key, '%Y-%m-%d')
                year = date.year
                month = date.month
                season = get_season(month)
                if season == 'rabi':
                    current_year = year -1  if month in (1, 2) else year
                elif season == 'zaid':
                    current_year = year - 1 if month in (3,4,5,6) else year
                else:
                    current_year = year
                data = json.loads(value)

                for json_var, result_var in variable_mapping.items():
                    if json_var in data:
                        if current_year not in results[result_var][season]:
                            results[result_var][season][current_year] = 0.0
                        results[result_var][season][current_year] += float(data[json_var])

            except (ValueError, json.JSONDecodeError) as e:
                print(f"Error processing data for date {key}: {e}")
                continue
    return results

def create_excel_seas_mws(processed_data, output_file, writer):
    variables = ['precipitation', 'et', 'runoff', 'delta g', 'g']
    seasons = ['kharif', 'rabi', 'zaid']
    years = range(2017, 2023)

    data = {'UID': []}
    for variable in variables:
        for year in years:
            for season in seasons:
                end_year = year + 1
                column_name = f'{variable}_{season}_{year}-{end_year}'
                data[column_name] = []

    for feature_data in processed_data:
        data['UID'].append(feature_data['UID'])
        for variable in variables:
            for year in years:
                for season in seasons:
                    end_year = year + 1
                    column_name = f'{variable}_{season}_{year}-{end_year}'
                    value = feature_data[variable].get(season, {}).get(year, 0.0)
                    data[column_name].append(value)

    df = pd.DataFrame(data)
    df = df.sort_values('UID')
    df.to_excel(writer, sheet_name='hydrological_seasonal', index=False)
    print(f"Excel file created: {output_file}")


def create_excel_for_village_boun(old_geojson, writer):
    results = []

    village_data = {}

    for feature in old_geojson['features']:
        properties = feature['properties']
        
        # Extract properties
        state_census_ID = properties.get('state_cen', None)
        dist_census_ID = properties.get('dist_cen', None)
        block_census_ID = properties.get('block_cen', None)
        village_id = properties.get('vill_ID', None)
        village_name = properties.get('vill_name', None)

        # Initialize village data using village_id as the key
        if village_id not in village_data:
            village_data[village_id] = {
                'village_name': village_name,
                'TOT_P': 0,
                'P_LIT': 0,
                'P_SC': 0,
                'P_ST': 0,
                'state_census_ID': state_census_ID,
                'dist_census_ID': dist_census_ID,
                'block_census_ID': block_census_ID,
                'geometry': feature['geometry']
            }

        village_data[village_id]['TOT_P'] += properties.get('TOT_P', 0)
        village_data[village_id]['P_LIT'] += properties.get('P_LIT', 0)
        village_data[village_id]['P_SC'] += properties.get('P_SC', 0)
        village_data[village_id]['P_ST'] += properties.get('P_ST', 0)

    for village_id, data in village_data.items():
        total_popu = data['TOT_P']
        literacy_rate = data['P_LIT'] * 100 / total_popu if total_popu > 0 else 0.0
        total_SC_popu = data['P_SC']
        total_ST_popu = data['P_ST']
        sc_perce = (data['P_SC'] * 100 / total_popu) if total_popu > 0 else 0.0
        st_perce = (data['P_ST'] * 100 / total_popu) if total_popu > 0 else 0.0

        results.append({
            'state_census_ID': data['state_census_ID'],
            'dist_census_ID': data['dist_census_ID'],
            'block_census_ID': data['block_census_ID'],
            'village_id': village_id,
            'village_name': data['village_name'],
            'total_population': total_popu,
            'total_SC_population': total_SC_popu,
            'total_ST_population': total_ST_popu,
            'literacy_rate': literacy_rate,
            'SC_percentage': sc_perce,
            'ST_percentage': st_perce,
        })

    results_df = pd.DataFrame(results)
    results_df.to_excel(writer, sheet_name='social_economic_indicator', index=False)

    print(f"Excel file created with {len(results_df)} villages.")



def download_layers_excel_file(state, district, block):
    state_folder = state.replace(" ", "_").upper()
    district_folder = district.replace(" ", "_").upper()
    base_path = path_to_add + 'data/stats_excel_files/'

    state_path = os.path.join(base_path, state_folder)
    if not os.path.exists(state_path):
        os.makedirs(state_path)

    district_path = os.path.join(state_path, district_folder)
    if not os.path.exists(district_path):
        os.makedirs(district_path)

    filename = f"{district}_{block}.xlsx"
    file_path = os.path.join(district_path, filename)
    if os.path.exists(file_path):
        return file_path
    else:
        return None


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



def get_generate_filter_mws_data(state, district, block):
    state_folder = state.replace(" ", "_").upper()
    district_folder = district.replace(" ", "_").upper() 
    file_xl_path = path_to_add + 'data/stats_excel_files/' + state_folder + '/' + district_folder + '/' + district + '_' + block
    xlsx_file = file_xl_path + '.xlsx'
    df_hydrological_annual = pd.read_excel(xlsx_file, sheet_name='hydrological_annual')
    df_terrain_vector = pd.read_excel(xlsx_file, sheet_name='terrain')
    df_crp_intensity = pd.read_excel(xlsx_file, sheet_name='croppingIntensity_annual')
    df_swb_annual = pd.read_excel(xlsx_file, sheet_name='surfaceWaterBodies_annual')
    df_croppingDrought_kharif = pd.read_excel(xlsx_file, sheet_name='croppingDrought_kharif')
    df_nrega_annual = pd.read_excel(xlsx_file, sheet_name='nrega_annual')
    df_mws_inters_villages = pd.read_excel(xlsx_file, sheet_name='mws_intersect_villages')
    df_mws_change_detection = pd.read_excel(xlsx_file, sheet_name='change_detection')
    try:
        df_mws_terrain_lulc_slope = pd.read_excel(xlsx_file, sheet_name='terrain_lulc_slope')
    except Exception as e:
        print(e)
    df_mws_terrain_lulc_plain = pd.read_excel(xlsx_file, sheet_name='terrain_lulc_plain')


    results = []
    trend_info = {
            0: "no trend",
            1: "increasing",
            -1: "decreasing"
        }


    for specific_mws_id in df_hydrological_annual['UID'].unique():
        hydro_annual_mws_data = df_hydrological_annual[df_hydrological_annual['UID'] == specific_mws_id]
        precipitation_columns = hydro_annual_mws_data.filter(like='Precipitation')          # Avg_precipitation
        total_percipitation_column = precipitation_columns.shape[1]
        sum_precipitation = precipitation_columns.sum(axis=1).sum()
        avg_percipitation = round(sum_precipitation / total_percipitation_column, 4) 


        try:
            terrain_vector_mws_data = df_terrain_vector[df_terrain_vector['UID'] == specific_mws_id]
            terrainCluster_ID = terrain_vector_mws_data.get('terrainCluster_ID', None).iloc[0]        # terrain
        except:
            terrainCluster_ID = ''

        try:
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
            df_nrega_assets_mws_data = df_nrega_annual[df_nrega_annual['mws_id'] == specific_mws_id]
            nrega_assets_sum = int(df_nrega_assets_mws_data.sum(axis=1).sum())
        except:
            nrega_assets_sum = 0


        ############ MWS Intersect Villages  ######################## 
        import ast
        try:
            df_mws_inters_villages_mws_data = df_mws_inters_villages[df_mws_inters_villages['MWS UID'] == specific_mws_id]
            mws_intersect_villages = df_mws_inters_villages_mws_data.get('Village IDs', None).iloc[0]
            mws_intersect_villages = ast.literal_eval(mws_intersect_villages)
        except:
            mws_intersect_villages = ''


        ############  Change Detection  ###################
        try:
            df_change_detection_mws_data = df_mws_change_detection[df_mws_change_detection['UID'] == specific_mws_id]
            degradation_column = ['DEGR_Farm-Barren', 'DEGR_Farm-Built_Up', 'DEGR_Farm-Scrub_Land']
            afforestation_column = ['AFFO_Barren-Forest', 'AFFO_Farm-Forest']
            deforestation_column = ['DEFO_Forest-Scrub_land', 'DEFO_Forest-Barren', 'DEFO_Forest-Built_Up', 'DEFO_Forest-Farm']
            urbanization_column = ['URBA_Barren/Shrub-Built_Up', 'URBA_Built_Up-Built_Up', 'URBA_Tree/Farm-Built_Up', 'URBA_Water-Built_Up']

            degradation_land_area = df_change_detection_mws_data.get('DEGR_Total', None).iloc[0]
            afforestation_land_area = df_change_detection_mws_data.get('AFFO_total', None).iloc[0]
            deforestation_land_area = df_change_detection_mws_data.get('DEFO_total', None).iloc[0]
            urbanization_land_area = df_change_detection_mws_data.get('URBA_Total', None).iloc[0]
        except:
            degradation_land_area = afforestation_land_area = deforestation_land_area = urbanization_land_area = 0


        ############# Terrain lulc slope / plain  #####################
        try:
            df_lulc_slope_mws_data = df_mws_terrain_lulc_slope[df_mws_terrain_lulc_slope['UID'] == specific_mws_id]
            lulc_slope_category = df_lulc_slope_mws_data.get('cluster_name', pd.NA).iloc[0] if not df_lulc_slope_mws_data.empty else None
        except:
            lulc_slope_category=''

        try:
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
    results_df.to_excel(file_xl_path + '_KYL_filter_data.xlsx', index=False)
    results_list = results_df.to_dict(orient='records')
    with open(file_xl_path + '_KYL_filter_data.json', 'w') as json_file:
        json.dump(results_list, json_file, indent=4)
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
    if file_type=='excel':
        file_path = os.path.join(file_xl_path + '_KYL_filter_data.xlsx')
    elif file_type=='json':
        file_path = os.path.join(file_xl_path + '_KYL_filter_data.json')
    elif file_type=='geojson':
        file_path = os.path.join(file_xl_path + '_KYL_filter_data.geojson')

    if os.path.exists(file_path):
        return file_path
    else:
        return None



def get_generate_filter_data_village(state, district, block): 
    state_folder = state.replace(" ", "_").upper()
    district_folder = district.replace(" ", "_").upper()  
    file_xl_path = path_to_add + 'data/stats_excel_files/' + state_folder + '/' + district_folder + '/' + district + '_' + block 
    xlsx_file = file_xl_path + '.xlsx'
    
    df_soc_eco_indi = pd.read_excel(xlsx_file, sheet_name='social_economic_indicator')
    df_nrega_village = pd.read_excel(xlsx_file, sheet_name='nrega_assets_village')

    results = []

    for specific_village_id in df_soc_eco_indi['village_id'].unique():
        village_id_data = df_soc_eco_indi[df_soc_eco_indi['village_id'] == specific_village_id]
        village_nrega_data = df_nrega_village[df_nrega_village['vill_id'] == specific_village_id]

        total_population = village_id_data.get('total_population', None).iloc[0]
        SC_percentage = round(village_id_data.get('SC_percentage', None).iloc[0], 4)
        ST_percentage = round(village_id_data.get('ST_percentage', None).iloc[0], 4)
        literacy_rate = round(village_id_data.get('literacy_rate', None).iloc[0], 4)
        total_assets = int(village_nrega_data.drop(columns=['vill_id', 'vill_name']).sum(axis=1).sum())

        if specific_village_id!=0:
            results.append({
                'village_id': specific_village_id,
                'total_population': total_population,
                'percent_st_population': ST_percentage,
                'percent_sc_population': SC_percentage,
                'literacy_level': literacy_rate,
                'total_assets': total_assets
            })

    results_df = pd.DataFrame(results)
    results_df.to_excel(file_xl_path + '_KYL_village_data.xlsx', index=False)
    results_list = results_df.to_dict(orient='records')

    with open(file_xl_path + '_KYL_village_data.json', 'w') as json_file:
        json.dump(results_list, json_file, indent=4)

    layer_name = district + '_' + block
    panchayat_bound_geojson = get_url('panchayat_boundaries', layer_name)

    response = requests.get(panchayat_bound_geojson)
    response.raise_for_status()

    if response.content:
        geojson_data = response.json()
        for feature in geojson_data['features']:
            vill_id = feature['properties']['vill_ID']
            village_data = next((item for item in results_list if item['village_id'] == vill_id), None)
            
            if village_data:
                feature['properties'].update({
                    'total_population': village_data['total_population'],
                    'percent_st_population': village_data['percent_st_population'],
                    'percent_sc_population': village_data['percent_sc_population'],
                    'literacy_level': village_data['literacy_level']
                })

        deltaG_geojson = file_xl_path + '_panchayat_boundaries_nw.geojson'
        with open(deltaG_geojson, 'w') as f:
            json.dump(geojson_data, f)

        file_path = file_xl_path + '_KYL_village_data.json'
        if os.path.exists(file_path):
            return file_path
        else:
            return None
