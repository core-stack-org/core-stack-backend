import requests
import json
from collections import defaultdict
from core_stack_orm import *
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import ast
import pandas as pd

def parse_layers(layer_list):
    """
    Convert JSON output of a list of layers into a clean structure,
    grouped by layer_name. Handles duplicates by appending.
    """
    parsed = defaultdict(list)

    for layer in layer_list:
        name = layer.get("layer_name", "Unknown")
        entry = {
            "type": layer.get("layer_type"),
            "url": layer.get("layer_url"),
            "version": layer.get("layer_version"),
            "style": layer.get("style_url"),
            "gee_asset": layer.get("gee_asset_path"),
        }
        parsed[name].append(entry)

    return dict(parsed)

def download_layer(url, output_file):
    # Send an HTTP GET request to the URL
    # verify=False disables SSL certificate verification (only use this if absolutely necessary)
    response = requests.get(url, verify=False)

    # Check if the request was successful (HTTP status code 200 means OK)
    if response.status_code == 200:
        # Open a local file in binary write mode and write the downloaded content to it
        with open(output_file, "wb") as f:
            f.write(response.content)

        # Confirm that the file was saved successfully
        print(f"File saved as {output_file}")
    else:
        # Print an error message if the request failed
        print(f"Failed to download: HTTP {response.status_code}")

def invoke_api(api_endpoint, api_params):
    # Base URL and (add your) API key
    base_url = 'https://geoserver.core-stack.org/api/v1/'
    Api_key = ''

    # Define the HTTP request headers with the API key
    headers = {
        "X-API-Key": Api_key
    }

    # Construct the full API URL by combining base URL and endpoint
    api = f"{base_url}{api_endpoint}"

    # Make the GET request with headers and parameters
    response = requests.get(api, params=api_params, headers=headers)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # You can now work with 'response_data_mws_lat_long'
        print("API call successful. Data received.")
    else:
        # Handle errors - print status code and response text for debugging
        print(f"API call failed with status code: {response.status_code}")

#    print("Response content:", response.text)
    return response.json()

def get_active_tehsils():
    # API endpoint to get list of tehsils for which data is available
    active_locations_api_endpoint = 'get_active_locations/'

    active_tehsils_response = invoke_api(active_locations_api_endpoint, {})

    results = []
    data = active_tehsils_response

    for state_obj in data:
        state = (state_obj.get("label"))

        for dist_obj in state_obj.get("district", []):
            district = (dist_obj.get("label"))

            for tehsil_obj in dist_obj.get("blocks", []):
                tehsil = (tehsil_obj.get("label"))
                results.append({'STATE': state.lower(), 'District': district.lower(), 'TEHSIL': tehsil.lower()})

    return results

def get_tehsil_data_from_layers(state, district, tehsil):
    # API endpoint to get various layers and extract data from the layers
    tehsil_layers_api_endpoint = 'get_generated_layer_urls/'

    # Define the query parameters to be sent in the API request
    params = {
        "state": state,
        "district": district,
        "tehsil": tehsil
    }

    tehsil_layers_response = invoke_api(tehsil_layers_api_endpoint, params)

    # Build a neat list of layers
    layers_dict = parse_layers(tehsil_layers_response)
    with open(f"data/layers_{state}_{district}_{tehsil}.json", "wb") as f:
        f.write(json.dumps(layers_dict).encode('utf-8'))

    # Set up a tehsil_data object to hold the data
    tehsil_obj = tehsil_data(state, district, tehsil)

    # Parse the layers and populate properties
    hydrology_url = next(
        (layer["url"] for layer in layers_dict.get("Hydrology", []) if "deltaG_well_depth" in layer.get("url", "")),
        None
    )
    hydrology_output = f"data/hydrology_{state}_{district}_{tehsil}.geojson"
    download_layer(hydrology_url, hydrology_output)
    with open(hydrology_output) as f:
        hydrology = json.load(f)
    loading_util.load_well_depth(tehsil_obj, hydrology)

    # Parse the layers and populate properties
    terrain_url = layers_dict["Terrain Vector"][0]["url"]
    terrain_output = f"data/terrain_{state}_{district}_{tehsil}.geojson"
    download_layer(terrain_url, terrain_output)
    with open(terrain_output) as f:
        terrain = json.load(f)
    loading_util.load_terrain(tehsil_obj, terrain)

    # Parse the layers and populate properties
    cropping_url = layers_dict["Cropping Intensity"][0]["url"]
    cropping_output = f"data/cropping_{state}_{district}_{tehsil}.geojson"
    download_layer(cropping_url, cropping_output)
    with open(cropping_output) as f:
        cropping = json.load(f)
    loading_util.load_cropping_intensity(tehsil_obj, cropping)

    # Parse the layers and populate properties
    drought_url = layers_dict["Drought"][0]["url"]
    drought_output = f"data/drought_{state}_{district}_{tehsil}.geojson"
    download_layer(drought_url, drought_output)
    with open(drought_output) as f:
        drought = json.load(f)
    loading_util.load_drought_frequency(tehsil_obj, drought)

    # Parse the layers and populate properties
    swb_url = layers_dict["Surface Water Bodies"][0]["url"]
    swb_output = f"data/swb_{state}_{district}_{tehsil}.geojson"
    download_layer(swb_url, swb_output)
    with open(swb_output) as f:
        swb = json.load(f)
    loading_util.load_waterbodies(tehsil_obj, swb)

    return tehsil_obj

def get_tehsil_data_from_api(state, district, tehsil, mws_params):
    # API endpoint to get tehsil data and populate into objects
    tehsil_layers_api_endpoint = 'get_tehsil_data/'

    # Define the query parameters to be sent in the API request
    params = {
        "state": state,
        "district": district,
        "tehsil": tehsil
    }

    tehsil_layers_response = invoke_api(tehsil_layers_api_endpoint, params)

    # Set up a tehsil_data object to hold the data
    tehsil_obj = tehsil_data(state, district, tehsil)

    loading_util.load_from_api_payload(tehsil_obj, tehsil_layers_response, mws_params)

    return tehsil_obj

def build_df(tehsil_obj, mws_list):
    # --- Build DataFrame summarizing each MWS ---
    mws_rows = []
    for mws in tehsil_obj.microwatersheds.values():
        if mws_list and mws in mws_list:
            continue
        row = {}
        row['uid'] = mws.uid
        row['area_in_ha'] = float(mws.area_in_ha) if mws.area_in_ha is not None else np.nan
        row['plain_area'] = float(mws.plain_area) if mws.plain_area is not None else np.nan
        row['slopy_area'] = float(mws.slopy_area) if mws.slopy_area is not None else np.nan
        row['hill_slope'] = float(mws.hill_slope) if mws.hill_slope is not None else np.nan
        row['valley_are'] = float(mws.valley_are) if mws.valley_are is not None else np.nan
        row['ridge_area'] = float(mws.ridge_area) if mws.ridge_area is not None else np.nan

        row['forest_to_barren'] = float(mws.forest_to_barren) if mws.forest_to_barren is not None else np.nan
        row['forest_to_builtu'] = float(mws.forest_to_builtu) if mws.forest_to_builtu is not None else np.nan
        row['forest_to_farm'] = float(mws.forest_to_farm) if mws.forest_to_farm is not None else np.nan
        row['forest_to_forest'] = float(mws.forest_to_forest) if mws.forest_to_forest is not None else np.nan
        row['forest_to_scrub']  = float(mws.forest_to_scrub) if mws.forest_to_scrub is not None else np.nan

        # Surface water properties - store time series as lists
        sw_k_dict = getattr(mws, 'sw_k', {})
        row['sw_k'] = [float(v) if v is not None else np.nan 
                       for _, v in sorted(sw_k_dict.items())] if sw_k_dict else []
        
        sw_kr_dict = getattr(mws, 'sw_kr', {})
        row['sw_r'] = [float(v) if v is not None else np.nan 
                       for _, v in sorted(sw_kr_dict.items())] if sw_kr_dict else []  # rabi from sw_kr
        
        sw_krz_dict = getattr(mws, 'sw_krz', {})
        row['sw_z'] = [float(v) if v is not None else np.nan 
                       for _, v in sorted(sw_krz_dict.items())] if sw_krz_dict else []  # zaid from sw_krz

        crop_vals = [float(v) for v in getattr(mws, 'cropping_intensity', {}).values() if v is not None]
        row['mean_cropping_intensity'] = np.mean(crop_vals) if crop_vals else np.nan
        w_mods = [float(v) for v in getattr(mws, 'w_mod', {}).values() if v is not None]
        w_sevs = [float(v) for v in getattr(mws, 'w_sev', {}).values() if v is not None]
        total_weeks = [mod + sev for mod, sev in zip(w_mods, w_sevs)]
        row['mean_drought_weeks'] = np.mean(total_weeks) if total_weeks else np.nan
        # Water balance: mean(precip-ET-runoff) from well_depth dict
        wb_years = []
        if hasattr(mws, 'well_depth') and isinstance(mws.well_depth, dict):
            for v in mws.well_depth.values():
                if v:
                    try:
                        if isinstance(v, str):
                            v = ast.literal_eval(v)
                        p = float(v.get('Precipitation', 'nan'))
                        et = float(v.get('ET', 'nan'))
                        r = float(v.get('RunOff', 'nan'))
                        if not np.isnan([p,et,r]).any():
                            wb_years.append(p-et-r)
                    except Exception as e:
                        print(e)
                        pass
        row['water_balance'] = np.mean(wb_years) if wb_years else np.nan
        # Mean kr/k ratio (per MWS)
        krk_ratios = []
        for wb in getattr(mws, 'waterbodies', {}).values():
            k_vals = [float(k) for k in getattr(wb, 'k', {}).values() if k not in (None,0)]
            kr_vals = [float(kr) for kr in getattr(wb, 'kr', {}).values() if kr is not None]
            if k_vals and kr_vals:
                k_mean = np.mean(k_vals)
                kr_mean = np.mean(kr_vals)
                if k_mean != 0:
                    krk_ratios.append(kr_mean/k_mean)
        row['mean_krk_ratio'] = np.mean(krk_ratios) if krk_ratios else np.nan
        mws_rows.append(row)
    df = pd.DataFrame(mws_rows)
    print(df)
    return df

#demo implementation to show analysis of different micro-watersheds in the tehsil
#useful to characterize the irrigation and water security profile of a tehsil
if __name__ == "__main__":
    #state = "Jharkhand"
    #district = "Dumka"
    #tehsil = "Masalia"

    #state = "Karnataka"
    #district = "Raichur"
    #tehsil = "Devadurga"

    state = "Gujarat"
    district = "Bhavnagar"
    tehsil = "Bhavnagar"

    tehsil_obj = get_tehsil_data_from_layers(state, district, tehsil)
    df = build_df(tehsil_obj, None)

    # --- PDF of plain area ---
    plain_areas = df['plain_area'].dropna()
    if not plain_areas.empty:
        plt.figure(figsize=(8,6))
        sns.histplot(plain_areas, kde=True, stat='density', bins=20, color='skyblue')
        plt.xlabel('Plain Area')
        plt.title('PDF of Plain Area')
        plt.tight_layout()
        plt.savefig('data/plain_area_pdf.png')
        plt.close()

    # --- PDF of mean drought weeks ---
    drought_weeks = df['mean_drought_weeks'].dropna()
    if not drought_weeks.empty:
        plt.figure(figsize=(8,6))
        sns.histplot(drought_weeks, kde=True, stat='density', bins=20, color='salmon')
        plt.xlabel('Weeks Moderate+Severe Drought (mean, years)')
        plt.title('PDF: Weeks of Moderate+Severe Drought')
        plt.tight_layout()
        plt.savefig('data/drought_frequency_pdf.png')
        plt.close()

    # --- Scatterplot of drought weeks vs cropping intensity ---
    df2 = df.dropna(subset=['mean_drought_weeks', 'mean_cropping_intensity'])
    if not df2.empty:
        plt.figure(figsize=(8,6))
        sns.scatterplot(x=df2['mean_drought_weeks'], y=df2['mean_cropping_intensity'], color='teal', edgecolor='w', s=60)
        plt.xlabel('Weeks Moderate+Severe Drought (mean, years)')
        plt.ylabel('Mean Cropping Intensity')
        plt.title('Drought Frequency vs Cropping Intensity')
        plt.tight_layout()
        plt.savefig('data/drought_frequency_vs_cropping_scatter.png')
        plt.close()

    # --- PDF of water balance ---
    water_balance = df['water_balance'].dropna()
    if not water_balance.empty:
        plt.figure(figsize=(8,6))
        sns.histplot(water_balance, kde=True, stat='density', bins=20, color='salmon')
        plt.xlabel('Water balance (mean, mm)')
        plt.title('PDF: Water balance')
        plt.tight_layout()
        plt.savefig('data/water_balance_pdf.png')
        plt.close()

    # --- Scatterplot of water balance vs cropping intensity ---
    df3 = df.dropna(subset=['water_balance', 'mean_cropping_intensity'])
    if not df3.empty:
        plt.figure(figsize=(8,6))
        sns.scatterplot(x=df3['water_balance'], y=df3['mean_cropping_intensity'], color='teal', edgecolor='w', s=60)
        plt.xlabel('Water Balance [mm]')
        plt.ylabel('Mean Cropping Intensity')
        plt.title('Water Balance vs Cropping Intensity')
        plt.tight_layout()
        plt.savefig('data/water_balance_vs_cropping_scatter.png')
        plt.close()

    # --- PDF of mean kr/k ratio ---
    krkr = df['mean_krk_ratio'].dropna()
    if not krkr.empty:
        plt.figure(figsize=(8,6))
        sns.histplot(krkr, kde=True, stat='density', bins=20, color='green')
        plt.xlabel('Mean kr/k Ratio')
        plt.title('PDF: Mean kr/k Ratio in Microwatersheds')
        plt.tight_layout()
        plt.savefig('data/krk_ratio_pdf.png')
        plt.close()

    # --- Scatterplot of cropping intensity vs kr/k ratio ---
    df4 = df.dropna(subset=['mean_krk_ratio', 'mean_cropping_intensity'])
    if not df4.empty:
        plt.figure(figsize=(8,6))
        sns.scatterplot(x=df4['mean_krk_ratio'], y=df4['mean_cropping_intensity'], color='navy', edgecolor='w', s=60)
        plt.xlabel('Mean kr/k Ratio')
        plt.ylabel('Mean Cropping Intensity')
        plt.title('kr/k Ratio vs Cropping Intensity (MWS)')
        plt.tight_layout()
        plt.savefig('data/cropping_vs_krk_scatter.png')
        plt.close()


