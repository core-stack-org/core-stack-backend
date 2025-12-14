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

# Base URL and (add your) API key
base_url = 'https://geoserver.core-stack.org/api/v1/'
Api_key = ''

# API endpoint to get list of layers
tehsil_layers_api_endpoint = 'get_generated_layer_urls/'
#state = "Jharkhand"
#district = "Dumka"
#tehsil = "Masalia"

state = "Gujarat"
district = "Bhavnagar"
tehsil = "Bhavnagar"

#state = "Karnataka"
#district = "Raichur"
#tehsil = "Devadurga"

# Define the HTTP request headers with the API key
headers = {
    "X-API-Key": Api_key
}

# Define the query parameters to be sent in the API request
params = {
    "state": state,
    "district": district,
    "tehsil": tehsil
}

# Construct the full API URL by combining base URL and endpoint
tehsil_layer_api = f"{base_url}{tehsil_layers_api_endpoint}"

# Make the GET request with headers and parameters
response = requests.get(tehsil_layer_api, params=params, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse JSON response data
    tehsil_layers_response = response.json()
    # You can now work with 'response_data_mws_lat_long'
    print("API call successful. Data received.")
#    print(tehsil_layers_response)
else:
    # Handle errors - print status code and response text for debugging
    print(f"API call failed with status code: {response.status_code}")
    print("Response content:", response.text)
    # Optional: you can raise an exception or handle retries here

# Build a neat list of layers
layers_dict = parse_layers(tehsil_layers_response)
with open(f"data/layers_{state}_{district}_{tehsil}.json", "wb") as f:
    f.write(json.dumps(layers_dict).encode('utf-8'))
#exit()

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

# --- Build DataFrame summarizing each MWS ---
mws_rows = []
for mws in tehsil_obj.microwatersheds.values():
    row = {}
    row['uid'] = getattr(mws, 'uid', None)
    row['plain_area'] = float(mws.plain_area) if mws.plain_area is not None else np.nan
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



