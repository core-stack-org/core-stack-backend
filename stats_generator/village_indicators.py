import os
import requests
import json
from rest_framework.response import Response
import pandas as pd
import numpy as np
from shapely.geometry import Point, shape
from .utils import get_url
from nrm_app.settings import EXCEL_PATH


def get_generate_filter_data_village(state, district, block):
    # Clean up folder names
    state_folder = state.replace(" ", "_").upper()
    district_folder = district.replace(" ", "_").upper()
    file_xl_path = os.path.join(
        EXCEL_PATH,
        "data/stats_excel_files",
        state_folder,
        district_folder,
        f"{district}_{block}",
    )
    xlsx_file = file_xl_path + ".xlsx"

    print("File path:", xlsx_file)

    # Try to load required sheets
    try:
        df_soc_eco_indi = pd.read_excel(
            xlsx_file, sheet_name="social_economic_indicator"
        )
    except Exception as e:
        print("Failed to load social_economic_indicator:", e)
        df_soc_eco_indi = pd.DataFrame()

    try:
        df_nrega_village = pd.read_excel(xlsx_file, sheet_name="nrega_assets_village")
    except Exception as e:
        print("Failed to load nrega_assets_village:", e)
        df_nrega_village = pd.DataFrame()

    results = []

    # Process villages even if NREGA data is missing
    if not df_soc_eco_indi.empty:
        for specific_village_id in df_soc_eco_indi["village_id"].unique():
            # Get village socio-economic data
            village_id_data = df_soc_eco_indi[
                df_soc_eco_indi["village_id"] == specific_village_id
            ]

            # Try to get NREGA data for this village
            if not df_nrega_village.empty:
                village_nrega_data = df_nrega_village[
                    df_nrega_village["vill_id"] == specific_village_id
                ]
                if not village_nrega_data.empty:
                    total_assets = int(
                        village_nrega_data.drop(
                            columns=["vill_id", "vill_name"], errors="ignore"
                        )
                        .sum(axis=1)
                        .sum()
                    )
                else:
                    total_assets = 0
            else:
                total_assets = -1

            # Extract basic indicators
            total_population = village_id_data.get("total_population_count", None).iloc[
                0
            ]
            SC_percentage = round(village_id_data.get("SC_percent", None).iloc[0], 4)
            ST_percentage = round(village_id_data.get("ST_percent", None).iloc[0], 4)
            literacy_rate = round(
                village_id_data.get("literacy_rate_percent", None).iloc[0], 4
            )

            # Skip invalid IDs
            if specific_village_id != 0:
                results.append(
                    {
                        "village_id": specific_village_id,
                        "total_population": total_population,
                        "percent_st_population": ST_percentage,
                        "percent_sc_population": SC_percentage,
                        "literacy_level": literacy_rate,
                        "total_assets": total_assets,
                    }
                )

    # Save results (empty or filled)
    results_df = pd.DataFrame(results)
    json_path = file_xl_path + "_KYL_village_data.json"
    excel_path = file_xl_path + "_KYL_village_data.xlsx"

    results_df.to_excel(excel_path, index=False)
    results_list = results_df.to_dict(orient="records")

    with open(json_path, "w") as json_file:
        json.dump(results_list, json_file, indent=4)

    # Skip geojson merge if no data
    if not results_list:
        print("No data found for villages.")
        return json_path

    # Merge data into Panchayat GeoJSON
    layer_name = district + "_" + block
    panchayat_bound_geojson = get_url("panchayat_boundaries", layer_name)

    try:
        response = requests.get(panchayat_bound_geojson)
        response.raise_for_status()
        geojson_data = response.json()

        for feature in geojson_data["features"]:
            vill_id = feature["properties"].get("vill_ID")
            village_data = next(
                (item for item in results_list if item["village_id"] == vill_id), None
            )

            if village_data:
                feature["properties"].update(
                    {
                        "total_population": village_data["total_population"],
                        "percent_st_population": village_data["percent_st_population"],
                        "percent_sc_population": village_data["percent_sc_population"],
                        "literacy_level": village_data["literacy_level"],
                        "total_assets": village_data["total_assets"],
                    }
                )

        deltaG_geojson = file_xl_path + "_panchayat_boundaries_nw.geojson"
        with open(deltaG_geojson, "w") as f:
            json.dump(geojson_data, f)

    except Exception as e:
        print("Failed to fetch or update GeoJSON:", e)

    return json_path if os.path.exists(json_path) else None
