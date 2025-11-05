import os
import requests
import json
from rest_framework.response import Response
import pandas as pd
import pymannkendall as mk
import numpy as np
from shapely.geometry import Point, shape
from .utils import get_url
from nrm_app.settings import EXCEL_PATH


def get_generate_filter_data_village(state, district, block):
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

    print("file path", xlsx_file)

    # Try to load required sheets
    try:
        df_soc_eco_indi = pd.read_excel(
            xlsx_file, sheet_name="social_economic_indicator"
        )
    except Exception as e:
        df_soc_eco_indi = pd.DataFrame()

    try:
        df_nrega_village = pd.read_excel(xlsx_file, sheet_name="nrega_assets_village")
    except Exception as e:
        df_nrega_village = pd.DataFrame()

    results = []

    if not df_soc_eco_indi.empty and not df_nrega_village.empty:
        for specific_village_id in df_soc_eco_indi["village_id"].unique():
            village_id_data = df_soc_eco_indi[
                df_soc_eco_indi["village_id"] == specific_village_id
            ]
            village_nrega_data = df_nrega_village[
                df_nrega_village["vill_id"] == specific_village_id
            ]

            total_population = village_id_data.get("total_population_count", None).iloc[
                0
            ]
            SC_percentage = round(village_id_data.get("SC_percent", None).iloc[0], 4)
            ST_percentage = round(village_id_data.get("ST_percent", None).iloc[0], 4)
            literacy_rate = round(
                village_id_data.get("literacy_rate_percent", None).iloc[0], 4
            )
            total_assets = int(
                village_nrega_data.drop(
                    columns=["vill_id", "vill_name"], errors="ignore"
                )
                .sum(axis=1)
                .sum()
            )

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
    results_df.to_excel(file_xl_path + "_KYL_village_data.xlsx", index=False)
    results_list = results_df.to_dict(orient="records")

    with open(file_xl_path + "_KYL_village_data.json", "w") as json_file:
        json.dump(results_list, json_file, indent=4)

    # If no data, skip geojson merging
    if not results_list:
        return file_xl_path + "_KYL_village_data.json"

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
                    }
                )

        deltaG_geojson = file_xl_path + "_panchayat_boundaries_nw.geojson"
        with open(deltaG_geojson, "w") as f:
            json.dump(geojson_data, f)

    except Exception as e:
        # Log or handle geojson fetch failure gracefully
        pass

    file_path = file_xl_path + "_KYL_village_data.json"
    return file_path if os.path.exists(file_path) else None
