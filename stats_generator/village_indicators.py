import os
import requests
import json
from rest_framework.response import Response
import pandas as pd
import numpy as np
from .utils import get_url
from nrm_app.settings import EXCEL_PATH
from django.http import HttpResponse
from rest_framework import status


def get_generate_filter_data_village(state, district, block, regenerate=False):
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
    json_path = file_xl_path + "_KYL_village_data.json"
    excel_output = file_xl_path + "_KYL_village_data.xlsx"

    if not regenerate and os.path.exists(json_path):
        with open(json_path, "rb") as file:
            response = HttpResponse(
                file.read(),
                content_type="application/json",
            )
            response["Content-Disposition"] = (
                f"attachment; filename={district}_{block}_KYL_village_data.json"
            )
            return response

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

    if not df_soc_eco_indi.empty:
        for v_id in df_soc_eco_indi["village_id"].unique():
            village_row = df_soc_eco_indi[df_soc_eco_indi["village_id"] == v_id]

            # total assets (if NREGA present)
            if not df_nrega_village.empty:
                nrega_row = df_nrega_village[df_nrega_village["vill_id"] == v_id]
                total_assets = (
                    int(
                        nrega_row.drop(
                            columns=["vill_id", "vill_name"], errors="ignore"
                        )
                        .sum(axis=1)
                        .sum()
                    )
                    if not nrega_row.empty
                    else 0
                )
            else:
                total_assets = -1  # indicator of no NREGA data

            # extract indicators
            total_population = village_row["total_population_count"].iloc[0]
            SC_percentage = round(village_row["SC_percent"].iloc[0], 4)
            ST_percentage = round(village_row["ST_percent"].iloc[0], 4)
            literacy_rate = round(village_row["literacy_rate_percent"].iloc[0], 4)

            # skip invalid IDs
            if v_id != 0:
                results.append(
                    {
                        "village_id": v_id,
                        "total_population": total_population,
                        "percent_st_population": ST_percentage,
                        "percent_sc_population": SC_percentage,
                        "literacy_level": literacy_rate,
                        "total_assets": total_assets,
                    }
                )

    results_df = pd.DataFrame(results)
    results_df.to_excel(excel_output, index=False)

    results_list = results_df.to_dict(orient="records")

    with open(json_path, "w") as f:
        json.dump(results_list, f, indent=4)

    if results_list:
        layer_name = f"{district}_{block}"
        url = get_url("panchayat_boundaries", layer_name)

        try:
            r = requests.get(url)
            r.raise_for_status()
            geojson_data = r.json()

            for feature in geojson_data["features"]:
                vill_id = feature["properties"].get("vill_ID")

                match = next(
                    (item for item in results_list if item["village_id"] == vill_id),
                    None,
                )

                if match:
                    feature["properties"].update(match)

            out_geojson = file_xl_path + "_panchayat_boundaries_nw.geojson"
            with open(out_geojson, "w") as f:
                json.dump(geojson_data, f)

        except Exception as e:
            print("Failed updating GeoJSON:", e)

    if os.path.exists(json_path):
        with open(json_path, "rb") as file:
            response = HttpResponse(
                file.read(),
                content_type="application/json",
            )
            response["Content-Disposition"] = (
                f"attachment; filename={district}_{block}_KYL_village_data.json"
            )
            return response

    return Response(
        {"status": "error", "message": "Failed to generate village data file"},
        status=status.HTTP_404_NOT_FOUND,
    )
