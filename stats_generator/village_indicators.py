import os
import json
from rest_framework.response import Response
import pandas as pd
from nrm_app.settings import EXCEL_PATH
from django.http import HttpResponse
from rest_framework import status


def extract_facilities(df_facilities, v_id):
    """Extract facility distances for a given village ID from the facilities DataFrame."""
    FACILITIES_COLUMN_MAP = {
        "school_primary_distance": "school_primary_distance",
        "school_upper_primary_distance": "school_upper_primary_distance",
        "school_secondary_distance": "school_secondary_distance",
        "school_higher_secondary_distance": "school_higher_secondary_distance",
        "college_distance": "college_distance",
        "universities_distance": "universities_distance",
        "health_sub_center_distance": "health_sub_cen_distance",
        "health_phc_distance": "health_phc_distance",
        "health_chc_distance": "health_chc_distance",
        "health_dis_h_distance": "health_dis_h_distance",
        "health_s_t_h_distance": "health_s_t_h_distance",
        "pds_distance": "pds_distance",
        "csc_distance": "csc_distance",
        "bank_mitra_distance": "bank_mitra_distance",
        "bank_branch_distance": "bank_branch_distance",
        "bank_atm_distance": "bank_atm_distance",
        "apmc_distance": "apmc_distance",
        "agri_industry_markets_trading_distance": "agri_industry_markets_trading_distance",
        "agri_industry_storage_warehousing_distance": "agri_industry_storage_warehousing_distance",
        "agri_industry_distri_utilities_distance": "agri_industry_distribution_utilities_distance",
        "agri_industry_agri_processing_distance": "agri_industry_agri_processing_distance",
        "agri_industry_industrial_manu_distance": "agri_industry_industrial_manufacturing_distance",
        "agri_industry_co_operatives_soci_distance": "agri_industry_co_operatives_societies_distance",
        "agri_industry_dairy_animal_hus_distance": "agri_industry_dairy_animal_husbandry_distance",
        "agri_industry_agri_support_infra_distance": "agri_industry_agri_support_infrastructure_distance",
    }

    fac_data = {key: -1 for key in FACILITIES_COLUMN_MAP}

    if df_facilities.empty:
        return fac_data

    fac_row = df_facilities[df_facilities["lgd_village"] == v_id]
    if fac_row.empty:
        return fac_data

    row = fac_row.iloc[0]
    for output_key, excel_col in FACILITIES_COLUMN_MAP.items():
        if excel_col in row and pd.notna(row[excel_col]):
            fac_data[output_key] = round(row[excel_col], 4)

    return fac_data


def extract_nrega(df_nrega_village, v_id):
    """Extract total NREGA assets for a given village ID from the NREGA DataFrame."""
    if df_nrega_village.empty:
        return -1

    nrega_row = df_nrega_village[df_nrega_village["vill_id"] == v_id]
    total_assets = (
        int(
            nrega_row.drop(columns=["vill_id", "vill_name"], errors="ignore")
            .sum(axis=1)
            .sum()
        )
        if not nrega_row.empty
        else 0
    )
    return total_assets


def extract_soc_eco(df_soc_eco_indi, v_id):
    """Extract social economic indicators for a given village ID."""
    village_row = df_soc_eco_indi[df_soc_eco_indi["village_id"] == v_id]
    return {
        "total_population": village_row["total_population_count"].iloc[0],
        "percent_sc_population": round(village_row["SC_percent"].iloc[0], 4),
        "percent_st_population": round(village_row["ST_percent"].iloc[0], 4),
        "literacy_level": round(village_row["literacy_rate_percent"].iloc[0], 4),
    }


def get_generate_filter_data_village(state, district, block, regenerate=0):
    print("Generation of village filter json")
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

    if not regenerate and os.path.exists(json_path):
        with open(json_path, "rb") as file:
            response = HttpResponse(file.read(), content_type="application/json")
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

    try:
        df_facilities = pd.read_excel(xlsx_file, sheet_name="facilities_proximity")
    except Exception as e:
        print("Failed to load facilities_proximity:", e)
        df_facilities = pd.DataFrame()

    results = []

    if not df_soc_eco_indi.empty:
        for v_id in df_soc_eco_indi["village_id"].unique():
            if v_id == 0:
                continue

            soc_eco = extract_soc_eco(df_soc_eco_indi, v_id)
            total_assets = extract_nrega(df_nrega_village, v_id)
            fac_data = extract_facilities(df_facilities, v_id)

            results.append(
                {
                    "village_id": v_id,
                    **soc_eco,
                    "total_assets": total_assets,
                    # Essential education infrastructure
                    **fac_data,
                }
            )

    results_list = pd.DataFrame(results).to_dict(orient="records")

    with open(json_path, "w") as f:
        json.dump(results_list, f, indent=4)

    if os.path.exists(json_path):
        with open(json_path, "rb") as file:
            response = HttpResponse(file.read(), content_type="application/json")
            response["Content-Disposition"] = (
                f"attachment; filename={district}_{block}_KYL_village_data.json"
            )
            return response

    return Response(
        {"status": "error", "message": "Failed to generate village data file"},
        status=status.HTTP_404_NOT_FOUND,
    )
