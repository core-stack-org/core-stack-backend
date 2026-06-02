import os
import json
from rest_framework.response import Response
import pandas as pd
from nrm_app.settings import EXCEL_PATH
from django.http import HttpResponse
from rest_framework import status


import pandas as pd


def extract_facilities(df_facilities, v_id):
    """Return only grouped max facility indicators."""
    DEFAULT_VALUE = {
        "essential_education_infra": -1,
        "higher_education_infra": -1,
        "essential_health_services": -1,
        "advanced_health_services": -1,
        "public_distribution_system": -1,
        "financial_inclusion": -1,
        "agri_market_access": -1,
        "post_harvest_infra": -1,
        "farmer_cooperatives_access": -1,
        "livestock_management_centers": -1,
        "agricultural_support_infrastructure": -1,
    }

    # Safely check the nan and pass the max or -1 value
    def get_max(values):
        valid = [v for v in values if pd.notna(v) and v != -1]
        return round(max(valid), 4) if valid else -1

    def get_min(values):
        valid = [v for v in values if pd.notna(v) and v != -1]
        return round(min(valid), 4) if valid else -1

    # If a indicators contain only single column the safely check for Nan
    def safe_val(v):
        return round(v, 4) if pd.notna(v) and v != -1 else -1

    if df_facilities.empty:
        return DEFAULT_VALUE.copy()

    fac_row = df_facilities[df_facilities["censuscode2011"] == v_id]
    if fac_row.empty:
        return DEFAULT_VALUE.copy()

    row = fac_row.iloc[0]

    result = {
        "essential_education_infra": get_max(
            [
                row.get("school_primary_distance", -1),
                row.get("school_upper_primary_distance", -1),
                row.get("school_secondary_distance", -1),
            ]
        ),
        "higher_education_infra": get_min(
            [
                row.get("school_higher_secondary_distance", -1),
                row.get("college_distance", -1),
                row.get("universities_distance", -1),
            ]
        ),
        "essential_health_services": get_max(
            [
                row.get("health_sub_cen_distance", -1),
                row.get("health_phc_distance", -1),
            ]
        ),
        "advanced_health_services": get_min(
            [
                row.get("health_chc_distance", -1),
                row.get("health_dis_h_distance", -1),
                row.get("health_s_t_h_distance", -1),
            ]
        ),
        "public_distribution_system": get_max(
            [
                row.get("pds_distance", -1),
            ]
        ),
        "financial_inclusion": get_max(
            [
                row.get("csc_distance", -1),
                row.get("bank_mitra_distance", -1),
                row.get("bank_branch_distance", -1),
                row.get("bank_atm_distance", -1),
            ]
        ),
        "agri_market_access": get_min(
            [
                row.get("apmc_distance", -1),
                row.get("agri_industry_markets_trading_distance", -1),
            ]
        ),
        "post_harvest_infra": get_min(
            [
                row.get("agri_industry_storage_warehousing_distance", -1),
                row.get("agri_industry_distribution_utilities_distance", -1),
                row.get("agri_industry_agri_processing_distance", -1),
                row.get("agri_industry_industrial_manufacturing_distance", -1),
            ]
        ),
        "farmer_cooperatives_access": safe_val(
            row.get("agri_industry_co_operatives_societies_distance", -1)
        ),
        "livestock_management_centers": safe_val(
            row.get("agri_industry_dairy_animal_husbandry_distance", -1)
        ),
        "agricultural_support_infrastructure": safe_val(
            row.get("agri_industry_agri_support_infrastructure_distance", -1)
        ),
    }

    return result


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

    # Return existing json if already generated
    if not regenerate and os.path.exists(json_path):
        with open(json_path, "rb") as file:
            response = HttpResponse(file.read(), content_type="application/json")
            response["Content-Disposition"] = (
                f"attachment; " f"filename={district}_{block}_KYL_village_data.json"
            )

            return response

    # --------------------------------------------------
    # Mandatory sheet check
    # --------------------------------------------------
    try:
        df_soc_eco_indi = pd.read_excel(
            xlsx_file, sheet_name="social_economic_indicator"
        )

        if df_soc_eco_indi.empty:
            raise ValueError("Empty social_economic_indicator sheet")
    except Exception as e:
        print("No data found for panchayat boundary:", e)

        empty_data = []

        # Save empty json
        with open(json_path, "w") as f:
            json.dump(empty_data, f, indent=4)

        return HttpResponse(
            json.dumps(
                {
                    "message": "No data found for the panchayat boundary",
                    "data": empty_data,
                }
            ),
            content_type="application/json",
            status=200,
        )

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

    # --------------------------------------------------
    # Generate village json
    # --------------------------------------------------
    results = []

    for v_id in df_soc_eco_indi["village_id"].dropna().unique():
        if v_id == 0:
            continue

        try:
            soc_eco = extract_soc_eco(df_soc_eco_indi, v_id)
        except Exception as e:
            print(f"extract_soc_eco failed " f"for village {v_id}: {e}")
            soc_eco = {}

        # ----------------------------------------------
        # NREGA data
        # ----------------------------------------------
        try:
            total_assets = (
                extract_nrega(df_nrega_village, v_id)
                if not df_nrega_village.empty
                else 0
            )
        except Exception as e:
            print(f"extract_nrega failed " f"for village {v_id}: {e}")
            total_assets = 0

        # ----------------------------------------------
        # Facilities data
        # ----------------------------------------------
        try:
            fac_data = (
                extract_facilities(df_facilities, v_id)
                if not df_facilities.empty
                else {}
            )
        except Exception as e:
            print(f"extract_facilities failed " f"for village {v_id}: {e}")
            fac_data = {}

        # ----------------------------------------------
        # Final village object
        # ----------------------------------------------
        results.append(
            {
                "village_id": int(v_id),
                **soc_eco,
                "total_assets": total_assets,
                **fac_data,
            }
        )

    # --------------------------------------------------
    # Save generated json
    # --------------------------------------------------
    with open(json_path, "w") as f:
        json.dump(results, f, indent=4, default=str)

    # --------------------------------------------------
    # Return response
    # --------------------------------------------------
    return HttpResponse(
        json.dumps(
            {"message": "Village data generated successfully", "data": results},
            default=str,
        ),
        content_type="application/json",
        status=200,
    )
