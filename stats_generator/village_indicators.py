import os
import json
from rest_framework.response import Response
import pandas as pd
from nrm_app.settings import EXCEL_PATH
from django.http import HttpResponse
from rest_framework import status


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

    try:
        df_facilities = pd.read_excel(xlsx_file, sheet_name="facilities_proximity")
    except Exception as e:
        print("Failed to load facilities_proximity:", e)
        df_facilities = pd.DataFrame()

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

            ###### facilities proximity data   ############
            school_primary_dist = school_upper_primary_dist = school_secondary_dist = (
                school_higher_secondary_dist
            ) = college_dist = universities_dist = health_sub_cen_dist = (
                health_phc_dist
            ) = health_chc_dist = health_dis_h_dist = health_s_t_h_dist = pds_dist = (
                csc_dist
            ) = bank_mitra_dist = bank_branch_dist = bank_atm_dist = apmc_dist = (
                agri_industry_markets_trading_dist
            ) = agri_industry_storage_warehousing_dist = (
                agri_industry_distribution_utilities_dist
            ) = agri_industry_agri_processing_dist = (
                agri_industry_industrial_manufacturing_dist
            ) = agri_industry_co_operatives_societies_dist = (
                agri_industry_dairy_animal_husbandry_dist
            ) = agri_industry_agri_support_infrastructure_dist = -1
            if not df_facilities.empty:
                fac_row = df_facilities[df_facilities["lgd_village"] == v_id]

                if not fac_row.empty:
                    school_primary_dist = round(
                        fac_row["school_primary_distance"].iloc[0], 4
                    )
                    school_upper_primary_dist = round(
                        fac_row["school_upper_primary_distance"].iloc[0], 4
                    )
                    school_secondary_dist = round(
                        fac_row["school_secondary_distance"].iloc[0], 4
                    )

                    school_higher_secondary_dist = round(
                        fac_row["school_higher_secondary_distance"].iloc[0], 4
                    )
                    college_dist = round(fac_row["college_distance"].iloc[0], 4)
                    universities_dist = round(fac_row["universities_distance"].iloc[0])

                    health_sub_cen_dist = round(
                        fac_row["health_sub_cen_distance"].iloc[0], 4
                    )
                    health_phc_dist = round(fac_row["health_phc_distance"].iloc[0], 4)

                    health_chc_dist = round(fac_row["health_chc_distance"].iloc[0], 4)
                    health_dis_h_dist = round(
                        fac_row["health_dis_h_distance"].iloc[0], 4
                    )
                    health_s_t_h_dist = round(
                        fac_row["health_s_t_h_distance"].iloc[0], 4
                    )

                    pds_dist = round(fac_row["pds_distance"].iloc[0], 4)

                    csc_dist = round(fac_row["csc_distance"].iloc[0], 4)
                    bank_mitra_dist = round(fac_row["bank_mitra_distance"].iloc[0], 4)
                    bank_branch_dist = round(fac_row["bank_branch_distance"].iloc[0], 4)
                    bank_atm_dist = round(fac_row["bank_atm_distance"].iloc[0], 4)

                    apmc_dist = round(fac_row["apmc_distance"].iloc[0], 4)
                    agri_industry_markets_trading_dist = round(
                        fac_row["agri_industry_markets_trading_distance"].iloc[0], 4
                    )

                    agri_industry_storage_warehousing_dist = round(
                        fac_row["agri_industry_storage_warehousing_distance"].iloc[0], 4
                    )
                    agri_industry_distribution_utilities_dist = round(
                        fac_row["agri_industry_distribution_utilities_distance"].iloc[
                            0
                        ],
                        4,
                    )
                    agri_industry_agri_processing_dist = round(
                        fac_row["agri_industry_agri_processing_distance"].iloc[0], 4
                    )
                    agri_industry_industrial_manufacturing_dist = round(
                        fac_row["agri_industry_industrial_manufacturing_distance"].iloc[
                            0
                        ],
                        4,
                    )

                    agri_industry_co_operatives_societies_dist = round(
                        fac_row["agri_industry_co_operatives_societies_distance"].iloc[
                            0
                        ],
                        4,
                    )
                    agri_industry_dairy_animal_husbandry_dist = round(
                        fac_row["agri_industry_dairy_animal_husbandry_distance"].iloc[
                            0
                        ],
                        4,
                    )
                    agri_industry_agri_support_infrastructure_dist = round(
                        fac_row[
                            "agri_industry_agri_support_infrastructure_distance"
                        ].iloc[0],
                        4,
                    )

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
                        # Essential education infrastructure
                        "school_primary_distance": school_primary_dist,
                        "school_upper_primary_distance": school_upper_primary_dist,
                        "school_secondary_distance": school_secondary_dist,
                        # Higher education infrastructure
                        "school_higher_secondary_distance": school_higher_secondary_dist,
                        "college_distance": college_dist,
                        "universities_distance": universities_dist,
                        # Essential health services
                        "health_sub_center_distance": health_sub_cen_dist,
                        "health_phc_distance": health_phc_dist,
                        # Advanced health services
                        "health_chc_distance": health_chc_dist,
                        "health_dis_h_distance": health_dis_h_dist,
                        "health_s_t_h_distance": health_s_t_h_dist,
                        # Public distribution system
                        "pds_distance": pds_dist,
                        # Financial Inclusion
                        "csc_distance": csc_dist,
                        "bank_mitra_distance": bank_mitra_dist,
                        "bank_branch_distance": bank_branch_dist,
                        "bank_atm_distance": bank_atm_dist,
                        # Agricultural Market Access
                        "apmc_distance": apmc_dist,
                        "agri_market_distance": agri_industry_markets_trading_dist,
                        # Post Agricultural Produce Harvest Infrastructure
                        "storage_warehousing_distance": agri_industry_storage_warehousing_dist,
                        "distribution_utilities_distance": agri_industry_distribution_utilities_dist,
                        "agri_processing_distance": agri_industry_agri_processing_dist,
                        "industrial_manufacturing_distance": agri_industry_industrial_manufacturing_dist,
                        # Access to farmer cooperatives
                        "cooperative_distance": agri_industry_co_operatives_societies_dist,
                        # Livestock Management
                        "livestock_distance": agri_industry_dairy_animal_husbandry_dist,
                        # Agricultural support infrastructure
                        "agri_support_distance": agri_industry_agri_support_infrastructure_dist,
                    }
                )

    results_df = pd.DataFrame(results)
    results_list = results_df.to_dict(orient="records")

    with open(json_path, "w") as f:
        json.dump(results_list, f, indent=4)

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
