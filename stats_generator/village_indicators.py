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

    fac_row = df_facilities[df_facilities["village_id"] == v_id]
    if fac_row.empty:
        return DEFAULT_VALUE.copy()

    row = fac_row.iloc[0]

    result = {
        "essential_education_infra": row.get(
            "essential_education_cat_distance_in_km", -1
        ),
        "higher_education_infra": safe_val(
            row.get("higher_education_cat_distance_in_km", -1)
        ),
        "essential_health_services": safe_val(
            row.get("essential_health_cat_distance_in_km", -1)
        ),
        "advanced_health_services": safe_val(
            row.get("advanced_health_cat_distance_in_km", -1)
        ),
        "public_distribution_system": safe_val(
            row.get("essential_services_cat_distance_in_km", -1)
        ),
        "financial_inclusion": safe_val(
            row.get("financial_inclusion_cat_distance_in_km", -1)
        ),
        "agri_market_access": safe_val(row.get("apmc_markets_cat_distance_in_km", -1)),
        "post_harvest_infra": safe_val(row.get("post_harvest_cat_distance_in_km", -1)),
        "farmer_cooperatives_access": safe_val(
            row.get("cooperative_cat_distance_in_km", -1)
        ),
        "livestock_management_centers": safe_val(
            row.get("livestock_cat_distance_in_km", -1)
        ),
        "agricultural_support_infrastructure": safe_val(
            row.get("agri_support_infra_cat_distance_in_km", -1)
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


def extract_livestock(df_livestock, v_id):
    village_row = df_livestock[df_livestock["village_id"] == v_id]
    return {
        "large_animals_total": village_row["large_animals_total"].iloc[0],
        "small_animals_total": village_row["small_animals_total"].iloc[0],
    }


# def extract_antyodaya(df_antyodaya, v_id):
#     """Extract social economic indicators for a given village ID."""
#     data_map = {
#         "Low": 0,
#         "Medium": 1,
#         "High": 2,
#     }
#
#     def get_cluster_from_score(value):
#         if pd.isna(value):
#             return None
#
#         nearest = min([0, 0.5, 1], key=lambda x: abs(value - x))
#
#         return {
#             0: 0,  # Low
#             0.5: 1,  # Medium
#             1: 2,  # High
#         }[nearest]
#
#     village_row = df_antyodaya[df_antyodaya["village_id"] == v_id]
#     coverage_accross_pds_cols = [
#         "pds_util_feat_value",
#         "nfsa_cov_feat_value",
#         "bpl_cov_feat_value",
#         "pension_cov_feat_value",
#     ]
#     coverage_across_PDS_NFSA_BPL_and_Pension = (
#         village_row[coverage_accross_pds_cols].fillna(0).mean(axis=1).iloc[0]
#     )
#
#     coverage_across_PDS_NFSA_BPL_and_Pension = get_cluster_from_score(
#         coverage_across_PDS_NFSA_BPL_and_Pension
#     )
#     print("coverage cluster", coverage_across_PDS_NFSA_BPL_and_Pension)
#
#     return {
#         "road_connectivity": data_map.get(
#             village_row["road_connectivity_cat_cluster"].iloc[0], -9999
#         ),
#         "electricity_supply": data_map.get(
#             village_row["electricity_supply_to_msme_feat_cluster"].iloc[0], -9999
#         ),
#         "housing_quality": data_map.get(
#             village_row["housing_quality_cat_cluster"].iloc[0], -9999
#         ),
#         "maternal_and_child_health_service_access": data_map.get(
#             village_row["maternal_child_health_cat_cluster"].iloc[0], -9999
#         ),
#         "water_and_sanitation_infrastructure": data_map.get(
#             village_row["water_sanitation_cat_cluster"].iloc[0], -9999
#         ),
#         "access_to_formal_banking_services": data_map.get(
#             village_row["bank_feat_cluster"].iloc[0], -9999
#         ),
#         "coverage_across_PDS_NFSA_BPL_and_Pension": coverage_across_PDS_NFSA_BPL_and_Pension,
#         "institutionalization_strength": data_map.get(
#             village_row["institutionalization_cat_cluster"].iloc[0], -9999
#         ),
#         "civic_infrastructure": data_map.get(
#             village_row["civic_infrastructure_cat_cluster"].iloc[0], -9999
#         ),
#         "farm_employment": data_map.get(
#             village_row["farm_employment_feat_cluster"].iloc[0], -9999
#         ),
#         "forest-based_livelihood": data_map.get(
#             village_row["livelihoods_forest_resources_cat_cluster"].iloc[0], -9999
#         ),
#         "alternate_farming": data_map.get(
#             village_row["livelihoods_alternative_farming_cat_cluster"].iloc[0], -9999
#         ),
#         "fisheries_adoption": data_map.get(
#             village_row["livelihoods_fisheries_cat_cluster"].iloc[0], -9999
#         ),
#         "cottage_industry": data_map.get(
#             village_row["livelihoods_cottage_traditional_industry_cat_cluster"].iloc[0],
#             -9999,
#         ),
#         "livestock_management_service_quality": data_map.get(
#             village_row["electricity_supply_to_msme_feat_cluster"].iloc[0], -9999
#         ),
#         "common_pasture_access": data_map.get(
#             village_row["common_pastures_feat_cluster"].iloc[0], -9999
#         ),
#         "watershed_infrastructure_and_modern_irrigation": data_map.get(
#             village_row["irrigation_infra_watershed_dev_feat_cluster"].iloc[0], -9999
#         ),
#         "organic_farming_adoption": data_map.get(
#             village_row["agriculture_organic_farming_cat_cluster"].iloc[0], -9999
#         ),
#         "pension_coverage_and_soil_testing_services_adoption": data_map.get(
#             village_row["pension_cov_feat_cluster"].iloc[0], -9999
#         ),
#     }


def extract_antyodaya(df_antyodaya, v_id):
    """Return finalized category and raw Antyodaya fields for one village.

    Category clusters, category values, and raw values are copied directly from
    the Excel row.  No feature-level values or derived calculations are used.
    """
    category_raw_columns = {
        "institutionalization": (
            "availability_of_fpos_pacs",
            "total_hhd",
            "total_hhd_mobilized_into_pg",
            "total_hhd_mobilized_into_shg",
            "total_no_of_shg_promoted",
            "total_shg",
        ),
        "social_protection": (
            "gp_total_hhd_eligible_under_nfsa",
            "gp_total_hhd_receiving_food_grains_from_fps",
            "total_hhd",
            "total_hhd_availing_pension_under_nsap",
            "total_hhd_having_bpl_cards",
        ),
        "civic_infrastructure": (
            "availability_of_panchayat_bhawan",
            "availability_of_public_information_board",
            "availability_of_public_library",
            "is_post_office_available",
            "total_no_of_elect_rep_oriented_under_rgsa",
            "total_no_of_elect_rep_undergone_training_under_rgsa",
            "total_no_of_elected_representatives",
        ),
        "financial_inclusion": (
            "is_atm_available",
            "is_bank_available",
            "is_bank_buss_correspondent_with_internet",
            "total_hhd",
            "total_hhd_availing_pmjdy_bank_ac",
            "total_shg",
            "total_shg_accessed_bank_loans",
        ),
        "energy_access": (
            "availability_of_elect_supply_to_msme",
            "availablility_hours_of_domestic_electricity",
            "total_hhd",
            "total_hhd_with_clean_energy",
        ),
        "road_connectivity": (
            "availability_of_internal_pucca_road",
            "availability_of_public_transport",
            "availability_of_railway_station",
            "is_village_connected_to_all_weather_road",
        ),
        "housing_quality": (
            "total_hhd",
            "total_hhd_availing_pmuy_benefits",
            "total_hhd_got_benefit_under_state_housing_scheme",
            "total_hhd_have_got_pmay_house",
            "total_hhd_in_pmay_permanent_wait_list",
            "total_hhd_with_kuccha_wall_kuccha_roof",
        ),
        "maternal_child_health": (
            "availability_of_mother_child_health_facilities",
            "gp_total_no_of_beneficiaries_receiving_benefits_under_pmjay",
            "gp_total_no_of_eligible_beneficiaries_under_pmjay",
            "is_aanganwadi_centre_available",
            "is_early_childhood_edu_provided_in_anganwadi",
            "total_anemic_pregnant_women",
            "total_childs_aged_0_to_3_years",
            "total_childs_aged_0_to_3_years_immunized",
            "total_childs_aged_0_to_3_years_reg_under_aanganwadi",
            "total_childs_aged_3_to_6_years_reg_under_aanganwadi",
            "total_childs_categorized_non_stunted_as_per_icds",
            "total_female_child_age_bw_0_6",
            "total_hhd",
            "total_hhd_registered_under_pmjay",
            "total_male_child_age_bw_0_6",
            "total_no_of_beneficiaries_receiving_benefits_under_pmmvy",
            "total_no_of_children_in_icds_cas",
            "total_no_of_eligible_beneficiaries_under_pmmvy",
            "total_no_of_lactating_mothers",
            "total_no_of_lactating_mothers_receiving_services_under_icds",
            "total_no_of_newly_born_children",
            "total_no_of_newly_born_underweight_children",
            "total_no_of_pregnant_women",
            "total_no_of_pregnant_women_receiving_services_under_icds",
            "total_no_of_registered_children_in_anganwadi",
            "total_no_of_women_delivered_babies_at_hospitals_registered_asha",
            "total_no_of_young_anemic_children_6_59_months_in_icds_cas",
            "total_underweight_child_age_under_6_years",
        ),
        "water_sanitation": (
            "availability_of_drainage_system",
            "availability_of_piped_tap_water",
            "is_community_biogas_waste_recycle_for_production",
            "is_community_waste_disposal_system",
            "total_hhd",
            "total_hhd_having_piped_water_connection",
            "total_hhd_not_having_sanitary_latrines",
        ),
        "livelihoods_cottage_traditional_industry": (
            "availability_of_cottage_small_scale_units",
            "is_handicrafts",
            "is_handloom",
            "total_hhd",
            "total_hhd_engaged_cottage_small_scale_units",
        ),
        "livelihoods_employment": (
            "total_hhd",
            "total_hhd_engaged_in_farm_activities",
        ),
        "livelihoods_forest_resources": (
            "availability_of_community_forest",
            "availability_of_minor_forest_production",
            "total_hhd",
            "total_hhd_source_of_minor_forest_production",
        ),
        "livelihoods_common_resources": ("is_common_pastures_available",),
        "livelihoods_alternative_farming": (
            "is_bee_farming",
            "is_sericulture",
        ),
        "livelihoods_fisheries": (
            "availability_of_aquaculture_ext_facility",
            "availability_of_fish_community_ponds",
            "availability_of_fish_farming",
        ),
        "livestock_veterinary": (
            "availability_of_goatary_dev_project",
            "availability_of_livestock_extension_services",
            "availability_of_milk_routes",
            "availability_of_pigery_development",
            "availability_of_poultry_dev_project",
            "is_veterinary_hospital_available",
        ),
        "agriculture_land_cultivation": (
            "area_irrigated_in_hac",
            "net_sown_area_in_hac",
            "net_sown_area_kharif_in_hac",
            "net_sown_area_other_in_hac",
            "net_sown_area_rabi_in_hac",
            "total_cultivable_area_in_hac",
        ),
        "agriculture_irrigation_watershed": (
            "availability_of_major_source_of_irrigation",
            "availability_of_rain_harvest_system",
            "availability_of_watershed_dev_project",
            "no_of_farmers_using_drip_sprinkler",
            "total_approved_labour_budget_for_year",
            "total_expenditure_approved_under_nrm_labour_budget_during_yr",
            "total_no_of_farmers",
        ),
        "agriculture_organic_farming": (
            "total_no_farmers_adopted_organic_farming",
            "total_no_of_farmers",
        ),
        "agriculture_support_services": (
            "is_fertilizer_shop_available",
            "is_govt_seed_centre_available",
            "is_soil_testing_centre_available",
            "total_no_of_farmers",
            "total_no_of_farmers_add_fert_in_soil_as_per_report",
            "total_no_of_farmers_received_benefit_under_pmfby",
            "total_no_of_farmers_registered_under_pmkpy",
        ),
        "agricultural_markets": (
            "availability_of_food_storage_warehouse",
            "availability_of_market",
        ),
    }

    required_columns = []
    for category, raw_columns in category_raw_columns.items():
        required_columns.extend(
            (f"{category}_cat_cluster", f"{category}_cat_value", *raw_columns)
        )
    required_columns = list(dict.fromkeys(required_columns))

    default_row = {column: -9999 for column in required_columns}

    if df_antyodaya.empty or "village_id" not in df_antyodaya.columns:
        return default_row

    village_rows = df_antyodaya[df_antyodaya["village_id"] == v_id]
    if village_rows.empty:
        return default_row

    missing_columns = [
        column for column in required_columns if column not in df_antyodaya.columns
    ]
    if missing_columns:
        raise ValueError(
            "Antyodaya sheet is missing required columns: " + ", ".join(missing_columns)
        )

    row = village_rows.iloc[0]

    def excel_value(value):
        if pd.isna(value):
            return None
        return value.item() if hasattr(value, "item") else value

    return {column: excel_value(row[column]) for column in required_columns}


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

    try:
        df_livestock = pd.read_excel(xlsx_file, sheet_name="livestock")
    except Exception as e:
        print("Failed to load livestock:", e)
        df_livestock = pd.DataFrame()

    try:
        df_antyodaya = pd.read_excel(xlsx_file, sheet_name="antyodaya")
    except Exception as e:
        print("Failed to load antyodaya:", e)
        df_antyodaya = pd.DataFrame()

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
        # livestock data
        # ----------------------------------------------
        try:
            livestock_data = (
                extract_livestock(df_livestock, v_id) if not df_livestock.empty else 0
            )
        except Exception as e:
            print(f"extract_livestock failed " f"for village {v_id}: {e}")
            livestock_data = 0

        # ----------------------------------------------
        # Antyodaya data
        # ----------------------------------------------
        try:
            antyodaya_data = (
                extract_antyodaya(df_antyodaya, v_id) if not df_antyodaya.empty else {}
            )
        except Exception as e:
            print(f"extract_antyodaya failed " f"for village {v_id}: {e}")
            antyodaya_data = {}

        # ----------------------------------------------
        # Final village object
        # ----------------------------------------------
        results.append(
            {
                "village_id": int(v_id),
                **soc_eco,
                "total_assets": total_assets,
                **fac_data,
                **livestock_data,
                **antyodaya_data,
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
