#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data" / "antyodaya"
DEFAULT_RAW_DIR = DATA_DIR / "raw_files" / "2020"
DEFAULT_DESCRIPTIONS = DATA_DIR / "ma_2020_raw_column_descriptions.csv"
DEFAULT_OUTPUT_DIR = DATA_DIR / "mappings"
DEFAULT_EXISTING_MAPPING = DEFAULT_OUTPUT_DIR / "antyodaya_variable_mapping.json"

# Bootstrapping source for the finalized feature/category skeleton. The output
# config is raw-only and does not retain comparison/baseline fields.
DEFAULT_FINALIZED_METADATA = (
    DEFAULT_EXISTING_MAPPING
)

ID_GROUP_COLUMNS = (
    "state_code",
    "district_code",
    "sub_district_code",
    "village_code",
)
ID_TEXT_COLUMNS = (
    "state_name",
    "district_name",
    "sub_district_name",
    "village_name",
)
ADMIN_AUDIT_COLUMNS = (
    "block_code",
    "block_name",
    "gp_code",
    "gp_name",
)

COUNT_SUM_COLUMNS = {
    "total_hhd",
    "total_shg",
    "total_hhd_mobilized_into_shg",
    "total_no_of_shg_promoted",
    "total_hhd_mobilized_into_pg",
    "total_hhd_having_bpl_cards",
    "total_hhd_availing_pension_under_nsap",
    "total_shg_accessed_bank_loans",
    "total_hhd_availing_pmjdy_bank_ac",
    "total_hhd_having_solor_wind_energy",
    "total_hhd_with_clean_energy",
    "total_hhd_with_kuccha_wall_kuccha_roof",
    "total_hhd_have_got_pmay_house",
    "total_hhd_got_benefit_under_state_housing_scheme",
    "total_hhd_in_pmay_permanent_wait_list",
    "total_hhd_availing_pmuy_benefits",
    "total_no_of_registered_children_in_anganwadi",
    "total_childs_aged_0_to_3_years",
    "total_childs_aged_0_to_3_years_reg_under_aanganwadi",
    "total_childs_aged_3_to_6_years_reg_under_aanganwadi",
    "total_childs_aged_0_to_3_years_immunized",
    "total_no_of_children_in_icds_cas",
    "total_male_child_age_bw_0_6",
    "total_female_child_age_bw_0_6",
    "total_underweight_child_age_under_6_years",
    "total_childs_categorized_non_stunted_as_per_icds",
    "total_no_of_young_anemic_children_6_59_months_in_icds_cas",
    "total_no_of_pregnant_women",
    "total_anemic_pregnant_women",
    "total_no_of_pregnant_women_receiving_services_under_icds",
    "total_no_of_lactating_mothers",
    "total_no_of_lactating_mothers_receiving_services_under_icds",
    "total_no_of_newly_born_underweight_children",
    "total_no_of_newly_born_children",
    "total_no_of_women_delivered_babies_at_hospitals_registered_asha",
    "total_no_of_beneficiaries_receiving_benefits_under_pmmvy",
    "total_no_of_eligible_beneficiaries_under_pmmvy",
    "total_hhd_registered_under_pmjay",
    "total_hhd_having_piped_water_connection",
    "total_hhd_not_having_sanitary_latrines",
    "total_hhd_engaged_cottage_small_scale_units",
    "net_sown_area_in_hac",
    "total_cultivable_area_in_hac",
    "area_irrigated_in_hac",
    "net_sown_area_kharif_in_hac",
    "net_sown_area_rabi_in_hac",
    "net_sown_area_other_in_hac",
    "total_unirrigated_land_area_in_hac",
    "no_of_farmers_using_drip_sprinkler",
    "total_no_of_farmers",
    "total_no_of_farmers_received_benefit_under_pmfby",
    "total_no_farmers_adopted_organic_farming",
    "total_no_of_farmers_registered_under_pmkpy",
    "total_no_of_farmers_add_fert_in_soil_as_per_report",
    "total_approved_labour_budget_for_year",
    "total_expenditure_approved_under_nrm_labour_budget_during_yr",
    "total_hhd_source_of_minor_forest_production",
    "total_hhd_engaged_in_farm_activities",
    "total_hhd_engaged_in_non_farm_activities",
}

COUNT_MAX_COLUMNS = {
    "gp_total_hhd_receiving_food_grains_from_fps",
    "gp_total_hhd_eligible_under_nfsa",
    "total_no_of_elect_rep_undergone_training_under_rgsa",
    "total_no_of_elected_representatives",
    "total_no_of_elect_rep_oriented_under_rgsa",
    "gp_total_no_of_eligible_beneficiaries_under_pmjay",
    "gp_total_no_of_beneficiaries_receiving_benefits_under_pmjay",
}

DISTANCE_MIN_COLUMNS: set[str] = set()

DIRECT_BINARY_PRESENCE = {
    "availability_of_panchayat_bhawan": [1],
    "is_post_office_available": [1],
    "availability_of_public_information_board": [2, 3],
    "availability_of_public_library": [1],
    "availability_of_food_storage_warehouse": [1],
    "is_bank_available": [1],
    "is_atm_available": [1],
    "is_bank_buss_correspondent_with_internet": [1],
    "availability_of_elect_supply_to_msme": [1],
    "is_village_connected_to_all_weather_road": [1],
    "availability_of_railway_station": [1],
    "availability_of_primary_school": [1],
    "availability_of_middle_school": [1],
    "availability_of_high_school": [1],
    "availability_of_jan_aushadhi_kendra": [1],
    "is_aanganwadi_centre_available": [1],
    "is_early_childhood_edu_provided_in_anganwadi": [1],
    "availability_of_mother_child_health_facilities": [1],
    "is_community_waste_disposal_system": [1],
    "is_community_biogas_waste_recycle_for_production": [1],
    "availability_of_cottage_small_scale_units": [1],
    "is_handloom": [1],
    "is_handicrafts": [1],
    "availability_of_community_forest": [1],
    "availability_of_minor_forest_production": [1],
    "is_common_pastures_available": [1],
    "availability_of_fish_farming": [1],
    "availability_of_fish_community_ponds": [1],
    "availability_of_aquaculture_ext_facility": [1],
    "availability_of_milk_routes": [1],
    "availability_of_poultry_dev_project": [1],
    "availability_of_goatary_dev_project": [1],
    "availability_of_pigery_development": [1],
    "is_veterinary_hospital_available": [1],
    "availability_of_rain_harvest_system": [1],
    "availability_of_watershed_dev_project": [1],
    "is_soil_testing_centre_available": [1],
    "is_fertilizer_shop_available": [1],
    "is_govt_seed_centre_available": [1],
    "is_bee_farming": [1],
    "is_sericulture": [1],
    "availability_of_livestock_extension_services": [1, 2],
}


def normalized_feature_id(value: str) -> str:
    if value.endswith("_cluster"):
        return value[: -len("_cluster")] + "_feature"
    return value


def legacy_feature_id(value: str) -> str:
    if value.endswith("_feature"):
        return value[: -len("_feature")] + "_cluster"
    return value


def normalized_category_column(value: str) -> str:
    if value.endswith("_index"):
        return value[: -len("_index")] + "_category"
    return value

def normalized_description(value: str) -> str:
    return (
        value.replace("clustered from", "feature from")
        .replace("Clustered from", "Feature from")
        .replace("Clustered", "Feature")
    )

PRESENCE_DERIVATIONS = {
    "public_transport": {
        "kind": "presence_flag",
        "source_column": "availability_of_public_transport",
        "presence_codes": [1, 2, 3],
        "description": "Public transport access from transport code.",
    },
}

CATEGORICAL_DERIVATIONS = {
    "availability_of_major_source_of_irrigation": {
        "kind": "categorical_one_hot",
        "code_map": {
            "1": "canal",
            "2": "surface_water",
            "3": "ground_water",
            "4": "other_irrigation",
        },
        "description": "Main irrigation source helpers; canal feeds irrigation/watershed.",
    },
    "availability_of_phc_chc": {
        "kind": "categorical_one_hot",
        "code_map": {
            "1": "phc",
            "2": "chc",
            "3": "sub_centre",
        },
        "description": "Health facility type helpers.",
    },
}

SCORE_DERIVATIONS = {
    "domestic_electricity_hours_score": {
        "kind": "ordinal_code_score",
        "source_column": "availablility_hours_of_domestic_electricity",
        "code_scores": {
            "1": 0.25,
            "2": 0.5,
            "3": 0.75,
            "4": 1.0,
            "5": 0.0,
        },
        "formula": "1-4 hrs=0.25, 4-8 hrs=0.50, 8-12 hrs=0.75, >12 hrs=1.00, no electricity=0.00",
        "description": "Domestic electricity supply-hours score derived from the 2020 electricity availability code.",
    },
    "piped_tap_water_coverage_score": {
        "kind": "ordinal_code_score",
        "source_column": "availability_of_piped_tap_water",
        "code_scores": {
            "1": 1.0,
            "2": 0.6,
            "3": 0.25,
            "4": 0.1,
            "5": 0.0,
        },
        "formula": "100% habitations=1.00, 50-100%=0.60, <50%=0.25, only one habitation=0.10, not covered=0.00",
        "description": "Piped tap water coverage score derived from the raw habitation coverage code.",
    },
    "piped_water_coverage_score": {
        "kind": "mean_mixed_score",
        "components": [
            {
                "score_column": "piped_tap_water_coverage_score",
                "label": "habitation_coverage",
            },
            {
                "numerator": "total_hhd_having_piped_water_connection",
                "denominator": "total_hhd",
                "label": "household_connection_ratio",
            },
        ],
        "formula": "mean(piped_tap_water_coverage_score, clipped(total_hhd_having_piped_water_connection / total_hhd))",
        "description": "Composite piped water coverage score using habitation coverage and household connection ratio.",
    },
    "pucca_housing_rate_score": {
        "kind": "ratio_score",
        "numerator": "total_hhd_with_kuccha_wall_kuccha_roof",
        "denominator": "total_hhd",
        "invert": True,
        "formula": "1.0 - min(1.0, total_hhd_with_kuccha_wall_kuccha_roof / total_hhd)",
        "description": "Pucca housing rate score from the inverse share of households with both kuccha wall and kuccha roof.",
    },
    "housing_scheme_coverage_score": {
        "kind": "weighted_ratio_score",
        "components": [
            {
                "numerators": [
                    "total_hhd_have_got_pmay_house",
                    "total_hhd_got_benefit_under_state_housing_scheme",
                ],
                "denominators": ["total_hhd"],
                "cap": 1.0,
                "weight": 1.0,
                "label": "pmay_and_state_housing_benefit_share",
            },
        ],
        "formula": "min(1.0, (total_hhd_have_got_pmay_house + total_hhd_got_benefit_under_state_housing_scheme) / total_hhd)",
        "description": "Housing scheme coverage score combining PMAY and state housing scheme benefits over total households.",
    },
    "pmay_demand_met_score": {
        "kind": "weighted_ratio_score",
        "components": [
            {
                "numerators": ["total_hhd_have_got_pmay_house"],
                "denominators": [
                    "total_hhd_have_got_pmay_house",
                    "total_hhd_in_pmay_permanent_wait_list",
                ],
                "cap": 1.0,
                "weight": 1.0,
                "label": "pmay_houses_over_pmay_houses_plus_waitlist",
            },
        ],
        "formula": "min(1.0, total_hhd_have_got_pmay_house / (total_hhd_have_got_pmay_house + total_hhd_in_pmay_permanent_wait_list))",
        "description": "PMAY demand-met score from PMAY houses divided by PMAY houses plus PMAY permanent waitlist.",
    },
    "ujjwala_coverage_score": {
        "kind": "ratio_score",
        "numerator": "total_hhd_availing_pmuy_benefits",
        "denominator": "total_hhd",
        "formula": "min(1.0, total_hhd_availing_pmuy_benefits / total_hhd)",
        "description": "Ujjwala coverage score from PMUY beneficiary households over total households.",
    },
    "internal_pucca_road_quality_score": {
        "kind": "ordinal_code_score",
        "source_column": "availability_of_internal_pucca_road",
        "code_scores": {
            "1": 1.0,
            "2": 0.5,
            "3": 0.0,
        },
        "formula": "fully covered=1.00, partially covered=0.50, not covered=0.00",
        "description": "Internal pucca road quality score derived from the raw coverage code.",
    },
    "drainage_quality_score": {
        "kind": "ordinal_code_score",
        "source_column": "availability_of_drainage_system",
        "code_scores": {
            "1": 1.0,
            "2": 0.75,
            "3": 0.5,
            "4": 0.25,
            "5": 0.0,
        },
        "formula": "closed=1.00, covered open=0.75, uncovered open=0.50, kuchha=0.25, no drainage=0.00",
        "description": "Drainage quality score derived from the raw drainage system code.",
    },
    "farmers_collective_score": {
        "kind": "ordinal_code_score",
        "source_column": "availability_of_fpos_pacs",
        "code_scores": {
            "1": 1.0,
            "2": 1.0,
            "3": 2.0,
            "4": 0.0,
        },
        "formula": "FPO=1, PACS=1, both FPO and PACS=2, none=0; normalized during feature scoring",
        "description": "Farmers collective strength score derived from FPO/PACS availability.",
    },
    "market_access_score": {
        "kind": "ordinal_code_score",
        "source_column": "availability_of_market",
        "code_scores": {
            "1": 1.0,
            "2": 0.75,
            "3": 0.5,
            "4": 0.0,
        },
        "formula": "mandi=1.00, regular market=0.75, weekly haat=0.50, no local market=0.00",
        "description": "Market hierarchy score from local market type only.",
    },
    "children_under_age_6": {
        "kind": "sum_columns",
        "columns": [
            "total_male_child_age_bw_0_6",
            "total_female_child_age_bw_0_6",
        ],
        "formula": "total_male_child_age_bw_0_6 + total_female_child_age_bw_0_6",
        "description": "Estimated children under age 6 from male and female child counts.",
    },
    "children_3_6_estimated": {
        "kind": "difference_score",
        "minuend": "children_under_age_6",
        "subtrahend": "total_childs_aged_0_to_3_years",
        "floor": 0.0,
        "formula": "max(0, children_under_age_6 - total_childs_aged_0_to_3_years)",
        "description": "Estimated age 3-6 child population from under-6 children minus age 0-3 children.",
    },
    "awc_0_3_coverage_score": {
        "kind": "ratio_score",
        "numerator": "total_childs_aged_0_to_3_years_reg_under_aanganwadi",
        "denominator": "total_childs_aged_0_to_3_years",
        "formula": "min(1.0, total_childs_aged_0_to_3_years_reg_under_aanganwadi / total_childs_aged_0_to_3_years)",
        "description": "AWC registration coverage among children aged 0-3.",
    },
    "awc_3_6_coverage_score": {
        "kind": "ratio_score",
        "numerator": "total_childs_aged_3_to_6_years_reg_under_aanganwadi",
        "denominator": "children_3_6_estimated",
        "formula": "min(1.0, total_childs_aged_3_to_6_years_reg_under_aanganwadi / children_3_6_estimated)",
        "description": "AWC registration coverage among estimated children aged 3-6.",
    },
    "overall_awc_enrollment_score": {
        "kind": "ratio_score",
        "numerator": "total_no_of_children_in_icds_cas",
        "denominator": "children_under_age_6",
        "formula": "min(1.0, total_no_of_children_in_icds_cas / children_under_age_6)",
        "description": "Overall ICDS CAS enrollment coverage among children under age 6.",
    },
    "awc_total_enrollment_score": {
        "kind": "ratio_score",
        "numerator": "total_no_of_registered_children_in_anganwadi",
        "denominator": "children_under_age_6",
        "formula": "min(1.0, total_no_of_registered_children_in_anganwadi / children_under_age_6)",
        "description": "Total AWC registered child coverage among children under age 6.",
    },
    "awc_infra_enrollment_coverage_score": {
        "kind": "mean_mixed_score",
        "components": [
            {"binary_column": "is_aanganwadi_centre_available", "label": "awc_available"},
            {"binary_column": "availability_of_mother_child_health_facilities", "label": "mch_facility_available"},
            {"binary_column": "is_early_childhood_edu_provided_in_anganwadi", "label": "ece_available"},
            {"score_column": "awc_0_3_coverage_score", "label": "awc_0_3_coverage"},
            {"score_column": "awc_3_6_coverage_score", "label": "awc_3_6_coverage"},
            {"score_column": "overall_awc_enrollment_score", "label": "overall_awc_enrollment"},
            {"score_column": "awc_total_enrollment_score", "label": "awc_total_enrollment"},
        ],
        "formula": "mean(awc_available, mch_facility_available, ece_available, awc_0_3_coverage_score, awc_3_6_coverage_score, overall_awc_enrollment_score, awc_total_enrollment_score)",
        "description": "Composite AWC infrastructure and enrollment coverage score. Ratio components are capped at 1.0.",
    },
    "pregnant_service_coverage_score": {
        "kind": "ratio_score",
        "numerator": "total_no_of_pregnant_women_receiving_services_under_icds",
        "denominator": "total_no_of_pregnant_women",
        "formula": "min(1.0, total_no_of_pregnant_women_receiving_services_under_icds / total_no_of_pregnant_women)",
        "description": "Pregnant women ICDS service coverage.",
    },
    "lactating_service_coverage_score": {
        "kind": "ratio_score",
        "numerator": "total_no_of_lactating_mothers_receiving_services_under_icds",
        "denominator": "total_no_of_lactating_mothers",
        "formula": "min(1.0, total_no_of_lactating_mothers_receiving_services_under_icds / total_no_of_lactating_mothers)",
        "description": "Lactating mothers ICDS service coverage.",
    },
    "maternal_anemia_rate": {
        "kind": "ratio_score",
        "numerator": "total_anemic_pregnant_women",
        "denominator": "total_no_of_pregnant_women",
        "formula": "min(1.0, total_anemic_pregnant_women / total_no_of_pregnant_women)",
        "description": "Maternal anemia rate among pregnant women.",
    },
    "maternal_anemia_score": {
        "kind": "ratio_score",
        "numerator": "total_anemic_pregnant_women",
        "denominator": "total_no_of_pregnant_women",
        "invert": True,
        "formula": "1.0 - min(1.0, total_anemic_pregnant_women / total_no_of_pregnant_women)",
        "description": "Inverse maternal anemia score, so higher means less anemia.",
    },
    "institutional_delivery_rate_score": {
        "kind": "ratio_score",
        "numerator": "total_no_of_women_delivered_babies_at_hospitals_registered_asha",
        "denominator": "total_no_of_pregnant_women",
        "formula": "min(1.0, total_no_of_women_delivered_babies_at_hospitals_registered_asha / total_no_of_pregnant_women)",
        "description": "Institutional delivery coverage among pregnant women.",
    },
    "maternal_health_care_access_score": {
        "kind": "mean_mixed_score",
        "components": [
            {"score_column": "pregnant_service_coverage_score", "label": "pregnant_service_coverage"},
            {"score_column": "lactating_service_coverage_score", "label": "lactating_service_coverage"},
            {"score_column": "maternal_anemia_score", "label": "maternal_anemia_score"},
            {"score_column": "institutional_delivery_rate_score", "label": "institutional_delivery_rate"},
        ],
        "formula": "mean(pregnant_service_coverage_score, lactating_service_coverage_score, maternal_anemia_score, institutional_delivery_rate_score)",
        "description": "Composite maternal care access score with anemia inverted so higher is better.",
    },
    "child_immunization_rate_score": {
        "kind": "ratio_score",
        "numerator": "total_childs_aged_0_to_3_years_immunized",
        "denominator": "total_childs_aged_0_to_3_years",
        "formula": "min(1.0, total_childs_aged_0_to_3_years_immunized / total_childs_aged_0_to_3_years)",
        "description": "Immunization coverage among children aged 0-3.",
    },
    "child_underweight_rate": {
        "kind": "ratio_score",
        "numerator": "total_underweight_child_age_under_6_years",
        "denominator": "children_under_age_6",
        "formula": "min(1.0, total_underweight_child_age_under_6_years / children_under_age_6)",
        "description": "Underweight rate among children under age 6.",
    },
    "child_nutrition_score": {
        "kind": "ratio_score",
        "numerator": "total_underweight_child_age_under_6_years",
        "denominator": "children_under_age_6",
        "invert": True,
        "formula": "1.0 - min(1.0, total_underweight_child_age_under_6_years / children_under_age_6)",
        "description": "Inverse underweight score, so higher means fewer underweight children.",
    },
    "stunted_children": {
        "kind": "difference_score",
        "minuend": "total_no_of_registered_children_in_anganwadi",
        "subtrahend": "total_childs_categorized_non_stunted_as_per_icds",
        "floor": 0.0,
        "formula": "max(0, total_no_of_registered_children_in_anganwadi - total_childs_categorized_non_stunted_as_per_icds)",
        "description": "Approximate stunted child count from registered children minus non-stunted children.",
    },
    "child_stunting_rate": {
        "kind": "ratio_score",
        "numerator": "stunted_children",
        "denominator": "total_no_of_registered_children_in_anganwadi",
        "formula": "min(1.0, stunted_children / total_no_of_registered_children_in_anganwadi)",
        "description": "Approximate stunting rate among registered AWC children.",
    },
    "child_stunting_score": {
        "kind": "ratio_score",
        "numerator": "stunted_children",
        "denominator": "total_no_of_registered_children_in_anganwadi",
        "invert": True,
        "formula": "1.0 - min(1.0, stunted_children / total_no_of_registered_children_in_anganwadi)",
        "description": "Inverse stunting score, so higher means fewer stunted children.",
    },
    "child_anemia_rate": {
        "kind": "ratio_score",
        "numerator": "total_no_of_young_anemic_children_6_59_months_in_icds_cas",
        "denominator": "total_no_of_children_in_icds_cas",
        "undefined": "null",
        "formula": "min(1.0, total_no_of_young_anemic_children_6_59_months_in_icds_cas / total_no_of_children_in_icds_cas)",
        "description": "Young child anemia rate among children in ICDS CAS.",
    },
    "child_anemia_score": {
        "kind": "ratio_score",
        "numerator": "total_no_of_young_anemic_children_6_59_months_in_icds_cas",
        "denominator": "total_no_of_children_in_icds_cas",
        "invert": True,
        "undefined": "null",
        "formula": "1.0 - min(1.0, total_no_of_young_anemic_children_6_59_months_in_icds_cas / total_no_of_children_in_icds_cas)",
        "description": "Inverse child anemia score, so higher means fewer anemic children.",
    },
    "child_nutrition_development_score": {
        "kind": "mean_mixed_score",
        "components": [
            {"score_column": "child_immunization_rate_score", "label": "child_immunization_rate"},
            {"score_column": "child_nutrition_score", "label": "child_nutrition_score"},
            {"score_column": "child_stunting_score", "label": "child_stunting_score"},
            {"score_column": "child_anemia_score", "label": "child_anemia_score"},
        ],
        "formula": "mean(child_immunization_rate_score, child_nutrition_score, child_stunting_score, child_anemia_score)",
        "description": "Composite child nutrition and development score. Underweight, stunting, and anemia are inverted before averaging.",
    },
    "low_birth_weight_rate": {
        "kind": "ratio_score",
        "numerator": "total_no_of_newly_born_underweight_children",
        "denominator": "total_no_of_newly_born_children",
        "undefined": "null",
        "formula": "min(1.0, total_no_of_newly_born_underweight_children / total_no_of_newly_born_children)",
        "description": "Low birth weight rate among newly born children.",
    },
    "newborn_health_score": {
        "kind": "ratio_score",
        "numerator": "total_no_of_newly_born_underweight_children",
        "denominator": "total_no_of_newly_born_children",
        "invert": True,
        "undefined": "null",
        "formula": "1.0 - min(1.0, total_no_of_newly_born_underweight_children / total_no_of_newly_born_children)",
        "description": "Newborn health score from inverse low birth weight rate.",
    },
    "matru_vandana_utilization_score": {
        "kind": "ratio_score",
        "numerator": "total_no_of_beneficiaries_receiving_benefits_under_pmmvy",
        "denominator": "total_no_of_eligible_beneficiaries_under_pmmvy",
        "formula": "min(1.0, total_no_of_beneficiaries_receiving_benefits_under_pmmvy / total_no_of_eligible_beneficiaries_under_pmmvy)",
        "description": "PMMVY benefit utilization among eligible beneficiaries.",
    },
    "health_insurance_coverage_score": {
        "kind": "ratio_score",
        "numerator": "total_hhd_registered_under_pmjay",
        "denominator": "total_hhd",
        "formula": "min(1.0, total_hhd_registered_under_pmjay / total_hhd)",
        "description": "PMJAY household registration coverage.",
    },
    "ayushman_bharat_utilization_score": {
        "kind": "ratio_score",
        "numerator": "gp_total_no_of_beneficiaries_receiving_benefits_under_pmjay",
        "denominator": "gp_total_no_of_eligible_beneficiaries_under_pmjay",
        "formula": "min(1.0, gp_total_no_of_beneficiaries_receiving_benefits_under_pmjay / gp_total_no_of_eligible_beneficiaries_under_pmjay)",
        "description": "Gram Panchayat-level PMJAY utilization among eligible beneficiaries.",
    },
    "health_schemes_utilization_score": {
        "kind": "mean_mixed_score",
        "components": [
            {"score_column": "matru_vandana_utilization_score", "label": "matru_vandana_utilization"},
            {"score_column": "health_insurance_coverage_score", "label": "health_insurance_coverage"},
            {"score_column": "ayushman_bharat_utilization_score", "label": "ayushman_bharat_utilization"},
        ],
        "formula": "mean(matru_vandana_utilization_score, health_insurance_coverage_score, ayushman_bharat_utilization_score)",
        "description": "Composite health-scheme utilization score from PMMVY and PMJAY coverage/utilization.",
    },
    "fisheries_aquaculture_score": {
        "kind": "mean_mixed_score",
        "components": [
            {
                "binary_column": "availability_of_fish_farming",
                "label": "fish_farming_available",
            },
            {
                "binary_column": "availability_of_fish_community_ponds",
                "label": "community_ponds_available",
            },
            {
                "binary_column": "availability_of_aquaculture_ext_facility",
                "label": "aquaculture_extension_facility_available",
            },
        ],
        "formula": "mean(fish_farming_available, fish_community_pond_available, aquaculture_extension_facility_available)",
        "description": "Fisheries and aquaculture score using only local activity, pond, and extension facility flags.",
    },
    "livestock_extension_services_score": {
        "kind": "ordinal_code_score",
        "source_column": "availability_of_livestock_extension_services",
        "code_scores": {
            "1": 1.0,
            "2": 1.0,
            "3": 0.0,
        },
        "formula": "1 if availability_of_livestock_extension_services is 1 or 2, else 0",
        "description": "Livestock extension services presence score: Livestock Extension Officer or PashuSakhi/Mitra available = 1, not available = 0.",
    },
    "veterinary_services_access_score": {
        "kind": "mean_mixed_score",
        "components": [
            {
                "binary_column": "is_veterinary_hospital_available",
                "label": "veterinary_hospital_available",
            },
            {
                "score_column": "livestock_extension_services_score",
                "label": "livestock_extension_service_access",
            },
        ],
        "formula": "mean(veterinary_hospital_available, livestock_extension_services_score)",
        "description": "Veterinary services score combining clinic/hospital availability and livestock extension support.",
    },
    "land_utilization_intensity_score": {
        "kind": "weighted_ratio_score",
        "components": [
            {
                "numerators": ["net_sown_area_in_hac"],
                "denominators": ["total_cultivable_area_in_hac"],
                "cap": 1.0,
                "weight": 0.6,
                "label": "net_sown_share_of_cultivable_area",
            },
            {
                "numerators": [
                    "net_sown_area_kharif_in_hac",
                    "net_sown_area_rabi_in_hac",
                    "net_sown_area_other_in_hac",
                ],
                "denominators": ["total_cultivable_area_in_hac"],
                "cap": 3.0,
                "weight": 0.4,
                "label": "seasonal_sown_intensity_over_cultivable_area",
            },
        ],
        "formula": "0.60 * min(net_sown / cultivable, 1) + 0.40 * min((kharif + rabi + other) / cultivable, 3) / 3",
        "description": "Composite land utilization score combining net sown coverage with multi-season use of cultivable area.",
    },
    "irrigation_coverage_score": {
        "kind": "weighted_ratio_score",
        "components": [
            {
                "numerators": ["area_irrigated_in_hac"],
                "denominators": ["total_cultivable_area_in_hac"],
                "cap": 1.0,
                "weight": 0.5,
                "label": "irrigated_share_of_cultivable_area",
            },
            {
                "numerators": ["total_unirrigated_land_area_in_hac"],
                "denominators": ["total_cultivable_area_in_hac"],
                "cap": 1.0,
                "invert": True,
                "weight": 0.5,
                "label": "inverse_unirrigated_share_of_cultivable_area",
            },
        ],
        "formula": "0.50 * min(irrigated / cultivable, 1) + 0.50 * (1 - min(unirrigated / cultivable, 1))",
        "description": "Composite irrigation score treating irrigated land as positive coverage and unirrigated land as an inverted additive signal.",
    },
    "seasonal_cropping_intensity_score": {
        "kind": "weighted_ratio_score",
        "components": [
            {
                "numerator_terms": [
                    {"column": "net_sown_area_kharif_in_hac", "multiplier": 1.0},
                    {"column": "net_sown_area_rabi_in_hac", "multiplier": 2.0},
                    {"column": "net_sown_area_other_in_hac", "multiplier": 3.0},
                ],
                "denominators": ["net_sown_area_in_hac"],
                "cap": 3.0,
                "weight": 1.0,
                "label": "weighted_seasonal_sown_area_over_net_sown_area",
            },
        ],
        "formula": "min((kharif + (2 * rabi) + (3 * other)) / net_sown, 3) / 3",
        "description": "Seasonal cropping intensity score using weighted Kharif, Rabi, and other-season sown area over net sown area.",
    },
    "cropping_intensity": {
        "kind": "weighted_ratio_score",
        "components": [
            {
                "numerator_terms": [
                    {"column": "net_sown_area_kharif_in_hac", "multiplier": 1.0},
                    {"column": "net_sown_area_rabi_in_hac", "multiplier": 2.0},
                    {"column": "net_sown_area_other_in_hac", "multiplier": 3.0},
                ],
                "denominators": ["net_sown_area_in_hac"],
                "cap": 3.0,
                "weight": 1.0,
                "label": "weighted_seasonal_sown_area_over_net_sown_area",
            },
        ],
        "formula": "min((kharif + (2 * rabi) + (3 * other)) / net_sown, 3) / 3",
        "description": "Seasonal cropping intensity score using weighted Kharif, Rabi, and other-season sown area over net sown area.",
    },
    "land_utilization_feature_score": {
        "kind": "mean_mixed_score",
        "components": [
            {
                "numerator": "net_sown_area_in_hac",
                "denominator": "total_cultivable_area_in_hac",
                "label": "land_cultivation_rate",
            },
            {
                "numerator": "area_irrigated_in_hac",
                "denominator": "total_cultivable_area_in_hac",
                "label": "irrigation_coverage",
            },
            {
                "score_column": "cropping_intensity",
                "label": "cropping_intensity",
            },
        ],
        "formula": "mean(clipped(net_sown_area_in_hac / total_cultivable_area_in_hac), clipped(area_irrigated_in_hac / total_cultivable_area_in_hac), cropping_intensity)",
        "description": "Composite agriculture land cultivation feature score from land cultivation rate, irrigation coverage, and seasonal cropping intensity.",
    },
    "agri_inputs_availability_score": {
        "kind": "mean_mixed_score",
        "components": [
            {
                "binary_column": "is_soil_testing_centre_available",
                "label": "soil_testing_centre_available",
            },
            {
                "binary_column": "is_fertilizer_shop_available",
                "label": "fertilizer_shop_available",
            },
            {
                "binary_column": "is_govt_seed_centre_available",
                "label": "government_seed_centre_available",
            },
        ],
        "formula": "mean(soil_testing_centre_available, fertilizer_shop_available, government_seed_centre_available)",
        "description": "Agricultural input availability score using only local soil-testing, fertilizer-shop, and government seed-centre availability flags.",
    },
    "agri_risk_support_score": {
        "kind": "mean_ratio_score",
        "components": [
            {
                "numerator": "total_no_of_farmers_registered_under_pmkpy",
                "denominator": "total_no_of_farmers",
                "label": "pmkpy_registration_coverage",
            },
            {
                "numerator": "total_no_of_farmers_received_benefit_under_pmfby",
                "denominator": "total_no_of_farmers",
                "label": "crop_insurance_coverage",
            },
        ],
        "formula": "mean(clipped(total_no_of_farmers_registered_under_pmkpy / total_no_of_farmers), clipped(total_no_of_farmers_received_benefit_under_pmfby / total_no_of_farmers))",
        "description": "Agricultural risk-support score combining PMKPY registration and PMFBY benefit coverage.",
    },
}


@dataclass(frozen=True)
class RawDescription:
    description: str = ""
    sector: str = ""
    data_type: str = ""
    value_codes: str = ""
    unit: str = ""


def slugify(text: str) -> str:
    text = text.replace("&", " and ")
    text = text.replace("/", " ")
    text = text.replace("-", " ")
    text = text.replace("\n", " ")
    text = re.sub(r"[^A-Za-z0-9]+", "_", text.strip().lower())
    return text.strip("_")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create the raw-column mapping/config for the Antyodaya 2020 clustering flow."
    )
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--column-descriptions", type=Path, default=DEFAULT_DESCRIPTIONS)
    parser.add_argument("--finalized-metadata", type=Path, default=DEFAULT_FINALIZED_METADATA)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def load_raw_descriptions(path: Path) -> dict[str, RawDescription]:
    descriptions: dict[str, RawDescription] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            column = (row.get("column_name") or "").strip()
            if not column:
                continue
            descriptions[column] = RawDescription(
                description=(row.get("description") or "").strip(),
                sector=(row.get("sector") or "").strip(),
                data_type=(row.get("data_type") or "").strip(),
                value_codes=(row.get("value_codes") or "").strip(),
                unit=(row.get("unit") or "").strip(),
            )
    return descriptions


def read_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return next(csv.reader(handle))


def raw_header_profile(raw_dir: Path) -> dict[str, Any]:
    paths = sorted(raw_dir.glob("*.csv"))
    union: set[str] = set()

    for path in paths:
        header = read_header(path)
        union.update(header)

    return {
        "raw_file_count": len(paths),
        "header_union": sorted(union),
    }


def load_finalized_metadata(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates = [
        path,
        DEFAULT_EXISTING_MAPPING,
        REPO_ROOT
        / "data"
        / "antyodaya_v2"
        / "output"
        / "antyodaya_village_reprocessing_metadata.json",
    ]
    source_path = next((candidate for candidate in candidates if candidate.exists()), None)
    if source_path is None:
        raise FileNotFoundError(
            "No feature/category skeleton found. Expected one of: "
            + ", ".join(str(candidate) for candidate in candidates)
        )
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    if "feature_cluster_columns" in payload and "category_cluster_columns" in payload:
        return payload["feature_cluster_columns"], payload["category_cluster_columns"]
    if "features" in payload and "categories" in payload:
        feature_rows = []
        for feature in payload["features"]:
            feature_rows.append(
                {
                    "feature_id": feature["feature_id"],
                    "normalized_output_column": feature["feature_column"],
                    "display_name": feature["display_name"],
                    "category_id": feature["category_id"],
                    "method": feature["method"],
                    "inverse": feature.get("inverse", False),
                    "input_columns": feature["input_variables"],
                    "description": feature.get("description", ""),
                }
            )
        category_rows = []
        for category in payload["categories"]:
            category_rows.append(
                {
                    "category_id": category["category_id"],
                    "normalized_output_column": category["category_column"],
                    "display_name": category["display_name"],
                    "legacy_output_column": category["index_column"],
                    "feature_ids": category["feature_ids"],
                }
            )
        return feature_rows, category_rows
    raise ValueError(f"Unsupported feature/category skeleton format: {source_path}")


def raw_dependencies_for_variable(variable: str) -> set[str]:
    if variable in PRESENCE_DERIVATIONS:
        return {PRESENCE_DERIVATIONS[variable]["source_column"]}
    if variable in SCORE_DERIVATIONS:
        seen: set[str] = set()
        deps: set[str] = set()
        definition = SCORE_DERIVATIONS[variable]
        if "source_column" in SCORE_DERIVATIONS[variable]:
            deps.add(SCORE_DERIVATIONS[variable]["source_column"])
        if "distance_column" in SCORE_DERIVATIONS[variable]:
            deps.add(SCORE_DERIVATIONS[variable]["distance_column"])
        for key in ("numerator", "denominator", "minuend", "subtrahend"):
            dependency = definition.get(key)
            if dependency:
                if dependency in SCORE_DERIVATIONS and dependency not in seen:
                    seen.add(dependency)
                    deps.update(raw_dependencies_for_variable(dependency))
                else:
                    deps.add(dependency)
        deps.update(definition.get("columns", []))
        for component in definition.get("components", []):
            for key in ("availability_column", "distance_column", "numerator", "denominator", "binary_column"):
                if key in component:
                    dependency = component[key]
                    if dependency in SCORE_DERIVATIONS and dependency not in seen:
                        seen.add(dependency)
                        deps.update(raw_dependencies_for_variable(dependency))
                    else:
                        deps.add(dependency)
            for key in ("numerators", "denominators"):
                deps.update(component.get(key, []))
            for key in ("numerator_terms", "denominator_terms"):
                deps.update(term["column"] for term in component.get(key, []))
            score_column = component.get("score_column")
            if score_column and score_column in SCORE_DERIVATIONS and score_column not in seen:
                seen.add(score_column)
                deps.update(raw_dependencies_for_variable(score_column))
        return deps
    for source_column, definition in CATEGORICAL_DERIVATIONS.items():
        if variable in set(definition["code_map"].values()):
            return {source_column}
    return {variable}


def derived_dependencies_for_variable(variable: str) -> set[str]:
    if variable in PRESENCE_DERIVATIONS or variable in SCORE_DERIVATIONS:
        deps = {variable}
        for component in SCORE_DERIVATIONS.get(variable, {}).get("components", []):
            score_column = component.get("score_column")
            if score_column and score_column in SCORE_DERIVATIONS:
                deps.update(derived_dependencies_for_variable(score_column))
        for key in ("numerator", "denominator", "minuend", "subtrahend"):
            dependency = SCORE_DERIVATIONS.get(variable, {}).get(key)
            if dependency and dependency in SCORE_DERIVATIONS:
                deps.update(derived_dependencies_for_variable(dependency))
        return deps
    for definition in CATEGORICAL_DERIVATIONS.values():
        if variable in set(definition["code_map"].values()):
            return {variable}
    return set()


def raw_dependencies_for_feature(input_columns: list[str]) -> set[str]:
    deps: set[str] = set()
    for variable in input_columns:
        deps.update(raw_dependencies_for_variable(variable))
    return deps


def derived_dependencies_for_feature(input_columns: list[str]) -> set[str]:
    deps: set[str] = set()
    for variable in input_columns:
        deps.update(derived_dependencies_for_variable(variable))
    return deps


def raw_aggregation(column: str) -> str:
    presence_source_columns = {definition["source_column"] for definition in PRESENCE_DERIVATIONS.values()}
    score_input_columns = set()
    for definition in SCORE_DERIVATIONS.values():
        for key in ("source_column", "distance_column"):
            if key in definition:
                score_input_columns.add(definition[key])
        for key in ("numerator", "denominator", "minuend", "subtrahend"):
            dependency = definition.get(key)
            if dependency and dependency not in SCORE_DERIVATIONS:
                score_input_columns.add(dependency)
        score_input_columns.update(definition.get("columns", []))
        for component in definition.get("components", []):
            for key in ("availability_column", "distance_column", "numerator", "denominator", "binary_column"):
                dependency = component.get(key)
                if dependency and dependency not in SCORE_DERIVATIONS:
                    score_input_columns.add(dependency)
            for key in ("numerators", "denominators"):
                score_input_columns.update(component.get(key, []))
            for key in ("numerator_terms", "denominator_terms"):
                score_input_columns.update(term["column"] for term in component.get(key, []))
    if column in ID_GROUP_COLUMNS:
        return "group_by"
    if column in ID_TEXT_COLUMNS:
        return "first_text_within_group"
    if column in ADMIN_AUDIT_COLUMNS:
        return "audit_only"
    if column in COUNT_SUM_COLUMNS:
        return "sum"
    if column in COUNT_MAX_COLUMNS:
        return "max"
    if column in DISTANCE_MIN_COLUMNS:
        return "min_distance_code"
    if column in DIRECT_BINARY_PRESENCE:
        return "presence_flag_then_max"
    if column in presence_source_columns:
        return "min_categorical_code_then_presence_flag"
    if column in CATEGORICAL_DERIVATIONS:
        return "min_categorical_code_then_derive"
    if column in score_input_columns:
        return "row_level_score_input"
    return "not_used_by_current_mapping"


def sorted_unique(values: set[str] | list[str] | tuple[str, ...]) -> list[str]:
    return sorted(set(values))


def build_feature_config(feature_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    skipped_feature_ids = {
        "open_kuchha_drain_cluster",
        "closed_drainage_cluster",
        "open_uncov_drain_cluster",
        "open_covered_drain_cluster",
        "piped_water_access_cluster",
        "piped_water_fully_covered_cluster",
        "non_farm_employment_cluster",
        "minority_credit_cluster",
        "pmay_coverage_cluster",
        "irrigation_coverage_cluster",
        "cropping_intensity_cluster",
        "children_3_6_registered_cluster",
        "awc_available_cluster",
        "early_child_education_cluster",
        "awc_0_3_coverage_cluster",
        "overall_awc_enrollment_cluster",
        "maternal_anemia_cluster",
        "pregnant_service_cov_cluster",
        "lactating_service_cov_cluster",
        "low_birth_weight_cluster",
        "institutional_delivery_cluster",
        "child_immunization_cluster",
        "matru_vandana_cluster",
        "mother_child_health_facility_access_cluster",
        "land_cultivation_rate",
        "irrigation_coverage",
        "cropping_intensity",
    }
    features = []
    seen_feature_ids: set[str] = set()
    for row in feature_rows:
        source_feature_id = row["feature_id"]
        legacy_id = legacy_feature_id(source_feature_id)
        feature_id = normalized_feature_id(source_feature_id)
        if legacy_id in skipped_feature_ids:
            continue
        if feature_id in seen_feature_ids:
            continue
        seen_feature_ids.add(feature_id)
        input_columns = list(row["input_columns"])
        method = row["method"]
        if method == "additive":
            method = "composite_binary_share"
        inverse = bool(row.get("inverse", False))
        description = normalized_description(row.get("description", ""))
        display_name = row["display_name"]
        category_id = row["category_id"]
        if category_id == "livelihoods_cottage_traditional":
            category_id = "livelihoods_cottage_traditional_industry"
        if legacy_id == "modern_irrigation_cluster":
            category_id = "agriculture_irrigation_watershed"
            display_name = "Modern Irrigation"
            description = (
                "Modern Irrigation feature from farmers using drip/sprinkler "
                "over total farmers, as part of Agriculture Irrigation and Watershed."
            )
        if legacy_id == "electrification_rate_cluster":
            input_columns = ["domestic_electricity_hours_score"]
            method = "score"
            inverse = False
            description = "Electrification Rate feature from the 0-1 ordinal domestic electricity supply-hours score."
        elif legacy_id == "post_office_cluster":
            input_columns = ["is_post_office_available"]
            method = "binary"
            inverse = False
            display_name = "Post Office Availability"
            description = "Post Office Availability feature from the local availability flag."
        elif legacy_id == "public_library_cluster":
            input_columns = ["availability_of_public_library"]
            method = "binary"
            inverse = False
            display_name = "Public Library Availability"
            description = "Public Library Availability feature from the local availability flag."
        elif legacy_id == "bank_cluster":
            input_columns = ["is_bank_available"]
            method = "binary"
            inverse = False
            display_name = "Bank Availability"
            description = "Bank Availability feature from the local availability flag."
        elif legacy_id == "atm_cluster":
            input_columns = ["is_atm_available"]
            method = "binary"
            inverse = False
            display_name = "ATM Availability"
            description = "ATM Availability feature from the local availability flag."
        elif legacy_id == "all_weather_road_cluster":
            input_columns = ["is_village_connected_to_all_weather_road"]
            method = "binary"
            inverse = False
            display_name = "All-Weather Road Connection"
            description = "All-Weather Road Connection feature from the village connection flag."
        elif legacy_id == "public_transport_cluster":
            input_columns = ["public_transport"]
            method = "score"
            inverse = False
            display_name = "Public Transport Availability"
            description = "Public Transport Availability feature from bus/van/taxi transport codes."
        elif legacy_id == "railway_station_cluster":
            input_columns = ["availability_of_railway_station"]
            method = "binary"
            inverse = False
            display_name = "Railway Station Availability"
            description = "Railway Station Availability feature from the local station availability flag."
        elif legacy_id == "pucca_housing_rate_cluster":
            input_columns = ["pucca_housing_rate_score"]
            method = "score"
            inverse = False
            display_name = "Pucca Housing Rate"
            description = "Pucca Housing Rate feature from inverse kuccha wall and kuccha roof household share."
        elif legacy_id == "pmay_demand_met_cluster":
            input_columns = ["pmay_demand_met_score"]
            method = "score"
            inverse = False
            display_name = "PMAY Demand Met"
            description = "PMAY Demand Met feature from PMAY houses divided by PMAY houses plus PMAY permanent waitlist."
        elif legacy_id == "ujjwala_coverage_cluster":
            input_columns = ["ujjwala_coverage_score"]
            method = "score"
            inverse = False
            display_name = "Ujjwala Coverage"
            description = "Ujjwala Coverage feature from PMUY beneficiary households over total households."
        elif legacy_id == "fpo_cluster":
            input_columns = ["farmers_collective_score"]
            method = "score"
            inverse = False
            display_name = "FPO/PACS Strength"
            description = "FPO/PACS Strength feature from the farmers collective score: none=0, one collective=1, both=2."
        elif legacy_id == "internal_pucca_road_cluster":
            input_columns = ["internal_pucca_road_quality_score"]
            method = "score"
            inverse = False
            display_name = "Internal Pucca Road Quality"
            description = "Internal Pucca Road Quality feature from fully/partially/not-covered road quality score."
        elif legacy_id == "awc_available_cluster":
            input_columns = ["is_aanganwadi_centre_available"]
            method = "binary"
            inverse = False
            display_name = "Anganwadi Centre Availability"
            description = "Anganwadi Centre Availability feature from the local availability flag."
        elif legacy_id == "piped_water_coverage_cluster":
            input_columns = ["piped_water_coverage_score"]
            method = "score"
            inverse = False
            display_name = "Piped Water Coverage"
            description = "Piped Water Coverage feature from habitation coverage and household connection ratio."
        elif legacy_id == "market_access_cluster":
            input_columns = ["market_access_score"]
            method = "score"
            inverse = False
            description = "Market Access feature from local market type only."
        elif legacy_id == "food_storage_cluster":
            input_columns = ["availability_of_food_storage_warehouse"]
            method = "binary"
            inverse = False
            display_name = "Food Storage Availability"
            description = "Food Storage Availability feature from the local warehouse availability flag."
        elif legacy_id == "fisheries_aquaculture_cluster":
            input_columns = ["fisheries_aquaculture_score"]
            method = "score"
            inverse = False
            display_name = "Fisheries and Aquaculture Access"
            description = "Fisheries and Aquaculture Access feature from fish farming, community ponds, and aquaculture extension access."
        elif legacy_id == "veterinary_services_cluster":
            input_columns = ["veterinary_services_access_score"]
            method = "score"
            inverse = False
            display_name = "Veterinary Services Access"
            description = "Veterinary Services Access feature from veterinary hospital availability and livestock extension support."
        elif legacy_id == "land_utilization_cluster":
            input_columns = ["land_utilization_feature_score"]
            method = "score"
            inverse = False
            display_name = "Land Utilization"
            description = "Land Utilization feature from land cultivation rate, irrigation coverage, and seasonal cropping intensity."
        elif legacy_id == "irrigation_coverage_cluster":
            input_columns = ["area_irrigated_in_hac", "total_cultivable_area_in_hac"]
            method = "ratio"
            inverse = False
            display_name = "Irrigation Coverage"
            description = "Irrigation Coverage feature from irrigated area divided by total cultivable area."
        elif legacy_id == "cropping_intensity_cluster":
            input_columns = ["seasonal_cropping_intensity_score"]
            method = "score"
            inverse = False
            display_name = "Seasonal Cropping Intensity"
            description = "Seasonal Cropping Intensity feature from Kharif, Rabi, and other-season sown area over net sown area."
            category_id = "agriculture_land_cultivation"
        elif legacy_id == "agri_inputs_availability_cluster":
            input_columns = ["agri_inputs_availability_score"]
            method = "score"
            inverse = False
            display_name = "Agricultural Inputs Availability"
            description = "Agricultural Inputs Availability feature from local soil-testing, fertilizer-shop, and government seed-centre availability flags."
        elif legacy_id == "mother_child_health_facility_access_cluster":
            input_columns = ["availability_of_mother_child_health_facilities"]
            method = "binary"
            inverse = False
            display_name = "Mother-Child Health Facility Availability"
            description = "Mother-Child Health Facility Availability feature from the local availability flag."
        feature = {
                "feature_id": feature_id,
                "feature_column": feature_id,
                "display_name": display_name,
                "category_id": category_id,
                "method": method,
                "method_label": method,
                "inverse": inverse,
                "input_variables": input_columns,
                "raw_dependencies": sorted_unique(raw_dependencies_for_feature(input_columns)),
                "derived_dependencies": sorted_unique(derived_dependencies_for_feature(input_columns)),
                "description": description,
            }
        features.append(feature)
    if not any(feature["feature_id"] == "drainage_quality_feature" for feature in features):
        features.append(
            {
                "feature_id": "drainage_quality_feature",
                "feature_column": "drainage_quality_feature",
                "display_name": "Drainage Quality",
                "category_id": "water_sanitation",
                "method": "score",
                "method_label": "score",
                "inverse": False,
                "input_variables": ["drainage_quality_score"],
                "raw_dependencies": sorted_unique(raw_dependencies_for_feature(["drainage_quality_score"])),
                "derived_dependencies": ["drainage_quality_score"],
                "description": "Drainage Quality feature from closed/open/kuchha/no-drainage ordinal score.",
            }
        )
    if not any(feature["feature_id"] == "piped_water_coverage_feature" for feature in features):
        features.append(
            {
                "feature_id": "piped_water_coverage_feature",
                "feature_column": "piped_water_coverage_feature",
                "display_name": "Piped Water Coverage",
                "category_id": "water_sanitation",
                "method": "score",
                "method_label": "score",
                "inverse": False,
                "input_variables": ["piped_water_coverage_score"],
                "raw_dependencies": sorted_unique(raw_dependencies_for_feature(["piped_water_coverage_score"])),
                "derived_dependencies": sorted_unique(derived_dependencies_for_feature(["piped_water_coverage_score"])),
                "description": "Piped Water Coverage feature from habitation coverage and household connection ratio.",
            }
        )
    maternal_child_health_features = [
        (
            "awc_infra_enrollment_coverage_feature",
            "AWC Infrastructure and Enrollment Coverage",
            ["awc_infra_enrollment_coverage_score"],
            "Composite feature from AWC availability, MCH facility availability, early-childhood education availability, 0-3 AWC coverage, estimated 3-6 AWC coverage, ICDS CAS enrollment, and total AWC enrollment.",
        ),
        (
            "maternal_health_care_access_feature",
            "Maternal Health and Care Access",
            ["maternal_health_care_access_score"],
            "Composite feature from pregnant-service coverage, lactating-service coverage, inverse maternal anemia, and institutional delivery coverage.",
        ),
        (
            "child_nutrition_development_feature",
            "Child Nutrition and Development",
            ["child_nutrition_development_score"],
            "Composite feature from child immunization, inverse underweight rate, inverse stunting rate, and inverse child anemia rate.",
        ),
        (
            "newborn_health_outcomes_feature",
            "Newborn Health Outcomes",
            ["newborn_health_score"],
            "Feature from inverse low-birth-weight rate among newly born children.",
        ),
        (
            "health_schemes_utilization_feature",
            "Health Schemes Utilization",
            ["health_schemes_utilization_score"],
            "Composite feature from PMMVY utilization, PMJAY household coverage, and GP-level Ayushman Bharat utilization.",
        ),
    ]
    existing_feature_ids = {feature["feature_id"] for feature in features}
    for feature_id, display_name, input_variables, description in maternal_child_health_features:
        if feature_id in existing_feature_ids:
            continue
        features.append(
            {
                "feature_id": feature_id,
                "feature_column": feature_id,
                "display_name": display_name,
                "category_id": "maternal_child_health",
                "method": "score",
                "method_label": "score",
                "inverse": False,
                "input_variables": input_variables,
                "raw_dependencies": sorted_unique(raw_dependencies_for_feature(input_variables)),
                "derived_dependencies": sorted_unique(derived_dependencies_for_feature(input_variables)),
                "description": description,
            }
        )
    if not any(feature["feature_id"] == "housing_scheme_coverage_feature" for feature in features):
        features.append(
            {
                "feature_id": "housing_scheme_coverage_feature",
                "feature_column": "housing_scheme_coverage_feature",
                "display_name": "Housing Scheme Coverage",
                "category_id": "housing_quality",
                "method": "score",
                "method_label": "score",
                "inverse": False,
                "input_variables": ["housing_scheme_coverage_score"],
                "raw_dependencies": sorted_unique(raw_dependencies_for_feature(["housing_scheme_coverage_score"])),
                "derived_dependencies": sorted_unique(derived_dependencies_for_feature(["housing_scheme_coverage_score"])),
                "description": "Housing Scheme Coverage feature from PMAY plus state housing scheme beneficiary households over total households.",
            }
        )
    if not any(feature["feature_id"] == "waste_disposal_feature" for feature in features):
        features.append(
            {
                "feature_id": "waste_disposal_feature",
                "feature_column": "waste_disposal_feature",
                "display_name": "Community Waste Disposal",
                "category_id": "water_sanitation",
                "method": "binary",
                "method_label": "binary",
                "inverse": False,
                "input_variables": ["is_community_waste_disposal_system"],
                "raw_dependencies": ["is_community_waste_disposal_system"],
                "derived_dependencies": [],
                "description": "Community Waste Disposal feature from the binary community waste disposal system flag.",
            }
        )
    return features


def build_category_config(category_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    categories = []
    for row in category_rows:
        if row["category_id"] in {
            "agriculture_seasonal_cultivation",
            "agriculture_modern_irrigation",
        }:
            continue
        feature_ids = [normalized_feature_id(feature_id) for feature_id in row["feature_ids"]]
        if row["category_id"] == "water_sanitation":
            drainage_ids = {
                normalized_feature_id("open_kuchha_drain_cluster"),
                normalized_feature_id("closed_drainage_cluster"),
                normalized_feature_id("open_uncov_drain_cluster"),
                normalized_feature_id("open_covered_drain_cluster"),
            }
            piped_ids = {
                normalized_feature_id("piped_water_access_cluster"),
                normalized_feature_id("piped_water_fully_covered_cluster"),
            }
            revised_feature_ids = []
            inserted = False
            piped_inserted = False
            for feature_id in feature_ids:
                if feature_id in piped_ids:
                    if not piped_inserted:
                        revised_feature_ids.append("piped_water_coverage_feature")
                        piped_inserted = True
                    continue
                if feature_id in drainage_ids:
                    if not inserted:
                        revised_feature_ids.append("drainage_quality_feature")
                        inserted = True
                    continue
                revised_feature_ids.append(feature_id)
            feature_ids = revised_feature_ids
            if "waste_disposal_feature" not in feature_ids:
                insert_after = (
                    feature_ids.index("drainage_quality_feature") + 1
                    if "drainage_quality_feature" in feature_ids
                    else len(feature_ids)
                )
                feature_ids.insert(insert_after, "waste_disposal_feature")
        if row["category_id"] == "livelihoods_employment":
            feature_ids = [
                feature_id
                for feature_id in feature_ids
                if feature_id != normalized_feature_id("non_farm_employment_cluster")
            ]
        if row["category_id"] == "financial_inclusion":
            feature_ids = [
                feature_id
                for feature_id in feature_ids
                if feature_id != normalized_feature_id("minority_credit_cluster")
            ]
        if row["category_id"] == "maternal_child_health":
            feature_ids = [
                "awc_infra_enrollment_coverage_feature",
                "maternal_health_care_access_feature",
                "child_nutrition_development_feature",
                "newborn_health_outcomes_feature",
                "health_schemes_utilization_feature",
            ]
        if row["category_id"] == "housing_quality":
            feature_ids = [
                "pucca_housing_rate_feature",
                "housing_scheme_coverage_feature",
                "pmay_demand_met_feature",
                "ujjwala_coverage_feature",
            ]
        display_name = row["display_name"]
        category_id = row["category_id"]
        if category_id == "livelihoods_cottage_traditional":
            category_id = "livelihoods_cottage_traditional_industry"
        if category_id == "livelihoods_employment":
            display_name = "Farm Employment"
        if row["category_id"] == "agriculture_land_cultivation":
            display_name = "Agriculture Land Cultivation"
            feature_ids = ["land_utilization_feature"]
        if row["category_id"] == "agriculture_irrigation_watershed":
            display_name = "Agriculture Irrigation and Watershed"
            if "modern_irrigation_feature" not in feature_ids:
                feature_ids.append("modern_irrigation_feature")
        category_column = normalized_category_column(row["normalized_output_column"])
        if category_id == "livelihoods_cottage_traditional_industry":
            category_column = "livelihoods_cottage_traditional_industry_category"
        category = {
            "category_id": category_id,
            "category_column": category_column,
            "display_name": display_name,
            "index_column": row["legacy_output_column"],
            "feature_ids": feature_ids,
        }
        if category_id in {
            "livelihoods_cottage_traditional_industry",
            "livelihoods_forest_resources",
            "livelihoods_alternative_farming",
        }:
            category["category_cluster_rule"] = {
                "n_clusters": 2,
                "reason": "Final category intentionally uses Low/High because the middle class did not add a clear qualitative distinction.",
            }
        categories.append(
            category
        )
    return categories


def build_variable_to_flow(
    features: list[dict[str, Any]], categories: list[dict[str, Any]]
) -> dict[str, dict[str, set[str]]]:
    category_by_feature: dict[str, set[str]] = defaultdict(set)
    category_name_by_feature: dict[str, set[str]] = defaultdict(set)
    for category in categories:
        for feature_id in category["feature_ids"]:
            category_by_feature[feature_id].add(category["category_id"])
            category_name_by_feature[feature_id].add(category["display_name"])

    flow: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for feature in features:
        feature_id = feature["feature_id"]
        for raw_column in feature["raw_dependencies"]:
            flow[raw_column]["features"].add(feature_id)
            flow[raw_column]["categories"].update(category_by_feature[feature_id])
            flow[raw_column]["category_names"].update(category_name_by_feature[feature_id])
        for variable in feature["derived_dependencies"]:
            flow[variable]["features"].add(feature_id)
            flow[variable]["categories"].update(category_by_feature[feature_id])
            flow[variable]["category_names"].update(category_name_by_feature[feature_id])
    return flow


def build_derivation_config(used_derived_variables: set[str]) -> dict[str, Any]:
    presence_flags = {
        variable: definition
        for variable, definition in PRESENCE_DERIVATIONS.items()
        if variable in used_derived_variables
    }
    categorical_variables = {}
    for source_column, definition in CATEGORICAL_DERIVATIONS.items():
        for code, variable in definition["code_map"].items():
            if variable not in used_derived_variables:
                continue
            categorical_variables[variable] = {
                "kind": "categorical_flag",
                "source_column": source_column,
                "match_code": int(code),
                "description": definition["description"],
            }

    return {
        "presence_flags": presence_flags,
        "categorical_flags": categorical_variables,
        "scores": {
            variable: definition
            for variable, definition in SCORE_DERIVATIONS.items()
            if variable in used_derived_variables
        },
    }


def build_raw_column_rows(
    descriptions: dict[str, RawDescription],
    header_profile: dict[str, Any],
    features: list[dict[str, Any]],
    categories: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    flow = build_variable_to_flow(features, categories)
    header_union = set(header_profile["header_union"])
    all_columns = sorted(set(descriptions) | header_union)
    required_raw = set(ID_GROUP_COLUMNS) | set(ID_TEXT_COLUMNS) | set(ADMIN_AUDIT_COLUMNS)
    for feature in features:
        required_raw.update(feature["raw_dependencies"])

    rows = []
    for column in all_columns:
        desc = descriptions.get(column, RawDescription())
        column_flow = flow[column]
        roles = []
        if column in ID_GROUP_COLUMNS:
            roles.append("group_key")
        if column in ID_TEXT_COLUMNS:
            roles.append("display_identifier")
        if column in ADMIN_AUDIT_COLUMNS:
            roles.append("admin_audit")
        if column in required_raw:
            roles.append("feature_dependency")
        if column in DIRECT_BINARY_PRESENCE:
            roles.append("direct_binary_presence")
        if column in CATEGORICAL_DERIVATIONS:
            roles.append("categorical_source")
        for variable, definition in PRESENCE_DERIVATIONS.items():
            if definition["source_column"] == column:
                roles.append(f"source_for:{variable}")
        for variable, definition in SCORE_DERIVATIONS.items():
            if definition.get("source_column") == column:
                roles.append(f"source_for:{variable}")
            if definition.get("distance_column") == column:
                roles.append(f"source_for:{variable}")
            for component in definition.get("components", []):
                if column in component.values():
                    roles.append(f"source_for:{variable}")
                for key in ("numerators", "denominators"):
                    if column in component.get(key, []):
                        roles.append(f"source_for:{variable}")
                for key in ("numerator_terms", "denominator_terms"):
                    if column in {term["column"] for term in component.get(key, [])}:
                        roles.append(f"source_for:{variable}")

        rows.append(
            {
                "column_name": column,
                "description": desc.description,
                "sector": desc.sector,
                "data_type": desc.data_type,
                "value_codes": desc.value_codes,
                "unit": desc.unit,
                "in_raw_2020_files": column in header_union,
                "used_in_current_mapping": column in required_raw,
                "aggregation": raw_aggregation(column),
                "roles": sorted_unique(roles),
                "flows_to_features": sorted_unique(column_flow["features"]),
                "flows_to_categories": sorted_unique(column_flow["categories"]),
                "flows_to_category_names": sorted_unique(column_flow["category_names"]),
            }
        )
    return rows


def json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, set):
        return sorted(value)
    raise TypeError(f"Unsupported JSON value: {type(value)!r}")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=json_default) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            serialized = {}
            for key, value in row.items():
                if isinstance(value, (list, dict)):
                    serialized[key] = json.dumps(value, ensure_ascii=False, sort_keys=True)
                else:
                    serialized[key] = value
            writer.writerow(serialized)


def feature_category_rows(features: list[dict[str, Any]], categories: list[dict[str, Any]]) -> list[dict[str, Any]]:
    features_by_id = {feature["feature_id"]: feature for feature in features}
    rows = []
    for category in categories:
        for order, feature_id in enumerate(category["feature_ids"], start=1):
            feature = features_by_id[feature_id]
            rows.append(
                {
                    "category_id": category["category_id"],
                    "category_column": category["category_column"],
                    "category_display_name": category["display_name"],
                    "feature_order": order,
                    "feature_id": feature_id,
                    "feature_display_name": feature["display_name"],
                    "feature_method": feature.get("method_label", feature["method"]),
                    "inverse": feature["inverse"],
                    "input_variables": feature["input_variables"],
                    "raw_dependencies": feature["raw_dependencies"],
                    "derived_dependencies": feature["derived_dependencies"],
                }
            )
    return rows


def quality_summary(
    config: dict[str, Any],
    raw_rows: list[dict[str, Any]],
    *,
    raw_file_count: int,
    raw_header_column_count: int,
    description_column_count: int,
) -> dict[str, Any]:
    raw_columns = raw_rows
    missing_required = [
        row["column_name"]
        for row in raw_columns
        if row["used_in_current_mapping"] and not row["in_raw_2020_files"]
    ]
    described_missing = [
        row["column_name"]
        for row in raw_columns
        if row["used_in_current_mapping"] and not row["description"]
    ]
    unused_described = [
        row["column_name"]
        for row in raw_columns
        if row["description"] and not row["used_in_current_mapping"]
    ]
    return {
        "raw_file_count": raw_file_count,
        "raw_header_column_count": raw_header_column_count,
        "description_column_count": description_column_count,
        "required_raw_column_count": len(config["processing"]["required_raw_columns"]),
        "feature_count": len(config["features"]),
        "category_count": len(config["categories"]),
        "missing_required_raw_columns": missing_required,
        "used_columns_without_description": described_missing,
        "described_columns_not_used_count": len(unused_described),
    }


def report_text(
    config: dict[str, Any],
    raw_rows: list[dict[str, Any]],
    output_paths: dict[str, Path],
    *,
    raw_file_count: int,
    raw_header_column_count: int,
    description_column_count: int,
) -> str:
    summary = quality_summary(
        config,
        raw_rows,
        raw_file_count=raw_file_count,
        raw_header_column_count=raw_header_column_count,
        description_column_count=description_column_count,
    )
    lines = [
        "# Antyodaya Variable Mapping Report",
        "",
        "This report describes the clean raw-column mapping used by the Antyodaya 2020 clustering workflow.",
        "",
        "## Outputs",
        "",
        f"- JSON config: `{output_paths['json'].relative_to(REPO_ROOT)}`",
        f"- Raw column flow CSV: `{output_paths['raw_csv'].relative_to(REPO_ROOT)}`",
        f"- Category/feature CSV: `{output_paths['category_csv'].relative_to(REPO_ROOT)}`",
        "",
        "## Coverage",
        "",
        f"- raw files: `{summary['raw_file_count']}`",
        f"- raw header columns: `{summary['raw_header_column_count']}`",
        f"- described columns: `{summary['description_column_count']}`",
        f"- required raw columns: `{summary['required_raw_column_count']}`",
        f"- features: `{summary['feature_count']}`",
        f"- final categories: `{summary['category_count']}`",
        "",
        "## Validation",
        "",
    ]
    if summary["missing_required_raw_columns"]:
        lines.append("Missing required raw columns:")
        lines.extend(f"- `{column}`" for column in summary["missing_required_raw_columns"])
    else:
        lines.append("- No required raw columns are missing from the 2020 raw files.")
    if summary["used_columns_without_description"]:
        lines.append("")
        lines.append("Used columns without descriptions:")
        lines.extend(f"- `{column}`" for column in summary["used_columns_without_description"])
    else:
        lines.append("- Every used raw column has a description entry.")
    lines.extend(
        [
            f"- Described but unused columns: `{summary['described_columns_not_used_count']}`",
            "",
            "## Processing Skeleton",
            "",
            "- Group rows by state, district, sub-district, and village code.",
            "- Aggregate count/area/amount columns by sum.",
            "- Aggregate denominator/count helper columns marked as max by max.",
            "- Derive binary/categorical helper variables from raw survey codes.",
            "- Derive score variables, including ordinal service-quality scores, composite piped-water coverage, agricultural input availability, agricultural risk support, and seasonal cropping intensity.",
            "- Convert raw parameters into feature scores/classes, then aggregate features into categories and cluster categories by the finalized category rules.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_config(args: argparse.Namespace) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, int]]:
    descriptions = load_raw_descriptions(args.column_descriptions)
    header_profile = raw_header_profile(args.raw_dir)
    feature_rows, category_rows = load_finalized_metadata(args.finalized_metadata)
    features = build_feature_config(feature_rows)
    categories = build_category_config(category_rows)
    raw_rows = build_raw_column_rows(descriptions, header_profile, features, categories)

    required_raw_columns: set[str] = set(ID_GROUP_COLUMNS) | set(ID_TEXT_COLUMNS) | set(ADMIN_AUDIT_COLUMNS)
    used_derived_variables: set[str] = set()
    for feature in features:
        required_raw_columns.update(feature["raw_dependencies"])
        used_derived_variables.update(feature["derived_dependencies"])
    derivation_config = build_derivation_config(used_derived_variables)
    direct_binary_presence = {
        column: codes
        for column, codes in DIRECT_BINARY_PRESENCE.items()
        if column in required_raw_columns
    }
    categorical_sources = {
        definition["source_column"]
        for definition in derivation_config["categorical_flags"].values()
    }
    row_level_score_variables = {
        variable
        for variable, definition in derivation_config["scores"].items()
        if definition["kind"] in {"ordinal_code_score", "code_score_with_distance_fallback"}
    }

    config = {
        "schema_version": "1.0",
        "project": "antyodaya_2020_raw_category_mapping",
        "source": {
            "raw_files_dir": args.raw_dir,
            "column_descriptions_csv": args.column_descriptions,
        },
        "processing": {
            "group_by_columns": list(ID_GROUP_COLUMNS),
            "display_identifier_columns": list(ID_TEXT_COLUMNS),
            "admin_audit_columns": list(ADMIN_AUDIT_COLUMNS),
            "required_raw_columns": sorted(required_raw_columns),
            "aggregation_groups": {
                "sum": sorted(COUNT_SUM_COLUMNS & required_raw_columns),
                "max": sorted(COUNT_MAX_COLUMNS & required_raw_columns),
                "min_distance_code": sorted(DISTANCE_MIN_COLUMNS & required_raw_columns),
                "presence_flag_then_max": sorted(direct_binary_presence),
                "min_categorical_code_then_derive": sorted(categorical_sources),
                "derived_score_then_max": sorted(row_level_score_variables),
            },
            "direct_binary_presence": direct_binary_presence,
            "derived_variables": derivation_config,
            "labels": {
                "0": "Low",
                "1": "Medium",
                "2": "High",
            },
        },
        "features": features,
        "categories": categories,
    }
    audit_counts = {
        "raw_file_count": int(header_profile["raw_file_count"]),
        "raw_header_column_count": len(header_profile["header_union"]),
        "description_column_count": len(descriptions),
    }
    return config, raw_rows, audit_counts


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    config, raw_rows, audit_counts = build_config(args)
    output_paths = {
        "json": args.output_dir / "antyodaya_variable_mapping.json",
        "raw_csv": args.output_dir / "antyodaya_raw_column_flow.csv",
        "category_csv": args.output_dir / "antyodaya_category_feature_mapping.csv",
        "report": args.output_dir / "antyodaya_variable_mapping_report.md",
    }

    write_json(output_paths["json"], config)
    write_csv(output_paths["raw_csv"], raw_rows)
    write_csv(output_paths["category_csv"], feature_category_rows(config["features"], config["categories"]))
    output_paths["report"].write_text(
        report_text(config, raw_rows, output_paths, **audit_counts),
        encoding="utf-8",
    )

    summary = quality_summary(config, raw_rows, **audit_counts)
    print(json.dumps({**summary, "outputs": {key: str(path) for key, path in output_paths.items()}}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
