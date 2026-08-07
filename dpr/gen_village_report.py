import re
import requests
import geopandas as gpd
import pandas as pd
import numpy as np
import pymannkendall as mk
import json
import ast

from nrm_app.settings import EXCEL_DIR, GEOSERVER_URL, OVERPASS_URL
from utilities.logger import setup_logger

logger = setup_logger(__name__)

DATA_DIR_TEMP = EXCEL_DIR

# ---------------------------------------------------------------------------
# Raw parameter display config for each basic-infrastructure sub-category.
# repr: "binary"  → 1 = Yes, 0 = No
#       "string"  → value is already a human-readable string
#       "numeric" → show as integer with thousand-separator
# ---------------------------------------------------------------------------
BASIC_INFRA_RAW_PARAMS = {
    "road_connectivity": [
        {"col": "is_village_connected_to_all_weather_road",  "label": "All-weather road connection",              "repr": "binary"},
        {"col": "availability_of_internal_pucca_road",       "label": "Internal pucca road quality",              "repr": "string"},
        {"col": "availability_of_public_transport",          "label": "Public transport availability",            "repr": "string"},
        {"col": "availability_of_railway_station",           "label": "Railway station availability",             "repr": "binary"},
    ],
    "energy_access": [
        {"col": "availablility_hours_of_domestic_electricity", "label": "Domestic electricity supply (hours/day)", "repr": "string"},
        {"col": "availability_of_elect_supply_to_msme",      "label": "Electricity supply to MSME units",        "repr": "binary"},
        {"col": "total_hhd",                                 "label": "Total number of households",                         "repr": "numeric"},
        {"col": "total_hhd_with_clean_energy",               "label": "HHs using clean energy (LPG / Biogas)",   "repr": "numeric"},
    ],
    "housing_quality": [
        {"col": "total_hhd",                                        "label": "Total number of households",                              "repr": "numeric"},
        {"col": "total_hhd_with_kuccha_wall_kuccha_roof",           "label": "HHs with kuccha wall & kuccha roof",            "repr": "numeric"},
        {"col": "total_hhd_got_benefit_under_state_housing_scheme", "label": "State housing scheme beneficiaries",            "repr": "numeric"},
        {"col": "total_hhd_have_got_pmay_house",                    "label": "PMAY houses (completed / sanctioned)",          "repr": "numeric"},
        {"col": "total_hhd_in_pmay_permanent_wait_list",            "label": "PMAY permanent waitlist households",            "repr": "numeric"},
        {"col": "total_hhd_availing_pmuy_benefits",                 "label": "PMUY (Ujjwala Yojana) beneficiaries",          "repr": "numeric"},
    ],
}


def _format_raw_param(value, repr_type):
    """Format a raw Excel value for display based on its representation type."""
    try:
        is_null = value is None or (isinstance(value, float) and pd.isna(value))
    except Exception:
        is_null = False
    if is_null:
        return "N/A"
    if repr_type == "binary":
        try:
            return "Yes" if int(float(value)) == 1 else "No"
        except Exception:
            return str(value)
    if repr_type == "numeric":
        try:
            return f"{int(float(value)):,}"
        except Exception:
            s = str(value)
            return "N/A" if s in ("nan", "None", "") else s
    # string
    s = str(value).strip()
    return "N/A" if s in ("nan", "None", "") else s


def _cluster_to_color(raw):
    """Convert a cat_cluster string (HIGH/MEDIUM/LOW) to a color string."""
    c = str(raw).strip().upper() if raw is not None else ""
    if c == "HIGH":   return "green"
    if c == "MEDIUM": return "yellow"
    return "red"


def _cluster_label(raw):
    """Convert a cat_cluster string to a title-cased label (High/Medium/Low)."""
    c = str(raw).strip().title() if raw is not None else ""
    return c if c in ("High", "Medium", "Low") else "Low"


def _safe_cluster(raw):
    """Return True if raw cluster value is valid (not None/NaN/empty)."""
    if raw is None:
        return False
    try:
        return not pd.isna(raw)
    except Exception:
        return bool(str(raw).strip())


def _build_raw_params(row, param_list):
    """Build a list of {label, value} dicts from a df row and param spec list."""
    result = []
    for p in param_list:
        result.append({
            "label": p["label"],
            "value": _format_raw_param(row.get(p["col"]), p["repr"]),
        })
    return result


# ---------------------------------------------------------------------------
# Raw parameter display config for each section sub-category (all remaining).
# Same repr convention as BASIC_INFRA_RAW_PARAMS.
# ---------------------------------------------------------------------------
SECTION_RAW_PARAMS = {
    "maternal_child_health": [
        {"col": "availability_of_mother_child_health_facilities",            "label": "Availability of Mother and Child Health facilities",          "repr": "binary"},
        {"col": "is_aanganwadi_centre_available",                            "label": "Availability of Aanganwadi Centre",                        "repr": "binary"},
        {"col": "is_early_childhood_edu_provided_in_anganwadi",              "label": "Is Early Childhood Education provided in the Anganwadi",            "repr": "binary"},
        {"col": "total_childs_aged_0_to_3_years",                            "label": "Total no of children in the age group of 0-3 years",                           "repr": "numeric"},
        {"col": "total_childs_aged_0_to_3_years_reg_under_aanganwadi",       "label": "Total no of children aged 0-3 years registered in Aanganwadi",              "repr": "numeric"},
        {"col": "total_no_of_pregnant_women",                                "label": "Total number of Pregnant women",                                    "repr": "numeric"},
        {"col": "total_no_of_pregnant_women_receiving_services_under_icds",  "label": "No of pregnant women receiving services under ICDS",            "repr": "numeric"},
        {"col": "total_no_of_lactating_mothers",                             "label": "Total number of lactating mothers",                                 "repr": "numeric"},
        {"col": "total_anemic_pregnant_women",                               "label": "No. of Anaemic Pregnant Women",                            "repr": "numeric"},
        {"col": "total_childs_aged_0_to_3_years_immunized",                  "label": "No of children aged 0-3 years immunized",                      "repr": "numeric"},
        {"col": "total_no_of_newly_born_children",                           "label": "Total number of newly born children during the year",                               "repr": "numeric"},
        {"col": "total_no_of_newly_born_underweight_children",               "label": "No of newly born children underweight",                   "repr": "numeric"},
        {"col": "gp_total_no_of_beneficiaries_receiving_benefits_under_pmjay","label": "No. of beneficiaries receiving benefits under PMJAY",           "repr": "numeric"},
        {"col": "gp_total_no_of_eligible_beneficiaries_under_pmjay",         "label": "Total no. of eligible beneficiaries under PMJAY",                      "repr": "numeric"},
        {"col": "total_hhd_registered_under_pmjay",                          "label": "No. of Households registered under PMJAY/State Health Insurance",                 "repr": "numeric"},
        {"col": "total_no_of_beneficiaries_receiving_benefits_under_pmmvy",  "label": "No of beneficiaries receiving benefits under PMMVY",            "repr": "numeric"},
        {"col": "total_no_of_eligible_beneficiaries_under_pmmvy",            "label": "Total no of eligible beneficiaries under PMMVY",                      "repr": "numeric"},
    ],
    "water_sanitation": [
        {"col": "availability_of_piped_tap_water",                   "label": "Availability of Piped tap water (Coverage)",                    "repr": "string"},
        {"col": "total_hhd",                                         "label": "Total number of households",                                  "repr": "numeric"},
        {"col": "total_hhd_having_piped_water_connection",           "label": "No of households having piped water connection",                  "repr": "numeric"},
        {"col": "total_hhd_not_having_sanitary_latrines",            "label": "No of households not having sanitary latrines",                    "repr": "numeric"},
        {"col": "availability_of_drainage_system",                   "label": "Availability of drainage facilities",                      "repr": "string"},
        {"col": "is_community_waste_disposal_system",                "label": "Community waste disposal system",                   "repr": "binary"},
        {"col": "is_community_biogas_waste_recycle_for_production",  "label": "Community bio gas or recycle of waste",               "repr": "binary"},
    ],
    "financial_inclusion": [
        {"col": "is_bank_available",                                 "label": "Availability of banks",                         "repr": "binary"},
        {"col": "is_atm_available",                                  "label": "Availability of ATM",                                     "repr": "binary"},
        {"col": "is_bank_buss_correspondent_with_internet",          "label": "Availability of Business Correspondent with internet connectivity",                "repr": "binary"},
        {"col": "total_shg",                                         "label": "Number of Self Help Groups (SHGs)",                                        "repr": "numeric"},
        {"col": "total_shg_accessed_bank_loans",                     "label": "No of SHGs which accessed bank loans",                    "repr": "numeric"},
        {"col": "total_hhd",                                         "label": "Total number of households",                                  "repr": "numeric"},
        {"col": "total_hhd_availing_pmjdy_bank_ac",                  "label": "Number of households having Jan-Dhan bank account",                      "repr": "numeric"},
    ],
    "social_protection": [
        {"col": "gp_total_hhd_eligible_under_nfsa",                  "label": "Total number of eligible households under NFSA",                   "repr": "numeric"},
        {"col": "gp_total_hhd_receiving_food_grains_from_fps",       "label": "Total no of households receiving food grains from Fair Price Shops",               "repr": "numeric"},
        {"col": "total_hhd",                                         "label": "Total households",                                  "repr": "numeric"},
        {"col": "total_hhd_having_bpl_cards",                        "label": "Number of Households having BPL ration cards",                               "repr": "numeric"},
        {"col": "total_hhd_availing_pension_under_nsap",             "label": "Number of Households getting pensions under NSAP",                        "repr": "numeric"},
    ],
    "institutionalization": [
        {"col": "total_hhd",                                         "label": "Total number of households",                                  "repr": "numeric"},
        {"col": "total_hhd_mobilized_into_shg",                      "label": "Number of households mobilized into SHGs",                          "repr": "numeric"},
        {"col": "total_no_of_shg_promoted",                          "label": "Number of SHGs federated into Village Organisations",                                    "repr": "numeric"},
        {"col": "total_shg",                                         "label": "Number of Self Help Groups (SHGs)",                                        "repr": "numeric"},
        {"col": "total_hhd_mobilized_into_pg",                       "label": "Number of households mobilized into Producer Groups",               "repr": "numeric"},
        {"col": "availability_of_fpos_pacs",                         "label": "Availability of Farmers Collective (Farmer Producer Organizations (FPOs)/Primary Agricultural Credit Societies (PACS))",                         "repr": "string"},
    ],
    "civic_infrastructure": [
        {"col": "availability_of_panchayat_bhawan",                  "label": "Availability of Panchayat Bhawan",                        "repr": "binary"},
        {"col": "is_post_office_available",                          "label": "Availability of Post office/Sub-Post office",                             "repr": "binary"},
        {"col": "total_no_of_elected_representatives",               "label": "Total no of elected representatives",                           "repr": "numeric"},
        {"col": "total_no_of_elect_rep_undergone_training_under_rgsa","label": "No of elected representatives undergone refresher training under RGSA",              "repr": "numeric"},
        {"col": "total_no_of_elect_rep_oriented_under_rgsa",         "label": "No of elected representatives oriented under RGSA",              "repr": "numeric"},
        {"col": "availability_of_public_information_board",          "label": "Availability of Public Information Board under People's Plan Campaign",            "repr": "string"},
        {"col": "availability_of_public_library",                    "label": "Availability of Public Library",                          "repr": "binary"},
    ],
    "livelihoods_employment": [
        {"col": "total_hhd",                                         "label": "Total number of households",                                  "repr": "numeric"},
        {"col": "total_hhd_engaged_in_farm_activities",              "label": "Number of households engaged majorly in farm activities",                   "repr": "numeric"},
    ],
    "livelihoods_forest_resources": [
        {"col": "availability_of_community_forest",                  "label": "Availability of Community Forest",                        "repr": "binary"},
        {"col": "availability_of_minor_forest_production",           "label": "Availability of minor forest production",                   "repr": "binary"},
        {"col": "total_hhd",                                         "label": "Total number of households",                                  "repr": "numeric"},
        {"col": "total_hhd_source_of_minor_forest_production",       "label": "Number of Households where only source of livelihood is minor forest production",                "repr": "numeric"},
    ],
    "livelihoods_fisheries": [
        {"col": "availability_of_aquaculture_ext_facility",          "label": "Extension facilities for Aquaculture",          "repr": "binary"},
        {"col": "availability_of_fish_community_ponds",              "label": "Community Ponds Used for Fisheries",                   "repr": "binary"},
        {"col": "availability_of_fish_farming",                      "label": "Pisciculture - InLand Fishery/Coastal Fishery",                            "repr": "binary"},
    ],
    "livelihoods_alternative_farming": [
        {"col": "is_bee_farming",                                    "label": "Bee Keeping",                                       "repr": "binary"},
        {"col": "is_sericulture",                                    "label": "Sericulture (Silk Production)",                                       "repr": "binary"},
    ],
    "livelihoods_cottage_traditional_industry": [
        {"col": "availability_of_cottage_small_scale_units",         "label": "Availability of cottage and small scale units",            "repr": "binary"},
        {"col": "total_hhd",                                         "label": "Total number of households",                                  "repr": "numeric"},
        {"col": "total_hhd_engaged_cottage_small_scale_units",       "label": "Number of Households engaged in cottage/small scale units",              "repr": "numeric"},
        {"col": "is_handloom",                                       "label": "Handloom",                                          "repr": "binary"},
        {"col": "is_handicrafts",                                    "label": "Handicrafts",                                       "repr": "binary"},
    ],
    "livelihoods_common_resources": [
        {"col": "is_common_pastures_available",                      "label": "Common pastures as per revenue records",                         "repr": "binary"},
    ],
    "livestock_veterinary": [
        {"col": "availability_of_livestock_extension_services",      "label": "Availability of Livestock Extension services",         "repr": "string"},
        {"col": "is_veterinary_hospital_available",                  "label": "Availability of Veterinary Clinic or Hospital",                     "repr": "binary"},
        {"col": "availability_of_goatary_dev_project",               "label": "Project supporting Goatary Development",                       "repr": "binary"},
        {"col": "availability_of_pigery_development",                "label": "Project supporting Piggery Development",                       "repr": "binary"},
        {"col": "availability_of_poultry_dev_project",               "label": "Project supporting Poultry Development",                       "repr": "binary"},
        {"col": "availability_of_milk_routes",                       "label": "Availability of Milk Collection Centre/Milk routes/Chilling Centres",                             "repr": "binary"},
    ],
    "agriculture_land_cultivation": [
        {"col": "area_irrigated_in_hac",                             "label": "Total area irrigated",                              "repr": "numeric"},
        {"col": "net_sown_area_in_hac",                              "label": "Net sown Area",                               "repr": "numeric"},
        {"col": "net_sown_area_kharif_in_hac",                       "label": "Net sown Area during Kharif season",                      "repr": "numeric"},
        {"col": "net_sown_area_other_in_hac",                        "label": "Net sown Area during other seasons",                       "repr": "numeric"},
        {"col": "net_sown_area_rabi_in_hac",                         "label": "Net sown Area during Rabi season",                        "repr": "numeric"},
        {"col": "total_cultivable_area_in_hac",                      "label": "Total Cultivable Area",                       "repr": "numeric"},
    ],
    "agriculture_irrigation_watershed": [
        {"col": "availability_of_major_source_of_irrigation",        "label": "Main Source of irrigation",              "repr": "string"},
        {"col": "availability_of_rain_harvest_system",               "label": "Availability of Community Rain Water Harvesting System/Pond/Dam/Check Dam",                      "repr": "binary"},
        {"col": "availability_of_watershed_dev_project",             "label": "Whether village is part of Watershed Development Project",                     "repr": "binary"},
        {"col": "total_approved_labour_budget_for_year",             "label": "Total approved Labour Budget for the year (₹)",                        "repr": "numeric"},
        {"col": "total_expenditure_approved_under_nrm_labour_budget_during_yr", "label": "Total expenditure approved under NRM in the Labour Budget (₹)",                "repr": "numeric"},
        {"col": "no_of_farmers_using_drip_sprinkler",                "label": "Number of farmers using drip/sprinkler irrigation",        "repr": "numeric"},
        {"col": "total_no_of_farmers",                               "label": "Total no of farmers",                                     "repr": "numeric"},
    ],
    "agriculture_support_services": [
        {"col": "is_fertilizer_shop_available",                      "label": "Availability of fertilizer shop",                         "repr": "binary"},
        {"col": "is_govt_seed_centre_available",                     "label": "Availability of government seed centres",                        "repr": "binary"},
        {"col": "is_soil_testing_centre_available",                  "label": "Availability of soil testing centres",                     "repr": "binary"},
        {"col": "total_no_of_farmers",                               "label": "Total no of farmers",                                     "repr": "numeric"},
        {"col": "total_no_of_farmers_received_benefit_under_pmfby",  "label": "No of farmers received benefits under PMFBY",                    "repr": "numeric"},
        {"col": "total_no_of_farmers_registered_under_pmkpy",        "label": "Total number of farmers registered under PM Kisan Pension Yojana",                   "repr": "numeric"},
        {"col": "total_no_of_farmers_add_fert_in_soil_as_per_report","label": "Number of farmers received the soil testing report",  "repr": "numeric"},
    ],
    "agricultural_markets": [
        {"col": "availability_of_market",                            "label": "Availability of markets",                               "repr": "string"},
        {"col": "availability_of_food_storage_warehouse",            "label": "Availability of warehouse for Food Grain Storage",                "repr": "binary"},
    ],
    "agriculture_organic_farming": [
        {"col": "total_no_farmers_adopted_organic_farming",          "label": "No of farmers adopted organic farming",                  "repr": "numeric"},
        {"col": "total_no_of_farmers",                               "label": "Total no of farmers",                                     "repr": "numeric"},
    ],
}


def _build_file_path(state, district, block):
    return (
        DATA_DIR_TEMP
        + state.upper() + "/"
        + district.upper() + "/"
        + district.lower() + "_" + block.lower() + ".xlsx"
    )


def load_block_sheets(state, district, block):
    """Load all Excel sheets for a block once. Returns (df, df_facilities, df_nrega, df_livestock)."""
    try:
        excel_file = pd.ExcelFile(_build_file_path(state, district, block))
        df = pd.read_excel(excel_file, sheet_name="antyodaya")
        df["village_id"] = df["village_id"].astype(str).str.strip()
        df_facilities = pd.read_excel(excel_file, sheet_name="facilities_proximity")
        df_facilities["village_id"] = df_facilities["village_id"].astype(str).str.strip()
        df_nrega = pd.read_excel(excel_file, sheet_name="nrega_assets_village")
        df_nrega["vill_id"] = df_nrega["vill_id"].astype(str).str.strip()
        df_livestock = pd.read_excel(excel_file, sheet_name="livestock")
        df_livestock["village_id"] = df_livestock["village_id"].astype(str).str.strip()
        return df, df_facilities, df_nrega, df_livestock
    except Exception as e:
        logger.error(
            "Failed to load block sheets for %s/%s/%s: %s", state, district, block, str(e)
        )
        return None, None, None, None


# ? MARK: HELPER FUNCTIONS
def get_geojson(workspace, layer_name):
    """Construct the GeoServer WFS request URL for fetching GeoJSON data."""
    geojson_url = f"{GEOSERVER_URL}/{workspace}/ows?service=WFS&version=1.0.0&request=GetFeature&typeName={workspace}:{layer_name}&outputFormat=application/json"
    return geojson_url

def calculate_demographics(properties):
    """
    Calculate demographic metrics and percentages from village properties.
    """
    
    # Extract base values
    tot_p = properties.get('TOT_P', 0)  # Total Population
    p_lit = properties.get('P_LIT', 0)  # Total Literate
    p_sc = properties.get('P_SC', 0)  # Total SC Population
    p_st = properties.get('P_ST', 0)  # Total ST Population
    
    # Calculate percentages (avoid division by zero)
    literacy_percentage = round((p_lit / tot_p * 100), 2) if tot_p > 0 else 0
    sc_percentage = round((p_sc / tot_p * 100), 2) if tot_p > 0 else 0
    st_percentage = round((p_st / tot_p * 100), 2) if tot_p > 0 else 0
    
    # Build demographic data dictionary
    demographic_data = {
        # Population
        'TOT_P': properties.get('TOT_P', 0),
        'TOT_M': properties.get('TOT_M', 0),
        'TOT_F': properties.get('TOT_F', 0),
        'No_HH': properties.get('No_HH', 0),
        
        # SC Population with percentage
        'P_SC': properties.get('P_SC', 0),
        'M_SC': properties.get('M_SC', 0),
        'F_SC': properties.get('F_SC', 0),
        'sc_percentage': sc_percentage,
        
        # ST Population with percentage
        'P_ST': properties.get('P_ST', 0),
        'M_ST': properties.get('M_ST', 0),
        'F_ST': properties.get('F_ST', 0),
        'st_percentage': st_percentage,
        
        # Literacy
        'P_LIT': properties.get('P_LIT', 0),
        'M_LIT': properties.get('M_LIT', 0),
        'F_LIT': properties.get('F_LIT', 0),
        'literacy_percentage': literacy_percentage,
        
        # Illiteracy
        'P_ILL': properties.get('P_ILL', 0),
        'M_ILL': properties.get('M_ILL', 0),
        'F_ILL': properties.get('F_ILL', 0),
        
        # Development Index
        'ADI_2011': properties.get('ADI_2011', 0),
        'ADI_2019': properties.get('ADI_2019', 0),
    }
    
    return demographic_data


def get_mwses_ids(state, district, block, village_id):

    try:

        file_path = (
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )

        excel_file = pd.ExcelFile(file_path)

        df_mws = pd.read_excel(
            excel_file,
            sheet_name="mws_intersect_villages"
        )

        village_id = int(village_id)

        mws_ids = []

        for _, row in df_mws.iterrows():

            village_ids_raw = row.get("Village IDs")

            if pd.isnull(village_ids_raw):
                continue

            try:
                village_ids = ast.literal_eval(
                    str(village_ids_raw)
                )

            except Exception:
                continue

            if village_id in village_ids:

                mws_ids.append(
                    str(row.get("MWS UID"))
                )

        return mws_ids

    except Exception as e:

        logger.info(
            "Not able to access MWS data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return []

# ? MARK: MAIN SECTION
def get_village_polygon_and_info(state, district, block, village_id):

    try:
        # Construct the layer name based on state/district/block
        workspace = 'panchayat_boundaries'
        layer_name = f"{district}_{block}".lower()  # e.g., "ajmer_bhinay"
        
        # Create WFS URL with CQL_FILTER to query by vill_ID
        base_url = f"{GEOSERVER_URL}/{workspace}/ows"
        
        # CQL_FILTER: search for the specific village by ID
        cql_filter = f"vill_ID={village_id}"
        
        params = {
            'service': 'WFS',
            'version': '1.0.0',
            'request': 'GetFeature',
            'typeName': f'{workspace}:{layer_name}',
            'outputFormat': 'application/json',
            'CQL_FILTER': cql_filter
        }
        
        logger.info(f"Querying GeoServer for village: state={state}, district={district}, block={block}, village_id={village_id}")
        
        # Make request to GeoServer
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        
        geojson_data = response.json()
        
        # Check if we got features
        if not geojson_data.get('features') or len(geojson_data['features']) == 0:
            logger.warning(f"No village found with ID {village_id} in {district}, {block}")
            return {
                'village_polygon': None,
                'village_name': None,
                'gram_panchayat_name': None,
                'area_hectares': None,
                'properties': {}
            }
        
        # Extract the first feature (should be only one with specific vill_ID)
        feature = geojson_data['features'][0]
        properties = feature.get('properties', {})
        
        # Extract village information from properties
        village_name = properties.get('vill_name', None)
        
        # Try to construct gram panchayat name (may not be in properties, so optional)
        # You can customize this based on your actual data structure
        gram_panchayat_name = properties.get('gram_panchayat', None) or properties.get('gp_name', None)
        
        # Extract area if available (in hectares)
        # This depends on your data; adjust the property name if different
        area_hectares = properties.get('area_hectares', None) or properties.get('area_ha', None)
        
        logger.info(f"Found village: {village_name} (ID: {village_id})")
        
        # Return the village polygon as a FeatureCollection (required for template)
        village_polygon_geojson = {
            'type': 'FeatureCollection',
            'features': [feature]
        }

        return {
            'village_polygon': village_polygon_geojson,
            'village_name': village_name,
            'gram_panchayat_name': gram_panchayat_name,
            'area_hectares': area_hectares,
            'properties': properties
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"GeoServer request failed: {str(e)}")
        return {
            'village_polygon': None,
            'village_name': None,
            'gram_panchayat_name': None,
            'area_hectares': None,
            'properties': {}
        }
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error parsing GeoServer response: {str(e)}")
        return {
            'village_polygon': None,
            'village_name': None,
            'gram_panchayat_name': None,
            'area_hectares': None,
            'properties': {}
        }
    

def get_development_data(state, district, block, village_id, df=None, df_facilities=None, df_nrega=None):

    def normalize_column(df, column):
        df[column] = df[column].astype(str).str.strip()

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

    def get_numeric(row, column):
        return pd.to_numeric(row.get(column, None), errors="coerce")

    def calculate_band_score(value):
        if value <= 0.33:
            return 0.33
        elif value <= 0.66:
            return 0.66
        return 1

    def distance_score(distance, high_limit, medium_limit=None):

        if pd.isnull(distance):
            return 0.33

        if medium_limit is None:
            return 1 if distance < high_limit else 0.33

        if distance < high_limit:
            return 1
        elif high_limit <= distance <= medium_limit:
            return 0.66
        return 0.33

    def get_distance_logic(row, columns, logic="max"):

        values = [
            get_numeric(row, col)
            for col in columns
        ]

        values = [v for v in values if pd.notnull(v)]

        if not values:
            return None

        return max(values) if logic == "max" else min(values)

    try:

        if df is None or df_facilities is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))

            if df is None:
                df = pd.read_excel(excel_file, sheet_name="antyodaya")
            if df_facilities is None:
                df_facilities = pd.read_excel(excel_file, sheet_name="facilities_proximity")

        normalize_column(df, "village_id")
        normalize_column(df_facilities, "village_id")

        village_id = str(village_id).strip()

        matched_rows = df[df["village_id"] == village_id]

        if matched_rows.empty:
            logger.info(
                "No data found for village_id %s in %s district, %s block",
                village_id,
                district,
                block,
            )
            return []

        row = matched_rows.iloc[0]

        facility_match = df_facilities[
            df_facilities["village_id"] == village_id
        ]

        facility_row = (
            facility_match.iloc[0]
            if not facility_match.empty
            else None
        )

        scores = []

        # =========================================================
        # Infrastructure Score
        # =========================================================

        infrastructure_avg = (
            safe_float(row.get("road_connectivity_cat_value", 0))
            + safe_float(row.get("energy_access_cat_value", 0))
            + safe_float(row.get("housing_quality_cat_value", 0))
        ) / 3

        scores.append(calculate_band_score(infrastructure_avg))

        # =========================================================
        # Health Score
        # =========================================================

        maternal_child_score = safe_float(
            row.get("maternal_child_health_cat_value", 0)
        )

        water_sanitation_score = safe_float(
            row.get("water_sanitation_cat_value", 0)
        )

        essential_health_services_score = 0.33
        advanced_health_services_score = 0.33

        if facility_row is not None:

            essential_distance = get_distance_logic(
                facility_row,
                [
                    "health_sub_cen_distance_in_km",
                    "health_phc_distance_in_km"
                ],
                logic="max"
            )

            essential_health_services_score = distance_score(
                essential_distance,
                high_limit=2,
                medium_limit=5
            )

            advanced_distance = get_distance_logic(
                facility_row,
                [
                    "health_chc_distance_in_km",
                    "health_dis_h_distance_in_km",
                    "health_s_t_h_distance_in_km"
                ],
                logic="min"
            )

            advanced_health_services_score = distance_score(
                advanced_distance,
                high_limit=10,
                medium_limit=25
            )

        health_avg = (
            maternal_child_score
            + water_sanitation_score
            + essential_health_services_score
            + advanced_health_services_score
        ) / 4

        scores.append(calculate_band_score(health_avg))

        #* Education Score
        education_score = 0.33

        if facility_row is not None:

            essential_education_distance = get_distance_logic(
                facility_row,
                [
                    "school_primary_distance_in_km",
                    "school_upper_primary_distance_in_km",
                    "school_secondary_distance_in_km"
                ],
                logic="max"
            )

            higher_education_distance = get_distance_logic(
                facility_row,
                [
                    "school_higher_secondary_distance_in_km",
                    "college_distance_in_km",
                    "universities_distance_in_km"
                ],
                logic="min"
            )

            if (
                essential_education_distance is not None
                and higher_education_distance is not None
            ):

                if (
                    essential_education_distance > 2
                    and higher_education_distance > 8
                ):
                    education_score = 0.33

                elif (
                    essential_education_distance < 2
                    and higher_education_distance < 8
                ):
                    education_score = 1

                else:
                    education_score = 0.67

        scores.append(education_score)

        #* Financial Inclusion Score
        financial_inclusion_score = 0.33

        if facility_row is not None:

            financial_distance = get_distance_logic(
                facility_row,
                [
                    "csc_distance_in_km",
                    "bank_mitra_distance_in_km",
                    "bank_branch_distance_in_km",
                    "bank_atm_distance_in_km"
                ],
                logic="max"
            )

            financial_inclusion_score = distance_score(
                financial_distance,
                high_limit=2,
                medium_limit=5
            )

        scores.append(financial_inclusion_score)


        #* Welfare Inclusion Score
        social_protection_score = safe_float(row.get("social_protection_cat_value", 0))

        pds_score = 0.5

        if facility_row is not None:

            pds_distance = get_numeric(
                facility_row,
                "pds_distance_in_km"
            )

            pds_score = 1 if (
                pd.notnull(pds_distance)
                and pds_distance < 2
            ) else 0.5

        welfare_avg = (
            social_protection_score + pds_score
        ) / 2

        scores.append(calculate_band_score(welfare_avg))

        #* Community Institutions
        community_score = safe_float(row.get("institutionalization_cat_value", 0))
        civic_score = safe_float(row.get("civic_infrastructure_cat_value", 0))

        community_avg_score = (community_score + civic_score) / 2

        scores.append(calculate_band_score(community_avg_score))

        #* Livelihood Diversification Score
        livelihood_farm_score = safe_float(row.get("livelihoods_employment_cat_value", 0))
        livelihood_forest_score = safe_float(row.get("livelihoods_forest_resources_cat_value", 0))
        livelihood_fish_score = safe_float(row.get("livelihoods_fisheries_cat_value", 0))
        livelihood_alternate_score = safe_float(row.get("livelihoods_alternative_farming_cat_value", 0))
        livelihood_cottage_score = safe_float(row.get("livelihoods_cottage_traditional_industry_cat_value", 0))

        livelihood_avg_score = (livelihood_farm_score + livelihood_forest_score + livelihood_fish_score + livelihood_alternate_score + livelihood_cottage_score)/5
        
        scores.append(calculate_band_score(livelihood_avg_score))


        #* Livestock
        livestock_support_score = safe_float(row.get("livestock_veterinary_cat_value", 0))
        livestock_pasture_score = safe_float(row.get("livelihoods_common_resources_cat_value", 0))

        livestock_support_avg = (livestock_support_score + livestock_pasture_score) / 2

        if facility_row is not None:
            husbandry_distance = get_numeric(
                facility_row,
                "agri_industry_dairy_animal_husbandry_distance_in_km"
            )

            # High: < 10 km
            if pd.notnull(husbandry_distance) and husbandry_distance < 10:
                husbandry_score = 1

            # Moderate: 10 - 30 km
            elif (
                pd.notnull(husbandry_distance)
                and 10 <= husbandry_distance <= 30
            ):
                husbandry_score = 0.67

            # Low: > 30 km
            else:
                husbandry_score = 0.33

        livestock_avg_score = (livestock_support_avg + husbandry_score) / 2

        scores.append(calculate_band_score(livestock_avg_score))

        #* Agricultural Productivity and Resource Use
        agri_avg_score = (
            safe_float(row.get("agricultural_markets_cat_value", 0))
            + safe_float(row.get("agriculture_land_cultivation_cat_value", 0))
            + safe_float(row.get("agriculture_irrigation_watershed_cat_value", 0))
            + safe_float(row.get("agriculture_support_services_cat_value", 0))
        ) / 4


        facility_scores = []

        if facility_row is not None:

            agri_facility_configs = [
                {
                    "column": "agri_industry_agri_support_infrastructure_distance_in_km",
                    "high_limit": 10,
                    "medium_limit": 50,
                },
                {
                    "column": "agri_industry_agri_processing_distance_in_km",
                    "high_limit": 5,
                    "medium_limit": 20,
                },
                {
                    "column": "agri_industry_co_operatives_societies_distance_in_km",
                    "high_limit": 10,
                    "medium_limit": 30,
                },
                {
                    "column": "agri_industry_markets_trading_distance_in_km",
                    "high_limit": 3,
                    "medium_limit": 10,
                },
            ]

            facility_scores = [
                distance_score(
                    get_numeric(facility_row, config["column"]),
                    high_limit=config["high_limit"],
                    medium_limit=config["medium_limit"],
                )
                for config in agri_facility_configs
            ]


        agri_produce_resource_score = (agri_avg_score + sum(facility_scores)) / (1 + len(facility_scores))

        scores.append(calculate_band_score(agri_produce_resource_score))

        #* Ecology and Climate Resilience
        organic_farm_score = safe_float(row.get("agriculture_organic_farming_cat_value", 0))
        if df_nrega is None:
            df_nrega = pd.read_excel(
                pd.ExcelFile(_build_file_path(state, district, block)),
                sheet_name="nrega_assets_village"
            )

        normalize_column(df_nrega, "vill_id")

        nrega_match = df_nrega[
            df_nrega["vill_id"] == village_id
        ]

        nrega_score = 0.33

        if not nrega_match.empty:

            nrega_row = nrega_match.iloc[0]

            # Exclude non-numeric columns
            exclude_columns = ["vill_id", "vill_name"]

            year_columns = [
                col for col in df_nrega.columns
                if col not in exclude_columns
            ]

            # Sum all yearly NREGA asset columns
            total_nrega_assets = sum([
                safe_float(nrega_row.get(col, 0))
                for col in year_columns
            ])

            # Assign score
            if total_nrega_assets < 100:
                nrega_score = 0.33

            elif 100 <= total_nrega_assets <= 300:
                nrega_score = 0.67

            else:
                nrega_score = 1

        # Final Ecology and Climate Resilience Score

        ecology_climate_avg = (
            organic_farm_score + nrega_score
        ) / 2

        scores.append(
            calculate_band_score(ecology_climate_avg)
        )

        return scores

    except Exception as e:

        logger.info(
            "Not able to access excel for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return []
    

def get_block_development_data(state, district, block, df=None, df_facilities=None, df_nrega=None):

    def normalize_column(df, column):
        df[column] = df[column].astype(str).str.strip()

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

    def get_numeric(row, column):
        return pd.to_numeric(row.get(column, None), errors="coerce")

    def calculate_band_score(value):
        if value <= 0.33:
            return 0.33
        elif value <= 0.66:
            return 0.66
        return 1

    def distance_score(distance, high_limit, medium_limit=None):

        if pd.isnull(distance):
            return 0.33

        if medium_limit is None:
            return 1 if distance < high_limit else 0.33

        if distance < high_limit:
            return 1

        elif high_limit <= distance <= medium_limit:
            return 0.67

        return 0.33

    def get_distance_logic(row, columns, logic="max"):

        values = [
            get_numeric(row, col)
            for col in columns
        ]

        values = [v for v in values if pd.notnull(v)]

        if not values:
            return None

        return max(values) if logic == "max" else min(values)

    try:

        if df is None or df_facilities is None or df_nrega is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))

            if df is None:
                df = pd.read_excel(excel_file, sheet_name="antyodaya")
            if df_facilities is None:
                df_facilities = pd.read_excel(excel_file, sheet_name="facilities_proximity")
            if df_nrega is None:
                df_nrega = pd.read_excel(excel_file, sheet_name="nrega_assets_village")

        normalize_column(df, "village_id")
        normalize_column(df_facilities, "village_id")
        normalize_column(df_nrega, "vill_id")

        block_scores = []

        # =========================================================
        # Infrastructure Score
        # =========================================================

        infrastructure_avg = (
            df[
                [
                    "road_connectivity_cat_value",
                    "energy_access_cat_value",
                    "housing_quality_cat_value"
                ]
            ]
            .apply(pd.to_numeric, errors="coerce")
            .mean()
            .mean()
        )

        block_scores.append(
            calculate_band_score(infrastructure_avg)
        )

        # =========================================================
        # Health Score
        # =========================================================

        maternal_child_avg = pd.to_numeric(
            df["maternal_child_health_cat_value"],
            errors="coerce"
        ).mean()

        water_sanitation_avg = pd.to_numeric(
            df["water_sanitation_cat_value"],
            errors="coerce"
        ).mean()

        essential_health_scores = []
        advanced_health_scores = []

        for _, facility_row in df_facilities.iterrows():

            essential_distance = get_distance_logic(
                facility_row,
                [
                    "health_sub_cen_distance_in_km",
                    "health_phc_distance_in_km"
                ],
                logic="max"
            )

            essential_health_scores.append(
                distance_score(
                    essential_distance,
                    high_limit=2,
                    medium_limit=5
                )
            )

            advanced_distance = get_distance_logic(
                facility_row,
                [
                    "health_chc_distance_in_km",
                    "health_dis_h_distance_in_km",
                    "health_s_t_h_distance_in_km"
                ],
                logic="min"
            )

            advanced_health_scores.append(
                distance_score(
                    advanced_distance,
                    high_limit=10,
                    medium_limit=25
                )
            )

        health_avg = (
            maternal_child_avg
            + water_sanitation_avg
            + (sum(essential_health_scores) / len(essential_health_scores))
            + (sum(advanced_health_scores) / len(advanced_health_scores))
        ) / 4

        block_scores.append(
            calculate_band_score(health_avg)
        )

        # =========================================================
        # Education Score
        # =========================================================

        education_scores = []

        for _, facility_row in df_facilities.iterrows():

            essential_education_distance = get_distance_logic(
                facility_row,
                [
                    "school_primary_distance_in_km",
                    "school_upper_primary_distance_in_km",
                    "school_secondary_distance_in_km"
                ],
                logic="max"
            )

            higher_education_distance = get_distance_logic(
                facility_row,
                [
                    "school_higher_secondary_distance_in_km",
                    "college_distance_in_km",
                    "universities_distance_in_km"
                ],
                logic="min"
            )

            if (
                essential_education_distance is not None
                and higher_education_distance is not None
            ):

                if (
                    essential_education_distance > 2
                    and higher_education_distance > 8
                ):
                    education_scores.append(0.33)

                elif (
                    essential_education_distance < 2
                    and higher_education_distance < 8
                ):
                    education_scores.append(1)

                else:
                    education_scores.append(0.67)

        education_avg = (
            sum(education_scores) / len(education_scores)
            if education_scores else 0.33
        )

        block_scores.append(
            calculate_band_score(education_avg)
        )

        # =========================================================
        # Financial Inclusion Score
        # =========================================================

        financial_scores = []

        for _, facility_row in df_facilities.iterrows():

            financial_distance = get_distance_logic(
                facility_row,
                [
                    "csc_distance_in_km",
                    "bank_mitra_distance_in_km",
                    "bank_branch_distance_in_km",
                    "bank_atm_distance_in_km"
                ],
                logic="max"
            )

            financial_scores.append(
                distance_score(
                    financial_distance,
                    high_limit=2,
                    medium_limit=5
                )
            )

        financial_avg = (
            sum(financial_scores) / len(financial_scores)
            if financial_scores else 0.33
        )

        block_scores.append(
            calculate_band_score(financial_avg)
        )

        # =========================================================
        # Welfare Inclusion Score
        # =========================================================

        social_protection_avg = pd.to_numeric(
            df["social_protection_cat_value"],
            errors="coerce"
        ).mean()

        pds_scores = []

        for _, facility_row in df_facilities.iterrows():

            pds_distance = get_numeric(
                facility_row,
                "pds_distance_in_km"
            )

            pds_scores.append(
                1 if (
                    pd.notnull(pds_distance)
                    and pds_distance < 2
                ) else 0.5
            )

        welfare_avg = (
            social_protection_avg
            + (sum(pds_scores) / len(pds_scores))
        ) / 2

        block_scores.append(
            calculate_band_score(welfare_avg)
        )

        
        # Community Score
        community_score = pd.to_numeric(
            df["institutionalization_cat_value"],
            errors="coerce"
        ).mean()

        civic_score = pd.to_numeric(
            df["civic_infrastructure_cat_value"],
            errors="coerce"
        ).mean()

        community_avg = (community_score + civic_score) / 2

        block_scores.append(
            calculate_band_score(community_avg)
        )

        
        # Livelihood
        livelihood_avg = (
            df[
                [
                    "livelihoods_employment_cat_value",
                    "livelihoods_forest_resources_cat_value",
                    "livelihoods_fisheries_cat_value",
                    "livelihoods_alternative_farming_cat_value",
                    "livelihoods_cottage_traditional_industry_cat_value"
                ]
            ]
            .apply(pd.to_numeric, errors="coerce")
            .mean()
            .mean()
        )

        block_scores.append(
            calculate_band_score(livelihood_avg)
        )


        # Livestock
        livestock_support_avg = pd.to_numeric(
            df["livestock_veterinary_cat_value"],
            errors="coerce"
        ).mean()

        husbandry_scores = []

        for _, facility_row in df_facilities.iterrows():

            husbandry_distance = get_numeric(
                facility_row,
                "agri_industry_dairy_animal_husbandry_distance_in_km"
            )

            if pd.notnull(husbandry_distance) and husbandry_distance < 10:
                husbandry_scores.append(1)

            elif (
                pd.notnull(husbandry_distance)
                and 10 <= husbandry_distance <= 30
            ):
                husbandry_scores.append(0.67)

            else:
                husbandry_scores.append(0.33)

        livestock_avg = (
            livestock_support_avg
            + sum(husbandry_scores)/len(husbandry_scores)
        ) / 2

        block_scores.append(
            calculate_band_score(livestock_avg)
        )

        # Agricultural Productivity
        agri_scores = []

        for _, facility_row in df_facilities.iterrows():

            village_id = facility_row["village_id"]

            village_match = df[
                df["village_id"] == village_id
            ]

            if village_match.empty:
                continue

            row = village_match.iloc[0]

            # reuse same logic from village function
            # compute agri_produce_resource_score
            agri_avg_score = (
                safe_float(row.get("agricultural_markets_cat_value", 0))
                + safe_float(row.get("agriculture_land_cultivation_cat_value", 0))
                + safe_float(row.get("agriculture_irrigation_watershed_cat_value", 0))
                + safe_float(row.get("agriculture_support_services_cat_value", 0))
            ) / 4


            facility_scores = []

            if facility_row is not None:

                agri_facility_configs = [
                    {
                        "column": "agri_industry_agri_support_infrastructure_distance_in_km",
                        "high_limit": 10,
                        "medium_limit": 50,
                    },
                    {
                        "column": "agri_industry_agri_processing_distance_in_km",
                        "high_limit": 5,
                        "medium_limit": 20,
                    },
                    {
                        "column": "agri_industry_co_operatives_societies_distance_in_km",
                        "high_limit": 10,
                        "medium_limit": 30,
                    },
                    {
                        "column": "agri_industry_markets_trading_distance_in_km",
                        "high_limit": 3,
                        "medium_limit": 10,
                    },
                ]

                facility_scores = [
                    distance_score(
                        get_numeric(facility_row, config["column"]),
                        high_limit=config["high_limit"],
                        medium_limit=config["medium_limit"],
                    )
                    for config in agri_facility_configs
                ]

            agri_produce_resource_score = (agri_avg_score + sum(facility_scores)) / (1 + len(facility_scores))

            agri_scores.append(
                agri_produce_resource_score
            )

        block_scores.append(
            calculate_band_score(
                sum(agri_scores)/len(agri_scores)
            )
        )

        #Ecology & Climate Resilience
        organic_farm_avg = pd.to_numeric(
            df["agriculture_organic_farming_cat_value"],
            errors="coerce"
        ).mean()

        nrega_scores = []

        exclude_columns = ["vill_id", "vill_name"]

        year_columns = [
            col for col in df_nrega.columns
            if col not in exclude_columns
        ]

        for _, nrega_row in df_nrega.iterrows():

            total_nrega_assets = sum(
                safe_float(nrega_row.get(col, 0))
                for col in year_columns
            )

            if total_nrega_assets < 100:
                nrega_scores.append(0.33)

            elif 100 <= total_nrega_assets <= 300:
                nrega_scores.append(0.67)

            else:
                nrega_scores.append(1)

        nrega_avg = (
            sum(nrega_scores) / len(nrega_scores)
            if nrega_scores
            else 0.33
        )

        ecology_avg = (
            organic_farm_avg + nrega_avg
        ) / 2

        block_scores.append(
            calculate_band_score(ecology_avg)
        )

        return block_scores

    except Exception as e:

        logger.info(
            "Not able to calculate block scores for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return []
    

def get_basic_infrastructure(state, district, block, village_id, df=None):

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

    try:

        # Only load excel if dataframe not supplied
        if df is None:

            file_path = (
                DATA_DIR_TEMP
                + state.upper()
                + "/"
                + district.upper()
                + "/"
                + district.lower()
                + "_"
                + block.lower()
                + ".xlsx"
            )

            excel_file = pd.ExcelFile(file_path)

            df = pd.read_excel(
                excel_file,
                sheet_name="antyodaya"
            )

            df["village_id"] = (
                df["village_id"]
                .astype(str)
                .str.strip()
            )

        village_id = str(village_id).strip()

        matched_rows = df[
            df["village_id"] == village_id
        ]

        if matched_rows.empty:
            return {}

        row = matched_rows.iloc[0]

        scores = [
            safe_float(row.get("road_connectivity_cat_value", 0)),
            safe_float(row.get("energy_access_cat_value", 0)),
            safe_float(row.get("housing_quality_cat_value", 0)),
        ]

        def cluster_to_color(raw):
            c = str(raw).strip().upper() if raw is not None else ""
            if c == "HIGH":   return "green"
            if c == "MEDIUM": return "yellow"
            return "red"

        def cluster_label(raw):
            c = str(raw).strip().title() if raw is not None else ""
            return c if c in ("High", "Medium", "Low") else "Low"

        road_cluster    = row.get("road_connectivity_cat_cluster")
        energy_cluster  = row.get("energy_access_cat_cluster")
        housing_cluster = row.get("housing_quality_cat_cluster")

        colors = [
            cluster_to_color(road_cluster),
            cluster_to_color(energy_cluster),
            cluster_to_color(housing_cluster),
        ]
        performance = [
            cluster_label(road_cluster),
            cluster_label(energy_cluster),
            cluster_label(housing_cluster),
        ]

        raw_params = {
            cat_key: [
                {
                    "label": p["label"],
                    "value": _format_raw_param(row.get(p["col"]), p["repr"]),
                }
                for p in params
            ]
            for cat_key, params in BASIC_INFRA_RAW_PARAMS.items()
        }

        return {
            "scores":      scores,
            "colors":      colors,
            "performance": performance,
            "raw_params":  raw_params,
        }

    except Exception as e:

        logger.info(
            "Not able to access infrastructure data. Error: %s",
            str(e),
        )

        return {"scores": [], "colors": [], "performance": [], "raw_params": {}}


def get_health_and_wash(state, district, block, village_id, df=None, df_facilities=None):

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

    def get_numeric(row, column):
        return pd.to_numeric(row.get(column, None), errors="coerce")

    def get_distance_logic(row, columns, logic="max"):

        values = [
            get_numeric(row, col)
            for col in columns
        ]

        values = [v for v in values if pd.notnull(v)]

        if not values:
            return None

        return max(values) if logic == "max" else min(values)

    try:

        if df is None or df_facilities is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))

            if df is None:
                df = pd.read_excel(excel_file, sheet_name="antyodaya")
            if df_facilities is None:
                df_facilities = pd.read_excel(excel_file, sheet_name="facilities_proximity")

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        df_facilities["village_id"] = (
            df_facilities["village_id"]
            .astype(str)
            .str.strip()
        )

        village_id = str(village_id).strip()

        matched_rows = df[
            df["village_id"] == village_id
        ]

        if matched_rows.empty:
            logger.info(
                "No data found for village_id %s in %s district, %s block",
                village_id,
                district,
                block,
            )
            return {}

        row = matched_rows.iloc[0]

        facility_match = df_facilities[
            df_facilities["village_id"] == village_id
        ]

        facility_row = (
            facility_match.iloc[0]
            if not facility_match.empty
            else None
        )

        maternal_child_score = safe_float(
            row.get("maternal_child_health_cat_value", 0)
        )

        water_sanitation_score = safe_float(
            row.get("water_sanitation_cat_value", 0)
        )

        if facility_row is not None:

            essential_distance = get_distance_logic(
                facility_row,
                [
                    "health_sub_cen_distance_in_km",
                    "health_phc_distance_in_km"
                ],
                logic="max"
            )

            advanced_distance = get_distance_logic(
                facility_row,
                [
                    "health_chc_distance_in_km",
                    "health_dis_h_distance_in_km",
                    "health_s_t_h_distance_in_km"
                ],
                logic="min"
            )

        maternal_color = _cluster_to_color(row.get("maternal_child_health_cat_cluster"))
        water_color    = _cluster_to_color(row.get("water_sanitation_cat_cluster"))
        maternal_perf  = _cluster_label(row.get("maternal_child_health_cat_cluster"))
        water_perf     = _cluster_label(row.get("water_sanitation_cat_cluster"))

        raw_params = {
            "maternal_child_health": _build_raw_params(row, SECTION_RAW_PARAMS["maternal_child_health"]),
            "water_sanitation":      _build_raw_params(row, SECTION_RAW_PARAMS["water_sanitation"]),
        }

        return {
            "data":        [
                maternal_child_score,
                water_sanitation_score,
                round(essential_distance, 2) if essential_distance is not None else None,
                round(advanced_distance, 2)   if advanced_distance   is not None else None,
            ],
            "colors":      [maternal_color, water_color],
            "performance": [maternal_perf, water_perf],
            "raw_params":  raw_params,
        }

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )
        return {}


def get_education_institutions(state, district, block, village_id, df_facilities=None):

    def get_numeric(row, column):
        return pd.to_numeric(row.get(column, None), errors="coerce")

    def get_distance_logic(row, columns, logic="max"):

        values = [
            get_numeric(row, col)
            for col in columns
        ]

        values = [v for v in values if pd.notnull(v)]

        if not values:
            return None

        return max(values) if logic == "max" else min(values)

    try:

        if df_facilities is None:

            file_path = (
                DATA_DIR_TEMP
                + state.upper()
                + "/"
                + district.upper()
                + "/"
                + district.lower()
                + "_"
                + block.lower()
                + ".xlsx"
            )

            excel_file = pd.ExcelFile(file_path)

            df_facilities = pd.read_excel(
                excel_file,
                sheet_name="facilities_proximity"
            )

            df_facilities["village_id"] = (
                df_facilities["village_id"]
                .astype(str)
                .str.strip()
            )

        village_id = str(village_id).strip()

        facility_match = df_facilities[
            df_facilities["village_id"] == village_id
        ]

        if facility_match.empty:
            logger.info(
                "No education data found for village_id %s in %s district, %s block",
                village_id,
                district,
                block,
            )
            return []

        facility_row = facility_match.iloc[0]

        essential_education_distance = get_distance_logic(
            facility_row,
            [
                "school_primary_distance_in_km",
                "school_upper_primary_distance_in_km",
                "school_secondary_distance_in_km"
            ],
            logic="max"
        )

        higher_education_distance = get_distance_logic(
            facility_row,
            [
                "school_higher_secondary_distance_in_km",
                "college_distance_in_km",
                "universities_distance_in_km"
            ],
            logic="min"
        )

        color = "yellow"

        if (essential_education_distance is not None and higher_education_distance is not None):

            if (essential_education_distance > 2 and higher_education_distance > 8):
                color = "red"

            elif (essential_education_distance < 2 and higher_education_distance < 8):
                color = "green"

            else:
                color = "yellow"

        return [
            round(essential_education_distance, 2)
            if essential_education_distance is not None
            else None,

            round(higher_education_distance, 2)
            if higher_education_distance is not None
            else None,

            color
        ]

    except Exception as e:
        logger.info(
            "Not able to access education data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )
        return []


def get_financial_inclusion(state, district, block, village_id, df=None, df_facilities=None):

    def get_numeric(row, column):
        return pd.to_numeric(row.get(column, None), errors="coerce")

    def distance_score(distance, high_limit, medium_limit=None):

        if pd.isnull(distance):
            return 0.33

        if medium_limit is None:
            return 1 if distance < high_limit else 0.33

        if distance < high_limit:
            return 1

        elif high_limit <= distance <= medium_limit:
            return 0.66

        return 0.33

    def get_distance_logic(row, columns, logic="max"):

        values = [
            get_numeric(row, col)
            for col in columns
        ]

        values = [v for v in values if pd.notnull(v)]

        if not values:
            return None

        return max(values) if logic == "max" else min(values)

    try:

        if df is None or df_facilities is None:
            file_path = (
                DATA_DIR_TEMP
                + state.upper()
                + "/"
                + district.upper()
                + "/"
                + district.lower()
                + "_"
                + block.lower()
                + ".xlsx"
            )

            excel_file = pd.ExcelFile(file_path)

            if df is None:
                df = pd.read_excel(excel_file, sheet_name="antyodaya")

            if df_facilities is None:
                df_facilities = pd.read_excel(
                    excel_file,
                    sheet_name="facilities_proximity"
                )

        df["village_id"] = df["village_id"].astype(str).str.strip()
        df_facilities["village_id"] = df_facilities["village_id"].astype(str).str.strip()

        village_id = str(village_id).strip()

        facility_match = df_facilities[
            df_facilities["village_id"] == village_id
        ]

        if facility_match.empty:
            logger.info(
                "No financial inclusion data found for village_id %s",
                village_id,
            )
            return {}

        facility_row = facility_match.iloc[0]

        financial_distance = get_distance_logic(
            facility_row,
            [
                "csc_distance_in_km",
                "bank_mitra_distance_in_km",
                "bank_branch_distance_in_km",
                "bank_atm_distance_in_km"
            ],
            logic="max"
        )

        financial_inclusion_score = distance_score(
            financial_distance,
            high_limit=2,
            medium_limit=5
        )

        # Cluster-based color from antyodaya
        anty_match = df[df["village_id"] == village_id]
        anty_row   = anty_match.iloc[0] if not anty_match.empty else None

        if anty_row is not None and _safe_cluster(anty_row.get("financial_inclusion_cat_cluster")):
            color = _cluster_to_color(anty_row.get("financial_inclusion_cat_cluster"))
            perf  = _cluster_label(anty_row.get("financial_inclusion_cat_cluster"))
        else:
            color = "green" if financial_inclusion_score == 1 else "red"
            perf  = "High"  if financial_inclusion_score == 1 else "Low"

        raw_params = {
            "financial_inclusion": _build_raw_params(
                anty_row if anty_row is not None else {},
                SECTION_RAW_PARAMS["financial_inclusion"]
            ),
        }

        return {
            "data": [
                financial_inclusion_score,
                round(financial_distance, 2) if financial_distance is not None else None,
                color,
            ],
            "colors":      [color],
            "performance": [perf],
            "raw_params":  raw_params,
        }

    except Exception as e:
        logger.info(
            "Not able to access financial inclusion data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )
        return {}


def get_welfare_inclusion(state, district, block, village_id, df=None, df_facilities=None):

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

    def get_numeric(row, column):
        return pd.to_numeric(row.get(column, None), errors="coerce")

    try:
        if df is None or df_facilities is None:

            file_path = (
                DATA_DIR_TEMP
                + state.upper()
                + "/"
                + district.upper()
                + "/"
                + district.lower()
                + "_"
                + block.lower()
                + ".xlsx"
            )

            excel_file = pd.ExcelFile(file_path)

            if df is None:

                df = pd.read_excel(
                    excel_file,
                    sheet_name="antyodaya"
                )

                df["village_id"] = (
                    df["village_id"]
                    .astype(str)
                    .str.strip()
                )

            if df_facilities is None:

                df_facilities = pd.read_excel(
                    excel_file,
                    sheet_name="facilities_proximity"
                )

                df_facilities["village_id"] = (
                    df_facilities["village_id"]
                    .astype(str)
                    .str.strip()
                )

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        df_facilities["village_id"] = (
            df_facilities["village_id"]
            .astype(str)
            .str.strip()
        )

        village_id = str(village_id).strip()

        matched_rows = df[
            df["village_id"] == village_id
        ]

        if matched_rows.empty:
            logger.info(
                "No data found for village_id %s",
                village_id
            )
            return {}

        row = matched_rows.iloc[0]

        facility_match = df_facilities[
            df_facilities["village_id"] == village_id
        ]

        facility_row = (
            facility_match.iloc[0]
            if not facility_match.empty
            else None
        )

        social_protection_score = safe_float(
            row.get("social_protection_cat_value", 0)
        )

        pds_distance = None

        if facility_row is not None:

            pds_distance = get_numeric(
                facility_row,
                "pds_distance_in_km"
            )

        color = _cluster_to_color(row.get("social_protection_cat_cluster"))
        perf  = _cluster_label(row.get("social_protection_cat_cluster"))

        raw_params = {
            "social_protection": _build_raw_params(row, SECTION_RAW_PARAMS["social_protection"]),
        }

        return {
            "data": [
                social_protection_score,
                round(pds_distance, 2) if pd.notnull(pds_distance) else None,
                color,
            ],
            "colors":      [color],
            "performance": [perf],
            "raw_params":  raw_params,
        }

    except Exception as e:

        logger.info(
            "Not able to access welfare inclusion data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}


def get_community_institutes(state, district, block, village_id, df=None):

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

    try:

        if df is None:

            file_path = (
                DATA_DIR_TEMP
                + state.upper()
                + "/"
                + district.upper()
                + "/"
                + district.lower()
                + "_"
                + block.lower()
                + ".xlsx"
            )

            excel_file = pd.ExcelFile(file_path)

            df = pd.read_excel(
                excel_file,
                sheet_name="antyodaya"
            )

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        village_id = str(village_id).strip()

        matched_rows = df[
            df["village_id"] == village_id
        ]

        if matched_rows.empty:
            logger.info(
                "No data found for village_id %s in %s district, %s block",
                village_id,
                district,
                block,
            )
            return {}

        row = matched_rows.iloc[0]

        community_score = safe_float(
            row.get("institutionalization_cat_value", 0)
        )

        civic_score = safe_float(
            row.get("civic_infrastructure_cat_value", 0)
        )

        community_color = _cluster_to_color(row.get("institutionalization_cat_cluster"))
        civic_color     = _cluster_to_color(row.get("civic_infrastructure_cat_cluster"))
        community_perf  = _cluster_label(row.get("institutionalization_cat_cluster"))
        civic_perf      = _cluster_label(row.get("civic_infrastructure_cat_cluster"))

        raw_params = {
            "institutionalization": _build_raw_params(row, SECTION_RAW_PARAMS["institutionalization"]),
            "civic_infrastructure": _build_raw_params(row, SECTION_RAW_PARAMS["civic_infrastructure"]),
        }

        return {
            "data":        [community_score, civic_score, community_color, civic_color],
            "colors":      [community_color, civic_color],
            "performance": [community_perf, civic_perf],
            "raw_params":  raw_params,
        }

    except Exception as e:

        logger.info(
            "Not able to access community institution data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}


def get_livelihood_diversification(state, district, block, village_id, df=None):

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

    try:

        if df is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))
            df = pd.read_excel(excel_file, sheet_name="antyodaya")

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        village_id = str(village_id).strip()

        matched_rows = df[
            df["village_id"] == village_id
        ]

        if matched_rows.empty:
            logger.info(
                "No data found for village_id %s in %s district, %s block",
                village_id,
                district,
                block,
            )
            return {}

        row = matched_rows.iloc[0]

        scores = [
            safe_float(row.get("livelihoods_employment_cat_value", 0)),
            safe_float(row.get("livelihoods_forest_resources_cat_value", 0)),
            safe_float(row.get("livelihoods_alternative_farming_cat_value", 0)),
            safe_float(row.get("livelihoods_fisheries_cat_value", 0)),
            safe_float(row.get("livelihoods_cottage_traditional_industry_cat_value", 0)),
        ]

        cluster_cols = [
            "livelihoods_employment_cat_cluster",
            "livelihoods_forest_resources_cat_cluster",
            "livelihoods_alternative_farming_cat_cluster",
            "livelihoods_fisheries_cat_cluster",
            "livelihoods_cottage_traditional_industry_cat_cluster",
        ]
        colors      = [_cluster_to_color(row.get(c)) for c in cluster_cols]
        performance = [_cluster_label(row.get(c))     for c in cluster_cols]

        raw_params = {
            "livelihoods_employment":               _build_raw_params(row, SECTION_RAW_PARAMS["livelihoods_employment"]),
            "livelihoods_forest_resources":         _build_raw_params(row, SECTION_RAW_PARAMS["livelihoods_forest_resources"]),
            "livelihoods_alternative_farming":      _build_raw_params(row, SECTION_RAW_PARAMS["livelihoods_alternative_farming"]),
            "livelihoods_fisheries":                _build_raw_params(row, SECTION_RAW_PARAMS["livelihoods_fisheries"]),
            "livelihoods_cottage_traditional_industry": _build_raw_params(row, SECTION_RAW_PARAMS["livelihoods_cottage_traditional_industry"]),
        }

        return {
            "data":        scores,
            "colors":      colors,
            "performance": performance,
            "raw_params":  raw_params,
        }

    except Exception as e:

        logger.info(
            "Not able to access livelihood diversification data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}


def get_livestock_management(state, district, block, village_id, df=None, df_facilities=None):

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

    def get_numeric(row, column):
        return pd.to_numeric(row.get(column, None), errors="coerce")

    try:

        if df is None or df_facilities is None:

            file_path = (
                DATA_DIR_TEMP
                + state.upper()
                + "/"
                + district.upper()
                + "/"
                + district.lower()
                + "_"
                + block.lower()
                + ".xlsx"
            )

            excel_file = pd.ExcelFile(file_path)

            if df is None:
                df = pd.read_excel(
                    excel_file,
                    sheet_name="antyodaya"
                )

            if df_facilities is None:
                df_facilities = pd.read_excel(
                    excel_file,
                    sheet_name="facilities_proximity"
                )

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        df_facilities["village_id"] = (
            df_facilities["village_id"]
            .astype(str)
            .str.strip()
        )

        village_id = str(village_id).strip()

        matched_rows = df[df["village_id"] == village_id]

        if matched_rows.empty:
            logger.info(
                "No data found for village_id %s in %s district, %s block",
                village_id,
                district,
                block,
            )
            return {}

        row = matched_rows.iloc[0]

        facility_match = df_facilities[
            df_facilities["village_id"] == village_id
        ]

        facility_row = (
            facility_match.iloc[0]
            if not facility_match.empty
            else None
        )

        livestock_support_score = safe_float(
            row.get("livestock_veterinary_cat_value", 0)
        )

        livestock_pasture_score = safe_float(row.get("livelihoods_common_resources_cat_value", 0))

        husbandry_distance = None

        if facility_row is not None:

            husbandry_distance = get_numeric(
                facility_row,
                "agri_industry_dairy_animal_husbandry_distance_in_km"
            )

        veterinary_color = _cluster_to_color(row.get("livestock_veterinary_cat_cluster"))
        pasture_color    = _cluster_to_color(row.get("livelihoods_common_resources_cat_cluster"))
        veterinary_perf  = _cluster_label(row.get("livestock_veterinary_cat_cluster"))
        pasture_perf     = _cluster_label(row.get("livelihoods_common_resources_cat_cluster"))

        raw_params = {
            "livestock_veterinary":       _build_raw_params(row, SECTION_RAW_PARAMS["livestock_veterinary"]),
            "livelihoods_common_resources": _build_raw_params(row, SECTION_RAW_PARAMS["livelihoods_common_resources"]),
        }

        return {
            "data": [
                livestock_support_score,
                livestock_pasture_score,
                round(husbandry_distance, 2) if pd.notnull(husbandry_distance) else None,
                veterinary_color,
                pasture_color,
            ],
            "colors":      [veterinary_color, pasture_color],
            "performance": [veterinary_perf, pasture_perf],
            "raw_params":  raw_params,
        }

    except Exception as e:

        logger.info(
            "Not able to access livestock management data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}


def get_livestock_count(state, district, block, village_id, df_livestock=None):

    def safe_int(value):
        try:
            if value is None or pd.isna(value):
                return None
            v = int(float(value))
            return v if v >= 0 else None
        except:
            return None

    try:

        if df_livestock is None:

            file_path = (
                DATA_DIR_TEMP
                + state.upper()
                + "/"
                + district.upper()
                + "/"
                + district.lower()
                + "_"
                + block.lower()
                + ".xlsx"
            )

            excel_file = pd.ExcelFile(file_path)

            df_livestock = pd.read_excel(
                excel_file,
                sheet_name="livestock"
            )

        df_livestock["village_id"] = (
            df_livestock["village_id"]
            .astype(str)
            .str.strip()
        )

        village_id = str(village_id).strip()

        matched_rows = df_livestock[
            df_livestock["village_id"] == village_id
        ]

        if matched_rows.empty:
            logger.info(
                "No livestock count data found for village_id %s in %s district, %s block",
                village_id,
                district,
                block,
            )
            return {}

        row = matched_rows.iloc[0]

        status = str(row.get("data_availability_status", "")).strip().lower()
        if status != "matched":
            logger.info(
                "Livestock data not available for village_id %s in %s district, %s block: %s",
                village_id, district, block, status,
            )
            return {}

        return {
            "large_animals_total": safe_int(row.get("large_animals_total")),
            "cattle_total":        safe_int(row.get("cattle_total")),
            "buffalo_total":       safe_int(row.get("buffalo_total")),
            "small_animals_total": safe_int(row.get("small_animals_total")),
            "sheep_total":         safe_int(row.get("sheep_total")),
            "goat_total":          safe_int(row.get("goat_total")),
            "pig_total":           safe_int(row.get("pig_total")),
            "all_livestock_total": safe_int(row.get("all_livestock_total")),
        }

    except Exception as e:

        logger.info(
            "Not able to access livestock count data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}


def get_land_cultivation(state, district, block, village_id, df=None):

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

    try:
        if df is None:
            file_path = (
                DATA_DIR_TEMP
                + state.upper()
                + "/"
                + district.upper()
                + "/"
                + district.lower()
                + "_"
                + block.lower()
                + ".xlsx"
            )

            excel_file = pd.ExcelFile(file_path)

            df = pd.read_excel(
                excel_file,
                sheet_name="antyodaya"
            )

            df["village_id"] = (
                df["village_id"]
                .astype(str)
                .str.strip()
            )

        village_id = str(village_id).strip()

        matched_rows = df[df["village_id"] == village_id]

        if matched_rows.empty:
            logger.info(
                "No data found for village_id %s in %s district, %s block",
                village_id,
                district,
                block,
            )
            return {}

        row = matched_rows.iloc[0]

        land_utilization_score = safe_float(
            row.get("agriculture_land_cultivation_cat_value", 0)
        )

        # Seasonal cultivation score: kharif sown / total cultivable, clamped to [0, 1]
        kharif_ha = safe_float(row.get("net_sown_area_kharif_in_hac", 0))
        cultivable_ha = safe_float(row.get("total_cultivable_area_in_hac", 0))
        if cultivable_ha > 0:
            seasonal_cultivation_score = min(kharif_ha / cultivable_ha, 1.0)
        else:
            seasonal_cultivation_score = 0.0

        # Map color from numeric score (consistent with all other sections)
        if land_utilization_score < 0.33:
            land_utilization_color = "red"
        elif land_utilization_score <= 0.66:
            land_utilization_color = "yellow"
        else:
            land_utilization_color = "green"

        # Seasonal cultivation color (score-based, 3 levels)
        if seasonal_cultivation_score <= 0.33:
            seasonal_cultivation_color = "red"
        elif seasonal_cultivation_score <= 0.66:
            seasonal_cultivation_color = "yellow"
        else:
            seasonal_cultivation_color = "green"

        # Text cluster
        cultivation_cluster = str(
            row.get("agriculture_land_cultivation_cat_cluster", "Low")
        ).strip()

        raw_params = {
            "agriculture_land_cultivation": _build_raw_params(row, SECTION_RAW_PARAMS["agriculture_land_cultivation"]),
        }

        return {
            "data": [
                round(land_utilization_score, 4),
                round(seasonal_cultivation_score, 4),
                land_utilization_color,
                cultivation_cluster,
                seasonal_cultivation_color,
            ],
            "colors":      [land_utilization_color, seasonal_cultivation_color],
            "performance": [cultivation_cluster, "High" if seasonal_cultivation_score > 0.66 else ("Medium" if seasonal_cultivation_score >= 0.33 else "Low")],
            "raw_params":  raw_params,
        }

    except Exception as e:

        logger.info(
            "Not able to access Land cultivation data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}


def get_all_villages_land_cultivation(state, district, block, df=None):

    try:

        if df is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))
            df = pd.read_excel(excel_file, sheet_name="antyodaya")

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        village_ids = [
            str(v).strip()
            for v in df["village_id"].dropna().unique()
            if str(v).strip() not in ("", "0")
        ]

        village_data = {}

        for village_id in village_ids:

            cultivation_info = get_land_cultivation(
                state,
                district,
                block,
                village_id,
                df=df
            )

            if not cultivation_info:
                village_data[village_id] = {
                    "land_utilization_color": "black",
                    "seasonal_cultivation_color": "black"
                }
                continue

            village_data[village_id] = {
                "land_utilization_color": cultivation_info[2],
                "seasonal_cultivation_color": cultivation_info[4]
            }

        return village_data

    except Exception as e:

        logger.info(
            "Not able to access land cultivation map data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}


def get_irrigation_Infra(state, district, block, village_id, df=None):

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

    try:

        if df is None:

            file_path = (
                DATA_DIR_TEMP
                + state.upper()
                + "/"
                + district.upper()
                + "/"
                + district.lower()
                + "_"
                + block.lower()
                + ".xlsx"
            )

            excel_file = pd.ExcelFile(file_path)

            df = pd.read_excel(
                excel_file,
                sheet_name="antyodaya"
            )

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        village_id = str(village_id).strip()

        matched_rows = df[
            df["village_id"] == village_id
        ]

        if matched_rows.empty:
            logger.info(
                "No data found for village_id %s in %s district, %s block",
                village_id,
                district,
                block,
            )
            return {}

        row = matched_rows.iloc[0]

        irrigation_watershed_score = safe_float(
            row.get(
                "agriculture_irrigation_watershed_cat_value",
                0
            )
        )

        cluster_raw = row.get("agriculture_irrigation_watershed_cat_cluster", None)
        try:
            cluster_valid = cluster_raw is not None and not pd.isna(cluster_raw)
        except Exception:
            cluster_valid = False

        if cluster_valid:
            irrigation_cluster = str(cluster_raw).strip().lower()
            if irrigation_cluster == "high":
                irrigation_watershed_color = "green"
            elif irrigation_cluster == "medium":
                irrigation_watershed_color = "yellow"
            else:
                irrigation_watershed_color = "red"
            irrigation_cluster_label = str(cluster_raw).strip().title()
        else:
            # fallback to score-based
            if irrigation_watershed_score < 0.33:
                irrigation_watershed_color = "red"
                irrigation_cluster_label = "Low"
            elif irrigation_watershed_score <= 0.66:
                irrigation_watershed_color = "yellow"
                irrigation_cluster_label = "Medium"
            else:
                irrigation_watershed_color = "green"
                irrigation_cluster_label = "High"

        raw_params = {
            "agriculture_irrigation_watershed": _build_raw_params(row, SECTION_RAW_PARAMS["agriculture_irrigation_watershed"]),
        }

        return {
            "data":        [irrigation_watershed_score, irrigation_watershed_color, irrigation_cluster_label],
            "colors":      [irrigation_watershed_color],
            "performance": [irrigation_cluster_label],
            "raw_params":  raw_params,
        }

    except Exception as e:

        logger.info(
            "Not able to access irrigation infrastructure data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}
    

def get_agri_support_service(state, district, block, village_id, df=None, df_facilities=None):

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

    def get_numeric(row, column):
        return pd.to_numeric(row.get(column, None), errors="coerce")

    def get_distance_logic(row, columns, logic="max"):

        values = [
            get_numeric(row, col)
            for col in columns
        ]

        values = [v for v in values if pd.notnull(v)]

        if not values:
            return None

        return max(values) if logic == "max" else min(values)

    try:

        if df is None or df_facilities is None:

            file_path = (
                DATA_DIR_TEMP
                + state.upper()
                + "/"
                + district.upper()
                + "/"
                + district.lower()
                + "_"
                + block.lower()
                + ".xlsx"
            )

            excel_file = pd.ExcelFile(file_path)

            if df is None:
                df = pd.read_excel(
                    excel_file,
                    sheet_name="antyodaya"
                )

            if df_facilities is None:
                df_facilities = pd.read_excel(
                    excel_file,
                    sheet_name="facilities_proximity"
                )

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        df_facilities["village_id"] = (
            df_facilities["village_id"]
            .astype(str)
            .str.strip()
        )

        village_id = str(village_id).strip()

        matched_rows = df[
            df["village_id"] == village_id
        ]

        if matched_rows.empty:
            logger.info(
                "No data found for village_id %s in %s district, %s block",
                village_id,
                district,
                block,
            )
            return {}

        row = matched_rows.iloc[0]

        facility_match = df_facilities[
            df_facilities["village_id"] == village_id
        ]

        facility_row = (
            facility_match.iloc[0]
            if not facility_match.empty
            else None
        )

        # =====================================================
        # Scores from Antyodaya
        # =====================================================

        agri_support_score = safe_float(
            row.get(
                "agriculture_support_services_cat_value",
                0
            )
        )

        agri_market_score = safe_float(
            row.get(
                "agricultural_markets_cat_value",
                0
            )
        )

        # =====================================================
        # Distances from Facilities
        # =====================================================

        post_harvest_distance = None
        apmc_access_distance = None
        agri_support_socities_distance = None
        agri_support_infra_distance = None
        agri_processing_distance = None

        if facility_row is not None:

            post_harvest_distance = get_distance_logic(
                facility_row,
                [
                    "agri_industry_storage_warehousing_distance_in_km",
                    "agri_industry_distribution_utilities_distance_in_km",
                    "agri_industry_agri_processing_distance_in_km",
                    "agri_industry_industrial_manufacturing_distance_in_km"
                ],
                logic="min"
            )

            apmc_access_distance = get_distance_logic(
                facility_row,
                [
                    "apmc_markets_distance_in_km",
                    "agri_industry_markets_trading_distance_in_km"
                ],
                logic="min"
            )

            agri_support_socities_distance = get_distance_logic(
                facility_row,
                [
                    "agri_industry_co_operatives_societies_distance_in_km",
                ],
                logic="min"
            )

            agri_support_infra_distance = get_distance_logic(
                facility_row,
                [
                    "agri_industry_agri_support_infrastructure_distance_in_km",
                ],
                logic="min"
            )

            agri_processing_distance = get_distance_logic(
                facility_row,
                [
                    "agri_industry_agri_processing_distance_in_km",
                ],
                logic="min"
            )

        # =====================================================
        # Colors (cluster-based)
        # =====================================================

        agri_support_color = _cluster_to_color(row.get("agriculture_support_services_cat_cluster"))
        agri_market_color  = _cluster_to_color(row.get("agricultural_markets_cat_cluster"))
        agri_support_perf  = _cluster_label(row.get("agriculture_support_services_cat_cluster"))
        agri_market_perf   = _cluster_label(row.get("agricultural_markets_cat_cluster"))

        raw_params = {
            "agriculture_support_services": _build_raw_params(row, SECTION_RAW_PARAMS["agriculture_support_services"]),
            "agricultural_markets":         _build_raw_params(row, SECTION_RAW_PARAMS["agricultural_markets"]),
        }

        return {
            "data": [
                agri_support_score,                                                            # index 0
                agri_market_score,                                                             # index 1
                round(post_harvest_distance, 2)        if post_harvest_distance        is not None else None,  # index 2
                round(apmc_access_distance, 2)         if apmc_access_distance         is not None else None,  # index 3
                agri_support_color,                                                            # index 4
                agri_market_color,                                                             # index 5
                round(agri_support_socities_distance, 2) if agri_support_socities_distance is not None else None,  # index 6
                round(agri_support_infra_distance, 2)  if agri_support_infra_distance  is not None else None,  # index 7
                round(agri_processing_distance, 2)     if agri_processing_distance     is not None else None,  # index 8
            ],
            "colors":      [agri_support_color, agri_market_color],
            "performance": [agri_support_perf, agri_market_perf],
            "raw_params":  raw_params,
        }

    except Exception as e:

        logger.info(
            "Not able to access agri support service data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}


def get_ecological_climate_resiliance(state, district, block, village_id, df=None, df_nrega=None):

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

    try:

        if df is None or df_nrega is None:

            file_path = (
                DATA_DIR_TEMP
                + state.upper()
                + "/"
                + district.upper()
                + "/"
                + district.lower()
                + "_"
                + block.lower()
                + ".xlsx"
            )

            excel_file = pd.ExcelFile(file_path)

            if df is None:
                df = pd.read_excel(
                    excel_file,
                    sheet_name="antyodaya"
                )

            if df_nrega is None:
                df_nrega = pd.read_excel(
                    excel_file,
                    sheet_name="nrega_assets_village"
                )

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        df_nrega["vill_id"] = (
            df_nrega["vill_id"]
            .astype(str)
            .str.strip()
        )

        village_id = str(village_id).strip()

        # =====================================================
        # Organic Farming Score
        # =====================================================

        matched_rows = df[
            df["village_id"] == village_id
        ]

        if matched_rows.empty:
            return {}

        row = matched_rows.iloc[0]

        organic_farming_score = safe_float(
            row.get(
                "agriculture_organic_farming_cat_value",
                0
            )
        )

        # Cluster-based color for organic farming
        organic_farming_color = _cluster_to_color(row.get("agriculture_organic_farming_cat_cluster"))
        organic_farming_perf  = _cluster_label(row.get("agriculture_organic_farming_cat_cluster"))

        raw_params = {
            "agriculture_organic_farming": _build_raw_params(row, SECTION_RAW_PARAMS["agriculture_organic_farming"]),
        }

        # =====================================================
        # NREGA Assets
        # =====================================================

        nrega_match = df_nrega[
            df_nrega["vill_id"] == village_id
        ]

        if nrega_match.empty:

            return {
                "data": [
                    None,                   # year_range
                    {},                     # category_counts
                    0,                      # total_work_count
                    "red",                  # nrega_work_color
                    organic_farming_score,
                    organic_farming_color,
                ],
                "colors":      [organic_farming_color, "red"],
                "performance": [organic_farming_perf, "Low"],
                "raw_params":  raw_params,
            }

        nrega_row = nrega_match.iloc[0]

        category_counts = {}

        years = set()

        for column in df_nrega.columns:

            if column in ["vill_id", "vill_name"]:
                continue

            try:

                work_type, year = column.rsplit("_", 1)

                year = int(year)

                years.add(year)

            except Exception:
                continue

            category_name = (
                work_type
                .replace("_count", "")
                .replace("_", " ")
                .strip()
            )

            category_counts.setdefault(
                category_name,
                0
            )

            category_counts[category_name] += safe_float(
                nrega_row.get(column, 0)
            )

        year_range = {
            "from_year": min(years) if years else None,
            "to_year": max(years) if years else None,
        }

        total_work_count = sum(
            category_counts.values()
        )

        nrega_work_color = (
            "green"
            if total_work_count > 100
            else "red"
        )

        return {
            "data": [
                year_range,             # index 0
                category_counts,        # index 1
                total_work_count,       # index 2
                nrega_work_color,       # index 3
                organic_farming_score,  # index 4
                organic_farming_color,  # index 5
            ],
            "colors":      [organic_farming_color, nrega_work_color],
            "performance": [organic_farming_perf, "High" if nrega_work_color == "green" else "Low"],
            "raw_params":  raw_params,
        }

    except Exception as e:

        logger.info(
            "Not able to access ecology data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}
    


#? Get Tehsil Map Data
def get_all_villages_basic_infrastructure(state, district, block, df=None, df_nrega=None):
    try:
        if df is None or df_nrega is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))
            if df is None:
                df = pd.read_excel(excel_file, sheet_name="antyodaya")
            if df_nrega is None:
                df_nrega = pd.read_excel(excel_file, sheet_name="nrega_assets_village")

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        village_ids = (
            df_nrega["vill_id"]
            .dropna()
            .astype(str)
            .str.strip()
        )

        village_ids = [
            vid
            for vid in village_ids.unique()
            if vid and vid != "0"
        ]

        result = {}

        for village_id in village_ids:

            matched = df[df["village_id"] == village_id]

            if matched.empty:
                result[village_id] = {
                    "road_color": "black",
                    "energy_color": "black",
                    "housing_color": "black",
                }
                continue

            row = matched.iloc[0]

            def _c2col(raw):
                c = str(raw).strip().upper() if raw is not None else ""
                if c == "HIGH":   return "green"
                if c == "MEDIUM": return "yellow"
                return "red"

            result[village_id] = {
                "road_color":    _c2col(row.get("road_connectivity_cat_cluster")),
                "energy_color":  _c2col(row.get("energy_access_cat_cluster")),
                "housing_color": _c2col(row.get("housing_quality_cat_cluster")),
            }

        return result

    except Exception as e:

        logger.info(
            "Error calculating infrastructure colors for all villages: %s",
            str(e)
        )

        return {}


def get_all_villages_health_and_wash(state, district, block, df=None):

    try:

        if df is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))
            df = pd.read_excel(excel_file, sheet_name="antyodaya")

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        village_ids = [
            str(v).strip()
            for v in df["village_id"].dropna().unique()
            if str(v).strip() not in ("", "0")
        ]

        village_data = {}

        for village_id in village_ids:

            matched_rows = df[
                df["village_id"] == village_id
            ]

            if matched_rows.empty:

                village_data[village_id] = {
                    "maternal_child_health_color": "black",
                    "water_sanitation_color": "black"
                }

                continue

            row = matched_rows.iloc[0]

            village_data[village_id] = {
                "maternal_child_health_color": _cluster_to_color(row.get("maternal_child_health_cat_cluster")),
                "water_sanitation_color":      _cluster_to_color(row.get("water_sanitation_cat_cluster")),
            }

        return village_data

    except Exception as e:

        logger.info(
            "Not able to access health and wash data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}    


def get_all_villages_education_institutions(state, district, block, df_facilities=None, df_nrega=None):
    try:

        if df_facilities is None or df_nrega is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))
            if df_nrega is None:
                df_nrega = pd.read_excel(excel_file, sheet_name="nrega_assets_village")
            if df_facilities is None:
                df_facilities = pd.read_excel(excel_file, sheet_name="facilities_proximity")

        df_facilities["village_id"] = (
            df_facilities["village_id"]
            .astype(str)
            .str.strip()
        )

        village_ids = (
            df_nrega["vill_id"]
            .dropna()
            .astype(str)
            .str.strip()
        )

        village_ids = [
            vid
            for vid in village_ids.unique()
            if vid and vid != "0"
        ]

        result = {}

        for village_id in village_ids:

            education_data = get_education_institutions(state, district, block, village_id, df_facilities=df_facilities)

            if not education_data:

                result[village_id] = {
                    "education_color": "black"
                }

                continue

            result[village_id] = {
                "education_color": education_data[2]
            }

        return result

    except Exception as e:

        logger.info(
            "Error calculating education colors for all villages: %s",
            str(e)
        )

        return {}


def get_all_villages_financial_inclusion(state, district, block, df=None, df_facilities=None, df_nrega=None):

    try:

        if df is None or df_nrega is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))
            if df is None:
                df = pd.read_excel(excel_file, sheet_name="antyodaya")
            if df_nrega is None:
                df_nrega = pd.read_excel(excel_file, sheet_name="nrega_assets_village")

        df["village_id"] = df["village_id"].astype(str).str.strip()

        result = {}

        for _, row in df.iterrows():
            village_id = str(row.get("village_id", "")).strip()
            if not village_id or village_id == "0":
                continue
            result[village_id] = {
                "financial_color": _cluster_to_color(row.get("financial_inclusion_cat_cluster")),
            }

        return result

    except Exception as e:

        logger.info(
            "Error calculating financial inclusion colors for all villages: %s",
            str(e)
        )

        return {}


def get_all_villages_welfare_inclusion(state, district, block, df=None, df_facilities=None, df_nrega=None):

    try:

        if df is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))
            df = pd.read_excel(excel_file, sheet_name="antyodaya")

        df["village_id"] = df["village_id"].astype(str).str.strip()

        result = {}
        for _, row in df.iterrows():
            village_id = str(row.get("village_id", "")).strip()
            if not village_id or village_id == "0":
                continue
            result[village_id] = {
                "welfare_color": _cluster_to_color(row.get("social_protection_cat_cluster")),
            }

        return result

    except Exception as e:

        logger.info(
            "Error calculating welfare inclusion colors for all villages: %s",
            str(e)
        )

        return {}


def get_all_villages_community_institutes(state, district, block, df=None):

    try:

        if df is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))
            df = pd.read_excel(excel_file, sheet_name="antyodaya")

        df["village_id"] = df["village_id"].astype(str).str.strip()

        village_data = {}
        for _, row in df.iterrows():
            village_id = str(row.get("village_id", "")).strip()
            if not village_id or village_id == "0":
                continue
            village_data[village_id] = {
                "community_color": _cluster_to_color(row.get("institutionalization_cat_cluster")),
                "civic_color":     _cluster_to_color(row.get("civic_infrastructure_cat_cluster")),
            }

        return village_data

    except Exception as e:

        logger.info(
            "Not able to access community institution data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}


def get_all_villages_livestock_management(state, district, block, df=None, df_facilities=None):

    try:

        if df is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))
            df = pd.read_excel(excel_file, sheet_name="antyodaya")

        df["village_id"] = df["village_id"].astype(str).str.strip()

        village_data = {}
        for _, row in df.iterrows():
            village_id = str(row.get("village_id", "")).strip()
            if not village_id or village_id == "0":
                continue
            village_data[village_id] = {
                "veterinary_color": _cluster_to_color(row.get("livestock_veterinary_cat_cluster")),
                "pasture_color":    _cluster_to_color(row.get("livelihoods_common_resources_cat_cluster")),
            }

        return village_data

    except Exception as e:

        logger.info(
            "Not able to access livestock management data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}


def get_all_villages_irrigation_infra(state, district, block, df=None):

    try:

        if df is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))
            df = pd.read_excel(excel_file, sheet_name="antyodaya")

        df["village_id"] = df["village_id"].astype(str).str.strip()

        village_data = {}
        for _, row in df.iterrows():
            village_id = str(row.get("village_id", "")).strip()
            if not village_id or village_id == "0":
                continue
            cluster_raw = row.get("agriculture_irrigation_watershed_cat_cluster")
            if _safe_cluster(cluster_raw):
                color = _cluster_to_color(cluster_raw)
            else:
                # fallback to score-based
                score = 0
                try:
                    score = float(row.get("agriculture_irrigation_watershed_cat_value", 0))
                except Exception:
                    pass
                color = "green" if score > 0.66 else ("yellow" if score >= 0.33 else "red")
            village_data[village_id] = {"irrigation_watershed_color": color}

        return village_data

    except Exception as e:

        logger.info(
            "Not able to access irrigation infrastructure data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}


def get_all_villages_agri_support_service(state, district, block, df=None, df_facilities=None):

    try:

        if df is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))
            df = pd.read_excel(excel_file, sheet_name="antyodaya")

        df["village_id"] = df["village_id"].astype(str).str.strip()

        village_data = {}
        for _, row in df.iterrows():
            village_id = str(row.get("village_id", "")).strip()
            if not village_id or village_id == "0":
                continue
            village_data[village_id] = {
                "agri_support_color": _cluster_to_color(row.get("agriculture_support_services_cat_cluster")),
                "agri_market_color":  _cluster_to_color(row.get("agricultural_markets_cat_cluster")),
            }

        return village_data

    except Exception as e:

        logger.info(
            "Not able to access agri support service data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}


def get_all_villages_ecological_climate_resiliance(state, district, block, df=None, df_nrega=None):

    try:

        if df is None:
            excel_file = pd.ExcelFile(_build_file_path(state, district, block))
            df = pd.read_excel(excel_file, sheet_name="antyodaya")

        df["village_id"] = df["village_id"].astype(str).str.strip()

        # NREGA-based color is computed per-village via get_ecological_climate_resiliance;
        # for the map we read organic_farming cluster from antyodaya and compute nrega
        # color via the full function — but to keep the map fast we approximate nrega as
        # "red" here (the full per-village data is still shown in the report itself).
        # For the organic farming map color we use the cluster column directly.
        village_data = {}
        for _, row in df.iterrows():
            village_id = str(row.get("village_id", "")).strip()
            if not village_id or village_id == "0":
                continue
            village_data[village_id] = {
                "organic_farming_color": _cluster_to_color(row.get("agriculture_organic_farming_cat_cluster")),
                "nrega_work_color":      "red",   # fast fallback; individual report uses full NREGA data
            }

        return village_data

    except Exception as e:

        logger.info(
            "Not able to access ecology data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}

