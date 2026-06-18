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
    

def get_development_data(state, district, block, village_id):

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

        df = pd.read_excel(excel_file, sheet_name="antyodaya")
        df_facilities = pd.read_excel(
            excel_file,
            sheet_name="facilities_proximity"
        )

        normalize_column(df, "village_id")
        normalize_column(df_facilities, "censuscode2011")

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
            df_facilities["censuscode2011"] == village_id
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
                    "health_sub_cen_distance",
                    "health_phc_distance"
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
                    "health_chc_distance",
                    "health_dis_h_distance",
                    "health_s_t_h_distance"
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
                    "school_primary_distance",
                    "school_upper_primary_distance",
                    "school_secondary_distance"
                ],
                logic="max"
            )

            higher_education_distance = get_distance_logic(
                facility_row,
                [
                    "school_higher_secondary_distance",
                    "college_distance",
                    "universities_distance"
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
                    "csc_distance",
                    "bank_mitra_distance",
                    "bank_branch_distance",
                    "bank_atm_distance"
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
                "pds_distance"
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
        livelihood_farm_score = safe_float(row.get("farm_employment_feat_value", 0))
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
                "agri_industry_dairy_animal_husbandry_distance"
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
                    "column": "agri_industry_agri_support_infrastructure_distance",
                    "high_limit": 10,
                    "medium_limit": 50,
                },
                {
                    "column": "agri_industry_agri_processing_distance",
                    "high_limit": 5,
                    "medium_limit": 20,
                },
                {
                    "column": "agri_industry_co_operatives_societies_distance",
                    "high_limit": 10,
                    "medium_limit": 30,
                },
                {
                    "column": "agri_industry_markets_trading_distance",
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
        df_nrega = pd.read_excel(
            excel_file,
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
    

def get_block_development_data(state, district, block):

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

        df = pd.read_excel(excel_file, sheet_name="antyodaya")
        df_facilities = pd.read_excel(
            excel_file,
            sheet_name="facilities_proximity"
        )

        df_nrega = pd.read_excel(
            excel_file,
            sheet_name="nrega_assets_village"
        )

        normalize_column(df, "village_id")
        normalize_column(df_facilities, "censuscode2011")
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
                    "health_sub_cen_distance",
                    "health_phc_distance"
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
                    "health_chc_distance",
                    "health_dis_h_distance",
                    "health_s_t_h_distance"
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
                    "school_primary_distance",
                    "school_upper_primary_distance",
                    "school_secondary_distance"
                ],
                logic="max"
            )

            higher_education_distance = get_distance_logic(
                facility_row,
                [
                    "school_higher_secondary_distance",
                    "college_distance",
                    "universities_distance"
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
                    "csc_distance",
                    "bank_mitra_distance",
                    "bank_branch_distance",
                    "bank_atm_distance"
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
                "pds_distance"
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
                    "farm_employment_feat_value",
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
                "agri_industry_dairy_animal_husbandry_distance"
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

            village_id = facility_row["censuscode2011"]

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
                        "column": "agri_industry_agri_support_infrastructure_distance",
                        "high_limit": 10,
                        "medium_limit": 50,
                    },
                    {
                        "column": "agri_industry_agri_processing_distance",
                        "high_limit": 5,
                        "medium_limit": 20,
                    },
                    {
                        "column": "agri_industry_co_operatives_societies_distance",
                        "high_limit": 10,
                        "medium_limit": 30,
                    },
                    {
                        "column": "agri_industry_markets_trading_distance",
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
            return []

        row = matched_rows.iloc[0]

        return [
            safe_float(
                row.get(
                    "road_connectivity_cat_value",
                    0
                )
            ),
            safe_float(
                row.get(
                    "energy_access_cat_value",
                    0
                )
            ),
            safe_float(
                row.get(
                    "housing_quality_cat_value",
                    0
                )
            )
        ]

    except Exception as e:

        logger.info(
            "Not able to access infrastructure data. Error: %s",
            str(e),
        )

        return []


def get_health_and_wash(state, district, block, village_id):

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

        df_facilities = pd.read_excel(
            excel_file,
            sheet_name="facilities_proximity"
        )

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        df_facilities["censuscode2011"] = (
            df_facilities["censuscode2011"]
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
            return []

        row = matched_rows.iloc[0]

        facility_match = df_facilities[
            df_facilities["censuscode2011"] == village_id
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
                    "health_sub_cen_distance",
                    "health_phc_distance"
                ],
                logic="max"
            )

            advanced_distance = get_distance_logic(
                facility_row,
                [
                    "health_chc_distance",
                    "health_dis_h_distance",
                    "health_s_t_h_distance"
                ],
                logic="min"
            )

        return [
            maternal_child_score,                 # index 0
            water_sanitation_score,               # index 1
            round(essential_distance, 2) if essential_distance is not None else None,
            round(advanced_distance, 2) if advanced_distance is not None else None
        ]

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )
        return []


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

            df_facilities["censuscode2011"] = (
                df_facilities["censuscode2011"]
                .astype(str)
                .str.strip()
            )

        village_id = str(village_id).strip()

        facility_match = df_facilities[
            df_facilities["censuscode2011"] == village_id
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
                "school_primary_distance",
                "school_upper_primary_distance",
                "school_secondary_distance"
            ],
            logic="max"
        )

        higher_education_distance = get_distance_logic(
            facility_row,
            [
                "school_higher_secondary_distance",
                "college_distance",
                "universities_distance"
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


def get_financial_inclusion(state, district, block, village_id, df_facilities=None):

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

            df_facilities["censuscode2011"] = (
                df_facilities["censuscode2011"]
                .astype(str)
                .str.strip()
            )

        village_id = str(village_id).strip()

        facility_match = df_facilities[
            df_facilities["censuscode2011"] == village_id
        ]

        if facility_match.empty:
            logger.info(
                "No financial inclusion data found for village_id %s",
                village_id,
            )
            return []

        facility_row = facility_match.iloc[0]

        financial_distance = get_distance_logic(
            facility_row,
            [
                "csc_distance",
                "bank_mitra_distance",
                "bank_branch_distance",
                "bank_atm_distance"
            ],
            logic="max"
        )

        financial_inclusion_score = distance_score(
            financial_distance,
            high_limit=2,
            medium_limit=5
        )

        color = (
            "green"
            if financial_inclusion_score == 1
            else "red"
        )

        return [
            financial_inclusion_score,
            round(financial_distance, 2)
            if financial_distance is not None
            else None,
            color
        ]

    except Exception as e:
        logger.info(
            "Not able to access financial inclusion data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )
        return []


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

                df_facilities["censuscode2011"] = (
                    df_facilities["censuscode2011"]
                    .astype(str)
                    .str.strip()
                )

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        df_facilities["censuscode2011"] = (
            df_facilities["censuscode2011"]
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
            return []

        row = matched_rows.iloc[0]

        facility_match = df_facilities[
            df_facilities["censuscode2011"] == village_id
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
                "pds_distance"
            )

        if social_protection_score <= 0.33:
            color = "red"

        elif social_protection_score <= 0.66:
            color = "yellow"

        else:
            color = "green"

        return [
            social_protection_score,
            round(pds_distance, 2)
            if pd.notnull(pds_distance)
            else None,
            color
        ]

    except Exception as e:

        logger.info(
            "Not able to access welfare inclusion data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return []


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
            return []

        row = matched_rows.iloc[0]

        community_score = safe_float(
            row.get("institutionalization_cat_value", 0)
        )

        civic_score = safe_float(
            row.get("civic_infrastructure_cat_value", 0)
        )

        # Institutionalization Strength Color
        if community_score <= 0.33:
            community_color = "red"

        elif community_score <= 0.66:
            community_color = "yellow"

        else:
            community_color = "green"

        # Civic Infrastructure Availability Color
        civic_color = (
            "green"
            if civic_score > 0.66
            else "red"
        )

        return [
            community_score,
            civic_score,
            community_color,
            civic_color
        ]

    except Exception as e:

        logger.info(
            "Not able to access community institution data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return []


def get_livelihood_diversification(state, district, block, village_id):

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

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
            return []

        row = matched_rows.iloc[0]

        return [
            safe_float(row.get("farm_employment_feat_value", 0)),
            safe_float(row.get("livelihoods_forest_resources_cat_value", 0)),
            safe_float(row.get("livelihoods_alternative_farming_cat_value", 0)),
            safe_float(row.get("livelihoods_fisheries_cat_value", 0)),
            safe_float(row.get("livelihoods_cottage_traditional_industry_cat_value", 0))
        ]

    except Exception as e:

        logger.info(
            "Not able to access livelihood diversification data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return []


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

        df_facilities["censuscode2011"] = (
            df_facilities["censuscode2011"]
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
            return []

        row = matched_rows.iloc[0]

        facility_match = df_facilities[
            df_facilities["censuscode2011"] == village_id
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
                "agri_industry_dairy_animal_husbandry_distance"
            )

        veterinary_color = (
            "green"
            if livestock_support_score > 0.66
            else "red"
        )

        pasture_color = (
            "green"
            if livestock_pasture_score > 0.66
            else "red"
        )

        return [
            livestock_support_score,
            livestock_pasture_score,
            round(husbandry_distance, 2)
            if pd.notnull(husbandry_distance)
            else None,
            veterinary_color,
            pasture_color
        ]

    except Exception as e:

        logger.info(
            "Not able to access livestock management data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return []


def get_land_cultivation(state, district, block, village_id):
    
    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default
    
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

        df = pd.read_excel(
            excel_file,
            sheet_name="antyodaya"
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
            return []

        row = matched_rows.iloc[0]

        land_utilization_score = safe_float(row.get("agriculture_land_cultivation_cat_value", 0))


    
    except Exception as e:

        logger.info(
            "Not able to access Land cultivation data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return []


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
            return []

        row = matched_rows.iloc[0]

        irrigation_watershed_score = safe_float(
            row.get(
                "agriculture_irrigation_watershed_cat_value",
                0
            )
        )

        modern_irrigation_score = safe_float(
            row.get(
                "modern_irrigation_feat_value",
                0
            )
        )

        # Irrigation Watershed Color
        irrigation_watershed_color = (
            "green"
            if irrigation_watershed_score > 0.66
            else "red"
        )

        # Modern Irrigation Color
        if modern_irrigation_score <= 0.33:
            modern_irrigation_color = "red"

        elif modern_irrigation_score <= 0.66:
            modern_irrigation_color = "yellow"

        else:
            modern_irrigation_color = "green"

        return [
            irrigation_watershed_score,
            modern_irrigation_score,
            irrigation_watershed_color,
            modern_irrigation_color
        ]

    except Exception as e:

        logger.info(
            "Not able to access irrigation infrastructure data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return []
    

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

        df_facilities["censuscode2011"] = (
            df_facilities["censuscode2011"]
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
            return []

        row = matched_rows.iloc[0]

        facility_match = df_facilities[
            df_facilities["censuscode2011"] == village_id
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

        if facility_row is not None:

            post_harvest_distance = get_distance_logic(
                facility_row,
                [
                    "agri_industry_storage_warehousing_distance",
                    "agri_industry_distribution_utilities_distance",
                    "agri_industry_agri_processing_distance",
                    "agri_industry_industrial_manufacturing_distance"
                ],
                logic="min"
            )

            apmc_access_distance = get_distance_logic(
                facility_row,
                [
                    "apmc_distance",
                    "agri_industry_markets_trading_distance"
                ],
                logic="min"
            )

        # =====================================================
        # Colors
        # =====================================================

        if agri_support_score <= 0.33:
            agri_support_color = "red"

        elif agri_support_score <= 0.66:
            agri_support_color = "yellow"

        else:
            agri_support_color = "green"

        agri_market_color = (
            "green"
            if agri_market_score > 0.66
            else "red"
        )

        return [
            agri_support_score,                                # index 0
            agri_market_score,                                 # index 1
            round(post_harvest_distance, 2)
            if post_harvest_distance is not None
            else None,                                         # index 2
            round(apmc_access_distance, 2)
            if apmc_access_distance is not None
            else None,                                         # index 3
            agri_support_color,                                # index 4
            agri_market_color                                  # index 5
        ]

    except Exception as e:

        logger.info(
            "Not able to access agri support service data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return []


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
            return []

        row = matched_rows.iloc[0]

        organic_farming_score = safe_float(
            row.get(
                "agriculture_organic_farming_cat_value",
                0
            )
        )

        if organic_farming_score <= 0.33:
            organic_farming_color = "red"

        elif organic_farming_score <= 0.66:
            organic_farming_color = "yellow"

        else:
            organic_farming_color = "green"

        # =====================================================
        # NREGA Assets
        # =====================================================

        nrega_match = df_nrega[
            df_nrega["vill_id"] == village_id
        ]

        if nrega_match.empty:

            return [
                None,                       # year_range
                {},                         # category_counts
                0,                          # total_work_count
                "red",                      # nrega_work_color
                organic_farming_score,
                organic_farming_color
            ]

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

        return [
            year_range,                 # index 0
            category_counts,            # index 1
            total_work_count,           # index 2
            nrega_work_color,           # index 3
            organic_farming_score,      # index 4
            organic_farming_color       # index 5
        ]

    except Exception as e:

        logger.info(
            "Not able to access ecology data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return []
    


#? Get Tehsil Map Data
def get_all_villages_basic_infrastructure(state, district, block):
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

        # Read once
        df = pd.read_excel(
            excel_file,
            sheet_name="antyodaya"
        )

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        df_nrega = pd.read_excel(
            excel_file,
            sheet_name="nrega_assets_village"
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

            village_data = get_basic_infrastructure(state, district, block, village_id, df=df)

            if not village_data:

                result[village_id] = {
                    "road_color": "black",
                    "energy_color": "black",
                    "housing_color": "black",
                }

                continue

            road_score, energy_score, housing_score = village_data

            result[village_id] = {

                "road_color": (
                    "green"
                    if road_score > 0.66
                    else "red"
                ),

                "energy_color": (
                    "green"
                    if energy_score > 0.66
                    else "red"
                ),

                "housing_color": (
                    "red"
                    if housing_score <= 0.33
                    else "green"
                    if housing_score > 0.66
                    else "yellow"
                ),
            }

        return result

    except Exception as e:

        logger.info(
            "Error calculating infrastructure colors for all villages: %s",
            str(e)
        )

        return {}


def get_all_villages_health_and_wash(state, district, block):

    def safe_float(value, default=0):
        try:
            return float(value)
        except:
            return default

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

        df = pd.read_excel(
            excel_file,
            sheet_name="antyodaya"
        )

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

            maternal_child_score = safe_float(
                row.get(
                    "maternal_child_health_cat_value",
                    0
                )
            )

            water_sanitation_score = safe_float(
                row.get(
                    "water_sanitation_cat_value",
                    0
                )
            )

            # Maternal Child Health Color

            if maternal_child_score <= 0.33:
                maternal_child_health_color = "red"

            elif maternal_child_score <= 0.66:
                maternal_child_health_color = "yellow"

            else:
                maternal_child_health_color = "green"

            # Water & Sanitation Color

            if water_sanitation_score <= 0.33:
                water_sanitation_color = "red"

            elif water_sanitation_score <= 0.66:
                water_sanitation_color = "yellow"

            else:
                water_sanitation_color = "green"

            village_data[village_id] = {
                "maternal_child_health_color": (
                    maternal_child_health_color
                ),
                "water_sanitation_color": (
                    water_sanitation_color
                )
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


def get_all_villages_education_institutions( state, district, block):
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

        df_nrega = pd.read_excel(
            excel_file,
            sheet_name="nrega_assets_village"
        )

        df_facilities = pd.read_excel(
            excel_file,
            sheet_name="facilities_proximity"
        )

        df_facilities["censuscode2011"] = (
            df_facilities["censuscode2011"]
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


def get_all_villages_financial_inclusion(state, district, block):

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

        df_nrega = pd.read_excel(
            excel_file,
            sheet_name="nrega_assets_village"
        )

        df_facilities = pd.read_excel(
            excel_file,
            sheet_name="facilities_proximity"
        )

        df_facilities["censuscode2011"] = (
            df_facilities["censuscode2011"]
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

            financial_data = get_financial_inclusion(
                state,
                district,
                block,
                village_id,
                df_facilities=df_facilities
            )

            if not financial_data:

                result[village_id] = {
                    "financial_color": "black"
                }

                continue

            result[village_id] = {
                "financial_color": financial_data[2]
            }

        return result

    except Exception as e:

        logger.info(
            "Error calculating financial inclusion colors for all villages: %s",
            str(e)
        )

        return {}


def get_all_villages_welfare_inclusion(state, district, block):

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

        df = pd.read_excel(
            excel_file,
            sheet_name="antyodaya"
        )

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        df_facilities = pd.read_excel(
            excel_file,
            sheet_name="facilities_proximity"
        )

        df_facilities["censuscode2011"] = (
            df_facilities["censuscode2011"]
            .astype(str)
            .str.strip()
        )

        df_nrega = pd.read_excel(
            excel_file,
            sheet_name="nrega_assets_village"
        )

        village_ids = [
            str(v).strip()
            for v in df_nrega["vill_id"].dropna().unique()
            if str(v).strip() not in ("", "0")
        ]

        result = {}

        for village_id in village_ids:

            welfare_data = get_welfare_inclusion(
                state,
                district,
                block,
                village_id,
                df=df,
                df_facilities=df_facilities
            )

            if not welfare_data:

                result[village_id] = {
                    "welfare_color": "black"
                }

                continue

            result[village_id] = {
                "welfare_color": welfare_data[2]
            }

        return result

    except Exception as e:

        logger.info(
            "Error calculating welfare inclusion colors for all villages: %s",
            str(e)
        )

        return {}


def get_all_villages_community_institutes(state, district, block):

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

        df = pd.read_excel(
            excel_file,
            sheet_name="antyodaya"
        )

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

            community_info = get_community_institutes(
                state,
                district,
                block,
                village_id,
                df=df
            )

            if not community_info:

                village_data[village_id] = {
                    "community_color": "black",
                    "civic_color": "black"
                }

                continue

            village_data[village_id] = {
                "community_color": community_info[2],
                "civic_color": community_info[3]
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


def get_all_villages_livestock_management(state, district, block):

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

        df = pd.read_excel(
            excel_file,
            sheet_name="antyodaya"
        )

        df_facilities = pd.read_excel(
            excel_file,
            sheet_name="facilities_proximity"
        )

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

            livestock_info = get_livestock_management(
                state,
                district,
                block,
                village_id,
                df=df,
                df_facilities=df_facilities
            )

            if not livestock_info:

                village_data[village_id] = {
                    "veterinary_color": "black",
                    "pasture_color": "black"
                }

                continue

            village_data[village_id] = {
                "veterinary_color": livestock_info[3],
                "pasture_color": livestock_info[4]
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


def get_all_villages_irrigation_infra(state, district, block):

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

        df = pd.read_excel(
            excel_file,
            sheet_name="antyodaya"
        )

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

            irrigation_info = get_irrigation_Infra(
                state,
                district,
                block,
                village_id,
                df=df
            )

            if not irrigation_info:

                village_data[village_id] = {
                    "irrigation_watershed_color": "black",
                    "modern_irrigation_color": "black"
                }

                continue

            village_data[village_id] = {
                "irrigation_watershed_color": irrigation_info[2],
                "modern_irrigation_color": irrigation_info[3]
            }

        return village_data

    except Exception as e:

        logger.info(
            "Not able to access irrigation infrastructure data for %s district, %s block. Error: %s",
            district,
            block,
            str(e),
        )

        return {}


def get_all_villages_agri_support_service(state, district, block):

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

        df = pd.read_excel(
            excel_file,
            sheet_name="antyodaya"
        )

        df_facilities = pd.read_excel(
            excel_file,
            sheet_name="facilities_proximity"
        )

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

            agri_info = get_agri_support_service(
                state,
                district,
                block,
                village_id,
                df=df,
                df_facilities=df_facilities
            )

            if not agri_info:

                village_data[village_id] = {
                    "agri_support_color": "black",
                    "agri_market_color": "black"
                }

                continue

            village_data[village_id] = {
                "agri_support_color": agri_info[4],
                "agri_market_color": agri_info[5]
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


def get_all_villages_ecological_climate_resiliance(state, district, block):

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

        df = pd.read_excel(
            excel_file,
            sheet_name="antyodaya"
        )

        df_nrega = pd.read_excel(
            excel_file,
            sheet_name="nrega_assets_village"
        )

        df["village_id"] = (
            df["village_id"]
            .astype(str)
            .str.strip()
        )

        village_ids = [
            str(v).strip()
            for v in df_nrega["vill_id"].dropna().unique()
            if str(v).strip() not in ("", "0")
        ]

        village_data = {}

        for village_id in village_ids:

            ecology_info = (
                get_ecological_climate_resiliance(
                    state,
                    district,
                    block,
                    village_id,
                    df=df,
                    df_nrega=df_nrega
                )
            )

            if not ecology_info:

                village_data[village_id] = {
                    "organic_farming_color": "black",
                    "nrega_work_color": "black"
                }

                continue

            village_data[village_id] = {
                "organic_farming_color": ecology_info[5],
                "nrega_work_color": ecology_info[3]
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

