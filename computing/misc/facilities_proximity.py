"""
Facilities Proximity Module

This module provides functionality to access and analyze facility proximity data
for villages across different administrative levels (village, MWS, block/tehsil, district).

The data is sourced from state-wise GeoJSON files containing distance information
to various facilities like healthcare, education, agriculture, and markets.
"""

import os
import json
from typing import Dict, List, Optional, Union
from collections import defaultdict
from nrm_app.settings import BASE_DIR

# Path to the facilities GeoJSON data
FACILITIES_DATA_PATH = os.path.join(BASE_DIR, "data", "statewise_geojsons_facilities")

# Facility categories for organized access
FACILITY_CATEGORIES = {
    "agriculture": [
        "agri_industry_agri_processing_distance",
        "agri_industry_agri_support_infrastructure_distance",
        "agri_industry_co_operatives_societies_distance",
        "agri_industry_dairy_animal_husbandry_distance",
        "agri_industry_distribution_utilities_distance",
        "agri_industry_industrial_manufacturing_distance",
        "agri_industry_markets_trading_distance",
        "agri_industry_storage_warehousing_distance",
        "apmc_distance",
    ],
    "health": [
        "health_chc_distance",
        "health_dis_h_distance",
        "health_phc_distance",
        "health_s_t_h_distance",
        "health_sub_cen_distance",
    ],
    "education": [
        "college_distance",
        "school_informal_unrecognized_distance",
        "school_private_aided_distance",
        "school_private_market_driven_distance",
        "school_public_advanced_education_distance",
        "school_public_basic_education_distance",
        "school_public_comprehensive_distance",
        "school_public_selective_addmision_distance",
        "school_special_interest_religious_distance",
        "universities_distance",
    ],
}

# All distance fields
ALL_DISTANCE_FIELDS = [
    "agri_industry_agri_processing_distance",
    "agri_industry_agri_support_infrastructure_distance",
    "agri_industry_co_operatives_societies_distance",
    "agri_industry_dairy_animal_husbandry_distance",
    "agri_industry_distribution_utilities_distance",
    "agri_industry_industrial_manufacturing_distance",
    "agri_industry_markets_trading_distance",
    "agri_industry_storage_warehousing_distance",
    "apmc_distance",
    "college_distance",
    "health_chc_distance",
    "health_dis_h_distance",
    "health_phc_distance",
    "health_s_t_h_distance",
    "health_sub_cen_distance",
    "school_informal_unrecognized_distance",
    "school_private_aided_distance",
    "school_private_market_driven_distance",
    "school_public_advanced_education_distance",
    "school_public_basic_education_distance",
    "school_public_comprehensive_distance",
    "school_public_selective_addmision_distance",
    "school_special_interest_religious_distance",
    "universities_distance",
]

# Human-readable facility names
FACILITY_DISPLAY_NAMES = {
    "agri_industry_agri_processing_distance": "Agricultural Processing Unit",
    "agri_industry_agri_support_infrastructure_distance": "Agricultural Support Infrastructure",
    "agri_industry_co_operatives_societies_distance": "Co-operative Societies",
    "agri_industry_dairy_animal_husbandry_distance": "Dairy/Animal Husbandry",
    "agri_industry_distribution_utilities_distance": "Distribution Utilities",
    "agri_industry_industrial_manufacturing_distance": "Industrial Manufacturing",
    "agri_industry_markets_trading_distance": "Markets/Trading Centers",
    "agri_industry_storage_warehousing_distance": "Storage/Warehousing",
    "apmc_distance": "APMC Market",
    "college_distance": "College",
    "health_chc_distance": "Community Health Center (CHC)",
    "health_dis_h_distance": "District Hospital",
    "health_phc_distance": "Primary Health Center (PHC)",
    "health_s_t_h_distance": "Sub-Divisional Hospital",
    "health_sub_cen_distance": "Health Sub-Center",
    "school_informal_unrecognized_distance": "Informal/Unrecognized School",
    "school_private_aided_distance": "Private Aided School",
    "school_private_market_driven_distance": "Private Market-Driven School",
    "school_public_advanced_education_distance": "Public Advanced Education",
    "school_public_basic_education_distance": "Public Basic Education School",
    "school_public_comprehensive_distance": "Public Comprehensive School",
    "school_public_selective_addmision_distance": "Public Selective Admission School",
    "school_special_interest_religious_distance": "Special Interest/Religious School",
    "universities_distance": "University",
}


class FacilitiesProximityData:
    """
    Class to handle loading and querying facilities proximity data.
    """

    def __init__(self):
        self._cache = {}  # Cache for loaded state data

    def _get_state_filename(self, state: str) -> str:
        """Convert state name to filename format."""
        # Handle common variations in state names
        state_mapping = {
            "andaman & nicobar": "Andaman_and_Nicobar_Islands",
            "andaman and nicobar": "Andaman_and_Nicobar_Islands",
            "jammu & kashmir": "Jammu_And_Kashmir",
            "jammu and kashmir": "Jammu_And_Kashmir",
            "delhi": "Delhi",
            "nct of delhi": "Delhi",
        }
        
        state_lower = state.lower().strip()
        if state_lower in state_mapping:
            return f"{state_mapping[state_lower]}.geojson"
        
        # Convert to title case and replace spaces with underscores
        return f"{state.title().replace(' ', '_')}.geojson"

    def _load_state_data(self, state: str) -> Optional[Dict]:
        """Load GeoJSON data for a specific state."""
        if state in self._cache:
            return self._cache[state]

        filename = self._get_state_filename(state)
        filepath = os.path.join(FACILITIES_DATA_PATH, filename)

        if not os.path.exists(filepath):
            # Try with original state name
            filepath = os.path.join(FACILITIES_DATA_PATH, f"{state}.geojson")
            if not os.path.exists(filepath):
                return None

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                self._cache[state] = data
                return data
        except Exception as e:
            print(f"Error loading facilities data for {state}: {e}")
            return None

    def _get_subdistrict_value(self, props: Dict) -> str:
        """
        Get subdistrict/block value from properties.
        Handles both 'subdistrict' and 'block' column names.
        """
        # Try subdistrict first
        subdistrict = props.get("subdistrict", "")
        if subdistrict:
            return subdistrict
        
        # Try block as fallback
        block = props.get("block", "")
        return block

    def get_village_facilities(
        self, 
        state: str, 
        censuscode: Optional[Union[int, str]] = None,
        district: Optional[str] = None,
        block: Optional[str] = None,
        village_name: Optional[str] = None,
        include_geometry: bool = False
    ) -> Optional[Dict]:
        """
        Get facility distances for a specific village.
        
        Can query by:
        - state + censuscode (direct lookup)
        - state + district + block + village_name (search by name)

        Args:
            state: State name
            censuscode: Village census code (2011)
            district: District name (required if using village_name)
            block: Block/subdistrict name (required if using village_name)
            village_name: Village name (requires district and block)
            include_geometry: Whether to include geometry in response

        Returns:
            Dictionary with village info and facility distances, or None if not found
        """
        data = self._load_state_data(state)
        if not data:
            return None

        # Direct lookup by censuscode
        if censuscode:
            censuscode = int(censuscode)
            for feature in data.get("features", []):
                props = feature.get("properties", {})
                if props.get("censuscode2011") == censuscode:
                    return {
                        "village_info": {
                            "censuscode2011": props.get("censuscode2011"),
                            "name": props.get("name"),
                            "subdistrict": self._get_subdistrict_value(props),
                            "district": props.get("district"),
                            "state": props.get("state"),
                            "censuscode2001": props.get("censuscode2001"),
                            "level_2011": props.get("level_2011"),
                            "tru_2011": props.get("tru_2011"),
                        },
                        "facilities": self._extract_facilities(props),
                        "geometry": feature.get("geometry") if include_geometry else None,
                    }
            return None

        # Search by name (requires district and block)
        if village_name and district and block:
            village_name_lower = village_name.lower().strip()
            district_lower = district.lower().strip()
            block_lower = block.lower().strip()

            for feature in data.get("features", []):
                props = feature.get("properties", {})
                props_village = props.get("name", "").lower().strip()
                props_district = props.get("district", "").lower().strip()
                props_block = self._get_subdistrict_value(props).lower().strip()

                if (props_village == village_name_lower and 
                    props_district == district_lower and 
                    props_block == block_lower):
                    return {
                        "village_info": {
                            "censuscode2011": props.get("censuscode2011"),
                            "name": props.get("name"),
                            "subdistrict": self._get_subdistrict_value(props),
                            "district": props.get("district"),
                            "state": props.get("state"),
                            "censuscode2001": props.get("censuscode2001"),
                            "level_2011": props.get("level_2011"),
                            "tru_2011": props.get("tru_2011"),
                        },
                        "facilities": self._extract_facilities(props),
                        "geometry": feature.get("geometry") if include_geometry else None,
                    }

        return None

    def get_villages_by_block(
        self, state: str, block: str, district: Optional[str] = None,
        include_geometry: bool = False
    ) -> List[Dict]:
        """
        Get all villages in a block/subdistrict/tehsil with their facility distances.

        Args:
            state: State name
            block: Block/Subdistrict/Tehsil name
            district: Optional district name for filtering
            include_geometry: Whether to include geometry in response

        Returns:
            List of village dictionaries with facility data
        """
        data = self._load_state_data(state)
        if not data:
            return []

        villages = []
        block_lower = block.lower().strip()

        for feature in data.get("features", []):
            props = feature.get("properties", {})
            props_block = self._get_subdistrict_value(props).lower().strip()

            if props_block == block_lower:
                # If district is specified, verify it matches
                if district and props.get("district", "").lower().strip() != district.lower().strip():
                    continue

                villages.append({
                    "village_info": {
                        "censuscode2011": props.get("censuscode2011"),
                        "name": props.get("name"),
                        "subdistrict": self._get_subdistrict_value(props),
                        "district": props.get("district"),
                        "state": props.get("state"),
                    },
                    "facilities": self._extract_facilities(props),
                    "geometry": feature.get("geometry") if include_geometry else None,
                })

        return villages

    def get_villages_by_district(
        self, state: str, district: str,
        include_geometry: bool = False
    ) -> List[Dict]:
        """
        Get all villages in a district with their facility distances.

        Args:
            state: State name
            district: District name
            include_geometry: Whether to include geometry in response

        Returns:
            List of village dictionaries with facility data
        """
        data = self._load_state_data(state)
        if not data:
            return []

        villages = []
        district_lower = district.lower().strip()

        for feature in data.get("features", []):
            props = feature.get("properties", {})
            props_district = props.get("district", "").lower().strip()

            if props_district == district_lower:
                villages.append({
                    "village_info": {
                        "censuscode2011": props.get("censuscode2011"),
                        "name": props.get("name"),
                        "subdistrict": self._get_subdistrict_value(props),
                        "district": props.get("district"),
                        "state": props.get("state"),
                    },
                    "facilities": self._extract_facilities(props),
                    "geometry": feature.get("geometry") if include_geometry else None,
                })

        return villages

    def get_villages_by_mws(
        self, state: str, village_codes: List[Union[int, str]],
        include_geometry: bool = False
    ) -> List[Dict]:
        """
        Get villages by a list of census codes (for MWS-level queries).

        Args:
            state: State name
            village_codes: List of village census codes
            include_geometry: Whether to include geometry in response

        Returns:
            List of village dictionaries with facility data
        """
        data = self._load_state_data(state)
        if not data:
            return []

        codes_set = {int(code) for code in village_codes}
        villages = []

        for feature in data.get("features", []):
            props = feature.get("properties", {})
            if props.get("censuscode2011") in codes_set:
                villages.append({
                    "village_info": {
                        "censuscode2011": props.get("censuscode2011"),
                        "name": props.get("name"),
                        "subdistrict": self._get_subdistrict_value(props),
                        "district": props.get("district"),
                        "state": props.get("state"),
                    },
                    "facilities": self._extract_facilities(props),
                    "geometry": feature.get("geometry") if include_geometry else None,
                })

        return villages

    def _extract_facilities(self, props: Dict) -> Dict:
        """Extract facility distances from properties."""
        facilities = {}
        for field in ALL_DISTANCE_FIELDS:
            value = props.get(field)
            if value is not None:
                facilities[field] = {
                    "distance_km": round(value, 2),
                    "display_name": FACILITY_DISPLAY_NAMES.get(field, field),
                    "category": self._get_category(field),
                }
        return facilities

    def _get_category(self, field: str) -> str:
        """Get the category for a facility field."""
        for category, fields in FACILITY_CATEGORIES.items():
            if field in fields:
                return category
        return "other"

    def get_aggregated_stats(self, villages: List[Dict]) -> Dict:
        """
        Calculate aggregated statistics for a list of villages.

        Args:
            villages: List of village dictionaries with facility data

        Returns:
            Dictionary with aggregated statistics per facility type
        """
        if not villages:
            return {}

        stats = defaultdict(lambda: {"distances": []})

        # Collect all distances
        for village in villages:
            facilities = village.get("facilities", {})
            for field, data in facilities.items():
                stats[field]["distances"].append(data["distance_km"])

        # Calculate statistics
        result = {}
        for field, data in stats.items():
            distances = data["distances"]
            if distances:
                result[field] = {
                    "display_name": FACILITY_DISPLAY_NAMES.get(field, field),
                    "category": self._get_category(field),
                    "min": round(min(distances), 2),
                    "max": round(max(distances), 2),
                    "mean": round(sum(distances) / len(distances), 2),
                    "median": round(sorted(distances)[len(distances) // 2], 2),
                    "villages_within_5km": sum(1 for d in distances if d <= 5),
                    "villages_within_10km": sum(1 for d in distances if d <= 10),
                    "villages_within_20km": sum(1 for d in distances if d <= 20),
                    "total_villages": len(distances),
                }

        return result

    def get_category_summary(self, villages: List[Dict]) -> Dict:
        """
        Get a summary by facility category.

        Args:
            villages: List of village dictionaries with facility data

        Returns:
            Dictionary with summary statistics per category
        """
        aggregated = self.get_aggregated_stats(villages)
        
        category_summary: Dict[str, Dict] = {}
        for category in FACILITY_CATEGORIES.keys():
            category_summary[category] = {
                "facilities": [],
                "avg_distance": 0,
                "total_villages": 0,
            }

        for field, stats in aggregated.items():
            category = stats["category"]
            if category not in category_summary:
                category_summary[category] = {
                    "facilities": [],
                    "avg_distance": 0,
                    "total_villages": 0,
                }
            category_summary[category]["facilities"].append({
                "field": field,
                "display_name": stats["display_name"],
                "mean_distance": stats["mean"],
            })

        # Calculate averages per category
        for category, data in category_summary.items():
            facilities_list = data["facilities"]
            if facilities_list:
                avg = sum(f["mean_distance"] for f in facilities_list) / len(facilities_list)
                data["avg_distance"] = round(avg, 2)
                data["facilities"].sort(key=lambda x: x["mean_distance"])

        return category_summary

    def get_facilities_geojson(
        self, 
        state: str, 
        district: Optional[str] = None, 
        block: Optional[str] = None,
        village_codes: Optional[List] = None
    ) -> Dict:
        """
        Get facilities data as a GeoJSON FeatureCollection.

        Args:
            state: State name
            district: Optional district filter
            block: Optional block/subdistrict filter
            village_codes: Optional list of village codes (for MWS)

        Returns:
            GeoJSON FeatureCollection with facility distance properties
        """
        # print(f"DEBUG get_facilities_geojson: state={state}, district={district}, block={block}, village_codes={village_codes}")
        
        data = self._load_state_data(state)
        if not data:
            # print(f"DEBUG: No data loaded for state={state}")
            return {"type": "FeatureCollection", "features": []}

        features = []
        total_features = len(data.get("features", []))
        # print(f"DEBUG: Total features in state data: {total_features}")

        for feature in data.get("features", []):
            props = feature.get("properties", {})
            include = True

            # Apply filters
            if village_codes:
                include = props.get("censuscode2011") in {int(c) for c in village_codes}
            elif block:
                props_block = self._get_subdistrict_value(props).lower().strip()
                include = props_block == block.lower().strip()
                if include and district:
                    include = props.get("district", "").lower().strip() == district.lower().strip()
            elif district:
                include = props.get("district", "").lower().strip() == district.lower().strip()

            if include:
                # Create a clean feature with organized properties
                new_feature = {
                    "type": "Feature",
                    "properties": {
                        "censuscode2011": props.get("censuscode2011"),
                        "name": props.get("name"),
                        "subdistrict": self._get_subdistrict_value(props),
                        "district": props.get("district"),
                        "state": props.get("state"),
                        "facilities": self._extract_facilities(props),
                    },
                    "geometry": feature.get("geometry"),
                }
                features.append(new_feature)

        # print(f"DEBUG: Returning {len(features)} features")
        return {
            "type": "FeatureCollection",
            "features": features,
        }

    def get_available_states(self) -> List[str]:
        """Get list of states for which facilities data is available."""
        states = []
        if os.path.exists(FACILITIES_DATA_PATH):
            for filename in os.listdir(FACILITIES_DATA_PATH):
                if filename.endswith(".geojson"):
                    state_name = filename.replace(".geojson", "").replace("_", " ")
                    states.append(state_name)
        return sorted(states)


# Create a singleton instance
facilities_data = FacilitiesProximityData()


# Convenience functions for direct access
def get_village_facilities(
    state: str, 
    censuscode: Optional[Union[int, str]] = None,
    district: Optional[str] = None,
    block: Optional[str] = None,
    village_name: Optional[str] = None,
    include_geometry: bool = False
) -> Optional[Dict]:
    """Get facility distances for a specific village."""
    return facilities_data.get_village_facilities(
        state, censuscode, district, block, village_name, include_geometry
    )


def get_block_facilities(
    state: str, block: str, district: Optional[str] = None,
    include_geometry: bool = False
) -> List[Dict]:
    """Get all villages with facilities in a block/subdistrict."""
    return facilities_data.get_villages_by_block(state, block, district, include_geometry)


def get_district_facilities(
    state: str, district: str,
    include_geometry: bool = False
) -> List[Dict]:
    """Get all villages with facilities in a district."""
    return facilities_data.get_villages_by_district(state, district, include_geometry)


def get_mws_facilities(
    state: str, village_codes: List,
    include_geometry: bool = False
) -> List[Dict]:
    """Get villages with facilities for an MWS (list of village codes)."""
    return facilities_data.get_villages_by_mws(state, village_codes, include_geometry)


def get_facilities_summary(villages: List[Dict]) -> Dict:
    """Get aggregated statistics for villages."""
    return {
        "aggregated_stats": facilities_data.get_aggregated_stats(villages),
        "category_summary": facilities_data.get_category_summary(villages),
    }
