from typing import Optional, Dict
from datetime import datetime
import json

class tehsil_data:
    """
    Tehsil/Block administrative unit data class containing location information
    and a collection of microwatersheds within the tehsil.
    """
    
    def __init__(self, state, district, tehsil):
        # === IDENTIFICATION ===
        self.tehsil_name: str = tehsil
        self.district_name: str = district
        self.state_name: str = state
        
        # === GEOMETRY ===
        self.geometry: Optional[object] = None
        
        # === MICROWATERSHEDS ===
        self.microwatersheds: Optional[Dict(str, MWS_data)] = {}

        # === WATERBODIES ===
        self.waterbodies: Optional[Dict(str, waterbody_data)] = {}


class MWS_data:
    """
    Microwatershed data class combining terrain, cropping intensity, 
    drought frequency, and well depth information.
    """
    
    def __init__(self):
        # === IDENTIFICATION ===
        self.uid: str = None
        
        # === AREA METRICS ===
        self.area_in_ha: Optional[float] = None
        
        # === TERRAIN CLASSIFICATION ===
        self.plain_area: Optional[float] = None
        self.slopy_area: Optional[float] = None
        self.hill_slope: Optional[float] = None
        self.valley_are: Optional[float] = None
        self.ridge_area: Optional[float] = None
        self.terrainClu: Optional[int] = None
        
        # === CROPPING INTENSITY - TOTALS ===
        self.total_cropable_area_ever_hydroyear_2017_2023: Optional[float] = None
        
        # === CROPPING INTENSITY - ANNUAL (Dict[year, value]) ===
        self.cropping_intensity: Dict[str, Optional[float]] = {
            '2017': None, '2018': None, '2019': None, '2020': None,
            '2021': None, '2022': None, '2023': None
        }
        
        # === SINGLE CROPPED AREA (Dict[year, value]) ===
        self.single_cropped_area: Dict[str, Optional[float]] = {
            '2017': None, '2018': None, '2019': None, '2020': None,
            '2021': None, '2022': None, '2023': None
        }
        
        # === SINGLE KHARIF CROPPED AREA (Dict[year, value]) ===
        self.single_kharif_cropped_area: Dict[str, Optional[float]] = {
            '2017': None, '2018': None, '2019': None, '2020': None,
            '2021': None, '2022': None, '2023': None
        }
        
        # === SINGLE NON-KHARIF CROPPED AREA (Dict[year, value]) ===
        self.single_non_kharif_cropped_area: Dict[str, Optional[float]] = {
            '2017': None, '2018': None, '2019': None, '2020': None,
            '2021': None, '2022': None, '2023': None
        }
        
        # === DOUBLE CROPPED AREA (Dict[year, value]) ===
        self.doubly_cropped_area: Dict[str, Optional[float]] = {
            '2017': None, '2018': None, '2019': None, '2020': None,
            '2021': None, '2022': None, '2023': None
        }
        
        # === TRIPLE CROPPED AREA (Dict[year, value]) ===
        self.triply_cropped_area: Dict[str, Optional[float]] = {
            '2017': None, '2018': None, '2019': None, '2020': None,
            '2021': None, '2022': None, '2023': None
        }
        
        # === DROUGHT CLASSIFICATION (Dict[year, label]) ===
        self.drlb: Dict[str, Optional[str]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        # === DRY SPELL ANALYSIS ===
        self.avg_dryspell: Optional[float] = None
        self.drysp: Dict[str, Optional[int]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        # === DROUGHT WEEKS BY SEVERITY (Dict[year, weeks]) ===
        self.w_no: Dict[str, Optional[int]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        self.w_mld: Dict[str, Optional[int]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        self.w_mod: Dict[str, Optional[int]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        self.w_sev: Dict[str, Optional[int]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        # === TOTAL ANALYSIS WEEKS (Dict[year, weeks]) ===
        self.t_wks: Dict[str, Optional[int]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        # === DROUGHT FREQUENCY BY THRESHOLD (Dict[year, frequency]) ===
        self.frth0: Dict[str, Optional[int]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        self.frth1: Dict[str, Optional[int]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        self.frth2: Dict[str, Optional[int]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        self.frth3: Dict[str, Optional[int]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        # === DROUGHT INTENSITY BY THRESHOLD (Dict[year, intensity]) ===
        self.inth0: Dict[str, Optional[float]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        self.inth1: Dict[str, Optional[float]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        self.inth2: Dict[str, Optional[float]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        self.inth3: Dict[str, Optional[int]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
                
        # === MONSOON ONSET (Dict[year, datetime]) ===
        self.m_ons: Dict[str, Optional[datetime]] = {
            '2017': None, '2018': None, '2019': None,
            '2020': None, '2021': None, '2022': None
        }
        
        # === WELL DEPTH - AQUIFER ===
        self.weighted_a: Optional[float] = None
        
        # === WELL DEPTH - ANNUAL CHANGE (Dict[year_range, value]) ===
        self.well_depth: Dict[str, Optional[str]] = {
            '2017': None, '2018': None, '2019': None, '2020': None,
            '2021': None, '2022': None, '2023': None, '2024': None
        }
        
        # === WELL DEPTH - MULTI-YEAR NET CHANGE (Dict[year_range, value]) ===
        self.well_depth_net: Dict[str, Optional[float]] = {
            '2017_2022': None, '2018_2023': None, '2019_2024': None, '2020_2025': None
        }
        
        # === GEOMETRY ===
        self.geometry: Optional[object] = None

        # === WATERBODIES ===
        self.waterbodies: Optional[Dict(str, waterbody_data)] = {}


class waterbody_data:
    """
    Surface water body data class containing comprehensive waterbody information
    including location, physical characteristics, seasonal availability, and 
    management details.
    """
    
    def __init__(self):
        # === IDENTIFICATION ===
        self.UID: str = None
        self.census_id: Optional[str] = None
        self.MWS_UID: str = None
                
        # === HYDROLOGICAL CLASSIFICATION ===
        self.basin_name: Optional[str] = None
        self.sub_basin_name: Optional[str] = None
        
        # === SURFACE WATER AREA (Dict[year_range, hectares]) ===
        self.area: Dict[str, Optional[float]] = {
            '2017': None, '2018': None, '2019': None, '2020': None,
            '2021': None, '2022': None, '2023': None
        }
        
        # === WATER AVAILABILITY - KHARIF SEASON (Dict[year_range, percent]) ===
        self.k: Dict[str, Optional[float]] = {
            '2017': None, '2018': None, '2019': None, '2020': None,
            '2021': None, '2022': None, '2023': None
        }
        
        # === WATER AVAILABILITY - KHARIF & RABI SEASONS (Dict[year_range, percent]) ===
        self.kr: Dict[str, Optional[float]] = {
            '2017': None, '2018': None, '2019': None, '2020': None,
            '2021': None, '2022': None, '2023': None
        }
        
        # === WATER AVAILABILITY - ALL SEASONS (Dict[year_range, percent]) ===
        self.krz: Dict[str, Optional[float]] = {
            '2017': None, '2018': None, '2019': None, '2020': None,
            '2021': None, '2022': None, '2023': None
        }
        
        # === LOCATION INFORMATION ===
        self.State_Name: Optional[str] = None
        self.District_Name: Optional[str] = None
        self.Block_Tehsil_Name: Optional[str] = None
        self.Village_Name: Optional[str] = None
        self.town_municipalty_name: Optional[str] = None
        self.ward_name: Optional[str] = None
        self.rural_or_urban: Optional[str] = None
        self.latitude_dec: Optional[float] = None
        self.longitude_dec: Optional[float] = None

        # === WATER BODY CHARACTERISTICS ===
        self.water_body_name: Optional[str] = None
        self.water_body_loc_name: Optional[str] = None
        self.ref_water_body_type_id_name: Optional[str] = None
        self.water_body_nature_name: Optional[str] = None
        self.manmade_water_body_type_name: Optional[str] = None
        self.water_body_ownership_name: Optional[str] = None
        self.nature_of_storage: Optional[str] = None
        
        # === PHYSICAL CHARACTERISTICS ===
        self.water_spread_area_of_water_body: Optional[float] = None
        self.area_ored: Optional[float] = None
        self.max_depth_water_body_fully_filled: Optional[float] = None
        self.storage_capacity_water_body_original: Optional[float] = None
        self.storage_capacity_water_body_present: Optional[float] = None
        self.category_sq_m: Optional[str] = None
        
        # === STORAGE STATUS ===
        self.filled_up_storage_name: Optional[str] = None
        self.filled_up_storage_space_name: Optional[str] = None
        
        # === CONSTRUCTION & ADMINISTRATIVE ===
        self.construcion_year: Optional[float] = None
        self.construction_cost: Optional[float] = None
        self.khasra_number: Optional[str] = None
        self.si_no_of_water_body_within_village_town: Optional[float] = None
        
        # === USAGE & BENEFICIARIES ===
        self.ref_water_body_in_use_id_name: Optional[str] = None
        self.ref_reason_water_body_in_use_id1_name: Optional[str] = None
        self.reason_water_body_in_use_name2: Optional[str] = None
        self.reason_water_body_in_use_name3: Optional[str] = None
        self.no_people_benefited_by_water_body: Optional[float] = None
        self.no_villages_benefited: Optional[float] = None
        self.no_town_cities_benefited: Optional[float] = None
        self.cca_water_body: Optional[float] = None
        self.ipc_water_body: Optional[float] = None
        
        # === MANAGEMENT & MAINTENANCE ===
        self.ref_water_body_under_repair_renovation_restoration_id_name: Optional[str] = None
        self.scheme_status_reason_name: Optional[str] = None
        self.scheme_under_revival_is_done: Optional[str] = None
        self.ref_selection_id_dip_sip_exists_name: Optional[str] = None
        self.ref_selection_id_wua_exists_name: Optional[str] = None
        
        # === ENCROACHMENT ===
        self.ref_selection_id_water_body_encroached_name: Optional[str] = None
        self.ref_selection_id_encroachment_assessed_name: Optional[str] = None
                
        # === GEOMETRY ===
        self.geometry: Optional[object] = None

class loading_util:
    @staticmethod
    def ensure_feature_list(geojson):
        """Normalize structure: Accepts Feature or FeatureCollection."""
        if geojson.get("type") == "FeatureCollection":
            return geojson.get("features", [])
        return [geojson]

    @staticmethod
    def load_terrain(obj, geojson):
        for feat in loading_util.ensure_feature_list(geojson):
            props = feat.get("properties", {})
            if props.get("uid") not in obj.microwatersheds.keys():
                obj.microwatersheds[props.get("uid")] = MWS_data()
            mws = obj.microwatersheds[props.get("uid")]
            loading_util.load_base_fields(mws, props)
            mws.plain_area = props.get("plain_area")
            mws.slopy_area = props.get("slopy_area")
            mws.hill_slope = props.get("hill_slope")
            mws.valley_are = props.get("valley_are")
            mws.ridge_area = props.get("ridge_area")
            mws.terrainClu = props.get("terrainClu")

    @staticmethod
    def load_cropping_intensity(obj, geojson):
        for feat in loading_util.ensure_feature_list(geojson):
            props = feat.get("properties", {})
            if props.get("uid") not in obj.microwatersheds.keys():
                obj.microwatersheds[props.get("uid")] = MWS_data()
            mws = obj.microwatersheds[props.get("uid")]
            loading_util.load_base_fields(mws, props)
            for y in mws.cropping_intensity.keys():
                key = f"cropping_intensity_{y}"
                if key in props:
                    mws.cropping_intensity[y] = props[key]
                key = f'single_cropped_area_{y}'
                if key in props:
                    mws.single_cropped_area[y] = props[key]
                key = f'single_kharif_cropped_area_{y}'
                if key in props:
                    mws.single_kharif_cropped_area[y] = props[key]
                key = f'single_non_kharif_cropped_area_{y}'
                if key in props:
                    mws.single_non_kharif_cropped_area[y] = props[key]
                key = f'doubly_cropped_area_{y}'
                if key in props:
                    mws.doubly_cropped_area[y] = props[key]
                key = f'triply_cropped_area_{y}'
                if key in props:
                    mws.triply_cropped_area[y] = props[key]
            mws.total_cropable_area_ever_hydroyear_2017_2023 = props.get("total_cropable_area_ever_hydroyear_2017_2023")

    @staticmethod
    def load_well_depth(obj, geojson):
        for feat in loading_util.ensure_feature_list(geojson):
            props = feat.get("properties", {})
            if props.get("uid") not in obj.microwatersheds.keys():
                obj.microwatersheds[props.get("uid")] = MWS_data()
            mws = obj.microwatersheds[props.get("uid")]
            loading_util.load_base_fields(mws, props)
            mws.weighted_a = props.get("weighted_a")
            for yr in mws.well_depth.keys():
                yr_range = "_".join([yr, str(int(yr)+1)])
                if yr_range in props:
                    mws.well_depth[yr] = props[yr_range]
            for k in mws.well_depth_net.keys():
                prop_key = f"Net{k.replace('_', '_')}"
                if prop_key in props:
                    mws.well_depth_net[k] = props[prop_key]

    @staticmethod
    def load_drought_frequency(obj, geojson):
        for feat in loading_util.ensure_feature_list(geojson):
            props = feat.get("properties", {})
            if props.get("uid") not in obj.microwatersheds.keys():
                obj.microwatersheds[props.get("uid")] = MWS_data()
            mws = obj.microwatersheds[props.get("uid")]
            loading_util.load_base_fields(mws, props)
            mws.avg_dryspell = props.get("avg_dryspell")
            for y in mws.drlb.keys():
                dr = f"drlb_{y}"
                if dr in props:
                    mws.drlb[y] = props[dr]
                ds = f"drysp_{y}"
                if ds in props:
                    mws.drysp[y] = props[ds]
                for severity in ['no', 'mld', 'mod', 'sev']:
                    key = f'w_{severity}_{y}'
                    if key in props:
                        getattr(mws, f'w_{severity}')[y] = props[key]
                key = f't_wks_{y}'
                if key in props:
                    mws.t_wks[y] = props[key]
                for t in ["frth0", "frth1", "frth2", "frth3"]:
                    key = f"{t}_{y}"
                    if key in props:
                        getattr(mws, t)[y] = props[key]
                for t in ["inth0", "inth1", "inth2", "inth3"]:
                    key = f"{t}_{y}"
                    if key in props:
                        getattr(mws, t)[y] = props[key]
                key = f'm_ons_{y}'
                if key in props:
                    mws.m_ons[y] = props[key]

    @staticmethod
    def split_every_second_underscore(s):
        parts = s.split('_')
        if len(parts) > 2:
            return ['_'.join(parts[i:i+2]) for i in range(0, len(parts), 2)]
        else:
            return [s]

    @staticmethod
    def load_waterbody_in_mws(obj, waterbody):
        mws_ids = loading_util.split_every_second_underscore(waterbody.MWS_UID)
        for mws_id in mws_ids:
            mws = obj.microwatersheds[mws_id]
            mws.waterbodies[waterbody.UID] = waterbody

    @staticmethod
    def load_waterbodies(obj, geojson):
        for feat in loading_util.ensure_feature_list(geojson):
            props = feat.get("properties", {})
            if props.get("UID") not in obj.waterbodies.keys():
                obj.waterbodies[props.get("UID")] = waterbody_data()
            waterbody = obj.waterbodies[props.get("UID")]
            waterbody.UID = props.get("UID")
            waterbody.census_id = props.get("census_id")
            waterbody.MWS_UID = props.get("MWS_UID")
            loading_util.load_waterbody_in_mws(obj, waterbody)
            waterbody.geometry = feat.get("geometry")
            for yr in waterbody.area.keys():
                src = f"area_{str(int(yr))[2:]}-{str(int(yr)+1)[2:]}"
                if src in props:
                    waterbody.area[yr] = props[src]
            for prefix, target in [("k", waterbody.k), ("kr", waterbody.kr), ("krz", waterbody.krz)]:
                for yr in target.keys():
                    src = f"{prefix}_{str(int(yr))[2:]}-{str(int(yr)+1)[2:]}"
                    if src in props:
                        target[yr] = props[src]

    @staticmethod
    def load_base_fields(mws, props):
        mws.uid = mws.uid or props.get("uid")
        mws.area_in_ha = mws.area_in_ha or props.get("area_in_ha")
        mws.geometry = mws.geometry or props.get("geometry")  # Keep if already loaded

