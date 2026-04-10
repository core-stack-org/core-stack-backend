# Directory Path
ADMIN_BOUNDARY_INPUT_DIR = "data/admin-boundary/input"
ADMIN_BOUNDARY_OUTPUT_DIR = "data/admin-boundary/output"

NREGA_ASSETS_INPUT_DIR = "data/nrega_assets/input"
NREGA_ASSETS_OUTPUT_DIR = "data/nrega_assets/output"

MERGE_MWS_PATH = "data/merge_mws"

RASTERS_PATH = "data/rasters"
CROP_GRID_PATH = "data/crop_grid"

KML_PATH = "data/kml/"
SHAPEFILE_DIR = "data/kml/shapefiles"

DRAINAGE_LINES_SHAPEFILES = "data/drainage_lines/input"
BASIN_BOUNDARIES = "data/drainage_lines/input/basin_boundaries"
DRAINAGE_LINES_OUTPUT = "data/drainage_lines/output"
DRAINAGE_DENSITY_OUTPUT = "data/drainage_density"

LITHOLOGY_PATH = "data/lithology/"
SITE_DATA_PATH = "data/site_data"

WHATSAPP_MEDIA_PATH = "data/whatsapp_media/"


# MARK: ODK URLs
ODK_BASE_URL = "https://odk.core-stack.org/v1/projects/"
ODK_URL_SESSION = "https://odk.core-stack.org/v1/sessions"
ODK_PROJECT_ID = "2"

# Resource Mapping
ODK_URL_settlement = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/Add_Settlements_form%20_V1.0.1.svc/Submissions"
)
ODK_URL_well = (
    ODK_BASE_URL + ODK_PROJECT_ID + "/forms/Add_well_form_V1.0.1.svc/Submissions"
)
ODK_URL_waterbody = (
    ODK_BASE_URL + ODK_PROJECT_ID + "/forms/Add_Waterbodies_Form_V1.0.3.svc/Submissions"
)

ODK_URL_crop = ODK_BASE_URL + ODK_PROJECT_ID + "/forms/crop_form_V1.0.0.svc/Submissions"

# Planning Forms
ODK_URL_gw = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/NRM_form_propose_new_recharge_structure_V1.0.0.svc/Submissions"
)
ODK_URL_swb = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/NRM_form_NRM_form_Waterbody_Screen_V1.0.0.svc/Submissions"
)
ODK_URL_agri = (
    ODK_BASE_URL + ODK_PROJECT_ID + "/forms/NRM_form_Agri_Screen_V1.0.0.svc/Submissions"
)
ODK_URL_livelihood = (
    ODK_BASE_URL + ODK_PROJECT_ID + "/forms/NRM%20Livelihood%20Form.svc/Submissions"
)

# Maintenance forms
ODK_URL_WATERBODY_MAINTENANCE = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/Propose_Maintenance_on_Existing_Water_Recharge_Structures_V1.1.1.svc/Submissions"
)
ODK_URL_RS_WATERBODY_MAINTENANCE = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/PM_Remote_Sensed_Surface_Water_structure_V1.0.0.svc/Submissions"
)
ODK_URL_GW_MAINTENANCE = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/NRM_form_NRM_form_Waterbody_Screen_V1.0.0.svc/Submissions"
)
ODK_URL_AGRI_MAINTENANCE = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/Propose_Maintenance_on_Existing_Irrigation_Structures_V1.1.1.svc/Submissions"
)

# MARK: Sync Offline ODK URLs
ODK_SYNC_URL_SETTLEMENT = (
    ODK_BASE_URL + ODK_PROJECT_ID + "/forms/Add_Settlements_form%20_V1.0.1/submissions"
)
ODK_SYNC_URL_WELL = (
    ODK_BASE_URL + ODK_PROJECT_ID + "/forms/Add_well_form_V1.0.1/submissions"
)
ODK_SYNC_URL_WATER_STRUCTURES = (
    ODK_BASE_URL + ODK_PROJECT_ID + "/forms/Add_Waterbodies_Form_V1.0.3/submissions"
)

ODK_SYNC_URL_CROP = (
    ODK_BASE_URL + ODK_PROJECT_ID + "/forms/crop_form_V1.0.0/submissions"
)

ODK_SYNC_URL_RECHARGE_STRUCTURE = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/NRM_form_propose_new_recharge_structure_V1.0.0/submissions"
)

ODK_SYNC_URL_IRRIGATION_STRUCTURE = (
    ODK_BASE_URL + ODK_PROJECT_ID + "/forms/NRM_form_Agri_Screen_V1.0.0/submissions"
)

ODK_SYNC_URL_LIVELIHOOD = (
    ODK_BASE_URL + ODK_PROJECT_ID + "/forms/NRM%20Livelihood%20Form/submissions"
)

ODK_SYNC_URL_RS_WATERBODY_MAINTENANCE = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/PM_Remote_Sensed_Surface_Water_structure_V1.0.0/submissions"
)

ODK_SYNC_URL_WATER_STRUCTURES_MAINTENANCE = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/NRM_form_NRM_form_Waterbody_Screen_V1.0.0/submissions"
)

ODK_SYNC_URL_GW_MAINTENANCE = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/Propose_Maintenance_on_Existing_Water_Recharge_Structures_V1.1.1/submissions"
)

ODK_SYNC_URL_AGRI_MAINTENANCE = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/Propose_Maintenance_on_Existing_Irrigation_Structures_V1.1.1/submissions"
)

ODK_SYNC_URL_GW_FEEDBACK = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/nrm_groundwater_analysis_feedback_form_V1.0.0/submissions"
)

ODK_SYNC_URL_SWB_FEEDBACK = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/nrm_waterbody_analysis_feedback_form_V1.0.0/submissions"
)

ODK_SYNC_URL_AGRI_FEEDBACK = (
    ODK_BASE_URL
    + ODK_PROJECT_ID
    + "/forms/nrm_agri_analysis_feedback_form_V1.0.0/submissions"
)

# MARK: GEE Paths
GCS_BUCKET_NAME = "core_stack"

GEE_ASSET_PATH = "projects/ee-corestackdev/assets/apps/mws/"
GEE_HELPER_PATH = "projects/ee-corestack-helper/assets/apps/mws/"

GEE_PATH_PLANTATION = "projects/ee-corestackdev/assets/apps/plantation/"
GEE_PATH_PLANTATION_HELPER = "projects/ee-corestack-helper/assets/apps/plantation/"

GEE_BASE_PATH = "projects/ee-corestackdev/assets/apps"
GEE_HELPER_BASE_PATH = "projects/ee-corestack-helper/assets/apps"

GEE_DATASET_PATH = "projects/corestack-datasets/assets/datasets"
AQUIFER_DATASET_PATH = "projects/corestack-datasets/assets/datasets/Aquifer_vector"
PAN_INDIA_DRAINAGE_LINES_DATASET = (
    "projects/corestack-datasets/assets/datasets/drainage-line/pan_india_drainage_lines"
)
GEE_EXT_DATASET_PATH = "projects/ext-datasets/assets/datasets"
AGROECOLOGICAL_PAN_INDIA_DATASET = (
    "projects/ext-datasets/assets/datasets/Agroecological_space_pan_india"
)

GEE_FACILITIES_DATASET_PATH = (
    "projects/corestack-datasets/assets/datasets/pan_india_facilities"
)

GEE_PATHS = {
    "MWS": {
        "GEE_ASSET_PATH": GEE_BASE_PATH + "/mws/",
        "GEE_HELPER_PATH": GEE_HELPER_BASE_PATH + "/mws/",
    },
    "PLANTATION": {
        "GEE_ASSET_PATH": GEE_BASE_PATH + "/plantation/",
        "GEE_HELPER_PATH": GEE_HELPER_BASE_PATH + "/plantation/",
    },
    "WATERBODY": {
        "GEE_ASSET_PATH": GEE_BASE_PATH + "/waterbody/",
        "GEE_HELPER_PATH": GEE_HELPER_BASE_PATH + "waterbody/",
        "GEE_ASSET_FOLDER": "waterbody/",
    },
}

PAN_INDIA_RIVER_BASIN_LULC_V3_BASE_PATH = (
    "projects/corestack-datasets/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3"
)

LULC_V2_RIVER_BASIN_OUTPUT_PATH = (
    "projects/corestack-datasets/assets/datasets/lulc_v2_river_basin/"
)

LULC_V3_OUTPUT_ASSET_PATH = "projects/corestack-datasets/assets/datasets/lulc_v3/"


# Moderation Constants
filter_query_updated = "$filter=__system/submissionDate ge 2025-11-28T00:00:00.000Z"
filter_query_edited = "$filter=__system/submissionDate lt 2025-11-28T00:00:00.000Z and __system/updatedAt ge 2025-11-28T00:00:00.000Z"
filter_query = (
    "$filter=(day(__system/submissionDate) ge 14 "
    "and month(__system/submissionDate) ge 12 "
    "and year(__system/submissionDate) ge 2025) "
    "or (day(__system/updatedAt) ge 14 "
    "and month(__system/updatedAt) ge 12 "
    "and year(__system/updatedAt) eq 2025)"
)
project_id = 2

# demand vaidator constants
DRAINAGE_LINES_ASSET = (
    "projects/corestack-datasets/assets/datasets/drainage-line/pan_india_drainage_lines"
)
GLOBAL_DRAINAGE_EPS_M = 10.0
GEOSERVER_BASE = "https://geoserver.core-stack.org:8443/geoserver/"
WORKSPACE_URL_END = "wms?service=WMS&request=GetCapabilities"
WORKS_WORKSPACE = "works"
RESOURCES_WORKSPACE = "resources"
LULC_ASSET = "projects/corestack-datasets/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3_2024_2025"
SRTM_DIGITAL_ELEVATION = "USGS/SRTMGL1_003"
CATCHMENT_ASSET = "projects/ext-datasets/assets/datasets/catchment_area_multiflow"
STREAM_ORDER_ASSET = (
    "projects/corestack-datasets/assets/datasets/Stream_Order_Raster_India"
)

# Datasets
INDIA_LINEAMENTS = "projects/ee-harshita-om/assets/india_lineaments"
CHIRPS_PPT = "UCSB-CHG/CHIRPS/DAILY"
MODIS_TERRA_NET_ET_GAP_FILLED_8_DAY = "MODIS/061/MOD16A2GF"
MODIS_TERRA_SURFACE_REFLECTANCE = "MODIS/061/MOD09A1"
CGWB_BASIN = "projects/corestack-datasets/assets/datasets/CGWB_basin"
SENTINEL2_LEVEL_1C_TOA = "COPERNICUS/S2_HARMONIZED"
LAND_COVER_CLASSIFICATION_10_METER = "GOOGLE/DYNAMICWORLD/V1"
SENTINEL1_GRD = "COPERNICUS/S1_GRD"
DEM_OF_90_M_RESOLUTION = "CGIAR/SRTM90_V4"
CROPLAND_DATASET_PATH = "projects/ee-indiasat/assets/Rasterized_Groundtruth/L2_TrainingData_SAR_TimeSeries_1Year"
LANDSAT7_T1_CALIBERATED_TOA = "LANDSAT/LE07/C02/T1_TOA"
LANDSAT8_T1_CALIBERATED_TOA = "LANDSAT/LC08/C02/T1_TOA"
VEGETATION_INDEX_OF_16_DAY = "MODIS/061/MOD13Q1"
PAN_INDIA_L3_LULC_CLUSTERS = (
    "projects/ee-indiasat/assets/L3_LULC_Clusters/Final_Level3_PanIndia_Clusters"
)
AEZ = "users/mtpictd/agro_eco_regions"
FACILITIES_DATASET_NAME = "Facilities Proximity"
LCW_PAN_INDIA_DATASET = "projects/ext-datasets/assets/datasets/lcw_conflict_pan_india"
MINING_PAN_INDIA_DATASET = "projects/ext-datasets/assets/datasets/Mining_data_pan_india"
FACTORY_PAN_INDIA_DATASET = (
    "projects/ext-datasets/assets/datasets/Factory_CSR_pan_india"
)
GREEN_CREDIT_PAN_INDIA_DATASET = (
    "projects/ext-datasets/assets/datasets/Green_credit_pan_india"
)
HARMONIZED_LANDSAT_SENTINEL = "NASA/HLS/HLSL30/v002"
NBAR_MSI = "NASA/HLS/HLSS30/v002"
SOGE_DATASET = "projects/corestack-datasets/assets/datasets/SOGE_vector_2020"
WRI_LAND_RESTORATION_DATASET = (
    "projects/corestack-datasets/assets/datasets/WRI/LandscapeRestorationOpportunities"
)
MWS_DATASET = (
    "projects/corestack-datasets/assets/datasets/hydrological_boundaries/microwatershed"
)
MWS_CONNECTIVITY_DATASET = (
    "projects/corestack-datasets/assets/datasets/India_mws_connectivity"
)
ET_FLDAS_FORTNIGHT = "projects/corestack-datasets-alpha/assets/datasets/ET_FLDAS/ET_fortnight/Hydro_20200111_20200124"
ET_FLDAS_ANNUAL = (
    "projects/corestack-datasets-alpha/assets/datasets/ET_FLDAS/ET_annual/ET_"
)
ET_FLDAS_HYDRO = (
    "projects/corestack-datasets-alpha/assets/datasets/ET_FLDAS/ET_fortnight/Hydro_"
)
FLDAS_NOAH_LAND_SURFACE_MODEL = "NASA/FLDAS/NOAH01/C/GL/M/V001"
JAXA_PPT = "JAXA/GPM_L3/GSMaP/v6/operational"
RUNOFF_ASSET = "projects/ee-dharmisha-siddharth/assets/HYSOGs250m"
PRINCIPAL_AQUIFER = "projects/ee-anz208490/assets/principalAquifer"
INDIA_SAT_LULC_V3_PAN_INDIA = "/LULC_v3_river_basin/pan_india_lulc_v3_"
ROAD_DRRP = "projects/ext-datasets/assets/datasets/Road_DRRP/"
WWF_HYDROSHEDS_DRAINAGE_DIRECTION = "WWF/HydroSHEDS/03DIR"
PAN_INDIA_RASTER_FABDEM = "projects/corestack-datasets/assets/datasets/terrain/pan_india_terrain_raster_fabdem"
SOI_TEHSIL = "data/admin-boundary/input/soi_tehsil.geojson"
FABDEM = "projects/sat-io/open-datasets/FABDEM"
WATERREJUVENATION = "projects/ee-corestackdev/assets/apps/waterrej/proj1"
WATERREJ_LULCFORM = "projects/ee-corestackdev/assets/apps/waterrej/lulcfrom"
WATER_REJ_GEE_ASSET = "projects/ee-corestackdev/assets/apps/waterbody/"
WATER_REJ_TEST_GEE_ASSET = "projects/ee-kapil-test/assets/apps/waterbody/"
PAN_INDIA_LULC_V3_DATASET = (
    "projects/corestack-datasets/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3_"
)
# CRS
CRS_4326 = "EPSG:4326"

# Algorithm
DROUGHT_ALGORITHM = "MOD09A1-NDVI/NDWI"

# workspace
FACILITIES_GEOSERVER_WORKSPACE = "facilities_proximity"

# other
FIRST_COMPUTING_API_PATH = "/api/v1/generate_block_layer/"
VATSAL_WBC = "projects/ee-vatsal/assets/WBC_"

# Plantation
ANNUAL_PPT = "projects/ee-plantationsitescores/assets/AnnualPrecipitation"
MEAN_ANNUAL_TEMP = "projects/ee-plantationsitescores/assets/MeanAnnualTemp"
ARDITY_INDEX = "projects/ee-plantationsitescores/assets/India-AridityIndex"
REFERENCE_ET = "projects/ee-plantationsitescores/assets/ReferenceEvapotranspiration"
AWC = "projects/ee-plantationsitescores/assets/Raster-AWC_CLASS"
TOPOSOILPH = "projects/ee-plantationsitescores/assets/Raster-T_PH_H2O"
TOPOSOILBD = "projects/ee-plantationsitescores/assets/Raster-T_BULK_DEN"
TOPOSOILOC = "projects/ee-plantationsitescores/assets/Raster-T_OC"
TOPOSOILEC = "projects/ee-plantationsitescores/assets/Raster-T_CEC_SOIL"
TOPOSOILTEXTURE = "projects/ee-plantationsitescores/assets/Raster-T_TEXTURE"
SUBSOILPH = "projects/ee-plantationsitescores/assets/Raster-S_PH_H2O"
SUBSOILBD = "projects/ee-plantationsitescores/assets/Raster-S_BULK_DEN"
SUBSOILOC = "projects/ee-plantationsitescores/assets/Raster-S_OC"
SUBSOILEC = "projects/ee-plantationsitescores/assets/Raster-S_CEC_SOIL"
SUBSOILTEXTURE = "projects/ee-plantationsitescores/assets/Raster-S_USDA_TEX_CLASS"
RASTER_DRAINAGE = "projects/ee-plantationsitescores/assets/Raster-Drainage"
PLANTATION_SITE_SCORE = "projects/ee-plantationsitescores/assets/so_thinned2"

# CSV Paths
STAT_CSV_PATH = "/home/ankit/gramvaani/nrm/checkin/backend/fromgitlab/nrm-app/data/admin-boundaries/odisha.csv"
OUTPUT_JSON_PATH = "/home/ankit/gramvaani/nrm/checkin/backend/fromgitlab/nrm-app/data/output/all_states.json"
folder_path = (
    "/home/ankit/gramvaani/nrm/checkin/backend/nrm-app/data/lulc/LULC_devdurga_3oct2024"
)
zip_to_geoserver_folder_path = "/home/ankit/gramvaani/nrm/checkin/backend/fromgitlab/nrm-app/data/zip-shapefiles/drainage"
