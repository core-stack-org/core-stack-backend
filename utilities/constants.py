# contains the URLs of various resources
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
ODK_URL_crop = ODK_BASE_URL + ODK_PROJECT_ID + "/forms/crop_form_V1.0.0.svc/Submissions"

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

# Sync Offline ODK
ODK_SYNC_URL_SETTLEMENT = (
    ODK_BASE_URL + ODK_PROJECT_ID + "/forms/Add_Settlements_form%20_V1.0.1/submissions"
)
ODK_SYNC_URL_WELL = (
    ODK_BASE_URL + ODK_PROJECT_ID + "/forms/Add_well_form_V1.0.1/submissions"
)
ODK_SYNC_URL_WATER_STRUCTURES = (
    ODK_BASE_URL + ODK_PROJECT_ID + "/forms/Add_Waterbodies_Form_V1.0.3/submissions"
)

GCS_BUCKET_NAME = "core_stack"

GEE_ASSET_PATH = "projects/ee-corestackdev/assets/apps/mws/"
GEE_HELPER_PATH = "projects/ee-corestack-helper/assets/apps/mws/"

GEE_PATH_PLANTATION = "projects/ee-corestackdev/assets/apps/plantation/"
GEE_PATH_PLANTATION_HELPER = "projects/ee-corestack-helper/assets/apps/plantation/"

GEE_BASE_PATH = "projects/ee-corestackdev/assets/apps"
GEE_HELPER_BASE_PATH = "projects/ee-corestack-helper/assets/apps"

GEE_PATHS = {
    "MWS": {
        "GEE_ASSET_PATH": GEE_BASE_PATH + "/mws/",
        "GEE_HELPER_PATH": GEE_HELPER_BASE_PATH + "/mws/",
    },
    "PLANTATION": {
        "GEE_ASSET_PATH": GEE_BASE_PATH + "/plantation/",
        "GEE_HELPER_PATH": GEE_HELPER_BASE_PATH + "/plantation/",
    },
    "WATER_REJ": {
        "GEE_ASSET_PATH": GEE_BASE_PATH + "/waterrej/",
        "GEE_HELPER_PATH": GEE_HELPER_BASE_PATH + "/waterrej/",
    },
}
