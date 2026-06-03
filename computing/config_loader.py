from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT1 = "/home/cfpt-jedi/developer/shiv/core-stack-backend/data/canal/canal_local"


PAN_INDIA_DRAINAGE_LINES_GPKG_PATH = (
    PROJECT_ROOT / "data/base_layers/drainage_lines_pan_india.gpkg"
)

PAN_INDIA_DRAINAGE_LINES_PATH = PROJECT_ROOT / "data/layers/drainage_lines/Pan_India_drainage_lines.gpkg"
LOCAL_DRAINAGE_LINES_OUTPUT = PROJECT_ROOT / "data/layers/drainage_lines/drainage_lines_local"

LOCAL_DRAINAGE_DENSITY_OUTPUT = PROJECT_ROOT / "data/drainage_density"

PAN_INDIA_CANAL_PATH = PROJECT_ROOT / "data/canal/Canal_pan_india.geojson"
LOCAL_CANAL_OUTPUT = PROJECT_ROOT1 / "data/canal/canal_local"

PAN_INDIA_AGROECOLOGICAL_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_agroecological_farming.geojson"
LOCAL_AGROECOLOGICAL_OUTPUT = PROJECT_ROOT / "data/layers/agroecological"

PAN_INDIA_LCW_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_lcw_conflict.geojson"
LOCAL_LCW_OUTPUT = PROJECT_ROOT / "data/layers/lcw_conflict"

PAN_INDIA_SOGE_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_SOGE_2020.geojson"
LOCAL_SOGE_OUTPUT = PROJECT_ROOT / "data/layers/SOGE_vector"

PAN_INDIA_FACTORY_CSR_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_factory_csr.geojson"
LOCAL_FACTORY_CSR_OUTPUT = PROJECT_ROOT / "data/layers/factory_csr"

PAN_INDIA_GREEN_CREDIT_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_green_credit.geojson"
LOCAL_GREEN_CREDIT_OUTPUT = PROJECT_ROOT / "data/layers/green_credit"

PAN_INDIA_MINING_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_mining.geojson"
LOCAL_MINING_OUTPUT = PROJECT_ROOT / "data/layers/mining"

PAN_INDIA_NATURALDEPRESSION_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_natural_depression.tif"
LOCAL_NATURALDEPRESSION_OUTPUT = PROJECT_ROOT / "data/layers/natural_depression"

PAN_INDIA_DISTANCETONEARESTDRAINAGE_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_distance_to_nearest_drainage.tif"
LOCAL_DISTANCETONEARESTDRAINAGE_OUTPUT = PROJECT_ROOT / "data/layers/distance_nearest_upstream_DL"

PAN_INDIA_FACILITIES_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_facilities_polygon.geojson"
LOCAL_FACILITIES_OUTPUT = PROJECT_ROOT / "data/layers/facilities"
PAN_INDIA_CATCHMENT_AREA_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_catchment_area.tif"
LOCAL_CATCHMENT_AREA_OUTPUT = PROJECT_ROOT / "data/layers/catchment_area_singleflow"

PAN_INDIA_SLOPE_PERCENTAGE_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_slope_percentage.tif"
LOCAL_SLOPE_PERCENTAGE_OUTPUT = PROJECT_ROOT / "data/layers/slope_percentage"

PAN_INDIA_MWS_CONNECTIVITY_PATH = PROJECT_ROOT / "data/layers/mws_connectivity/Pan_India_mws_connectivity.geojson"
LOCAL_MWS_CONNECTIVITY_OUTPUT = PROJECT_ROOT / "data/layers/mws_connectivity/mws_connectivity_local"

LOCAL_MWS_CENTROID_OUTPUT = PROJECT_ROOT / "data/layers/mws_centroid"

NREGA_LOCAL_OUTPUT = PROJECT_ROOT / "data/layers/nrega_assets"
PAN_INDIA_RESTORATION_PATH = PROJECT_ROOT / "data/base_layers/Pan_India_WRI_Restoration.tif"
LOCAL_RESTORATION_OUTPUT = PROJECT_ROOT / "data/layers/restoration_opportunity"

PAN_INDIA_RIVER_PATH = PROJECT_ROOT / "data/river/River_pan_india.geojson"
LOCAL_RIVER_OUTPUT = PROJECT_ROOT1 / "data/river/river_local"

PAN_INDIA_FABDEM_PATH = str(PROJECT_ROOT / "data/fabdem/fabdem_pan_india.tif")
LOCAL_FABDEM_OUTPUT = str(PROJECT_ROOT1 / "data/fabdem/fabdem_local")
