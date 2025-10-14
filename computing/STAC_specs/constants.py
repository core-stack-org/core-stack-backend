# constants.py
from datetime import datetime

DEFAULT_START_DATE = datetime(2017, 7, 1)
DEFAULT_END_DATE = datetime(2024, 6, 30)

SRTM_DEM_START_DATE = datetime(2020,2,11) #used for terrain raster as it 
#uses SRTMGL1_003 product
# https://www.earthdata.nasa.gov/data/catalog/lpcloud-srtmgl1-003
SRTM_DEM_END_DATE = datetime(2020,2,21)

NO_DATA_CLASSNAMES_LIST = ['clear','background','No data']

BASE_URL="https://raw.githubusercontent.com/Nirzaree/STAC-spec/stac-spec-common/"
GITHUB_DATA_URL="https://raw.githubusercontent.com/Nirzaree/STAC-spec/stac-spec-common/data/"

STAC_VERSION="1.0.0"
ROOT_CATALOG_TITLE="CoREStack Spatio Temporal Asset Catalog"
ROOT_CATALOG_DESCRIPTION="This spatio temporal asset catalog contains all data layers of CoREStack (https://core-stack.org/). The data layers are generated at an administrative block level, and some layers are available for Pan India."

GEOSERVER_BASE_URL = "https://geoserver.core-stack.org:8443/geoserver"

AGRI_YEAR_START_DATE = '07-01'
AGRI_YEAR_END_DATE = '06-30'

#Ground sample distance (aka Raster spatial resolution) in meters for different layers 
# specifying here because no easy way to get these from geoserver data
# LULC_RASTER_GSD_M = 10
# TERRAIN_RASTER_GSD_M = 30
# TREE_CANOPY_HEIGHT_GSD_M = 25
# TREE_CANOPY_COVER_DENSITY_GSD_M = 25
# STREAM_ORDER_RASTER_GSD_M = 30
# CLART_RASTER_GSD_M = 30
