import os
import geopandas as gpd
import pandas as pd
import json
from nrm_app.celery import app
from shapely import wkt
from shapely.geometry import Point
from computing.utils import push_shape_to_geoserver, save_layer_info_to_db
from utilities.constants import (
    ADMIN_BOUNDARY_INPUT_DIR,
    NREGA_ASSETS_INPUT_DIR,
    NREGA_ASSETS_OUTPUT_DIR,
)
from unidecode import unidecode
import boto3
from io import BytesIO
from nrm_app.settings import NREGA_BUCKET, NREGA_ACCESS_KEY, NREGA_SECRET_KEY



@app.task(bind=True)
def clip_nrega_district_block(self, state_name, district_name, block_name):
    print("inside clip")
    s3 = boto3.resource(
        service_name="s3",
        region_name="ap-south-1",
        aws_access_key_id=NREGA_ACCESS_KEY,
        aws_secret_access_key=NREGA_SECRET_KEY
    )

    formatted_state_name = state_name.title()
    formatted_district_name = district_name.title()
    if " " in formatted_state_name:
        formatted_state_name = formatted_state_name.replace(" ", "_")
    if " " in district_name:
        formatted_district_name = district_name.replace(" ", "_")

    nrega_dist_file = f"{formatted_state_name.upper()}/{formatted_district_name.upper()}.geojson"

    try:
        file_obj = s3.Object(NREGA_BUCKET, nrega_dist_file).get()
        district_gdf = gpd.read_file(BytesIO(file_obj['Body'].read()))
    except Exception as e:
        print(f"Error reading file from S3: {e}")
        district_gdf = gpd.GeoDataFrame(columns=["Work Name", "Panchayat", "lon", "lat", "geometry"], geometry="geometry", crs="EPSG:4326")

    # If geometry is missing but lat/lon present, build Point geometry
    if 'geometry' not in district_gdf.columns and 'lon' in district_gdf.columns and 'lat' in district_gdf.columns:
        print("Creating geometry from lat/lon...")
        district_gdf['geometry'] = [Point(xy) for xy in zip(district_gdf["lon"], district_gdf["lat"])]
        district_gdf = gpd.GeoDataFrame(district_gdf, geometry='geometry', crs="EPSG:4326")
    
    # If GeoJSON was loaded but has no geometry, create Point geometries from lat/lon
    if 'geometry' not in district_gdf.columns and 'lon' in district_gdf.columns and 'lat' in district_gdf.columns:
        district_gdf['geometry'] = [
            Point(xy) for xy in zip(district_gdf["lon"], district_gdf["lat"])
        ]
        district_gdf = gpd.GeoDataFrame(district_gdf, geometry='geometry', crs="EPSG:4326")

    # Load SOI tehsil boundary
    soi = gpd.read_file(ADMIN_BOUNDARY_INPUT_DIR + "/soi_tehsil.geojson")

    # Filter by state, district, block
    soi = soi[(soi["STATE"].str.lower() == state_name.lower())]
    soi = soi[(soi["District"].str.lower() == district_name.lower())]
    soi = soi[(soi["TEHSIL"].str.lower() == block_name.lower())]
    soi = soi.dissolve()

    block_bounds = soi.geometry.iloc[0] if not soi.empty else None
    
    # Create empty dataframe if no matching boundary was found
    if block_bounds is None:
        print(f"No matching boundary found for state={state_name}, district={district_name}, block={block_name}")
        block_metadata_df = district_gdf.iloc[0:0].copy()  # Empty dataframe with same schema
        
    elif not district_gdf.empty and 'geometry' in district_gdf.columns:
        block_metadata_df = district_gdf[district_gdf.geometry.within(block_bounds)]
        if block_metadata_df.empty:
            print("No NREGA assets found within the block boundary")
            block_metadata_df = district_gdf.iloc[0:0].copy()  # Empty dataframe with same schema
    else:
        print("Using empty dataframe due to missing data or geometry")
        block_metadata_df = district_gdf.iloc[0:0].copy()  # Empty dataframe with same schema
    
    # Apply unidecode to string columns
    string_columns = block_metadata_df.select_dtypes(include=['object']).columns.tolist()
    for col in string_columns:
        if col != 'geometry':
            block_metadata_df[col] = block_metadata_df[col].apply(
                lambda x: unidecode(x) if isinstance(x, str) else x
            )
    
    # Ensure CRS is set
    block_metadata_df.crs = "EPSG:4326"

    path = os.path.join(
        NREGA_ASSETS_OUTPUT_DIR,
        f"""{"_".join(district_name.split())}_{"_".join(block_name.split())}""",
    )

    block_metadata_df.to_file(path, driver="ESRI Shapefile", encoding="UTF-8")
    return push_shape_to_geoserver(path, workspace="nrega_assets")
    save_layer_info_to_db(state_name, district_name, block_name, f"{district_name.title()}_{block_name.title()}","no asset id", "NREGA Assets")