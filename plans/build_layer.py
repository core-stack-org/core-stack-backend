import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from geopandas import GeoDataFrame
from io import BytesIO
import zipfile
import requests
import os
import sys
from typing import Optional
import tempfile
from dpr.utils import transform_name
from utilities.logger import logger
import traceback
from nrm_app.settings import GEOSERVER_URL, GEOSERVER_PASSWORD, GEOSERVER_USERNAME


class Geoserver_BB:
    def __init__(
        self,
        service_url: str = GEOSERVER_URL,
        username: str = GEOSERVER_USERNAME,
        password: str = GEOSERVER_PASSWORD,
    ):
        self.service_url = service_url
        self.username = username
        self.password = password
        logger.debug(f"Initialized Geoserver_BB with URL: {service_url}")

    def test_connection(self):
        try:
            test_url = f"{self.service_url}/rest/about/status"
            response = requests.get(
                test_url,
                auth=(self.username, self.password),
                verify=True,  # Change to False if using self-signed cert
            )
            logger.debug(f"Connection test status code: {response.status_code}")
            return response.status_code in [200, 201]
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False

    def create_datastore(
        self,
        path: BytesIO,
        store_name: str,
        workspace: str,
        file_extension: str = "shp",
    ):
        try:
            headers = {
                "Content-type": "application/zip",
                "Accept": "application/xml",
            }

            url = f"{self.service_url}/rest/workspaces/{workspace}/datastores/{store_name}/file.{file_extension}?filename={store_name}&update=overwrite"
            logger.debug(f"Attempting to create datastore at URL: {url}")

            if not self.test_connection():
                raise Exception("Failed to connect to GeoServer")

            r = requests.put(
                url,
                data=path.getvalue(),
                auth=(self.username, self.password),
                headers=headers,
                verify=True,  # Change to False if using self-signed cert
            )

            logger.debug(f"Create datastore response: {r.status_code}")
            logger.debug(f"Response content: {r.content}")

            if r.status_code in [200, 201, 202]:
                return "The shapefile datastore created successfully!"
            else:
                raise Exception(f"GeoServer Error: {r.status_code}, {r.content}")
        except Exception as e:
            logger.error(f"Error in create_datastore: {str(e)}")
            raise


def build_layer(layer_type, item_type, plan_id, district, block, csv_path):
    try:
        logger.info(
            f"Starting build_layer with params: type={layer_type}, item={item_type}, plan={plan_id}, district={district}, block={block}"
        )
        logger.debug(f"CSV path: {csv_path}")
        logger.debug(f"Current working directory: {os.getcwd()}")

        # Verify CSV exists and is readable
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        # Load CSV and create geometry
        df_geom = pd.read_csv(csv_path)
        logger.debug(f"CSV loaded successfully with {len(df_geom)} rows")

        # Verify required columns exist
        required_columns = ["longitude", "latitude"]
        missing_columns = [
            col for col in required_columns if col not in df_geom.columns
        ]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        geometry = [Point(xy) for xy in zip(df_geom["longitude"], df_geom["latitude"])]
        gdf = gpd.GeoDataFrame(df_geom, geometry=geometry)
        gdf.crs = "EPSG:4326"

        formatted_block = transform_name(block_name=block)
        store_layer_name = f"{item_type}_{plan_id}_{district}_{formatted_block}"
        logger.debug(f"Store layer name: {store_layer_name}")

        # Use a temporary directory with explicit permissions
        with tempfile.TemporaryDirectory(prefix="geoserver_") as tmpdirname:
            os.chmod(tmpdirname, 0o777)  # Ensure write permissions
            shapefile_path = os.path.join(tmpdirname, f"{store_layer_name}.shp")
            logger.debug(f"Temporary shapefile path: {shapefile_path}")

            gdf.to_file(shapefile_path, driver="ESRI Shapefile")
            logger.debug("Shapefile created successfully")

            with BytesIO() as shapefile_buffer:
                with zipfile.ZipFile(
                    shapefile_buffer, "w", zipfile.ZIP_DEFLATED
                ) as zip_buffer:
                    for filename in os.listdir(tmpdirname):
                        file_path = os.path.join(tmpdirname, filename)
                        with open(file_path, "rb") as file:
                            zip_buffer.writestr(filename, file.read())

                shapefile_buffer.seek(0)
                logger.debug("ZIP file created in memory")

                push_result = push_layer_to_geoserver(
                    shapefile_buffer, store_layer_name, workspace=layer_type
                )
                logger.info(f"Geoserver Push Result: {push_result}")

        return True
    except Exception as e:
        logger.error(f"Exception in build_layer: {str(e)}")
        logger.error(traceback.format_exc())
        return False


def push_layer_to_geoserver(
    in_memory_zip, store_layer_name, workspace="test_workspace"
):
    try:
        logger.debug(
            f"Pushing layer to geoserver: {store_layer_name} in workspace {workspace}"
        )
        geo = Geoserver_BB()
        return geo.create_datastore(
            path=in_memory_zip, store_name=store_layer_name, workspace=workspace
        )
    except Exception as e:
        logger.error(f"Error pushing to geoserver: {str(e)}")
        raise
