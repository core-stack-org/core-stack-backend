import os
import tempfile
import traceback
from io import BytesIO

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point

from dpr.utils import transform_name
from nrm_app.settings import GEOSERVER_PASSWORD, GEOSERVER_URL, GEOSERVER_USERNAME
from utilities.logger import logger


# GeoPackage is intentionally preferred over ESRI Shapefile:
#   - Shapefile DBF truncates field names to 10 chars, capping fields at ~255
#     and causing collisions across the merged ODK-blob + moderation columns.
#   - GeoPackage has effectively no limit on field names or count, native
#     UTF-8, richer types, and is a single file (no zip bundle needed).
_DEFAULT_FORMAT = "gpkg"
_DRIVER_BY_EXTENSION = {
    "gpkg": "GPKG",
    "shp": "ESRI Shapefile",
}
_CONTENT_TYPE_BY_EXTENSION = {
    "gpkg": "application/x-sqlite3",
    "shp": "application/zip",
}


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

    def test_connection(self) -> bool:
        try:
            test_url = f"{self.service_url}/rest/about/status"
            response = requests.get(
                test_url, auth=(self.username, self.password), verify=True
            )
            logger.debug(f"Connection test status code: {response.status_code}")
            return response.status_code in (200, 201)
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False

    def delete_datastore_if_exists(self, workspace: str, store_name: str) -> bool:
        """
        Idempotently remove an existing datastore (with its layers) so a
        subsequent upload registers a fresh datastore of the new type.
        GeoServer's `file.<ext>?update=overwrite` only replaces the file
        inside an existing datastore — it does NOT change the datastore's
        type, so a stale shapefile store would keep pointing at a missing
        `.shp` after we switch to GeoPackage.
        """
        url = (
            f"{self.service_url}/rest/workspaces/{workspace}/datastores/"
            f"{store_name}?recurse=true"
        )
        try:
            r = requests.delete(
                url, auth=(self.username, self.password), verify=True
            )
            if r.status_code in (200, 202):
                logger.info(
                    f"Deleted existing datastore '{workspace}:{store_name}' "
                    f"(status={r.status_code})"
                )
                return True
            if r.status_code == 404:
                logger.debug(
                    f"No existing datastore '{workspace}:{store_name}' to delete"
                )
                return False
            logger.warning(
                f"Unexpected status {r.status_code} deleting datastore "
                f"'{workspace}:{store_name}': {r.content}"
            )
            return False
        except Exception as e:
            logger.warning(
                f"Failed to delete datastore '{workspace}:{store_name}': {e}"
            )
            return False

    def create_datastore(
        self,
        data: bytes,
        store_name: str,
        workspace: str,
        file_extension: str = _DEFAULT_FORMAT,
    ) -> str:
        try:
            content_type = _CONTENT_TYPE_BY_EXTENSION.get(
                file_extension, "application/octet-stream"
            )
            headers = {"Content-type": content_type, "Accept": "application/xml"}
            url = (
                f"{self.service_url}/rest/workspaces/{workspace}/datastores/"
                f"{store_name}/file.{file_extension}"
                f"?filename={store_name}&update=overwrite"
            )
            logger.debug(
                f"Uploading datastore: store='{store_name}', workspace='{workspace}', "
                f"extension='{file_extension}', size={len(data)} bytes, "
                f"content_type='{content_type}'"
            )

            if not self.test_connection():
                raise Exception("Failed to connect to GeoServer")

            r = requests.put(
                url,
                data=data,
                auth=(self.username, self.password),
                headers=headers,
                verify=True,
            )
            logger.debug(f"Create datastore response: {r.status_code}")
            logger.debug(f"Response content: {r.content}")

            if r.status_code in (200, 201, 202):
                return (
                    f"GeoServer datastore '{store_name}' created/updated "
                    f"(extension={file_extension})"
                )
            raise Exception(f"GeoServer Error: {r.status_code}, {r.content}")
        except Exception as e:
            logger.error(f"Error in create_datastore: {str(e)}")
            raise


def build_layer(
    layer_type: str,
    item_type: str,
    plan_id,
    district: str,
    block: str,
    csv_path: str,
    file_extension: str = _DEFAULT_FORMAT,
) -> bool:
    try:
        driver = _DRIVER_BY_EXTENSION.get(file_extension)
        if driver is None:
            raise ValueError(
                f"Unsupported file_extension '{file_extension}'. "
                f"Expected one of {sorted(_DRIVER_BY_EXTENSION)}."
            )

        logger.info(
            f"build_layer: starting — layer_type={layer_type}, item_type={item_type}, "
            f"plan_id={plan_id}, district={district}, block={block}, "
            f"format={file_extension}/{driver}"
        )
        logger.debug(f"build_layer: csv_path={csv_path}, cwd={os.getcwd()}")

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        df_geom = pd.read_csv(csv_path)
        logger.info(
            f"build_layer: loaded CSV with {len(df_geom)} row(s) "
            f"and {len(df_geom.columns)} column(s)"
        )

        missing_columns = [c for c in ("longitude", "latitude") if c not in df_geom.columns]
        if missing_columns:
            raise ValueError(
                f"No record carried GPS coordinates (missing columns: {missing_columns}); "
                f"check the source form's GPS_point field"
            )

        before = len(df_geom)
        df_geom = df_geom.dropna(subset=["longitude", "latitude"]).reset_index(drop=True)
        dropped = before - len(df_geom)
        if dropped:
            logger.warning(
                f"build_layer: dropped {dropped}/{before} row(s) without GPS coordinates"
            )
        if df_geom.empty:
            raise ValueError("No rows with valid GPS coordinates after filtering")

        geometry = [Point(xy) for xy in zip(df_geom["longitude"], df_geom["latitude"])]
        gdf = gpd.GeoDataFrame(df_geom, geometry=geometry, crs="EPSG:4326")

        formatted_block = transform_name(name=block)
        store_layer_name = f"{item_type}_{plan_id}_{district}_{formatted_block}"
        logger.info(f"build_layer: store/layer name='{store_layer_name}'")

        with tempfile.TemporaryDirectory(prefix="geoserver_") as tmpdirname:
            os.chmod(tmpdirname, 0o777)
            payload_bytes = _write_layer_payload(
                gdf, tmpdirname, store_layer_name, file_extension, driver
            )
            logger.info(
                f"build_layer: payload prepared ({len(payload_bytes)} bytes, "
                f"{len(gdf.columns)} attribute(s) including geometry)"
            )

            push_result = push_layer_to_geoserver(
                payload_bytes,
                store_layer_name,
                workspace=layer_type,
                file_extension=file_extension,
            )
            logger.info(f"build_layer: geoserver push result — {push_result}")

        return True
    except Exception as e:
        logger.error(f"build_layer: exception — {str(e)}")
        logger.error(traceback.format_exc())
        return False


def _write_layer_payload(
    gdf: gpd.GeoDataFrame,
    tmpdirname: str,
    store_layer_name: str,
    file_extension: str,
    driver: str,
) -> bytes:
    """
    Serialise `gdf` to disk in the requested format and return the wire payload
    GeoServer expects. GeoPackage uploads as a single file; Shapefile uploads
    as a zipped bundle of the .shp + sidecars.
    """
    if file_extension == "gpkg":
        gpkg_path = os.path.join(tmpdirname, f"{store_layer_name}.gpkg")
        gdf.to_file(gpkg_path, driver=driver, layer=store_layer_name)
        logger.debug(f"build_layer: wrote GeoPackage at {gpkg_path}")
        with open(gpkg_path, "rb") as fp:
            return fp.read()

    import zipfile

    shapefile_path = os.path.join(tmpdirname, f"{store_layer_name}.shp")
    gdf.to_file(shapefile_path, driver=driver)
    logger.debug(f"build_layer: wrote Shapefile bundle at {shapefile_path}")
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename in os.listdir(tmpdirname):
            with open(os.path.join(tmpdirname, filename), "rb") as src:
                zf.writestr(filename, src.read())
    return buffer.getvalue()


def push_layer_to_geoserver(
    data: bytes,
    store_layer_name: str,
    workspace: str = "test_workspace",
    file_extension: str = _DEFAULT_FORMAT,
) -> str:
    try:
        logger.debug(
            f"push_layer_to_geoserver: store='{store_layer_name}', "
            f"workspace='{workspace}', extension='{file_extension}', size={len(data)} bytes"
        )
        geo = Geoserver_BB()
        deleted = geo.delete_datastore_if_exists(workspace, store_layer_name)
        if deleted:
            logger.info(
                f"push_layer_to_geoserver: cleared stale datastore "
                f"'{workspace}:{store_layer_name}' before re-publishing"
            )
        return geo.create_datastore(
            data=data,
            store_name=store_layer_name,
            workspace=workspace,
            file_extension=file_extension,
        )
    except Exception as e:
        logger.error(f"push_layer_to_geoserver: error — {str(e)}")
        raise
