import requests
import geopandas as gpd
from pathlib import Path

from nrm_app import settings
from utilities.gee_utils import valid_gee_text

"""
from utilities.download_gpkg_from_geoserver import generate_gpkg
generate_gpkg(state="Odisha", district="Srikakulam", block="Bhamini", workspace="panchayat_boundaries")
"""

BASE_OUTPUT_DIR = Path(
    "/home/cfpt-jedi/developer/shiv/core-stack-backend/data/base_layers/"
)


def build_layer_and_path(state, district, block, workspace):
    """
    Create:
    - sanitized names
    - geoserver layer name
    - gpkg output path

    Example Output:
    mws_gpkg/odisha/srikakulam/bhamini.gpkg
    """

    state = valid_gee_text(state.lower())
    district = valid_gee_text(district.lower())
    block = valid_gee_text(block.lower())

    # GeoServer layer
    output_base = BASE_OUTPUT_DIR

    if workspace == "mws":
        layer_name = f"mws:mws_{district}_{block}"
        output_base = output_base / "tehsil_watersheds"
    elif workspace == "panchayat_boundaries":
        layer_name = f"panchayat_boundaries:{district}_{block}"
        output_base = output_base / "panchayat_boundaries"

    output_dir = output_base / state / district
    output_dir.mkdir(parents=True, exist_ok=True)
    gpkg_path = output_dir / f"{block}.gpkg"

    return layer_name, gpkg_path, district, block


def read_layer_from_geoserver(layer_name):
    """
    Read GeoServer WFS layer directly into GeoDataFrame.
    """

    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": layer_name,
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
    }

    geoserver_url = settings.PROD_GEOSERVER_URL.rstrip("/")
    wfs_url = f"{geoserver_url}/wfs"

    # Some workspaces might be protected, use auth if available
    auth = None
    if hasattr(settings, "GEOSERVER_USER") and hasattr(settings, "GEOSERVER_PASSWORD"):
        auth = (settings.GEOSERVER_USER, settings.GEOSERVER_PASSWORD)

    response = requests.get(
        wfs_url,
        params=params,
        auth=auth,
        timeout=120,
        verify=False,
    )

    response.raise_for_status()

    # Verify we actually got JSON and not an HTML redirect page
    if "text/html" in response.headers.get("Content-Type", ""):
        raise ValueError(
            f"GeoServer returned HTML instead of JSON. Check the WFS URL and layer name: {layer_name}"
        )

    return gpd.read_file(response.text)


def generate_gpkg(state, district, block, workspace):
    """
    Generate GPKG for each location.
    """

    try:
        layer_name, gpkg_path, district, block = build_layer_and_path(
            state, district, block, workspace
        )
        gdf = read_layer_from_geoserver(layer_name)

        if gdf.empty:
            print("Layer is empty")
            return None

        # CRS handling
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        else:
            gdf = gdf.to_crs("EPSG:4326")

        # Remove existing gpkg
        if gpkg_path.exists():
            gpkg_path.unlink()

        # Write gpkg
        gdf.to_file(
            gpkg_path,
            layer=f"{district}_{block}",
            driver="GPKG",
        )
        print(f"GPKG file created successfully : {gpkg_path}")
        return str(gpkg_path)

    except Exception as e:
        print(f"FAILED : {e}")
        return None
