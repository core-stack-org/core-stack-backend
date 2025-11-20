
import requests
from dpr.utils import get_url
from nrm_app.settings import GEOSERVER_URL
from utilities.gee_utils import valid_gee_text
from .layer_status.layer_mapping import workspace_config
import xml.etree.ElementTree as ET
from nrm_app.celery import app
from computing.models import *
from utilities.geoserver_utils import Geoserver


@app.task(bind=True)
def layer_status(self, state, district, block):
    """

    Args:
        self:
        state:
        district:
        block:

    Returns: All the layers for particular location exist or not

    """
    print(f"{state=}")
    all_workspace_statuses = {}
    district = valid_gee_text(district.lower())
    block = valid_gee_text(block.lower())
    for workspace_display, config in workspace_config.items():
        workspace = config.get("name")
        suffix = config.get("suffix", "")
        prefix = config.get("prefix", "")
        layer_type = config.get("type", "")

        # constructing layer name
        if workspace_display.startswith("LULC_level_"):
            layer_name_parts = [prefix, block, suffix]
        else:
            layer_name_parts = [prefix, district, block, suffix]
        layer_name = "_".join(part for part in layer_name_parts if part)

        total_features = None
        end_date = None
        start_date = None
        # checking for vector layer
        if layer_type == "vector":
            server_url = get_url(GEOSERVER_URL, workspace, layer_name)
            res_server = requests.get(server_url)
            content_type = res_server.headers.get("Content-Type", "")

            if "application/json" in content_type and res_server.status_code == 200:
                try:
                    data = res_server.json()
                    status_code = 200 if data.get("totalFeatures") > 0 else 400
                    total_features = data.get("totalFeatures")
                    layer = (
                        Layer.objects.filter(layer_name=layer_name)
                        .order_by("-layer_version")
                        .first()
                    )
                    end_date = (
                        layer.misc.get("end_date") if layer and layer.misc else None
                    )
                    start_date = (
                        layer.misc.get("start_date") if layer and layer.misc else None
                    )
                except ValueError:
                    print("Invalid JSON.")
                    status_code = 400
            else:
                status_code = 400
        else:  # checking for raster layer
            status_code = 400
            capabilities_url = (
                f"https://geoserver.core-stack.org:8443/geoserver/{workspace}/wms"
                "?service=WMS&request=GetCapabilities"
            )
            response = requests.get(capabilities_url)
            if response.status_code != 200:
                print(f"Failed to retrieve capabilities from {workspace}.")
                continue
            root = ET.fromstring(response.content)
            ns = {"wms": root.tag.split("}")[0].strip("{")}
            layers = root.findall(".//wms:Layer/wms:Name", namespaces=ns)
            available_layers = [layer.text for layer in layers]
            if layer_name in available_layers:
                status_code = 200
        all_workspace_statuses[workspace_display] = {
            "workspace": workspace,
            "layer_name": layer_name,
            "status_code": status_code,
            "totalFeature": total_features,
            "endDate": end_date,
            "startDate": start_date,
        }

    return all_workspace_statuses


@app.task(bind=True)
def get_layers_of_workspace(self, workspace):
    """
    It will take workspace as argument and returns all the layers which is present on geoserver.
    """
    raster_workspace = ["LULC_level_1", "LULC_level_2", "LULC_level_3", "clart"]
    vector_workspace = [
        "cropping_drought",
        "drought_causality",
        "swb",
        "mws_layers",
        "crop_intensity",
        "terrain_lulc",
        "aquifer",
        "soge",
        "lulc_vector",
        "crop_grid_layers",
        "panchayat_boundaries",
        "nrega_assets",
        "drainage",
    ]
    raster_and_vector_workspace = [
        "restoration",
        "stream_order",
        "change_detection",
        "terrain",
        "tree_overall_ch",
        "canopy_height",
        "ccd",
    ]
    geo = Geoserver()
    layers = geo.get_layers(workspace)
    layer_names = [layer["name"] for layer in layers["layers"]["layer"]]
    if workspace in raster_workspace:
        print("you passed raster workspace")
        available_layers = valid_raster_layers_for_workspace(workspace)
        valid_layers = [ln for ln in layer_names if ln in available_layers]
        invalid_layers = [ln for ln in layer_names if ln not in available_layers]
        return {"valid_layer": valid_layers, "invalid_layers": invalid_layers}
    elif workspace in vector_workspace:
        print("you passed vector workspace")
        valid_layers = []
        invalid_layers = []
        for layer_name in layer_names:
            if is_valid_vector_layer(workspace, layer_name):
                valid_layers.append(layer_name)
            else:
                invalid_layers.append(layer_name)
        return {"valid_layer": valid_layers, "invalid_layers": invalid_layers}
    elif workspace in raster_and_vector_workspace:
        print("you passed workspace which contain both layers(raster and vector)")
        valid_layers = []
        invalid_layers = []
        for layer_name in layer_names:
            if "vector" in layer_name.lower():
                if is_valid_vector_layer(workspace, layer_name):
                    valid_layers.append(layer_name)
                else:
                    invalid_layers.append(layer_name)
            elif "raster" in layer_name.lower():
                available_layers = valid_raster_layers_for_workspace(workspace)
                if layer_name in available_layers:
                    valid_layers.append(layer_name)
                else:
                    invalid_layers.append(layer_name)
        return {"valid_layer": valid_layers, "invalid_layers": invalid_layers}
    else:
        print("you passed wrong workspace")
    return []


def valid_raster_layers_for_workspace(workspace):
    """

    Args:
        workspace:

    Returns:
        all valid (have data)layers for particular workspace
    """
    capabilities_url = (
        f"https://geoserver.core-stack.org:8443/geoserver/{workspace}/wms"
        "?service=WMS&request=GetCapabilities"
    )
    response = requests.get(capabilities_url)
    if response.status_code != 200:
        print(f"Failed to retrieve capabilities from {workspace}.")
        return []
    root = ET.fromstring(response.content)
    ns = {"wms": root.tag.split("}")[0].strip("{")}
    layers = root.findall(".//wms:Layer/wms:Name", namespaces=ns)
    available_layers = [layer.text for layer in layers]
    return available_layers


def is_valid_vector_layer(workspace, layer_name):
    """

    Args:
        workspace:
        layer_name:

    Returns:
        True if layer have data else False
    """
    server_url = get_url(GEOSERVER_URL, workspace, layer_name)
    res_server = requests.get(server_url)
    content_type = res_server.headers.get("Content-Type", "")

    if "application/json" in content_type and res_server.status_code == 200:
        data = res_server.json()
        return data.get("totalFeatures", 0) > 0

    return False
