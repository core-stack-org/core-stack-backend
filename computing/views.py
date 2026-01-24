import requests
from nrm_app.settings import GEOSERVER_URL
from utilities.gee_utils import valid_gee_text
import xml.etree.ElementTree as ET
from nrm_app.celery import app
from computing.models import *
from utilities.geoserver_utils import Geoserver
import json



def get_url(geoserver_url, workspace, layer_name):
    return (
        f"{geoserver_url}/{workspace}/ows"
        f"?service=WFS"
        f"&version=1.1.0"
        f"&request=GetFeature"
        f"&typeName={workspace}:{layer_name}"
        f"&resultType=hits"
    )


def load_workspace_config():
    """
    Load workspace configuration from JSON file.
    """
    config_path = "data/layers/layer_status/layer_mapping.json"
    with open(config_path, "r") as f:
        return json.load(f)


@app.task(bind=True)
def layer_status(self, state, district, block):
    """
    Check the status of all layers for a particular location.

    Args:
        self: Instance reference
        state: State name
        district: District name
        block: Block name

    Returns:
        Dictionary with status of all workspace layers
    """
    print(f"{state=}")
    all_workspace_statuses = {}
    district = valid_gee_text(district.lower())
    block = valid_gee_text(block.lower())

    # Load workspace configuration from JSON
    workspace_config = load_workspace_config()

    for workspace_display, config in workspace_config.items():
        workspace = config.get("name")
        suffix = config.get("suffix", "")
        prefix = config.get("prefix", "")
        layer_type = config.get("type", "")

        # constructing layer name
        layer_name_parts = [prefix, district, block, suffix]
        layer_name = "_".join(part for part in layer_name_parts if part)

        total_features = 0
        end_date = None
        start_date = None
        status_code = 400
        # checking for vector layer
        if layer_type == "vector":
            layer_url = get_url(GEOSERVER_URL, workspace, layer_name)
            res_layer_url = requests.get(layer_url)

            if res_layer_url.status_code == 200:
                try:
                    root = ET.fromstring(res_layer_url.text)
                    # Extract feature count from WFS hits response
                    total_features = int(root.attrib.get("numberOfFeatures", 0))
                    status_code = 200 if total_features > 0 else 400
                    layer = (
                        Layer.objects.filter(layer_name=layer_name)
                        .order_by("-layer_version")
                        .first()
                    )
                    if layer and layer.misc:
                        start_date = layer.misc.get("start_date")
                        end_date = layer.misc.get("end_date")

                except ET.ParseError:
                    print(f"Invalid XML for layer: {layer_name}")
                    status_code = 400
        else:
            capabilities_url = (
                f"https://geoserver.core-stack.org:8443/geoserver/{workspace}/wms"
                "?service=WMS&request=GetCapabilities"
            )
            response = requests.get(capabilities_url)
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                ns = {"wms": root.tag.split("}")[0].strip("{")}
                layers = root.findall(".//wms:Layer/wms:Name", namespaces=ns)
                available_layers = {layer.text for layer in layers}

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


def load_workspace_types():
    """
    Load workspace types configuration from JSON file.
    """
    config_path = "data/layers/workspace_layers/layers_in_workspace.json"
    with open(config_path, "r") as f:
        return json.load(f)


@app.task(bind=True)
def get_layers_of_workspace(self, workspace):
    """
    It will take workspace as argument and returns all the layers which is present on geoserver.
    """
    # Load workspace types from JSON
    workspace_types = load_workspace_types()
    raster_workspace = workspace_types["raster_workspace"]
    vector_workspace = workspace_types["vector_workspace"]
    raster_and_vector_workspace = workspace_types["raster_and_vector_workspace"]

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
    layer_url = get_url(GEOSERVER_URL, workspace, layer_name)
    res_layer_url = requests.get(layer_url)
    if res_layer_url.status_code == 200:
        root = ET.fromstring(res_layer_url.text)
        total_features = int(root.attrib.get("numberOfFeatures", 0))
        return True if total_features > 0 else False
