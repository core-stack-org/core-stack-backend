from django.shortcuts import render, redirect
import requests
from dpr.utils import get_url
from nrm_app.settings import GEOSERVER_URL
from .layer_status.layer_mapping import workspace_config
import xml.etree.ElementTree as ET


def layer_status(request):
    state_census_code = request.GET.get('state_census_code')
    district_id = request.GET.get('district_id')
    block_id = request.GET.get('block_id')
    states = []
    districts = []
    blocks = []

    # Fetch states
    response_states = requests.get("https://geoserver.core-stack.org/api/v1/get_states/")
    if response_states.status_code == 200:
        state_data = response_states.json()
        states = [state for state in state_data.get("states", []) if state.get("active_status")]
        states.sort(key=lambda x: x.get("state_name", "").lower())

    # Fetch districts for the selected state
    if state_census_code:
        response_districts = requests.get(f"https://geoserver.core-stack.org/api/v1/get_districts/{state_census_code}/")
        if response_districts.status_code == 200:
            district_data = response_districts.json()
            districts = district_data.get("districts", [])
            districts.sort(key=lambda x: x.get("district_name", "").lower())

    # Fetch blocks for the selected district
    if district_id:
        response_blocks = requests.get(f"https://geoserver.core-stack.org/api/v1/get_blocks/{district_id}/")
        if response_blocks.status_code == 200:
            block_data = response_blocks.json()
            blocks = block_data.get("blocks", [])
            blocks.sort(key=lambda x: x.get("block_name", "").lower())

    # Get district and blocks names
    district_name = next((d['district_name'] for d in districts if str(d['id']) == str(district_id)), None)
    block_name = next((b['block_name'] for b in blocks if str(b['block_census_code']) == str(block_id)), None)

    # proceed when district and block selected
    all_workspace_statuses = {}
    if district_name and block_name:
        formatted_district = district_name.lower().replace(" ", "_")
        formatted_block = block_name.lower().replace(" ", "_")

        for workspace_display, config in workspace_config.items():
            workspace = config.get("name")
            suffix = config.get("suffix", "")
            prefix = config.get("prefix", "")
            layer_type = config.get("type", "")

            layer_name_parts = [prefix, formatted_district, formatted_block, suffix]
            layer_name = "_".join(part for part in layer_name_parts if part)

            if layer_type == "vector":
                server_url = get_url(GEOSERVER_URL, workspace, layer_name)
                res_server = requests.get(server_url)
                content_type = res_server.headers.get("Content-Type", "")

                if "application/json" in content_type:
                    try:
                        data = res_server.json()
                        status_code = 200 if data.get("totalFeatures") else 400
                    except ValueError:
                        print("Invalid JSON.")
                        status_code = 400
                else:
                    status_code = 400

            else:  
                raster_workspaces = ['clart', 'terrain', 'restoration', 'tree_overall_ch', 'change_detection']
                status_code = 400  

                for raster_workspace in raster_workspaces:
                    capabilities_url = (
                        f"https://geoserver.core-stack.org:8443/geoserver/{raster_workspace}/wms"
                        "?service=WMS&request=GetCapabilities"
                    )
                    response = requests.get(capabilities_url)

                    if response.status_code != 200:
                        print(f"Failed to retrieve capabilities from {raster_workspace}.")
                        continue

                    root = ET.fromstring(response.content)
                    ns = {'wms': root.tag.split("}")[0].strip("{")}
                    layers = root.findall(".//wms:Layer/wms:Name", namespaces=ns)
                    available_layers = [layer.text for layer in layers]

                    if layer_name in available_layers:
                        status_code = 200
                        break  

            all_workspace_statuses[workspace_display] = {
                "workspace": workspace,
                "layer_name": layer_name,
                "status_code": status_code,
            }

    return render(request, 'layer-status.html', {
        "states": states,
        "districts": districts,
        "blocks": blocks,
        "selected_state": state_census_code,
        "selected_district": district_id,
        "selected_block": block_id,
        "all_workspace_statuses": all_workspace_statuses,
        "workspace_dict": workspace_config,
    })
