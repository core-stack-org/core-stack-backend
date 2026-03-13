from django.shortcuts import render
from rest_framework import viewsets, permissions, serializers, status
from rest_framework.decorators import action
from rest_framework.response import Response
import requests
from requests.auth import HTTPBasicAuth
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods


def search_layer_by_coordinates(request):
    """
    Search which layer a specific coordinate falls into

    Expected Query Parameters:
    - lat: Latitude
    - lon: Longitude
    - workspace: GeoServer workspace name
    """
    # Get parameters from request
    latitude = request.GET.get("lat")
    longitude = request.GET.get("lon")
    workspace = request.GET.get("workspace", "your_default_workspace")

    # Validate input
    if not all([latitude, longitude, workspace]):
        return JsonResponse(
            {
                "error": "Missing required parameters",
                "required_params": ["lat", "lon", "workspace"],
            },
            status=400,
        )

    try:
        # Convert to float for validation
        lat = float(latitude)
        lon = float(longitude)
    except ValueError:
        return JsonResponse(
            {
                "error": "Invalid coordinate format",
                "message": "Coordinates must be numeric",
            },
            status=400,
        )

    # GeoServer connection details
    geoserver_url = "http://geoserver.core-stack.org:8443/geoserver"
    username = "admin"
    password = "tbiuBlock1@123"

    # Layers in the workspace
    layers_url = f"{geoserver_url}/rest/workspaces/{workspace}/layers"

    try:
        # Fetch layers in the workspace
        layers_response = requests.get(
            layers_url,
            auth=HTTPBasicAuth(username, password),
            headers={"Accept": "application/json"},
        )
        layers_response.raise_for_status()
        layers_data = layers_response.json()

        # Intersecting layers
        intersecting_layers = []

        # Check each layer for intersection
        for layer in layers_data["layers"]["layer"]:
            layer_name = layer["name"]

            # Construct WFS GetFeature request
            wfs_url = (
                f"{geoserver_url}/wfs?"
                f"service=WFS&version=1.1.0&request=GetFeature"
                f"&typeName={workspace}:{layer_name}"
                f"&CQL_FILTER=INTERSECTS(geometry,POINT({lon} {lat}))"
            )

            # Send WFS request
            wfs_response = requests.get(wfs_url, auth=HTTPBasicAuth(username, password))

            # Check if layer intersects with point
            if len(wfs_response.content) > 50:  # Basic check for non-empty response
                intersecting_layers.append(layer_name)

        return JsonResponse(
            {
                "coordinates": {"latitude": lat, "longitude": lon},
                "workspace": workspace,
                "intersecting_layers": intersecting_layers,
            }
        )

    except requests.exceptions.RequestException as e:
        return JsonResponse(
            {"error": "GeoServer connection error", "details": str(e)}, status=500
        )


# URL Configuration
# In urls.py
# path('search-layer/', views.search_layer_by_coordinates, name='search_layer')
