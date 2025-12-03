import ee
import os
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
)
from rest_framework.response import Response
from rest_framework import status
from nrm_app.settings import EXCEL_PATH
import json
import requests
import pandas as pd
import numpy as np
from stats_generator.mws_indicators import generate_mws_data_for_kyl_filters
from geoadmin.models import StateSOI, DistrictSOI, TehsilSOI

from computing.models import Layer, LayerType
from stats_generator.utils import get_url
from nrm_app.settings import GEOSERVER_URL
from nrm_app.settings import EXCEL_PATH, GEE_HELPER_ACCOUNT_ID

# Create your views here.


def is_valid_string(value):
    if not value:
        return True
    cleaned = value.replace(" ", "").replace("_", "")
    return cleaned.isalpha()


def is_valid_mws_id(value):
    if not value:
        return True
    return all(c.isdigit() or c == "_" for c in value)


def excel_file_exists(state, district, tehsil):
    base_path = os.path.join(EXCEL_PATH, "data/stats_excel_files")
    state_path = os.path.join(base_path, state.upper())
    district_path = os.path.join(state_path, district.upper())
    filename = f"{district}_{tehsil}.xlsx"
    file_path = os.path.join(district_path, filename)
    return file_path, os.path.exists(file_path)


def raster_tiff_download_url(workspace, layer_name):
    geotiff_url = f"{GEOSERVER_URL}/{workspace}/wcs?service=WCS&version=2.0.1&request=GetCoverage&CoverageId={workspace}:{layer_name}&format=geotiff&compression=LZW&tiling=true&tileheight=256&tilewidth=256"
    return geotiff_url


def fetch_generated_layer_urls(state_name, district_name, block_name):
    """
    Fetch all vector and raster layers for given state, district, and block,
    and return their metadata as JSON.
    """
    state = StateSOI.objects.get(state_name__iexact=state_name)
    district = DistrictSOI.objects.get(district_name__iexact=district_name, state=state)
    tehsil = TehsilSOI.objects.get(tehsil_name__iexact=block_name, district=district)

    layers = Layer.objects.filter(state=state, district=district, block=tehsil)

    EXCLUDE_LAYER_KEYWORDS = [
        "run off",
        "run_off",
        "evapotranspiration",
        "precipitation",
        "MWS",
    ]
    for word in EXCLUDE_LAYER_KEYWORDS:
        layers = layers.exclude(layer_name__icontains=word)

    layer_data = []

    for layer in layers:
        dataset = layer.dataset
        workspace = dataset.workspace
        layer_type = dataset.layer_type
        layer_name = layer.layer_name
        gee_asset_path = layer.gee_asset_path
        style_url = dataset.style_name

        if layer_type in [LayerType.VECTOR, LayerType.POINT]:
            layer_url = get_url(workspace, layer_name)
        elif layer_type == LayerType.RASTER:
            layer_url = raster_tiff_download_url(workspace, layer_name)
        else:
            continue  # Skip unknown types

        layer_data.append(
            {
                "layer_name": dataset.name,
                "layer_type": layer_type,
                "layer_url": layer_url,
                "layer_version": layer.layer_version,
                "style_url": "",
                "gee_asset_path": gee_asset_path,
            }
        )

    return layer_data


def get_location_info_by_lat_lon(lat, lon):
    ee_initialize()
    point = ee.Geometry.Point([lon, lat])
    feature_collection = ee.FeatureCollection(
        "projects/corestack-datasets/assets/datasets/SOI_tehsil"
    )
    try:
        intersected = feature_collection.filterBounds(point)
        collection_size = intersected.size().getInfo()
        if collection_size == 0:
            return Response(
                {"error": "Latitude and longitude is not in SOI boundary."},
                status=status.HTTP_404_NOT_FOUND,
            )
        features = intersected.toList(intersected.size())
        for i in range(intersected.size().getInfo()):
            feature = ee.Feature(features.get(i))
            feature_loc = feature.getInfo()["properties"]
            locat_details = {
                "State": feature_loc.get("STATE"),
                "District": feature_loc.get("District"),
                "Tehsil": feature_loc.get("TEHSIL"),
            }
            return locat_details
    except Exception as e:
        print("Exception while getting admin details", str(e))
        return {"State": "", "District": "", "Tehsil": ""}


def get_mws_id_by_lat_lon(lon, lat):
    data_dict = get_location_info_by_lat_lon(lat, lon)
    if hasattr(data_dict, "status_code") and data_dict.status_code != 200:
        return Response(
            {"error": "Latitude and longitude is not in SOI boundary."},
            status=status.HTTP_404_NOT_FOUND,
        )
    state = data_dict.get("State")
    district = data_dict.get("District")
    tehsil = data_dict.get("Tehsil")

    try:
        asset_path = get_gee_asset_path(state, district, tehsil)
        mws_asset_id = (
            asset_path
            + f"filtered_mws_{valid_gee_text(district.lower())}_{valid_gee_text(tehsil.lower())}_uid"
        )
        if is_gee_asset_exists(mws_asset_id):
            mws_fc = ee.FeatureCollection(mws_asset_id)
            point = ee.Geometry.Point([lon, lat])
            matching_feature = mws_fc.filterBounds(point).first()
            uid = ee.String(matching_feature.get("uid")).getInfo()
            data_dict["uid"] = uid
            return data_dict
        else:
            return Response(
                {"error": "Mws Layer is not generated for the given lat lon location."},
                status=status.HTTP_404_NOT_FOUND,
            )
    except Exception as e:
        print("Exception while getting mws_id using lat lon", str(e))
        return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


def get_mws_time_series_data(state, district, tehsil, mws_id):
    base_url = "https://geoserver.core-stack.org:8443/geoserver/mws_layers/ows"

    params = {
        "service": "WFS",
        "version": "1.0.0",
        "request": "GetFeature",
        "typeName": f"mws_layers:deltaG_fortnight_{district}_{tehsil}",
        "outputFormat": "application/json",
        "CQL_FILTER": f"uid='{mws_id}'",
    }

    try:
        response = requests.get(base_url, params=params, verify=True, timeout=10)
        response.raise_for_status()
        geojson = response.json()

        if not geojson["features"]:
            return {"error": f"MWS ID {mws_id} not found"}

        properties = geojson["features"][0]["properties"]

        # Helper function to round values
        def roundoff_value(value):
            return round(value, 2) if value is not None else None

        # Build time series
        time_series = []
        for date, data in sorted(properties.items()):

            try:
                values = json.loads(data)
                time_series.append(
                    {
                        "date": date,
                        "et": roundoff_value(values.get("ET")),
                        "runoff": roundoff_value(values.get("RunOff")),
                        "precipitation": roundoff_value(values.get("Precipitation")),
                    }
                )
            except (json.JSONDecodeError, TypeError):
                continue

        return {"mws_id": mws_id, "time_series": time_series}

    except Exception as e:
        return {"Error in get mws data": str(e)}


def get_mws_json_from_kyl_indicator(state, district, tehsil, mws_id):
    state_folder = state.replace(" ", "_").upper()
    district_folder = district.replace(" ", "_").upper()
    file_xl_path = (
        EXCEL_PATH
        + "data/stats_excel_files/"
        + state_folder
        + "/"
        + district_folder
        + "/"
        + district
        + "_"
        + tehsil
    )
    json_file = file_xl_path + "_KYL_filter_data.json"

    try:
        with open(json_file, "r") as f:
            data = json.load(f)

        df = pd.DataFrame(data)
        df.columns = [col.strip().lower() for col in df.columns]

        if "mws_id" not in df.columns:
            return {"error": "'mws_id' column not found in JSON file."}

        filtered_df = df[df["mws_id"] == mws_id]
        filtered_df = filtered_df.replace([np.inf, -np.inf], np.nan)

        json_compatible = json.loads(
            filtered_df.to_json(orient="records", default_handler=str)
        )

        return json_compatible

    except Exception as e:
        return {"error": f"Error reading or processing file: {str(e)}"}


def get_tehsil_json(state, district, tehsil):
    file_path, file_exists = excel_file_exists(state, district, tehsil)
    json_path = file_path.replace(".xlsx", ".json")

    if os.path.exists(json_path):
        with open(json_path, "r") as f:
            return json.load(f)

    xls = pd.read_excel(file_path, sheet_name=None)
    json_data = {}

    for sheet_name, df in xls.items():
        df.columns = [col.strip().lower() for col in df.columns]
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.where(pd.notnull(df), None)
        json_data[sheet_name] = df.to_dict(orient="records")

    # Save JSON file
    with open(json_path, "w") as f:
        json.dump(json_data, f)
    return json_data


def generate_mws_report_url(state, district, tehsil, mws_id, base_url):
    ee_initialize(GEE_HELPER_ACCOUNT_ID)
    asset_path = get_gee_asset_path(state, district, tehsil)
    mws_asset_id = asset_path + f"filtered_mws_{district}_{tehsil}_uid"

    if not is_gee_asset_exists(mws_asset_id):
        return None, Response(
            {"error": "Mws Layer not found for the given location."},
            status=status.HTTP_404_NOT_FOUND,
        )

    # Filter feature collection by MWS ID
    mws_fc = ee.FeatureCollection(mws_asset_id)
    matching_feature = mws_fc.filter(ee.Filter.eq("uid", mws_id)).first()

    if matching_feature is None or matching_feature.getInfo() is None:
        return None, Response(
            {"error": "Data not found for the given mws_id"},
            status=status.HTTP_404_NOT_FOUND,
        )

    # Check if Excel file exists
    if not excel_file_exists(state, district, tehsil):
        return None, Response(
            {"Message": "Data not found for this state, district, tehsil."},
            status=status.HTTP_404_NOT_FOUND,
        )

    # Generate report URL
    report_url = f"{base_url}/api/v1/generate_mws_report/?state={state}&district={district}&block={tehsil}&uid={mws_id}"

    return {"Mws_report_url": report_url}, None
