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
    print("Geojson url", geotiff_url)
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
    print("Data dict for the lat lon", data_dict)
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


def get_mws_json_from_stats_excel(state, district, tehsil, mws_id, xlsx_file):

    sheets = {
        "hydrological_annual": -1,
        "terrain": -1,
        "croppingIntensity_annual": -1,
        "surfaceWaterBodies_annual": -1,
        "croppingDrought_kharif": -1,
        "nrega_annual": -1,
        "mws_intersect_villages": -1,
        "change_detection_degradation": -1,
        "change_detection_afforestation": -1,
        "change_detection_deforestation": -1,
        "change_detection_urbanization": -1,
        "change_detection_cropintensity": -1,
        "terrain_lulc_slope": -1,
        "terrain_lulc_plain": -1,
        "restoration_vector": -1,
        "aquifer_vector": -1,
        "soge_vector": -1,
    }

    result = {}

    try:
        xls = pd.ExcelFile(xlsx_file)
    except Exception as e:
        return {"error": f"Failed to read Excel file: {str(e)}"}

    for sheet_name in sheets:
        if sheet_name not in xls.sheet_names:
            print(sheet_name, "is not available for this location")
            continue

        try:
            df = xls.parse(sheet_name)
            df.columns = [col.strip().lower() for col in df.columns]
            if sheet_name == "nrega_annual":
                filtered_df = df[df["mws_id"] == mws_id]
            elif sheet_name == "mws_intersect_villages":
                filtered_df = df[df["mws uid"] == mws_id]
            else:
                filtered_df = df[df["uid"] == mws_id]

            if not filtered_df.empty:
                result[sheet_name] = filtered_df.to_dict(orient="records")

        except Exception as e:
            result[sheet_name] = {"error": f"Error processing sheet: {str(e)}"}

    return result


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

    print(f"Generating JSON from Excel: {file_path}")
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
