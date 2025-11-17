from rest_framework.decorators import schema
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
import pandas as pd
import numpy as np
import os

import ee
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
)

# from .utils import *
from .views import (
    fetch_generated_layer_urls,
    get_mws_id_by_lat_lon,
    get_mws_json_from_stats_excel,
    get_mws_json_from_kyl_indicator,
    get_location_info_by_lat_lon,
    is_valid_string,
    is_valid_mws_id,
    excel_file_exists,
    get_tehsil_json,
)

from utilities.auth_check_decorator import api_security_check
from drf_yasg.utils import swagger_auto_schema
from nrm_app.settings import EXCEL_PATH, GEE_HELPER_ACCOUNT_ID

from .swagger_schemas import (
    admin_by_latlon_schema,
    mws_by_latlon_schema,
    tehsil_data_schema,
    kyl_indicators_schema,
    generated_layer_urls_schema,
    mws_report_urls_schema,
)


@swagger_auto_schema(**admin_by_latlon_schema)
@api_security_check(auth_type="API_key")
def get_admin_details_by_lat_lon(request):
    """
    Retrieve admin data based on given latitude and longitude coordinates.
    """
    try:
        lat_param = request.query_params.get("latitude")
        lon_param = request.query_params.get("longitude")

        if lat_param is None or lon_param is None:
            return Response(
                {"error": "Both 'latitude' and 'longitude' parameters are required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            lat = float(lat_param)
            lon = float(lon_param)
        except (ValueError, TypeError):
            return Response(
                {"error": "Latitude and longitude must be valid numbers(float)."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # To Validate the coordinate
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return Response(
                {
                    "error": "Latitude must be between -90 and 90, longitude must be between -180 and 180."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        properties_list = get_location_info_by_lat_lon(lat, lon)
        return properties_list

    except Exception as e:
        print(f"Error occurred: {e}")
        return Response(
            {
                "status": "error",
                "message": "Unable to retrieve location data for the given coordinates",
            },
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


######### Get Mws Id by lat lon #########
@swagger_auto_schema(**mws_by_latlon_schema)
@api_security_check(auth_type="Auth_free")
def get_mws_by_lat_lon(request):
    """
    Retrieve MWS ID based on given latitude and longitude coordinates.
    """
    print("Inside Get mws id by lat lon layer API")
    try:
        lat_param = request.query_params.get("latitude")
        lon_param = request.query_params.get("longitude")

        if lat_param is None or lon_param is None:
            return Response(
                {"error": "Both 'latitude' and 'longitude' parameters are required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            lat = float(lat_param)
            lon = float(lon_param)
        except (ValueError, TypeError):
            return Response(
                {"error": "Latitude and longitude must be valid numbers(float)."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return Response(
                {
                    "error": "Latitude must be between -90 and 90, longitude must be between -180 and 180."
                },
                status=400,
            )
        data = get_mws_id_by_lat_lon(lon, lat)
        return data
    except Exception as e:
        print("Exception while getting the mws_id by lat long", str(e))
        return Response(
            {"State": "", "District": "", "Tehsil": "", "uid": ""}, status=404
        )


########## Get MWS Data by MWS ID  ##########
@api_security_check(auth_type="API_key")
@schema(None)
def get_mws_json_by_stats_excel(request):
    """
    Retrieve MWS data for a given state, district, tehsil, and MWS ID.
    """
    print("Inside mws data by excel api")
    try:
        state_param = request.query_params.get("state")
        district_param = request.query_params.get("district")
        tehsil_param = request.query_params.get("tehsil")
        mws_id = request.query_params.get("mws_id")

        if (
            state_param is None
            or district_param is None
            or tehsil_param is None
            or mws_id is None
        ):
            return Response(
                {
                    "error": "'state', 'district', 'tehsil', and 'mws_id' parameters are required."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state_param)
            or not is_valid_string(district_param)
            or not is_valid_string(tehsil_param)
        ):
            return Response(
                {
                    "error": "State/District/Tehsil must contain only letters, spaces, and underscores"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not is_valid_mws_id(mws_id):
            return Response(
                {"error": "MWS id can only contain numbers and underscores"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        state = state_param.lower().strip().replace(" ", "_")
        district = district_param.lower().strip().replace(" ", "_")
        tehsil = tehsil_param.lower().strip().replace(" ", "_")

        if not excel_file_exists(state, district, tehsil):
            return Response(
                {"Message": "Data not found for this state, district, tehsil"},
                status=status.HTTP_404_NOT_FOUND,
            )

        data = get_mws_json_from_stats_excel(state, district, tehsil, mws_id)
        if not data:
            return Response(
                {"error": "Data not found for the given mws_id"},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(data, status=200)
    except Exception as e:
        print("Exception in stats mws json :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


######### Get MWS DATA by Admin Details  ##########
@swagger_auto_schema(**tehsil_data_schema)
@api_security_check(auth_type="Auth_free")
def generate_tehsil_data(request):
    """
    Retrieve Tehsil-level JSON data for a given state, district, and tehsil.
    """
    print("Inside generating tehsil excel data")
    try:
        # Get query parameters
        state_param = request.query_params.get("state")
        district_param = request.query_params.get("district")
        tehsil_param = request.query_params.get("tehsil")

        if state_param is None or district_param is None or tehsil_param is None:
            return Response(
                {"error": "'state', 'district', and 'tehsil' parameters are required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state_param)
            or not is_valid_string(district_param)
            or not is_valid_string(tehsil_param)
        ):
            return Response(
                {
                    "error": "State/District/Tehsil must contain only letters, spaces, and underscores"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        state = state_param.lower().strip().replace(" ", "_")
        district = district_param.lower().strip().replace(" ", "_")
        tehsil = tehsil_param.lower().strip().replace(" ", "_")

        # Construct file path
        base_path = os.path.join(EXCEL_PATH, "data/stats_excel_files")
        state_path = os.path.join(base_path, state.upper())
        district_path = os.path.join(state_path, district.upper())
        filename = f"{district}_{tehsil}.xlsx"
        file_path = os.path.join(district_path, filename)

        if not os.path.exists(file_path):
            print("Excel file does not exist.")
            return Response(
                {"Message": "Data not found for this state, district, tehsil"},
                status=status.HTTP_404_NOT_FOUND,
            )

        # Get JSON (from cache or generate)
        json_data = get_tehsil_json(file_path)

        return JsonResponse(json_data, status=200)

    except Exception as e:
        print(f"Error: {str(e)}")
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


########### Get KYL Data based on MWS ID  ###############
@api_security_check(auth_type="API_key")
@schema(None)
def get_mws_json_by_kyl_indicator(request):
    """
    Retrieve KYL indicator data for a specific MWS ID in a given state, district, and tehsil.
    """
    print("Inside Mws kyl Indicator api")
    try:
        state_param = request.query_params.get("state")
        district_param = request.query_params.get("district")
        tehsil_param = request.query_params.get("tehsil")
        mws_id = request.query_params.get("mws_id")

        if (
            state_param is None
            or district_param is None
            or tehsil_param is None
            or mws_id is None
        ):
            return Response(
                {
                    "error": "'state', 'district', 'tehsil', and 'mws_id' parameters are required."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state_param)
            or not is_valid_string(district_param)
            or not is_valid_string(tehsil_param)
        ):
            return Response(
                {
                    "error": "State/District/Tehsil must contain only letters, spaces, and underscores"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not is_valid_mws_id(mws_id):
            return Response(
                {"error": "MWS id can only contain numbers and underscores"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        state = state_param.lower().strip().replace(" ", "_")
        district = district_param.lower().strip().replace(" ", "_")
        tehsil = tehsil_param.lower().strip().replace(" ", "_")

        if not excel_file_exists(state, district, tehsil):
            return Response(
                {"Message": "Data not found for this state, district, tehsil."},
                status=status.HTTP_404_NOT_FOUND,
            )

        data = get_mws_json_from_kyl_indicator(state, district, tehsil, mws_id)
        if not data:
            return Response(
                {"error": "Data not found for the given mws_id."},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(data, status=200)
    except Exception as e:
        print("Exception in stats mws json :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


#############  Get Generated Layers Urls  ##################
@swagger_auto_schema(**generated_layer_urls_schema)
@api_security_check(auth_type="API_key")
def get_generated_layer_urls(request):
    try:
        print("Inside Get Generated Layer Urls API.")
        state_param = request.query_params.get("state")
        district_param = request.query_params.get("district")
        tehsil_param = request.query_params.get("tehsil")

        if state_param is None or district_param is None or tehsil_param is None:
            return Response(
                {"error": "'state', 'district', and 'tehsil' parameters are required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state_param)
            or not is_valid_string(district_param)
            or not is_valid_string(tehsil_param)
        ):
            return Response(
                {
                    "error": "State/District/Tehsil must contain only letters, spaces, and underscores"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        layers_details_json = fetch_generated_layer_urls(
            state_param, district_param, tehsil_param
        )
        if not layers_details_json:
            return Response(
                {"error": "Data not found for this state, district, tehsil."},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(layers_details_json, status=200)

    except Exception as e:
        print(f"Error in get_generated_layer_urls: {str(e)}")
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


#############  Get MWS Report Urls  ##################
@swagger_auto_schema(**mws_report_urls_schema)
@api_security_check(auth_type="API_key")
def get_mws_report_urls(request):
    try:
        print("Inside Get Generated Layer Urls API.")
        state_param = request.query_params.get("state")
        district_param = request.query_params.get("district")
        tehsil_param = request.query_params.get("tehsil")
        mws_id = request.query_params.get("mws_id")

        if (
            state_param is None
            or district_param is None
            or tehsil_param is None
            or mws_id is None
        ):
            return Response(
                {
                    "error": "'state', 'district', 'tehsil', and 'mws_id' parameters are required."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state_param)
            or not is_valid_string(district_param)
            or not is_valid_string(tehsil_param)
        ):
            return Response(
                {
                    "error": "State/District/Tehsil must contain only letters, spaces, and underscores"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not is_valid_mws_id(mws_id):
            return Response(
                {"error": "MWS id can only contain numbers and underscores"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        state = state_param.lower().strip().replace(" ", "_")
        district = district_param.lower().strip().replace(" ", "_")
        tehsil = tehsil_param.lower().strip().replace(" ", "_")

        ee_initialize(GEE_HELPER_ACCOUNT_ID)
        asset_path = get_gee_asset_path(state, district, tehsil)
        mws_asset_id = (
            asset_path
            + f"filtered_mws_{valid_gee_text(district.lower())}_{valid_gee_text(tehsil.lower())}_uid"
        )
        if not is_gee_asset_exists(mws_asset_id):
            return Response(
                {"error": "Mws Layer not found for the given location."},
                status=status.HTTP_404_NOT_FOUND,
            )

        mws_fc = ee.FeatureCollection(mws_asset_id)
        matching_feature = mws_fc.filter(ee.Filter.eq("uid", mws_id)).first()
        if matching_feature is None or matching_feature.getInfo() is None:
            return Response(
                {"error": "Data not found for the given mws_id"},
                status=status.HTTP_404_NOT_FOUND,
            )

        if not excel_file_exists(state, district, tehsil):
            return Response(
                {"Message": "Data not found for this state, district, tehsil."},
                status=status.HTTP_404_NOT_FOUND,
            )

        base_url = request.build_absolute_uri("/")[:-1]
        report_url = f"{base_url}/api/v1/generate_mws_report/?state={state}&district={district}&block={tehsil}&uid={mws_id}"
        return Response({"Mws_report_url": report_url}, status=status.HTTP_200_OK)

    except Exception as e:
        print(f"Error in get_generated_layer_urls: {str(e)}")
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
