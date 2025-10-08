from rest_framework.decorators import api_view
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
)
from utilities.auth_check_decorator import api_security_check
from django.http import HttpResponse
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from nrm_app.settings import GEOSERVER_URL, EXCEL_PATH, GEE_HELPER_ACCOUNT_ID

# Common parameters that can be reused across endpoints
latitude_param = openapi.Parameter(
    "latitude",
    openapi.IN_QUERY,
    description="Latitude coordinate (-90 to 90)",
    type=openapi.TYPE_NUMBER,
    required=True,
)
longitude_param = openapi.Parameter(
    "longitude",
    openapi.IN_QUERY,
    description="Longitude coordinate (-180 to 180)",
    type=openapi.TYPE_NUMBER,
    required=True,
)
authorization_param = openapi.Parameter(
    "X-API-Key",
    openapi.IN_HEADER,
    description="API Key in format: <your-api-key>",
    type=openapi.TYPE_STRING,
    required=True,
)
state_param = openapi.Parameter(
    "state",
    openapi.IN_QUERY,
    description="Name of the state (e.g. 'Uttar Pradesh')",
    type=openapi.TYPE_STRING,
    required=True,
)
district_param = openapi.Parameter(
    "district",
    openapi.IN_QUERY,
    description="Name of the district (e.g. 'Jaunpur')",
    type=openapi.TYPE_STRING,
    required=True,
)
tehsil_param = openapi.Parameter(
    "tehsil",
    openapi.IN_QUERY,
    description="Name of the tehsil (e.g. 'Badlapur')",
    type=openapi.TYPE_STRING,
    required=True,
)
mws_id_param = openapi.Parameter(
    "mws_id",
    openapi.IN_QUERY,
    description="Unique MWS identifier (e.g. '12_234647')",
    type=openapi.TYPE_STRING,
    required=True,
)
file_type_param = openapi.Parameter(
    "file_type",
    openapi.IN_QUERY,
    description="Output format - 'json' or 'excel' (default: 'excel')",
    type=openapi.TYPE_STRING,
    required=False,
)


########## Admin Details by lat lon ##########
response_param = openapi.Parameter(
    "X-API-Key",
    openapi.IN_HEADER,
    description="API Key in format: <your-api-key>",
    type=openapi.TYPE_STRING,
    required=True,
)


@swagger_auto_schema(
    method='get',
    operation_id='get_admin_details_by_latlon',
    operation_summary="Get Admin Details by Lat Lon", 
    operation_description="""
    Retrieve admin data based on given latitude and longitude coordinates.
    
    **Response dataset details:**
    ```
        [
            "State": "State name",
            "District": "District name",
            "Tehsil": "Tehsil name"
        ]
    ```
    """,
    manual_parameters=[latitude_param, longitude_param, authorization_param],
    responses={
        200: openapi.Response(
            description="Success - It will return JSON data having admin details.",
            examples={
                "application/json": {
                    "State": "UTTAR PRADESH",
                    "District": "JAUNPUR",
                    "Tehsil": "BADLAPUR"
                }
            }
        ),
        400: openapi.Response(description="Bad Request - Both 'latitude' and 'longitude' parameters are required. OR Latitude and longitude must be valid numbers(float)."),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(description="Not Found - Latitude and longitude is not in SOI boundary."),
        500: openapi.Response(description="Internal Server Error")
    },
    tags=['Dataset APIs']
)
@api_security_check(auth_type="Api_key")
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
@swagger_auto_schema(
    method='get',
    operation_id='get_mwsid_by_latlon',
    operation_summary="Get MWSID by Lat Lon",
    operation_description="""
    Retrieve MWS ID data based on given latitude and longitude coordinates.
    
    **Response dataset details:**
    ```
        [
            "uid": "MWS_id"
            "State": "State name",
            "District": "District name",
            "Tehsil": "Tehsil name"
        ]
    ```
    """,
    manual_parameters=[latitude_param, longitude_param, authorization_param],
    responses={
        200: openapi.Response(
            description="Success - It will return JSON data having admin detail with mws_id.",
            examples={
                "application/json": {
                    "uid": "12_234647",
                    "state": "UTTAR PRADESH",
                    "district": "JAUNPUR",
                    "tehsil": "BADLAPUR"
                }
            }
        ),
        400: openapi.Response(description="Bad Request - Both 'latitude' and 'longitude' parameters are required. OR Latitude and longitude must be valid numbers(float)."),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(description="Not Found - Latitude and longitude is not in SOI boundary. OR Mws Layer is not generated for the given lat lon location."),
        500: openapi.Response(description="Internal Server Error")
    },
    tags=['Dataset APIs']
)
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
@swagger_auto_schema(
    method='get',
    operation_id='get_mws_data',
    operation_summary="Get MWS Data", 
    operation_description="""
    Retrieve MWS data for a given state, district, tehsil, and MWS ID.
    
    **Response dataset details:**
    ```
        [
            {
                    "hydrological_annual": [
                        {
                            "uid": "MWS_id",
                            "et_in_mm_2017-2018": "Evapotranspiration for year in mm",
                            "runoff_in_mm_2017-2018": "Runoff for year in mm",
                            "g_in_mm_2017-2018": "Groundwater for year in mm",
                            "deltag_in_mm_2017-2018": "Change in groundwater for year in mm",
                            "precipitation_in_mm_2017-2018": "Precipitation for year in mm",
                            "welldepth_in_m_2017-2018": "Well Depth for year in m"
                        }
                    ]
                }
        ]
    ```
    """,
    manual_parameters=[state_param, district_param, tehsil_param, mws_id_param, authorization_param],
    responses={
        200: openapi.Response(
            description="Success - It will return JSON data for the mws_id.",
            examples={
                "application/json": {
                    "hydrological_annual": [
                        {
                            "uid": "12_234647",
                            "et_in_mm_2017-2018": 894.1,
                            "runoff_in_mm_2017-2018": 148.57,
                            "g_in_mm_2017-2018": -321.06,
                            "deltag_in_mm_2017-2018": -321.06,
                            "precipitation_in_mm_2017-2018": 721.62,
                            "welldepth_in_m_2017-2018": -1.78
                        }
                    ],
                    "terrain": [
                        {
                            "uid": "12_234647",
                            "area_in_ha": 4317.15,
                            "terrain_cluster_id": 1,
                            "terrain_description": "Mostly Plains",
                            "hill_slope_area_percent": 0.02,
                            "plain_area_percent": 95.75,
                            "ridge_area_percent": 2.17,
                            "slopy_area_percent": 1.1,
                            "valley_area_percent": 0.96
                        }
                    ]
                }
            }
        ),
        400: openapi.Response(description="Bad Request - 'state', 'district', 'tehsil', and 'mws_id' parameters are required. OR State/District/Tehsil must contain only letters, spaces, and underscores. OR MWS id can only contain numbers and underscores."),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(description="Not Found - Data not found for this state, district, tehsil. OR Data not found for the given mws_id."),
        500: openapi.Response(description="Internal Server Error")
    },
    tags=['Dataset APIs']
)
@api_security_check(auth_type="API_key")
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
@swagger_auto_schema(
    method='get',
    operation_id='get_tehsil_data',
    operation_summary="Get Tehsil Data", 
    operation_description="""
    Retrieve tehsil-level JSON data for a given state, district, and tehsil.
    
    **Response dataset details:**
    ```
        [
           "aquifer_vector": [
                {
                    "uid": "MWS_id",
                    "area_in_ha": "Area for the mws",
                    "aquifer_class": "Class for the aquifer",
                    "principle_aq_alluvium_percent": "Total percentage area under aquifer class",
                    "principle_aq_banded gneissic complex_percent": "Total percentage area under aquifer class"
                }
              ]  
        ]
    ```
    """,
    manual_parameters=[state_param, district_param, tehsil_param, authorization_param],
    responses={
        200: openapi.Response(
            description="Success - It will return JSON data for the tehsil.",
            examples={
                "application/json": {
                    "aquifer_vector": [
                        {
                            "uid": "12_207597",
                            "area_in_ha": 2336.11,
                            "aquifer_class": "Alluvium",
                            "principle_aq_alluvium_percent": 100,
                            "principle_aq_banded gneissic complex_percent": 0
                        },
                        {
                            "uid": "12_208413",
                            "area_in_ha": 864.04,
                            "aquifer_class": "Alluvium",
                            "principle_aq_alluvium_percent": 100,
                            "principle_aq_banded gneissic complex_percent": 0
                        }
                    ],
                "Soge_vector": [
                    "..............."
                ]
                }
            }
        ),
        400: openapi.Response(description="Bad Request - 'state', 'district', and 'tehsil' are required. OR State/District/Tehsil must contain only letters, spaces, and underscores"),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(description="Not Found - Data not found for this state, district, tehsil."),
        500: openapi.Response(description="Internal Server Error")
    },
    tags=['Dataset APIs']
)
@api_security_check(auth_type="API_key")
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
            print("Excel file does not exist. Generating...")
            return Response(
                {"Message": "Data not found for this state, district, tehsil"},
                status=status.HTTP_404_NOT_FOUND,
            )

        xls = pd.read_excel(file_path, sheet_name=None)
        json_data = {}
        for sheet_name, df in xls.items():
            df.columns = [col.strip().lower() for col in df.columns]
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df = df.where(pd.notnull(df), None)
            json_data[sheet_name] = df.to_dict(orient="records")
        return JsonResponse(json_data, status=200)

        # Otherwise, return Excel file
        with open(file_path, "rb") as file:
            response = HttpResponse(
                file.read(),
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            response["Content-Disposition"] = f"attachment; filename={filename}"
            return response

    except Exception as e:
        print(f"Error generating Excel file: {str(e)}")
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


########### Get KYL Data based on MWS ID  ###############
@swagger_auto_schema(
    method='get',
    operation_id='get_mws_kyl_indicators',
    operation_summary="Get MWS KYL Indicators",  
    operation_description="""
    Retrieve KYL indicator data for a specific MWS ID in a given state, district, and tehsil.
    
    **Example Response:**
    ```
        [
            {
                "mws_id": "MWS id",
                "terraincluster_id": "Cluster id",
                "avg_precipitation": "Average precipitation in mm",
                "cropping_intensity_trend": "Cropping intensity trend value",
                "cropping_intensity_avg": "Average cropping Intensity",
                "avg_single_cropped": "Average Single cropped area",
                "avg_double_cropped": "Average Double cropped area",
                "avg_triple_cropped": "Average Triple cropped area",
                ".................": ".................",
                "avg_number_dry_spell": "Average number of dry spell",
                "avg_runoff": "Average runoff",
                "total_nrega_assets": "Total nrega assets"
            }
        ]
    ```
    """,
    manual_parameters=[state_param, district_param, tehsil_param, mws_id_param, authorization_param],
    responses={
        200: openapi.Response(
            description="Success - It will return JSON data of the KYL Indicator for the mws_id.",
            examples={
                "application/json": [
                    {
                        "mws_id": "12_234647",
                        "terraincluster_id": 1,
                        "avg_precipitation": 764.4457,
                        "cropping_intensity_trend": 0,
                        "cropping_intensity_avg": 1.7417,
                        "avg_single_cropped": 8.2647,
                        "avg_double_cropped": 80.1709,
                        "avg_triple_cropped": 1.8198,
                        "..................": ".......",
                        "avg_number_dry_spell": 2.1667,
                        "avg_runoff": 167.7886,
                        "total_nrega_assets": 550
                    }
                ]
            }
        ),
        400: openapi.Response(description="Bad Request - 'state', 'district', 'tehsil', and 'mws_id' parameters are required. OR State/District/Tehsil must contain only letters, spaces, and underscores OR MWS id can only contain numbers and underscores"),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(description="Not Found - Data not found for this state, district, tehsil. OR Not Found - Data not found for the given mws_id."),
        500: openapi.Response(description="Internal Server Error")
    },
    tags=['Dataset APIs']
)
@api_security_check(auth_type="API_key")
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
@swagger_auto_schema(
    method='get',
    operation_id='get_generated_layer_urls',
    operation_summary="Get Generated Layer Url",  
    operation_description="""
    Retrieve generated layer URLs for a given state, district, and tehsil.
    
    **Example Response:**
    ```
        [
                "layer_name": "Name of the layer",
                "layer_type": "Vector/ Raster",
                "layer_url": "Geoserver url for the layer",
                "layer_version": "Version of the layer",
                "style_url": "Url for the style",
                "gee_asset_path": "GEE Asset path for the layer"
        ]
    ```
    """,
    manual_parameters=[state_param, district_param, tehsil_param, authorization_param],
    responses={
        200: openapi.Response(
            description="Success - It will return JSON data for the generated layers.",
            examples={
                "application/json": [
                {
                        "layer_name": "SOGE",
                        "layer_type": "vector",
                        "layer_url": "https://geoserver.core-stack.org:8443/geoserver/soge/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=soge:soge_vector_nalanda_hilsa&outputFormat=application/json",
                        "layer_version": "1.0",
                        "style_url": "https://github.com/core-stack-org/QGIS-Styles/blob/main/Hydrology/SOGE_style.qml",
                        "gee_asset_path": "projects/ee-corestackdev/assets/apps/mws/bihar/nalanda/hilsa/soge_vector_nalanda_hilsa"
                    },
                    {
                        "layer_name": "Drainage",
                        "layer_type": "vector",
                        "layer_url": "https://geoserver.core-stack.org:8443/geoserver/drainage/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=drainage:nalanda_hilsa&outputFormat=application/json",
                        "layer_version": "1.0",
                        "style_url": "https://github.com/core-stack-org/QGIS-Styles/blob/main/Hydrology/Drainage-Layer-Style.qml",
                        "gee_asset_path": "projects/ee-corestackdev/assets/apps/mws/bihar/nalanda/hilsa/drainage_lines_nalanda_hilsa"
                    }
                ]
            }
        ),
        400: openapi.Response(description="Bad Request - 'state', 'district', and 'tehsil' parameters are required. OR State/District/Tehsil must contain only letters, spaces, and underscores"),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(description="Not Found - Data not found for this state, district, tehsil."),
        500: openapi.Response(description="Internal Server Error")
    },
    tags=['Dataset APIs']
)
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
@swagger_auto_schema(
    method='get',
    operation_id='get_mws_report',
    operation_summary="Get MWS Report url", 
    operation_description="""
    Retrieve MWS report url for a given state, district, tehsil and mws_id.
    
    **Response dataset details:**
    ```
        [
            "Mws_report_url": "Url for the MWS report"
        ]
    ```
    """,
    manual_parameters=[state_param, district_param, tehsil_param, mws_id_param, authorization_param],
    responses={
        200: openapi.Response(
            description="Success - It will return JSON having mws report url.",
            examples={
                "application/json": {
                    "Mws_report_url": "http://127.0.0.1:8000/api/v1/generate_mws_report/?state=uttar_pradesh&district=bara_banki&block=fatehpur&uid=12_208104",
                }
            }
        ),
        400: openapi.Response(description="Bad Request - 'state', 'district', 'tehsil', and 'mws_id' parameters are required. OR State/District/Tehsil must contain only letters, spaces, and underscores OR MWS id can only contain numbers and underscores"),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(description="Not Found - Data not found for the given mws_id OR Data not found for this state, district, tehsil. OR Mws Layer not found for the given location."),
        500: openapi.Response(description="Internal Server Error")
    },
    tags=['Dataset APIs']
)
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
