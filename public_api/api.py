from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
import pandas as pd
import numpy as np
import os

#from .utils import *
from utilities.auth_utils import auth_free
from .views import fetch_generated_layer_urls, get_mws_id_by_lat_lon, get_mws_json_from_stats_excel, get_mws_json_from_kyl_indicator, get_location_info_by_lat_lon
from utilities.auth_check_decorator import api_security_check
from django.http import HttpResponse
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from nrm_app.settings import GEOSERVER_URL, EXCEL_PATH

# Common parameters that can be reused across endpoints
latitude_param = openapi.Parameter('latitude', openapi.IN_QUERY, description="Latitude coordinate (-90 to 90)", type=openapi.TYPE_NUMBER,required=True)
longitude_param = openapi.Parameter('longitude', openapi.IN_QUERY, description="Longitude coordinate (-180 to 180)", type=openapi.TYPE_NUMBER,required=True)
authorization_param = openapi.Parameter('X-API-Key', openapi.IN_HEADER, description="API Key in format: <your-api-key>", type=openapi.TYPE_STRING,required=True)
state_param = openapi.Parameter('state',openapi.IN_QUERY,description="Name of the state (e.g. 'Uttar Pradesh')",type=openapi.TYPE_STRING,required=True)
district_param = openapi.Parameter('district',openapi.IN_QUERY,description="Name of the district (e.g. 'Jaunpur')",type=openapi.TYPE_STRING,required=True)
tehsil_param = openapi.Parameter('tehsil',openapi.IN_QUERY,description="Name of the tehsil (e.g. 'Badlapur')",type=openapi.TYPE_STRING,required=True)
mws_id_param = openapi.Parameter('mws_id',openapi.IN_QUERY,description="Unique MWS identifier (e.g. '12_234647')",type=openapi.TYPE_STRING,required=True)
file_type_param = openapi.Parameter('file_type',openapi.IN_QUERY,description="Output format - 'json' or 'excel' (default: 'excel')",type=openapi.TYPE_STRING,required=False)


########## Admin Details by lat lon ##########
@swagger_auto_schema(
    method='get',
    manual_parameters=[latitude_param, longitude_param, authorization_param],
    responses={
        200: openapi.Response(
            description="Success",
            examples={
                "application/json": {
                    "State": "UTTAR PRADESH",
                    "District": "JAUNPUR",
                    "Tehsil": "BADLAPUR"
                }
            }
        ),
        400: openapi.Response(description="Bad Request - Invalid parameters"),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        500: openapi.Response(description="Internal Server Error")
    }
)

@api_security_check(auth_type="API_key")
def get_admin_details_by_lat_lon(request):
    """
        Retrieve admin data based on given latitude and longitude coordinates.
    """
    try:
        lat = float(request.query_params.get("latitude"))
        lon = float(request.query_params.get("longitude"))
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return Response({"error": "Latitude or longitude out of bounds."}, status=400)
        properties_list = get_location_info_by_lat_lon(lat, lon)            
        return properties_list
    except Exception as e:
        print(f"error occurred as {e}")
        return Response({
            "status": "error",
            "message": str("SOI does not contain the given lat lon")
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


######### Get Mws Id by lat lon #########
@swagger_auto_schema(
    method='get',
    manual_parameters=[latitude_param, longitude_param, authorization_param],
    responses={
        200: openapi.Response(
            description="Success",
            examples={
                "application/json": {
                    "uid": "12_234647",
                    "state": "UTTAR PRADESH",
                    "district": "JAUNPUR",
                    "tehsil": "BADLAPUR"
                }
            }
        ),
        400: openapi.Response(description="Bad Request - Invalid parameters"),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        500: openapi.Response(description="Internal Server Error")
    }
)

@api_security_check(auth_type="Auth_free")
def get_mws_by_lat_lon(request):
    """
        Retrieve MWS ID data based on given latitude and longitude coordinates.
    """
    print("Inside Get mws id by lat lon layer API")
    try:
        lat = float(request.query_params.get("latitude"))
        lon = float(request.query_params.get("longitude"))

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return Response({"error": "Latitude or longitude out of bounds."}, status=400)
        data = get_mws_id_by_lat_lon(lon, lat)
        return data
    except Exception as e:
        print("Exception while getting the mws_id by lat long", str(e))
        return Response({"State": "", "District": "", "Tehsil": "", "uid": ""}, status=404)


########## Get MWS Data by MWS ID  ##########
@swagger_auto_schema(
    method='get',
    manual_parameters=[state_param, district_param, tehsil_param, mws_id_param, authorization_param],
    responses={
        200: openapi.Response(
            description="Success",
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
        400: openapi.Response(description="Bad Request - Invalid parameters"),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        500: openapi.Response(description="Internal Server Error")
    }
)

@api_security_check(auth_type="API_key")
def get_mws_json_by_stats_excel(request):
    """
        Retrieve MWS data for a given state, district, tehsil, and MWS ID.
    """
    print("Inside mws data by excel api")
    try:
        state = request.query_params.get("state").lower()
        district = request.query_params.get("district").lower()
        tehsil = request.query_params.get("tehsil").lower()
        mws_id = request.query_params.get("mws_id")
        data = get_mws_json_from_stats_excel(state, district, tehsil, mws_id)
        return JsonResponse(data, status=200)
    except Exception as e:
        print("Exception in stats mws json :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


######### Get MWS DATA by Admin Details  ##########
@swagger_auto_schema(
    method='get',
    manual_parameters=[state_param, district_param, tehsil_param, file_type_param, authorization_param],
    responses={
        200: openapi.Response(
            description="Success",
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
        400: openapi.Response(description="Bad Request - Invalid parameters"),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        500: openapi.Response(description="Internal Server Error")
    }
)

@api_security_check(auth_type="API_key")
def generate_tehsil_data(request):
    """
        Retrieve Tehsil-level Excel or JSON data for a given state, district, and tehsil.
    """
    print("Inside generating tehsil excel data")
    try:
        # Get query parameters
        state = request.query_params.get("state", "").lower().strip().replace(" ", "_")
        district = request.query_params.get("district", "").lower().strip().replace(" ", "_")
        tehsil = request.query_params.get("tehsil", "").lower().strip().replace(" ", "_")
        file_type = request.query_params.get("file_type", "excel").lower()

        # Construct file path
        base_path = os.path.join(EXCEL_PATH, 'data/stats_excel_files')
        state_path = os.path.join(base_path, state.upper())
        district_path = os.path.join(state_path, district.upper())
        filename = f"{district}_{tehsil}.xlsx"
        file_path = os.path.join(district_path, filename)

        # Generate file if not exists
        if not os.path.exists(file_path):
            print("Excel file does not exist. Generating...")
            if not get_vector_layer_geoserver(state, district, tehsil):
                raise ValueError("Failed to generate vector layer from GeoServer.")
            os.makedirs(district_path, exist_ok=True)
            file_path = download_layers_excel_file(state, district, tehsil)
            if not file_path or not os.path.exists(file_path):
                raise ValueError("Failed to download or locate generated Excel file.")

        # If JSON is requested, return parsed content
        if file_type == "json":
            xls = pd.read_excel(file_path, sheet_name=None)
            json_data = {}
            for sheet_name, df in xls.items():
                df.columns = [col.strip().lower() for col in df.columns]
                df.replace([np.inf, -np.inf], np.nan, inplace=True)
                df = df.where(pd.notnull(df), None)
                json_data[sheet_name] = df.to_dict(orient='records')
            return JsonResponse(json_data, status=200)

        # Otherwise, return Excel file
        with open(file_path, 'rb') as file:
            response = HttpResponse(
                file.read(),
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            response['Content-Disposition'] = f'attachment; filename={filename}'
            return response

    except Exception as e:
        print(f"Error generating Excel file: {str(e)}")
        return Response({
            "status": "error",
            "message": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


########### Get KYL Data based on MWS ID  ###############
@swagger_auto_schema(
    method='get',
    manual_parameters=[state_param, district_param, tehsil_param, mws_id_param, authorization_param],
    responses={
        200: openapi.Response(
            description="Success",
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
                        "avg_wsr_ratio_kharif": 0.0262,
                        "avg_wsr_ratio_rabi": 0.028,
                        "avg_wsr_ratio_zaid": 0.43,
                        "avg_kharif_surface_water_mws": 28.2033,
                        "avg_rabi_surface_water_mws": 27.7095,
                        "avg_zaid_surface_water_mws": 9.381,
                        "trend_g": -1,
                        "drought_category": 2,
                        "avg_number_dry_spell": 2.1667,
                        "avg_runoff": 167.7886,
                        "total_nrega_assets": 550
                    }
                ]
            }
        ),
        400: openapi.Response(description="Bad Request - Invalid parameters"),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        500: openapi.Response(description="Internal Server Error")
    }
)

@api_security_check(auth_type="API_key")
def get_mws_json_by_kyl_indicator(request):
    """
        Retrieve KYL indicator data for a specific MWS ID in a given state, district, and tehsil.
    """
    print("Inside Mws kyl Indicator api")
    try:
        state = request.query_params.get("state").lower()
        district = request.query_params.get("district").lower()
        tehsil = request.query_params.get("tehsil").lower()
        mws_id = request.query_params.get("mws_id").lower()
        data = get_mws_json_from_kyl_indicator(state, district, tehsil, mws_id)
        return JsonResponse(data, status=200, safe=False)
    except Exception as e:
        print("Exception in stats mws json :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


#############  Get Generated Layers Urls  ##################
@swagger_auto_schema(
    method='get',
    manual_parameters=[state_param, district_param, tehsil_param, authorization_param],
    responses={
        200: openapi.Response(
            description="Success",
            examples={
                "application/json": [
                    {
                        "layer_desc": "Change Detection Afforestation",
                        "layer_type": "raster",
                        "layer_url": "https://geoserver.core-stack.org:8443/geoserver/change_detection/wcs?...",
                        "layer_version": "v1",
                        "style_url": "https://github.com/core-stack-org/QGIS-Styles/blob/main/Restoration/Afforestation_climate_change.qml"
                    },
                    {
                        "layer_desc": "Aquifer layer data",
                        "layer_type": "vector",
                        "layer_url": "https://geoserver.core-stack.org:8443/geoserver/aquifer/ows?...",
                        "layer_version": "v1",
                        "style_url": "https://github.com/core-stack-org/QGIS-Styles/blob/main/Hydrology/Aquifer_style.qml"
                    }
                ]
            }
        ),
        400: openapi.Response(description="Bad Request - Invalid parameters"),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        500: openapi.Response(description="Internal Server Error")
    }
)

@api_security_check(auth_type="API_key")
def get_generated_layer_urls(request):
    """
        Retrieve generated layer URLs for a given state, district, and block.
    """
    try:
        print("Inside Get Generated Layer Urls API.")
        state = request.query_params.get("state", "").lower()
        district = request.query_params.get("district", "").lower().replace(" ", "_")
        tehsil = request.query_params.get("tehsil", "").lower().replace(" ", "_")

        layers_details_json = fetch_generated_layer_urls(state, district, tehsil)
        return Response(layers_details_json, status=200)

    except Exception as e:
        print(f"Error in get_generated_layer_urls: {str(e)}")
        return Response({
            "status": "error",
            "message": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)