from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse

from .utils import *
from .mws_indicators import get_generate_filter_mws_data, download_KYL_filter_data
from .village_indicators import get_generate_filter_data_village
from utilities.auth_utils import auth_free
from computing.misc.lat_lon_with_mws_data import get_mws_id_by_lat_lon, get_mws_json_from_stats_excel, get_mws_json_from_kyl_indicator, get_location_info_by_lat_lon
from .views import fetch_generated_layer_urls
import logging
from utilities.auth_check_decorator import api_security_check
from django.http import HttpResponse


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@api_view(["GET"])
@auth_free
def generate_excel_file_layer(request):
    try:
        state = request.query_params.get("state", "").lower().strip().replace(" ", "_")
        district = request.query_params.get("district", "").lower().strip().replace(" ", "_")
        block = request.query_params.get("block", "").lower().strip().replace(" ", "_")

        logging.info(f"Request to generate Excel for state: {state}, district: {district}, block: {block}")
        
        base_path = os.path.join(EXCEL_PATH, 'data/stats_excel_files')
        state_path = os.path.join(base_path, state.upper())
        district_path = os.path.join(state_path, district.upper())
        filename = f"{district}_{block}.xlsx"
        file_path = os.path.join(district_path, filename)

        # If file exists, return it directly
        if os.path.exists(file_path):
            logging.info(f"Excel file already exists at: {file_path}")
        else:
            logging.info("Excel file does not exist. Generating...")
            if not get_vector_layer_geoserver(state, district, block):
                raise ValueError("Failed to generate vector layer from GeoServer.")
            
            os.makedirs(district_path, exist_ok=True)
            
            excel_file_path = download_layers_excel_file(state, district, block)
            logging.info(f"Excel file generated at: {excel_file_path}")
            
            if not excel_file_path or not os.path.exists(excel_file_path):
                raise ValueError("Failed to download or locate generated Excel file.")
            
            file_path = excel_file_path  # Use the actual generated path, in case it's different

        # Serve the file
        with open(file_path, 'rb') as file:
            response = HttpResponse(
                file.read(),
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            response['Content-Disposition'] = f'attachment; filename={filename}'
            return response

    except Exception as e:
        logging.error(f"Error generating Excel file: {str(e)}")
        return Response({
            "status": "error",
            "message": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)




@api_view(["GET"])
@auth_free
def generate_kyl_data_excel(request):
    try:
        print("Inside generate_kyl_data_excel API.")
        
        state = request.query_params.get("state", "").lower().strip()
        district = request.query_params.get("district", "").lower().strip().replace(" ", "_")
        block = request.query_params.get("block", "").lower().strip().replace(" ", "_")
        file_type = request.query_params.get("file_type", "").lower().strip()
        
        # Generate data for the file
        creating_kyl_data = get_generate_filter_mws_data(state, district, block, file_type)
        print("Data generated in the file")
        excel_file = download_KYL_filter_data(state, district, block, file_type)
        logging.info(f"Download function returned: {excel_file}")
        if excel_file:
            if isinstance(excel_file, str) and os.path.exists(excel_file):
                with open(excel_file, 'rb') as file:
                    response = HttpResponse(file.read(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    response['Content-Disposition'] = f'attachment; filename={district}_{block}_KYL_filter_data.{file_type}'
                    return response
            else:
                raise ValueError("Invalid file format received from download_KYL_filter_data.")
        else:
            raise ValueError("Failed to download the KYL filter data file")
        
    except Exception as e:
        logging.error(f"Validation error: {str(e)}")
        return Response({
            "status": "error",
            "message": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@auth_free
def generate_kyl_village_data(request):
    try:
        print("Inside generate_filter_data_village API.")
        
        state = request.query_params.get("state").lower()
        district = request.query_params.get("district").lower().replace(" ", "_")
        block = request.query_params.get("block").lower().replace(" ", "_")
        village_kyl_json =  get_generate_filter_data_village(state, district, block)
        if village_kyl_json:
            if isinstance(village_kyl_json, str) and os.path.exists(village_kyl_json):
                with open(village_kyl_json, 'rb') as file:
                    response = HttpResponse(file.read(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    response['Content-Disposition'] = f'attachment; filename={district}_{block}_KYL_village_data.json'
                    return response
            else:
                raise ValueError("Invalid file format received from download_KYL_filter_data.")
        else:
            raise ValueError("Failed to download the KYL filter data file")

    except Exception as e:
        logging.error(f"Validation error: {str(e)}")
        return Response({
            "status": "error",
            "message": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_security_check(auth_type="Auth_free", allowed_methods=["GET"])
def get_admin_details_by_lat_lon(request):
    """
        **Description**:  
    This API accepts **latitude** and **longitude** as input parameters and returns the corresponding administrative details—such as **state**, **district**, and **tehsil**—in **JSON** format.

    **Response Example**:
    {
        "properties": {
            "STATE": "ODISHA",
            "District": "KALAHANDI",
            "TEHSIL": "BHAWANIPATNA"}
    }
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
            "message": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_security_check(auth_type="Auth_free", allowed_methods=["GET"], include_in_schema=False)
def get_mws_by_lat_lon(request):
    """
        Retrieve MWS ID data based on given latitude and longitude coordinates.

        Authorization in header:
        - Requires an API key passed in the `Authorization` header.
        - Example: `Authorization: Api-Key <your-api-key>`

        Query params should contain:
        - `latitude` (float): Latitude coordinate (-90 to 90)
        - `longitude` (float): Longitude coordinate (-180 to 180)

        Example Request:
        `GET /api/v1/get_mws_id_by_lat_lon/?latitude=25.9717644&longitude=82.44364023`

        Returns code:
        - 200 OK: JSON data (if file_type=json) or Excel file download
        - 400 Bad Request: Invalid parameters or logic error
        - 401 Unauthorized: Invalid or missing API key
        - 500 Internal Server Error: File generation or reading issue

        Response data:
            `{
                "uid": "12_234647",
                "state": "UTTAR PRADESH",
                "district": "JAUNPUR",
                "tehsil": "BADLAPUR"
            }`
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
        return Response({"error": str(e)}, status=500)


@api_security_check(auth_type="Auth_free", allowed_methods=["GET"])
def get_mws_json_by_stats_excel(request):
    """
        Retrieve MWS data for a given state, district, tehsil, and MWS ID.

        Authorization in header:
        - Requires an API key passed in the `Authorization` header.
        - Example: `Authorization: Api-Key <your-api-key>`

        Query params should contain:
        - `state` (str): Name of the state (e.g. Odisha)
        - `district` (str): Name of the district (e.g. Ganjam)
        - `tehsil` (str): Name of the tehsil (e.g. Chatrapur)
        - `mws_id` (str): Unique MWS identifier (e.g. 12_12345)

        Example Request:
        `GET /api/v1/get_mws_data?state=Uttar Pradesh&district=Jaunpur&tehsil=Badlapur&mws_id=12_234647`

        Returns code:
        - 200 OK: JSON data (if file_type=json) or Excel file download
        - 400 Bad Request: Invalid parameters or logic error
        - 401 Unauthorized: Invalid or missing API key
        - 500 Internal Server Error: File generation or reading issue

        Response Data:
            `{
                "hydrological_annual": [
                    {
                        "uid": "12_234647",
                        "et_in_mm_2017-2018": 894.1,
                        "runoff_in_mm_2017-2018": 148.57,
                        "g_in_mm_2017-2018": -321.06,
                        "deltag_in_mm_2017-2018": -321.06,
                        "precipitation_in_mm_2017-2018": 721.62,
                        "welldepth_in_m_2017-2018": -1.78,
                        ............
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
            }`
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


@api_security_check(auth_type="Auth_free", allowed_methods=["GET"])
def generate_tehsil_data(request):
    """
    Retrieve Tehsil-level Excel or JSON data for a given state, district, and tehsil.

    Authorization in header:
    - Requires an API key passed in the `Authorization` header.
    - Example: `Authorization: Api-Key <your-api-key>`

    Query params should contain:
    - `state` (str): Name of the state (e.g. Odisha)
    - `district` (str): Name of the district (e.g. Ganjam)
    - `tehsil` (str): Name of the tehsil (e.g. Chatrapur)
    - `file_type` (optional str): For file type if passed json then json else excel (e.g. json)

    Example Request:
    GET `/api/v1/get_tehsil_data?state=Uttar Pradesh&district=Jaunpur&tehsil=Badlapur&file_type=json`

    Returns code:
    - 200 OK: JSON data (if file_type=json) or Excel file download
    - 400 Bad Request: Invalid parameters or logic error
    - 401 Unauthorized: Invalid or missing API key
    - 500 Internal Server Error: File generation or reading issue

    Response Data:
        `{
            "aquifer_vector": [
                {
                "uid": "12_207597",
                "area_in_ha": 2336.11,
                "aquifer_class": "Alluvium",
                "principle_aq_alluvium_percent": 100,
                "principle_aq_banded gneissic complex_percent": 0,
                "principle_aq_basalt_percent": 0,
                "principle_aq_charnockite_percent": 0,
                "principle_aq_gneiss_percent": 0,
                "principle_aq_granite_percent": 0,
                "principle_aq_intrusive_percent": 0,
                "principle_aq_khondalite_percent": 0,
                "principle_aq_laterite_percent": 0,
                "principle_aq_limestone_percent": 0,
                "principle_aq_none_percent": 0,
                "principle_aq_quartzite_percent": 0,
                "principle_aq_sandstone_percent": 0,
                "principle_aq_schist_percent": 0,
                "principle_aq_shale_percent": 0
                },
                {
                "uid": "12_208413",
                "area_in_ha": 864.04,
                "aquifer_class": "Alluvium",
                "principle_aq_alluvium_percent": 100,
                "principle_aq_banded gneissic complex_percent": 0,
                "principle_aq_basalt_percent": 0,
                "principle_aq_charnockite_percent": 0,
                "principle_aq_gneiss_percent": 0,
                "principle_aq_granite_percent": 0,
                "principle_aq_intrusive_percent": 0,
                "principle_aq_khondalite_percent": 0,
                "principle_aq_laterite_percent": 0,
                "principle_aq_limestone_percent": 0,
                "principle_aq_none_percent": 0,
                "principle_aq_quartzite_percent": 0,
                "principle_aq_sandstone_percent": 0,
                "principle_aq_schist_percent": 0,
                "principle_aq_shale_percent": 0
                }
            ]
            .............
            ............
        }`

    """

    print("Inside generating tehsil excel data")
    try:
        # Get query parameters
        state = request.query_params.get("state", "").lower().strip().replace(" ", "_")
        district = request.query_params.get("district", "").lower().strip().replace(" ", "_")
        tehsil = request.query_params.get("tehsil", "").lower().strip().replace(" ", "_")
        file_type = request.query_params.get("file_type", "excel").lower()

        logging.info(f"Request to generate Excel for state: {state}, district: {district}, tehsil: {tehsil}, file_type: {file_type}")

        # Construct file path
        base_path = os.path.join(EXCEL_PATH, 'data/stats_excel_files')
        state_path = os.path.join(base_path, state.upper())
        district_path = os.path.join(state_path, district.upper())
        filename = f"{district}_{tehsil}.xlsx"
        file_path = os.path.join(district_path, filename)

        # Generate file if not exists
        if not os.path.exists(file_path):
            logging.info("Excel file does not exist. Generating...")
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
        logging.error(f"Error generating Excel file: {str(e)}")
        return Response({
            "status": "error",
            "message": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_security_check(auth_type="Auth_free", allowed_methods=["GET"])
def get_mws_json_by_kyl_indicator(request):
    """
        Retrieve KYL indicator data for a specific MWS ID in a given state, district, and tehsil.

        Authorization in header:
        - Requires an API key passed in the `Authorization` header.
        - Example: `Authorization: Api-Key <your-api-key>`

        Query params should contain:
        - `state` (str): Name of the state (e.g. Odisha)
        - `district` (str): Name of the district (e.g. Ganjam)
        - `tehsil` (str): Name of the tehsil (e.g. Chatrapur)
        - `mws_id` (str): Unique MWS identifier

        Example Request:
        - `GET /api/v1/get_mws_kyl_indicator?state=Uttar Pradesh&district=Jaunpur&tehsil=Badlapur&mws_id=12_234647`
        
        Returns code :
        - 200 OK: JSON list of KYL indicator data
        - 400 Bad Request: Missing or invalid parameters
        - 401 Unauthorized: Missing or invalid API key
        - 500 Internal Server Error: Unexpected failure

        Response Data:
            `[
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
                    "total_nrega_assets": 550,
                    "mws_intersect_villages": "[200586, 200587, 200588, 200589, 200594, 200595, 200722, 200725, 200726, 200727, 200728, 200729, 200730, 200731, 200604, 200605, 200606, 200607, 200608, 200733, 200734, 200611, 200612, 200613, 200614, 200615, 200616, 200617, 200618, 200739, 200744, 200634, 200635, 200639, 200640, 200735]",
                    "degradation_land_area": 108.92,
                    "increase_in_tree_cover": 150.02,
                    "decrease_in_tree_cover": 109.18,
                    "built_up_area": 87.33,
                    "lulc_slope_category": null,
                    "lulc_plain_category": "~67% Double Cropped",
                    "area_wide_scale_restoration": 21.84,
                    "area_protection": 3453.75,
                    "aquifer_class": 1,
                    "soge_class": 2
                }
                ]`
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


@api_security_check(auth_type="API_key", allowed_methods=["GET"])
def get_generated_layer_urls(request):
    """
    Retrieve generated layer URLs for a given state, district, and block.
    
    This endpoint is hidden from API documentation.
    
    Query params:
    - state (str): Name of the state
    - district (str): Name of the district  
    - block (str): Name of the block

    Example Request:
    - `GET /api/v1/get_generated_layer_urls?state=Uttar Pradesh&district=Jaunpur&tehsil=Badlapur`

    Response Data:
    [
        {
            "layer_desc": "Change Detection Afforestation",
            "layer_type": "raster",
            "layer_url": "https://geoserver.core-stack.org:8443/geoserver/change_detection/wcs?service=WCS&version=2.0.1&request=GetCoverage&CoverageId=change_detection:change_jaunpur__Afforestation&format=geotiff&compression=LZW&tiling=true&tileheight=256&tilewidth=256",
            "style_name": "deforestation"
        },
        {
            "layer_desc": "Aquifer layer data",
            "layer_type": "vector",
            "layer_url": "https://geoserver.core-stack.org:8443/geoserver/aquifer/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=aquifer:aquifer_vector_jaunpur_&outputFormat=application/json",
            "style_name": ""
        }
    ]
    
    Returns:
    - 200 OK: Success with layer details
    - 500 Internal Server Error: Processing error
    """
    try:
        print("Inside Get Generated Layer Urls API.")
        state = request.query_params.get("state", "").lower()
        district = request.query_params.get("district", "").lower().replace(" ", "_")
        block = request.query_params.get("block", "").lower().replace(" ", "_")

        layers_details_json = fetch_generated_layer_urls(district, block)
        return JsonResponse(layers_details_json, status=200, safe=False)

    except Exception as e:
        logging.error(f"Error in get_generated_layer_urls: {str(e)}")
        return Response({
            "status": "error",
            "message": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    


