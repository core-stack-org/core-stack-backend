from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from .utils import *
from .mws_indicators import get_generate_filter_mws_data, download_KYL_filter_data
from .village_indicators import get_generate_filter_data_village
from utilities.auth_utils import auth_free
from rest_framework_api_key.permissions import HasAPIKey
from rest_framework.decorators import permission_classes
import logging
from computing.misc.lat_lon_with_mws_data import get_mws_id_by_lat_lon, get_mws_json_from_stats_excel, get_mws_json_from_kyl_indicator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@api_view(["GET"])
@auth_free
def generate_excel_file_layer(request):
    try:
        # Get query parameters
        state = request.query_params.get("state", "").lower().strip().replace(" ", "_")
        district = request.query_params.get("district", "").lower().strip().replace(" ", "_")
        block = request.query_params.get("block", "").lower().strip().replace(" ", "_")
        file_type = request.query_params.get("file_type", "excel").lower()

        logging.info(f"Request to generate Excel for state: {state}, district: {district}, block: {block}, file_type: {file_type}")

        # Construct file path
        base_path = os.path.join(EXCEL_PATH, 'data/stats_excel_files')
        state_path = os.path.join(base_path, state.upper())
        district_path = os.path.join(state_path, district.upper())
        filename = f"{district}_{block}.xlsx"
        file_path = os.path.join(district_path, filename)

        # Generate file if not exists
        if not os.path.exists(file_path):
            logging.info("Excel file does not exist. Generating...")
            if not get_vector_layer_geoserver(state, district, block):
                raise ValueError("Failed to generate vector layer from GeoServer.")
            os.makedirs(district_path, exist_ok=True)
            file_path = download_layers_excel_file(state, district, block)
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
            return Response(json_data)

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


@api_view(["GET"])
@permission_classes([HasAPIKey])
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
        GET /api/v1/get_mws_id_by_lat_lon/?latitude=25.9717644&longitude=82.44364023

        Returns code:
        - 200 OK: JSON data (if file_type=json) or Excel file download
        - 400 Bad Request: Invalid parameters or logic error
        - 401 Unauthorized: Invalid or missing API key
        - 500 Internal Server Error: File generation or reading issue
    """

    print("Inside Get mws id by lat lon layer API")
    try:
        lat = float(request.query_params.get("latitude"))
        lon = float(request.query_params.get("longitude"))

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return Response({"error": "Latitude or longitude out of bounds."}, status=400)
        data = get_mws_id_by_lat_lon(lon, lat)
        return Response(data)

    except (TypeError, ValueError):
        return Response({"error": "Latitude or longitude is missing or invalid."}, status=400)

    except Exception as e:
        return Response({"error": str(e)}, status=500)


@api_view(["GET"])
@permission_classes([HasAPIKey])
def get_mws_json_by_stats_excel(request):
    """
        Retrieve MWS data for a given state, district, block, and MWS ID.

        Authorization in header:
        - Requires an API key passed in the `Authorization` header.
        - Example: `Authorization: Api-Key <your-api-key>`

        Query params should contain:
        - `state` (str): Name of the state (e.g. Odisha)
        - `district` (str): Name of the district (e.g. Ganjam)
        - `block` (str): Name of the block (e.g. Chatrapur)
        - `mws_id` (str): Unique MWS identifier (e.g. 12_12345)

        Example Request:
        GET /api/v1/get_mws_data?state=Uttar Pradesh&district=Jaunpur&block=Badlapur&mws_id=12_234647

        Returns code:
        - 200 OK: JSON data (if file_type=json) or Excel file download
        - 400 Bad Request: Invalid parameters or logic error
        - 401 Unauthorized: Invalid or missing API key
        - 500 Internal Server Error: File generation or reading issue
    """

    print("Inside mws data by excel api")
    try:
        state = request.query_params.get("state").lower()
        district = request.query_params.get("district").lower()
        block = request.query_params.get("block").lower()
        mws_id = request.query_params.get("mws_id")
        data = get_mws_json_from_stats_excel(state, district, block, mws_id)
        return Response(data)
    except Exception as e:
        print("Exception in stats mws json :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@permission_classes([HasAPIKey])
def generate_tehsil_data(request):
    """
    Retrieve Tehsil-level Excel or JSON data for a given state, district, and block.

    Authorization in header:
    - Requires an API key passed in the `Authorization` header.
    - Example: `Authorization: Api-Key <your-api-key>`

    Query params should contain:
    - `state` (str): Name of the state (e.g. Odisha)
    - `district` (str): Name of the district (e.g. Ganjam)
    - `block` (str): Name of the block (e.g. Chatrapur)
    - `file_type` (optional str): For file type if passed json then json else excel (e.g. json)

    Example Request:
    GET /api/v1/get_tehsil_data?state=Uttar Pradesh&district=Jaunpur&block=Badlapur&file_type=json

    Returns code:
    - 200 OK: JSON data (if file_type=json) or Excel file download
    - 400 Bad Request: Invalid parameters or logic error
    - 401 Unauthorized: Invalid or missing API key
    - 500 Internal Server Error: File generation or reading issue
    """

    print("Inside generating tehsil excel data")
    try:
        # Get query parameters
        state = request.query_params.get("state", "").lower().strip().replace(" ", "_")
        district = request.query_params.get("district", "").lower().strip().replace(" ", "_")
        block = request.query_params.get("block", "").lower().strip().replace(" ", "_")
        file_type = request.query_params.get("file_type", "excel").lower()

        logging.info(f"Request to generate Excel for state: {state}, district: {district}, block: {block}, file_type: {file_type}")

        # Construct file path
        base_path = os.path.join(EXCEL_PATH, 'data/stats_excel_files')
        state_path = os.path.join(base_path, state.upper())
        district_path = os.path.join(state_path, district.upper())
        filename = f"{district}_{block}.xlsx"
        file_path = os.path.join(district_path, filename)

        # Generate file if not exists
        if not os.path.exists(file_path):
            logging.info("Excel file does not exist. Generating...")
            if not get_vector_layer_geoserver(state, district, block):
                raise ValueError("Failed to generate vector layer from GeoServer.")
            os.makedirs(district_path, exist_ok=True)
            file_path = download_layers_excel_file(state, district, block)
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
            return Response(json_data)

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


@api_view(["GET"])
@permission_classes([HasAPIKey])
def get_mws_json_by_kyl_indicator(request):
    """
        Retrieve KYL indicator data for a specific MWS ID in a given state, district, and block.

        Authorization in header:
        - Requires an API key passed in the `Authorization` header.
        - Example: `Authorization: Api-Key <your-api-key>`

        Query params should contain:
        - `state` (str): Name of the state (e.g. Odisha)
        - `district` (str): Name of the district (e.g. Ganjam)
        - `block` (str): Name of the block (e.g. Chatrapur)
        - `mws_id` (str): Unique MWS identifier

        Example Request:
        - GET /api/v1/get_mws_kyl_indicator?state=Uttar Pradesh&district=Jaunpur&block=Badlapur&mws_id=12_234647
        
        Returns code :
        - 200 OK: JSON list of KYL indicator data
        - 400 Bad Request: Missing or invalid parameters
        - 401 Unauthorized: Missing or invalid API key
        - 500 Internal Server Error: Unexpected failure
    """
    print("Inside Mws kyl Indicator api")
    try:
        state = request.query_params.get("state").lower()
        district = request.query_params.get("district").lower()
        block = request.query_params.get("block").lower()
        mws_id = request.query_params.get("mws_id").lower()
        data = get_mws_json_from_kyl_indicator(state, district, block, mws_id)
        return Response(data)
    except Exception as e:
        print("Exception in stats mws json :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
