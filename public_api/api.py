from rest_framework.response import Response
from rest_framework import status
from utilities.gee_utils import (
    valid_gee_text,
)
from .views import (
    is_valid_string,
    is_valid_mws_id,
    excel_file_exists,
    fetch_generated_layer_urls,
    get_location_info_by_lat_lon,
    get_mws_id_by_lat_lon,
    get_mws_time_series_data,
    get_mws_json_from_kyl_indicator,
    get_tehsil_json,
    generate_mws_report_url,
)
from utilities.auth_check_decorator import api_security_check
from drf_yasg.utils import swagger_auto_schema
from .swagger_schemas import (
    admin_by_latlon_schema,
    mws_by_latlon_schema,
    tehsil_data_schema,
    generated_layer_urls_schema,
    mws_report_urls_schema,
    kyl_indicators_schema,
    generate_active_locations_schema,
    get_mws_data_schema,
)
from geoadmin.utils import (
    transform_data,
    activated_entities,
    get_activated_location_json,
)


def _success_response(data, http_status=status.HTTP_200_OK):
    return Response(
        {
            "status": "success",
            "error_message": None,
            "data": data,
        },
        status=http_status,
    )


def _error_response(message, http_status, details=None):
    payload = {
        "status": "error",
        "error_message": message,
        "error": message,
    }
    if details is not None:
        payload["details"] = details
    return Response(payload, status=http_status)


def _normalize_external_result(result, default_error="Unable to process request"):
    if isinstance(result, Response):
        data = result.data if hasattr(result, "data") else {}
        if 200 <= result.status_code < 300:
            return _success_response(data, http_status=result.status_code)
        message = (
            data.get("error")
            or data.get("message")
            or data.get("Message")
            or default_error
        )
        return _error_response(message, result.status_code, details=data)
    return _success_response(result)


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
            return _error_response(
                "Both 'latitude' and 'longitude' parameters are required.",
                status.HTTP_400_BAD_REQUEST,
            )

        try:
            lat = float(lat_param)
            lon = float(lon_param)
        except (ValueError, TypeError):
            return _error_response(
                "Latitude and longitude must be valid numbers(float).",
                status.HTTP_400_BAD_REQUEST,
            )

        # To Validate the coordinate
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return _error_response(
                "Latitude must be between -90 and 90, longitude must be between -180 and 180.",
                status.HTTP_400_BAD_REQUEST,
            )

        properties_list = get_location_info_by_lat_lon(lat, lon)
        return _normalize_external_result(
            properties_list,
            default_error="Unable to retrieve location data for the given coordinates",
        )

    except Exception as e:
        print(f"Error occurred: {e}")
        return _error_response(
            "Unable to retrieve location data for the given coordinates",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )


######### Get Mws Id by lat lon #########
@swagger_auto_schema(**mws_by_latlon_schema)
@api_security_check(auth_type="API_key")
def get_mws_by_lat_lon(request):
    """
    Retrieve MWS ID based on given latitude and longitude coordinates.
    """
    print("Inside Get mws id by lat lon layer API")
    try:
        lat_param = request.query_params.get("latitude")
        lon_param = request.query_params.get("longitude")

        if lat_param is None or lon_param is None:
            return _error_response(
                "Both 'latitude' and 'longitude' parameters are required.",
                status.HTTP_400_BAD_REQUEST,
            )

        try:
            lat = float(lat_param)
            lon = float(lon_param)
        except (ValueError, TypeError):
            return _error_response(
                "Latitude and longitude must be valid numbers(float).",
                status.HTTP_400_BAD_REQUEST,
            )

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return _error_response(
                "Latitude must be between -90 and 90, longitude must be between -180 and 180.",
                status.HTTP_400_BAD_REQUEST,
            )
        data = get_mws_id_by_lat_lon(lon, lat)
        return _normalize_external_result(
            data, default_error="Unable to retrieve MWS id for the given coordinates"
        )
    except Exception as e:
        print("Exception while getting the mws_id by lat long", str(e))
        return _error_response(
            "Unable to retrieve MWS id for the given coordinates",
            status.HTTP_404_NOT_FOUND,
            details=str(e),
        )


########## Get MWS Data by MWS ID  ##########
@swagger_auto_schema(**get_mws_data_schema)
@api_security_check(auth_type="API_key")
def get_mws_data(request):
    """
    Retrieve MWS data for a given state, district, tehsil, and MWS ID.
    """
    print("Inside mws data by excel api")
    try:
        state = valid_gee_text(request.query_params.get("state").lower())
        district = valid_gee_text(request.query_params.get("district").lower())
        tehsil = valid_gee_text(request.query_params.get("tehsil").lower())
        mws_id = request.query_params.get("mws_id")

        if state is None or district is None or tehsil is None or mws_id is None:
            return _error_response(
                "'state', 'district', 'tehsil', and 'mws_id' parameters are required.",
                status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state)
            or not is_valid_string(district)
            or not is_valid_string(tehsil)
        ):
            return _error_response(
                "State/District/Tehsil must contain only letters, spaces, and underscores",
                status.HTTP_400_BAD_REQUEST,
            )

        if not is_valid_mws_id(mws_id):
            return _error_response(
                "MWS id can only contain numbers and underscores",
                status.HTTP_400_BAD_REQUEST,
            )

        data = get_mws_time_series_data(state, district, tehsil, mws_id)
        if not data:
            return _error_response(
                "Data not found for the given mws_id",
                status.HTTP_404_NOT_FOUND,
            )
        return _success_response(data, http_status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in stats mws json :: ", e)
        return _error_response(
            "Internal server error while fetching MWS data",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )


######### Get MWS DATA by Admin Details  ##########
@swagger_auto_schema(**tehsil_data_schema)
@api_security_check(auth_type="API_key")
def generate_tehsil_data(request):
    """
    Retrieve Tehsil-level JSON data for a given state, district, and tehsil.
    """
    print("Inside generating tehsil excel data")
    try:
        # Get query parameters
        state = valid_gee_text(request.query_params.get("state").lower())
        district = valid_gee_text(request.query_params.get("district").lower())
        tehsil = valid_gee_text(request.query_params.get("tehsil").lower())
        regenerate = request.query_params.get("regenerate", "").lower()

        if state is None or district is None or tehsil is None:
            return _error_response(
                "'state', 'district', and 'tehsil' parameters are required.",
                status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state)
            or not is_valid_string(district)
            or not is_valid_string(tehsil)
        ):
            return _error_response(
                "State/District/Tehsil must contain only letters, spaces, and underscores",
                status.HTTP_400_BAD_REQUEST,
            )

        file_path, file_exists = excel_file_exists(state, district, tehsil)
        if not file_exists:
            return _error_response(
                "Data not found for this state, district, tehsil",
                status.HTTP_404_NOT_FOUND,
            )

        # Get JSON (from cache or generate)
        json_data = get_tehsil_json(state, district, tehsil, regenerate)
        return _success_response(json_data, http_status=status.HTTP_200_OK)

    except Exception as e:
        print(f"Error: {str(e)}")
        return _error_response(
            "Internal server error while generating tehsil data",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )


########### Get KYL Data based on MWS ID  ###############
@swagger_auto_schema(**kyl_indicators_schema)
@api_security_check(auth_type="API_key")
def get_mws_json_by_kyl_indicator(request):
    """
    Retrieve KYL indicator data for a specific MWS ID in a given state, district, and tehsil.
    """
    print("Inside Mws kyl Indicator api")
    try:
        state = valid_gee_text(request.query_params.get("state").lower())
        district = valid_gee_text(request.query_params.get("district").lower())
        tehsil = valid_gee_text(request.query_params.get("tehsil").lower())
        mws_id = request.query_params.get("mws_id")

        if state is None or district is None or tehsil is None or mws_id is None:
            return _error_response(
                "'state', 'district', 'tehsil', and 'mws_id' parameters are required.",
                status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state)
            or not is_valid_string(district)
            or not is_valid_string(tehsil)
        ):
            return _error_response(
                "State/District/Tehsil must contain only letters, spaces, and underscores",
                status.HTTP_400_BAD_REQUEST,
            )

        if not is_valid_mws_id(mws_id):
            return _error_response(
                "MWS id can only contain numbers and underscores",
                status.HTTP_400_BAD_REQUEST,
            )

        if not excel_file_exists(state, district, tehsil):
            return _error_response(
                "Data not found for this state, district, tehsil.",
                status.HTTP_404_NOT_FOUND,
            )

        data = get_mws_json_from_kyl_indicator(state, district, tehsil, mws_id)
        if not data:
            return _error_response(
                "Data not found for the given mws_id.",
                status.HTTP_404_NOT_FOUND,
            )
        return _success_response(data, http_status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in stats mws json :: ", e)
        return _error_response(
            "Internal server error while fetching KYL indicator data",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )


#############  Get Generated Layers Urls  ##################
@swagger_auto_schema(**generated_layer_urls_schema)
@api_security_check(auth_type="API_key")
def get_generated_layer_urls(request):
    try:
        print("Inside Get Generated Layer Urls API.")
        state = valid_gee_text(request.query_params.get("state").lower())
        district = valid_gee_text(request.query_params.get("district").lower())
        tehsil = valid_gee_text(request.query_params.get("tehsil").lower())

        if state is None or district is None or tehsil is None:
            return _error_response(
                "'state', 'district', and 'tehsil' parameters are required.",
                status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state)
            or not is_valid_string(district)
            or not is_valid_string(tehsil)
        ):
            return _error_response(
                "State/District/Tehsil must contain only letters, spaces, and underscores",
                status.HTTP_400_BAD_REQUEST,
            )

        layers_details_json = fetch_generated_layer_urls(state, district, tehsil)
        if not layers_details_json:
            return _error_response(
                "Data not found for this state, district, tehsil.",
                status.HTTP_404_NOT_FOUND,
            )
        return _success_response(layers_details_json, http_status=status.HTTP_200_OK)

    except Exception as e:
        print(f"Error in get_generated_layer_urls: {str(e)}")
        return _error_response(
            "Internal server error while fetching generated layer urls",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )


#############  Get MWS Report Urls  ##################
@swagger_auto_schema(**mws_report_urls_schema)
@api_security_check(auth_type="API_key")
def get_mws_report_urls(request):
    """
    API endpoint to get MWS report URLs.
    Handles request/response and parameter validation.
    """
    try:
        print("Inside Get Generated Layer Urls API.")

        # Get and validate parameters
        state = valid_gee_text(request.query_params.get("state").lower())
        district = valid_gee_text(request.query_params.get("district").lower())
        tehsil = valid_gee_text(request.query_params.get("tehsil").lower())
        mws_id = request.query_params.get("mws_id")

        if state is None or district is None or tehsil is None or mws_id is None:
            return _error_response(
                "'state', 'district', 'tehsil', and 'mws_id' parameters are required.",
                status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state)
            or not is_valid_string(district)
            or not is_valid_string(tehsil)
        ):
            return _error_response(
                "State/District/Tehsil must contain only letters, spaces, and underscores",
                status.HTTP_400_BAD_REQUEST,
            )

        if not is_valid_mws_id(mws_id):
            return _error_response(
                "MWS id can only contain numbers and underscores",
                status.HTTP_400_BAD_REQUEST,
            )

        # Call business logic function
        base_url = request.build_absolute_uri("/")[:-1]
        result, error_response = generate_mws_report_url(
            state, district, tehsil, mws_id, base_url
        )

        if error_response:
            err_payload = (
                error_response.data if hasattr(error_response, "data") else {}
            )
            err_message = (
                err_payload.get("error")
                or err_payload.get("message")
                or err_payload.get("Message")
                or "Failed to generate MWS report URL"
            )
            return _error_response(
                err_message,
                error_response.status_code,
                details=err_payload,
            )

        return _success_response(result, http_status=status.HTTP_200_OK)

    except Exception as e:
        print(f"Error in get_generated_layer_urls: {str(e)}")
        return _error_response(
            "Internal server error while fetching MWS report urls",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )


@swagger_auto_schema(**generate_active_locations_schema)
@api_security_check(auth_type="API_key")
def generate_active_locations(request):
    """
    Return proposed blocks data from get_activated_location_json if available,
    otherwise generate and store the data
    """
    try:
        activated_locations_data = get_activated_location_json()

        if activated_locations_data is not None:
            return _success_response(
                activated_locations_data, http_status=status.HTTP_200_OK
            )

        response_data = activated_entities()
        transformed_data = transform_data(data=response_data)
        return _success_response(transformed_data, http_status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in proposed_blocks api :: ", e)
        return _error_response(
            "Internal server error while generating active locations",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )
