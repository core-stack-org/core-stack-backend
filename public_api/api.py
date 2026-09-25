from datetime import datetime, timezone

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.http import JsonResponse
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
    get_mws_geometry,
    get_mws_geometries_data,
    get_village_geometries,
    get_village_geometries_data,
    filter_village_feature_collection,
    village_rows_to_feature_collection,
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
    get_mws_data_v2_schema,
    get_mws_geometries_schema,
    get_village_geometries_schema,
    admin_by_latlon_schema_v2,
    mws_by_latlon_schema_v2,
    tehsil_data_schema_v2,
    kyl_indicators_schema_v2,
    generated_layer_urls_schema_v2,
    mws_report_urls_schema_v2,
    mws_geometries_schema_v2,
    village_geometries_schema_v2,
    generate_active_locations_schema_v2,
)
from geoadmin.utils import (
    transform_data,
    activated_tehsils,
    get_activated_location_json,
)
from .dataset_filters import (
    filter_active_locations,
    filter_tehsil_payload,
    parse_tehsil_data_filter,
)
from utilities.openmeteo_format import (
    error_envelope,
    flat_active_locations_payload,
    flat_admin_detail_payload,
    flat_generated_layers_payload,
    flat_kyl_indicator_payload,
    flat_mws_report_url_payload,
    flat_mws_by_latlon_payload,
    flat_mws_geometry_payload,
    fortnight_structure_from_mws,
    legacy_hourly_to_fortnight_inner_block,
    normalize_payload,
    tehsil_structure_from_dict,
    success_envelope,
)

try:
    from pymongo import MongoClient
except Exception:
    MongoClient = None

_PUBLIC_API_V2_MONGO_LOGGED = False


def _get_mongo_collection_public_api_v2():
    global _PUBLIC_API_V2_MONGO_LOGGED
    uri = getattr(settings, "MONGODB_URI", "") or ""
    db_name = getattr(settings, "MONGODB_DB_NAME", "core_stack")
    coll_name = getattr(
        settings, "MONGODB_PUBLIC_API_V2_COLLECTION", "public_api_mws_v2_cache"
    )
    if not uri or MongoClient is None:
        if not _PUBLIC_API_V2_MONGO_LOGGED:
            print(
                "Public API v2 Mongo cache disabled: set MONGODB_URI and install pymongo."
            )
            _PUBLIC_API_V2_MONGO_LOGGED = True
        return None, None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        collection = client[db_name][coll_name]
        if not _PUBLIC_API_V2_MONGO_LOGGED:
            print(f"Public API v2 Mongo cache enabled: db={db_name}, collection={coll_name}")
            _PUBLIC_API_V2_MONGO_LOGGED = True
        return client, collection
    except Exception as exc:
        if not _PUBLIC_API_V2_MONGO_LOGGED:
            print(f"Public API v2 Mongo unavailable: {exc}")
            _PUBLIC_API_V2_MONGO_LOGGED = True
        return None, None


def _mongo_mws_v2_key(state_norm, district_l, tehsil_l, mws_id):
    return {
        "state": state_norm,
        "district": district_l,
        "tehsil": tehsil_l,
        "mws_id": str(mws_id),
    }


def _load_mws_v2_from_mongo(state_norm, district_l, tehsil_l, mws_id):
    client, collection = _get_mongo_collection_public_api_v2()
    if collection is None:
        return None
    try:
        doc = collection.find_one(
            _mongo_mws_v2_key(state_norm, district_l, tehsil_l, mws_id),
            {"_id": 0, "payload": 1},
        )
        return doc.get("payload") if doc else None
    except Exception as exc:
        print(f"Public API v2 Mongo read failed: {exc}")
        return None
    finally:
        if client is not None:
            client.close()


def _save_mws_v2_to_mongo(state_norm, district_l, tehsil_l, mws_id, payload):
    client, collection = _get_mongo_collection_public_api_v2()
    if collection is None:
        return
    try:
        collection.update_one(
            _mongo_mws_v2_key(state_norm, district_l, tehsil_l, mws_id),
            {
                "$set": {
                    **_mongo_mws_v2_key(state_norm, district_l, tehsil_l, mws_id),
                    "payload": payload,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
            upsert=True,
        )
    except Exception as exc:
        print(f"Public API v2 Mongo write failed: {exc}")
    finally:
        if client is not None:
            client.close()


def _success_response(data, http_status=status.HTTP_200_OK):
    inner = normalize_payload(data)
    return Response(success_envelope(inner), status=http_status)


def _success_response_v2(data, http_status=status.HTTP_200_OK):
    """data is already an Open-Meteo inner block (e.g. cached MWS fortnightly bundle)."""
    inner = legacy_hourly_to_fortnight_inner_block(data)
    return Response(success_envelope(inner), status=http_status)


def _error_response(message, http_status, details=None):
    return Response(error_envelope(message, details), status=http_status)


def _error_response_v2(message, http_status, details=None):
    return _error_response(message, http_status, details)


def _get_required_query_param(request, name):
    value = request.query_params.get(name)
    if value is None:
        return None
    value = str(value).strip()
    return value if value else None


def _normalize_geo_params(request):
    state_raw = _get_required_query_param(request, "state")
    district_raw = _get_required_query_param(request, "district")
    tehsil_raw = _get_required_query_param(request, "tehsil")
    if not state_raw or not district_raw or not tehsil_raw:
        return None, None, None
    return (
        valid_gee_text(state_raw.lower()),
        valid_gee_text(district_raw.lower()),
        valid_gee_text(tehsil_raw.lower()),
    )


def _normalize_external_result(result, default_error="Unable to process request"):
    if isinstance(result, Response):
        data = result.data if hasattr(result, "data") else {}
        if 200 <= result.status_code < 300:
            inner = normalize_payload(data)
            return Response(success_envelope(inner), http_status=result.status_code)
        message = (
            data.get("error")
            or data.get("message")
            or data.get("Message")
            or default_error
        )
        return Response(error_envelope(message, data), status=result.status_code)
    return _success_response(result)


@swagger_auto_schema(**admin_by_latlon_schema)
@api_security_check(auth_type="API_key")
def get_admin_details_by_lat_lon(request):
    """
    v1: raw admin details for the given coordinates.
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

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return Response(
                {
                    "error": "Latitude must be between -90 and 90, longitude must be between -180 and 180."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        return get_location_info_by_lat_lon(lat, lon)
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
@api_security_check(auth_type="API_key")
def get_mws_by_lat_lon(request):
    """
    v1: raw MWS id lookup for the given coordinates.
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
        return get_mws_id_by_lat_lon(lon, lat)
    except Exception as e:
        print("Exception while getting the mws_id by lat long", str(e))
        return Response(
            {"State": "", "District": "", "Tehsil": "", "uid": ""}, status=404
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
        state, district, tehsil = _normalize_geo_params(request)
        mws_id = _get_required_query_param(request, "mws_id")

        if state is None or district is None or tehsil is None or mws_id is None:
            return Response(
                {
                    "error": "'state', 'district', 'tehsil', and 'mws_id' parameters are required."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state)
            or not is_valid_string(district)
            or not is_valid_string(tehsil)
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

        data = get_mws_time_series_data(state, district, tehsil, mws_id)
        if not data:
            return Response(
                {"error": "Data not found for the given mws_id"},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(data, status=200)
    except Exception as e:
        print("Exception in stats mws json :: ", e)
        return Response({"Exception": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@swagger_auto_schema(**get_mws_data_v2_schema)
@api_security_check(auth_type="API_key")
def get_mws_data_v2(request):
    """
    Retrieve MWS data in Open-Meteo-style time-series structure.
    Cached in MongoDB (same pattern as waterbodies); use regenerate=true to refresh from GeoServer.
    """
    print("Inside mws data by excel api v2")
    try:
        state, district, tehsil = _normalize_geo_params(request)
        mws_id = _get_required_query_param(request, "mws_id")
        regenerate = str(request.query_params.get("regenerate", "")).lower() in {
            "1",
            "true",
            "yes",
        }

        if state is None or district is None or tehsil is None or mws_id is None:
            return _error_response_v2(
                "'state', 'district', 'tehsil', and 'mws_id' parameters are required.",
                status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state)
            or not is_valid_string(district)
            or not is_valid_string(tehsil)
        ):
            return _error_response_v2(
                "State/District/Tehsil must contain only letters, spaces, and underscores",
                status.HTTP_400_BAD_REQUEST,
            )

        if not is_valid_mws_id(mws_id):
            return _error_response_v2(
                "MWS id can only contain numbers and underscores",
                status.HTTP_400_BAD_REQUEST,
            )

        state_norm = state.upper()
        if not regenerate:
            cached = _load_mws_v2_from_mongo(state_norm, district, tehsil, mws_id)
            if cached is not None:
                return _success_response_v2(cached, http_status=status.HTTP_200_OK)

        data = get_mws_time_series_data(state, district, tehsil, mws_id)
        if not data:
            return _error_response_v2(
                "Data not found for the given mws_id",
                status.HTTP_404_NOT_FOUND,
            )
        if isinstance(data, dict) and data.get("error"):
            return _error_response_v2(
                str(data["error"]),
                status.HTTP_404_NOT_FOUND,
            )
        if isinstance(data, dict) and "Error in get mws data" in data:
            return _error_response_v2(
                "Failed to fetch MWS data from GeoServer.",
                status.HTTP_502_BAD_GATEWAY,
                details=data.get("Error in get mws data"),
            )

        v2_payload = fortnight_structure_from_mws(data)
        _save_mws_v2_to_mongo(state_norm, district, tehsil, mws_id, v2_payload)

        return _success_response_v2(v2_payload, http_status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in stats mws json v2 :: ", e)
        return _error_response_v2(
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
        state, district, tehsil = _normalize_geo_params(request)
        regenerate = request.query_params.get("regenerate", "").lower()

        if state is None or district is None or tehsil is None:
            return Response(
                {"error": "'state', 'district', and 'tehsil' parameters are required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state)
            or not is_valid_string(district)
            or not is_valid_string(tehsil)
        ):
            return Response(
                {
                    "error": "State/District/Tehsil must contain only letters, spaces, and underscores"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        file_path, file_exists = excel_file_exists(state, district, tehsil)
        if not file_exists:
            return Response(
                {"Message": "Data not found for this state, district, tehsil"},
                status=status.HTTP_404_NOT_FOUND,
            )

        # Get JSON (from cache or generate)
        json_data = get_tehsil_json(state, district, tehsil, regenerate)
        return JsonResponse(json_data, status=200)

    except Exception as e:
        print(f"Error: {str(e)}")
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
        state, district, tehsil = _normalize_geo_params(request)
        mws_id = _get_required_query_param(request, "mws_id")

        if state is None or district is None or tehsil is None or mws_id is None:
            return Response(
                {
                    "error": "'state', 'district', 'tehsil', and 'mws_id' parameters are required."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state)
            or not is_valid_string(district)
            or not is_valid_string(tehsil)
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
        return Response({"Exception": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


#############  Get Generated Layers Urls  ##################
@swagger_auto_schema(**generated_layer_urls_schema)
@api_security_check(auth_type="API_key")
def get_generated_layer_urls(request):
    try:
        print("Inside Get Generated Layer Urls API.")
        state, district, tehsil = _normalize_geo_params(request)

        if state is None or district is None or tehsil is None:
            return Response(
                {"error": "'state', 'district', and 'tehsil' parameters are required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state)
            or not is_valid_string(district)
            or not is_valid_string(tehsil)
        ):
            return Response(
                {
                    "error": "State/District/Tehsil must contain only letters, spaces, and underscores"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        layers_details_json = fetch_generated_layer_urls(state, district, tehsil)
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
    """
    API endpoint to get MWS report URLs.
    Handles request/response and parameter validation.
    """
    try:
        print("Inside Get Generated Layer Urls API.")

        # Get and validate parameters
        state, district, tehsil = _normalize_geo_params(request)
        mws_id = _get_required_query_param(request, "mws_id")

        if state is None or district is None or tehsil is None or mws_id is None:
            return Response(
                {
                    "error": "'state', 'district', 'tehsil', and 'mws_id' parameters are required."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (
            not is_valid_string(state)
            or not is_valid_string(district)
            or not is_valid_string(tehsil)
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

        base_url = request.build_absolute_uri("/")[:-1]
        result, error_response = generate_mws_report_url(
            state, district, tehsil, mws_id, base_url
        )

        if error_response:
            return error_response

        return Response(result, status=status.HTTP_200_OK)

    except Exception as e:
        print(f"Error in get_generated_layer_urls: {str(e)}")
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


#############  Get MWS Geometry  ##################
@swagger_auto_schema(**get_mws_geometries_schema)
@api_security_check(auth_type="API_key")
def get_mws_geometries(request):
    """
    v1: raw GeoJSON FeatureCollection for every MWS in the tehsil.
    """
    print("Inside get MWS geometries")
    try:
        state = valid_gee_text(request.query_params.get("state", "").lower())
        district = valid_gee_text(request.query_params.get("district", "").lower())
        tehsil = valid_gee_text(request.query_params.get("tehsil", "").lower())

        if not all([state, district, tehsil]):
            return Response(
                {"error": "All parameters (state, district, tehsil) are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        success, result = get_mws_geometries_data(state, district, tehsil)
        if not success:
            return Response(
                {"error": result},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(result, status=status.HTTP_200_OK)
    except Exception as e:
        return Response(
            {"error": f"Internal server error: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


#############  Get Village Geometries  ##################
@swagger_auto_schema(**get_village_geometries_schema)
@api_security_check(auth_type="API_key")
def get_village_geometries_api(request):
    """
    v1: raw GeoJSON FeatureCollection for every village in the tehsil.
    """
    print("Inside get Village geometries")
    try:
        state = valid_gee_text(request.query_params.get("state", "").lower())
        district = valid_gee_text(request.query_params.get("district", "").lower())
        tehsil = valid_gee_text(request.query_params.get("tehsil", "").lower())

        if not all([state, district, tehsil]):
            return Response(
                {"error": "All parameters (state, district, tehsil) are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        success, result = get_village_geometries_data(state, district, tehsil)
        if not success:
            return Response(
                {"error": result},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(result, status=status.HTTP_200_OK)
    except Exception as e:
        return Response(
            {"error": f"Internal server error: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
            return Response(activated_locations_data, status=status.HTTP_200_OK)

        response_data = activated_tehsils()
        transformed_data = transform_data(data=response_data)
        return Response(transformed_data, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in proposed_blocks api :: ", e)
        return Response(
            {"Exception": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@swagger_auto_schema(**admin_by_latlon_schema_v2)
@api_security_check(auth_type="API_key")
def get_admin_details_by_lat_lon_v2(request):
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

        properties_list = get_location_info_by_lat_lon(lat, lon)
        if isinstance(properties_list, Response):
            data = properties_list.data if hasattr(properties_list, "data") else {}
            message = (
                data.get("error")
                or data.get("message")
                or "Unable to retrieve location data for the given coordinates"
            )
            return _error_response(message, properties_list.status_code, details=data)
        payload = flat_admin_detail_payload(properties_list)
        return Response(success_envelope(payload), status=status.HTTP_200_OK)
    except Exception as e:
        print(f"Error occurred: {e}")
        return _error_response(
            "Unable to retrieve location data for the given coordinates",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )


@swagger_auto_schema(**mws_by_latlon_schema_v2)
@api_security_check(auth_type="API_key")
def get_mws_by_lat_lon_v2(request):
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
        if isinstance(data, Response):
            payload = data.data if hasattr(data, "data") else {}
            message = (
                payload.get("error")
                or payload.get("message")
                or "Unable to retrieve MWS id for the given coordinates"
            )
            return _error_response(message, data.status_code, details=payload)
        payload = flat_mws_by_latlon_payload(data)
        return Response(success_envelope(payload), status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception while getting the mws_id by lat long", str(e))
        return _error_response(
            "Unable to retrieve MWS id for the given coordinates",
            status.HTTP_404_NOT_FOUND,
            details=str(e),
        )


@swagger_auto_schema(**tehsil_data_schema_v2)
@api_security_check(auth_type="API_key")
def generate_tehsil_data_v2(request):
    print("Inside generating tehsil excel data")
    try:
        state, district, tehsil = _normalize_geo_params(request)
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

        try:
            requested_sheets = parse_tehsil_data_filter(
                request.query_params.getlist("data")
            )
        except ValueError as exc:
            return _error_response(str(exc), status.HTTP_400_BAD_REQUEST)

        json_data = get_tehsil_json(state, district, tehsil, regenerate)
        payload = filter_tehsil_payload(
            tehsil_structure_from_dict(json_data), requested_sheets
        )
        return Response(success_envelope(payload), status=status.HTTP_200_OK)
    except Exception as e:
        print(f"Error: {str(e)}")
        return _error_response(
            "Internal server error while generating tehsil data",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )


@swagger_auto_schema(**kyl_indicators_schema_v2)
@api_security_check(auth_type="API_key")
def get_mws_json_by_kyl_indicator_v2(request):
    print("Inside Mws kyl Indicator api")
    try:
        state, district, tehsil = _normalize_geo_params(request)
        mws_id = _get_required_query_param(request, "mws_id")

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
        if isinstance(data, dict) and data.get("error"):
            return _error_response(
                str(data["error"]),
                status.HTTP_404_NOT_FOUND,
            )
        if not data:
            return _error_response(
                "Data not found for the given mws_id.",
                status.HTTP_404_NOT_FOUND,
            )
        rows = data if isinstance(data, list) else []
        payload = flat_kyl_indicator_payload(rows)
        return Response(success_envelope(payload), status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in stats mws json :: ", e)
        return _error_response(
            "Internal server error while fetching KYL indicator data",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )


@swagger_auto_schema(**generated_layer_urls_schema_v2)
@api_security_check(auth_type="API_key")
def get_generated_layer_urls_v2(request):
    try:
        print("Inside Get Generated Layer Urls API.")
        state, district, tehsil = _normalize_geo_params(request)

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
        payload = flat_generated_layers_payload(layers_details_json)
        return Response(success_envelope(payload), status=status.HTTP_200_OK)
    except ObjectDoesNotExist as e:
        print(f"Error in get_generated_layer_urls: {str(e)}")
        return _error_response(
            "State, district, or tehsil not found.",
            status.HTTP_404_NOT_FOUND,
            details=str(e),
        )
    except Exception as e:
        print(f"Error in get_generated_layer_urls: {str(e)}")
        return _error_response(
            "Internal server error while fetching generated layer urls",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )


@swagger_auto_schema(**mws_report_urls_schema_v2)
@api_security_check(auth_type="API_key")
def get_mws_report_urls_v2(request):
    try:
        print("Inside Get Generated Layer Urls API.")
        state, district, tehsil = _normalize_geo_params(request)
        mws_id = _get_required_query_param(request, "mws_id")

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

        payload = flat_mws_report_url_payload(result)
        return Response(success_envelope(payload), status=status.HTTP_200_OK)
    except Exception as e:
        print(f"Error in get_generated_layer_urls: {str(e)}")
        return _error_response(
            "Internal server error while fetching MWS report urls",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )


@swagger_auto_schema(**mws_geometries_schema_v2)
@api_security_check(auth_type="API_key")
def get_mws_geometries_v2(request):
    """
    v2: envelope plus optional mws_id. Polygon coordinates stay at full precision.
    """
    try:
        state, district, tehsil = _normalize_geo_params(request)
        mws_id = _get_required_query_param(request, "mws_id")

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

        if mws_id is not None and not is_valid_mws_id(mws_id):
            return _error_response(
                "MWS id can only contain numbers and underscores",
                status.HTTP_400_BAD_REQUEST,
            )

        if mws_id is None:
            ok, geojson_or_err = get_mws_geometries_data(state, district, tehsil)
            if ok:
                return Response(
                    success_envelope(geojson_or_err), status=status.HTTP_200_OK
                )
            result, error_response = get_mws_geometry(
                state, district, tehsil, mws_id=None
            )
            if error_response:
                err_payload = (
                    error_response.data if hasattr(error_response, "data") else {}
                )
                err_message = (
                    err_payload.get("error")
                    or err_payload.get("message")
                    or err_payload.get("Message")
                    or (
                        geojson_or_err
                        if isinstance(geojson_or_err, str)
                        else "Failed to fetch MWS geometries"
                    )
                )
                return _error_response(
                    err_message,
                    error_response.status_code,
                    details=err_payload,
                )
            return Response(success_envelope(result), status=status.HTTP_200_OK)

        result, error_response = get_mws_geometry(state, district, tehsil, mws_id)
        if error_response:
            err_payload = (
                error_response.data if hasattr(error_response, "data") else {}
            )
            err_message = (
                err_payload.get("error")
                or err_payload.get("message")
                or err_payload.get("Message")
                or "Failed to fetch MWS geometry"
            )
            return _error_response(
                err_message,
                error_response.status_code,
                details=err_payload,
            )

        payload = flat_mws_geometry_payload(result)
        return Response(success_envelope(payload), status=status.HTTP_200_OK)
    except Exception as e:
        print(f"Error in get_mws_geometries: {str(e)}")
        return _error_response(
            "Internal server error while fetching MWS geometry",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )


@swagger_auto_schema(**village_geometries_schema_v2)
@api_security_check(auth_type="API_key")
def get_village_geometries_api_v2(request):
    try:
        state, district, tehsil = _normalize_geo_params(request)
        village_id = _get_required_query_param(request, "village_id")

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

        if village_id is not None and not str(village_id).isdigit():
            return _error_response(
                "village_id must be numeric",
                status.HTTP_400_BAD_REQUEST,
            )

        success, result = get_village_geometries_data(state, district, tehsil)
        if success:
            geojson = filter_village_feature_collection(result, village_id)
            if village_id is not None and not geojson.get("features"):
                return _error_response(
                    "Village not found for the given village_id.",
                    status.HTTP_404_NOT_FOUND,
                )
            return Response(success_envelope(geojson), status=status.HTTP_200_OK)

        rows, error_response = get_village_geometries(
            state, district, tehsil, village_id=village_id
        )
        if error_response:
            err_payload = (
                error_response.data if hasattr(error_response, "data") else {}
            )
            err_message = (
                err_payload.get("error")
                or err_payload.get("message")
                or (
                    result
                    if isinstance(result, str)
                    else "Failed to fetch village geometries"
                )
            )
            return _error_response(
                err_message,
                error_response.status_code,
                details=err_payload,
            )

        return Response(
            success_envelope(village_rows_to_feature_collection(rows)),
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print(f"Error in get_village_geometries_api: {str(e)}")
        return _error_response(
            "Internal server error while fetching village geometries",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )


@swagger_auto_schema(**generate_active_locations_schema_v2)
@api_security_check(auth_type="API_key")
def generate_active_locations_v2(request):
    try:
        activated_locations_data = get_activated_location_json()

        if activated_locations_data is not None:
            locations = activated_locations_data
        else:
            locations = transform_data(data=activated_tehsils())

        if isinstance(locations, dict) and "locations" in locations:
            locations = locations.get("locations")

        locations = filter_active_locations(
            locations,
            state=request.query_params.get("state"),
            district=request.query_params.get("district"),
            block=request.query_params.get("tehsil")
            or request.query_params.get("block"),
        )
        payload = flat_active_locations_payload(locations)
        return Response(success_envelope(payload), status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in proposed_blocks api :: ", e)
        return _error_response(
            "Internal server error while generating active locations",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(e),
        )
