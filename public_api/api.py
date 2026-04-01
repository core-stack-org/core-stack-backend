from rest_framework.decorators import schema
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
from django.db.models import Q
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
    get_mws_geometries_data,
    get_village_geometries_data,
)
from computing.models import Layer, Dataset, LayerType
from geoadmin.models import StateSOI, DistrictSOI, TehsilSOI
from stats_generator.utils import get_url
from nrm_app.settings import GEOSERVER_URL
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
    get_village_geometries_schema,
    get_mws_geometries_schema,
)
from geoadmin.utils import (
    transform_data,
    activated_tehsils,
    get_activated_location_json,
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
@swagger_auto_schema(**get_mws_data_schema)
@api_security_check(auth_type="API_key")
def get_mws_data(request):
    """
    Retrieve MWS data for a given state, district, tehsil, and MWS ID.
    """
    print("Inside mws data by excel api")
    try:
        state = request.query_params.get("state").lower()
        district = request.query_params.get("district").lower()
        tehsil = request.query_params.get("tehsil").lower()
        mws_id = request.query_params.get("mws_id")

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
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


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
        state = valid_gee_text(request.query_params.get("state").lower())
        district = valid_gee_text(request.query_params.get("district").lower())
        tehsil = valid_gee_text(request.query_params.get("tehsil").lower())
        mws_id = request.query_params.get("mws_id")

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
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


#############  Get Generated Layers Urls  ##################
@swagger_auto_schema(**generated_layer_urls_schema)
@api_security_check(auth_type="API_key")
def get_generated_layer_urls(request):
    try:
        print("Inside Get Generated Layer Urls API.")
        state = request.query_params.get("state").lower()
        district = request.query_params.get("district").lower()
        tehsil = request.query_params.get("tehsil").lower()

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
        state = valid_gee_text(request.query_params.get("state").lower())
        district = valid_gee_text(request.query_params.get("district").lower())
        tehsil = valid_gee_text(request.query_params.get("tehsil").lower())
        mws_id = request.query_params.get("mws_id")

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

        # Call business logic function
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


@swagger_auto_schema(**get_mws_geometries_schema)
@api_security_check(auth_type="API_key")
def get_mws_geometries(request):
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
        # Get geometry data
        success, result = get_mws_geometries_data(state, district, tehsil)
        if not success:
            return Response(
                {"error": result},  # result contains error message
                status=status.HTTP_404_NOT_FOUND,
            )

        # Return geometry
        return Response(result, status=status.HTTP_200_OK)

    except Exception as e:
        return Response(
            {"error": f"Internal server error: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@swagger_auto_schema(**get_village_geometries_schema)
@api_security_check(auth_type="API_key")
def get_village_geometries(request):
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

        # Get geometry data
        success, result = get_village_geometries_data(state, district, tehsil)

        if not success:
            return Response(
                {"error": result},  # result contains error message
                status=status.HTTP_404_NOT_FOUND,
            )

        # Return geometry
        return Response(result, status=status.HTTP_200_OK)

    except Exception as e:
        return Response(
            {"error": f"Internal server error: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


#############  Get Layer Manifest for GeoNode/QGIS  ##################
@api_security_check(auth_type="API_key")
def get_layer_manifest(request):
    """
    Return a GeoNode/QGIS-ready manifest of all layers.
    
    Query parameters:
    - state: Filter by state (optional)
    - district: Filter by district (optional)
    - tehsil: Filter by tehsil/block (optional)
    - all_active: Return all active locations (optional, boolean)
    - format: Output format - 'json' or 'csv' (default: json)
    """
    from datetime import datetime, timezone
    from urllib.parse import parse_qs, urlparse
    from django.http import HttpResponse
    import csv
    from io import StringIO

    def raster_tiff_download_url(workspace, layer_name):
        return (
            f"{GEOSERVER_URL}/{workspace}/wcs?service=WCS&version=2.0.1"
            f"&request=GetCoverage&CoverageId={workspace}:{layer_name}"
            f"&format=geotiff&compression=LZW&tiling=true&tileheight=256&tilewidth=256"
        )

    def infer_service_details(layer_url):
        if not layer_url:
            return {
                "service": "", "workspace": "", "resource_name": "",
                "resource_identifier": "", "geoserver_root": "", "ows_url": "", "wms_url": "",
            }
        parsed = urlparse(layer_url)
        query_params = parse_qs(parsed.query)
        
        path = parsed.path or ""
        marker_index = path.find("/geoserver")
        if marker_index == -1:
            geoserver_root = f"{parsed.scheme}://{parsed.netloc}"
        else:
            prefix = path[: marker_index + len("/geoserver")]
            geoserver_root = f"{parsed.scheme}://{parsed.netloc}{prefix}"

        service = query_params.get("service", [""])[0].upper()
        workspace = ""
        resource_name = ""
        resource_identifier = ""

        if "typeName" in query_params and query_params["typeName"]:
            resource_identifier = query_params["typeName"][0]
            if ":" in resource_identifier:
                workspace, resource_name = resource_identifier.split(":", 1)
        elif "CoverageId" in query_params and query_params["CoverageId"]:
            resource_identifier = query_params["CoverageId"][0]
            if ":" in resource_identifier:
                workspace, resource_name = resource_identifier.split(":", 1)

        ows_url = f"{geoserver_root}/{workspace}/ows" if workspace else ""
        wms_url = f"{geoserver_root}/wms" if geoserver_root else ""

        return {
            "service": service, "workspace": workspace,
            "resource_name": resource_name, "resource_identifier": resource_identifier,
            "geoserver_root": geoserver_root, "ows_url": ows_url, "wms_url": wms_url,
        }

    def infer_qgis_provider(layer_type, service):
        lt = str(layer_type).strip().lower()
        sv = str(service).strip().lower()
        if lt in {"vector", "point"} or sv == "wfs":
            return "WFS"
        if lt == "raster" or sv == "wcs":
            return "WCS"
        if sv == "wms":
            return "WMS"
        return ""

    def infer_download_format(provider):
        p = str(provider).strip().lower()
        if p == "wfs":
            return "GeoJSON"
        if p == "wcs":
            return "GeoTIFF"
        if p == "wms":
            return "Rendered map image"
        return ""

    def infer_style_format(style_url):
        lowered = str(style_url).lower()
        if lowered.endswith(".qml"):
            return "QML"
        if lowered.endswith(".sld"):
            return "SLD"
        if lowered.endswith(".json"):
            return "JSON"
        return ""

    try:
        state = request.query_params.get("state", "").lower()
        district = request.query_params.get("district", "").lower()
        tehsil = request.query_params.get("tehsil", "").lower()
        all_active = request.query_params.get("all_active", "").lower() == "true"
        output_format = request.query_params.get("format", "json").lower()

        # Build base queryset
        layers_qs = Layer.objects.select_related(
            "dataset", "state", "district", "block"
        ).filter(is_sync_to_geoserver=True)

        # Filter by location
        locations = []
        if all_active:
            locations = (
                layers_qs.values("state__state_name", "district__district_name", "block__tehsil_name")
                .distinct()
            )
            locations = [
                {
                    "state": loc["state__state_name"],
                    "district": loc["district__district_name"],
                    "tehsil": loc["block__tehsil_name"],
                }
                for loc in locations
            ]
        elif state and district and tehsil:
            locations = [{"state": state, "district": district, "tehsil": tehsil}]
        else:
            locations = [{"state": state, "district": district, "tehsil": tehsil}]

        # Collect layer records
        all_records = []
        EXCLUDE_KEYWORDS = ["run_off", "evapotranspiration", "precipitation"]

        for loc in locations:
            filters = Q(is_sync_to_geoserver=True)
            if loc.get("state"):
                filters &= Q(state__state_name__iexact=loc["state"])
            if loc.get("district"):
                filters &= Q(district__district_name__iexact=loc["district"])
            if loc.get("tehsil"):
                filters &= Q(block__tehsil_name__iexact=loc["tehsil"])

            for kw in EXCLUDE_KEYWORDS:
                filters &= ~Q(layer_name__icontains=kw)

            location_layers = layers_qs.filter(filters).order_by("layer_name", "-layer_version")

            # Deduplicate
            seen = {}
            for layer in location_layers:
                name = layer.layer_name.lower()
                if name not in seen:
                    seen[name] = layer
                else:
                    cv = float(seen[name].layer_version or 0)
                    nv = float(layer.layer_version or 0)
                    if nv > cv:
                        seen[name] = layer

            for layer in seen.values():
                dataset = layer.dataset
                workspace = dataset.workspace or ""
                layer_type = dataset.layer_type or ""
                layer_name = layer.layer_name or ""

                # Get style URLs
                style_url = ""
                sld_url = ""
                if dataset.misc:
                    style_url = dataset.misc.get("style_url", "")
                    sld_url = dataset.misc.get("sld_url", "")

                # Generate layer URL
                if layer_type in [LayerType.VECTOR, LayerType.POINT]:
                    layer_url = get_url(workspace, layer_name)
                elif layer_type == LayerType.RASTER:
                    layer_url = raster_tiff_download_url(workspace, layer_name)
                else:
                    layer_url = ""

                service_details = infer_service_details(layer_url)
                qgis_provider = infer_qgis_provider(layer_type, service_details["service"])

                all_records.append({
                    "state": layer.state.state_name.lower() if layer.state else "",
                    "district": layer.district.district_name.lower() if layer.district else "",
                    "tehsil": layer.block.tehsil_name.lower() if layer.block else "",
                    "dataset_name": dataset.name or "",
                    "layer_name": layer_name,
                    "layer_type": layer_type,
                    "layer_version": layer.layer_version or "",
                    "layer_url": layer_url,
                    "style_url": style_url,
                    "sld_url": sld_url,
                    "style_format": infer_style_format(style_url),
                    "sld_format": "SLD" if sld_url else "",
                    "gee_asset_path": layer.gee_asset_path or "",
                    "service_type": service_details["service"],
                    "workspace": service_details["workspace"],
                    "resource_identifier": service_details["resource_identifier"],
                    "resource_name": service_details["resource_name"] or layer_name,
                    "geoserver_root": service_details["geoserver_root"],
                    "ows_url": service_details["ows_url"],
                    "wms_url": service_details["wms_url"],
                    "qgis_provider": qgis_provider,
                    "download_format": infer_download_format(qgis_provider),
                    "geonode_publish_strategy": "remote-service-from-geoserver",
                })

        # Build summary
        unique_wms = sorted(set(r["wms_url"] for r in all_records if r.get("wms_url")))
        unique_ws = sorted(set(r["workspace"] for r in all_records if r.get("workspace")))
        type_counts = {}
        for r in all_records:
            lt = r.get("layer_type", "unknown") or "unknown"
            type_counts[lt] = type_counts.get(lt, 0) + 1

        manifest = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "django_database",
            "scope": {
                "all_active_locations": all_active,
                "requested_state": state,
                "requested_district": district,
                "requested_tehsil": tehsil,
                "location_count": len(locations),
            },
            "summary": {
                "layer_count": len(all_records),
                "layer_type_counts": type_counts,
                "unique_workspaces": unique_ws,
                "unique_wms_urls": unique_wms,
            },
            "locations": locations,
            "layers": all_records,
        }

        if output_format == "csv":
            if not all_records:
                return Response(
                    {"error": "No layers found for the specified location(s)"},
                    status=status.HTTP_404_NOT_FOUND,
                )
            fieldnames = list(all_records[0].keys())
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_records)

            response = HttpResponse(output.getvalue(), content_type="text/csv")
            response["Content-Disposition"] = "attachment; filename=core_stack_manifest.csv"
            return response

        return Response(manifest, status=status.HTTP_200_OK)

    except Exception as e:
        print(f"Error in get_layer_manifest: {str(e)}")
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
