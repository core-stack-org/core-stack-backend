from rest_framework.decorators import api_view, schema
from rest_framework.response import Response
from rest_framework import status
from .utils import *
from .mws_indicators import generate_mws_data_for_kyl_filters
from .village_indicators import get_generate_filter_data_village
from utilities.auth_utils import auth_free
from utilities.gee_utils import (
    valid_gee_text,
)


def _compute_param(request):
    """Optional `compute` query param: "local" or "gee" (default, GeoServer)."""
    compute = request.query_params.get("compute", "gee").strip().lower() or "gee"
    if compute not in ("local", "gee"):
        raise ValueError(f"compute must be 'local' or 'gee', got {compute!r}")
    return compute


@api_view(["GET"])
@auth_free
@schema(None)
def download_stats_excel_file(request):
    print("Inside download_stats_excel_file API.")
    try:
        state = valid_gee_text(request.query_params.get("state", "").lower())
        district = valid_gee_text(request.query_params.get("district", "").lower())
        block = valid_gee_text(request.query_params.get("block", "").lower())
        compute = _compute_param(request)

        return download_layers_excel_file(state, district, block, compute=compute)

    except Exception as e:
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
@auth_free
@schema(None)
def generate_stats_excel_file_data(request):
    print("Inside generate_stats_excel_file_data API.")
    try:
        state = valid_gee_text(request.query_params.get("state", "").lower())
        district = valid_gee_text(request.query_params.get("district", "").lower())
        block = valid_gee_text(request.query_params.get("block", "").lower())
        # Optional compute=local: build the layer sheets from locally generated
        # layers instead of GeoServer.
        compute = _compute_param(request)

        return generate_stats_excel_file(state, district, block, compute=compute)

    except Exception as e:
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
@auth_free
@schema(None)
def add_sheets_in_stats_excel(request):
    print("Inside add sheet to excel API.")
    try:
        state = valid_gee_text(request.query_params.get("state", "").lower())
        district = valid_gee_text(request.query_params.get("district", "").lower())
        block = valid_gee_text(request.query_params.get("block", "").lower())
        workspace = request.query_params.get("workspace", "")

        response_data = add_sheets_to_excel(state, district, block, workspace)
        return Response({"status": "success"}, status=status.HTTP_200_OK)

    except Exception as e:
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
@auth_free
@schema(None)
def generate_mws_data_for_kyl(request):
    print("Inside generate_mws_data_for_kyl API.")
    try:
        state = valid_gee_text(request.query_params.get("state", "").lower())
        district = valid_gee_text(request.query_params.get("district", "").lower())
        block = valid_gee_text(request.query_params.get("block", "").lower())
        file_type = request.query_params.get("file_type", "").lower().strip()
        regenerate = request.query_params.get("regenerate", "").lower()

        return generate_mws_data_for_kyl_filters(
            state, district, block, file_type, regenerate
        )

    except Exception as e:
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
@auth_free
@schema(None)
def generate_village_data_for_kyl(request):
    print("Inside generate_filter_data_village API.")
    try:
        state = valid_gee_text(request.query_params.get("state", "").lower())
        district = valid_gee_text(request.query_params.get("district", "").lower())
        block = valid_gee_text(request.query_params.get("block", "").lower())
        regenerate = request.query_params.get("regenerate", "")

        return get_generate_filter_data_village(state, district, block, regenerate)

    except Exception as e:
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
