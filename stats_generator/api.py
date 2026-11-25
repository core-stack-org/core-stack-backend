import hmac

from django.conf import settings
from rest_framework.decorators import api_view, schema
from rest_framework.response import Response
from rest_framework import status
from .models import LayerInfo
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
def list_layer_info(request):
    """
    Read-only list of LayerInfo rows for local compute instances.
    Optional query filters: layer_type, workspace, excel_to_be_generated.
    Validates X-Api-Key against PROD_BACKEND_API_KEY when configured.
    """
    api_key = getattr(settings, "PROD_BACKEND_API_KEY", "")
    if api_key and not hmac.compare_digest(
        request.headers.get("X-Api-Key", ""), api_key
    ):
        return Response(
            {"error": "Unauthorized"}, status=status.HTTP_401_UNAUTHORIZED
        )

    queryset = LayerInfo.objects.all()

    layer_type = request.query_params.get("layer_type")
    if layer_type:
        queryset = queryset.filter(layer_type=layer_type)

    workspace = request.query_params.get("workspace")
    if workspace:
        queryset = queryset.filter(workspace=workspace)

    excel_param = request.query_params.get("excel_to_be_generated")
    if excel_param is not None and excel_param != "":
        normalized = str(excel_param).strip().lower()
        if normalized in ("1", "true", "yes"):
            queryset = queryset.filter(excel_to_be_generated=True)
        elif normalized in ("0", "false", "no"):
            queryset = queryset.filter(excel_to_be_generated=False)
        else:
            return Response(
                {
                    "error": (
                        f"Invalid boolean for excel_to_be_generated: {excel_param!r}"
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

    return Response({"layers": list(queryset.values(*LAYER_INFO_FIELDS))})


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
