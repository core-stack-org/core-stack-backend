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


@api_view(["GET"])
@auth_free
@schema(None)
def generate_excel_file_layer(request):
    print("Inside generate_excel_file_layer API.")
    try:
        state = valid_gee_text(request.query_params.get("state", "").lower())
        district = valid_gee_text(request.query_params.get("district", "").lower())
        block = valid_gee_text(request.query_params.get("block", "").lower())

        return download_layers_excel_file(state, district, block)

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

        return generate_mws_data_for_kyl_filters(state, district, block, file_type)

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

        return get_generate_filter_data_village(state, district, block)

    except Exception as e:
        return Response(
            {"status": "error", "message": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
