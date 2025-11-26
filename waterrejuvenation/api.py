import json
import os

from rest_framework.decorators import schema
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
from utilities.gee_utils import (
    valid_gee_text,
)
from .utils import get_merged_waterbodies_with_zoi

from utilities.auth_check_decorator import api_security_check
from drf_yasg.utils import swagger_auto_schema


# @swagger_auto_schema(**admin_by_latlon_schema)
@api_security_check(auth_type="Auth_free")
def get_waterbodies_by_admin_and_uid(request):
    """
    Retrieve merged waterbody + ZOI data for a given administrative area (state, district, tehsil/block).
    If `uid` is provided, return only that UID's entry.
    Query params:
      - state (required)
      - district (required)
      - tehsil or block (required)
      - uid (optional)
    """
    try:

        state = request.query_params.get("state")
        district = request.query_params.get("district")
        # allow either 'tehsil' or 'block' param name
        block = request.query_params.get("tehsil") or request.query_params.get("block")
        uid = request.query_params.get("uid")

        # Validate required params
        if not state or not district or not block:
            return Response(
                {
                    "error": "'state' and 'district' and 'tehsil' (or 'block') parameters are required."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Normalize names just like the generator does
        state_norm = str(state).upper()
        district_l = str(district).lower()
        block_l = str(block).lower()

        # Determine merged file path (same convention used by generator)
        base_dir = "data/states_excel_files"
        merged_fname = f"{district_l}_{block_l}_merged_data.json"
        merged_path = os.path.join(
            base_dir, state_norm, district_l.upper(), merged_fname
        )

        merged_data = None

        # If cached merged file exists -> load it
        if os.path.exists(merged_path):
            try:
                with open(merged_path, "r", encoding="utf-8") as fh:
                    merged_data = json.load(fh)
            except Exception as e:
                # If reading cache fails, attempt to regenerate
                merged_data = None
                print(f"Failed to read cached merged file ({merged_path}): {e}")

        # If not cached, try to generate using your merge function
        if merged_data is None:
            try:
                merged_data = get_merged_waterbodies_with_zoi(
                    state=state_norm, district=district_l, block=block_l
                )
            except Exception as e:
                print(f"Error generating merged data: {e}")
                return Response(
                    {"status": "error", "message": "Failed to generate merged data."},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

        if merged_data is None:
            return Response(
                {
                    "status": "error",
                    "message": "Merged data not available for given area.",
                },
                status=status.HTTP_502_BAD_GATEWAY,
            )

        # If uid provided -> return only that entry
        if uid:
            # try several key forms
            item = merged_data.get(uid) or merged_data.get(str(uid))
            if item is None:
                # try numeric form if possible
                try:
                    if str(uid).isdigit():
                        item = merged_data.get(int(uid))
                except Exception:
                    item = None

            if item is None:
                return Response(
                    {
                        "detail": f"UID '{uid}' not found for state={state_norm} district={district_l} tehsil={block_l}."
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )

            return Response({str(uid): item}, status=status.HTTP_200_OK)

        # Otherwise return full merged dataset
        return Response(merged_data, status=status.HTTP_200_OK)

    except Exception as e:
        print(f"Unexpected error in get_waterbodies_by_admin_and_uid: {e}")
        return Response(
            {
                "status": "error",
                "message": "Unable to retrieve waterbody data for the given parameters.",
            },
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
