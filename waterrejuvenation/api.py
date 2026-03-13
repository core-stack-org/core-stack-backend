import io
import json
import os

from rest_framework.decorators import schema
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
from utilities.gee_utils import (
    valid_gee_text,
)
from nrm_app.settings import MEDIA_ROOT
from .models import WaterbodiesDesiltingLog
from .swagger_schemas import waterbodies_by_admin_schema, waterbodies_by_uuid
from .utils import get_merged_waterbodies_with_zoi

from utilities.auth_check_decorator import api_security_check
from drf_yasg.utils import swagger_auto_schema
from rest_framework.decorators import api_view
import io
import pandas as pd
from django.http import HttpResponse
from rest_framework import status
from rest_framework.response import Response


@swagger_auto_schema(**waterbodies_by_admin_schema)
@api_security_check(auth_type="API_key")
# @schema(None)
def get_waterbodies_by_admin_and_uid(request):

    try:
        state = request.query_params.get("state")
        district = request.query_params.get("district")
        block = request.query_params.get("tehsil") or request.query_params.get("block")
        uid = request.query_params.get("uid")

        # Required params check
        if not state or not district or not block:
            return Response(
                {
                    "error": "'state', 'district' and 'tehsil' (or 'block') parameters are required."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Normalize
        state_norm = state.upper()
        district_l = district.lower()
        block_l = block.lower()

        base_dir = f"{MEDIA_ROOT}stats_excel_files"
        out_dir = os.path.join(base_dir, state_norm, district.upper())
        merged_fname = f"{district_l}_{block_l}_merged_data.json"
        merged_path = os.path.join(out_dir, merged_fname)
        print(f"file: {merged_path}")
        merged_data = None

        # 1️⃣ **If cached merged file exists → load**
        if os.path.exists(merged_path):
            try:
                with open(merged_path, "r", encoding="utf-8") as fh:
                    print("Serving cached merged:", merged_path)
                    merged_data = json.load(fh)
            except Exception as e:
                print("Error reading cached file:", e)
                merged_data = None

        # 2️⃣ **If file NOT found OR failed to read → generate using your merge function**
        if merged_data is None:
            try:
                print("Generating merged data...")
                merged_data = get_merged_waterbodies_with_zoi(
                    state=state_norm,
                    district=district_l,
                    block=block_l,
                )
            except Exception as e:
                print(f"Error generating merged data: {e}")
                return Response(
                    {
                        "status": "error",
                        "message": "Failed to generate merged dataset.",
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

        # If still None → problem with WFS fetch
        if merged_data is None:
            return Response(
                {
                    "status": "error",
                    "message": "Merged dataset not available for given area.",
                },
                status=status.HTTP_502_BAD_GATEWAY,
            )

        # 3️⃣ **If UID provided, return only that item**
        if uid:
            uid_str = str(uid)

            # Try direct key match
            item = merged_data.get(uid_str)

            # Try integer key
            if item is None and uid_str.isdigit():
                item = merged_data.get(int(uid_str))

            if item is None:
                return Response(
                    {
                        "detail": f"UID '{uid}' not found for state={state_norm}, district={district_l}, block={block_l}."
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )

            return Response({uid_str: item}, status=status.HTTP_200_OK)

        # 4️⃣ **Return full merged data**
        return Response(merged_data, status=status.HTTP_200_OK)

    except Exception as e:
        print("Unexpected error:", e)
        return Response(
            {
                "status": "error",
                "message": "Internal server error while retrieving waterbody data.",
            },
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@swagger_auto_schema(**waterbodies_by_uuid)
@api_security_check(auth_type="API_key")
# @schema(None)
def get_waterbodies_by_uid(request):

    try:
        state = request.query_params.get("state")
        district = request.query_params.get("district")
        block = request.query_params.get("tehsil") or request.query_params.get("block")
        uid = request.query_params.get("uid")

        # Required params check
        if not state or not district or not block:
            return Response(
                {
                    "error": "'state', 'district' and 'tehsil' (or 'block') parameters are required."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Normalize
        state_norm = state.upper()
        district_l = district.lower()
        block_l = block.lower()

        base_dir = "stats_excel_file"
        out_dir = os.path.join(base_dir, state_norm, district.upper())
        merged_fname = f"{district_l}_{block_l}_merged_data.json"
        merged_path = os.path.join(out_dir, merged_fname)

        merged_data = None

        # 1️⃣ **If cached merged file exists → load**
        if os.path.exists(merged_path):
            try:
                with open(merged_path, "r", encoding="utf-8") as fh:
                    print("Serving cached merged:", merged_path)
                    merged_data = json.load(fh)
            except Exception as e:
                print("Error reading cached file:", e)
                merged_data = None

        # 2️⃣ **If file NOT found OR failed to read → generate using your merge function**
        if merged_data is None:
            try:
                print("Generating merged data...")
                merged_data = get_merged_waterbodies_with_zoi(
                    state=state_norm,
                    district=district_l,
                    block=block_l,
                )
            except Exception as e:
                print(f"Error generating merged data: {e}")
                return Response(
                    {
                        "status": "error",
                        "message": "Failed to generate merged dataset.",
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

        # If still None → problem with WFS fetch
        if merged_data is None:
            return Response(
                {
                    "status": "error",
                    "message": "Merged dataset not available for given area.",
                },
                status=status.HTTP_502_BAD_GATEWAY,
            )

        # 3️⃣ **If UID provided, return only that item**
        if uid:
            uid_str = str(uid)

            # Try direct key match
            item = merged_data.get(uid_str)

            # Try integer key
            if item is None and uid_str.isdigit():
                item = merged_data.get(int(uid_str))

            if item is None:
                return Response(
                    {
                        "detail": f"UID '{uid}' not found for state={state_norm}, district={district_l}, block={block_l}."
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )

            return Response({uid_str: item}, status=status.HTTP_200_OK)

        # 4️⃣ **Return full merged data**
        return Response(merged_data, status=status.HTTP_200_OK)

    except Exception as e:
        print("Unexpected error:", e)
        return Response(
            {
                "status": "error",
                "message": "Internal server error while retrieving waterbody data.",
            },
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_security_check(allowed_methods="POST")
@schema(None)
def generate_result_excel(request):
    print("Inside generate_result_excel API.")

    try:
        project_id = request.data.get("project_id")

        if not project_id:
            return Response(
                {"error": "project_id is required"}, status=status.HTTP_400_BAD_REQUEST
            )

        queryset = WaterbodiesDesiltingLog.objects.filter(
            project_id=project_id
        ).order_by("id")

        if not queryset.exists():
            return Response(
                {"error": "No records found for this project"},
                status=status.HTTP_404_NOT_FOUND,
            )

        # ✅ Build rows for pandas
        rows = []
        for idx, obj in enumerate(queryset, start=1):
            rows.append(
                {
                    "sr no.": idx,
                    "name of ngo": obj.name_of_ngo,
                    "state": obj.State,
                    "district": obj.District,
                    "taluka": obj.Taluka,
                    "village": obj.Village,
                    "name of the waterbody": obj.waterbody_name,
                    "latitude": obj.lat,
                    "longitude": obj.lon,
                    "silt excavated as per app": obj.slit_excavated,
                    "intervention_year": obj.intervention_year,
                }
            )

        # ✅ Create DataFrame with EXACT headers
        df = pd.DataFrame(rows)

        # ⭐ Append derived column (NO utils change)
        failure_map = dict(queryset.values_list("id", "failure_reason"))
        df["closest waterbody found"] = df.index.map(
            lambda i: "true" if queryset[i].process else "false"
        )
        df["Reason for not mapped"] = df.index.map(lambda i: queryset[i].failure_reason)

        # ✅ Write Excel to memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="results")

        output.seek(0)

        response = HttpResponse(
            output,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = (
            f'attachment; filename="waterbody_results_project_{project_id}.xlsx"'
        )

        return response

    except Exception as e:
        print("Exception in generate_result_excel API ::", e)
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
