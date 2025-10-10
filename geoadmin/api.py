import json
from datetime import timedelta

from django.http import HttpRequest
from django.utils import timezone
from rest_framework import status
from rest_framework.decorators import api_view, schema
from rest_framework.request import Request
from rest_framework.response import Response

from utilities.auth_check_decorator import api_security_check
from utilities.auth_utils import auth_free

from .models import Block, District, DistrictSOI, State, StateSOI, TehsilSOI, UserAPIKey
from .serializers import BlockSerializer, DistrictSerializer, StateSerializer
from .utils import activated_blocks, normalize_name, transform_data


# state id is the census code while the district id is the id of the district from the DB
# block id is the id of the block from the DB
@api_security_check(auth_type="Auth_free")
@schema(None)
def get_states(request):
    try:
        states = StateSOI.objects.all()
        serializer = StateSerializer(states, many=True)
        states_data = serializer.data

        for state in states_data:
            state["normalized_state_name"] = normalize_name(state["state_name"])
        return Response({"states": states_data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_states api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_security_check(auth_type="Auth_free")
@schema(None)
def get_districts(request, state_id):
    try:
        districts = DistrictSOI.objects.filter(state_id=state_id)
        serializer = DistrictSerializer(districts, many=True)
        districts_data = serializer.data

        for district in districts_data:
            district["normalized_district_name"] = normalize_name(
                district["district_name"]
            )
        return Response({"districts": districts_data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_districts api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_security_check(auth_type="Auth_free")
@schema(None)
def get_blocks(request, district_id):
    try:
        blocks = TehsilSOI.objects.filter(district=district_id)
        serializer = BlockSerializer(blocks, many=True)
        tehsils_data = serializer.data

        for tehsil in tehsils_data:
            tehsil["normalized_tehsil_name"] = normalize_name(tehsil["tehsil_name"])
            tehsil["normalized_block_name"] = normalize_name(tehsil["tehsil_name"])
            tehsil["block_name"] = tehsil["tehsil_name"]
        return Response({"blocks": tehsils_data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_blocks api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_security_check(auth_type="Auth_free")
@schema(None)
def proposed_blocks(request):
    try:
        response_data = activated_blocks()
        transformed_data = transform_data(data=response_data)
        return Response(transformed_data, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in proposed_blocks api :: ", e)
        return Response(
            {"Exception": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(["PATCH"])
@auth_free
@schema(None)
def activate_location(request):
    """
    Update activation status of a location (state/district/block).

    Request body should contain:
    {
        "location_type": "state|district|block",
        "location_id": str,
        "active": bool
    }

    Hierarchical validation rules:
    - Districts can only be activated if their State is active
    - Blocks can only be activated if both their State and District are active
    """
    try:
        location_type = request.data.get("location_type")
        location_id = request.data.get("location_id")
        active = request.data.get("active")

        if not all([location_type, location_id, active is not None]):
            return Response(
                {
                    "error": "Missing required fields. Please provide location_type, location_id, and active status"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if location_type not in ["state", "district", "block"]:
            return Response(
                {
                    "error": "Invalid location_type. Must be one of: state, district, block"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            if location_type == "state":
                location = StateSOI.objects.get(id=location_id)
            elif location_type == "district":
                location = DistrictSOI.objects.get(id=location_id)

                if active and not location.state.active_status:
                    return Response(
                        {"error": "State not active yet, please activate."},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
            else:
                location = TehsilSOI.objects.get(id=location_id)

                if active:
                    state_active = location.district.state.active_status
                    district_active = location.district.active_status

                    if not state_active and not district_active:
                        return Response(
                            {
                                "error": "State and District not active yet, please activate them first."
                            },
                            status=status.HTTP_400_BAD_REQUEST,
                        )
                    elif not district_active:
                        return Response(
                            {"error": "District not active yet, please activate."},
                            status=status.HTTP_400_BAD_REQUEST,
                        )
                    elif not state_active:
                        return Response(
                            {"error": "State not active yet, please activate."},
                            status=status.HTTP_400_BAD_REQUEST,
                        )

            if location.active_status != active:
                location.active_status = active
                location.save()
                message = (
                    "Successfully activated a location"
                    if active
                    else "Successfully deactivated a location"
                )
                return Response(
                    {
                        "message": message,
                        "location_type": location_type,
                        "location_id": location_id,
                        "active": active,
                    },
                    status=status.HTTP_200_OK,
                )
            else:
                message = (
                    "Location already active" if active else "Location already inactive"
                )
                return Response(
                    {
                        "message": message,
                        "location_type": location_type,
                        "location_id": location_id,
                        "active": active,
                    },
                    status=status.HTTP_200_OK,
                )

        except (
            StateSOI.DoesNotExist,
            DistrictSOI.DoesNotExist,
            TehsilSOI.DoesNotExist,
        ):
            return Response(
                {"error": f"{location_type.title()} with id {location_id} not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

    except Exception as e:
        print(f"Exception in activate_location api: {e}")
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_security_check(allowed_methods="POST")
@schema(None)
def generate_api_key(request):
    """
    Single API for generating and deactivating API keys
    POST /api/v1/generate_api_key/

    Generate new API key (default action - no params needed):
    {}
    OR
    {
        "name": "My API Key",
        "expiry_days": 30
    }

    Deactivate API key:
    {
        "action": "deactivate",
        "user_id": 123,
        "api_key": "knog3H.OZ7LCmWNjLWK6HcaxUEM1tP2"
    }
    """
    try:
        if request.content_type == "application/json":
            data = json.loads(request.body)
        else:
            data = request.POST

        action = data.get("action", "generate")
        if action == "generate":
            try:
                expiry_days = int(data.get("expiry_days", 3000))
                name = data.get(
                    "name", f"API Key {timezone.now().strftime('%Y-%m-%d %H:%M')}"
                )

                expires_at = timezone.now() + timedelta(days=expiry_days)
                api_key_obj, generated_key = UserAPIKey.objects.create_key(
                    user=request.user, name=name, expires_at=expires_at
                )

                api_key_obj.api_key = generated_key
                api_key_obj.save()

                response_data = {
                    "action": "generate",
                    "success": True,
                    "message": "API key generated successfully",
                    "data": {
                        "api_key": generated_key,
                        "is_active": api_key_obj.is_active,
                        "expires_at": api_key_obj.expires_at,
                        "created_at": api_key_obj.created_at,
                    },
                }

                return Response(response_data, status=status.HTTP_201_CREATED)

            except ValueError:
                return Response(
                    {"success": False, "error": "Invalid expiry_days value"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        elif action == "deactivate":
            user_id = data.get("user_id")
            api_key = data.get("api_key")

            if not user_id or not api_key:
                return Response(
                    {
                        "success": False,
                        "error": "user_id and api_key are required for deactivation",
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            try:
                api_key_obj = UserAPIKey.objects.get(user_id=user_id, api_key=api_key)
                api_key_obj.is_active = False
                api_key_obj.expires_at = timezone.now()
                api_key_obj.save(update_fields=["is_active", "expires_at"])

                response_data = {
                    "action": "deactivate",
                    "success": True,
                    "message": "API key deactivated successfully",
                    "data": {
                        "api_key": api_key,
                        "is_active": api_key_obj.is_active,
                        "expires_at": api_key_obj.expires_at,
                    },
                }

                return Response(response_data, status=status.HTTP_200_OK)

            except UserAPIKey.DoesNotExist:
                return Response(
                    {"success": False, "error": "API key not found"},
                    status=status.HTTP_404_NOT_FOUND,
                )

        else:
            return Response(
                {"error": "Invalid action. Use: generate, deactivate"},
                status=status.HTTP_400_BAD_REQUEST,
            )

    except Exception as e:
        return Response(
            {"success": False, "error": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_security_check()
@schema(None)
def get_user_api_keys(request):
    """
    Get all API keys for the authenticated user (Simplified version)
    GET /api/v1/get_user_api_keys/
    """
    try:
        api_keys = UserAPIKey.objects.filter(
            user=request.user, is_active=True, api_key__isnull=False
        ).order_by("-created_at")
        formatted_keys = []
        for api_key_obj in api_keys:
            formatted_keys.append(
                {
                    "api_key": api_key_obj.api_key,
                    "is_active": api_key_obj.is_active,
                    "expires_at": api_key_obj.expires_at,
                    "created_at": api_key_obj.created_at,
                }
            )

        response_data = {
            "success": True,
            "message": "API keys retrieved successfully",
            "api_keys": formatted_keys,
        }

        return Response(response_data, status=status.HTTP_200_OK)

    except Exception as e:
        return Response(
            {"success": False, "error": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
