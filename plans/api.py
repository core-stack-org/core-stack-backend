from typing import Any, Dict, Optional

import requests
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status
from rest_framework.decorators import api_view, schema
from rest_framework.response import Response

from nrm_app.settings import ODK_USER_EMAIL_SYNC, ODK_USER_PASSWORD_SYNC
from utilities.auth_check_decorator import api_security_check
from utilities.auth_utils import auth_free
from utilities.constants import (
    ODK_SYNC_URL_AGRI_FEEDBACK,
    ODK_SYNC_URL_AGRI_MAINTENANCE,
    ODK_SYNC_URL_CROP,
    ODK_SYNC_URL_GW_FEEDBACK,
    ODK_SYNC_URL_GW_MAINTENANCE,
    ODK_SYNC_URL_IRRIGATION_STRUCTURE,
    ODK_SYNC_URL_LIVELIHOOD,
    ODK_SYNC_URL_RECHARGE_STRUCTURE,
    ODK_SYNC_URL_RS_WATERBODY_MAINTENANCE,
    ODK_SYNC_URL_SETTLEMENT,
    ODK_SYNC_URL_SWB_FEEDBACK,
    ODK_SYNC_URL_WATER_STRUCTURES,
    ODK_SYNC_URL_WATER_STRUCTURES_MAINTENANCE,
    ODK_SYNC_URL_WELL,
)

from .build_layer import build_layer
from .models import Plan
from .serializers import PlanSerializer
from .utils import fetch_bearer_token, fetch_odk_data


# MARK: Get Plans API
@api_security_check(auth_type="Auth_free")
@schema(None)
def get_plans(request):
    """
    Get Plans API

    Args:
        block_id (str, optional): Block ID. Defaults to None.

    Returns:
        Response: JSON response containing a list of plans of a block or all the plans
    """
    try:
        block_id = request.query_params.get("block_id", None)
        if block_id is not None:
            plans = Plan.objects.filter(block=block_id)
        else:
            plans = Plan.objects.all()
        serializer = PlanSerializer(plans, many=True)
        response = {"plans": serializer.data}

        return Response(response, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_plans api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@auth_free
@schema(None)
def add_plan(request):
    if request.method == "POST":
        serializer = PlanSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()  # Save the new Plan instance if validation passes
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    return Response(
        {"error": "Method not allowed"}, status=status.HTTP_405_METHOD_NOT_ALLOWED
    )


# api's for add settlement, add well, add waterbody | add work [new, maintenance]
@api_view(["POST"])
@auth_free
@schema(None)
def add_resources(request):
    layer_name = request.data.get("layer_name").lower()
    resource_type = request.data.get("resource_type").lower()
    plan_id = request.data.get("plan_id")
    plan_name = request.data.get("plan_name").lower()
    district = request.data.get("district_name").lower()
    block = request.data.get("block_name").lower()

    CSV_PATH = "/tmp/" + str(resource_type) + "_" + str(plan_id) + "_" + block + ".csv"

    odk_data_found = fetch_odk_data(CSV_PATH, resource_type, block, plan_id)

    if not odk_data_found:
        return Response(
            {"error": f"No ODK data found for the given Plan ID: {plan_id}"},
            status=status.HTTP_404_NOT_FOUND,
        )

    try:
        success = build_layer(
            layer_type="resources",
            item_type=resource_type,
            plan_id=plan_id,
            district=district,
            block=block,
            csv_path=CSV_PATH,
        )
        if not success:
            return Response(
                {"error": "Failed to build resource layer."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
    except Exception as e:
        return Response(
            {"error": f"An unexpected error occurred: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    return Response({"message": "Success"}, status=status.HTTP_201_CREATED)


@api_view(["POST"])
@auth_free
@schema(None)
def add_works(request):
    """
    work type: plan_gw: recharge st., main_swb: maintenance surface water bodies, plan_agri: irrigation works, livelihood
    works: work_type_plan_id_district_block
    """
    layer_name = request.data.get("layer_name").lower()
    work_type = request.data.get("work_type").lower()
    plan_id = request.data.get("plan_id")
    plan_name = request.data.get("plan_name").lower()
    district = request.data.get("district_name").lower()
    block = request.data.get("block_name").lower()

    CSV_PATH = "/tmp/" + str(work_type) + "_" + str(plan_id) + "_" + block + ".csv"

    odk_data_found = fetch_odk_data(CSV_PATH, work_type, block, plan_id)

    if not odk_data_found:
        return Response(
            {"error": f"No ODK data found for the given Plan ID: {plan_id}"},
            status=status.HTTP_404_NOT_FOUND,
        )

    try:
        success = build_layer(
            layer_type="works",
            item_type=work_type,
            plan_id=plan_id,
            district=district,
            block=block,
            csv_path=CSV_PATH,
        )
        if not success:
            return Response(
                {"error": "Failed to build work layer."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
    except Exception as e:
        return Response(
            {"error": f"An unexpected error occurred: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    return Response({"message": "Success"}, status=status.HTTP_201_CREATED)


# MARK: SYNC OFFLINE DATA HELPER FUNCTIONS
def _get_resource_config() -> Dict[str, Dict[str, Any]]:
    """Configuration mapping for different resource types."""
    return {
        "settlement": {
            "url": ODK_SYNC_URL_SETTLEMENT,
            "success_message": "Settlement data synced successfully",
        },
        "well": {
            "url": ODK_SYNC_URL_WELL,
            "success_message": "Well data synced successfully",
        },
        "water_structures": {
            "url": ODK_SYNC_URL_WATER_STRUCTURES,
            "success_message": "Water structures data synced successfully",
        },
        "cropping_pattern": {
            "url": ODK_SYNC_URL_CROP,
            "success_message": "Cropping pattern data synced successfully",
        },
    }


def _get_work_config() -> Dict[str, Dict[str, Any]]:
    """Configuration mapping for different work types."""
    return {
        "recharge_st": {
            "url": ODK_SYNC_URL_RECHARGE_STRUCTURE,
            "success_message": "Recharge structure data synced successfully",
        },
        "irrigation_st": {
            "url": ODK_SYNC_URL_IRRIGATION_STRUCTURE,
            "success_message": "Irrigation structure data synced successfully",
        },
        "propose_maintenance_recharge_st": {
            "url": ODK_SYNC_URL_GW_MAINTENANCE,
            "success_message": "Recharge structure maintenance data synced successfully",
        },
        "propose_maintenance_rs_swb": {
            "url": ODK_SYNC_URL_RS_WATERBODY_MAINTENANCE,
            "success_message": "Surface water body maintenance data synced successfully",
        },
        "propose_maintenance_ws_swb": {
            "url": ODK_SYNC_URL_WATER_STRUCTURES_MAINTENANCE,
            "success_message": "Water structures maintenance data synced successfully",
        },
        "propose_maintenance_irrigation_st": {
            "url": ODK_SYNC_URL_AGRI_MAINTENANCE,
            "success_message": "Irrigation structure maintenance data synced successfully",
        },
        "livelihood": {
            "url": ODK_SYNC_URL_LIVELIHOOD,
            "success_message": "Livelihood data synced successfully",
        },
    }


def _get_feedback_config() -> Dict[str, Dict[str, Any]]:
    """Configuration mapping of different feedback types"""
    return {
        "gw_feedback": {
            "url": ODK_SYNC_URL_GW_FEEDBACK,
            "success_message": "Groundwater feedback data synced successfully",
        },
        "swb_feedback": {
            "url": ODK_SYNC_URL_SWB_FEEDBACK,
            "success_message": "Surface water body feedback data synced successfully",
        },
        "agri_feedback": {
            "url": ODK_SYNC_URL_AGRI_FEEDBACK,
            "success_message": "Agriculture feedback data synced successfully",
        },
    }


def _validate_sync_request(
        request, resource_type: str = None, work_type: str = None, feedback_type: str = None
) -> Optional[Response]:
    """Validate the sync request parameters and content type."""

    if not resource_type and not work_type and not feedback_type:
        return Response(
            {
                "error": "Must specify either resource_type or work_type or feedback_type"
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    if resource_type:
        valid_resources = ["settlement", "well", "water_structures", "cropping_pattern"]
        if resource_type not in valid_resources:
            return Response(
                {"error": f"Invalid resource type. Must be one of {valid_resources}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

    if work_type:
        valid_work_types = [
            "recharge_st",
            "irrigation_st",
            "propose_maintenance_recharge_st",
            "propose_maintenance_rs_swb",
            "propose_maintenance_ws_swb",
            "propose_maintenance_irrigation_st",
            "livelihood",
        ]
        if work_type not in valid_work_types:
            return Response(
                {"error": f"Invalid work type. Must be one of {valid_work_types}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

    if feedback_type:
        valid_feedback_types = ["gw_feedback", "swb_feedback", "agri_feedback"]
        if feedback_type not in valid_feedback_types:
            return Response(
                {
                    "error": f"Invalid feedback type. Must be one of {valid_feedback_types}"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

    if request.content_type != "application/xml":
        return Response(
            {"error": "Content-Type must be application/xml"},
            status=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        )

    return None


def _sync_to_odk(
        xml_string: str, config: Dict[str, Any], bearer_token: str
) -> Response:
    """Handle the actual sync to ODK for a specific resource or work type."""
    try:
        response = requests.post(
            config["url"],
            headers={
                "Content-Type": "application/xml",
                "Authorization": f"Bearer {bearer_token}",
            },
            data=xml_string,
        )
        response.raise_for_status()

        return Response(
            {
                "sync_status": True,
                "message": config["success_message"],
                "odk_response": response.json() if response.content else None,
            },
            status=status.HTTP_201_CREATED,
        )

    except requests.exceptions.RequestException as e:
        item_name = config["success_message"].split()[0].lower()
        print(f"Error syncing {item_name} data to ODK: {str(e)}")
        return Response(
            {
                "sync_status": False,
                "error": f"Failed to sync {item_name} data to ODK",
                "details": str(e),
            },
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


# MARK: SYNC OFFLINE DATA
# API to sync offline data coming from CC app
@api_view(["POST"])
@csrf_exempt
@auth_free
@schema(None)
def sync_offline_data(request, resource_type=None, work_type=None, feedback_type=None):
    """
    Sync data to ODK based on resource type or work type
    Resource types: settlement, well, water_structures, cropping_pattern
    Work types: "recharge_st", "irrigation_st", "propose_maintenance_recharge_st", "propose_maintenance_rs_swb",
                "propose_maintenance_ws_swb", "propose_maintenance_irrigation_st", "livelihood",
    Feedback types: "gw_feedback", "swb_feedback", "agri_feedback"
        - fetch Bearer Token from ODK
        - send xmlString to ODK
    """
    print(
        f"Inside sync_offline_data API for resource type: {resource_type}, work type: {work_type}, feedback type: {feedback_type}"
    )

    # Validate request
    validation_error = _validate_sync_request(
        request, resource_type, work_type, feedback_type
    )
    if validation_error:
        return validation_error

    if resource_type:
        configs = _get_resource_config()
        config = configs[resource_type]
        sync_type = f"resource type: {resource_type}"
    elif work_type:
        configs = _get_work_config()
        config = configs[work_type]
        sync_type = f"work type: {work_type}"
    elif feedback_type:
        configs = _get_feedback_config()
        config = configs[feedback_type]
        sync_type = f"feedback type: {feedback_type}"
    else:
        return Response(
            {"error": "Invalid request"}, status=status.HTTP_400_BAD_REQUEST
        )

    xml_string = request.body.decode("utf-8")
    print("Sync Type: ", sync_type)

    try:
        bearer_token = fetch_bearer_token(ODK_USER_EMAIL_SYNC, ODK_USER_PASSWORD_SYNC)
        print("Bearer Token: ", bearer_token)

        return _sync_to_odk(xml_string, config, bearer_token)

    except Exception as e:
        print("Exception in sync_offline_data api :: ", e)
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
