from rest_framework.decorators import api_view, parser_classes
from rest_framework.response import Response
from rest_framework import status, renderers
from rest_framework.parsers import JSONParser
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from .models import Plan
from .serializers import PlanSerializer

from .build_layer import build_layer
from .utils import fetch_odk_data, fetch_bearer_token
import requests

# MARK: Get Plans API
@api_view(["GET"])
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


# MARK: SYNC OFFLINE DATA
# API to sync offline data coming from CC app
@api_view(["POST"])
@csrf_exempt
def sync_offline_data(request, resource_type=None):
    """
    Sync data to ODK based on resource type (settlement, well, water_structures)
        - fetch Bearer Token from ODK
        - send xmlString to ODK
    """
    print(f"Inside sync_offline_data API for resource type: {resource_type}")

    # Validating resource type
    valid_resources = ["settlement", "well", "water_structures"]
    if resource_type not in valid_resources:
        return Response(
            {"error": f"Invalid resource type. Must be one of {valid_resources}"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    if request.content_type != "application/xml":
        return Response(
            {"error": "Content-Type must be application/xml"},
            status=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        )

    xml_string = request.body.decode("utf-8")
    print("XML String: ", xml_string)
    print("Resource Type: ", resource_type)

    # ODK_USER_EMAIL = "ankit.kumar@oniondev.com"
    # ODK_USER_PASSWORD = "offlineNRM@22"

    ODK_USER_EMAIL = "sukriti.kumari@oniondev.com"
    ODK_USER_PASSWORD = "sukriti@123"

    try:
        bearer_token = fetch_bearer_token(ODK_USER_EMAIL, ODK_USER_PASSWORD)
        print("Bearer Token: ", bearer_token)

        # Handle different resource types
        if resource_type == "settlement":
            ODK_SYNC_URL_SETTLEMENT = "https://odk.gramvaani.org/v1/projects/9/forms/Add_Settlements_form%20_V1.0.1/submissions"

            try:
                response = requests.post(
                    ODK_SYNC_URL_SETTLEMENT,
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
                        "message": "Settlement data synced successfully",
                        "odk_response": response.json() if response.content else None,
                    },
                    status=status.HTTP_201_CREATED,
                )

            except requests.exceptions.RequestException as e:
                print(f"Error syncing settlement data to ODK: {str(e)}")
                return Response(
                    {
                        "sync_status": False,
                        "error": "Failed to sync settlement data to ODK",
                        "details": str(e),
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

        elif resource_type == "well":
            ODK_SYNC_URL_WELL = "https://odk.gramvaani.org/v1/projects/9/forms/Add_well_form_V1.0.1/submissions"

            try:
                response = requests.post(
                    ODK_SYNC_URL_WELL,
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
                        "message": "Settlement data synced successfully",
                        "odk_response": response.json() if response.content else None,
                    },
                    status=status.HTTP_201_CREATED,
                )

            except requests.exceptions.RequestException as e:
                print(f"Error syncing settlement data to ODK: {str(e)}")
                return Response(
                    {
                        "sync_status": False,
                        "error": "Failed to sync settlement data to ODK",
                        "details": str(e),
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

        elif resource_type == "water_structures":
            ODK_SYNC_URL_WATER_STRUCTURES = "https://odk.gramvaani.org/v1/projects/9/forms/Add_Waterbodies_Form_V1.0.3/submissions"

            try:
                response = requests.post(
                    ODK_SYNC_URL_WATER_STRUCTURES,
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
                        "message": "Settlement data synced successfully",
                        "odk_response": response.json() if response.content else None,
                    },
                    status=status.HTTP_201_CREATED,
                )

            except requests.exceptions.RequestException as e:
                print(f"Error syncing settlement data to ODK: {str(e)}")
                return Response(
                    {
                        "sync_status": False,
                        "error": "Failed to sync settlement data to ODK",
                        "details": str(e),
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

    except Exception as e:
        print("Exception in sync_offline_data api :: ", e)
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
