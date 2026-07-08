import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import requests
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status
from rest_framework.decorators import api_view, schema
from rest_framework.response import Response

from dpr.utils import transform_name
from moderation.utils.update_csdb import sync_form_type
from nrm_app.settings import ODK_USER_EMAIL_SYNC, ODK_USER_PASSWORD_SYNC, TMP_LOCATION
from utilities.auth_check_decorator import api_security_check
from utilities.auth_utils import auth_free
from utilities.constants import (
    ODK_SYNC_URL_AGRI_FEEDBACK,
    ODK_SYNC_URL_AGRI_MAINTENANCE,
    ODK_SYNC_URL_AGROHORTICULTURE,
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

logger = logging.getLogger(__name__)

from .build_layer import build_layer
from .models import ODKSyncLog, PlanApp, Plan
from .serializers import PlanAppSerializer
from .utils import fetch_bearer_token, fetch_db_data
from geoadmin.models import GramPanchayat
from django.db.models import Q

_COMMON_REQUIRED_FIELDS: Tuple[str, ...] = (
    "layer_name",
    "plan_id",
    "plan_name",
    "district_name",
    "block_name",
)

_LAYER_KIND_CONFIG: Dict[str, Dict[str, str]] = {
    "resources": {"type_field": "resource_type", "singular": "resource"},
    "works": {"type_field": "work_type", "singular": "work"},
}


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
        serializer = PlanAppSerializer(plans, many=True)
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
        serializer = PlanAppSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()  # Save the new Plan instance if validation passes
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    return Response(
        {"error": "Method not allowed"}, status=status.HTTP_405_METHOD_NOT_ALLOWED
    )


# MARK: Build Layer Helpers (shared by /add_resources and /add_works)
def _extract_payload(request, kind: str) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """Pull and normalize the request payload. Returns (payload, missing_fields)."""
    type_field = _LAYER_KIND_CONFIG[kind]["type_field"]
    required = (*_COMMON_REQUIRED_FIELDS, type_field)

    missing = [f for f in required if not request.data.get(f)]
    if missing:
        return None, missing

    def _lower(value: Any) -> Any:
        return value.lower() if isinstance(value, str) else value

    return {
        "layer_name": _lower(request.data.get("layer_name")),
        "item_type": _lower(request.data.get(type_field)),
        "plan_id": request.data.get("plan_id"),
        "plan_name": _lower(request.data.get("plan_name")),
        "district": _lower(request.data.get("district_name")),
        "block": _lower(request.data.get("block_name")),
    }, []


def _expected_layer_store_name(
    item_type: str, plan_id: Any, district: str, block: str
) -> str:
    """Mirror the naming convention used by build_layer.build_layer for transparency."""
    return f"{item_type}_{plan_id}_{district}_{transform_name(name=block)}"


def _safe_unlink(csv_path: str, request_id: str, kind: str) -> None:
    try:
        if os.path.exists(csv_path):
            os.remove(csv_path)
            logger.info(
                f"[{request_id}] {kind}.build: cleaned up temp CSV at {csv_path}"
            )
    except OSError as exc:
        logger.warning(
            f"[{request_id}] {kind}.build: failed to remove temp CSV at "
            f"{csv_path}: {exc}"
        )


def _error_response(
    request_id: str,
    code: str,
    message: str,
    http_status: int,
    extra: Optional[Dict[str, Any]] = None,
) -> Response:
    data: Dict[str, Any] = {"request_id": request_id}
    if extra:
        data.update(extra)
    return Response(
        {
            "status": "error",
            "code": code,
            "error": message,
            "data": data,
        },
        status=http_status,
    )


def _build_layer_for_kind(request, kind: str) -> Response:
    """
    Shared workflow for /add_resources and /add_works:
      1. validate payload
      2. trigger incremental ODK -> DB sync (best-effort)
      3. fetch source records from DB and stage a CSV
      4. publish the layer to GeoServer
      5. clean up the temp CSV and return a structured response
    """
    request_id = uuid.uuid4().hex[:12]
    type_field = _LAYER_KIND_CONFIG[kind]["type_field"]
    singular = _LAYER_KIND_CONFIG[kind]["singular"]
    started_at = time.perf_counter()

    logger.info(
        f"[{request_id}] {kind}.build: request received "
        f"(content_type={request.content_type}, keys={list(request.data.keys())})"
    )

    payload, missing = _extract_payload(request, kind)
    if missing:
        logger.warning(
            f"[{request_id}] {kind}.build: rejecting request — "
            f"missing/empty fields: {missing}"
        )
        return _error_response(
            request_id,
            code="missing_fields",
            message=f"Missing required field(s): {', '.join(missing)}.",
            http_status=status.HTTP_400_BAD_REQUEST,
            extra={"missing_fields": missing},
        )

    item_type = payload["item_type"]
    plan_id = payload["plan_id"]
    plan_name = payload["plan_name"]
    district = payload["district"]
    block = payload["block"]
    layer_name = payload["layer_name"]

    context = {
        type_field: item_type,
        "plan_id": plan_id,
        "plan_name": plan_name,
        "district": district,
        "block": block,
        "layer_name": layer_name,
    }
    logger.info(f"[{request_id}] {kind}.build: payload normalized — {context}")

    csv_path = os.path.join(TMP_LOCATION, f"{item_type}_{plan_id}_{block}.csv")
    logger.info(f"[{request_id}] {kind}.build: temp CSV path resolved to {csv_path}")

    sync_started = time.perf_counter()
    logger.info(
        f"[{request_id}] {kind}.build: triggering incremental ODK sync for "
        f"{type_field}={item_type}"
    )
    sync_ok = sync_form_type(item_type)
    sync_ms = int((time.perf_counter() - sync_started) * 1000)
    if sync_ok:
        logger.info(
            f"[{request_id}] {kind}.build: ODK sync completed for "
            f"{type_field}={item_type} in {sync_ms}ms"
        )
    else:
        logger.warning(
            f"[{request_id}] {kind}.build: ODK sync FAILED for "
            f"{type_field}={item_type} in {sync_ms}ms; proceeding with existing DB data"
        )

    fetch_started = time.perf_counter()
    logger.info(
        f"[{request_id}] {kind}.build: fetching DB data for {type_field}={item_type}, "
        f"plan_id={plan_id}, block={block}"
    )
    try:
        record_count = fetch_db_data(csv_path, item_type, block, plan_id)
    except Exception as exc:
        fetch_ms = int((time.perf_counter() - fetch_started) * 1000)
        logger.exception(
            f"[{request_id}] {kind}.build: unexpected error during fetch_db_data "
            f"for {type_field}={item_type}, plan_id={plan_id} "
            f"(fetch_ms={fetch_ms}): {exc}"
        )
        _safe_unlink(csv_path, request_id, kind)
        return _error_response(
            request_id,
            code="db_fetch_failed",
            message="Failed to fetch source data from the database.",
            http_status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            extra={
                **context,
                "details": str(exc),
                "sync_status": "success" if sync_ok else "failed",
                "sync_duration_ms": sync_ms,
                "fetch_duration_ms": fetch_ms,
            },
        )
    fetch_ms = int((time.perf_counter() - fetch_started) * 1000)

    if not record_count:
        total_ms = int((time.perf_counter() - started_at) * 1000)
        logger.warning(
            f"[{request_id}] {kind}.build: no DB data found for "
            f"{type_field}={item_type}, plan_id={plan_id}, block={block} "
            f"(sync_ok={sync_ok}, fetch_ms={fetch_ms}, total_ms={total_ms})"
        )
        return _error_response(
            request_id,
            code="no_data_found",
            message=(
                f"No records found for {type_field}='{item_type}', "
                f"plan_id='{plan_id}', block='{block}'."
            ),
            http_status=status.HTTP_404_NOT_FOUND,
            extra={
                **context,
                "record_count": 0,
                "sync_status": "success" if sync_ok else "failed",
                "sync_duration_ms": sync_ms,
                "fetch_duration_ms": fetch_ms,
                "total_duration_ms": total_ms,
            },
        )
    logger.info(
        f"[{request_id}] {kind}.build: DB fetch staged {record_count} row(s) "
        f"in {fetch_ms}ms"
    )

    layer_store_name = _expected_layer_store_name(item_type, plan_id, district, block)
    build_started = time.perf_counter()
    logger.info(
        f"[{request_id}] {kind}.build: publishing GeoServer layer "
        f"workspace='{kind}', store='{layer_store_name}'"
    )
    try:
        success = build_layer(
            layer_type=kind,
            item_type=item_type,
            plan_id=plan_id,
            district=district,
            block=block,
            csv_path=csv_path,
        )
    except Exception as exc:
        build_ms = int((time.perf_counter() - build_started) * 1000)
        total_ms = int((time.perf_counter() - started_at) * 1000)
        logger.exception(
            f"[{request_id}] {kind}.build: unexpected error during build_layer for "
            f"{type_field}={item_type}, plan_id={plan_id} "
            f"(build_ms={build_ms}, total_ms={total_ms}): {exc}"
        )
        _safe_unlink(csv_path, request_id, kind)
        return _error_response(
            request_id,
            code="internal_error",
            message="An unexpected error occurred while building the layer.",
            http_status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            extra={
                **context,
                "layer_store_name": layer_store_name,
                "details": str(exc),
                "sync_status": "success" if sync_ok else "failed",
                "sync_duration_ms": sync_ms,
                "fetch_duration_ms": fetch_ms,
                "build_duration_ms": build_ms,
                "total_duration_ms": total_ms,
            },
        )
    finally:
        _safe_unlink(csv_path, request_id, kind)

    build_ms = int((time.perf_counter() - build_started) * 1000)
    total_ms = int((time.perf_counter() - started_at) * 1000)

    if not success:
        logger.error(
            f"[{request_id}] {kind}.build: build_layer returned False for "
            f"{type_field}={item_type}, plan_id={plan_id} "
            f"(build_ms={build_ms}, total_ms={total_ms})"
        )
        return _error_response(
            request_id,
            code="layer_build_failed",
            message=(
                f"Failed to publish GeoServer layer '{layer_store_name}'. "
                "See server logs for details."
            ),
            http_status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            extra={
                **context,
                "layer_store_name": layer_store_name,
                "record_count": record_count,
                "sync_status": "success" if sync_ok else "failed",
                "sync_duration_ms": sync_ms,
                "fetch_duration_ms": fetch_ms,
                "build_duration_ms": build_ms,
                "total_duration_ms": total_ms,
            },
        )

    logger.info(
        f"[{request_id}] {kind}.build: SUCCESS — published layer "
        f"'{layer_store_name}' ({record_count} row(s)) in workspace='{kind}' "
        f"(sync={sync_ms}ms, fetch={fetch_ms}ms, build={build_ms}ms, total={total_ms}ms)"
    )
    return Response(
        {
            "status": "success",
            "code": "layer_published",
            "message": (
                f"Successfully published {singular} layer "
                f"'{layer_store_name}' to GeoServer with {record_count} record(s)."
            ),
            "data": {
                "request_id": request_id,
                "layer_type": kind,
                "workspace": kind,
                "layer_store_name": layer_store_name,
                "record_count": record_count,
                **context,
                "sync_status": "success" if sync_ok else "failed",
                "sync_duration_ms": sync_ms,
                "fetch_duration_ms": fetch_ms,
                "build_duration_ms": build_ms,
                "total_duration_ms": total_ms,
            },
        },
        status=status.HTTP_201_CREATED,
    )


# api's for add settlement, add well, add waterbody | add work [new, maintenance]
@api_view(["POST"])
@auth_free
@schema(None)
def add_resources(request):
    """
    Build and publish a GeoServer 'resources' layer for the given plan/block.

    Supported resource_type values: settlement, well, waterbody, cropping.
    Layer naming convention: <resource_type>_<plan_id>_<district>_<block>.
    """
    return _build_layer_for_kind(request, kind="resources")


@api_view(["POST"])
@auth_free
@schema(None)
def add_works(request):
    """
    Build and publish a GeoServer 'works' layer for the given plan/block.

    Supported work_type values:
      plan_gw           — new recharge structures (groundwater)
      main_gw           — maintenance of recharge structures
      plan_agri         — new irrigation structures
      main_agri         — maintenance of irrigation structures
      main_swb          — surface water body maintenance
      main_swb_rs       — remote-sensed surface water body maintenance
      livelihood        — livelihood
      agrohorticulture  — agrohorticulture

    Layer naming convention: <work_type>_<plan_id>_<district>_<block>.
    """
    return _build_layer_for_kind(request, kind="works")


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
        "agrohorticulture": {
            "url": ODK_SYNC_URL_AGROHORTICULTURE,
            "success_message": "Agrohorticulture data synced successfully",
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
            "agrohorticulture",
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
    xml_string: str,
    config: Dict[str, Any],
    bearer_token: str,
    category: str,
    sync_type: str,
) -> Response:
    """Handle the actual sync to ODK for a specific resource or work type."""
    sync_log = ODKSyncLog.objects.create(
        category=category,
        sync_type=sync_type,
        xml_content=xml_string,
        odk_url=config["url"],
        status=ODKSyncLog.SyncStatus.PENDING,
    )

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

        odk_response = response.json() if response.content else None
        sync_log.status = ODKSyncLog.SyncStatus.SUCCESS
        sync_log.odk_response = odk_response
        sync_log.save(update_fields=["status", "odk_response"])

        return Response(
            {
                "sync_status": True,
                "message": config["success_message"],
                "odk_response": odk_response,
            },
            status=status.HTTP_201_CREATED,
        )

    except requests.exceptions.RequestException as e:
        item_name = config["success_message"].split()[0].lower()
        print(f"Error syncing {item_name} data to ODK: {str(e)}")

        sync_log.status = ODKSyncLog.SyncStatus.FAILED
        sync_log.error_details = str(e)
        sync_log.save(update_fields=["status", "error_details"])

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
        category = ODKSyncLog.SyncCategory.RESOURCE
        item_type = resource_type
    elif work_type:
        configs = _get_work_config()
        config = configs[work_type]
        category = ODKSyncLog.SyncCategory.WORK
        item_type = work_type
    elif feedback_type:
        configs = _get_feedback_config()
        config = configs[feedback_type]
        category = ODKSyncLog.SyncCategory.FEEDBACK
        item_type = feedback_type
    else:
        return Response(
            {"error": "Invalid request"}, status=status.HTTP_400_BAD_REQUEST
        )

    xml_string = request.body.decode("utf-8")
    print(f"Sync Category: {category}, Type: {item_type}")

    try:
        bearer_token = fetch_bearer_token(ODK_USER_EMAIL_SYNC, ODK_USER_PASSWORD_SYNC)
        print("Bearer Token: ", bearer_token)

        return _sync_to_odk(xml_string, config, bearer_token, category, item_type)

    except Exception as e:
        print("Exception in sync_offline_data api :: ", e)
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# map plan to gp api
@api_view(["PATCh"])
@schema(None)
def map_plan_to_gp(request):

    plan_id = request.data.get("plan_id")
    gp_id = request.data.get("gp_id")

    if not plan_id or not gp_id:
        return Response(
            {
                "success": False,
                "message": "plan_id and gp_id are required",
            },
            status=400,
        )

    try:
        plan = PlanApp.objects.get(id=plan_id)

    except PlanApp.DoesNotExist:
        return Response(
            {
                "success": False,
                "message": "Plan not found",
            },
            status=404,
        )

    try:
        gp = GramPanchayat.objects.get(gram_panchayat_code=gp_id)

    except GramPanchayat.DoesNotExist:
        return Response(
            {
                "success": False,
                "message": "Gram Panchayat not found",
            },
            status=404,
        )

    # GP should belong to same tehsil

    if plan.tehsil_soi_id != gp.tehsil_id:
        return Response(
            {
                "success": False,
                "message": "Selected GP does not belong to plan tehsil",
            },
            status=400,
        )

    plan.gp = gp
    plan.updated_by = request.user

    plan.save(update_fields=["gp", "updated_by", "updated_at"])

    return Response(
        {
            "success": True,
            "message": "Plan mapped with GP successfully",
            "data": {
                "plan_id": plan.id,
                "gp_id": gp.gram_panchayat_code,
                "gp_name": gp.gram_panchayat_name,
            },
        }
    )
