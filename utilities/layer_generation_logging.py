"""
Structured logging for layer-generation HTTP APIs and Celery tasks.
"""

import logging
import traceback
from functools import wraps

logger = logging.getLogger("core_stack.layer_generation")

LAYER_REQUEST_FIELDS = (
    "state",
    "district",
    "block",
    "gee_account_id",
    "workspace",
    "layer_name",
    "layer_id",
    "asset_id",
)


def extract_request_context(request):
    """Best-effort extraction of common layer-generation params from a DRF request."""
    context = {}
    if request is None:
        return context
    try:
        body = getattr(request, "data", None) or {}
        for key in LAYER_REQUEST_FIELDS:
            if key in body:
                context[key] = body.get(key)
        query_params = getattr(request, "query_params", None)
        if query_params:
            for key in LAYER_REQUEST_FIELDS:
                if key in query_params:
                    context[key] = query_params.get(key)
    except Exception:
        pass
    return context


def log_layer_api_start(view_name, request):
    logger.info(
        "Layer API start | api=%s context=%s",
        view_name,
        extract_request_context(request),
    )


def log_layer_api_failure(view_name, exc, request=None, extra=None):
    logger.error(
        "Layer API failed | api=%s context=%s extra=%s error=%s",
        view_name,
        extract_request_context(request),
        extra or {},
        exc,
        exc_info=exc,
    )


def format_api_error_payload(view_name, exc):
    """Build a JSON-serializable error body for API clients and operators."""
    payload = {
        "error": str(exc),
        "api": view_name,
        "exception_type": type(exc).__name__,
    }
    cause = getattr(exc, "__cause__", None) or getattr(exc, "__context__", None)
    if cause:
        payload["caused_by"] = f"{type(cause).__name__}: {cause}"
    tb = traceback.format_exc()
    if tb and tb.strip() not in ("NoneType: None", "NoneType: None\n"):
        lines = [line for line in tb.strip().splitlines() if line.strip()]
        if lines:
            payload["traceback_tail"] = "\n".join(lines[-8:])
    return payload


def layer_api_error_response(view_name, exc, request=None):
    """Log failure and return a DRF Response with troubleshooting fields."""
    from rest_framework import status
    from rest_framework.response import Response

    log_layer_api_failure(view_name, exc, request=request)
    return Response(
        format_api_error_payload(view_name, exc),
        status=status.HTTP_500_INTERNAL_SERVER_ERROR,
    )


def layer_generation_api_logging(view_func):
    """
    Log API entry, unhandled exceptions (with traceback), and 5xx responses
    returned from inner try/except blocks.
    """

    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        view_name = view_func.__name__
        log_layer_api_start(view_name, request)
        try:
            response = view_func(request, *args, **kwargs)
        except Exception as exc:
            return layer_api_error_response(view_name, exc, request=request)

        status_code = getattr(response, "status_code", None)
        if status_code is not None and status_code >= 500:
            logger.error(
                "Layer API returned %s | api=%s context=%s body=%s",
                status_code,
                view_name,
                extract_request_context(request),
                getattr(response, "data", None),
            )
        return response

    return wrapper


def log_task_step(task_name, step, **context):
    logger.info(
        "Layer task step | task=%s step=%s context=%s",
        task_name,
        step,
        context,
    )


def log_task_failure(task_name, exc, **context):
    logger.error(
        "Layer task failed | task=%s context=%s error=%s",
        task_name,
        context,
        exc,
        exc_info=exc,
    )


def task_location_context(state=None, district=None, block=None, **extra):
    ctx = {}
    if state is not None:
        ctx["state"] = state
    if district is not None:
        ctx["district"] = district
    if block is not None:
        ctx["block"] = block
    ctx.update(extra)
    return ctx
