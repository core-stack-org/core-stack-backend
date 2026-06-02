from functools import wraps
from unittest.mock import patch
import logging
import os
import json
import re
from contextvars import ContextVar

from celery.app.task import Task
from django.conf import settings

from utilities.layer_generation_logging import log_task_failure, log_task_step

logger = logging.getLogger("core_stack.layer_generation")
_SYNC_LAYER_GENERATION_CONTEXT = ContextVar(
    "sync_layer_generation_context",
    default=False,
)


def _sync_layer_generation_enabled():
    return bool(getattr(settings, "LAYER_GENERATION_SYNC_MODE", False))


def _sanitize_text(text):
    text = re.sub(r"[^a-zA-Z0-9 .,:;_-]", "", text)
    return text.replace(" ", "_")


def _read_json(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def _collect_stac_for_request(request):
    if request is None or not hasattr(request, "data"):
        return None
    state = request.data.get("state")
    district = request.data.get("district")
    block = request.data.get("block")
    if not all([state, district, block]):
        return None

    state = _sanitize_text(str(state).lower())
    district = _sanitize_text(str(district).lower())
    block = _sanitize_text(str(block).lower())

    # Use the same directory STAC generation writes to (BASE_DIR/data/STAC_specs/...),
    # not settings.DATA_DIR which may point to a different mount in Docker.
    from computing.STAC_specs.stac_collection import STACConfig

    stac_config = STACConfig()
    stac_root = stac_config.stac_files_dir
    tehsil_dir = os.path.join(stac_root, stac_config.tehsil_dirname)
    state_dir = os.path.join(tehsil_dir, state)
    district_dir = os.path.join(state_dir, district)
    block_dir = os.path.join(district_dir, block)

    items = []
    if os.path.isdir(block_dir):
        for entry in sorted(os.listdir(block_dir)):
            item_dir = os.path.join(block_dir, entry)
            if not os.path.isdir(item_dir):
                continue
            item_json = os.path.join(item_dir, f"{entry}.json")
            item_spec = _read_json(item_json)
            if item_spec is not None:
                items.append(item_spec)

    root_catalog = _read_json(os.path.join(stac_root, "catalog.json"))
    tehsil_catalog = _read_json(os.path.join(tehsil_dir, "catalog.json"))
    state_collection = _read_json(os.path.join(state_dir, "collection.json"))
    district_collection = _read_json(os.path.join(district_dir, "collection.json"))
    block_collection = _read_json(os.path.join(block_dir, "collection.json"))

    has_stac_data = bool(block_collection) or bool(items)

    return {
        "stac_status": "available" if has_stac_data else "not_generated_yet",
        "root_catalog": root_catalog,
        "tehsil_catalog": tehsil_catalog,
        "state_collection": state_collection,
        "district_collection": district_collection,
        "block_collection": block_collection,
        "items": items,
    }


def _read_request_mode(request):
    if request is None or not hasattr(request, "data"):
        return None
    data = request.data
    for key in ("layer_generation_mode", "layerGenerationMode", "layer_mode", "mode"):
        value = data.get(key)
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            value = value[0] if value else None
        if value is None:
            continue
        normalized = str(value).strip().lower()
        if normalized:
            return normalized
    return None


def is_sync_layer_generation_request(request=None):
    request_mode = _read_request_mode(request)
    if request_mode is not None:
        return request_mode == "sync"
    return _sync_layer_generation_enabled()


def is_sync_layer_generation_context_active():
    return bool(_SYNC_LAYER_GENERATION_CONTEXT.get())


def _apply_async_in_process(
    task_self,
    args=None,
    kwargs=None,
    task_id=None,
    producer=None,
    link=None,
    link_error=None,
    shadow=None,
    **options,
):
    """
    Drop-in replacement for Task.apply_async that executes the task immediately.
    """
    task_args = args or ()
    task_kwargs = kwargs or {}
    task_name = getattr(task_self, "name", "unknown_task")
    log_task_step(
        task_name,
        "sync_execute_start",
        args=task_args,
        kwargs=task_kwargs,
    )

    eager_result = task_self.apply(
        args=task_args,
        kwargs=task_kwargs,
        task_id=task_id,
        link=link,
        link_error=link_error,
        **options,
    )

    if hasattr(eager_result, "failed") and eager_result.failed():
        err = getattr(eager_result, "result", "Unknown task failure")
        tb = getattr(eager_result, "traceback", None)
        log_task_failure(
            task_name,
            err if isinstance(err, BaseException) else RuntimeError(str(err)),
            args=task_args,
            kwargs=task_kwargs,
            sync_mode=True,
        )
        if tb:
            logger.error(
                "Layer task traceback (sync) | task=%s\n%s",
                task_name,
                tb,
            )
        if isinstance(err, BaseException):
            raise RuntimeError(f"{task_name} failed: {err}") from err
        raise RuntimeError(f"{task_name} failed: {err}")

    if hasattr(eager_result, "result") and eager_result.result is False:
        log_task_failure(
            task_name,
            RuntimeError("task returned False"),
            args=task_args,
            kwargs=task_kwargs,
            sync_mode=True,
        )
        raise RuntimeError(f"{task_name} returned False")

    log_task_step(task_name, "sync_execute_complete", result=eager_result.result)
    return eager_result


def sync_layer_generation_if_enabled(view_func):
    """
    Run Celery task dispatches synchronously for this view when the
    LAYER_GENERATION_SYNC_MODE setting is enabled.
    """

    @wraps(view_func)
    def wrapper(*args, **kwargs):
        request = args[0] if args else kwargs.get("request")
        if not is_sync_layer_generation_request(request):
            logger.debug(
                "Layer generation mode=async for view=%s",
                getattr(view_func, "__name__", "unknown"),
            )
            return view_func(*args, **kwargs)
        logger.info(
            "Layer generation mode=sync for view=%s",
            getattr(view_func, "__name__", "unknown"),
        )
        token = _SYNC_LAYER_GENERATION_CONTEXT.set(True)
        try:
            with patch.object(Task, "apply_async", _apply_async_in_process):
                response = view_func(*args, **kwargs)
        finally:
            _SYNC_LAYER_GENERATION_CONTEXT.reset(token)

        try:
            payload = getattr(response, "data", None)
            if isinstance(payload, dict):
                stac_spec = _collect_stac_for_request(request)
                if stac_spec is not None:
                    # Always return existing/generated STAC JSON for this block,
                    # even when is_stac_specs_generated was already True and
                    # the signal skipped re-generation.
                    payload["stac_spec"] = stac_spec
        except Exception:
            logger.exception("Failed to enrich sync response with STAC specs")

        return response

    return wrapper
