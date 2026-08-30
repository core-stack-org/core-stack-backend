from functools import wraps
from unittest.mock import patch
import logging
from contextvars import ContextVar

from celery.app.task import Task
from django.conf import settings

from utilities.layer_generation_logging import log_task_failure, log_task_step
from utilities.stac_spec_collector import collect_generated_stac_specs

logger = logging.getLogger("core_stack.layer_generation")
_SYNC_LAYER_GENERATION_CONTEXT = ContextVar(
    "sync_layer_generation_context",
    default=False,
)
_SYNC_STAC_LAYERS = ContextVar("sync_stac_layers", default=None)
_SYNC_LAYER_IDS = ContextVar("sync_layer_ids", default=None)
_SYNC_LAYER_GENERATED = ContextVar("sync_layer_generated", default=None)
_SYNC_STAC_ERRORS = ContextVar("sync_stac_errors", default=None)


def _sync_layer_generation_enabled():
    return bool(getattr(settings, "LAYER_GENERATION_SYNC_MODE", False))


def _get_request_value(data, *keys):
    """Read first non-empty value from request body (supports aliases and form lists)."""
    if data is None:
        return None
    for key in keys:
        val = data.get(key)
        if val is None:
            continue
        if isinstance(val, (list, tuple)):
            val = val[0] if val else None
        if val is not None and str(val).strip() != "":
            return str(val).strip()
    return None


def format_stac_for_api_response(stac_payload):
    """Return STAC Feature item(s) for the API ``stac`` field, or ``{}`` when absent."""
    if not stac_payload:
        return {}
    if isinstance(stac_payload, dict) and stac_payload.get("type") == "Feature":
        return stac_payload
    if isinstance(stac_payload, list):
        return stac_payload
    if not isinstance(stac_payload, dict):
        return {}
    items = stac_payload.get("items") or []
    if len(items) == 1:
        return items[0]
    if len(items) > 1:
        return items
    return {}


def read_location_from_request(request):
    """Resolve state/district/block from common request-body key aliases."""
    if request is None or not hasattr(request, "data"):
        return None, None, None
    data = request.data
    state = _get_request_value(data, "state", "State", "STATE")
    district = _get_request_value(data, "district", "District", "DISTRICT")
    block = _get_request_value(
        data, "block", "Block", "BLOCK", "tehsil", "Tehsil", "TEHSIL"
    )
    return state, district, block


def record_sync_stac_error(message):
    errors = _SYNC_STAC_ERRORS.get()
    if errors is not None:
        errors.append(str(message))


def record_sync_layer_id(layer_id):
    """Track a Layer row synced to GeoServer during the current sync request."""
    layer_ids = _SYNC_LAYER_IDS.get()
    if layer_ids is None or not layer_id:
        return
    layer_ids.append(layer_id)
    _SYNC_LAYER_GENERATED.set(True)


def record_sync_no_layer_generated():
    """Mark that the current sync request completed without creating a layer."""
    if _SYNC_LAYER_GENERATION_CONTEXT.get():
        _SYNC_LAYER_GENERATED.set(False)


def record_sync_stac_layer(
    *,
    state,
    district,
    block,
    layer_name,
    layer_type,
    start_year="",
    end_year="",
):
    """Track a layer whose STAC was generated during the current sync request."""
    layers = _SYNC_STAC_LAYERS.get()
    if layers is None:
        return
    layers.append(
        {
            "state": state,
            "district": district,
            "block": block,
            "layer_name": layer_name,
            "layer_type": layer_type,
            "start_year": start_year,
            "end_year": end_year,
        }
    )


def _request_layer_targets(request):
    """Build layer targets from explicit request fields (e.g. generate_stac_collection)."""
    if request is None or not hasattr(request, "data"):
        return []
    data = request.data
    state = data.get("state")
    district = data.get("district")
    block = data.get("block")
    layer_name = data.get("layer_name")
    layer_type = data.get("layer_type")
    if not all([state, district, block, layer_name, layer_type]):
        return _lulc_stac_targets_from_request(request)
    return [
        {
            "state": state,
            "district": district,
            "block": block,
            "layer_name": layer_name,
            "layer_type": layer_type,
            "start_year": data.get("start_year", "") or "",
            "end_year": data.get("end_year", "") or "",
        }
    ]


def _lulc_stac_targets_from_request(request):
    """STAC target for year-range LULC v3 clip APIs (land_use_land_cover_raster)."""
    state, district, block = read_location_from_request(request)
    if request is None or not hasattr(request, "data"):
        return []
    data = request.data
    start_year = data.get("start_year")
    end_year = data.get("end_year")
    if not all([state, district, block, start_year, end_year]):
        return []
    return [
        {
            "state": state,
            "district": district,
            "block": block,
            "layer_name": "land_use_land_cover_raster",
            "layer_type": "raster",
            "start_year": str(start_year),
            "end_year": str(end_year),
        }
    ]


def _merge_stac_targets(*target_lists):
    merged = []
    seen = set()
    for targets in target_lists:
        for target in targets or []:
            key = (
                target.get("state"),
                target.get("district"),
                target.get("block"),
                target.get("layer_name"),
                target.get("layer_type"),
                target.get("start_year"),
                target.get("end_year"),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(target)
    return merged


def _stac_targets_from_layer_ids(layer_ids):
    from computing.models import Layer
    from computing.stac_layer_resolution import stac_collect_target_for_layer

    targets = []
    seen = set()
    for layer_id in layer_ids:
        layer = Layer.objects.filter(id=layer_id).first()
        if layer is None:
            continue
        target = stac_collect_target_for_layer(layer)
        if target is None:
            continue
        key = (
            target["state"],
            target["district"],
            target["block"],
            target["layer_name"],
            target["layer_type"],
            target["start_year"],
            target["end_year"],
        )
        if key in seen:
            continue
        seen.add(key)
        targets.append(target)
    return targets


def _ensure_stac_for_sync_layer_ids(layer_ids):
    from computing.models import Layer
    from computing.STAC_specs.stac_collection import generate_stac_collection_task
    from computing.stac_layer_resolution import stac_task_kwargs_for_layer

    for layer_id in dict.fromkeys(layer_ids):
        layer = Layer.objects.filter(id=layer_id).first()
        if layer is None or layer.is_stac_specs_generated:
            continue
        task_kwargs = stac_task_kwargs_for_layer(layer)
        if task_kwargs is None:
            msg = (
                f"No STAC mapping for layer id={layer_id} "
                f"(dataset={getattr(layer.dataset, 'name', None)}, "
                f"layer_name={layer.layer_name}). "
                "Run: python manage.py load_layer_mappings"
            )
            logger.warning("Sync STAC skipped: %s", msg)
            record_sync_stac_error(msg)
            continue
        result = generate_stac_collection_task.apply(kwargs=task_kwargs)
        if getattr(result, "failed", lambda: False)():
            msg = f"STAC generation failed for layer id={layer_id}: {result.result}"
            logger.error(msg)
            record_sync_stac_error(msg)
            continue
        if not result.result:
            msg = (
                f"STAC generation returned False for layer id={layer_id} "
                f"({task_kwargs['layer_type']}/{task_kwargs['layer_name']}). "
                "Check GeoServer layer exists and GEOSERVER_URL in .env."
            )
            logger.error(msg)
            record_sync_stac_error(msg)
            continue
        record_sync_stac_layer(
            state=task_kwargs["state"],
            district=task_kwargs["district"],
            block=task_kwargs["block"],
            layer_name=task_kwargs["layer_name"],
            layer_type=task_kwargs["layer_type"],
            start_year=task_kwargs["start_year"],
            end_year=task_kwargs["end_year"],
        )


def _ensure_stac_for_layer_targets(layer_targets):
    from computing.STAC_specs.stac_collection import generate_stac_collection_task

    for target in layer_targets:
        existing = collect_generated_stac_specs(**target)
        if existing.get("items"):
            record_sync_stac_layer(
                state=target["state"],
                district=target["district"],
                block=target["block"],
                layer_name=target["layer_name"],
                layer_type=target["layer_type"],
                start_year=target.get("start_year", "") or "",
                end_year=target.get("end_year", "") or "",
            )
            continue

        task_kwargs = {
            "layer_type": target["layer_type"],
            "state": target["state"],
            "district": target["district"],
            "block": target["block"],
            "layer_name": target["layer_name"],
            "start_year": target.get("start_year", "") or "",
            "end_year": target.get("end_year", "") or "",
            "upload_to_s3": bool(getattr(settings, "STAC_UPLOAD_TO_S3", False)),
            "overwrite_metadata": bool(
                getattr(settings, "STAC_OVERWRITE_METADATA", True)
            ),
        }
        result = generate_stac_collection_task.apply(kwargs=task_kwargs)
        if getattr(result, "failed", lambda: False)():
            msg = (
                f"STAC generation failed for {target['layer_type']}/"
                f"{target['layer_name']}: {result.result}"
            )
            logger.error(msg)
            record_sync_stac_error(msg)
            continue
        if not result.result:
            msg = (
                f"STAC generation returned False for {target['layer_type']}/"
                f"{target['layer_name']}. Check GeoServer layer exists and "
                "GEOSERVER_URL in .env."
            )
            logger.error(msg)
            record_sync_stac_error(msg)
            continue
        record_sync_stac_layer(
            state=target["state"],
            district=target["district"],
            block=target["block"],
            layer_name=target["layer_name"],
            layer_type=target["layer_type"],
            start_year=target.get("start_year", "") or "",
            end_year=target.get("end_year", "") or "",
        )


def _empty_stac_spec(state, district, block):
    spec = collect_generated_stac_specs(
        state=state,
        district=district,
        block=block,
        layer_name="__none__",
        layer_type="vector",
    )
    spec["items"] = []
    spec["stac_status"] = "not_generated_yet"
    return spec


def _collect_stac_for_request(request):
    layer_targets = list(_SYNC_STAC_LAYERS.get() or [])
    layer_ids = list(_SYNC_LAYER_IDS.get() or [])

    if not layer_targets and layer_ids:
        _ensure_stac_for_sync_layer_ids(layer_ids)
        layer_targets = _stac_targets_from_layer_ids(layer_ids)

    layer_targets = _merge_stac_targets(
        layer_targets,
        _request_layer_targets(request),
        _lulc_stac_targets_from_request(request),
    )

    if layer_targets:
        _ensure_stac_for_layer_targets(layer_targets)

    if not layer_targets:
        state, district, block = read_location_from_request(request)
        if all([state, district, block]):
            if layer_ids:
                record_sync_stac_error(
                    "Layer synced but STAC mapping was not resolved "
                    "(run python manage.py load_layer_mappings)."
                )
            else:
                record_sync_stac_error(
                    "Layer was not marked synced to GeoServer (GeoServer publish may have failed)."
                )
            spec = _empty_stac_spec(state, district, block)
            errors = list(_SYNC_STAC_ERRORS.get() or [])
            if errors:
                spec["stac_errors"] = errors
            return spec
        return None

    merged_items = []
    merged_spec = None
    for target in layer_targets:
        spec = collect_generated_stac_specs(**target)
        if merged_spec is None:
            merged_spec = spec
        merged_items.extend(spec.get("items") or [])

    if merged_spec is None:
        return None

    has_stac_data = bool(merged_items)
    merged_spec["items"] = merged_items
    merged_spec["stac_status"] = "available" if has_stac_data else "not_generated_yet"
    if not has_stac_data and layer_targets:
        record_sync_stac_error(
            "STAC items not found on disk after generation. "
            "Verify GEOSERVER_URL points at the GeoServer where the layer was published."
        )
    errors = list(_SYNC_STAC_ERRORS.get() or [])
    if errors:
        merged_spec["stac_errors"] = errors
    if layer_targets:
        merged_spec["stac_layers"] = layer_targets
    return merged_spec


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
        stac_layers_token = _SYNC_STAC_LAYERS.set([])
        layer_ids_token = _SYNC_LAYER_IDS.set([])
        layer_generated_token = _SYNC_LAYER_GENERATED.set(None)
        stac_errors_token = _SYNC_STAC_ERRORS.set([])
        try:
            with patch.object(Task, "apply_async", _apply_async_in_process):
                response = view_func(*args, **kwargs)

            try:
                payload = getattr(response, "data", None)
                if isinstance(payload, dict):
                    if _SYNC_LAYER_GENERATED.get() is False:
                        payload.pop("asset_id", None)
                        payload.pop("asset_ids", None)
                        payload["layer_generated"] = False
                    if "stac" not in payload:
                        stac_spec = _collect_stac_for_request(request)
                        payload["stac"] = format_stac_for_api_response(stac_spec)
                        if stac_spec is not None and stac_spec.get("stac_errors"):
                            payload["stac_errors"] = stac_spec["stac_errors"]
                        payload.pop("stac_spec", None)
                    if payload.get("status") == "initiated":
                        payload["status"] = "completed"
                        payload["Success"] = "Layer generation completed"
                        payload["message"] = "Layer generation completed"
            except Exception:
                logger.exception("Failed to enrich sync response with STAC specs")
        finally:
            _SYNC_STAC_ERRORS.reset(stac_errors_token)
            _SYNC_LAYER_GENERATED.reset(layer_generated_token)
            _SYNC_LAYER_IDS.reset(layer_ids_token)
            _SYNC_STAC_LAYERS.reset(stac_layers_token)
            _SYNC_LAYER_GENERATION_CONTEXT.reset(token)

        return response

    return wrapper
