from functools import wraps
from unittest.mock import patch
import logging

from celery.app.task import Task
from django.conf import settings

logger = logging.getLogger(__name__)


def _sync_layer_generation_enabled():
    return bool(getattr(settings, "LAYER_GENERATION_SYNC_MODE", False))


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
    eager_result = task_self.apply(
        args=args or (),
        kwargs=kwargs or {},
        task_id=task_id,
        link=link,
        link_error=link_error,
        **options,
    )
    task_name = getattr(task_self, "name", "unknown_task")

    if hasattr(eager_result, "failed") and eager_result.failed():
        err = getattr(eager_result, "result", "Unknown task failure")
        raise RuntimeError(f"{task_name} failed: {err}")

    if hasattr(eager_result, "result") and eager_result.result is False:
        raise RuntimeError(f"{task_name} returned False")

    return eager_result


def sync_layer_generation_if_enabled(view_func):
    """
    Run Celery task dispatches synchronously for this view when the
    LAYER_GENERATION_SYNC_MODE setting is enabled.
    """

    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not _sync_layer_generation_enabled():
            logger.debug(
                "Layer generation mode=async for view=%s",
                getattr(view_func, "__name__", "unknown"),
            )
            return view_func(*args, **kwargs)
        logger.info(
            "Layer generation mode=sync for view=%s",
            getattr(view_func, "__name__", "unknown"),
        )
        with patch.object(Task, "apply_async", _apply_async_in_process):
            return view_func(*args, **kwargs)

    return wrapper
