from functools import wraps
from unittest.mock import patch
import logging

from celery.app.task import Task
from django.conf import settings

from utilities.layer_generation_logging import log_task_failure, log_task_step

logger = logging.getLogger("core_stack.layer_generation")


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
