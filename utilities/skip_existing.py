"""
Skip dispatching a local layer task when its layer already exists for the
tehsil, unless the API request asked to regenerate (``regenerate_all``).

The API decorator opens a context with the request's tehsil. Every
``Task.apply_async`` goes through ``maybe_skip`` (installed once at startup, and
also called by the sync-mode in-process dispatcher). Tasks without a dataset
mapping (``LOCAL_TASK_DATASETS``), such as GEE tasks, always run.
"""

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from types import SimpleNamespace

from celery.app.task import Task

logger = logging.getLogger("core_stack.layer_generation")

_CONTEXT = ContextVar("skip_existing_context", default=None)
_installed = False


@contextmanager
def skip_existing_context(enabled, state, district, block):
    ctx = {
        "enabled": bool(enabled) and bool(state and district and block),
        "location": {"state": state, "district": district, "block": block},
        "skipped": [],
    }
    token = _CONTEXT.set(ctx)
    try:
        yield ctx
    finally:
        _CONTEXT.reset(token)


def maybe_skip(task):
    """Returns a stand-in result if the task's layer already exists, else None."""
    ctx = _CONTEXT.get()
    if not ctx or not ctx["enabled"]:
        return None
    from computing.bulk_layer_generation import (
        LOCAL_TASK_DATASETS,
        datasets_exist_locally,
    )

    dataset_names = LOCAL_TASK_DATASETS.get(getattr(task, "name", None))
    if not dataset_names or not datasets_exist_locally(
        dataset_names, ctx["location"]
    ):
        return None
    logger.info(
        "Skipping %s for %s: layer already exists",
        task.name,
        ctx["location"],
    )
    ctx["skipped"].append(task.name)
    return SimpleNamespace(id=None, state="SKIPPED", skipped=True)


def install():
    """Wrap Task.apply_async once so every dispatch is checked."""
    global _installed
    if _installed:
        return
    original = Task.apply_async

    def apply_async(self, *args, **kwargs):
        skipped = maybe_skip(self)
        if skipped is not None:
            return skipped
        return original(self, *args, **kwargs)

    Task.apply_async = apply_async
    _installed = True
