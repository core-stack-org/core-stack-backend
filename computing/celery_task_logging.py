"""
Celery signal handlers for layer-generation task observability.
"""

import logging

from celery.signals import (
    before_task_publish,
    task_failure,
    task_postrun,
    task_prerun,
)

from utilities import geoserver_sync_gate

logger = logging.getLogger("core_stack.layer_generation")


@task_prerun.connect
def log_layer_task_prerun(
    sender=None, task_id=None, task=None, args=None, kwargs=None, **extra
):
    logger.info(
        "Celery task start | name=%s id=%s args=%s kwargs=%s",
        getattr(sender, "name", sender),
        task_id,
        args,
        kwargs,
    )


@task_postrun.connect
def log_layer_task_postrun(
    sender=None,
    task_id=None,
    task=None,
    args=None,
    kwargs=None,
    retval=None,
    state=None,
    **extra,
):
    logger.info(
        "Celery task finished | name=%s id=%s state=%s retval=%s",
        getattr(sender, "name", sender),
        task_id,
        state,
        retval,
    )


@task_failure.connect
def log_layer_task_failure(
    sender=None,
    task_id=None,
    exception=None,
    args=None,
    kwargs=None,
    traceback=None,
    einfo=None,
    **extra,
):
    tb = None
    if einfo is not None:
        tb = getattr(einfo, "traceback", None)
    if not tb:
        tb = traceback
    logger.error(
        "Celery task failed | name=%s id=%s args=%s kwargs=%s exception=%s\n%s",
        getattr(sender, "name", sender),
        task_id,
        args,
        kwargs,
        exception,
        tb or "",
    )


_GATE_TOKENS = {}


@before_task_publish.connect
def propagate_geoserver_sync_gate(headers=None, **extra):
    """Carry the GeoServer sync decision from the publisher to the worker."""
    ctx = geoserver_sync_gate.get_context()
    if ctx is not None and headers is not None:
        headers[geoserver_sync_gate.CELERY_HEADER] = ctx


@task_prerun.connect
def restore_geoserver_sync_gate(sender=None, task_id=None, task=None, **extra):
    request = getattr(task, "request", None)
    ctx = getattr(request, geoserver_sync_gate.CELERY_HEADER, None)
    if ctx is not None:
        _GATE_TOKENS[task_id] = geoserver_sync_gate.set_context(ctx)


@task_postrun.connect
def clear_geoserver_sync_gate(sender=None, task_id=None, **extra):
    token = _GATE_TOKENS.pop(task_id, None)
    if token is not None:
        try:
            geoserver_sync_gate.reset_context(token)
        except ValueError:
            pass
