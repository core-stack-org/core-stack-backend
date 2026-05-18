"""
Celery signal handlers for layer-generation task observability.
"""

import logging

from celery.signals import task_failure, task_postrun, task_prerun

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
