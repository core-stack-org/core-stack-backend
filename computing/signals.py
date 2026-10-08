"""Auto-trigger STAC collection generation when a Layer is synced to GeoServer."""

from __future__ import annotations

import logging

from django.db.models.signals import post_save
from django.dispatch import receiver

from computing.models import Layer
from computing.stac_layer_resolution import stac_task_kwargs_for_layer
from utilities.layer_generation_mode import (
    is_sync_layer_generation_context_active,
    record_sync_stac_layer,
)

# Same logger as layer task steps so Apache/WSGI error logs show STAC skips.
log = logging.getLogger("core_stack.layer_generation")

_STAC_QUEUE = "nrm"


@receiver(post_save, sender=Layer, dispatch_uid="computing.signals.trigger_stac_on_geoserver_sync")
def trigger_stac_on_geoserver_sync(sender, instance: Layer, created, **kwargs):
    if not instance.is_sync_to_geoserver:
        return

    if instance.is_stac_specs_generated:
        log.info(
            "STAC auto-trigger: skip layer id=%s (is_stac_specs_generated already True)",
            instance.id,
        )
        return

    task_kwargs = stac_task_kwargs_for_layer(instance)
    if task_kwargs is None:
        dataset_name = instance.dataset.name if instance.dataset_id else None
        log.warning(
            "STAC auto-trigger: skip layer id=%s name=%s dataset=%s "
            "(no LayerMapping / CSV match; run: python manage.py load_layer_mappings)",
            instance.id,
            instance.layer_name,
            dataset_name,
        )
        return

    from computing.STAC_specs.stac_collection import generate_stac_collection_task

    log.info(
        "STAC auto-trigger: dispatching task for layer id=%s (%s/%s)",
        instance.id,
        task_kwargs["layer_type"],
        task_kwargs["layer_name"],
    )

    try:
        if is_sync_layer_generation_context_active():
            generate_stac_collection_task.apply(kwargs=task_kwargs)
            record_sync_stac_layer(
                state=task_kwargs["state"],
                district=task_kwargs["district"],
                block=task_kwargs["block"],
                layer_name=task_kwargs["layer_name"],
                layer_type=task_kwargs["layer_type"],
                start_year=task_kwargs["start_year"],
                end_year=task_kwargs["end_year"],
            )
        else:
            generate_stac_collection_task.apply_async(
                kwargs=task_kwargs, queue=_STAC_QUEUE
            )
    except Exception as exc:  # noqa: BLE001
        log.error(
            "STAC auto-trigger: failed to dispatch task for layer id=%s: %s",
            instance.id,
            exc,
        )
