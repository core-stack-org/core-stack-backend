"""Auto-trigger STAC collection generation when a Layer is synced to GeoServer.

The handler resolves the saved `Layer` (which stores a *templated* GeoServer
layer name) back to the canonical STAC `layer_name` / `layer_type` via the
`LayerMapping` registry (sourced from `layer_mapping.csv`), then dispatches the
`generate_stac_collection_task` Celery task asynchronously.

This removes the need for every layer-generation task to hardcode the STAC
parameters and to call STAC generation synchronously.
"""

from __future__ import annotations

import logging

from django.db.models.signals import post_save
from django.dispatch import receiver

from computing.models import Layer, LayerMapping
from utilities.gee_utils import valid_gee_text

log = logging.getLogger(__name__)


_STAC_QUEUE = "nrm"


def _format_geoserver_name(template: str, layer: Layer) -> str:
    if not template:
        return ""
    misc = layer.misc or {}
    try:
        return template.format(
            district=valid_gee_text(layer.district.district_name.lower()),
            block=valid_gee_text(layer.block.tehsil_name.lower()),
            state=valid_gee_text(layer.state.state_name.lower()),
            start_year=str(misc.get("start_year", "") or ""),
            end_year=str(misc.get("end_year", "") or ""),
        )
    except (KeyError, IndexError, AttributeError):
        return ""


def _resolve_mapping(layer: Layer) -> LayerMapping | None:
    """Return the best matching LayerMapping for a saved Layer.

    Resolution is keyed on `dataset.name -> LayerMapping.db_dataset_name`.
    For datasets that fan out into multiple STAC layers (e.g. Change Detection
    has 5 sub-layers per dataset row), we disambiguate by formatting each
    candidate's `geoserver_layer_name` template against the Layer's location
    and year metadata and matching the result against `layer.layer_name`.
    """
    if not layer.dataset_id:
        return None

    dataset_name = (layer.dataset.name or "").strip()
    if not dataset_name:
        return None

    candidates = list(
        LayerMapping.objects.filter(db_dataset_name=dataset_name, auto_stac=True)
    )
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    layer_name = (layer.layer_name or "").strip()
    if not layer_name:
        return None

    matches = [
        c
        for c in candidates
        if _format_geoserver_name(c.geoserver_layer_name, layer) == layer_name
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        log.warning(
            "STAC auto-trigger: no LayerMapping match for layer id=%s dataset=%s name=%s",
            layer.id,
            dataset_name,
            layer_name,
        )
        return None

    log.info(
        "STAC auto-trigger: %d ambiguous LayerMapping matches for layer id=%s; picking first",
        len(matches),
        layer.id,
    )
    return matches[0]


@receiver(post_save, sender=Layer, dispatch_uid="computing.signals.trigger_stac_on_geoserver_sync")
def trigger_stac_on_geoserver_sync(sender, instance: Layer, created, **kwargs):
    if not instance.is_sync_to_geoserver or instance.is_stac_specs_generated:
        return

    mapping = _resolve_mapping(instance)
    if mapping is None:
        return

    # Lazy import: avoids importing Celery / stac_collection at app-loading time.
    from computing.STAC_specs.stac_collection import generate_stac_collection_task

    misc = instance.misc or {}
    task_kwargs = dict(
        layer_type=mapping.layer_type,
        state=instance.state.state_name,
        district=instance.district.district_name,
        block=instance.block.tehsil_name,
        layer_name=mapping.layer_name,
        start_year=str(misc.get("start_year", "") or ""),
        end_year=str(misc.get("end_year", "") or ""),
        upload_to_s3=True,
        layer_id=instance.id,
    )

    log.info(
        "STAC auto-trigger: dispatching task for layer id=%s (%s/%s)",
        instance.id,
        mapping.layer_type,
        mapping.layer_name,
    )

    try:
        generate_stac_collection_task.apply_async(kwargs=task_kwargs, queue=_STAC_QUEUE)
    except Exception as exc:  # noqa: BLE001
        log.error(
            "STAC auto-trigger: failed to dispatch task for layer id=%s: %s",
            instance.id,
            exc,
        )
