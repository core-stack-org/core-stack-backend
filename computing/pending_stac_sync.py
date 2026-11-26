"""Batch STAC generation and S3 sync for active tehsils.

Mirrors ``computing.pending_geoserver_sync``: finds layers belonging to active
tehsils that are already synced to GeoServer (``is_sync_to_geoserver=True``)
but still missing STAC specs (``is_stac_specs_generated=False``, or all synced
layers with ``force=True``), groups them by tehsil, builds their STAC items
serially per tehsil, and performs one targeted S3 upload per tehsil instead of
one per layer.
"""

from __future__ import annotations

import logging
from collections import defaultdict

from django.conf import settings

from computing.models import Layer
from computing.STAC_specs.stac_collection import STACCollectionGenerator
from computing.stac_layer_resolution import stac_task_kwargs_for_layer

logger = logging.getLogger(__name__)


def pending_stac_layers_queryset(
    state=None, district=None, block=None, layer_type=None, force=False
):
    """GeoServer-synced layers in active tehsils that still need STAC.

    ``force=True`` also includes layers already marked ``is_stac_specs_generated=True``
    (useful when STAC was generated locally earlier and now needs S3 upload or
    regeneration). ``layer_type`` accepts the same bulk pipeline names as
    ``sync_pending_geoserver``.
    """
    queryset = Layer.objects.filter(
        is_sync_to_geoserver=True,
        block__active_status=True,
        district__active_status=True,
        state__active_status=True,
    ).select_related("dataset", "state", "district", "block")
    if not force:
        queryset = queryset.filter(is_stac_specs_generated=False)
    if layer_type:
        from computing.bulk_layer_generation import get_layer_sync_filter

        dataset_names, layer_name_prefix = get_layer_sync_filter(layer_type)
        queryset = queryset.filter(dataset__name__in=dataset_names)
        if layer_name_prefix:
            queryset = queryset.filter(layer_name__startswith=layer_name_prefix)
    if state:
        queryset = queryset.filter(state__state_name__iexact=state)
    if district:
        queryset = queryset.filter(district__district_name__iexact=district)
    if block:
        queryset = queryset.filter(block__tehsil_name__iexact=block)
    return queryset.order_by(
        "state__state_name",
        "district__district_name",
        "block__tehsil_name",
        "dataset__name",
        "layer_name",
        "id",
    )


def plan_pending_stac_sync(
    state=None,
    district=None,
    block=None,
    layer_type=None,
    force=False,
    limit_tehsils=None,
):
    """Resolve pending Layer rows into deduplicated per-tehsil STAC targets."""
    if limit_tehsils is not None and limit_tehsils <= 0:
        raise ValueError("--limit-tehsils must be a positive integer.")

    queryset = pending_stac_layers_queryset(
        state=state,
        district=district,
        block=block,
        layer_type=layer_type,
        force=force,
    )

    layers_found = 0
    skipped_layers = []
    grouped = defaultdict(dict)  # (state, district, block) -> {dedup_key: target}

    for layer in queryset:
        layers_found += 1
        task_kwargs = stac_task_kwargs_for_layer(layer)
        if task_kwargs is None:
            skipped_layers.append(
                {
                    "layer_id": layer.id,
                    "dataset": getattr(layer.dataset, "name", ""),
                    "layer_name": layer.layer_name,
                    "state": layer.state.state_name,
                    "district": layer.district.district_name,
                    "block": layer.block.tehsil_name,
                    "reason": "no STAC mapping",
                }
            )
            continue

        tehsil_key = (
            task_kwargs["state"],
            task_kwargs["district"],
            task_kwargs["block"],
        )
        dedup_key = (
            task_kwargs["layer_type"],
            task_kwargs["layer_name"],
            task_kwargs["start_year"],
            task_kwargs["end_year"],
        )
        existing = grouped[tehsil_key].get(dedup_key)
        if existing is None:
            grouped[tehsil_key][dedup_key] = {
                "layer_type": task_kwargs["layer_type"],
                "layer_name": task_kwargs["layer_name"],
                "start_year": task_kwargs["start_year"],
                "end_year": task_kwargs["end_year"],
                "layer_ids": [layer.id],
                "db_layer_names": [layer.layer_name],
            }
        else:
            existing["layer_ids"].append(layer.id)
            existing["db_layer_names"].append(layer.layer_name)

    sorted_tehsils = sorted(grouped.keys())
    if limit_tehsils is not None:
        sorted_tehsils = sorted_tehsils[:limit_tehsils]

    tehsil_plans = [
        {
            "state": t_state,
            "district": t_district,
            "block": t_block,
            "targets": list(grouped[(t_state, t_district, t_block)].values()),
        }
        for (t_state, t_district, t_block) in sorted_tehsils
    ]

    return {
        "layers_found": layers_found,
        "tehsils_found": len(grouped),
        "tehsil_plans": tehsil_plans,
        "skipped_layers": skipped_layers,
    }


def sync_pending_stac(
    state=None,
    district=None,
    block=None,
    layer_type=None,
    force=False,
    upload_to_s3=None,
    overwrite=False,
    overwrite_metadata=False,
    dry_run=False,
    limit_tehsils=None,
    generator=None,
):
    """Generate STAC items and sync to S3 serially, one tehsil at a time."""
    if upload_to_s3 is None:
        upload_to_s3 = bool(getattr(settings, "STAC_UPLOAD_TO_S3", False))

    plan = plan_pending_stac_sync(
        state=state,
        district=district,
        block=block,
        layer_type=layer_type,
        force=force,
        limit_tehsils=limit_tehsils,
    )
    tehsil_plans = plan["tehsil_plans"]
    targets_planned = sum(len(tp["targets"]) for tp in tehsil_plans)

    summary = {
        "upload_to_s3": bool(upload_to_s3),
        "layers_found": plan["layers_found"],
        "tehsils_found": plan["tehsils_found"],
        "tehsils_selected": len(tehsil_plans),
        "tehsils_processed": 0,
        "targets_planned": targets_planned,
        "targets_built": 0,
        "targets_failed": 0,
        "s3_sync_failures": 0,
        "skipped_unmapped": len(plan["skipped_layers"]),
        "skipped_layers": plan["skipped_layers"],
        "tehsil_plans": tehsil_plans,
        "tehsil_results": [],
    }

    if dry_run or not tehsil_plans:
        return summary

    generator = generator or STACCollectionGenerator()
    if overwrite_metadata:
        logger.info("Refreshing STAC metadata CSVs once before batch sync.")
        generator.metadata.refresh_metadata()

    for tp in tehsil_plans:
        result = generator.sync_tehsil_layers(
            state=tp["state"],
            district=tp["district"],
            block=tp["block"],
            targets=tp["targets"],
            upload_to_s3=bool(upload_to_s3),
            overwrite=overwrite,
            overwrite_metadata=False,
        )
        summary["tehsils_processed"] += 1
        summary["targets_built"] += len(result["built"])
        summary["targets_failed"] += len(result["failed"])
        if upload_to_s3 and result["built"] and result["s3_synced"] is False:
            summary["s3_sync_failures"] += 1
        summary["tehsil_results"].append(result)

    return summary
