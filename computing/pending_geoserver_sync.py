"""
Push layers that were generated locally but kept off GeoServer (see
``utilities.geoserver_sync_gate``) once their tehsil is active.

A kept-local layer is a ``Layer`` row with ``is_sync_to_geoserver=False`` and
``misc.is_generated_locally=True``. Tehsil activation happens on the prod DB,
which the local compute DB does not mirror, so when PROD_BACKEND_URL is set the
rows are read from prod (which also applies the active check). Pushing flips
the flag through ``publish_local_layer``.
"""

import logging
import os

import requests
from django.conf import settings

from computing.models import Layer

logger = logging.getLogger(__name__)

VECTOR_EXTENSIONS = (".gpkg", ".shp", ".geojson")


def unsynced_local_layers_queryset(state=None, district=None, block=None):
    """Locally generated layers not on GeoServer whose tehsil chain is active."""
    queryset = Layer.objects.filter(
        is_sync_to_geoserver=False,
        misc__is_generated_locally=True,
        block__active_status=True,
        district__active_status=True,
        state__active_status=True,
    ).select_related("dataset", "state", "district", "block")
    if state:
        queryset = queryset.filter(state__state_name__iexact=state)
    if district:
        queryset = queryset.filter(district__district_name__iexact=district)
    if block:
        queryset = queryset.filter(block__tehsil_name__iexact=block)
    return queryset


def serialize_unsynced_layers(queryset):
    return [
        {
            "layer_id": layer.id,
            "layer_name": layer.layer_name,
            "path": layer.gee_asset_path,
            "workspace": layer.dataset.workspace,
            "style_name": layer.dataset.style_name,
            "layer_type": layer.dataset.layer_type,
            "state": layer.state.state_name,
            "district": layer.district.district_name,
            "block": layer.block.tehsil_name,
        }
        for layer in queryset
    ]


def fetch_unsynced_layers(state=None, district=None, block=None):
    prod_url = getattr(settings, "PROD_BACKEND_URL", "").rstrip("/")
    if not prod_url:
        return serialize_unsynced_layers(
            unsynced_local_layers_queryset(state, district, block)
        )
    response = requests.get(
        f"{prod_url}/api/v1/unsynced_local_layers/",
        params={
            key: value
            for key, value in (
                ("state", state),
                ("district", district),
                ("block", block),
            )
            if value
        },
        headers={"X-Api-Key": getattr(settings, "PROD_BACKEND_API_KEY", "")},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["layers"]


def _publish_args(entry):
    """Map a layer entry to publish_local_layer arguments, or None if unusable."""
    path = entry.get("path")
    if not path or not os.path.isabs(path):
        return None  # not a local file (e.g. a GEE asset)
    if entry.get("layer_type") == "raster":
        layer_type, file_type = "raster", "tif"
    else:
        layer_type = "vector"
        base, ext = os.path.splitext(path)
        if ext.lower() in VECTOR_EXTENSIONS:
            path, file_type = base, ext.lower().lstrip(".")
        else:
            file_type = "gpkg"
    exists = os.path.exists(path if layer_type == "raster" else f"{path}.{file_type}")
    if not exists or not entry.get("workspace") or not entry.get("layer_name"):
        return None
    return {
        "layer_type": layer_type,
        "path": path,
        "layer_name": entry["layer_name"],
        "workspace": entry["workspace"],
        "style_name": entry.get("style_name"),
        "file_type": file_type,
        "layer_id": entry.get("layer_id"),
    }


def _sync_boundaries(entries):
    """
    Boundaries are kept off GeoServer while a tehsil is inactive, so push them
    now for every tehsil that has layers to sync (already-published ones are
    skipped). Runs outside the gate, so it really pushes.
    """
    from computing.local_compute_helper import sync_tehsil_boundaries_to_geoserver

    tehsils = {(e["state"], e["district"], e["block"]) for e in entries}
    for state, district, block in sorted(tehsils):
        try:
            sync_tehsil_boundaries_to_geoserver(state, district, block)
        except Exception:
            logger.exception(
                "Boundary sync failed for %s/%s/%s; its layers are still queued",
                state,
                district,
                block,
            )


def queue_unsynced_layers(state=None, district=None, block=None, dry_run=False):
    """Queue the GeoServer push of every unsynced local layer of active tehsils."""
    from computing.tasks import GEOSERVER_QUEUE, publish_local_layer

    summary = {"found": 0, "queued": 0, "skipped_unusable": 0}
    entries = fetch_unsynced_layers(state, district, block)
    if not dry_run:
        _sync_boundaries(entries)
    for entry in entries:
        summary["found"] += 1
        args = _publish_args(entry)
        if args is None:
            logger.warning(
                "Cannot push %s:%s (no local file at %s); leaving it unsynced",
                entry.get("workspace"),
                entry.get("layer_name"),
                entry.get("path"),
            )
            summary["skipped_unusable"] += 1
            continue
        if not dry_run:
            publish_local_layer.apply_async(kwargs=args, queue=GEOSERVER_QUEUE)
        summary["queued"] += 1
    return summary
