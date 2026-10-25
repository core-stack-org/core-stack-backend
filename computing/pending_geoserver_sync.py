"""
Push layers that were generated locally but kept off GeoServer (see
``utilities.geoserver_sync_gate``) once their tehsil is active.

A kept-local layer is a ``Layer`` row with ``is_sync_to_geoserver=False`` and
``misc.is_generated_locally=True``. Tehsil activation happens on the prod DB,
which the local compute DB does not mirror, so when PROD_BACKEND_URL is set the
rows are read from prod (which also applies the active check). Pushing flips
the flag through ``publish_local_layer``.

GEE-generated layers are handled too: a ``Layer`` row with
``is_sync_to_geoserver=False`` and a real GEE asset path. Their vector asset is
re-read from GEE, and their raster is taken from the GCS tif exported at
generation time, by ``publish_gee_layer``.
"""

import logging
import os

import requests
from django.conf import settings
from django.db.models import Q

from computing.models import Layer

logger = logging.getLogger(__name__)

VECTOR_EXTENSIONS = (".gpkg", ".shp", ".geojson")

# Pipelines that publish their gpkg through utilities.pipelines.publish (WFS
# verified) rather than the zip push, so kept-local layers are pushed the same
# way. Facility points live in a second gpkg whose tables are the suffixes.
GPKG_PIPELINE_WORKSPACES = frozenset(
    {"antyodaya_2020", "livestocks", "facilities_proximity"}
)
FACILITY_POINT_LAYERS = (
    "tehsil_facility_collection",
    "village_nearest_facility_collection",
)

SOIL_HEALTH_RASTER_WORKSPACE = "soil_health_raster"
SOIL_HEALTH_NUTRIENTS = ("N", "K", "P", "OC", "OC_OLM")

# Datasets whose GEE sync is deliberately disabled in their pipeline.
GEE_SYNC_EXCLUDED_DATASETS = frozenset()


def unsynced_local_layers_queryset(
    state=None, district=None, block=None, layer_type=None, force=False
):
    """
    Locally generated layers not on GeoServer whose tehsil chain is active.
    ``force`` also returns layers already marked as synced.

    ``layer_type`` is a bulk pipeline name; it narrows the layers to the
    datasets that pipeline generates. Raises ValueError for an unknown one.
    """
    queryset = Layer.objects.filter(
        misc__is_generated_locally=True,
        block__active_status=True,
        district__active_status=True,
        state__active_status=True,
    ).select_related("dataset", "state", "district", "block")
    if not force:
        queryset = queryset.filter(is_sync_to_geoserver=False)
    if layer_type:
        from computing.bulk_layer_generation import get_regeneration_dataset_names

        queryset = queryset.filter(
            dataset__name__in=get_regeneration_dataset_names(layer_type)
        )
    if state:
        queryset = queryset.filter(state__state_name__iexact=state)
    if district:
        queryset = queryset.filter(district__district_name__iexact=district)
    if block:
        queryset = queryset.filter(block__tehsil_name__iexact=block)
    return queryset


def unsynced_gee_layers_queryset(
    state=None, district=None, block=None, layer_type=None, force=False
):
    """
    GEE-generated layers not on GeoServer whose tehsil chain is active.
    ``force`` also returns layers already marked as synced.
    """
    queryset = (
        Layer.objects.filter(
            gee_asset_path__startswith="projects/",
            block__active_status=True,
            district__active_status=True,
            state__active_status=True,
        )
        # A plain .exclude(misc__is_generated_locally=True) also drops rows whose
        # misc is a dict without that key (NOT of a NULL comparison).
        .filter(
            ~Q(misc__is_generated_locally=True)
            | Q(misc__is_generated_locally__isnull=True)
        )
        .exclude(dataset__name__in=GEE_SYNC_EXCLUDED_DATASETS)
        .select_related("dataset", "state", "district", "block")
    )
    if not force:
        queryset = queryset.filter(is_sync_to_geoserver=False)
    if layer_type:
        from computing.bulk_layer_generation import get_regeneration_dataset_names

        queryset = queryset.filter(
            dataset__name__in=get_regeneration_dataset_names(layer_type)
        )
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


def fetch_unsynced_layers(
    state=None, district=None, block=None, layer_type=None, source="local", force=False
):
    prod_url = getattr(settings, "PROD_BACKEND_URL", "").rstrip("/")
    if not prod_url:
        queryset_for = (
            unsynced_gee_layers_queryset
            if source == "gee"
            else unsynced_local_layers_queryset
        )
        return serialize_unsynced_layers(
            queryset_for(state, district, block, layer_type, force)
        )
    response = requests.get(
        f"{prod_url}/api/v1/unsynced_local_layers/",
        params={
            key: value
            for key, value in (
                ("state", state),
                ("district", district),
                ("block", block),
                ("layer_type", layer_type),
                ("source", source if source != "local" else None),
                ("force", "true" if force else None),
            )
            if value
        },
        headers={"X-Api-Key": getattr(settings, "PROD_BACKEND_API_KEY", "")},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["layers"]


def _is_gee_path(path):
    return bool(path) and str(path).startswith("projects/")


def _resolve_local_path(path):
    """
    Local generators store output paths relative to the repo (``data/...``).
    Resolve them under DATA_DIR (which may be relocated) or the repo root;
    absolute paths are returned as they are.
    """
    if os.path.isabs(path):
        return path
    from computing.config_loader import DATA_DIR, PROJECT_ROOT

    candidates = []
    if path == "data" or path.startswith("data/"):
        candidates.append(str(DATA_DIR / path[len("data/"):]))
    candidates.append(str(PROJECT_ROOT / path))
    for candidate in candidates:
        if os.path.exists(candidate) or os.path.exists(
            os.path.splitext(candidate)[0] + ".gpkg"
        ):
            return candidate
    return candidates[0]


def _expand_soil_health_raster(entry):
    """
    A soil health raster row holds one nutrient's path under the bare layer
    name, but each nutrient is a separate GeoServer layer
    ``<layer_name>_<nutrient>``. Returns one entry per nutrient file (the row
    is only flipped by the last one), or the entry itself if it doesn't apply.
    """
    path = entry.get("path")
    if (
        entry.get("workspace") != SOIL_HEALTH_RASTER_WORKSPACE
        or not path
        or _is_gee_path(path)
    ):
        return [entry]
    resolved = _resolve_local_path(path)
    stem = os.path.splitext(resolved)[0]
    for nutrient in sorted(SOIL_HEALTH_NUTRIENTS, key=len, reverse=True):
        if stem.endswith(f"_{nutrient}"):
            stem = stem[: -len(nutrient) - 1]
            break
    else:
        return [entry]
    expanded = [
        {
            **entry,
            "path": f"{stem}_{nutrient}.tif",
            "layer_name": f"{entry['layer_name']}_{nutrient}",
            "layer_type": "raster",
        }
        for nutrient in SOIL_HEALTH_NUTRIENTS
    ]
    for sub in expanded[:-1]:
        sub["layer_id"] = None
    return expanded


def _publish_args(entry):
    """Map a layer entry to publish_local_layer arguments, or None if unusable."""
    path = entry.get("path")
    if not path or _is_gee_path(path):
        return None  # not a local file (e.g. a GEE asset)
    path = _resolve_local_path(path)
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
    args = {
        "layer_type": layer_type,
        "path": path,
        "layer_name": entry["layer_name"],
        "workspace": entry["workspace"],
        "style_name": entry.get("style_name"),
        "file_type": file_type,
        "layer_id": entry.get("layer_id"),
    }
    if layer_type == "vector" and entry["workspace"] in GPKG_PIPELINE_WORKSPACES:
        layer_name = entry["layer_name"]
        point_layer = next(
            (t for t in FACILITY_POINT_LAYERS if layer_name.endswith(f"_{t}")), None
        )
        store_name = layer_name
        if point_layer:
            store_name = f"{layer_name[: -len(point_layer) - 1]}_facility_points"
        args["gpkg_layers"] = {layer_name: point_layer}
        args["store_name"] = store_name
    return args


def _gpkg_group_key(args):
    return (args["workspace"], args["store_name"], args["path"])


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


def _empty_summary():
    return {
        "found": 0,
        "queued": 0,
        "skipped_unusable": 0,
        "queued_layers": [],
        "skipped_layers": [],
    }


def _layer_info(entry):
    return {
        "layer_id": entry.get("layer_id"),
        "workspace": entry.get("workspace"),
        "layer_name": entry.get("layer_name"),
        "state": entry.get("state"),
        "district": entry.get("district"),
        "block": entry.get("block"),
    }


def _queue_local_entries(entries, dry_run):
    """
    Layers sharing one GeoServer datastore (facility points) are published by
    a single task, which flips every one of their flags; publishing them one by
    one would delete the datastore holding the other.
    """
    from computing.tasks import GEOSERVER_QUEUE, publish_local_layer

    summary = _empty_summary()
    expanded = [sub for entry in entries for sub in _expand_soil_health_raster(entry)]
    planned = []  # (info, args), args shared by layers of one datastore
    groups = {}
    for entry in expanded:
        summary["found"] += 1
        info = _layer_info(entry)
        args = _publish_args(entry)
        if args is None:
            logger.warning(
                "Cannot push %s:%s (no local file at %s); leaving it unsynced",
                entry.get("workspace"),
                entry.get("layer_name"),
                entry.get("path"),
            )
            summary["skipped_unusable"] += 1
            summary["skipped_layers"].append({**info, "reason": "no local file"})
            continue
        if "gpkg_layers" in args:
            first = groups.setdefault(_gpkg_group_key(args), args)
            if first is not args:
                first["gpkg_layers"].update(args["gpkg_layers"])
                if args.get("layer_id"):
                    first.setdefault("extra_layer_ids", []).append(args["layer_id"])
                args = first
        planned.append((info, args))

    task_ids = {}
    for info, args in planned:
        if not dry_run and id(args) not in task_ids:
            result = publish_local_layer.apply_async(
                kwargs=args, queue=GEOSERVER_QUEUE
            )
            task_ids[id(args)] = result.id
        if not dry_run:
            info["task_id"] = task_ids[id(args)]
        summary["queued"] += 1
        summary["queued_layers"].append(info)
    return summary


def _gee_skip_reason(entry, gee_account_id, ee_ready):
    """
    Why a GEE layer cannot be pushed, or None when its source is available:
    the GEE asset for vectors, the GCS tif for rasters.
    """
    from utilities.gee_utils import ee_initialize, gcs_file_exists, is_gee_asset_exists

    try:
        if entry.get("layer_type") == "raster":
            if not gcs_file_exists(entry["layer_name"]):
                return "no gcs file"
            return None
        if not ee_ready[0]:
            ee_initialize(gee_account_id)
            ee_ready[0] = True
        if not is_gee_asset_exists(entry["path"]):
            return "gee asset missing"
    except Exception as exc:
        logger.warning(
            "Could not check source of %s:%s: %s",
            entry.get("workspace"),
            entry.get("layer_name"),
            exc,
        )
        return f"source check failed: {exc}"
    return None


def _gee_layer_kind(entry, gee_account_id, ee_ready):
    """
    "raster" or "vector" for a GEE layer. The dataset's layer_type can be wrong
    when raster and vector layers share one dataset (e.g. Stream Order), so the
    asset itself decides: an Image is a raster, a table a vector. Falls back to
    the dataset type, or a "_raster" name suffix, if the asset can't be read.
    """
    kind = "raster" if entry.get("layer_type") == "raster" else "vector"
    try:
        import ee

        from utilities.gee_utils import ee_initialize

        if not ee_ready[0]:
            ee_initialize(gee_account_id)
            ee_ready[0] = True
        asset_type = ee.data.getAsset(entry["path"]).get("type")
    except Exception:
        asset_type = None
    if asset_type == "IMAGE":
        return "raster"
    if asset_type in ("TABLE", "FEATURE_COLLECTION"):
        return "vector"
    if kind == "vector" and str(entry.get("layer_name", "")).endswith("_raster"):
        return "raster"
    return kind


def _queue_gee_entries(entries, dry_run, gee_account_id):
    from computing.tasks import GEOSERVER_QUEUE, publish_gee_layer

    summary = {
        **_empty_summary(),
        "vector": {"found": 0, "queued": 0, "skipped": 0},
        "raster": {"found": 0, "queued": 0, "skipped": 0},
    }
    ee_ready = [False]
    for entry in entries:
        kind = _gee_layer_kind(entry, gee_account_id, ee_ready)
        entry = {**entry, "layer_type": kind}
        summary["found"] += 1
        summary[kind]["found"] += 1
        info = {**_layer_info(entry), "layer_type": kind}
        reason = (
            "missing workspace or layer name"
            if not entry.get("workspace") or not entry.get("layer_name")
            else _gee_skip_reason(entry, gee_account_id, ee_ready)
        )
        if reason:
            summary["skipped_unusable"] += 1
            summary[kind]["skipped"] += 1
            summary["skipped_layers"].append({**info, "reason": reason})
            continue
        if not dry_run:
            result = publish_gee_layer.apply_async(
                kwargs={
                    "layer_type": kind,
                    "asset_id": entry["path"],
                    "layer_name": entry["layer_name"],
                    "workspace": entry["workspace"],
                    "style_name": entry.get("style_name"),
                    "state": entry["state"],
                    "layer_id": entry.get("layer_id"),
                    "gee_account_id": gee_account_id,
                },
                queue=GEOSERVER_QUEUE,
            )
            info["task_id"] = result.id
        summary["queued"] += 1
        summary[kind]["queued"] += 1
        summary["queued_layers"].append(info)
    return summary


def queue_unsynced_layers(
    state=None,
    district=None,
    block=None,
    layer_type=None,
    dry_run=False,
    source="all",
    gee_account_id=None,
    force=False,
    sync_boundaries=True,
):
    """
    Queue the GeoServer push of every unsynced layer of active tehsils,
    optionally only for one ``layer_type`` (bulk pipeline name). ``force``
    re-pushes layers already marked as synced too. ``sync_boundaries=False``
    skips the per-tehsil admin/MWS boundary push (use when they are already on
    GeoServer).

    ``source`` is "local", "gee" or "all". The top-level keys describe locally
    generated layers; GEE-generated layers are under ``gee``.

    "queued" means handed to Celery, not yet on GeoServer; the layer flag flips
    when the publish task succeeds.
    """
    if source not in ("all", "local", "gee"):
        raise ValueError(f"Unknown source '{source}'; use all, local or gee.")

    local_entries = (
        fetch_unsynced_layers(state, district, block, layer_type, "local", force)
        if source in ("all", "local")
        else []
    )
    gee_entries = (
        fetch_unsynced_layers(state, district, block, layer_type, "gee", force)
        if source in ("all", "gee")
        else []
    )
    # Layers generated locally but kept as a GEE asset (e.g. surface water
    # bodies) are published from GEE, not from a local file.
    gee_entries = gee_entries + [e for e in local_entries if _is_gee_path(e.get("path"))]
    local_entries = [e for e in local_entries if not _is_gee_path(e.get("path"))]
    if not dry_run and sync_boundaries:
        _sync_boundaries(local_entries + gee_entries)

    summary = _queue_local_entries(local_entries, dry_run)
    summary["gee"] = _queue_gee_entries(gee_entries, dry_run, gee_account_id)
    summary["dry_run"] = dry_run
    summary["force"] = force
    return summary
