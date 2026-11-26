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
from pathlib import Path

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

# Many shared Dataset rows still carry the seed placeholder "default_style" (or
# blank/None), which does not exist on GeoServer and would either be silently
# skipped (leaving grayscale default "raster") or create a dangling reference.
# Keep these in sync with GEOSERVER_STYLE / *_STYLE_NAME in computing/ pipelines.
PLACEHOLDER_STYLES = frozenset({None, "", "default_style"})
TREE_HEALTH_RASTER_STYLES = {
    "tree_ch_raster": "tree_ch_style",
    "tree_ccd_raster": "tree_ccd_style",
    "tree_overall_raster": "tree_overall_style",
}
RASTER_DEFAULT_STYLES = {
    **TREE_HEALTH_RASTER_STYLES,
    # Legacy tree health raster workspaces
    "canopy_height": "ch_style",
    "ccd": "ccd_style",
    "tree_overall_ch": "tree_overall_ch_style",
    # LULC
    "LULC_level_1": "lulc_level_1_style",
    "LULC_level_2": "lulc_level_2_style",
    "LULC_level_3": "lulc_level_3_style",
    # Terrain, restoration, plantation, CLART, hydrology/topo rasters
    "terrain": "terrain_raster",
    "restoration": "restoration_style",
    "plantation": "site_suitability",
    "clart": "testClart",
    "stream_order": "stream_order",
    "catchment_area_singleflow": "catchment_area_singleflow",
    "distance_nearest_upstream_DL": "distance_nearest_upstream_DL",
    "natural_depression": "natural_depression",
    "slope_percentage": "slope_percentage",
    "dem": "dem_style",
}
# Change detection layers share one Dataset row ("Change Detection Raster") and
# derive their GeoServer style from the layer name suffix (param_name.lower()).
CHANGE_DETECTION_WORKSPACE = "change_detection"
CHANGE_DETECTION_STYLE_SUFFIXES = (
    "urbanization",
    "degradation",
    "deforestation",
    "afforestation",
    "cropintensity",
    "shrubchange",
)


def _resolve_raster_style(workspace, layer_name, style_name):
    """Resolve placeholder Dataset styles to the actual GeoServer raster style."""
    if style_name not in PLACEHOLDER_STYLES:
        return style_name
    if workspace == CHANGE_DETECTION_WORKSPACE and layer_name:
        lowered = str(layer_name).lower()
        for suffix in CHANGE_DETECTION_STYLE_SUFFIXES:
            if lowered.endswith(f"_{suffix}"):
                return suffix
    resolved = RASTER_DEFAULT_STYLES.get(workspace)
    if resolved is not None:
        return resolved
    return None if style_name == "default_style" else style_name

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


def _local_path_exists(candidate):
    if os.path.exists(candidate):
        return True
    _, ext = os.path.splitext(candidate)
    if ext.lower() not in (*VECTOR_EXTENSIONS, ".tif", ".tiff"):
        return os.path.exists(f"{candidate}.gpkg")
    return False


def _location_slug_variants(value):
    from utilities.gee_utils import valid_gee_text

    slug = valid_gee_text(str(value).strip().lower())
    if not slug:
        return []
    return list(dict.fromkeys([slug, slug.replace("-", "_"), slug.replace("_", "-")]))


def _derived_layer_subdirs(workspace, layer_type=None):
    if not workspace:
        return []
    from computing import config_loader

    prefix = config_loader._LOCATION_PREFIX
    exact, fallback = [], []
    for layer in config_loader.derived_layers():
        if layer.get("geoserver_workspace") != workspace:
            continue
        local_path = layer.get("local_path") or ""
        if not local_path.startswith(prefix):
            continue
        subdir = local_path[len(prefix) :].split("{")[0].rstrip("/")
        if not subdir:
            continue
        target = (
            exact
            if (not layer_type or layer.get("layer_type") == layer_type)
            else fallback
        )
        if subdir not in target:
            target.append(subdir)
    return exact + [s for s in fallback if s not in exact]


def _rebased_data_dir_candidates(raw_path, data_dir, project_root):
    candidates = []

    def add(candidate):
        candidate_str = str(candidate)
        if candidate_str not in candidates:
            candidates.append(candidate_str)

    path_obj = Path(raw_path)
    parts = path_obj.parts

    if os.path.isabs(raw_path):
        try:
            path_obj.relative_to(data_dir)
        except ValueError:
            in_data_dir = False
        else:
            in_data_dir = True

        if in_data_dir:
            add(raw_path)

        # Rebase absolute paths from another DATA_DIR (e.g. host vs Docker
        # mount or relocated data volume) onto the active DATA_DIR.
        for anchor in ("layers", "base_layers"):
            for idx in range(len(parts) - 1, -1, -1):
                if parts[idx] == anchor and idx + 1 < len(parts):
                    add(data_dir.joinpath(*parts[idx:]))
        for idx in range(len(parts) - 1, -1, -1):
            if parts[idx] == "data" and idx + 1 < len(parts):
                add(data_dir.joinpath(*parts[idx + 1 :]))

        if not in_data_dir:
            add(raw_path)
    else:
        if raw_path == "data":
            add(data_dir)
        elif raw_path.startswith("data/"):
            add(data_dir / raw_path[len("data/") :])
        add(data_dir / raw_path)
        add(project_root / raw_path)

    return candidates


def _relocated_layer_layout_candidates(raw_path, entry, data_dir):
    if not entry:
        return []
    state = entry.get("state")
    district = entry.get("district")
    block = entry.get("block")
    filename = Path(raw_path).name
    if not (state and district and block and filename):
        return []

    state_variants = _location_slug_variants(state)
    district_variants = _location_slug_variants(district)
    block_variants = _location_slug_variants(block)
    if not (state_variants and district_variants and block_variants):
        return []

    candidates = []

    def add(candidate):
        candidate_str = str(candidate)
        if candidate_str not in candidates:
            candidates.append(candidate_str)

    # 1. Look up the derived layer's subdir under
    #    {DATA_DIR}/layers/{state}/{district}/{block}/<subdir>/<filename>
    #    from computing/config.yaml.
    for subdir in _derived_layer_subdirs(
        entry.get("workspace"), layer_type=entry.get("layer_type")
    ):
        for s in state_variants:
            for d in district_variants:
                for b in block_variants:
                    add(data_dir / "layers" / s / d / b / subdir / filename)

    # 2. Reorder legacy <subdir>/{state}/{district}/{block}/{filename} paths
    #    (covers layers without a config.yaml workspace, e.g. tree health rasters).
    parts = Path(raw_path).parts
    if len(parts) >= 5:
        loc = parts[-4:-1]
        if (
            loc[0] in state_variants
            and loc[1] in district_variants
            and loc[2] in block_variants
        ):
            prefix = parts[:-4]
            rel_subdir = None
            for idx in range(len(prefix) - 1, -1, -1):
                if prefix[idx] in ("data", "layers"):
                    rel_subdir = prefix[idx + 1 :]
                    break
            if rel_subdir is None and not os.path.isabs(raw_path):
                rel_subdir = prefix
            if rel_subdir:
                if rel_subdir[0] == "layers":
                    rel_subdir = rel_subdir[1:]
                if rel_subdir:
                    add(
                        data_dir.joinpath(
                            "layers", loc[0], loc[1], loc[2], *rel_subdir, filename
                        )
                    )

    return candidates


def _resolve_local_path(path, entry=None):
    """
    Resolve a Layer.gee_asset_path against the active ``config_loader.DATA_DIR``.

    Handles:
    - Current ``DATA_DIR`` paths (returned directly when present).
    - Absolute paths written under a different ``DATA_DIR`` (e.g. host vs
      Docker mount or relocated volume), rebased onto ``DATA_DIR``.
    - Legacy relative ``data/...`` paths under ``DATA_DIR`` or ``PROJECT_ROOT``.
    - Pre-restructure paths whose files now live under
      ``{DATA_DIR}/layers/{state}/{district}/{block}/<subdir>/<filename>``.
    """
    from computing import config_loader

    data_dir = Path(config_loader.DATA_DIR)
    project_root = Path(config_loader.PROJECT_ROOT)

    candidates = _rebased_data_dir_candidates(path, data_dir, project_root)
    for candidate in candidates:
        if _local_path_exists(candidate):
            return candidate

    for candidate in _relocated_layer_layout_candidates(path, entry, data_dir):
        if _local_path_exists(candidate):
            return candidate

    return candidates[0] if candidates else path


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
    resolved = _resolve_local_path(path, entry=entry)
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
    path = _resolve_local_path(path, entry=entry)
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
    style_name = entry.get("style_name")
    if layer_type == "raster":
        style_name = _resolve_raster_style(
            entry["workspace"], entry["layer_name"], style_name
        )
    args = {
        "layer_type": layer_type,
        "path": path,
        "layer_name": entry["layer_name"],
        "workspace": entry["workspace"],
        "style_name": style_name,
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


def _queue_local_entries(entries, dry_run, skip_stac=True):
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
            publish_kwargs = {**args, "skip_stac": skip_stac} if skip_stac else args
            result = publish_local_layer.apply_async(
                kwargs=publish_kwargs, queue=GEOSERVER_QUEUE
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


def _queue_gee_entries(entries, dry_run, gee_account_id, skip_stac=True):
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
            style_name = entry.get("style_name")
            if kind == "raster":
                style_name = _resolve_raster_style(
                    entry["workspace"], entry["layer_name"], style_name
                )
            elif style_name in PLACEHOLDER_STYLES:
                style_name = None
            result = publish_gee_layer.apply_async(
                kwargs={
                    "layer_type": kind,
                    "asset_id": entry["path"],
                    "layer_name": entry["layer_name"],
                    "workspace": entry["workspace"],
                    "style_name": style_name,
                    "state": entry["state"],
                    "layer_id": entry.get("layer_id"),
                    "gee_account_id": gee_account_id,
                    **({"skip_stac": True} if skip_stac else {}),
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
    skip_stac=True,
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

    summary = _queue_local_entries(local_entries, dry_run, skip_stac=skip_stac)
    summary["gee"] = _queue_gee_entries(
        gee_entries, dry_run, gee_account_id, skip_stac=skip_stac
    )
    summary["dry_run"] = dry_run
    summary["force"] = force
    return summary
