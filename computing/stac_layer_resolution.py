"""Resolve Layer rows to STAC generation parameters via LayerMapping."""

from __future__ import annotations

import logging
import os
import re
from types import SimpleNamespace

import pandas as pd
from django.conf import settings

from computing.models import Layer, LayerMapping
from computing.STAC_specs import constants
from computing.STAC_specs.stac_collection import STACConfig
from utilities.gee_utils import valid_gee_text

log = logging.getLogger(__name__)

_LULC_DATASETS = frozenset({"LULC_level_1", "LULC_level_2", "LULC_level_3"})
_LULC_LAYER_RE = re.compile(r"^LULC_(\d{2})_(\d{2})_", re.IGNORECASE)


def _two_digit_hydrological_year(value) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) == 2 and text.isdigit():
        return text
    if len(text) == 4 and text.isdigit():
        return str(int(text) % 100).zfill(2)
    return text


def lulc_years_from_layer_name(layer_name: str):
    match = _LULC_LAYER_RE.match(layer_name or "")
    if not match:
        return None, None
    return match.group(1), match.group(2)


def lulc_calendar_start_year(layer_name: str):
    start_yy, _ = lulc_years_from_layer_name(layer_name)
    if not start_yy:
        return ""
    return str(2000 + int(start_yy))


def format_geoserver_name(template: str, layer: Layer) -> str:
    if not template:
        return ""
    misc = layer.misc or {}
    start_year = str(misc.get("start_year", "") or "")
    end_year = str(misc.get("end_year", "") or "")
    lulc_start, lulc_end = lulc_years_from_layer_name(layer.layer_name or "")
    if lulc_start:
        start_year = lulc_start
        end_year = lulc_end or lulc_start
    elif "{start_year}" in template:
        start_year = _two_digit_hydrological_year(start_year)
        end_year = _two_digit_hydrological_year(end_year or start_year)
    try:
        return template.format(
            district=valid_gee_text(layer.district.district_name.lower()),
            block=valid_gee_text(layer.block.tehsil_name.lower()),
            state=valid_gee_text(layer.state.state_name.lower()),
            start_year=start_year,
            end_year=end_year,
        )
    except (KeyError, IndexError, AttributeError):
        return ""


def _mapping_from_csv_row(row) -> SimpleNamespace:
    return SimpleNamespace(
        layer_type=str(row["layer_type"]),
        layer_name=str(row["layer_name"]),
    )


def _load_layer_mapping_csv():
    config = STACConfig()
    path = config.layer_map_csv
    if os.path.exists(path):
        return pd.read_csv(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.read_csv(constants.LAYER_MAP_GITHUB_URL)
    df.to_csv(path, index=False)
    return df


def _resolve_mapping_from_csv(layer: Layer):
    dataset_name = (layer.dataset.name or "").strip()
    if not dataset_name:
        return None

    try:
        df = _load_layer_mapping_csv()
    except Exception as exc:
        log.warning("STAC mapping CSV load failed: %s", exc)
        return None

    if "db_dataset_name" not in df.columns:
        return None

    candidates = df[df["db_dataset_name"].astype(str).str.strip() == dataset_name]
    if candidates.empty:
        return None
    if len(candidates) == 1:
        return _mapping_from_csv_row(candidates.iloc[0])

    layer_name = (layer.layer_name or "").strip()
    if not layer_name:
        return None

    matches = []
    for _, row in candidates.iterrows():
        template = str(row.get("geoserver_layer_name", "") or "")
        if format_geoserver_name(template, layer) == layer_name:
            matches.append(row)
    if len(matches) == 1:
        return _mapping_from_csv_row(matches[0])
    if not matches:
        log.warning(
            "STAC mapping CSV: no match for layer id=%s dataset=%s name=%s",
            layer.id,
            dataset_name,
            layer_name,
        )
        return None
    return _mapping_from_csv_row(matches[0])


def resolve_layer_mapping(layer: Layer):
    if not layer.dataset_id:
        return None

    dataset_name = (layer.dataset.name or "").strip()
    if not dataset_name:
        return None

    if dataset_name in _LULC_DATASETS and dataset_name != "LULC_level_3":
        # Only level_3 is catalogued for STAC; level_1/level_2 share GeoServer naming.
        return None

    # Only match within this dataset. Never fall back to all auto_stac rows —
    # Admin Boundary / Drainage / NREGA all share `{district}_{block}` templates.
    try:
        candidates = list(
            LayerMapping.objects.filter(db_dataset_name=dataset_name, auto_stac=True)
        )
    except Exception as exc:  # noqa: BLE001 — missing table / migrations
        log.warning(
            "STAC mapping DB unavailable (%s); falling back to CSV for dataset=%s",
            exc,
            dataset_name,
        )
        return _resolve_mapping_from_csv(layer)

    if not candidates:
        return _resolve_mapping_from_csv(layer)

    if len(candidates) == 1:
        return candidates[0]

    layer_name = (layer.layer_name or "").strip()
    if not layer_name:
        return _resolve_mapping_from_csv(layer)

    matches = [
        c
        for c in candidates
        if format_geoserver_name(c.geoserver_layer_name, layer) == layer_name
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        log.warning(
            "STAC mapping DB: no match for layer id=%s dataset=%s name=%s",
            layer.id,
            dataset_name,
            layer_name,
        )
        return _resolve_mapping_from_csv(layer)

    log.info(
        "STAC mapping DB: %d ambiguous matches for layer id=%s; picking first",
        len(matches),
        layer.id,
    )
    return matches[0]


def stac_task_kwargs_for_layer(layer: Layer, mapping=None):
    mapping = mapping or resolve_layer_mapping(layer)
    if mapping is None:
        return None

    misc = layer.misc or {}
    start_year = str(misc.get("start_year", "") or "")
    end_year = str(misc.get("end_year", "") or "")
    lulc_year = lulc_calendar_start_year(layer.layer_name or "")
    if lulc_year:
        start_year = lulc_year
        end_year = lulc_year
    return {
        "layer_type": mapping.layer_type,
        "state": layer.state.state_name,
        "district": layer.district.district_name,
        "block": layer.block.tehsil_name,
        "layer_name": mapping.layer_name,
        "start_year": start_year,
        "end_year": end_year,
        "upload_to_s3": bool(getattr(settings, "STAC_UPLOAD_TO_S3", False)),
        "overwrite_metadata": bool(getattr(settings, "STAC_OVERWRITE_METADATA", True)),
        "layer_id": layer.id,
    }


def stac_collect_target_for_layer(layer: Layer):
    task_kwargs = stac_task_kwargs_for_layer(layer)
    if task_kwargs is None:
        return None
    return {
        "state": task_kwargs["state"],
        "district": task_kwargs["district"],
        "block": task_kwargs["block"],
        "layer_name": task_kwargs["layer_name"],
        "layer_type": task_kwargs["layer_type"],
        "start_year": task_kwargs["start_year"],
        "end_year": task_kwargs["end_year"],
    }
