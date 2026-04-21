"""
Open-Meteo-style API payloads: aligned time arrays + units, shared across apps.
"""
import re

from utilities.renderers import round_floats

YEAR_SUFFIX_RE = re.compile(
    r"^(?P<metric>.+)_(?P<period>\d{2}-\d{2}|\d{4}(?:[-_]\d{4})?|\d{4})$"
)

UNIT_KEYWORDS = [
    ("precipitation", "mm"),
    ("rainfall", "mm"),
    ("runoff", "mm"),
    ("evapotranspiration", "mm"),
    ("et", "mm"),
    ("area", "ha"),
    ("count", "number"),
    ("total", "number"),
    ("percentage", "%"),
    ("percent", "%"),
    ("ratio", "ratio"),
    ("intensity", "ratio"),
    ("ndvi", "index"),
    ("ndmi", "index"),
    ("drought", "index"),
]


def _infer_unit(metric_name):
    metric = str(metric_name).strip().lower()
    for keyword, unit in UNIT_KEYWORDS:
        if keyword in metric:
            return unit
    return "value"


# Exact column names from KYL JSON (lowercase); anything else falls back to _infer_unit().
KYL_INDICATOR_UNIT_OVERRIDES = {
    "mws_id": "id",
    "terraincluster_id": "id",
    "avg_precipitation": "mm",
    "cropping_intensity_trend": "code",
    "cropping_intensity_avg": "ratio",
    "avg_single_cropped": "ha",
    "avg_double_cropped": "ha",
    "avg_triple_cropped": "ha",
    "avg_wsr_ratio_kharif": "ratio",
    "avg_wsr_ratio_rabi": "ratio",
    "avg_wsr_ratio_zaid": "ratio",
    "avg_kharif_surface_water_mws": "mm",
    "avg_rabi_surface_water_mws": "mm",
    "avg_zaid_surface_water_mws": "mm",
    "trend_swb": "code",
    "trend_g": "code",
    "drought_category": "category",
    "avg_number_dry_spell": "count",
    "avg_runoff": "mm",
    "total_nrega_assets": "count",
    "mws_intersect_villages": "list",
    "degradation_land_area": "ha",
    "increase_in_tree_cover": "ha",
    "decrease_in_tree_cover": "ha",
    "degradation_cropping_intensity": "ratio",
    "urbanization_area": "ha",
    "lulc_slope_category": "category",
    "lulc_plain_category": "category",
    "area_wide_scale_restoration": "ha",
    "area_protection": "ha",
    "aquifer_class": "class",
    "soge_class": "class",
    "lcw_conflict": "flag",
    "mining": "flag",
    "green_credit": "flag",
    "factory_csr": "flag",
}


def indicator_unit_for_key(key):
    """Unit label for a flat indicator column name (KYL & similar tabular APIs)."""
    k = str(key).strip().lower()
    if k in KYL_INDICATOR_UNIT_OVERRIDES:
        return KYL_INDICATOR_UNIT_OVERRIDES[k]
    return _infer_unit(k)


def flat_kyl_indicator_payload(rows):
    """
    Tabular KYL indicators (not time series): one or more flat rows + parallel unit map.

    ``rows`` is a list of dicts (typically one row per ``mws_id``).
    """
    if not isinstance(rows, list):
        rows = []
    if len(rows) == 0:
        return {"indicators": [], "indicator_units": {}}

    all_keys = set()
    for r in rows:
        if isinstance(r, dict):
            all_keys.update(r.keys())
    indicator_units = {k: indicator_unit_for_key(k) for k in sorted(all_keys)}

    if len(rows) == 1:
        return round_floats(
            {
                "indicators": rows[0],
                "indicator_units": indicator_units,
            },
            precision=2,
        )
    return round_floats(
        {"indicators": rows, "indicator_units": indicator_units},
        precision=2,
    )


# get_generated_layer_urls: list of GeoServer layers (no time-series axes).
GENERATED_LAYER_FIELD_UNITS = {
    "layer_name": "name",
    "layer_type": "vector|raster|point|custom",
    "layer_url": "geoserver_wfs_or_wcs_url",
    "layer_version": "version_label",
    "style_url": "style_url_or_empty",
    "gee_asset_path": "earth_engine_asset_id_or_null",
}


def flat_generated_layers_payload(layers):
    """Flat catalog of generated layers with field semantics (not annual/hourly bundles)."""
    if not isinstance(layers, list):
        layers = []
    return round_floats(
        {
            "layers": layers,
            "layer_field_units": dict(GENERATED_LAYER_FIELD_UNITS),
        },
        precision=2,
    )


# get_active_locations: hierarchical state → district → block/tehsil (not time series).
ACTIVE_LOCATION_FIELD_HINTS = {
    "label": "display_name",
    "value": "ordinal_code_in_ui_list",
    "state_id": "state_identifier",
    "district_id": "district_identifier",
    "block_id": "block_tehsil_identifier",
    "district": "districts_under_state",
    "blocks": "blocks_tehsils_under_district",
}


def flat_active_locations_payload(locations):
    """Nested admin tree with explicit field semantics (no annual/hourly placeholders)."""
    if isinstance(locations, dict):
        locations = [locations]
    elif not isinstance(locations, list):
        locations = []
    return round_floats(
        {
            "locations": locations,
            "location_field_hints": dict(ACTIVE_LOCATION_FIELD_HINTS),
        },
        precision=2,
    )


ADMIN_DETAIL_FIELD_HINTS = {
    "State": "state_name",
    "District": "district_name",
    "Tehsil": "tehsil_or_block_name",
}


def flat_admin_detail_payload(data):
    """Flat admin details for lat/lon lookup (no metadata/annual wrappers)."""
    if isinstance(data, dict) and isinstance(data.get("metadata"), dict):
        data = data.get("metadata")
    if not isinstance(data, dict):
        data = {}
    admin_details = {
        "State": data.get("State"),
        "District": data.get("District"),
        "Tehsil": data.get("Tehsil"),
    }
    return round_floats(
        {
            "admin_details": admin_details,
            "admin_field_hints": dict(ADMIN_DETAIL_FIELD_HINTS),
        },
        precision=2,
    )


MWS_BY_LATLON_FIELD_HINTS = {
    "uid": "mws_identifier",
    "State": "state_name",
    "District": "district_name",
    "Tehsil": "tehsil_or_block_name",
}


def flat_mws_by_latlon_payload(data):
    """Flat MWS id + admin details for lat/lon lookup (no annual/hourly blocks)."""
    if isinstance(data, dict) and isinstance(data.get("metadata"), dict):
        merged = dict(data.get("metadata"))
        if "uid" in data and "uid" not in merged:
            merged["uid"] = data.get("uid")
        data = merged
    if not isinstance(data, dict):
        data = {}
    mws_details = {
        "uid": data.get("uid"),
        "State": data.get("State"),
        "District": data.get("District"),
        "Tehsil": data.get("Tehsil"),
    }
    return round_floats(
        {
            "mws_details": mws_details,
            "mws_field_hints": dict(MWS_BY_LATLON_FIELD_HINTS),
        },
        precision=2,
    )


MWS_REPORT_FIELD_HINTS = {
    "Mws_report_url": "mws_pdf_or_html_report_url",
}


def flat_mws_report_url_payload(data):
    """Flat MWS report URL payload (no metadata/annual wrappers)."""
    if isinstance(data, dict) and isinstance(data.get("metadata"), dict):
        data = data.get("metadata")
    if not isinstance(data, dict):
        data = {}
    report = {"Mws_report_url": data.get("Mws_report_url")}
    return round_floats(
        {
            "report": report,
            "report_field_hints": dict(MWS_REPORT_FIELD_HINTS),
        },
        precision=2,
    )


MWS_GEOMETRY_FIELD_HINTS = {
    "uid": "mws_identifier",
    "state": "normalized_state_name",
    "district": "normalized_district_name",
    "tehsil": "normalized_tehsil_name",
    "geometry": "geojson_geometry_object",
}


def flat_mws_geometry_payload(data):
    """Flat MWS geometry payload for a single uid."""
    if isinstance(data, dict) and isinstance(data.get("metadata"), dict):
        merged = dict(data.get("metadata"))
        if "geometry" in data and "geometry" not in merged:
            merged["geometry"] = data.get("geometry")
        data = merged
    if not isinstance(data, dict):
        data = {}
    geometry_data = {
        "uid": data.get("uid"),
        "state": data.get("state"),
        "district": data.get("district"),
        "tehsil": data.get("tehsil"),
        "geometry": data.get("geometry"),
    }
    return round_floats(
        {
            "mws_geometry": geometry_data,
            "mws_geometry_field_hints": dict(MWS_GEOMETRY_FIELD_HINTS),
        },
        precision=2,
    )


VILLAGE_GEOMETRY_FIELD_HINTS = {
    "village_id": "vill_ID_from_layer",
    "village_name": "vill_name_from_layer",
    "state": "normalized_state_name",
    "district": "normalized_district_name",
    "tehsil": "normalized_tehsil_name",
    "geometry": "geojson_geometry_object",
}


def flat_village_geometries_payload(rows):
    """Flat village geometry payload for one or more villages."""
    if not isinstance(rows, list):
        rows = []
    return round_floats(
        {
            "villages": rows,
            "village_field_hints": dict(VILLAGE_GEOMETRY_FIELD_HINTS),
        },
        precision=2,
    )


def error_envelope(message, details=None):
    payload = {"status": "error", "error_message": message, "error": message}
    if details is not None:
        payload["details"] = details
    return round_floats(payload, precision=2)


def success_envelope(data_block):
    return round_floats(
        {"status": "success", "error_message": None, "data": data_block},
        precision=2,
    )


def _period_sort_key(period_label):
    if "-" in period_label:
        first = period_label.split("-")[0]
        if len(first) == 2:
            return int(f"20{first}")
        return int(first)
    return int(period_label)


def _normalize_period_label(period):
    if len(period) == 5 and period[2] == "-":
        start = int(period[:2])
        end = int(period[3:])
        return f"{2000 + start}-{2000 + end}"
    if len(period) == 9 and period[4] == "-":
        return period
    if len(period) == 9 and period[4] == "_":
        start = int(period[:4])
        end = int(period[5:])
        return f"{start}-{end}"
    if len(period) == 4:
        year = int(period)
        return f"{year}-{year + 1}"
    return period


def _has_timeseries_keys(payload):
    if not isinstance(payload, dict):
        return False
    return any(YEAR_SUFFIX_RE.match(str(key)) for key in payload.keys())


def annual_structure_from_dict(item):
    """Fold metric_2017, metric_17-18, etc. into annual.time + aligned arrays."""
    if not isinstance(item, dict):
        return round_floats(
            {
                "metadata": item,
                "annual": {"time": []},
                "annual_units": {"time": "period"},
            },
            precision=2,
        )

    grouped = {}
    metadata = {}

    for key, value in item.items():
        match = YEAR_SUFFIX_RE.match(key)
        if not match:
            metadata[key] = _convert_nested_timeseries(value)
            continue

        metric = match.group("metric")
        period_raw = match.group("period")
        period_label = _normalize_period_label(period_raw)
        grouped.setdefault(period_label, {})[metric] = value

    if not grouped:
        return round_floats(
            {
                "metadata": metadata,
                "annual": {"time": []},
                "annual_units": {"time": "period"},
            },
            precision=2,
        )

    periods = sorted(grouped.keys(), key=_period_sort_key)
    metric_names = sorted({m for period in periods for m in grouped[period].keys()})

    annual = {"time": periods}
    annual_units = {"time": "period"}
    for metric in metric_names:
        annual[metric] = [grouped[period].get(metric) for period in periods]
        annual_units[metric] = _infer_unit(metric)

    return round_floats(
        {
            "metadata": metadata,
            "annual": annual,
            "annual_units": annual_units,
        },
        precision=2,
    )


def _convert_nested_timeseries(value):
    if isinstance(value, dict):
        if _has_timeseries_keys(value):
            return annual_structure_from_dict(value)
        return {k: _convert_nested_timeseries(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_convert_nested_timeseries(v) for v in value]
    return value


def fortnight_structure_from_mws(payload):
    """GeoServer MWS JSON (15-day / fortnightly steps) -> aligned arrays (Open-Meteo-style keys).

    Each ``time_series`` row is one period (~15 days); ``date`` is the period start (ISO).
    """
    fortnight_units = {
        "time": "iso8601",
        "time_step": "15_days",
        "et": "mm",
        "runoff": "mm",
        "precipitation": "mm",
    }
    if not isinstance(payload, dict):
        return round_floats(
            {
                "metadata": payload,
                "fortnight": {"time": []},
                "fortnight_units": fortnight_units,
            },
            precision=2,
        )

    if not isinstance(payload.get("time_series"), list):
        return round_floats(
            {
                "metadata": payload,
                "fortnight": {"time": []},
                "fortnight_units": fortnight_units,
            },
            precision=2,
        )

    rows = payload.get("time_series", [])
    fortnight = {
        "time": [row.get("date") for row in rows],
        "et": [row.get("et") for row in rows],
        "runoff": [row.get("runoff") for row in rows],
        "precipitation": [row.get("precipitation") for row in rows],
    }
    return round_floats(
        {
            "metadata": {"mws_id": payload.get("mws_id")},
            "fortnight": fortnight,
            "fortnight_units": fortnight_units,
        },
        precision=2,
    )


# Backwards compatibility for imports; returns fortnight keys only.
hourly_structure_from_mws = fortnight_structure_from_mws


def legacy_hourly_to_fortnight_inner_block(inner):
    """Mongo / old clients may still store ``hourly`` / ``hourly_units``; expose ``fortnight``."""
    if not isinstance(inner, dict):
        return inner
    if "fortnight" in inner:
        return inner
    if "hourly" not in inner:
        return inner
    migrated = dict(inner)
    migrated["fortnight"] = migrated.pop("hourly")
    if "hourly_units" in migrated:
        migrated["fortnight_units"] = migrated.pop("hourly_units")
    return migrated


def metadata_only(payload):
    """Non time-series responses: metadata + empty hourly placeholders (unused axis)."""
    if isinstance(payload, dict):
        meta = dict(payload)
    else:
        meta = {"value": payload}
    return round_floats(
        {
            "metadata": meta,
            "hourly": {"time": []},
            "hourly_units": {"time": "iso8601"},
        },
        precision=2,
    )


def normalize_payload(payload):
    """
    Best-effort Open-Meteo shape for arbitrary API results.
    """
    if isinstance(payload, dict) and "metadata" in payload and (
        ("fortnight" in payload and isinstance(payload.get("fortnight"), dict))
        or ("hourly" in payload and isinstance(payload.get("hourly"), dict))
        or ("annual" in payload and isinstance(payload.get("annual"), dict))
    ):
        return round_floats(legacy_hourly_to_fortnight_inner_block(payload), precision=2)
    if isinstance(payload, dict) and isinstance(
        payload.get("time_series"), list
    ):
        return fortnight_structure_from_mws(payload)
    if isinstance(payload, list):
        normalized_items = []
        for x in payload:
            if isinstance(x, dict):
                normalized_items.append(annual_structure_from_dict(x))
            else:
                normalized_items.append(metadata_only(x))
        return round_floats(
            {
                "metadata": {"items": normalized_items},
                "hourly": {"time": []},
                "hourly_units": {"time": "iso8601"},
            },
            precision=2,
        )
    if isinstance(payload, dict):
        return annual_structure_from_dict(payload)
    return metadata_only(payload)
