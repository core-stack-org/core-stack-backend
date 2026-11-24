"""Query filters for public APIs (active locations and tehsil datasets)."""

import re

from stats_generator.sheet_names import TEHSIL_SHEET_SPECS

MWS_FORTNIGHT_METRIC_FIELDS = ("et", "runoff", "precipitation")
MWS_IDENTIFIER_KEYS = ("mws_id", "uid")


def _canonical_sheet_name(key):
    """Match ``utilities.openmeteo_format._normalize_key_name`` for tehsil sheets."""
    k = str(key).strip().lower().replace(" ", "_")
    k = k.replace("block", "tehsil")
    k = k.replace("afforestation", "tree_cover_increase")
    k = k.replace("deforestation", "tree_cover_decrease")
    if k == "deltag" or k.startswith("deltag_"):
        k = k.replace("deltag", "delta_g", 1)
    return k


# Canonical tehsil Excel / JSON sheet keys people can pass to ``data=``.
# Derived from ``stats_generator.sheet_names.TEHSIL_SHEET_SPECS`` so the
# pipeline and API filter share one source of truth and cannot drift.
# ``all`` returns every sheet present for that tehsil (default when omitted).
TEHSIL_DATA_TYPE_DOCS = [
    ("all", "Every dataset available for this tehsil (default if ``data`` is omitted)"),
    *[(api_key, description) for _, api_key, description in TEHSIL_SHEET_SPECS],
    *[(api_key, description) for _, api_key, description in TEHSIL_SHEET_SPECS],
]

# Extra aliases after ``_normalize_key_name`` (Excel sheet names or legacy aliases).
TEHSIL_DATA_ALIASES = {
    "change_detection_afforestation": "change_detection_tree_cover_increase",
    "change_detection_deforestation": "change_detection_tree_cover_decrease",
    "drought": "croppingdrought_kharif",
    "ndvi_shrub": "ndvi",
}

TEHSIL_DATA_TYPE_VALUES = ["all"] + [item[0] for item in TEHSIL_DATA_TYPE_DOCS if item[0] != "all"]


def tehsil_data_type_help_markdown():
    lines = [
        "Pass ``data=all`` (or omit ``data``) for every sheet. Pass one or more sheet names to filter.",
        "Multiple values: ``data=croppingdrought_kharif,stream_order`` or ``data=croppingdrought_kharif&data=stream_order``.",
        "",
        "| ``data`` value | What you get |",
        "| --- | --- |",
    ]
    for value, meaning in TEHSIL_DATA_TYPE_DOCS:
        lines.append(f"| ``{value}`` | {meaning} |")
    lines.append("")
    lines.append(
        "Aliases such as ``drought`` (→ ``croppingdrought_kharif``), ``ndvi_shrub`` (→ ``ndvi``), "
        "``Canopy_height``, ``change_detection_afforestation``, and ``surfaceWaterBodies_annual`` "
        "are accepted and mapped to the canonical sheet names above."
    )
    return "\n".join(lines)


def _place_key(value):
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = re.sub(r"[&\-()]", " ", text)
    text = re.sub(r"[_\s]+", " ", text)
    return text.strip()


def _place_matches(label, query):
    if not query:
        return True
    return _place_key(label) == _place_key(query)


def filter_active_locations(locations, state=None, district=None, block=None):
    """Narrow the state → district → block tree. All filters are optional."""
    if not isinstance(locations, list):
        locations = [locations] if locations else []

    filtered_states = []
    for state_item in locations:
        if not isinstance(state_item, dict):
            continue
        if not _place_matches(state_item.get("label"), state):
            continue
        districts = state_item.get("district") or []
        filtered_districts = []
        for dist in districts:
            if not isinstance(dist, dict):
                continue
            if not _place_matches(dist.get("label"), district):
                continue
            blocks = dist.get("blocks") or []
            if block:
                blocks = [
                    item
                    for item in blocks
                    if isinstance(item, dict) and _place_matches(item.get("label"), block)
                ]
                if not blocks:
                    continue
            filtered_districts.append({**dist, "blocks": blocks})
        if district or block:
            if not filtered_districts:
                continue
            filtered_states.append({**state_item, "district": filtered_districts})
        else:
            filtered_states.append({**state_item, "district": districts})
    return filtered_states


def canonical_tehsil_data_token(token):
    normalized = _canonical_sheet_name(token)
    if normalized == "all":
        return "all"
    return TEHSIL_DATA_ALIASES.get(normalized, normalized)


def allowed_tehsil_data_tokens():
    allowed = {"all"}
    allowed.update(TEHSIL_DATA_TYPE_VALUES)
    allowed.update(TEHSIL_DATA_ALIASES.keys())
    allowed.update(TEHSIL_DATA_ALIASES.values())
    return allowed


def parse_tehsil_data_filter(raw_values):
    """
    Return None for ``all`` / omitted (keep every sheet).
    Return a set of canonical tokens otherwise.
    Raise ValueError for unknown names.
    """
    tokens = []
    for raw in raw_values or []:
        if raw is None:
            continue
        for part in str(raw).split(","):
            part = part.strip()
            if part:
                tokens.append(part)
    if not tokens:
        return None
    canonical = [canonical_tehsil_data_token(token) for token in tokens]
    if any(item == "all" for item in canonical):
        return None
    allowed = allowed_tehsil_data_tokens()
    unknown = [
        token
        for token, canon in zip(tokens, canonical)
        if canon not in allowed
    ]
    if unknown:
        raise ValueError(
            "Unknown data filter value(s): "
            + ", ".join(sorted(set(unknown)))
            + ". Use data=all or one of: "
            + ", ".join(TEHSIL_DATA_TYPE_VALUES)
        )
    return set(canonical)


def filter_tehsil_payload(payload, requested_tokens):
    """Keep only requested sheet keys in ``tehsil_data`` / ``tehsil_units``."""
    if not requested_tokens:
        return payload
    if not isinstance(payload, dict):
        return payload
    tehsil_data = payload.get("tehsil_data") or {}
    tehsil_units = payload.get("tehsil_units") or {}
    wanted = {canonical_tehsil_data_token(token) for token in requested_tokens}
    filtered_data = {}
    filtered_units = {}
    for key, rows in tehsil_data.items():
        if canonical_tehsil_data_token(key) in wanted:
            filtered_data[key] = rows
            if key in tehsil_units:
                filtered_units[key] = tehsil_units[key]
    return {
        **payload,
        "tehsil_data": filtered_data,
        "tehsil_units": filtered_units,
    }


def parse_fields_filter(raw_values):
    """Parse ``fields=a,b`` / ``fields=a&fields=b`` into a deduplicated list, or None."""
    fields = []
    seen = set()
    for raw in raw_values or []:
        if raw is None:
            continue
        for part in str(raw).split(","):
            token = part.strip()
            if token and token.lower() not in seen:
                seen.add(token.lower())
                fields.append(token)
    return fields or None


def validate_mws_fortnight_fields(requested_fields):
    """Validate ``fields=`` tokens for v2 ``get_mws_data`` (fortnight time series)."""
    if not requested_fields:
        return None
    valid_metrics = set(MWS_FORTNIGHT_METRIC_FIELDS)
    valid_all = valid_metrics | {"time", "mws_id"}
    unknown = [f for f in requested_fields if f.lower() not in valid_all]
    if unknown:
        raise ValueError(
            "Unknown field(s): "
            + ", ".join(unknown)
            + ". Valid fields: "
            + ", ".join(MWS_FORTNIGHT_METRIC_FIELDS)
            + "."
        )
    return [f.lower() for f in requested_fields]


def filter_mws_fortnight_payload(payload, requested_fields):
    """Keep ``metadata.mws_id``, ``fortnight.time``, and only requested metric series."""
    if not requested_fields or not isinstance(payload, dict):
        return payload
    normalized = validate_mws_fortnight_fields(requested_fields)
    fortnight = payload.get("fortnight") or {}
    fortnight_units = payload.get("fortnight_units") or {}

    selected_metrics = [
        key for key in MWS_FORTNIGHT_METRIC_FIELDS if key in normalized and key in fortnight
    ]
    filtered_fortnight = {}
    if "time" in fortnight:
        filtered_fortnight["time"] = fortnight["time"]
    for metric in selected_metrics:
        filtered_fortnight[metric] = fortnight[metric]

    filtered_units = {}
    for always_key in ("time", "time_step"):
        if always_key in fortnight_units:
            filtered_units[always_key] = fortnight_units[always_key]
    for metric in selected_metrics:
        if metric in fortnight_units:
            filtered_units[metric] = fortnight_units[metric]

    return {
        **payload,
        "fortnight": filtered_fortnight,
        "fortnight_units": filtered_units,
    }


def filter_mws_v1_timeseries_payload(payload, requested_fields):
    """Filter v1 ``get_mws_data`` time_series rows, always keeping ``mws_id`` and ``date``."""
    if not requested_fields or not isinstance(payload, dict):
        return payload
    rows = payload.get("time_series")
    if not isinstance(rows, list) or not rows:
        return payload
    available_keys = []
    seen = set()
    for row in rows:
        if isinstance(row, dict):
            for k in row.keys():
                if k.lower() not in seen:
                    seen.add(k.lower())
                    available_keys.append(k)
    lower_to_key = {k.lower(): k for k in available_keys}
    valid_all = set(lower_to_key.keys()) | {"mws_id"}
    unknown = [f for f in requested_fields if f.lower() not in valid_all]
    if unknown:
        valid_display = sorted(k for k in available_keys if k.lower() != "date")
        raise ValueError(
            "Unknown field(s): "
            + ", ".join(unknown)
            + ". Valid fields: "
            + ", ".join(valid_display)
            + "."
        )
    keep_keys = set()
    if "date" in lower_to_key:
        keep_keys.add(lower_to_key["date"])
    for f in requested_fields:
        if f.lower() in lower_to_key:
            keep_keys.add(lower_to_key[f.lower()])
    filtered_rows = [
        {k: v for k, v in row.items() if k in keep_keys}
        if isinstance(row, dict)
        else row
        for row in rows
    ]
    return {**payload, "time_series": filtered_rows}


def _filter_indicator_rows(rows, requested_fields):
    """Validate and filter a list of indicator dicts, preserving identifier keys."""
    available_keys = []
    seen = set()
    for row in rows:
        if isinstance(row, dict):
            for k in row.keys():
                if k.lower() not in seen:
                    seen.add(k.lower())
                    available_keys.append(k)
    lower_to_key = {k.lower(): k for k in available_keys}
    id_keys_present = [
        k for k in available_keys if k.lower() in MWS_IDENTIFIER_KEYS
    ]
    # Allow passing either mws_id or uid when an identifier column is present.
    extra_valid = set(MWS_IDENTIFIER_KEYS) if id_keys_present else set()
    unknown = [
        f
        for f in requested_fields
        if f.lower() not in lower_to_key and f.lower() not in extra_valid
    ]
    if unknown:
        raise ValueError(
            "Unknown field(s): "
            + ", ".join(unknown)
            + ". Valid fields: "
            + ", ".join(sorted(available_keys))
            + "."
        )
    keep_keys = set(id_keys_present)
    for f in requested_fields:
        if f.lower() in lower_to_key:
            keep_keys.add(lower_to_key[f.lower()])
    filtered_rows = [
        {k: v for k, v in row.items() if k in keep_keys}
        if isinstance(row, dict)
        else row
        for row in rows
    ]
    return filtered_rows, keep_keys


def filter_kyl_indicator_payload(payload, requested_fields):
    """Keep identifier key(s) + requested fields in v2 ``get_mws_kyl_indicators``."""
    if not requested_fields or not isinstance(payload, dict):
        return payload
    indicators = payload.get("indicators")
    indicator_units = payload.get("indicator_units") or {}
    if isinstance(indicators, dict):
        filtered_rows, keep_keys = _filter_indicator_rows(
            [indicators], requested_fields
        )
        filtered_indicators = filtered_rows[0]
    elif isinstance(indicators, list) and indicators:
        filtered_indicators, keep_keys = _filter_indicator_rows(
            indicators, requested_fields
        )
    else:
        return payload

    filtered_units = {
        k: v for k, v in indicator_units.items() if k in keep_keys
    }
    return {
        **payload,
        "indicators": filtered_indicators,
        "indicator_units": filtered_units,
    }


def filter_kyl_v1_rows(rows, requested_fields):
    """Keep identifier key(s) + requested fields in v1 ``get_mws_kyl_indicators``."""
    if not requested_fields or not isinstance(rows, list) or not rows:
        return rows
    filtered_rows, _ = _filter_indicator_rows(rows, requested_fields)
    return filtered_rows
