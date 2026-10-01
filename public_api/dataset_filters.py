"""Query filters for public APIs (active locations and tehsil datasets)."""

import re

from .catalog_document import tehsil_data_type_docs


def _canonical_sheet_name(key):
    """Match ``utilities.openmeteo_format._normalize_key_name`` for tehsil sheets."""
    k = str(key).strip().lower().replace(" ", "_")
    k = k.replace("block", "tehsil")
    k = k.replace("afforestation", "tree_cover_increase")
    k = k.replace("deforestation", "tree_cover_decrease")
    if k == "deltag" or k.startswith("deltag_"):
        k = k.replace("deltag", "delta_g", 1)
    return k

# Canonical tehsil sheet keys people can pass to ``data=``.
# Names and descriptions are read from catalog.json.
# ``all`` returns every sheet present for that tehsil (default when omitted).
TEHSIL_DATA_TYPE_DOCS = tehsil_data_type_docs()

# Extra aliases after ``_normalize_key_name`` (Excel sheet names that differ).
TEHSIL_DATA_ALIASES = {
    "change_detection_afforestation": "change_detection_tree_cover_increase",
    "change_detection_deforestation": "change_detection_tree_cover_decrease",
}

TEHSIL_DATA_TYPE_VALUES = ["all"] + [item[0] for item in TEHSIL_DATA_TYPE_DOCS if item[0] != "all"]


def tehsil_data_type_help_markdown():
    lines = [
        "Omit ``data``, or pass ``data=all``, to return every dataset generated for that tehsil.",
        "The response includes the sheets present in that tehsil's file, one row per micro-watershed.",
        "Pass one or more sheet names to keep only those sheets:",
        "``data=drought,stream_order`` or ``data=drought&data=stream_order``.",
        "",
        "| ``data`` value | What you get |",
        "| --- | --- |",
    ]
    for value, meaning in TEHSIL_DATA_TYPE_DOCS:
        lines.append(f"| ``{value}`` | {meaning} |")
    lines.append("")
    lines.append(
        "Excel aliases such as ``Canopy_height``, ``change_detection_afforestation``, "
        "and ``surfaceWaterBodies_annual`` are accepted and mapped to the names above."
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
    wanted = {_canonical_sheet_name(token) for token in requested_tokens}
    filtered_data = {}
    filtered_units = {}
    for key, rows in tehsil_data.items():
        if _canonical_sheet_name(key) in wanted:
            filtered_data[key] = rows
            if key in tehsil_units:
                filtered_units[key] = tehsil_units[key]
    return {
        **payload,
        "tehsil_data": filtered_data,
        "tehsil_units": filtered_units,
    }
