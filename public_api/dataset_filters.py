"""Query filters for public APIs (active locations and tehsil datasets)."""

import re


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
# ``all`` returns every sheet present for that tehsil (default when omitted).
TEHSIL_DATA_TYPE_DOCS = [
    ("all", "Every dataset available for this tehsil (default if ``data`` is omitted)"),
    ("agroecological", "Agroecological zone attributes per MWS"),
    ("antyodaya", "Antyodaya / SECC socio-economic indicators"),
    ("aquifer_vector", "Aquifer class and area share per MWS"),
    ("canal", "Canal network intersection with the MWS"),
    ("canopy_cover_density", "Tree canopy cover density"),
    ("canopy_height", "Tree canopy height"),
    ("change_detection_cropintensity", "Change detection: cropping intensity"),
    ("change_detection_degradation", "Change detection: land degradation"),
    ("change_detection_shrubchange", "Change detection: shrub change"),
    ("change_detection_tree_cover_decrease", "Change detection: tree-cover decrease (deforestation)"),
    ("change_detection_tree_cover_increase", "Change detection: tree-cover increase (afforestation)"),
    ("change_detection_urbanization", "Change detection: urbanization"),
    ("croppingdrought_kharif", "Kharif cropping vs drought years"),
    ("croppingintensity_annual", "Annual cropping intensity"),
    ("dem", "Elevation / DEM summary per MWS"),
    ("drainage_density", "Drainage density"),
    ("drought", "Drought frequency / weeks (if present as a sheet)"),
    ("drought_causality", "Drought causality classes"),
    ("facilities_proximity", "Distance to facilities (km)"),
    ("factory_csr", "Factory / CSR sites intersecting the MWS"),
    ("green_credit", "Green credit / restoration opportunity"),
    ("hydrological_annual", "Annual hydrology (ET, runoff, precipitation, …)"),
    ("hydrological_seasonal", "Seasonal hydrology"),
    ("lcw_conflict", "Land conflict watch overlay"),
    ("livestock", "Livestock census indicators"),
    ("lulc_vector", "Land use / land cover class shares"),
    ("mining", "Mining overlay"),
    ("mws", "Micro-watershed identity and area"),
    ("mws_connectivity", "MWS drainage connectivity"),
    ("mws_intersect_swb", "Surface water bodies intersecting the MWS"),
    ("mws_intersect_villages", "Villages intersecting the MWS"),
    ("ndvi_shrub", "Shrub NDVI"),
    ("nrega_annual", "Annual MGNREGA works"),
    ("nrega_assets_village", "Village-level MGNREGA assets"),
    ("overall_tree_change", "Overall tree-cover change"),
    ("restoration_vector", "Restoration opportunity classes"),
    ("river", "River network intersection"),
    ("social_economic_indicator", "Social-economic indicator table"),
    ("soge_vector", "Stage of groundwater extraction (SOGE)"),
    ("soil_health", "Soil health parameters"),
    ("soil_type", "Soil type"),
    ("stream_order", "Stream-order length / density"),
    ("surfacewaterbodies_annual", "Annual surface water body extent"),
    ("terrain", "Terrain cluster / morphometry"),
    ("terrain_lulc_plain", "Terrain × LULC on plains"),
    ("terrain_lulc_slope", "Terrain × LULC on slopes"),
]

# Extra aliases after ``_normalize_key_name`` (Excel sheet names that differ).
TEHSIL_DATA_ALIASES = {
    "change_detection_afforestation": "change_detection_tree_cover_increase",
    "change_detection_deforestation": "change_detection_tree_cover_decrease",
}

TEHSIL_DATA_TYPE_VALUES = ["all"] + [item[0] for item in TEHSIL_DATA_TYPE_DOCS if item[0] != "all"]


def tehsil_data_type_help_markdown():
    lines = [
        "Pass ``data=all`` (or omit ``data``) for every sheet. Pass one or more sheet names to filter.",
        "Multiple values: ``data=drought,stream_order`` or ``data=drought&data=stream_order``.",
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
