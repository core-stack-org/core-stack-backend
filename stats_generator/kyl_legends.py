"""
Single source of truth for KYL coded indicator maps and API legends (IMP-17).

Both ``stats_generator.mws_indicators`` (encoding labels → codes) and
``utilities.openmeteo_format`` (exposing ``indicator_legends`` in the API
response) import these constants so generator codes and API legends cannot drift.
"""


def _group_labels_by_code(label_to_code):
    """Invert a ``{label: code}`` map into ``{str(code): 'label1, label2'}``."""
    grouped = {}
    for label, code in label_to_code.items():
        grouped.setdefault(code, []).append(label)
    return {
        str(code): ", ".join(grouped[code])
        for code in sorted(grouped)
    }


# Trend codes (Mann-Kendall & NDVI shrub trend)
TREND_LABEL_TO_CODE = {
    "decreasing": -1,
    "no trend": 0,
    "increasing": 1,
}
TREND_LABEL_TO_STR_CODE = {
    label: str(code) for label, code in TREND_LABEL_TO_CODE.items()
}
TREND_CODE_LEGEND = {
    str(code): label for label, code in TREND_LABEL_TO_CODE.items()
}

# Drought category (count of years with >= 5 weeks moderate + severe drought, capped at 2)
DROUGHT_CATEGORY_LEGEND = {
    "0": "No year with at least 5 weeks of moderate or severe drought",
    "1": "One year with at least 5 weeks of moderate or severe drought",
    "2": "Two or more years with at least 5 weeks of moderate or severe drought",
}

# Principal aquifer class
AQUIFER_CLASS_MAP = {
    0: "Hard Rock",
    1: "Alluvial",
}
AQUIFER_CLASS_TO_ID = {label: code for code, label in AQUIFER_CLASS_MAP.items()}
AQUIFER_CLASS_LEGEND = {
    str(code): label for code, label in AQUIFER_CLASS_MAP.items()
}

# Stage of groundwater extraction (SOGE)
SOGE_CLASS_MAP = {
    0: "Safe",
    1: "Semi-Critical",
    2: "Critical",
    3: "Over Exploited",
    4: "Not Assessed",
}
SOGE_CLASS_TO_ID = {label: code for code, label in SOGE_CLASS_MAP.items()}
SOGE_CLASS_LEGEND = {
    str(code): label for code, label in SOGE_CLASS_MAP.items()
}

# Soil drainage class (note: code 3 is Imperfectly drained, code 2 is Poorly/Very poorly drained)
SOIL_DRAINAGE_LABEL_TO_CODE = {
    "Excessively drained": 0,
    "Somewhat excessively drained": 0,
    "Well drained": 1,
    "Moderately well drained": 1,
    "Imperfectly drained": 3,
    "Poorly drained": 2,
    "Very poorly drained": 2,
}
SOIL_DRAINAGE_LEGEND = _group_labels_by_code(SOIL_DRAINAGE_LABEL_TO_CODE)

# Subsoil texture class
SOIL_TEXTURE_LABEL_TO_CODE = {
    "Clay (heavy)": 3,
    "Silty clay": 3,
    "Clay": 3,
    "Sandy clay": 3,
    "Sandy clay loam": 2,
    "Silty clay loam": 2,
    "Clay loam": 2,
    "Silt": 1,
    "Silt loam": 1,
    "Loam": 1,
    "sandy loam": 0,
    "Loamy sand": 0,
    "sand": 0,
}
SOIL_TEXTURE_LEGEND = _group_labels_by_code(SOIL_TEXTURE_LABEL_TO_CODE)

# Binary presence flags (0/1 integers)
LCW_CONFLICT_LEGEND = {"0": "No conflict record", "1": "Conflict record present"}
MINING_LEGEND = {"0": "No mining record", "1": "Mining record present"}
GREEN_CREDIT_LEGEND = {"0": "No green credit record", "1": "Green credit record present"}
FACTORY_CSR_LEGEND = {"0": "No factory CSR record", "1": "Factory CSR record present"}

# Canonical KYL indicator key -> {code_as_str: human_meaning}
KYL_INDICATOR_LEGENDS = {
    "trend_annual_rainfall": TREND_CODE_LEGEND,
    "cropping_intensity_trend": TREND_CODE_LEGEND,
    "trend_swb": TREND_CODE_LEGEND,
    "trend_g": TREND_CODE_LEGEND,
    "shrub_trend": TREND_CODE_LEGEND,
    "drought_category": DROUGHT_CATEGORY_LEGEND,
    "aquifer_class": AQUIFER_CLASS_LEGEND,
    "soge_class": SOGE_CLASS_LEGEND,
    "soil_drainage": SOIL_DRAINAGE_LEGEND,
    "soil_texture": SOIL_TEXTURE_LEGEND,
    "lcw_conflict": LCW_CONFLICT_LEGEND,
    "mining": MINING_LEGEND,
    "green_credit": GREEN_CREDIT_LEGEND,
    "factory_csr": FACTORY_CSR_LEGEND,
}


def indicator_legends_for_keys(keys):
    """Return ``{indicator_key: {code: label}}`` for coded keys present in ``keys``."""
    return {
        k: dict(KYL_INDICATOR_LEGENDS[k])
        for k in keys
        if k in KYL_INDICATOR_LEGENDS
    }
