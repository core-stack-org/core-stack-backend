import re
from moderation.views import EE_AVAILABLE

STRUCTURE_ALIASES = {
    "continuous_contour_trenches": "continuous_contour_trench",
    "continuous_contour_trench_cct": "continuous_contour_trench",
    "continuous_contour_trenches_cct": "continuous_contour_trench",
    "staggered_contour_trenches": "staggered_contour_trench",
    "earthen_gully_plug": "earthen_gully_plugs",
    "earthen_gully_plugs_egp": "earthen_gully_plugs",
    "drainage_soakage_channel": "drainage_soakage_channels",
    "drainage_soakage_channels": "drainage_soakage_channels",
    "trench_cum_bund_network": "trench_cum_bund",
    # 5% model variations
    "5_model_structure": "5_percent_model",
    "5_percent_model_structure": "5_percent_model",
    "5_percent_model": "5_percent_model",
    # 30_40 model variations
    "30_40_model_structure": "30_40_model",
    "30_40_model": "30_40_model",
}


def normalize_structure_name(structure_type: str) -> str:
    if not structure_type:
        return ""

    s = str(structure_type).strip().lower()

    # remove anything inside brackets/parentheses: "(cct)" "(egp)" etc.
    s = re.sub(r"\(.*?\)", " ", s)

    # IMPORTANT: handle % BEFORE underscore-joining
    s = s.replace("%", " percent ")

    # normalize separators
    s = s.replace("&", " and ")
    s = s.replace("/", " ")
    s = s.replace("-", " ")

    # remove non-alphanumeric (keep spaces for now)
    s = re.sub(r"[^a-z0-9\s]", " ", s)

    # optional plural standardization
    s = s.replace("trenches", "trench")

    # collapse whitespace -> underscores (FINAL canonical form)
    s = "_".join(s.split()).strip("_")

    # apply aliases
    return STRUCTURE_ALIASES.get(s, s)


LULC_MODE_BY_STRUCTURE = {
    # A) On-spot
    "farm_pond": "point",
    "farm_bund": "point",
    "30_40_model": "point",
    "well": "point",
    "soakage_pit": "point",
    "recharge_pit": "point",
    "rock_fill_dam": "point",
    "graded_bund": "point",
    "stone_bund": "point",
    "earthen_gully_plugs": "point",
    # B) 30m dominant
    "canal": "buffer",
    "diversion_drain": "buffer",
    "drainage_soakage_channels": "buffer",
    "check_dam": "buffer",
    "percolation_tank": "buffer",
    "community_pond": "buffer",
    "trench_cum_bund": "buffer",
    # C) downstream
    "contour_bund": "downstream",
    "loose_boulder_structure": "downstream",
    "continuous_contour_trench": "downstream",
    "staggered_contour_trench": "downstream",
    "wat": "downstream",
}


def compute_lulc_point(lat: float, lon: float) -> str | None:
    if not EE_AVAILABLE:
        return None

    lulc = _lulc_image()
    pt = ee.Geometry.Point([lon, lat])
    scale = lulc.projection().nominalScale()

    val = lulc.reduceRegion(
        reducer=ee.Reducer.first(), geometry=pt, scale=scale, maxPixels=1e9
    ).get("lulc")

    v = val.getInfo() if val else None
    if v is None:
        return None

    return LULC_NAMES.get(int(round(float(v))))


def compute_lulc_auto(lat: float, lon: float, structure_type: str) -> str | None:
    key = normalize_structure_name(structure_type)
    mode = LULC_MODE_BY_STRUCTURE.get(key, "point")

    if mode == "point":
        return compute_lulc_point(lat, lon)
    if mode == "buffer":
        return compute_lulc_buffer_dominant(lat, lon, buffer_m=30)
    if mode == "downstream":
        return compute_lulc_downstream(lat, lon)

    return compute_lulc_point(lat, lon)
