import re
import ee
from utilities.constants import LULC_ASSET, WWF_HYDROSHEDS_DRAINAGE_DIRECTION
from utilities.gee_utils import ee_initialize
from moderation.utils.utils import LULC_MODE_BY_STRUCTURE

EE_AVAILABLE = ee_initialize()


def _lulc_image():
    return ee.Image(LULC_ASSET).select(0).rename("lulc")


LULC_NAMES = {
    0: "Background",
    1: "Built-up",
    2: "Kharif water",
    3: "Kharif and rabi water",
    4: "Kharif, rabi and zaid water",
    5: "Croplands",
    6: "Trees/forest",
    7: "Barren lands",
    8: "Single Kharif cropping",
    9: "Single Non-Kharif cropping",
    10: "Double cropping",
    11: "Triple cropping",
    12: "Shrubs/Scrubs",
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


def compute_lulc_buffer_dominant(
    lat: float, lon: float, buffer_m: int = 30
) -> str | None:
    if not EE_AVAILABLE:
        return None

    lulc = _lulc_image()
    pt = ee.Geometry.Point([lon, lat])
    buf = pt.buffer(buffer_m)
    scale = lulc.projection().nominalScale()

    hist = ee.Dictionary(
        lulc.reduceRegion(
            reducer=ee.Reducer.frequencyHistogram(),
            geometry=buf,
            scale=scale,
            maxPixels=1e9,
            tileScale=4,
        ).get("lulc")
    )

    # empty/masked
    if hist.size().getInfo() == 0:
        return None

    keys = hist.keys()
    counts = hist.values()

    max_count = counts.reduce(ee.Reducer.max())
    max_idx = counts.indexOf(max_count)

    dom_key = ee.String(keys.get(max_idx))  # "10" or "10.0"
    dom_id = ee.Number.parse(dom_key)  # safe parse

    dom_val = dom_id.getInfo()
    if dom_val is None:
        return None

    return LULC_NAMES.get(int(dom_val))


def compute_lulc_downstream(lat: float, lon: float, n_steps: int = 3) -> str | None:
    """
    True downstream traversal using D8 flow direction (HydroSHEDS).
    Moves n_steps along flow direction and returns LULC at final point.
    """

    if not EE_AVAILABLE:
        return None

    # -----------------------------
    # Load datasets
    # -----------------------------
    fdir = ee.Image(WWF_HYDROSHEDS_DRAINAGE_DIRECTION).select("b1")
    lulc = _lulc_image()

    pt = ee.Geometry.Point([lon, lat])
    cell = fdir.projection().nominalScale()

    # -----------------------------
    # D8 direction offsets
    # -----------------------------
    D8 = {
        1: (1, 0),  # E
        2: (1, -1),  # SE
        4: (0, -1),  # S
        8: (-1, -1),  # SW
        16: (-1, 0),  # W
        32: (-1, 1),  # NW
        64: (0, 1),  # N
        128: (1, 1),  # NE
    }

    current_pt = pt

    for _ in range(n_steps):

        # Sample flow direction at current point
        dir_val = fdir.reduceRegion(
            reducer=ee.Reducer.first(), geometry=current_pt, scale=cell, maxPixels=1e9
        ).get("b1")

        dir_val = dir_val.getInfo() if dir_val else None

        if dir_val is None:
            break

        dir_val = int(dir_val)

        if dir_val not in D8:
            break

        dx_cell, dy_cell = D8[dir_val]

        dx_m = dx_cell * cell.getInfo()
        dy_m = dy_cell * cell.getInfo()

        # Move in projected coordinate system
        pt_3857 = current_pt.transform("EPSG:3857", 1)
        coords = pt_3857.coordinates().getInfo()

        new_x = coords[0] + dx_m
        new_y = coords[1] + dy_m

        current_pt = ee.Geometry.Point([new_x, new_y], "EPSG:3857").transform(
            "EPSG:4326", 1
        )

    # -----------------------------
    # Sample LULC at downstream point
    # -----------------------------
    lulc_val = lulc.reduceRegion(
        reducer=ee.Reducer.first(), geometry=current_pt, scale=30, maxPixels=1e9
    ).get("lulc")

    lulc_val = lulc_val.getInfo() if lulc_val else None

    if lulc_val is None:
        return None

    return LULC_NAMES.get(int(round(float(lulc_val))))


def compute_lulc_auto(lat: float, lon: float, structure_type: str) -> str | None:
    mode = LULC_MODE_BY_STRUCTURE.get(structure_type, "point")

    if mode == "point":
        return compute_lulc_point(lat, lon)
    if mode == "buffer":
        return compute_lulc_buffer_dominant(lat, lon, buffer_m=30)
    if mode == "downstream":
        return compute_lulc_downstream(lat, lon)

    return compute_lulc_point(lat, lon)
