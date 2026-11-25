import re

import ee
import pandas as pd


SQM_PER_HECTARE = 10000.0
ANNUAL_AREA_PATTERN = re.compile(r"^area_(\d{2})-(\d{2})$")


def _discover_annual_area_columns(columns):
    """Discover (area_YY-YY, k_YY-YY) pairs newest-first from column names."""
    discovered = []
    for column in columns:
        match = ANNUAL_AREA_PATTERN.fullmatch(column)
        if not match:
            continue
        start_yy, end_yy = match.groups()
        k_column = f"k_{start_yy}-{end_yy}"
        if k_column in columns:
            discovered.append(((int(end_yy), int(start_yy)), column, k_column))

    discovered.sort(reverse=True)
    return [(area_col, k_col) for _, area_col, k_col in discovered]


def add_area_ored_to_gdf(gdf):
    result = gdf.copy()
    area_ored = (
        pd.to_numeric(result["area_ored"], errors="coerce")
        if "area_ored" in result
        else pd.Series(float("nan"), index=result.index, dtype=float)
    )

    for area_column, k_column in _discover_annual_area_columns(result.columns):
        area = pd.to_numeric(result[area_column], errors="coerce")
        k = pd.to_numeric(result[k_column], errors="coerce")
        candidate = area.multiply(100).divide(k)
        area_ored = area_ored.fillna(candidate.where(k.notna() & k.ne(0)))

    if "total_area" in result:
        total_area = pd.to_numeric(result["total_area"], errors="coerce")
        area_ored = area_ored.fillna(total_area)

    result["area_ored"] = area_ored
    return result


def _gee_number_and_presence(feature, property_name):
    value = feature.get(property_name)
    missing = ee.Algorithms.IsEqual(value, None)
    number = ee.Number(ee.Algorithms.If(missing, 0, value))
    present = ee.Number(ee.Algorithms.If(missing, 0, 1)).eq(1)
    return number, present


def gee_area_ored(feature):
    feature = ee.Feature(feature)
    geometry_area = feature.geometry().area(maxError=1).divide(SQM_PER_HECTARE)

    total_area, has_total_area = _gee_number_and_presence(feature, "total_area")
    base_derived = ee.Number(
        ee.Algorithms.If(has_total_area, total_area, geometry_area)
    )

    area_properties = (
        feature.propertyNames()
        .filter(ee.Filter.stringStartsWith("item", "area_"))
        .filter(ee.Filter.neq("item", "area_ored"))
        .sort()
    )

    def _derive_from_area_property(prop_name, acc):
        prop_name = ee.String(prop_name)
        suffix = prop_name.slice(5)
        is_annual = suffix.match("^[0-9]{2}-[0-9]{2}$").length().gt(0)
        k_prop = ee.String("k_").cat(suffix)
        area, has_area = _gee_number_and_presence(feature, prop_name)
        k, has_k = _gee_number_and_presence(feature, k_prop)
        valid = is_annual.And(has_area).And(has_k).And(k.neq(0))
        candidate = area.multiply(100).divide(k)
        return ee.Number(ee.Algorithms.If(valid, candidate, acc))

    derived = ee.Number(
        area_properties.iterate(_derive_from_area_property, base_derived)
    )

    existing, has_existing = _gee_number_and_presence(feature, "area_ored")
    return ee.Number(ee.Algorithms.If(has_existing, existing, derived))


def ensure_gee_area_ored(feature):
    feature = ee.Feature(feature)
    return feature.set("area_ored", gee_area_ored(feature))
