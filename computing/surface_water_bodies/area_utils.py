import ee
import pandas as pd


SQM_PER_HECTARE = 10000.0
AREA_YEARS = tuple(range(2018, 2027))


def _short_year(end_year):
    return f"{(end_year - 1) % 100:02d}-{end_year % 100:02d}"


def add_area_ored_to_gdf(gdf):
    result = gdf.copy()
    area_ored = (
        pd.to_numeric(result["area_ored"], errors="coerce")
        if "area_ored" in result
        else pd.Series(float("nan"), index=result.index, dtype=float)
    )

    for year in reversed(AREA_YEARS):
        short_year = _short_year(year)
        area_column = f"area_{short_year}"
        k_column = f"k_{short_year}"
        if area_column in result and k_column in result:
            area = pd.to_numeric(result[area_column], errors="coerce")
            k = pd.to_numeric(result[k_column], errors="coerce")
            candidate = area.multiply(100).divide(k)
            area_ored = area_ored.fillna(candidate.where(k.notna() & k.ne(0)))

        legacy_area_column = f"area_{year}"
        legacy_k_column = f"k_{year}"
        if legacy_area_column in result and legacy_k_column in result:
            area = pd.to_numeric(result[legacy_area_column], errors="coerce")
            k = pd.to_numeric(result[legacy_k_column], errors="coerce")
            candidate = area.multiply(100).divide(k).divide(SQM_PER_HECTARE)
            area_ored = area_ored.fillna(candidate.where(k.notna() & k.ne(0)))

    if "total_area" in result:
        total_area = pd.to_numeric(result["total_area"], errors="coerce")
        area_ored = area_ored.fillna(total_area)
    elif "total_area_m2" in result:
        total_area = pd.to_numeric(result["total_area_m2"], errors="coerce").divide(
            SQM_PER_HECTARE
        )
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

    total_area_m2, has_total_area_m2 = _gee_number_and_presence(
        feature, "total_area_m2"
    )
    total_area, has_total_area = _gee_number_and_presence(feature, "total_area")
    annual_area_divisor = ee.Number(
        ee.Algorithms.If(has_total_area_m2, SQM_PER_HECTARE, 1)
    )
    derived = ee.Number(
        ee.Algorithms.If(
            has_total_area,
            total_area,
            ee.Algorithms.If(
                has_total_area_m2,
                total_area_m2.divide(SQM_PER_HECTARE),
                geometry_area,
            ),
        )
    )

    for year in AREA_YEARS:
        legacy_area, has_legacy_area = _gee_number_and_presence(
            feature, f"area_{year}"
        )
        legacy_k, has_legacy_k = _gee_number_and_presence(feature, f"k_{year}")
        legacy_valid = has_legacy_area.And(has_legacy_k).And(legacy_k.neq(0))
        legacy_candidate = (
            legacy_area.multiply(100).divide(legacy_k).divide(annual_area_divisor)
        )
        derived = ee.Number(ee.Algorithms.If(legacy_valid, legacy_candidate, derived))

        short_year = _short_year(year)
        area, has_area = _gee_number_and_presence(feature, f"area_{short_year}")
        k, has_k = _gee_number_and_presence(feature, f"k_{short_year}")
        valid = has_area.And(has_k).And(k.neq(0))
        candidate = area.multiply(100).divide(k)
        derived = ee.Number(ee.Algorithms.If(valid, candidate, derived))

    existing, has_existing = _gee_number_and_presence(feature, "area_ored")
    return ee.Number(ee.Algorithms.If(has_existing, existing, derived))


def ensure_gee_area_ored(feature):
    feature = ee.Feature(feature)
    return feature.set("area_ored", gee_area_ored(feature))
