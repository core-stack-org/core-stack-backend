"""
SPEI (Standardized Precipitation-Evapotranspiration Index) computation engine.

Uses CHIRPS precipitation and MODIS ET/PET data on Google Earth Engine
to compute SPEI values and classify drought severity (D0-D4).

SPEI Thresholds (standard classification):
    SPEI <= -0.5  → D0 (Abnormally Dry)
    SPEI <= -1.0  → D1 (Moderate Drought)
    SPEI <= -1.5  → D2 (Severe Drought)
    SPEI <= -2.0  → D3 (Extreme Drought)
    SPEI <= -2.5  → D4 (Exceptional Drought)
"""

import ee
from datetime import date

from utilities.gee_utils import (
    ee_initialize,
    get_gee_dir_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    check_task_status,
    make_asset_public,
    create_gee_dir,
)
from utilities.constants import GEE_PATHS

# SPEI severity thresholds
SPEI_THRESHOLDS = {
    "D4": -2.5,
    "D3": -2.0,
    "D2": -1.5,
    "D1": -1.0,
    "D0": -0.5,
}


def classify_spei(spei_value):
    """
    Classify an SPEI value into drought severity.
    Returns severity code (D0-D4) or None if no drought.
    """
    if spei_value is None:
        return None
    if spei_value <= SPEI_THRESHOLDS["D4"]:
        return "D4"
    elif spei_value <= SPEI_THRESHOLDS["D3"]:
        return "D3"
    elif spei_value <= SPEI_THRESHOLDS["D2"]:
        return "D2"
    elif spei_value <= SPEI_THRESHOLDS["D1"]:
        return "D1"
    elif spei_value <= SPEI_THRESHOLDS["D0"]:
        return "D0"
    return None


def compute_spei_for_aoi(
    roi_path,
    target_date=None,
    accumulation_months=1,
    gee_account_id=None,
):
    """
    Compute SPEI for an AoI using CHIRPS precipitation and MODIS ET/PET.

    Args:
        roi_path: GEE asset path for the region of interest FeatureCollection
        target_date: Date to compute SPEI for (defaults to current month)
        accumulation_months: SPEI accumulation period (1, 3, 6, or 12 months)
        gee_account_id: GEE service account ID

    Returns:
        list of dicts with keys: uid, spei_value, severity, area_sq_km, geometry
    """
    ee_initialize(gee_account_id)

    if target_date is None:
        target_date = date.today()

    roi = ee.FeatureCollection(roi_path)

    # Data sources
    chirps = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY").select("precipitation")
    modis_et = ee.ImageCollection("MODIS/061/MOD16A2GF").select(["ET", "PET"])

    target_year = target_date.year
    target_month = target_date.month

    # Define current period
    period_end = ee.Date.fromYMD(target_year, target_month, 1)
    period_start = period_end.advance(-accumulation_months, "month")

    # Compute current period P - PET (water balance)
    current_precip = (
        chirps.filterDate(period_start, period_end).sum().rename("precipitation")
    )

    # MODIS ET is in units of kg/m²/8day × 0.1, scale factor applied
    current_et_collection = modis_et.filterDate(period_start, period_end)
    current_pet = current_et_collection.select("PET").sum().multiply(0.1).rename("PET")

    current_water_balance = current_precip.subtract(current_pet)

    # Compute long-term statistics (1981-2020 for CHIRPS, 2000-2020 for MODIS)
    reference_start_year = 2001  # MODIS availability
    reference_end_year = target_year - 1

    years = ee.List.sequence(reference_start_year, reference_end_year)

    def get_yearly_water_balance(year):
        """Compute P - PET for the same calendar window in a reference year."""
        y = ee.Number(year).int()
        start = ee.Date.fromYMD(y, target_month, 1).advance(
            -accumulation_months, "month"
        )
        end = ee.Date.fromYMD(y, target_month, 1)

        p = chirps.filterDate(start, end).sum()
        pet = modis_et.filterDate(start, end).select("PET").sum().multiply(0.1)
        wb = p.subtract(pet).rename("water_balance")
        return wb

    reference_wb = ee.ImageCollection(years.map(get_yearly_water_balance))
    long_term_mean = reference_wb.mean()
    long_term_stddev = reference_wb.reduce(ee.Reducer.stdDev())

    # SPEI = (current_wb - long_term_mean) / long_term_stddev
    spei_image = (
        current_water_balance.subtract(long_term_mean)
        .divide(long_term_stddev.select([0]))
        .rename("spei")
    )

    # Reduce SPEI to regions
    spei_per_region = spei_image.reduceRegions(
        collection=roi, reducer=ee.Reducer.mean(), scale=5566
    )

    # Classify severity using GEE server-side
    def classify_feature(feature):
        spei_val = ee.Number(feature.get("mean"))
        severity = ee.Algorithms.If(
            spei_val.lte(-2.5),
            "D4",
            ee.Algorithms.If(
                spei_val.lte(-2.0),
                "D3",
                ee.Algorithms.If(
                    spei_val.lte(-1.5),
                    "D2",
                    ee.Algorithms.If(
                        spei_val.lte(-1.0),
                        "D1",
                        ee.Algorithms.If(spei_val.lte(-0.5), "D0", "NONE"),
                    ),
                ),
            ),
        )
        area = feature.geometry().area().divide(1e6)  # sq km
        return feature.set(
            {
                "spei_value": spei_val,
                "severity": severity,
                "area_sq_km": area,
                "alert_date": target_date.isoformat(),
            }
        )

    classified = spei_per_region.map(classify_feature)

    # Filter out non-drought features (NONE severity)
    drought_features = classified.filter(ee.Filter.neq("severity", "NONE"))

    # Get results
    results = drought_features.getInfo()

    alerts = []
    if results and "features" in results:
        for feature in results["features"]:
            props = feature.get("properties", {})
            geom = feature.get("geometry")
            alerts.append(
                {
                    "spei_value": props.get("spei_value"),
                    "severity": props.get("severity"),
                    "area_sq_km": props.get("area_sq_km"),
                    "geometry": geom,
                    "alert_date": target_date,
                    "uid": props.get("uid", props.get("id", "")),
                }
            )

    return alerts


def compute_spei_raster_for_aoi(
    roi_path,
    asset_suffix,
    asset_folder_list,
    target_date=None,
    accumulation_months=1,
    app_type="MWS",
    gee_account_id=None,
):
    """
    Compute SPEI as a raster, vectorize drought-affected areas,
    and export as a GEE asset.

    Returns the GEE asset ID of the exported drought alert vectors.
    """
    ee_initialize(gee_account_id)

    if target_date is None:
        target_date = date.today()

    roi = ee.FeatureCollection(roi_path)

    # Data sources
    chirps = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY").select("precipitation")
    modis_et = ee.ImageCollection("MODIS/061/MOD16A2GF").select(["ET", "PET"])

    target_year = target_date.year
    target_month = target_date.month

    period_end = ee.Date.fromYMD(target_year, target_month, 1)
    period_start = period_end.advance(-accumulation_months, "month")

    # Current water balance
    current_precip = chirps.filterDate(period_start, period_end).sum()
    current_pet = (
        modis_et.filterDate(period_start, period_end).select("PET").sum().multiply(0.1)
    )
    current_wb = current_precip.subtract(current_pet)

    # Reference statistics
    reference_start_year = 2001
    reference_end_year = target_year - 1
    years = ee.List.sequence(reference_start_year, reference_end_year)

    def get_yearly_water_balance(year):
        y = ee.Number(year).int()
        start = ee.Date.fromYMD(y, target_month, 1).advance(
            -accumulation_months, "month"
        )
        end = ee.Date.fromYMD(y, target_month, 1)
        p = chirps.filterDate(start, end).sum()
        pet = modis_et.filterDate(start, end).select("PET").sum().multiply(0.1)
        return p.subtract(pet).rename("water_balance")

    reference_wb = ee.ImageCollection(years.map(get_yearly_water_balance))
    long_term_mean = reference_wb.mean()
    long_term_stddev = reference_wb.reduce(ee.Reducer.stdDev())

    spei_image = (
        current_wb.subtract(long_term_mean)
        .divide(long_term_stddev.select([0]))
        .rename("spei")
        .clip(roi.geometry())
    )

    # Classify into severity bands
    severity_image = (
        ee.Image(0)
        .where(spei_image.lte(-0.5), 1)  # D0
        .where(spei_image.lte(-1.0), 2)  # D1
        .where(spei_image.lte(-1.5), 3)  # D2
        .where(spei_image.lte(-2.0), 4)  # D3
        .where(spei_image.lte(-2.5), 5)  # D4
        .rename("severity")
        .clip(roi.geometry())
    )

    # Vectorize drought areas (severity >= 1 means drought)
    drought_mask = severity_image.gte(1)
    drought_vectors = severity_image.updateMask(drought_mask).reduceToVectors(
        reducer=ee.Reducer.mode(),
        geometry=roi.geometry(),
        scale=5566,
        maxPixels=1e13,
        geometryType="polygon",
        eightConnected=True,
        labelProperty="severity",
    )

    # Add attributes to polygons
    severity_labels = ee.Dictionary(
        {"1": "D0", "2": "D1", "3": "D2", "4": "D3", "5": "D4"}
    )

    def add_attributes(feature):
        sev_code = ee.Number(feature.get("severity")).format()
        area = feature.geometry().area().divide(1e6)
        return feature.set(
            {
                "severity_class": severity_labels.get(sev_code, "UNKNOWN"),
                "area_sq_km": area,
                "alert_date": target_date.isoformat(),
                "source": "SPEI",
                "accumulation_months": accumulation_months,
            }
        )

    drought_vectors = drought_vectors.map(add_attributes)

    # Export to GEE
    date_str = target_date.strftime("%Y_%m")
    dst_filename = f"drought_alert_spei_{asset_suffix}_{date_str}"
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + dst_filename
    )

    create_gee_dir(asset_folder_list, GEE_PATHS[app_type]["GEE_ASSET_PATH"])

    if not is_gee_asset_exists(asset_id):
        task_id = export_vector_asset_to_gee(drought_vectors, dst_filename, asset_id)
        check_task_status([task_id])
        make_asset_public(asset_id)

    return asset_id, drought_vectors
