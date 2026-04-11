"""
Drought Live Alerts (SPEI-based)

Near real-time drought monitoring using SPEI (Standardized Precipitation-
Evapotranspiration Index) computed from CHIRPS precipitation and
ERA5-Land temperature/PET data.

Alerts are generated at the AoI level with severity categories:
    - Mild drought:     -1.0 < SPEI <= -0.5
    - Moderate drought: -1.5 < SPEI <= -1.0
    - Severe drought:   -2.0 < SPEI <= -1.5
    - Extreme drought:  SPEI <= -2.0

The pipeline can be triggered on a schedule (weekly/monthly) to
produce updated drought alert maps.

Data sources:
    - CHIRPS (precipitation)
    - ERA5-Land (temperature, for PET estimation)
    - India Drought Monitor (validation)
"""

import ee
from nrm_app.celery import app
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    is_gee_asset_exists,
    check_task_status,
    export_raster_asset_to_gee,
    export_vector_asset_to_gee,
    make_asset_public,
    get_gee_dir_path,
)
from utilities.constants import GEE_PATHS
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)

CHIRPS = "UCSB-CHG/CHIRPS/DAILY"
ERA5 = "ECMWF/ERA5_LAND/DAILY_AGGR"

# SPEI severity thresholds
SEVERITY = {
    "extreme": -2.0,
    "severe": -1.5,
    "moderate": -1.0,
    "mild": -0.5,
}

CLASS_EXTREME = 4
CLASS_SEVERE = 3
CLASS_MODERATE = 2
CLASS_MILD = 1
CLASS_NORMAL = 0


def _compute_monthly_precip(year, month, roi):
    """Compute total monthly precipitation from CHIRPS."""
    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")
    monthly = (
        ee.ImageCollection(CHIRPS)
        .filterDate(start, end)
        .filterBounds(roi)
        .sum()
        .rename("precip_mm")
        .clip(roi)
    )
    return monthly


def _compute_monthly_pet(year, month, roi):
    """Estimate monthly PET from ERA5-Land temperature.

    Uses Thornthwaite method (simplified):
        PET_monthly ≈ 16 * (10 * T_mean / I)^a  (mm/month)
    where I is annual heat index and a is a cubic function of I.

    For simplicity, we use ERA5 total_evaporation directly if available.
    """
    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")

    era5 = (
        ee.ImageCollection(ERA5)
        .filterDate(start, end)
        .filterBounds(roi)
    )

    # ERA5 has potential_evaporation band (negative values, in meters)
    # convert to positive mm
    pet = (
        era5.select("potential_evaporation")
        .sum()
        .multiply(-1000)
        .rename("pet_mm")
        .clip(roi)
    )

    return pet


def _compute_spei(precip, pet, precip_mean, pet_mean, precip_std):
    """Compute SPEI as standardized anomaly of (P - PET).

    SPEI = (D - D_mean) / D_std
    where D = P - PET (climatic water balance)
    """
    d = precip.subtract(pet)
    d_mean = precip_mean.subtract(pet_mean)
    d_std = precip_std  # approximate, using precip std

    # avoid division by zero
    d_std_safe = d_std.where(d_std.lt(1), 1)
    spei = d.subtract(d_mean).divide(d_std_safe).rename("spei")
    return spei


def _classify_drought(spei_image):
    """Classify SPEI into drought severity categories."""
    extreme = spei_image.lte(SEVERITY["extreme"]).multiply(CLASS_EXTREME)
    severe = (
        spei_image.gt(SEVERITY["extreme"])
        .And(spei_image.lte(SEVERITY["severe"]))
        .multiply(CLASS_SEVERE)
    )
    moderate = (
        spei_image.gt(SEVERITY["severe"])
        .And(spei_image.lte(SEVERITY["moderate"]))
        .multiply(CLASS_MODERATE)
    )
    mild = (
        spei_image.gt(SEVERITY["moderate"])
        .And(spei_image.lte(SEVERITY["mild"]))
        .multiply(CLASS_MILD)
    )

    classified = extreme.add(severe).add(moderate).add(mild)
    classified = classified.rename("drought_severity").toUint8()
    return classified


def _compute_baseline_stats(roi, baseline_start=2000, baseline_end=2020):
    """Compute long-term monthly mean and std of precipitation from baseline."""
    baseline = (
        ee.ImageCollection(CHIRPS)
        .filterDate(f"{baseline_start}-01-01", f"{baseline_end}-12-31")
        .filterBounds(roi)
    )

    # monthly sums across all baseline years, then mean and stddev
    monthly_totals = ee.List.sequence(1, 12).map(
        lambda m: baseline.filter(ee.Filter.calendarRange(m, m, "month")).sum()
    )

    # overall mean and std across all months
    precip_mean = ee.ImageCollection(monthly_totals).mean().rename("precip_mean")
    precip_std = ee.ImageCollection(monthly_totals).reduce(
        ee.Reducer.stdDev()
    ).rename("precip_std")

    # PET baseline mean (simplified)
    era5_baseline = (
        ee.ImageCollection(ERA5)
        .filterDate(f"{baseline_start}-01-01", f"{baseline_end}-12-31")
        .filterBounds(roi)
    )
    pet_mean = (
        era5_baseline.select("potential_evaporation")
        .mean()
        .multiply(-1000)
        .rename("pet_mean")
    )

    return precip_mean.clip(roi), pet_mean.clip(roi), precip_std.clip(roi)


def _vectorize_drought(classified, mws_fc, scale=5000):
    """Compute per-MWS drought alert summary."""
    pixel_area = ee.Image.pixelArea().divide(1e6)  # km2

    # area under each severity class
    def compute_class_area(cls_val, cls_name):
        area = pixel_area.updateMask(classified.eq(cls_val))
        return area.reduceRegions(
            collection=mws_fc, reducer=ee.Reducer.sum(), scale=scale
        ).map(lambda f: f.set(f"{cls_name}_area_km2", ee.Number(f.get("sum")).round()))

    # reduce SPEI mean per MWS
    # we need the raw SPEI for this, but we only have classified here
    # so just report the dominant severity class
    result = classified.reduceRegions(
        collection=mws_fc,
        reducer=ee.Reducer.mode(),
        scale=scale,
    )

    def add_label(f):
        mode = ee.Number(f.get("mode"))
        label = (
            ee.Algorithms.If(mode.eq(CLASS_EXTREME), "extreme",
            ee.Algorithms.If(mode.eq(CLASS_SEVERE), "severe",
            ee.Algorithms.If(mode.eq(CLASS_MODERATE), "moderate",
            ee.Algorithms.If(mode.eq(CLASS_MILD), "mild", "normal"))))
        )
        return f.set({"drought_alert": label, "severity_class": mode})

    return result.map(add_label)


@app.task(bind=True)
def generate_drought_alert(
    self,
    state=None,
    district=None,
    block=None,
    year=2024,
    month=6,
    gee_account_id=None,
    roi_path=None,
    asset_folder_list=None,
    asset_suffix=None,
    app_type="MWS",
):
    """Generate drought live alert for a given month and AoI."""
    ee_initialize(gee_account_id)

    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]
        roi_path = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + f"filtered_mws_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_uid"
        )

    mws_fc = ee.FeatureCollection(roi_path)
    roi_geom = mws_fc.geometry()

    month_str = str(month).zfill(2)
    raster_name = f"drought_alert_{year}_{month_str}_{asset_suffix}"
    vector_name = f"drought_alert_vec_{year}_{month_str}_{asset_suffix}"
    gee_dir = get_gee_dir_path(
        asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    raster_id = gee_dir + raster_name
    vector_id = gee_dir + vector_name

    if is_gee_asset_exists(vector_id):
        print(f"Alert already exists: {vector_id}")
        return

    # baseline stats
    print("Computing baseline climatology...")
    precip_mean, pet_mean, precip_std = _compute_baseline_stats(roi_geom)

    # current month data
    print(f"Computing SPEI for {year}-{month_str}...")
    precip = _compute_monthly_precip(year, month, roi_geom)
    pet = _compute_monthly_pet(year, month, roi_geom)

    # SPEI
    spei = _compute_spei(precip, pet, precip_mean, pet_mean, precip_std)
    classified = _classify_drought(spei)

    # export raster
    if not is_gee_asset_exists(raster_id):
        task_id = export_raster_asset_to_gee(
            classified, raster_name, raster_id, scale=5000, region=roi_geom
        )
        if task_id:
            check_task_status(task_id)
            make_asset_public(raster_id)

    # vectorize alerts
    print("Generating MWS-level alerts...")
    alerts = _vectorize_drought(classified, mws_fc)

    task_id = export_vector_asset_to_gee(alerts, vector_name, vector_id)
    if task_id:
        check_task_status(task_id)
        make_asset_public(vector_id)

    layer_name = f"{asset_suffix}_drought_alert_{year}_{month_str}"
    sync_fc_to_geoserver(vector_id, layer_name)
    save_layer_info_to_db(
        state=state, district=district, block=block,
        layer_name=layer_name, dataset_name="Drought Live Alert",
        metadata={
            "year": year, "month": month,
            "resolution": "~5km",
            "method": "SPEI (CHIRPS precip + ERA5 PET)",
            "severity_thresholds": SEVERITY,
            "baseline_period": "2000-2020",
            "source": "CHIRPS Daily, ERA5-Land",
        },
    )
    update_layer_sync_status(layer_name, status="synced")
    print(f"Drought alert generated for {year}-{month_str}.")
