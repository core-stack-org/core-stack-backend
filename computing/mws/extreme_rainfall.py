"""
Extreme Rainfall Events (Coarse Field Level @5km)

Identifies extreme rainfall events using CHIRPS precipitation data.
Computes annual/seasonal metrics:
    - Annual maximum daily rainfall
    - Frequency of extreme events (>95th percentile)
    - Total extreme rainfall
    - Seasonal distribution

Vectorizes at MWS level for integration with downstream apps.

Data source: CHIRPS Daily (UCSB-CHG/CHIRPS/DAILY)
Reference: https://www.sciencedirect.com/science/article/pii/S2214581825004963
"""

import ee
from nrm_app.celery import app
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    is_gee_asset_exists,
    check_task_status,
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

CHIRPS_COLLECTION = "UCSB-CHG/CHIRPS/DAILY"
# percentile threshold for "extreme" rainfall
EXTREME_PERCENTILE = 95


def _compute_extreme_rainfall_stats(year, roi, baseline_start=2000, baseline_end=2020):
    """Compute extreme rainfall metrics for a given year.

    Uses a baseline period to establish the 95th percentile threshold,
    then counts exceedances in the target year.
    """
    start = f"{year}-01-01"
    end = f"{year}-12-31"

    # target year daily rainfall
    chirps_year = (
        ee.ImageCollection(CHIRPS_COLLECTION)
        .filterDate(start, end)
        .filterBounds(roi)
    )

    # baseline period for percentile computation
    chirps_baseline = (
        ee.ImageCollection(CHIRPS_COLLECTION)
        .filterDate(f"{baseline_start}-01-01", f"{baseline_end}-12-31")
        .filterBounds(roi)
    )

    # compute 95th percentile from baseline
    p95 = chirps_baseline.select("precipitation").reduce(
        ee.Reducer.percentile([EXTREME_PERCENTILE])
    ).rename("p95_threshold")

    # annual max daily rainfall
    annual_max = chirps_year.select("precipitation").max().rename("annual_max_rainfall_mm")

    # total annual rainfall
    annual_total = chirps_year.select("precipitation").sum().rename("annual_total_rainfall_mm")

    # count extreme days (> 95th percentile)
    extreme_days = chirps_year.select("precipitation").map(
        lambda img: img.gt(p95).rename("extreme")
    ).sum().rename("extreme_days_count")

    # total extreme rainfall (sum of rainfall on extreme days only)
    extreme_total = chirps_year.select("precipitation").map(
        lambda img: img.updateMask(img.gt(p95))
    ).sum().rename("extreme_rainfall_total_mm")

    result = (
        annual_max
        .addBands(annual_total)
        .addBands(extreme_days)
        .addBands(extreme_total)
        .addBands(p95)
        .clip(roi)
    )

    return result


def _reduce_to_mws(stats_image, mws_fc, scale=5000):
    """Compute per-MWS extreme rainfall statistics."""
    reduced = stats_image.reduceRegions(
        collection=mws_fc,
        reducer=ee.Reducer.mean(),
        scale=scale,
    )

    def format_props(f):
        return f.set({
            "max_daily_rainfall_mm": ee.Number(f.get("annual_max_rainfall_mm")).round(),
            "total_rainfall_mm": ee.Number(f.get("annual_total_rainfall_mm")).round(),
            "extreme_days": ee.Number(f.get("extreme_days_count")).round(),
            "extreme_rainfall_mm": ee.Number(f.get("extreme_rainfall_total_mm")).round(),
            "p95_threshold_mm": ee.Number(f.get("p95_threshold")).round(),
            "area_km2": f.geometry().area().divide(1e6).round(),
        })

    return reduced.map(format_props)


@app.task(bind=True)
def compute_extreme_rainfall(
    self,
    state=None,
    district=None,
    block=None,
    year=2024,
    gee_account_id=None,
    roi_path=None,
    asset_folder_list=None,
    asset_suffix=None,
    app_type="MWS",
):
    """Compute extreme rainfall events for an AoI."""
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

    vector_name = f"extreme_rainfall_{year}_{asset_suffix}"
    gee_dir = get_gee_dir_path(
        asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    vector_id = gee_dir + vector_name

    if is_gee_asset_exists(vector_id):
        print(f"Already exists: {vector_id}")
        return

    print(f"Computing extreme rainfall for {year}...")
    stats = _compute_extreme_rainfall_stats(year, roi_geom)

    print("Reducing to MWS level...")
    mws_stats = _reduce_to_mws(stats, mws_fc)

    print(f"Exporting: {vector_id}")
    task_id = export_vector_asset_to_gee(mws_stats, vector_name, vector_id)
    if task_id:
        check_task_status(task_id)
        make_asset_public(vector_id)

    layer_name = f"{asset_suffix}_extreme_rainfall_{year}"
    sync_fc_to_geoserver(vector_id, layer_name)
    save_layer_info_to_db(
        state=state, district=district, block=block,
        layer_name=layer_name, dataset_name="Extreme Rainfall",
        metadata={
            "year": year, "resolution": "~5km",
            "source": "CHIRPS Daily",
            "percentile_threshold": EXTREME_PERCENTILE,
            "baseline_period": "2000-2020",
            "indicators": [
                "max_daily_rainfall", "total_rainfall",
                "extreme_days_count", "extreme_rainfall_total",
            ],
        },
    )
    update_layer_sync_status(layer_name, status="synced")
    print("Done.")
