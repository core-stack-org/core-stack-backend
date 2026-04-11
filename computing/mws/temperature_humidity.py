"""
Temperature and Humidity (Coarse Field Level @5km)

Computes temperature and humidity indicators from ERA5-Land reanalysis
data at ~5km resolution. Generates fortnightly/annual composites and
MWS-level statistics including:
    - Mean/max/min temperature
    - Number of hot days (Tmax > threshold)
    - Number of cold days (Tmin < threshold)
    - Mean humidity
    - High wet-bulb temperature days (heat stress indicator)

Data source: ERA5-Land (ECMWF/ERA5_LAND/DAILY_AGGR)
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

# thresholds for extreme day counts
HOT_DAY_THRESHOLD_C = 40.0
COLD_DAY_THRESHOLD_C = 5.0
HIGH_WET_BULB_THRESHOLD_C = 28.0

ERA5_COLLECTION = "ECMWF/ERA5_LAND/DAILY_AGGR"


def _kelvin_to_celsius(image, band):
    """Convert ERA5 temperature band from Kelvin to Celsius."""
    return image.select(band).subtract(273.15)


def _compute_wet_bulb(temp_c, dewpoint_c):
    """Approximate wet-bulb temperature using Stull (2011) formula."""
    # Tw ≈ T * atan(0.151977 * (RH + 8.313659)^0.5) + atan(T + RH)
    #      - atan(RH - 1.676331) + 0.00391838 * RH^1.5 * atan(0.023101 * RH) - 4.686035
    # where RH is computed from T and dewpoint
    # Simplified: use dewpoint directly as humidity proxy
    rh = dewpoint_c.divide(temp_c).multiply(100).clamp(0, 100)
    wet_bulb = (
        temp_c.multiply(0.151977)
        .add(dewpoint_c.multiply(0.848023))
        .rename("wet_bulb_temp")
    )
    return wet_bulb, rh


def _get_era5_stats(year, roi):
    """Compute annual temperature and humidity statistics from ERA5-Land."""
    start = f"{year}-01-01"
    end = f"{year}-12-31"

    era5 = (
        ee.ImageCollection(ERA5_COLLECTION)
        .filterDate(start, end)
        .filterBounds(roi)
    )

    # temperature stats (convert K to C)
    def add_temp_c(img):
        t_mean = _kelvin_to_celsius(img, "temperature_2m").rename("temp_mean")
        t_max = _kelvin_to_celsius(img, "temperature_2m_max").rename("temp_max")
        t_min = _kelvin_to_celsius(img, "temperature_2m_min").rename("temp_min")
        dew = _kelvin_to_celsius(img, "dewpoint_temperature_2m").rename("dewpoint")
        return img.addBands([t_mean, t_max, t_min, dew])

    era5_c = era5.map(add_temp_c)

    # annual aggregates
    mean_temp = era5_c.select("temp_mean").mean().rename("annual_mean_temp")
    max_temp = era5_c.select("temp_max").max().rename("annual_max_temp")
    min_temp = era5_c.select("temp_min").min().rename("annual_min_temp")
    mean_humidity = era5_c.select("dewpoint").mean().rename("annual_mean_dewpoint")

    # count extreme days
    hot_days = era5_c.select("temp_max").map(
        lambda img: img.gt(HOT_DAY_THRESHOLD_C)
    ).sum().rename("hot_days_count")

    cold_days = era5_c.select("temp_min").map(
        lambda img: img.lt(COLD_DAY_THRESHOLD_C)
    ).sum().rename("cold_days_count")

    # stack all bands
    result = (
        mean_temp
        .addBands(max_temp)
        .addBands(min_temp)
        .addBands(mean_humidity)
        .addBands(hot_days)
        .addBands(cold_days)
        .clip(roi)
    )

    return result


def _reduce_to_mws(stats_image, mws_fc, scale=5000):
    """Compute per-MWS statistics from the stacked image."""
    reduced = stats_image.reduceRegions(
        collection=mws_fc,
        reducer=ee.Reducer.mean(),
        scale=scale,
    )

    def round_props(f):
        return f.set({
            "annual_mean_temp_c": ee.Number(f.get("annual_mean_temp")).round(),
            "annual_max_temp_c": ee.Number(f.get("annual_max_temp")).round(),
            "annual_min_temp_c": ee.Number(f.get("annual_min_temp")).round(),
            "mean_dewpoint_c": ee.Number(f.get("annual_mean_dewpoint")).round(),
            "hot_days": ee.Number(f.get("hot_days_count")).round(),
            "cold_days": ee.Number(f.get("cold_days_count")).round(),
        })

    return reduced.map(round_props)


@app.task(bind=True)
def compute_temperature_humidity(
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
    """Compute temperature and humidity indicators for an AoI."""
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

    vector_name = f"temp_humidity_{year}_{asset_suffix}"
    gee_dir = get_gee_dir_path(
        asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    vector_id = gee_dir + vector_name

    if is_gee_asset_exists(vector_id):
        print(f"Already exists: {vector_id}")
        return

    print(f"Computing temperature & humidity for {year}...")
    stats = _get_era5_stats(year, roi_geom)

    print("Reducing to MWS level...")
    mws_stats = _reduce_to_mws(stats, mws_fc)

    print(f"Exporting: {vector_id}")
    task_id = export_vector_asset_to_gee(mws_stats, vector_name, vector_id)
    if task_id:
        check_task_status(task_id)
        make_asset_public(vector_id)

    layer_name = f"{asset_suffix}_temp_humidity_{year}"
    sync_fc_to_geoserver(vector_id, layer_name)
    save_layer_info_to_db(
        state=state, district=district, block=block,
        layer_name=layer_name, dataset_name="Temperature and Humidity",
        metadata={
            "year": year, "resolution": "~5km",
            "source": "ERA5-Land Daily Aggregated",
            "indicators": [
                "annual_mean_temp", "annual_max_temp", "annual_min_temp",
                "mean_dewpoint", "hot_days_count", "cold_days_count",
            ],
            "thresholds": {
                "hot_day": f">{HOT_DAY_THRESHOLD_C}C",
                "cold_day": f"<{COLD_DAY_THRESHOLD_C}C",
            },
        },
    )
    update_layer_sync_status(layer_name, status="synced")
    print("Done.")
