"""
ZOI NDVI using the ndvi_timeseries compute (HLS-interpolated NDVI).

Uses the fast band-stack + single reduceRegions path (same idea as ndvi_time_series._generate_ndvi),
while keeping the original NDVI_<year> JSON property output shape.
"""

import ee

from computing.misc.hls_interpolated_ndvi import get_padded_ndvi_ts_image
from computing.surface_water_bodies.swb import sync_asset_to_db_and_geoserver
from computing.utils import sync_project_fc_to_geoserver
from projects.models import Project
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    get_gee_dir_path,
    check_task_status,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
)
from waterrejuvenation.utils import wait_for_task_completion


def _merge_ndvi_year_assets(chunk_assets):
    """Same merge logic as waterrejuvenation.utils.merge_assets_chunked_on_year."""

    def merge_features(feature):
        uid = feature.get("UID")
        matched_features = []
        for i in range(1, len(chunk_assets)):
            matched_feature = ee.Feature(
                ee.FeatureCollection(chunk_assets[i])
                .filter(ee.Filter.eq("UID", uid))
                .first()
            )
            matched_features.append(matched_feature)

        merged_properties = feature.toDictionary()
        for f in matched_features:
            merged_properties = merged_properties.combine(
                f.toDictionary(), overwrite=False
            )

        return ee.Feature(feature.geometry(), merged_properties)

    return ee.FeatureCollection(chunk_assets[0]).map(merge_features)


def _ndvi_bands_to_json_property(feature, year):
    """Convert toBands reduceRegions output into NDVI_<year> JSON string."""

    props = feature.toDictionary()
    keys = props.keys().filter(ee.Filter.stringContains("item", "20"))

    def build_dict(k, acc):
        k = ee.String(k)
        # toBands prefixes band names with "<index>_" — drop that index
        new_key = k.split("_").slice(1).join("_")
        return ee.Dictionary(acc).set(new_key, props.get(k))

    ndvi_dict = ee.Dictionary(keys.iterate(build_dict, ee.Dictionary({})))
    ndvi_json = ee.String.encodeJSON(ndvi_dict)
    return feature.set(f"NDVI_{year}", ndvi_json)


def build_ndvi_timeseries_from_timeseries_compute(
    suitability_vector,
    start_year,
    end_year,
    description,
    asset_id,
    ndvi_interval_days=16,
    reducer_scale=30,
    tile_scale=4,
):
    """
    Build NDVI time series for ZOI polygons (NDVI_<year> JSON per feature).

    Fast path: one reduceRegions per hydrological year (not per timestep).
    """
    task_ids = []
    asset_ids = []
    year = start_year

    while year <= end_year:
        start_date = f"{year}-07-01"
        end_date = f"{year + 1}-06-30"
        ndvi_description = f"ndvi_{year}_{description}"
        ndvi_asset_id = f"{asset_id}_ndvi_{year}"

        if is_gee_asset_exists(ndvi_asset_id):
            ee.data.deleteAsset(ndvi_asset_id)

        ndvi = get_padded_ndvi_ts_image(
            start_date, end_date, suitability_vector.bounds(), ndvi_interval_days
        )

        # Stack all dates into one image, then a single reduceRegions (fast)
        ndvi_stack = ndvi.map(
            lambda img: img.select("gapfilled_NDVI_lsc").rename(
                img.date().format("YYYY-MM-dd")
            )
        ).toBands()

        reduced = ndvi_stack.reduceRegions(
            collection=suitability_vector,
            reducer=ee.Reducer.mean(),
            scale=reducer_scale,
            tileScale=tile_scale,
        )

        merged_fc = reduced.map(lambda f: _ndvi_bands_to_json_property(f, year))

        try:
            task_id = export_vector_asset_to_gee(
                merged_fc, ndvi_description, ndvi_asset_id
            )
            if task_id:
                print(f"Started export for {year}")
                asset_ids.append(ndvi_asset_id)
                task_ids.append(task_id)
        except Exception as e:
            print("Export error:", e)

        year += 1

    check_task_status(task_ids)
    return _merge_ndvi_year_assets(asset_ids)


def get_ndvi_for_zoi_from_timeseries_compute(
    state=None,
    district=None,
    block=None,
    zoi_roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    start_date="2017-07-01",
    end_date="2025-06-30",
    start_year=2017,
    end_year=2024,
    app_type="MWS",
    gee_account_id=None,
    proj_id=None,
):
    """
    Same orchestration and result as get_ndvi_for_zoi (zoi3), but NDVI values come from
    the ndvi_timeseries compute (get_padded_ndvi_ts_image) instead of get_ndvi_data.
    """
    print("started generating ndvi for zoi (timeseries compute, fast reduce)")
    ee_initialize(gee_account_id)

    description_zoi = "cropping_intensity_zoi_" + asset_suffix
    asset_id_zoi = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description_zoi
    )

    if zoi_roi:
        zoi_collections = ee.FeatureCollection(zoi_roi)
    else:
        zoi_collections = ee.FeatureCollection(asset_id_zoi)

    description_ndvi = asset_suffix
    ndvi_asset_path = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description_ndvi
    )

    fc = build_ndvi_timeseries_from_timeseries_compute(
        zoi_collections,
        start_year,
        end_year,
        description_ndvi,
        ndvi_asset_path,
    )

    task = ee.batch.Export.table.toAsset(
        collection=fc, description=description_ndvi, assetId=ndvi_asset_path
    )
    task.start()
    wait_for_task_completion(task)

    if state and district and block:
        layer_name = f"waterbodies_zoi_{asset_suffix}"
        sync_asset_to_db_and_geoserver(
            ndvi_asset_path,
            layer_name,
            asset_suffix,
            start_date,
            end_date,
            state,
            district,
            block,
        )
    elif proj_id:
        proj_obj = Project.objects.get(pk=proj_id)
        layer_name = f"waterbodies_zoi_{asset_suffix}"
        sync_project_fc_to_geoserver(
            fc, proj_obj.name, layer_name, "zoi_layers"
        )

    return fc
