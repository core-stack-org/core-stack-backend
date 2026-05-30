import re

import ee

from computing.misc.hls_interpolated_ndvi import get_padded_ndvi_ts_image
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    check_task_status,
    create_gee_dir,
    export_vector_asset_to_gee,
    get_gee_dir_path,
    is_gee_asset_exists,
    make_asset_public,
    valid_gee_text,
)


def _safe_suffix(text):
    text = valid_gee_text(str(text))
    return re.sub(r"_+", "_", text).strip("_")


def _annotate_for_spatial_chunking(feature, grid_size_deg):
    feature = ee.Feature(feature)
    centroid = feature.geometry().centroid(ee.ErrorMargin(1))
    coords = centroid.coordinates()
    lon = ee.Number(coords.get(0))
    lat = ee.Number(coords.get(1))
    grid_x = lon.divide(grid_size_deg).floor()
    grid_y = lat.divide(grid_size_deg).floor()
    grid_id = grid_x.format().cat("_").cat(grid_y.format())

    area_ha = ee.Number(
        ee.Algorithms.If(
            feature.propertyNames().contains("area_in_ha"),
            feature.get("area_in_ha"),
            feature.geometry().area(ee.ErrorMargin(1)).divide(10000),
        )
    )

    return feature.set(
        {
            "_grid_id": grid_id,
            "_area_ha": area_ha,
            "_centroid_lon": lon,
            "_centroid_lat": lat,
        }
    )


def create_spatial_area_chunks(
    aoi,
    description,
    max_features=50,
    max_area_ha=30000,
    grid_size_deg=0.10,
):
    """
    Create chunks that are spatially compact and bounded by total MWS area.

    This keeps exact MWS geometries. It only changes which MWS features are grouped
    together in a GEE export task.
    """
    annotated = aoi.map(
        lambda feature: _annotate_for_spatial_chunking(feature, grid_size_deg)
    )

    meta = ee.Dictionary(
        {
            "uid": annotated.aggregate_array("uid"),
            "area": annotated.aggregate_array("_area_ha"),
            "grid": annotated.aggregate_array("_grid_id"),
            "lon": annotated.aggregate_array("_centroid_lon"),
            "lat": annotated.aggregate_array("_centroid_lat"),
        }
    ).getInfo()

    rows = []
    for uid, area, grid, lon, lat in zip(
        meta["uid"], meta["area"], meta["grid"], meta["lon"], meta["lat"]
    ):
        rows.append(
            {
                "uid": uid,
                "area": float(area or 0),
                "grid": grid,
                "lon": float(lon or 0),
                "lat": float(lat or 0),
            }
        )

    rows.sort(key=lambda row: (row["grid"], row["lon"], row["lat"]))

    chunks = []
    current = []
    current_area = 0

    def flush():
        nonlocal current, current_area
        if current:
            chunks.append(current)
        current = []
        current_area = 0

    for row in rows:
        exceeds_count = len(current) >= max_features
        exceeds_area = current_area + row["area"] > max_area_ha

        if current and (exceeds_count or exceeds_area):
            flush()

        current.append(row)
        current_area += row["area"]

        if row["area"] >= max_area_ha:
            flush()

    flush()

    rois = []
    descs = []
    for index, chunk in enumerate(chunks):
        uid_values = [row["uid"] for row in chunk]
        total_area = round(sum(row["area"] for row in chunk))
        grid = _safe_suffix(chunk[0]["grid"])
        desc = _safe_suffix(
            f"{description}_g{grid}_c{index}_{len(chunk)}mws_{total_area}ha"
        )

        rois.append(aoi.filter(ee.Filter.inList("uid", uid_values)).select(["uid"]))
        descs.append(desc)

    return rois, descs


def export_ndvi_vector_chunk(
    roi,
    ndvi_asset_id,
    ndvi_description,
    asset_folder_list,
    app_type,
    asset_suffix,
    f_start_date,
    f_start_date_str,
    f_end_date_str,
):
    """
    Export one already-spatially-compact MWS chunk as a vector asset.
    """
    if is_gee_asset_exists(ndvi_asset_id):
        return None

    lulc = ee.Image(
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + asset_suffix
        + "_"
        + str(f_start_date.year)
        + "-07-01_"
        + str(f_start_date.year + 1)
        + "-06-30_LULCmap_10m"
    )

    crop_mask = lulc.remap([8, 9, 10, 11], [1, 1, 1, 1], 0)
    tree_mask = lulc.eq(6)
    shrub_mask = lulc.eq(12)

    ndvi = get_padded_ndvi_ts_image(f_start_date_str, f_end_date_str, roi, 14)

    def add_masked_bands(img):
        nd = img.select("gapfilled_NDVI_lsc")
        date = img.date().format("YYYY-MM-dd")

        return ee.Image.cat(
            [
                nd.updateMask(crop_mask).rename(ee.String("crop_").cat(date)),
                nd.updateMask(tree_mask).rename(ee.String("tree_").cat(date)),
                nd.updateMask(shrub_mask).rename(ee.String("shrub_").cat(date)),
            ]
        )

    ndvi_band_stack = (
        ndvi.map(add_masked_bands).toBands().regexpRename("^[0-9]+_", "item_")
    )

    reduced = ndvi_band_stack.reduceRegions(
        collection=roi.select(["uid"]),
        reducer=ee.Reducer.mean(),
        scale=30,
        tileScale=4,
    )

    def filter_props(f):
        props = f.toDictionary()
        keys = props.keys().filter(
            ee.Filter.Or(
                ee.Filter.stringContains("item", "_crop_"),
                ee.Filter.stringContains("item", "_tree_"),
                ee.Filter.stringContains("item", "_shrub_"),
            )
        )

        def build_dict(k, acc):
            k = ee.String(k)
            new_key = k.split("_").slice(1).join("_")
            return ee.Dictionary(acc).set(new_key, props.get(k))

        new_props = ee.Dictionary(keys.iterate(build_dict, ee.Dictionary({})))
        return ee.Feature(f.geometry(), new_props.set("uid", f.get("uid")))

    fc = reduced.map(filter_props)
    return export_vector_asset_to_gee(fc, ndvi_description, ndvi_asset_id)


def export_ndvi_year_spatial_chunks(
    app_type,
    asset_folder_list,
    final_asset_id,
    asset_suffix,
    description,
    f_start_date,
    f_start_date_str,
    f_end_date_str,
    roi,
    helper_asset_path,
    max_features=50,
    max_area_ha=30000,
    grid_size_deg=0.50,
):
    """
    Suggested replacement flow for problematic tehsils:
    1. group MWS by centroid grid, total area, and feature count;
    2. export each spatially compact chunk;
    3. merge chunk vector assets into the yearly vector asset.
    """
    create_gee_dir(asset_folder_list, helper_asset_path)

    rois, descs = create_spatial_area_chunks(
        roi,
        description,
        max_features=max_features,
        max_area_ha=max_area_ha,
        grid_size_deg=grid_size_deg,
    )

    chunk_asset_ids = []
    for chunk_roi, chunk_desc in zip(rois, descs):
        chunk_asset_id = (
            get_gee_dir_path(asset_folder_list, asset_path=helper_asset_path)
            + chunk_desc
        )
        chunk_asset_ids.append(chunk_asset_id)

    existing_chunks = export_ndvi_asset(
        app_type,
        asset_folder_list,
        asset_suffix,
        chunk_asset_ids,
        descs,
        f_end_date_str,
        f_start_date,
        f_start_date_str,
        rois,
        retries=3,
    )

    #     if not is_gee_asset_exists(chunk_asset_id):
    #         task_id = export_ndvi_vector_chunk(
    #             chunk_asset_id,
    #             asset_folder_list,
    #             app_type,
    #             asset_suffix,
    #             f_start_date,
    #             f_start_date_str,
    #             f_end_date_str,
    #             chunk_roi,
    #             chunk_desc,
    #             tile_scale=tile_scale,
    #         )
    #         if task_id:
    #             task_ids.append(task_id)
    #
    # check_task_status(task_ids)

    # existing_chunks = []
    # new_asset_ids = []
    # for chunk_asset_id in chunk_asset_ids:
    #     if is_gee_asset_exists(chunk_asset_id):
    #         make_asset_public(chunk_asset_id)
    #         existing_chunks.append(ee.FeatureCollection(chunk_asset_id))
    #     else:
    #         chunk_ind = chunk_asset_ids.index(chunk_asset_id)
    #         new_asset_ids.append(chunk_asset_id)
    #         task_id = export_ndvi_vector_chunk(
    #             chunk_asset_id,
    #             asset_folder_list,
    #             app_type,
    #             asset_suffix,
    #             f_start_date,
    #             f_start_date_str,
    #             f_end_date_str,
    #             rois[chunk_ind],
    #             descs[chunk_ind],
    #             tile_scale=tile_scale,
    #         )
    #         if task_id:
    #             task_ids.append(task_id)
    #
    # if len(task_ids) > 0:
    #     check_task_status(task_ids)
    #
    # if len(new_asset_ids) > 0:
    #     for new_asset_id in new_asset_ids:
    #         if is_gee_asset_exists(new_asset_id):
    #             make_asset_public(new_asset_id)
    #             existing_chunks.append(ee.FeatureCollection(new_asset_id))
    #         else:
    #             chunk_ind = chunk_asset_ids.index(new_asset_id)
    #             task_id = export_ndvi_vector_chunk(
    #                 new_asset_id,
    #                 asset_folder_list,
    #                 app_type,
    #                 asset_suffix,
    #                 f_start_date,
    #                 f_start_date_str,
    #                 f_end_date_str,
    #                 rois[chunk_ind],
    #                 descs[chunk_ind],
    #                 tile_scale=tile_scale,
    #             )
    #             if task_id:
    #                 task_ids.append(task_id)
    #
    #     if len(task_ids) > 0:
    #         check_task_status(task_ids)

    merged = ee.FeatureCollection(existing_chunks).flatten()
    if merged.size().eq(roi.size()):
        merge_task_id = export_vector_asset_to_gee(merged, description, final_asset_id)
        check_task_status([merge_task_id])

    return final_asset_id


def export_ndvi_asset(
    app_type,
    asset_folder_list,
    asset_suffix,
    chunk_asset_ids,
    descs,
    f_end_date_str,
    f_start_date,
    f_start_date_str,
    rois,
    retries=3,
):
    existing_chunks = []
    task_ids = []
    retry_count = retries

    while retry_count > 0:
        if retry_count < retries:
            for chunk_asset_id in chunk_asset_ids:
                if is_gee_asset_exists(chunk_asset_id):
                    make_asset_public(chunk_asset_id)
                    chunk_ind = chunk_asset_ids.index(chunk_asset_id)
                    existing_chunks.append(ee.FeatureCollection(chunk_asset_id))
                    del chunk_asset_ids[chunk_ind]
                    del rois[chunk_ind]
                    del descs[chunk_ind]
                # else:
                #     missing_chunks.append(chunk_asset_id)

        if len(chunk_asset_ids) > 0:
            for chunk_asset_id in chunk_asset_ids:
                chunk_ind = chunk_asset_ids.index(chunk_asset_id)
                task_id = export_ndvi_vector_chunk(
                    rois[chunk_ind],
                    chunk_asset_id,
                    descs[chunk_ind],
                    asset_folder_list,
                    app_type,
                    asset_suffix,
                    f_start_date,
                    f_start_date_str,
                    f_end_date_str,
                )
                if task_id:
                    task_ids.append(task_id)
            check_task_status(task_ids)
            retry_count = retry_count - 1
        else:
            break

    return existing_chunks
