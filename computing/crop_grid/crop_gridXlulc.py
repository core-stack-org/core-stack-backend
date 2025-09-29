import ee
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.gee_utils import (
    valid_gee_text,
    get_gee_asset_path,
    export_vector_asset_to_gee,
    is_gee_asset_exists,
    check_task_status,
    make_asset_public,
)


def crop_grids_lulc(state, district, block):
    lulc_image = ee.Image(
        get_gee_asset_path(state, district, block)
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_2023-07-01_2024-06-30_LULCmap_10m"
    )

    tiles_uid = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "crop_grid_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_with_uid_16ha"
    )

    # Generate crop tiles
    description = (
        "crop_gridXlulc_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_with_uid_16ha"
    )

    asset_id = get_gee_asset_path(state, district, block) + description
    crop_tiles = lulc_crop_tiles(tiles_uid, lulc_image)
    layer_name = (
        f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_grid"
    )
    if not is_gee_asset_exists(asset_id):
        task = export_vector_asset_to_gee(crop_tiles, description, asset_id)
        if task:
            task_id_list = check_task_status([task])
            print(f"crop gridXlulc task completed  - task_id_list: {task_id_list}")

    layer_at_geoserver = False
    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Crop GridXlulc",
        )

        res = sync_fc_to_geoserver(
            crop_tiles, state, layer_name, workspace="crop_grid_layers"
        )
        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag updated")
            layer_at_geoserver = True
        print("Successfully pushed to GeoServer!", res)
    return layer_at_geoserver


def lulc_crop_tiles(tiles_uid, lulc_image):
    # Apply is_valid function to all tiles
    check_tile = tiles_uid.map(lambda tile: is_valid(tile, lulc_image))

    # Filter tiles with fraction > 0.4
    crop_tiles = check_tile.filter(ee.Filter.gt("fraction", 0.4))

    return crop_tiles


def is_valid(poly, lulc_image):
    # col = ee.ImageCollection.fromImages([lulc_image]).filterBounds(poly.geometry())
    col = lulc_image.clip(poly.geometry())

    classification = col.select(["predicted_label"])
    dw_composite = classification.reduce(ee.Reducer.mode())

    single_kharif = cover(9.0, poly, dw_composite)  # single kharif
    single_non_kharif = cover(10.0, poly, dw_composite)  # single non-kharif
    double = cover(11.0, poly, dw_composite)  # double
    triple = cover(12.0, poly, dw_composite)  # triple

    # thresh = ee.Number(0.4)
    fraction = single_kharif.add(single_non_kharif.add(double.add(triple)))
    return poly.set("fraction", fraction)


def cover(cls, geo, dw_composite):
    relevant_area = dw_composite.eq(cls).rename(["relevant_area"])
    stats_total = relevant_area.reduceRegion(
        reducer=ee.Reducer.count(), geometry=geo.geometry(), scale=30, maxPixels=1e10
    )
    total_pixels = stats_total.get("relevant_area")

    relevant_area_masked = relevant_area.selfMask()
    stats_masked = relevant_area_masked.reduceRegion(
        reducer=ee.Reducer.count(), geometry=geo.geometry(), scale=30, maxPixels=1e10
    )
    relevant_area_pixels = stats_masked.get("relevant_area")
    fraction = ee.Number(relevant_area_pixels).divide(total_pixels)
    return fraction
