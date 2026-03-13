import ee
from nrm_app.celery import app
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    sync_raster_to_gcs,
    check_task_status,
    sync_raster_gcs_to_geoserver,
    export_raster_asset_to_gee,
    make_asset_public,
    get_gee_dir_path,
)
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from computing.STAC_specs import generate_STAC_layerwise
from constants.pan_india_urls import TREE_OVERALL_CHANGE


@app.task(bind=True)
def tree_health_overall_change_raster(
    self,
    state=None,
    district=None,
    block=None,
    start_year=None,
    end_year=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
):
    print("Inside process Tree health overall change raster")
    ee_initialize(gee_account_id)

    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

        roi = ee.FeatureCollection(
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + "filtered_mws_"
            + asset_suffix
            + "_uid"
        )

    description = f"overall_change_raster_{asset_suffix}"

    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )

    # Skip if asset already exists
    if not is_gee_asset_exists(asset_id):
        overall_change_clipped = (
            ee.ImageCollection(TREE_OVERALL_CHANGE)
            .filterBounds(roi.geometry())
            .mean()
            .clip(roi.geometry())
        )

        overall_change_clipped = mask_raster(
            app_type,
            asset_folder_list,
            asset_suffix,
            overall_change_clipped,
            start_year,
            end_year,
        )

        task_id = export_raster_asset_to_gee(
            image=overall_change_clipped,
            description=description,
            asset_id=asset_id,
            scale=25,
            region=roi.geometry(),
            crs="EPSG:4326",
        )

        task_id_list = check_task_status([task_id])
        print("Overall Change task_id_list", task_id_list)

    layer_at_geoserver = True
    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            description,
            asset_id,
            "Tree Overall Change Raster",
        )
        task_id = sync_raster_to_gcs(ee.Image(asset_id), 25, description)

        task_id_list = check_task_status([task_id])
        print("task_id_list sync to GCS", task_id_list)

        res = sync_raster_gcs_to_geoserver(
            "tree_overall_ch", description, description, "tree_overall_ch_style"
        )
        layer_at_geoserver = True

        if res and layer_id:
            layer_at_geoserver = True
            # layer_STAC_generated = False
            # layer_STAC_generated = generate_STAC_layerwise.generate_raster_stac(
            #     state=state,
            #     district=district,
            #     block=block,
            #     layer_name="tree_cover_change_raster",
            # )
            update_layer_sync_status(
                layer_id=layer_id,
                sync_to_geoserver=layer_at_geoserver,
            )
    return layer_at_geoserver


def mask_raster(
    app_type, asset_folder_list, asset_suffix, tree_change, start_year, end_year
):
    # STEP 1: Load IndiaSAT and Tree change layers and reproject to 25m
    deforestation = ee.Image(
        get_gee_dir_path(
            asset_folder_list,
            asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
        )
        + f"change_{asset_suffix}_Deforestation_{start_year}_{int(end_year)+1}"
    ).reproject(crs="EPSG:4326", scale=25)

    afforestation = ee.Image(
        get_gee_dir_path(
            asset_folder_list,
            asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
        )
        + f"change_{asset_suffix}_Afforestation_{start_year}_{int(end_year)+1}"
    ).reproject(crs="EPSG:4326", scale=25)

    # Overall tree change raster (DW + IndiaSAT)
    tree_change = tree_change.reproject(crs="EPSG:4326", scale=25)

    # STEP 2: Start from an empty canvas
    BACKGROUND = -9999
    masked_change_layer = ee.Image(BACKGROUND).rename(tree_change.bandNames())

    # STEP 3: IndiaSAT no-change area
    no_change_mask = afforestation.eq(1)
    masked_change_layer = masked_change_layer.where(no_change_mask, 0)

    # STEP 4: IndiaSAT deforestation --> -2
    defr_mask = deforestation.gte(2).And(deforestation.lte(5))
    masked_change_layer = masked_change_layer.where(defr_mask, -2)

    # STEP 5: IndiaSAT afforestation → 2
    aff_mask = afforestation.gte(2).And(afforestation.lte(5))
    masked_change_layer = masked_change_layer.where(aff_mask, 2)

    # STEP 6: Inside no-change, allow ONLY selected classes from tree change raster
    allowed_inside_no_change = (
        tree_change.eq(-1)  # degradation
        .Or(tree_change.eq(1))  # improvement
        .Or(tree_change.eq(3))  # partial degraded
        .Or(tree_change.eq(4))  # partial degraded
        .Or(tree_change.eq(5))  # missing data
    )

    masked_change_layer = masked_change_layer.where(
        no_change_mask.And(allowed_inside_no_change), tree_change
    )

    # STEP 7: Mask background
    masked_change_layer = masked_change_layer.updateMask(
        masked_change_layer.neq(BACKGROUND)
    )

    return masked_change_layer
