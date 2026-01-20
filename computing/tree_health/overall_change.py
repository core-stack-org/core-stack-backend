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
    print("Inside process Tree health ch raster")
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

    description = (
        "overall_change_raster_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )

    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )

    # Skip if asset already exists
    if not is_gee_asset_exists(asset_id):

        overall_change_raster = ee.ImageCollection(TREE_OVERALL_CHANGE)

        raster = (
            overall_change_raster.filterBounds(roi.geometry())
            .mean()
            .clip(roi.geometry())
        )

        raster = mask_raster(
            app_type, asset_folder_list, asset_suffix, raster, start_year, end_year
        )

        task_id = export_raster_asset_to_gee(
            image=raster,
            description=description,
            asset_id=asset_id,
            scale=25,
            region=roi.geometry(),
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
    app_type, asset_folder_list, asset_suffix, raster, start_year, end_year
):
    deforestation = ee.Image(
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + f"change_{asset_suffix}_Deforestation_{start_year}_{int(end_year)+1}"  # TODO Fix later with some better logic
    )
    afforestation = ee.Image(
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + f"change_{asset_suffix}_Afforestation_{start_year}_{int(end_year)+1}"
    )

    # Ignore Deforestation (-2) and Afforestation (2) pixels
    ignore_mask = raster.neq(-2).And(raster.neq(2))
    raster = raster.updateMask(ignore_mask)

    # Apply no change mask to the raster
    no_change_mask = afforestation.eq(1)
    raster = raster.updateMask(no_change_mask)

    # Join Degradation and Afforestation (to cover for tree cover gain/loss)
    defr_mask = (
        deforestation.eq(2)
        .Or(deforestation.eq(3))
        .Or(deforestation.eq(4))
        .Or(deforestation.eq(5))
    )
    aff_mask = (
        afforestation.eq(2)
        .Or(afforestation.eq(3))
        .Or(afforestation.eq(4))
        .Or(afforestation.eq(5))
    )
    # Apply the IndiaSAT LULC change pixel values into the raster
    # Deforestation pixels: write values from degradation
    BACKGROUND = -9999
    raster = raster.unmask(BACKGROUND)
    raster = raster.where(defr_mask, -2)

    # Afforestation pixels: write values from afforestation
    raster = raster.where(aff_mask, 2)

    raster = raster.updateMask(raster.neq(BACKGROUND))

    return raster
