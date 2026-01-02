import ee
import copy
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    export_raster_asset_to_gee,
    is_gee_asset_exists,
    make_asset_public,
)
from nrm_app.celery import app
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from computing.STAC_specs import generate_STAC_layerwise


@app.task(bind=True)
def get_change_detection(
    self, state, district, block, start_year, end_year, gee_account_id
):
    # Initialize the Earth Engine
    ee_initialize(gee_account_id)
    param_dict = {
        "Urbanization": built_up,
        "Degradation": change_degradation,
        "Deforestation": change_deforestation,
        "Afforestation": change_afforestation,
        "CropIntensity": change_cropping_intensity,
    }
    description = (
        f"change_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
    )
    l1_asset = []
    s_year = start_year
    while s_year <= end_year:
        l1_asset.append(
            ee.Image(
                get_gee_asset_path(state, district, block)
                + valid_gee_text(district.lower())
                + "_"
                + valid_gee_text(block.lower())
                + "_"
                + str(s_year)
                + "-07-01_"
                + str(s_year + 1)
                + "-06-30_LULCmap_10m"
            )
        )
        s_year += 1

    # Filter for the region of interest
    roi_boundary = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )
    task_list = []

    for change_detection_key, change_detection_values in param_dict.items():
        ch_description = f"{description}_{change_detection_key}_{start_year}_{end_year}"
        asset_id = get_gee_asset_path(state, district, block) + ch_description

        if not is_gee_asset_exists(asset_id):
            print(f"{asset_id} doesn't exist")

            result = eval("change_detection_values(roi_boundary, l1_asset)")
            task_id = export_raster_asset_to_gee(
                image=result,
                description=ch_description,
                asset_id=asset_id,
                scale=10,
                region=roi_boundary.geometry(),
            )
            task_list.append(task_id)
    task_id_list = check_task_status(task_list)
    print("Change detection task_id_list", task_id_list)

    layer_ids = {}
    for param in param_dict.keys():
        ch_description = f"{description}_{param}_{start_year}_{end_year}"
        asset_id = get_gee_asset_path(state, district, block) + ch_description
        if is_gee_asset_exists(asset_id):
            layer_id = save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=f"change_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_{param}",
                asset_id=asset_id,
                dataset_name="Change Detection Raster",
                misc={
                    "start_year": start_year,
                    "end_year": end_year,
                },
            )
            layer_ids[param] = layer_id
            make_asset_public(asset_id)

    layer_at_geoserver = sync_to_gcs_geoserver(
        state,
        district,
        block,
        description,
        param_dict.keys(),
        layer_ids,
        start_year,
        end_year,
    )
    return layer_at_geoserver


def built_up(roi_boundary, l1_asset):
    print("built_up function is runing")

    lulc_projection = l1_asset[0].projection()

    # Remap values function
    def remap_values(image):
        return image.remap(
            [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12],
            [1, 2, 2, 2, 3, 4, 3, 3, 3, 3, 4],
            0,
            "predicted_label",
        ).setDefaultProjection(lulc_projection)

    l1_asset_remapped = [remap_values(asset) for asset in l1_asset]

    # Create image collections
    then = ee.ImageCollection(l1_asset_remapped[:3]).mode().reproject(lulc_projection)
    now = ee.ImageCollection(l1_asset_remapped[3:]).mode().reproject(lulc_projection)

    # Compute mode and clip
    then = then.clip(roi_boundary.geometry())
    now = now.clip(roi_boundary.geometry())

    # Compute transitions
    trans_bu_bu = then.eq(1).And(now.eq(1))
    trans_w_bu = then.eq(2).And(now.eq(1)).multiply(2)
    trans_tr_bu = then.eq(3).And(now.eq(1)).multiply(3)
    trans_b_bu = then.eq(4).And(now.eq(1)).multiply(4)

    # Create a zero image and add transitions
    change_bu = (
        ee.Image.constant(0)
        .setDefaultProjection(lulc_projection)
        .clip(roi_boundary.geometry())
    )
    change_bu = (
        change_bu.add(trans_bu_bu).add(trans_w_bu).add(trans_tr_bu).add(trans_b_bu)
    )
    return change_bu


def change_degradation(roi_boundary, l1_asset):
    lulc_projection = l1_asset[0].projection()

    # Remap values function
    def remap_values(image):
        return image.remap(
            [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12],
            [1, 2, 2, 2, 4, 5, 3, 3, 3, 3, 6],
            0,
            "predicted_label",
        ).setDefaultProjection(lulc_projection)

    l1_asset_remapped = [remap_values(asset) for asset in l1_asset]

    # Create image collections
    then = ee.ImageCollection(l1_asset_remapped[:3]).mode().reproject(lulc_projection)
    now = ee.ImageCollection(l1_asset_remapped[3:]).mode().reproject(lulc_projection)

    # Compute mode and clip
    then = then.clip(roi_boundary.geometry())
    now = now.clip(roi_boundary.geometry())

    trans_f_f = then.eq(3).And(now.eq(3))
    trans_f_bu = then.eq(3).And(now.eq(1)).multiply(2)
    trans_f_ba = then.eq(3).And(now.eq(5)).multiply(3)
    trans_f_sc = then.eq(3).And(now.eq(6)).multiply(4)

    # Create a zero image and add transitions
    change_deg = (
        ee.Image.constant(0)
        .setDefaultProjection(lulc_projection)
        .clip(roi_boundary.geometry())
    )
    change_deg = (
        change_deg.add(trans_f_f).add(trans_f_bu).add(trans_f_ba).add(trans_f_sc)
    )
    return change_deg


def change_deforestation_afforestation(roi_boundary, l1_asset, lulc_projection):
    print("change_deforestation is running")
    # Create an initial zero image
    zero_image2 = (
        ee.Image.constant(0)
        .setDefaultProjection(lulc_projection)
        .clip(l1_asset[0].geometry())
    )

    # for i in range(1, 5):
    for i in range(1, len(l1_asset) - 1):
        before = l1_asset[i - 1]
        middle = l1_asset[i]
        after = l1_asset[i + 1]

        cond1 = (
            before.eq(12)
            .And(after.eq(12))
            .And(
                middle.eq(6)
                .Or(middle.eq(8))
                .Or(middle.eq(9))
                .Or(middle.eq(10))
                .Or(middle.eq(11))
            )
        )
        cond2 = (
            before.eq(2)
            .Or(before.eq(3))
            .Or(before.eq(4))
            .And(after.eq(2).Or(after.eq(3)).Or(after.eq(4)))
            .And(
                middle.eq(6)
                .Or(middle.eq(8))
                .Or(middle.eq(9))
                .Or(middle.eq(10))
                .Or(middle.eq(11))
            )
        )
        cond3 = before.eq(6).And(after.eq(6)).And(middle.eq(12))
        cond4 = (
            before.eq(8)
            .Or(before.eq(9))
            .Or(before.eq(10))
            .Or(before.eq(11))
            .And(after.eq(8).Or(after.eq(9)).Or(after.eq(10)).Or(after.eq(11)))
            .And(middle.eq(12))
        )
        cond5 = (
            before.eq(8)
            .Or(before.eq(9))
            .Or(before.eq(10))
            .Or(before.eq(11))
            .And(after.eq(8).Or(after.eq(9)).Or(after.eq(10)).Or(after.eq(11)))
            .And(middle.eq(7))
        )
        cond6 = (
            before.eq(6)
            .And(after.eq(6))
            .And(middle.eq(8).Or(middle.eq(9)).Or(middle.eq(10)).Or(middle.eq(11)))
        )
        cond7 = (
            before.eq(8)
            .Or(before.eq(9))
            .Or(before.eq(10))
            .Or(before.eq(11))
            .And(after.eq(8).Or(after.eq(9)).Or(after.eq(10)).Or(after.eq(11)))
            .And(middle.eq(6))
        )
        cond8 = before.eq(1).And(after.eq(1)).And(middle.eq(6))
        cond9 = before.eq(6).And(after.eq(6)).And(middle.eq(1))
        cond10 = (
            before.eq(1)
            .And(after.eq(1))
            .And(middle.eq(8).Or(middle.eq(9)).Or(middle.eq(10)).Or(middle.eq(11)))
        )
        cond11 = (
            before.eq(7)
            .And(after.eq(7))
            .And(
                middle.eq(6)
                .Or(middle.eq(8))
                .Or(middle.eq(9))
                .Or(middle.eq(10))
                .Or(middle.eq(11))
            )
        )

        zero_image2 = (
            zero_image2.add(cond1)
            .add(cond2)
            .add(cond3)
            .add(cond4)
            .add(cond5)
            .add(cond6)
            .add(cond7)
            .add(cond8)
            .add(cond9)
            .add(cond10)
            .add(cond11)
        )

    l1_asset_copy = copy.deepcopy(l1_asset)
    for i in range(1, len(l1_asset) - 1):
        # for i in range(1, 5):
        before = l1_asset[i - 1]
        middle = l1_asset[i]
        after = l1_asset[i + 1]

        cond1 = (
            before.eq(3)
            .And(middle.neq(3))
            .And(after.eq(3))
            .And((zero_image2.eq(3).Or(zero_image2.eq(4))))
        )
        cond2 = (
            before.neq(3)
            .And(middle.eq(3))
            .And(after.neq(3))
            .And((zero_image2.eq(3).Or(zero_image2.eq(4))))
        )

        middle = middle.where(cond1, 3)
        middle = middle.where(cond2, before)

        l1_asset_copy[i] = middle

    # Remap values function
    def remap_values(image):
        remapped = image.remap(
            [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12],
            [1, 2, 2, 2, 3, 5, 4, 4, 4, 4, 6],
            0,
            "predicted_label",
        ).setDefaultProjection(lulc_projection)
        return remapped

    l1_asset_remapped = [remap_values(asset) for asset in l1_asset_copy]

    # Create image collections
    then = ee.ImageCollection(l1_asset_remapped[:3]).mode().reproject(lulc_projection)
    now = ee.ImageCollection(l1_asset_remapped[3:]).mode().reproject(lulc_projection)

    # Compute mode and clip
    then = then.clip(roi_boundary.geometry())
    now = now.clip(roi_boundary.geometry())
    return now, then


def change_deforestation(roi_boundary, l1_asset):
    lulc_projection = l1_asset[0].projection()
    now, then = change_deforestation_afforestation(
        roi_boundary, l1_asset, lulc_projection
    )
    trans_fo_fo = then.eq(3).And(now.eq(3))
    trans_fo_bu = then.eq(3).And(now.eq(1)).multiply(2)
    trans_fo_fa = then.eq(3).And(now.eq(4)).multiply(3)
    trans_fo_ba = then.eq(3).And(now.eq(5)).multiply(4)
    trans_sc = then.eq(3).And(now.eq(6)).multiply(5)
    # Create a zero image and add transitions
    change_def = (
        ee.Image.constant(0)
        .setDefaultProjection(lulc_projection)
        .clip(roi_boundary.geometry())
    )
    change_def = (
        change_def.add(trans_fo_fo)
        .add(trans_fo_bu)
        .add(trans_fo_fa)
        .add(trans_fo_ba)
        .add(trans_sc)
    )
    return change_def


def change_afforestation(roi_boundary, l1_asset):
    lulc_projection = l1_asset[0].projection()
    now, then = change_deforestation_afforestation(
        roi_boundary, l1_asset, lulc_projection
    )
    trans_fo_fo = then.eq(3).And(now.eq(3))
    trans_bu_fo = then.eq(1).And(now.eq(3)).multiply(2)
    trans_fa_fo = then.eq(4).And(now.eq(3)).multiply(3)
    trans_ba_fo = then.eq(5).And(now.eq(3)).multiply(4)
    trans_sc_fo = then.eq(6).And(now.eq(3)).multiply(5)

    # Create a zero image and add transitions
    change_af = (
        ee.Image.constant(0)
        .setDefaultProjection(lulc_projection)
        .clip(roi_boundary.geometry())
    )
    change_af = (
        change_af.add(trans_fo_fo)
        .add(trans_bu_fo)
        .add(trans_fa_fo)
        .add(trans_ba_fo)
        .add(trans_sc_fo)
    )
    return change_af


def change_cropping_intensity(roi_boundary, l1_asset):
    lulc_projection = l1_asset[0].projection()

    # Remap values function
    def remap_values(image):
        return image.remap(
            [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12],
            [1, 2, 2, 2, 3, 4, 5, 5, 6, 7, 8],
            0,
            "predicted_label",
        ).setDefaultProjection(lulc_projection)

    l1_asset_remapped = [remap_values(asset) for asset in l1_asset]

    # Create image collections
    then = ee.ImageCollection(l1_asset_remapped[:3]).mode().reproject(lulc_projection)
    now = ee.ImageCollection(l1_asset_remapped[3:]).mode().reproject(lulc_projection)

    # Compute mode and clip
    then = then.clip(roi_boundary.geometry())
    now = now.clip(roi_boundary.geometry())

    trans_do_si = then.eq(6).And(now.eq(5))
    trans_tr_si = then.eq(7).And(now.eq(5)).multiply(2)
    trans_tr_do = then.eq(7).And(now.eq(6)).multiply(3)
    trans_si_do = then.eq(5).And(now.eq(6)).multiply(4)
    trans_si_tr = then.eq(5).And(now.eq(7)).multiply(5)
    trans_do_tr = then.eq(6).And(now.eq(7)).multiply(6)
    trans_same = (
        (then.eq(5).And(now.eq(5)))
        .Or(then.eq(6).And(now.eq(6)))
        .Or(then.eq(7).And(now.eq(7)))
        .multiply(7)
    )

    # Create a zero image and add transitions
    change_far = (
        ee.Image.constant(0)
        .setDefaultProjection(lulc_projection)
        .clip(roi_boundary.geometry())
    )
    change_far = (
        change_far.add(trans_do_si)
        .add(trans_tr_si)
        .add(trans_tr_do)
        .add(trans_si_do)
        .add(trans_si_tr)
        .add(trans_do_tr)
        .add(trans_same)
    )
    return change_far


def sync_to_gcs_geoserver(
    state, district, block, description, param_list, layer_ids, start_year, end_year
):
    task_list = []

    stac_spec_layer_name_dict = {
        "Urbanization": "change_urbanization_raster",
        "Degradation": "change_cropping_reduction_raster",
        "Deforestation": "change_tree_cover_loss_raster",
        "Afforestation": "change_tree_cover_gain_raster",
        "CropIntensity": "change_cropping_intensity_raster",
    }

    for change in param_list:
        image = ee.Image(
            get_gee_asset_path(state, district, block)
            + f"{description}_{change}_{start_year}_{end_year}"
        )
        task_id = sync_raster_to_gcs(
            image, 10, f"{description}_{change}_{start_year}_{end_year}"
        )
        task_list.append(task_id)
    task_id_list = check_task_status(task_list)
    print("task_id sync to gcs ", task_id_list)

    layer_at_geoserver = []
    for change in param_list:
        res = sync_raster_gcs_to_geoserver(
            "change_detection",
            f"{description}_{change}_{start_year}_{end_year}",
            description + "_" + change,
            change.lower(),
        )
        if res and layer_ids[change]:
            sync_status = update_layer_sync_status(
                layer_id=layer_ids[change], sync_to_geoserver=True
            )
            print("sync to geoserver flag updated")

            layer_name = stac_spec_layer_name_dict[change]
            layer_STAC_generated = False
            layer_STAC_generated = generate_STAC_layerwise.generate_raster_stac(
                state=state, district=district, block=block, layer_name=layer_name
            )
            update_layer_sync_status(
                layer_id=layer_ids[change], is_stac_specs_generated=layer_STAC_generated
            )

            if sync_status:
                layer_at_geoserver.append(sync_status)

    return len(layer_at_geoserver) == len(param_list)
