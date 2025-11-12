import ee
from computing.utils import (
    sync_layer_to_geoserver,
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
    get_existing_end_year,
)
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_dir_path,
    is_gee_asset_exists,
    make_asset_public,
    export_vector_asset_to_gee,
    merge_fc_into_existing_fc,
)
from nrm_app.celery import app
from utilities.geoserver_utils import Geoserver
from dataclasses import dataclass
from typing import Optional
from computing.STAC_specs import generate_STAC_layerwise
geo = Geoserver()


@app.task(bind=True)
def generate_cropping_intensity(
    self,
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    start_year=None,
    end_year=None,
    gee_account_id=None,
):
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
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )
    layer_name = f"{asset_suffix}_intensity"
    description = "cropping_intensity_" + asset_suffix
    print(f"{description=}")
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )
    print(f"{asset_id=}")
    if is_gee_asset_exists(asset_id):
        existing_end_date = get_existing_end_year(
            "Cropping Intensity", f"{asset_suffix}_intensity"
        )
        print("end_year", end_year)
        if existing_end_date < end_year:
            new_start_year = existing_end_date
            new_asset_id = f"{asset_id}_{new_start_year}_{end_year}"
            if not is_gee_asset_exists(new_asset_id):
                print(f"{new_asset_id} doesn't exist")
                new_task_id, new_asset_id = generate_gee_asset(
                    roi,
                    asset_suffix,
                    asset_folder_list,
                    app_type,
                    new_start_year,
                    end_year,
                )
                if new_task_id:
                    check_task_status([new_task_id])
                    print("Cropping Intensity new year data generated.")
                else:
                    print("task id not found")

                # Check if data for new year is generated, if yes then merge it in existing asset
                if is_gee_asset_exists(new_asset_id):
                    merge_fc_into_existing_fc(asset_id, description, new_asset_id)

                # create dataclass object
                config = LayerConfig(
                    layer_name=layer_name,
                    asset_id=asset_id,
                    dataset_name="Cropping Intensity",
                    workspace="crop_intensity",
                    start_year=start_year,
                    end_year=end_year,
                    asset_suffix=asset_suffix,
                    state=state,
                    district=district,
                    block=block,
                )
                layer_at_geoserver = save_to_db_and_sync_to_geoserver(config)
                return layer_at_geoserver
        else:
            print("already upto date...")
            return True

    task_id, asset_id = generate_gee_asset(
        roi, asset_suffix, asset_folder_list, app_type, start_year, end_year
    )
    if task_id:
        task_id_list = check_task_status([task_id])
        print("Cropping intensity task completed - task_id_list:", task_id_list)

    config = LayerConfig(
        layer_name=layer_name,
        asset_id=asset_id,
        dataset_name="Cropping Intensity",
        workspace="crop_intensity",
        start_year=start_year,
        end_year=end_year,
        asset_suffix=asset_suffix,
        state=state,
        district=district,
        block=block,
    )
    layer_at_geoserver = save_to_db_and_sync_to_geoserver(config)
    return layer_at_geoserver


def generate_gee_asset(
    roi, asset_suffix, asset_folder_list, app_type, start_year, end_year
):
    print("inside generate_gee_asset function ")
    filename = (
        "cropping_intensity_"
        + asset_suffix
        + "_"
        + str(start_year)
        + "-"
        + str(end_year % 100)
    )
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + filename
    )

    if is_gee_asset_exists(asset_id):
        return None, asset_id

    lulc_scale = 10
    lulc_band_name = ["predicted_label"]
    lulc_js_list = []
    s_year = start_year  # START_YEAR
    while s_year <= end_year:
        lulc_js_list.append(
            ee.Image(
                get_gee_dir_path(
                    asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
                )
                + asset_suffix
                + "_"
                + str(s_year)
                + "-07-01_"
                + str(s_year + 1)
                + "-06-30_LULCmap_10m"
            )
        )
        s_year += 1
    lulc = ee.List(lulc_js_list)
    # Labels OLD
    # • 1: Greenery
    # • 2: Water
    # • 3: Builtup
    # • 4: Barrenland
    # • 5: Cropland
    # • 6: Forest
    # • 9: Single Kharif
    # • 10: Single Non Kharif
    # • 11: Double
    # • 12: Triple
    # Label New
    # 0 - Background
    # 1 - Built-up
    # 2 - Water in Kharif
    # 3 - Water in Kharif+Rabi
    # 4 - Water in Kharif+Rabi+Zaid
    # 6 - Tree/Forests
    # 7 - Barrenlands
    # 8 - Single cropping cropland
    # 9 - Single Non-Kharif cropping cropland
    # 10 - Double cropping cropland
    # 11 - Triple cropping cropland
    # 12 - Shrub_Scrub
    SINGLE_KHARIF = 8
    SINGLE_NON_KHARIF = 9
    DOUBLE = 10
    TRIPLE = 11
    args = [
        {"label": SINGLE_KHARIF, "txt": "single_kharif_cropped_area_"},
        {"label": SINGLE_NON_KHARIF, "txt": "single_non_kharif_cropped_area_"},
        {"label": DOUBLE, "txt": "doubly_cropped_area_"},
        {"label": TRIPLE, "txt": "triply_cropped_area_"},
    ]

    def get_class_area(feature):
        value = feature.get("sum")
        value = ee.Number(value).multiply(0.0001)
        return feature.set(arg["txt"] + str(sy), value)

    for arg in args:
        s_year = start_year
        while s_year <= end_year:
            sy = s_year
            image = ee.Image(lulc.get(sy - start_year)).select(lulc_band_name)
            mask = image.eq(ee.Number(arg["label"]))
            pixel_area = ee.Image.pixelArea()
            forestArea = pixel_area.updateMask(mask)
            roi = forestArea.reduceRegions(
                roi, ee.Reducer.sum(), lulc_scale, image.projection()
            )
            s_year += 1
            roi = roi.map(get_class_area)
    # single cropped area
    s_year = start_year

    def get_single_cropped_area(feature):
        single_kharif = ee.Number(feature.get("single_kharif_cropped_area_" + str(sy)))
        single_non_kharif = ee.Number(
            feature.get("single_non_kharif_cropped_area_" + str(sy))
        )
        return feature.set(
            "single_cropped_area_" + str(sy),
            single_kharif.add(single_non_kharif),
        )

    while s_year <= end_year:
        sy = s_year
        s_year += 1
        roi = roi.map(get_single_cropped_area)

    # croppable area
    single_kharif_all_years = ee.Image.constant(0)
    single_non_kharif_all_years = ee.Image.constant(0)
    triple_all_years = ee.Image.constant(0)
    double_all_years = ee.Image.constant(0)
    s_year = start_year

    while s_year <= end_year:
        sy = s_year
        s_year += 1
        image = ee.Image(lulc.get(sy - start_year)).select(lulc_band_name)
        single_kharif_all_years = single_kharif_all_years.Or(image.eq(SINGLE_KHARIF))
        single_non_kharif_all_years = single_non_kharif_all_years.Or(
            image.eq(SINGLE_NON_KHARIF)
        )
        double_all_years = double_all_years.Or(image.eq(DOUBLE))
        triple_all_years = triple_all_years.Or(image.eq(TRIPLE))

    croppable_area_all_years = (
        single_kharif_all_years.Or(single_non_kharif_all_years)
        .Or(triple_all_years)
        .Or(double_all_years)
    )
    mask = croppable_area_all_years
    pixel_area = ee.Image.pixelArea()
    croppable_area = pixel_area.updateMask(mask)
    roi = croppable_area.reduceRegions(roi, ee.Reducer.sum(), lulc_scale)

    def calculate_total_cropped_area(feature):
        value = feature.get("sum")
        value = ee.Number(value).multiply(0.0001)
        return feature.set(
            "total_cropable_area_ever_hydroyear_"
            + str(start_year)
            + "_"
            + str(end_year),
            value,
        )

    roi = roi.map(calculate_total_cropped_area)

    def calculate_cropping_intensity(feature):
        st_year = start_year
        while st_year <= end_year:
            year = st_year
            st_year += 1
            total_croppable_area = feature.get(
                "total_cropable_area_ever_hydroyear_"
                + str(start_year)
                + "_"
                + str(end_year)
            )
            total_croppable_area = ee.Number(total_croppable_area)

            single_cropped_area_ = feature.get("single_cropped_area_" + str(year))
            single_cropped_area_ = ee.Number(single_cropped_area_)

            double_cropped_area_ = feature.get("doubly_cropped_area_" + str(year))
            double_cropped_area_ = ee.Number(double_cropped_area_)

            triple_cropped_area_ = feature.get("triply_cropped_area_" + str(year))
            triple_cropped_area_ = ee.Number(triple_cropped_area_)

            sngl_frac = (single_cropped_area_.divide(total_croppable_area)).multiply(1)
            dbl_frac = (double_cropped_area_.divide(total_croppable_area)).multiply(1)
            trpl_frac = (triple_cropped_area_.divide(total_croppable_area)).multiply(1)

            cropping_intensity_ = sngl_frac.add(dbl_frac.multiply(2)).add(
                trpl_frac.multiply(3)
            )

            feature = feature.set(
                "cropping_intensity_" + str(year), cropping_intensity_
            )

        return feature

    roi = ee.FeatureCollection(roi.map(calculate_cropping_intensity))

    # Export feature collection to GEE
    task_id = export_vector_asset_to_gee(roi, filename, asset_id)
    return task_id, asset_id


@dataclass
class LayerConfig:
    layer_name: str
    asset_id: str
    dataset_name: str
    workspace: str
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    asset_suffix: Optional[str] = None
    state: Optional[str] = None
    district: Optional[str] = None
    block: Optional[str] = None


def save_to_db_and_sync_to_geoserver(config: LayerConfig):
    print("inside save_to_db_and_sync_to_geoserver")
    layer_id = None
    if (
        config.state and config.district and config.block
    ):  # TODO currently saving info to DB for block level layers only, make changes to accommodate all
        layer_id = save_layer_info_to_db(
            state=config.state,
            district=config.district,
            block=config.block,
            layer_name=config.layer_name,
            asset_id=config.asset_id,
            dataset_name=config.dataset_name,
            misc={
                "start_year": config.start_year,
                "end_year": config.end_year,
            },
        )

    make_asset_public(config.asset_id)

    fc = ee.FeatureCollection(config.asset_id)
    res = sync_fc_to_geoserver(
        fc,
        config.asset_suffix,
        config.layer_name,
        config.workspace,
    )
    print(res)
    layer_at_geoserver = False
    if (
        res["status_code"] == 201 and layer_id
    ):  # TODO currently saving info to DB for block level layers only, make changes to accommodate all
        update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
        print("sync to geoserver flag updated")

        layer_STAC_generated = False
        layer_STAC_generated = generate_STAC_layerwise.generate_vector_stac(
            state=config.state,
            district=config.district,
            block=config.block,
            layer_name='cropping_intensity_vector')
        update_layer_sync_status(layer_id=layer_id,
                                 is_stac_specs_generated=layer_STAC_generated)       
        layer_at_geoserver = True
    return layer_at_geoserver
