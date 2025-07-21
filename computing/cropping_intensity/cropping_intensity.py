import ee
from computing.utils import (
    sync_layer_to_geoserver,
    sync_fc_to_geoserver,
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
)
from nrm_app.celery import app


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
):
    ee_initialize()

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

    task_id, asset_id = generate_gee_asset(
        roi, asset_suffix, asset_folder_list, app_type, start_year, end_year
    )
    if task_id:
        task_id_list = check_task_status([task_id])
        print("Cropping intensity task completed - task_id_list:", task_id_list)

    make_asset_public(asset_id)

    # Export asset to Geoserver
    fc = ee.FeatureCollection(asset_id)
    layer_name = f"{asset_suffix}_intensity"
    res = sync_fc_to_geoserver(fc, asset_suffix, layer_name, "crop_intensity")
    print(res)


def generate_gee_asset(
    roi, asset_suffix, asset_folder_list, app_type, start_year, end_year
):
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
