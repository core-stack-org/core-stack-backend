import ee
from computing.utils import (
    sync_layer_to_geoserver,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
)
from nrm_app.celery import app


@app.task(bind=True)
def generate_cropping_intensity(self, state, district, block, start_year, end_year):
    ee_initialize()

    task_id, asset_id = generate_gee_asset(state, district, block, start_year, end_year)
    if task_id:
        task_id_list = check_task_status([task_id])
        print("Cropping intensity task completed - task_id_list:", task_id_list)

    fc = ee.FeatureCollection(asset_id).getInfo()
    fc = {"features": fc["features"], "type": fc["type"]}
    res = sync_layer_to_geoserver(
        state,
        fc,
        valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_intensity",
        "cropping_intensity",
    )
    print(res)


def generate_gee_asset(state, district, block, start_year, end_year):
    filename = (
        "cropping_intensity_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_"
        + str(start_year)
        + "-"
        + str(end_year % 100)
    )
    asset_id = get_gee_asset_path(state, district, block) + filename

    if is_gee_asset_exists(asset_id):
        return None, asset_id

    fc = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )
    lulc_scale = 10
    lulc_bandname = "predicted_label"
    lulc_js_list = []
    s_year = start_year  # START_YEAR
    while s_year <= end_year:
        lulc_js_list.append(
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

    def res(feature):
        value = feature.get("sum")
        value = ee.Number(value)
        return feature.set(arg["txt"] + str(sy), value)  # sqm

    for arg in args:
        s_year = start_year
        while s_year <= end_year:
            sy = s_year
            image = ee.Image(lulc.get(sy - start_year)).select(lulc_bandname)
            mask = image.eq(ee.Number(arg["label"]))
            pixelArea = ee.Image.pixelArea()
            forestArea = pixelArea.updateMask(mask)
            fc = forestArea.reduceRegions(
                fc, ee.Reducer.sum(), lulc_scale, image.projection()
            )
            s_year += 1
            fc = fc.map(res)
    # single cropped area
    s_year = start_year

    def res1(feature):
        snglk = ee.Number(feature.get("single_kharif_cropped_area_" + str(sy)))
        snglnk = ee.Number(feature.get("single_non_kharif_cropped_area_" + str(sy)))
        return feature.set("single_cropped_area_" + str(sy), snglk.add(snglnk))

    while s_year <= end_year:
        sy = s_year
        s_year += 1
        fc = fc.map(res1)
    # cropable area
    snglk_allyears = ee.Image.constant(0)
    snglnk_allyears = ee.Image.constant(0)
    trpl_allyears = ee.Image.constant(0)
    dbl_allyears = ee.Image.constant(0)
    s_year = start_year
    while s_year <= end_year:
        sy = s_year
        s_year += 1
        image = ee.Image(lulc.get(sy - start_year)).select(lulc_bandname)
        snglk_allyears = snglk_allyears.Or(image.eq(SINGLE_KHARIF))
        snglnk_allyears = snglnk_allyears.Or(image.eq(SINGLE_NON_KHARIF))
        dbl_allyears = dbl_allyears.Or(image.eq(DOUBLE))
        trpl_allyears = trpl_allyears.Or(image.eq(TRIPLE))
    cropable_area_allyears = (
        snglk_allyears.Or(snglnk_allyears).Or(trpl_allyears).Or(dbl_allyears)
    )
    mask = cropable_area_allyears
    pixelArea = ee.Image.pixelArea()
    cropableArea = pixelArea.updateMask(mask)
    fc = cropableArea.reduceRegions(fc, ee.Reducer.sum(), lulc_scale)

    def res2(feature):
        value = feature.get("sum")
        value = ee.Number(value)
        return feature.set(
            "total_cropable_area_ever_hydroyear_"
            + str(start_year)
            + "_"
            + str(end_year),
            value,
        )  # sqm

    fc = fc.map(res2)

    def res3(feature):
        st_year = start_year
        while st_year <= end_year:
            year = st_year
            st_year += 1
            total_cropable_area = feature.get(
                "total_cropable_area_ever_hydroyear_"
                + str(start_year)
                + "_"
                + str(end_year)
            )
            total_cropable_area = ee.Number(total_cropable_area)

            single_cropped_area_ = feature.get("single_cropped_area_" + str(year))
            single_cropped_area_ = ee.Number(single_cropped_area_)

            double_cropped_area_ = feature.get("doubly_cropped_area_" + str(year))
            double_cropped_area_ = ee.Number(double_cropped_area_)

            triple_cropped_area_ = feature.get("triply_cropped_area_" + str(year))
            triple_cropped_area_ = ee.Number(triple_cropped_area_)

            sngl_frac = (single_cropped_area_.divide(total_cropable_area)).multiply(1)
            dbl_frac = (double_cropped_area_.divide(total_cropable_area)).multiply(1)
            trpl_frac = (triple_cropped_area_.divide(total_cropable_area)).multiply(1)

            cropping_intensity_ = sngl_frac.add(dbl_frac.multiply(2)).add(
                trpl_frac.multiply(3)
            )

            feature = feature.set(
                "cropping_intensity_" + str(year), cropping_intensity_
            )

        return feature

    fc = ee.FeatureCollection(fc.map(res3))
    try:
        ci_task = ee.batch.Export.table.toAsset(
            **{
                "collection": fc,
                "description": filename,
                "assetId": asset_id,
            }
        )
        ci_task.start()
        return ci_task.status()["id"], asset_id

    except Exception as e:
        print(f"Error occurred in running cropping_intensity task: {e}")
