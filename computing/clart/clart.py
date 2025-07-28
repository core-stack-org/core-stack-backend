import ee
import math
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    export_raster_asset_to_gee,
)
from utilities.constants import GEE_ASSET_PATH
from nrm_app.celery import app
from .drainage_density import drainage_density
from .lithology import generate_lithology_layer
from computing.utils import save_layer_info_to_db


@app.task(bind=True)
def generate_clart_layer(self, state, district, block):
    ee_initialize()
    drainage_density(state, district, block)
    generate_lithology_layer(state, district)
    clart_layer(state, district, block)


def clart_layer(state, district, block):
    description = (
        "clart_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )
    final_output_assetid = get_gee_asset_path(state, district, block) + description
    layer_name = (
        valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_clart"
    )
    if not is_gee_asset_exists(final_output_assetid):
        srtm = ee.Image("USGS/SRTMGL1_003")
        india_lin = ee.Image("projects/ee-harshita-om/assets/india_lineaments")
        roi = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )
        drainage = ee.Image(
            get_gee_asset_path(state, district, block)
            + "drainage_density_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
        )
        lithology = ee.Image(
            GEE_ASSET_PATH
            + valid_gee_text(state.lower())
            + "/"
            + valid_gee_text(state.lower())
            + "_lithology"
        )

        lin = india_lin.clip(roi)
        lith = lithology.clip(roi)
        dd = drainage.clip(roi)

        geometry = roi.geometry()

        gayaDEM = srtm.clip(geometry)

        # Calculating Slope
        slope = ee.Terrain.slope(gayaDEM)

        # Converting slope to slope percentage
        sp = slope.divide(180).multiply(math.pi).tan().multiply(100)

        lin_present = ee.Number(10)
        lin_absent = ee.Number(1)

        lin = lin.where(lin.eq(0), -1)
        lin = lin.where(lin.eq(1), -2)
        lin = lin.where(lin.eq(-1), lin_absent)
        lin = lin.where(lin.eq(-2), lin_present)

        max = dd.reduceRegion(
            reducer=ee.Reducer.max(), geometry=roi.geometry(), scale=30, maxPixels=1e9
        )

        min = dd.reduceRegion(
            reducer=ee.Reducer.min(), geometry=roi.geometry(), scale=30, maxPixels=1e9
        )

        # print(min, max)

        maxValue = ee.Number(max.get("b1"))
        minValue = ee.Number(min.get("b1"))
        diff = maxValue.subtract(minValue)

        dd = dd.subtract(minValue).divide(diff)

        dd_l1 = ee.Number(0.334)
        dd_s1 = ee.Number(1)  # score for areas in range 0 to dd_l1
        dd_l2 = ee.Number(0.667)
        dd_s2 = ee.Number(2)  # score for areas in range l1 to l2
        dd_s3 = ee.Number(3)  # score for areas in range l2 to 1

        dd = dd.where(dd.lte(dd_l1), -1000)
        dd = dd.where(dd.gt(dd_l1).And(dd.lte(dd_l2)), -2000)
        dd = dd.where(dd.gt(dd_l2), -3000)
        dd = dd.where(dd.eq(-1000), dd_s1)
        dd = dd.where(dd.eq(-2000), dd_s2)
        dd = dd.where(dd.eq(-3000), dd_s3)

        rp = dd.multiply(lin.multiply(lith))

        high_mask = (
            rp.select(["b1"])
            .eq(1)
            .Or(rp.select(["b1"]).eq(2))
            .Or(rp.select(["b1"]).eq(10))
            .Or(rp.select(["b1"]).eq(20))
            .Or(rp.select(["b1"]).eq(30))
            .Or(rp.select(["b1"]).eq(40))
            .Or(rp.select(["b1"]).eq(60))
            .Or(rp.select(["b1"]).eq(90))
        )
        rp = rp.where(high_mask, 1)

        med_mask = rp.select(["b1"]).eq(3).Or(rp.select(["b1"]).eq(4))
        rp = rp.where(med_mask, 2)

        low_mask = rp.select(["b1"]).eq(6).Or(rp.select(["b1"]).eq(9))
        rp = rp.where(low_mask, 3)

        else_mask = (
            rp.select(["b1"])
            .neq(1)
            .And(rp.select(["b1"]).neq(2))
            .And(rp.select(["b1"]).neq(3))
            .And(rp.select(["b1"]).neq(4))
            .And(rp.select(["b1"]).neq(6))
            .And(rp.select(["b1"]).neq(9))
            .And(rp.select(["b1"]).neq(10))
            .And(rp.select(["b1"]).neq(20))
            .And(rp.select(["b1"]).neq(30))
            .And(rp.select(["b1"]).neq(40))
            .And(rp.select(["b1"]).neq(60))
            .And(rp.select(["b1"]).neq(90))
        )
        rp = rp.where(else_mask, 0)

        max_sp = sp.reduceRegion(
            reducer=ee.Reducer.max(), geometry=geometry, maxPixels=1e9
        ).get("slope")
        # print(max_sp)

        tc = rp  # all classes init to 0
        tc = rp.where(rp.select(["b1"]), 0)

        class1 = (
            rp.select(["b1"])
            .eq(1)
            .And(sp.gte(ee.Number(max_sp).multiply(0.0)))
            .And(sp.lte(ee.Number(max_sp).multiply(0.20)))
        )
        tc = tc.where(class1, 1)

        class2 = (
            rp.select(["b1"])
            .eq(2)
            .And(sp.gte(ee.Number(max_sp).multiply(0.0)))
            .And(sp.lte(ee.Number(max_sp).multiply(0.25)))
        )
        tc = tc.where(class2, 2)

        class3 = (
            rp.select(["b1"])
            .eq(3)
            .And(sp.gte(ee.Number(max_sp).multiply(0.0)))
            .And(sp.lte(ee.Number(max_sp).multiply(0.20)))
        )
        tc = tc.where(class3, 3)

        class4 = (
            rp.select(["b1"])
            .eq(1)
            .Or(rp.select(["b1"]).eq(2))
            .Or(rp.select(["b1"]).eq(3))
            .And(sp.gte(ee.Number(max_sp).multiply(0.25)))
            .And(sp.lte(ee.Number(max_sp).multiply(0.30)))
        )
        tc = tc.where(class4, 4)

        class5 = (
            rp.select(["b1"])
            .eq(1)
            .Or(rp.select(["b1"]).eq(2))
            .Or(rp.select(["b1"]).eq(3))
            .And(sp.gt(ee.Number(max_sp).multiply(0.30)))
        )
        tc = tc.where(class5, 5)

        try:
            scale = 30
            task_id = export_raster_asset_to_gee(
                image=tc,
                description=description,
                asset_id=final_output_assetid,
                scale=scale,
                region=geometry,
            )

            clart_task_id_list = check_task_status([task_id])
            print("clart_task_id_list", clart_task_id_list)
            if is_gee_asset_exists(final_output_assetid):
                save_layer_info_to_db(
                    state,
                    district,
                    block,
                    layer_name=layer_name,
                    asset_id=final_output_assetid,
                    dataset_name="CLART",
                )
        except Exception as e:
            print(f"Error occurred in running clart: {e}")

    """ Sync image to google cloud storage and then to geoserver"""

    image = ee.Image(final_output_assetid)
    task_id = sync_raster_to_gcs(image, 30, layer_name)

    task_id_list = check_task_status([task_id])
    print("task_id_list sync to gcs ", task_id_list)

    res = sync_raster_gcs_to_geoserver("clart", layer_name, layer_name, "testClart")
    if res:
        save_layer_info_to_db(
            state,
            district,
            block,
            layer_name,
            final_output_assetid,
            "CLART",
            sync_to_geoserver=True,
        )
