import ee
from nrm_app.celery import app
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    sync_raster_to_gcs,
    check_task_status,
    sync_raster_gcs_to_geoserver,
    export_vector_asset_to_gee,
    make_asset_public,
)
from computing.utils import (
    sync_layer_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from computing.utils import sync_fc_to_geoserver
from utilities.constants import GEE_DATASET_PATH


@app.task(bind=True)
def generate_soge_vector(self, state, district, block):
    """Generate vector layer for the SOGE - Stage of Ground Water Extraction"""
    ee_initialize()

    description = f"soge_vector_{valid_gee_text(district)}_{valid_gee_text(block)}"
    asset_path = get_gee_asset_path(state, district, block)
    asset_id = asset_path + description

    soge_fc = ee.FeatureCollection(GEE_DATASET_PATH + "/SOGE_vector_2020")

    if is_gee_asset_exists(asset_id):
        print(f"Asset already exists: {asset_id}")
        return

    mws_asset_id = (
        asset_path
        + f"filtered_mws_{valid_gee_text(district)}_{valid_gee_text(block)}_uid"
    )
    mws_fc = ee.FeatureCollection(mws_asset_id)

    def process_mws(mws_feature):
        mws_geom = mws_feature.geometry()
        uid = mws_feature.get("uid")
        area_in_ha = mws_feature.get("area_in_ha")
        mws_area_ha = ee.Number(mws_geom.area(10)).divide(10000)
        soge_within_mws = soge_fc.filterBounds(mws_geom)

        def handle_no_data():
            return ee.Feature(
                mws_geom,
                {
                    "uid": uid,
                    "area_in_ha": area_in_ha,
                    "max_intersection_area_ha": 0,
                    "pct_area_soge": 0,
                    "class": "No Data",
                    "agwd_dom_i": -9999,
                    "agwd_irr": -9999,
                    "agwd_tot": -9999,
                    "ar_gwr_tot": -9999,
                    "code": -9999,
                    "gwr_2011_2": -9999,
                    "na_gwa": -9999,
                    "nat_discha": -9999,
                    "sgw_dev_pe": -9999,
                    "soge_block": "",
                    "soge_district": "",
                    "soge_objectid": -9999,
                    "soge_state": "",
                    "soge_tehsil": "",
                },
            )

        def handle_intersections():
            def calculate_intersection(soge_feature):
                soge_geom = soge_feature.geometry()
                intersection_geom = soge_geom.intersection(mws_geom, 10)
                intersection_area_ha = ee.Number(intersection_geom.area(10)).divide(
                    10000
                )
                return soge_feature.set({"intersection_area_ha": intersection_area_ha})

            soge_with_intersections = soge_within_mws.map(calculate_intersection)
            largest_soge = soge_with_intersections.sort(
                "intersection_area_ha", False
            ).first()
            largest_intersection_area = largest_soge.get("intersection_area_ha")
            pct_area = (
                ee.Number(largest_intersection_area).divide(mws_area_ha).multiply(100)
            )

            return ee.Feature(
                mws_geom,
                {
                    "uid": uid,
                    "area_in_ha": area_in_ha,
                    "max_intersection_area_ha": largest_intersection_area,
                    "pct_area_soge": pct_area,
                    "class": largest_soge.get("class"),
                    "agwd_dom_i": largest_soge.get("agwd_dom_i"),
                    "agwd_irr": largest_soge.get("agwd_irr"),
                    "agwd_tot": largest_soge.get("agwd_tot"),
                    "ar_gwr_tot": largest_soge.get("ar_gwr_tot"),
                    "code": largest_soge.get("code"),
                    "gwr_2011_2": largest_soge.get("gwr_2011_2"),
                    "na_gwa": largest_soge.get("na_gwa"),
                    "nat_discha": largest_soge.get("nat_discha"),
                    "sgw_dev_pe": largest_soge.get("sgw_dev_pe"),
                    "soge_block": largest_soge.get("block"),
                    "soge_district": largest_soge.get("district"),
                    "soge_objectid": largest_soge.get("objectid"),
                    "soge_state": largest_soge.get("state"),
                    "soge_tehsil": largest_soge.get("tehsil"),
                },
            )

        return ee.Algorithms.If(
            soge_within_mws.size().gt(0), handle_intersections(), handle_no_data()
        )

    all_results = mws_fc.map(process_mws)

    # EXPORT TO GEE ASSET
    task = export_vector_asset_to_gee(all_results, description, asset_id)
    check_task_status([task])

    if is_gee_asset_exists(asset_id):
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=f"soge_vector_{district.lower()}_{block.lower()}",
            asset_id=asset_id,
            dataset_name="SOGE",
        )
        make_asset_public(asset_id)

        print("Geoserver Sync task started")
        fc = ee.FeatureCollection(asset_id)
        res = sync_fc_to_geoserver(fc, state, description, "soge")
        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
