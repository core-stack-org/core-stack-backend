import ee
from nrm_app.celery import app
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    check_task_status,
    export_vector_asset_to_gee,
    make_asset_public,
)
from utilities.constants import GEE_DATASET_PATH
from computing.utils import sync_fc_to_geoserver, save_layer_info_to_db


@app.task(bind=True)
def generate_aquifer_vector(self, state, district, block):
    """Aquifer vector layer generation."""
    ee_initialize()

    description = f"aquifer_vector_{valid_gee_text(district)}_{valid_gee_text(block)}"
    input_asset_id = (
        get_gee_asset_path(state, district, block)
        + f"filtered_mws_{valid_gee_text(district)}_{valid_gee_text(block)}_uid"
    )
    roi = ee.FeatureCollection(input_asset_id)

    principal_aquifers = ee.FeatureCollection(GEE_DATASET_PATH + "/Aquifer_vector")

    # Yield value mapping dictionary
    yield_dict = ee.Dictionary(
        {
            "": "NA",
            "-": "NA",
            "Upto 2%": 0.02,
            "1-2%": 0.02,
            "Upto 1.5%": 0.015,
            "Upto 3%": 0.03,
            "Upto 2.5%": 0.025,
            "6 - 8%": 0.08,
            "1-1.5%": 0.015,
            "2-3%": 0.03,
            "Upto 4%": 0.04,
            "Upto 5%": 0.05,
            "Upto -3.5%": 0.035,
            "Upto 3 %": 0.03,
            "Upto 9%": 0.09,
            "1-2.5": 0.025,
            "Upto 1.2%": 0.012,
            "Upto 5-2%": 0.05,
            "Upto 1%": 0.01,
            "Up to 1.5%": 0.015,
            "Upto 8%": 0.08,
            "Upto 6%": 0.06,
            "0.08": 0.08,
            "8 - 16%": 0.16,
            "Not Explored": "NA",
            "8 - 15%": 0.15,
            "6 - 10%": 0.1,
            "6 - 15%": 0.15,
            "8 - 20%": 0.2,
            "8 - 10%": 0.1,
            "6 - 12%": 0.12,
            "6 - 16%": 0.16,
            "8 - 12%": 0.12,
            "8 - 18%": 0.18,
            "Upto 3.5%": 0.035,
            "Upto 15%": 0.15,
            "1.5-2%": 0.02,
        }
    )

    def map_yield(aquifer):
        yield_val = aquifer.get("yeild__")
        mapped_value = yield_dict.get(yield_val, "NA")
        return aquifer.set("y_value", mapped_value)

    # Process aquifers
    mapped = principal_aquifers.map(map_yield)
    aquifers_with_yield_value = mapped.filter(ee.Filter.neq("y_value", "NA"))

    def process_mws_feature(mws):
        """Process each MWS feature, handling holes/gaps properly"""
        mws_geom = mws.geometry()
        uid = mws.get("uid")
        feature_id = mws.get("id")
        area_in_ha = mws.get("area_in_ha")

        # Get all intersecting aquifers with valid yields
        intersecting_aquifers = aquifers_with_yield_value.filterBounds(mws_geom)

        def process_aquifer(aquifer):
            aquifer_geom = aquifer.geometry()
            intersection = mws_geom.intersection(aquifer_geom, 1)

            intersection_area = intersection.area(1)
            intersection_area_ha = intersection_area.divide(10000)

            fraction = ee.Number(intersection_area).divide(mws_geom.area(1))
            weighted_yield = fraction.multiply(aquifer.get("y_value"))

            principal_value = aquifer.get("Principal_")
            aquifer_class = ee.Algorithms.If(
                ee.String(principal_value).equals("Alluvium"), "Alluvium", "Hard-Rock"
            )

            properties = {
                "uid": uid,
                "id": feature_id,
                "area_in_ha": area_in_ha,
                "intersection_area_ha": intersection_area_ha,
                "%_area_aquifer": fraction.multiply(100),
                "weighted_contribution": weighted_yield,
                "aquifer_count": intersecting_aquifers.size(),
                "aquifer_class": aquifer_class,
                "Age": ee.String(aquifer.get("Age")).cat(""),
                "Lithology_": ee.Number(aquifer.get("Lithology_")).toInt(),
                "Major_Aq_1": ee.String(aquifer.get("Major_Aq_1")).cat(""),
                "Major_Aqui": ee.String(aquifer.get("Major_Aqui")).cat(""),
                "Principal_": ee.String(aquifer.get("Principal_")).cat(""),
                "Recommende": ee.Number(aquifer.get("Recommende")).toInt(),
                "area_re": ee.Number(aquifer.get("area_re")).toInt(),
                "avg_mbgl": ee.String(aquifer.get("avg_mbgl")).cat(""),
                "m2_perday": ee.String(aquifer.get("m2_perday")).cat(""),
                "m3_per_day": ee.String(aquifer.get("m3_per_day")).cat(""),
                "mbgl": ee.String(aquifer.get("mbgl")).cat(""),
                "newcode14": ee.String(aquifer.get("newcode14")).cat(""),
                "newcode43": ee.String(aquifer.get("newcode43")).cat(""),
                "objectid": ee.Number(aquifer.get("objectid")).toInt(),
                "pa_order": ee.Number(aquifer.get("pa_order")).toInt(),
                "per_cm": ee.String(aquifer.get("per_cm")).cat(""),
                "state": ee.String(aquifer.get("state")).cat(""),
                "system": ee.String(aquifer.get("system")).cat(""),
                "test": ee.String(aquifer.get("test")).cat(""),
                "yeild__": ee.String(aquifer.get("yeild__")).cat(""),
                "zone_m": ee.String(aquifer.get("zone_m")).cat(""),
                "y_value": aquifer.get("y_value"),
            }

            return ee.Feature(intersection, properties)

        aquifer_features = intersecting_aquifers.map(process_aquifer)
        return aquifer_features

    fc = roi.map(process_mws_feature).flatten()

    asset_id = get_gee_asset_path(state, district, block) + description
    task = export_vector_asset_to_gee(fc, description, asset_id)
    check_task_status([task])
    if is_gee_asset_exists(asset_id):
        save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=f"aquifer_vector_{district.title()}_{block.title()}",
            asset_id=asset_id,
            dataset_name="Aquifer",
        )
    make_asset_public(asset_id)
    fc = ee.FeatureCollection(asset_id)
    res = sync_fc_to_geoserver(fc, state, description, "aquifer")
    if res["status_code"] == 201:
        save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=f"aquifer_vector_{district.title()}_{block.title()}",
            asset_id=asset_id,
            dataset_name="Aquifer",
            sync_to_geoserver=True,
        )
    return res
