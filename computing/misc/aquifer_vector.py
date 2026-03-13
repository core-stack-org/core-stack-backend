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
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from computing.STAC_specs import generate_STAC_layerwise


@app.task(bind=True)
def generate_aquifer_vector(self, state, district, block, gee_account_id):
    """
    Generate Aquifer Vector Layer

    This task:
    1. Loads watershed (MWS) geometry
    2. Intersects with aquifer dataset
    3. Computes weighted groundwater yield (Yield = Percentage of water that an aquifer can give out)
    4. Calculates aquifer type percentage coverage
    5. Determines dominant aquifer
    6. Exports result to GEE
    7. Syncs to GeoServer
    """
    ee_initialize(gee_account_id)

    description = f"aquifer_vector_{valid_gee_text(district)}_{valid_gee_text(block)}"
    roi_asset_id = (
        get_gee_asset_path(state, district, block)
        + f"filtered_mws_{valid_gee_text(district)}_{valid_gee_text(block)}_uid"
    )
    roi = ee.FeatureCollection(roi_asset_id)

    aquifers_fc = ee.FeatureCollection(GEE_DATASET_PATH + "/Aquifer_vector")

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

    # All known principal aquifer names — must match the Excel function list exactly
    aquifers_lists = [
        "Laterite",
        "Basalt",
        "Sandstone",
        "Shale",
        "Limestone",
        "Granite",
        "Schist",
        "Quartzite",
        "Charnockite",
        "Khondalite",
        "Banded Gneissic Complex",
        "Gneiss",
        "Intrusive",
        "Alluvium",
        "None",
    ]

    def map_yield(aquifer):
        yield_val = aquifer.get("yeild__")
        mapped_value = yield_dict.get(yield_val, "NA")
        return aquifer.set("y_value", mapped_value)

    # Pre-filter aquifers with valid yields ONCE
    aquifers_with_yield_value = aquifers_fc.map(map_yield).filter(
        ee.Filter.neq("y_value", "NA")
    )

    # Pre-clip aquifers to ROI bounding box ONCE to reduce per-feature filterBounds cost
    roi_bounds = roi.geometry().bounds()
    aquifers_in_roi = aquifers_with_yield_value.filterBounds(roi_bounds)

    def process_mws_feature(mws):
        mws_geom = mws.geometry()
        mws_area = mws_geom.area(1)
        uid = mws.get("uid")
        feature_id = mws.get("id")
        area_in_ha = mws.get("area_in_ha")

        intersecting_aquifers = aquifers_in_roi.filterBounds(mws_geom)

        # Single pass: intersection computed once, all stats derived from it
        def compute_intersection_stats(aquifer):
            intersection = mws_geom.intersection(aquifer.geometry(), 1)
            intersection_area = intersection.area(1)
            intersection_area_ha = intersection_area.divide(10000)
            fraction = ee.Number(intersection_area).divide(mws_area)
            weighted_yield = fraction.multiply(aquifer.get("y_value"))

            # Normalize empty principal name to "None"
            principal_raw = ee.String(aquifer.get("Principal_"))
            principal_name = ee.Algorithms.If(
                principal_raw.equals(""), "None", principal_raw
            )

            return (
                aquifer.set("intersection_area", intersection_area)
                .set("intersection_area_ha", intersection_area_ha)
                .set("%_area_aquifer", fraction.multiply(100))
                .set("weighted_contribution", weighted_yield)
                .set("principal_name", principal_name)
            )

        aquifers_processed = intersecting_aquifers.map(compute_intersection_stats)

        # Total weighted yield — summed across all aquifers
        total_weighted_yield = aquifers_processed.aggregate_sum("weighted_contribution")

        # For each watershed, find which aquifers overlap with it and calculate how much of the watershed each aquifer covers (%).
        names_list = aquifers_processed.aggregate_array("principal_name")
        pcts_list = aquifers_processed.aggregate_array("%_area_aquifer")

        # Build a dict: { aquifer_name -> sum of % across all patches of that type }
        def accumulate_pcts(aq_name, acc):
            acc = ee.Dictionary(acc)
            aq_name = ee.String(aq_name)
            # Sum all occurrences by iterating positions where name matches
            matching_pcts = pcts_list.zip(names_list).map(
                lambda pair: ee.Algorithms.If(
                    ee.String(ee.List(pair).get(1)).equals(aq_name),
                    ee.Number(ee.List(pair).get(0)),
                    0,
                )
            )
            total_pct = ee.Array(matching_pcts).reduce(ee.Reducer.sum(), [0]).get([0])
            return acc.set(aq_name, total_pct)

        aquifer_pct_dict = ee.Dictionary(
            ee.List(aquifers_lists).iterate(accumulate_pcts, ee.Dictionary({}))
        )

        # Flatten into individual named properties: principle_aq_{Name}_percent
        aquifer_pct_props = {
            f"principle_aq_{aq}_percent": aquifer_pct_dict.get(aq)
            for aq in aquifers_lists
        }

        # Dominant aquifer (largest intersection area) — used for attribute fields
        largest_aquifer = ee.Feature(
            aquifers_processed.sort("intersection_area", False).first()
        )

        principal_value = largest_aquifer.get("Principal_")
        aquifer_class = ee.Algorithms.If(
            ee.String(principal_value).equals("Alluvium"), "Alluvium", "Hard-Rock"
        )

        properties = {
            "uid": uid,
            "id": feature_id,
            "area_in_ha": area_in_ha,
            "total_weighted_yield": total_weighted_yield,
            "%_area_aquifer": largest_aquifer.get("%_area_aquifer"),
            "aquifer_count": intersecting_aquifers.size(),
            "aquifer_class": aquifer_class,
            # Per-aquifer % columns (principle_aq_{Name}_percent)
            **aquifer_pct_props,
            "Age": ee.String(largest_aquifer.get("Age")).cat(""),
            "Lithology_": ee.Number(largest_aquifer.get("Lithology_")).toInt(),
            "Major_Aq_1": ee.String(largest_aquifer.get("Major_Aq_1")).cat(""),
            "Major_Aqui": ee.String(largest_aquifer.get("Major_Aqui")).cat(""),
            "Principal_": ee.String(largest_aquifer.get("Principal_")).cat(""),
            "Recommende": ee.Number(largest_aquifer.get("Recommende")).toInt(),
            "yeild__": ee.String(largest_aquifer.get("yeild__")).cat(""),
            "zone_m": ee.String(largest_aquifer.get("zone_m")).cat(""),
            "y_value": largest_aquifer.get("y_value"),
        }

        return ee.Feature(mws_geom, properties)

    fc = roi.map(process_mws_feature)

    asset_id = get_gee_asset_path(state, district, block) + description
    if not is_gee_asset_exists(asset_id):
        task = export_vector_asset_to_gee(fc, description, asset_id)
        check_task_status([task])

    layer_at_geoserver = False
    if is_gee_asset_exists(asset_id):
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=f"aquifer_vector_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
            asset_id=asset_id,
            dataset_name="Aquifer",
        )
        make_asset_public(asset_id)

        fc = ee.FeatureCollection(asset_id)
        res = sync_fc_to_geoserver(fc, state, description, "aquifer")
        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")

            layer_STAC_generated = generate_STAC_layerwise.generate_vector_stac(
                state=state, district=district, block=block, layer_name="aquifer_vector"
            )
            update_layer_sync_status(
                layer_id=layer_id, is_stac_specs_generated=layer_STAC_generated
            )

            layer_at_geoserver = True

    return layer_at_geoserver
