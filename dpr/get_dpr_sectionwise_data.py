import geopandas as gpd
from .gen_dpr import (
    get_settlement_coordinates_for_plan,
    get_mws_uid_for_settlement_gdf,
    get_data_for_settlement,
    get_crops_data,
    get_livestock_data,
    get_all_wells_with_mws,
    get_all_waterbodies_with_mws,
    sort_key,
)
from .utils import to_utf8
from collections import defaultdict
from shapely.geometry import Point
from dpr.utils import ensure_str, get_waterbody_repair_activities
from .templatetags.custom_filters import format_text
from .mapping import populate_maintenance_from_waterbody
from .services import get_maintenance_data, get_nrm_works_data, get_livelihood_data


def get_section_b_data(plan, total_settlements, mws_fortnight):

    mws_gdf = gpd.GeoDataFrame.from_features(mws_fortnight["features"])

    settlement_mws_ids = []
    settlement_coordinates = get_settlement_coordinates_for_plan(plan.id)

    for settlement_name, latitude, longitude in settlement_coordinates:
        mws_uid = get_mws_uid_for_settlement_gdf(mws_gdf, latitude, longitude)

        if mws_uid:
            settlement_mws_ids.append(
                {
                    "settlement": settlement_name,
                    "mws_id": mws_uid,
                }
            )

    centroid = None

    if settlement_mws_ids:
        intersecting_mws = mws_gdf[
            mws_gdf["uid"].isin([item["mws_id"] for item in settlement_mws_ids])
        ]

        if not intersecting_mws.empty:
            centroid = intersecting_mws.geometry.unary_union.centroid

    return (
        {
            "village_name": to_utf8(plan.village_name),
            "gram_panchayat": to_utf8(plan.gram_panchayat),
            "tehsil": to_utf8(plan.tehsil_soi.tehsil_name),
            "district": to_utf8(plan.district_soi.district_name),
            "state": to_utf8(plan.state_soi.state_name),
            "total_settlements": total_settlements,
            "settlement_mws_pairs": settlement_mws_ids,
            "village_coordinates": (
                f"{centroid.y:.8f}, {centroid.x:.8f}" if centroid else "Not available"
            ),
        },
        settlement_mws_ids,
        mws_gdf,
    )


def get_section_c_data(plan):
    settlement_data = get_data_for_settlement(plan.id)

    return {
        "socio_eco": settlement_data,
        "mgnrega": settlement_data,
        "crop_info": get_crops_data(plan.id),
        "livestock_info": get_livestock_data(plan.id),
    }


def get_section_d_data(plan, settlement_mws_ids, mws_gdf):

    unique_mws_ids = sorted(set([mws_id for _, mws_id in settlement_mws_ids]))

    all_wells_with_mws = get_all_wells_with_mws(
        plan,
        unique_mws_ids,
        mws_gdf,
    )

    all_waterbodies_with_mws = get_all_waterbodies_with_mws(
        plan,
        unique_mws_ids,
        mws_gdf,
    )

    return {
        "mws": get_mws_table_data(unique_mws_ids, mws_gdf),
        "well_summary": get_well_summary_data(all_wells_with_mws),
        "wells": get_detailed_well_data(all_wells_with_mws),
        "water_summary": get_waterbody_summary_data(all_waterbodies_with_mws),
        "water_structures": get_detailed_waterbody_data(all_waterbodies_with_mws),
    }


def get_mws_table_data(unique_mws_ids, mws_gdf):

    data = []

    for mws_id in unique_mws_ids:

        matching_feature = mws_gdf[mws_gdf["uid"] == mws_id]

        centroid = "NA"

        if not matching_feature.empty:
            c = matching_feature.geometry.centroid.iloc[0]
            centroid = f"{c.y:.8f}, {c.x:.8f}"

        data.append(
            {
                "mws_id": mws_id,
                "centroid": centroid,
            }
        )

    return data


def get_well_summary_data(all_wells_with_mws):

    wells_count = defaultdict(int)
    households_count = defaultdict(int)

    for well, _ in all_wells_with_mws:

        wells_count[well.beneficiary_settlement] += 1

        households_count[well.beneficiary_settlement] += int(
            well.households_benefitted or 0
        )

    rows = []

    for settlement in sorted(wells_count.keys(), key=sort_key):

        rows.append(
            {
                "settlement": settlement,
                "num_wells": wells_count[settlement],
                "households": households_count[settlement],
            }
        )

    return rows


def get_detailed_well_data(all_wells_with_mws):

    rows = []

    all_wells_with_mws_sorted = sorted(
        all_wells_with_mws,
        key=lambda x: (
            not x[0].beneficiary_settlement or x[0].beneficiary_settlement == "NA",
            (x[0].beneficiary_settlement or "").lower(),
        ),
    )

    for well, mws_id in all_wells_with_mws_sorted:

        well_usage = "NA"

        if well.data_well and "Well_usage" in well.data_well:

            usage = well.data_well["Well_usage"]

            used = ensure_str(usage.get("select_one_well_used"))

            other = usage.get("select_one_well_used_other")

            if used and used.lower() == "other" and other:
                well_usage = f"Other: {other}"

            elif used:
                well_usage = used

        repair_activities = "NA"

        if well.data_well and "Well_usage" in well.data_well:

            usage = well.data_well["Well_usage"]

            repairs = ensure_str(usage.get("repairs_type"))

            repairs_other = usage.get("repairs_type_other")

            if repairs and repairs.lower() == "other" and repairs_other:
                repair_activities = f"Other: {repairs_other}"

            elif repairs:
                repair_activities = repairs.replace(
                    "_",
                    " ",
                )

        rows.append(
            {
                "mws_id": mws_id,
                "settlement": well.beneficiary_settlement,
                "well_type": well.data_well.get("select_one_well_type") or "NA",
                "owner": well.owner,
                "beneficiary_name": well.data_well.get("Beneficiary_name") or "NA",
                "father_name": well.data_well.get("ben_father") or "NA",
                "water_availability": well.data_well.get("select_one_year") or "NA",
                "households_benefitted": well.households_benefitted,
                "caste_uses": well.caste_uses,
                "well_usage": well_usage,
                "need_maintenance": well.need_maintenance,
                "repair_activities": repair_activities,
                "latitude": well.latitude,
                "longitude": well.longitude,
            }
        )

    return rows


def get_waterbody_summary_data(
    all_waterbodies_with_mws,
):

    waterbody_count = defaultdict(int)

    households_count = defaultdict(int)

    for waterbody, _ in all_waterbodies_with_mws:

        structure_type = waterbody.water_structure_type

        key = (
            waterbody.beneficiary_settlement,
            structure_type,
        )

        waterbody_count[key] += 1

        households_count[key] += int(waterbody.household_benefitted or 0)

    rows = []

    for (
        settlement,
        structure_type,
    ) in sorted(
        waterbody_count.keys(),
        key=lambda x: sort_key(x[0]),
    ):

        rows.append(
            {
                "settlement": settlement,
                "structure_type": structure_type,
                "count": waterbody_count[(settlement, structure_type)],
                "households": households_count[(settlement, structure_type)],
            }
        )

    return rows


def get_detailed_waterbody_data(
    all_waterbodies_with_mws,
):

    rows = []

    for (
        waterbody,
        mws_id,
    ) in sorted(
        all_waterbodies_with_mws,
        key=lambda x: sort_key(x[0].beneficiary_settlement),
    ):

        who_manages = waterbody.who_manages or "NA"

        if who_manages.lower() == "other":
            who_manages = "Other: " + (waterbody.specify_other_manager or "")

        structure_type = waterbody.water_structure_type or "NA"

        if structure_type.lower() == "other":
            structure_type = "Other: " + (waterbody.water_structure_other or "")

        repair_activities = get_waterbody_repair_activities(
            waterbody.data_waterbody,
            structure_type,
        )

        rows.append(
            {
                "mws_id": mws_id,
                "settlement": waterbody.beneficiary_settlement,
                "owner": waterbody.owner,
                "beneficiary_name": waterbody.data_waterbody.get("Beneficiary_name")
                or "NA",
                "father_name": waterbody.data_waterbody.get("ben_father") or "NA",
                "who_manages": who_manages,
                "caste_uses": waterbody.caste_who_uses,
                "households_benefitted": waterbody.household_benefitted,
                "structure_type": structure_type,
                "usage": format_text(
                    waterbody.data_waterbody.get("select_multiple_uses_structure")
                ),
                "need_maintenance": waterbody.need_maintenance,
                "repair_activities": repair_activities,
                "latitude": waterbody.latitude,
                "longitude": waterbody.longitude,
            }
        )

    return rows


def get_section_e_data(plan):
    populate_maintenance_from_waterbody(plan)

    asset_types = [
        "Water Recharge Structures",
        "Irrigation Structures",
        "Surface Water Structures",
        "Remote Sensed Surface Water Structures",
    ]

    return {
        "asset_types": asset_types,
        "gw": get_maintenance_data(plan.id, "gw"),
        "agri": get_maintenance_data(plan.id, "agri"),
        "swb": get_maintenance_data(plan.id, "swb"),
        "swb_rs": get_maintenance_data(plan.id, "swb_rs"),
    }


def get_section_f_data(plan):
    return {"works": get_nrm_works_data(plan.id)}


def get_section_g_data(plan):
    all_livelihood = get_livelihood_data(plan.id)

    livestock_fisheries = [
        r for r in all_livelihood if r["livelihood_work"] in ("Livestock", "Fisheries")
    ]

    plantations_etc = [
        r
        for r in all_livelihood
        if r["livelihood_work"] not in ("Livestock", "Fisheries")
    ]

    return {
        "livestock_fisheries": livestock_fisheries,
        "plantations": plantations_etc,
    }
