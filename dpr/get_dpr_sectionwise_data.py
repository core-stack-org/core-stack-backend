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
from dpr.utils import ensure_str, get_waterbody_repair_activities
from .mapping import populate_maintenance_from_waterbody
from .services import (
    get_nrm_works_data,
)
from .service.form_download_service import (
    load_form_labels,
    translate_choice,
    translate_multiple_choices,
)
from dpr.models import (
    GW_maintenance,
    ODK_livelihood,
    ODK_agrohorticulture,
    Agri_maintenance,
    SWB_maintenance,
    SWB_RS_maintenance,
    ODK_agri,
    ODK_groundwater,
)


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


def get_section_c_data(plan, language):
    settlement_data = get_data_for_settlement(plan.id)
    labels = load_form_labels("Add_Settlements_form _V1.0.1")
    crop_data = get_crops_data(plan.id)
    crop_labels = load_form_labels("crop_form_V1.0.0")
    for settlement in settlement_data:

        settlement.largest_caste_label = translate_choice(
            labels,
            "select_one_type",
            settlement.largest_caste,
            language,
        )

        if settlement.largest_caste.lower() == "single caste group":

            settlement.smallest_caste_label = translate_choice(
                labels,
                "caste_group_single",
                settlement.smallest_caste,
                language,
            )

        elif settlement.largest_caste.lower() == "mixed caste group":

            settlement.settlement_status_label = translate_multiple_choices(
                labels,
                "caste_group_single",
                settlement.settlement_status,
                language,
            )
        settlement.nrega_past_work_label = translate_multiple_choices(
            labels,
            "work_demands",
            clean_odk_value(settlement.nrega_past_work),
            language,
        )

        settlement.nrega_demand_label = translate_choice(
            labels,
            "select_one_demands",
            clean_odk_value(settlement.nrega_demand),
            language,
        )

        settlement.nrega_issues_label = translate_multiple_choices(
            labels,
            "select_multiple_issues",
            settlement.nrega_issues,
            language,
        )
    for crop in crop_data:
        crop["irrigation_source"] = translate_multiple_choices(
            crop_labels,
            "select_multiple_widgets",
            clean_odk_value(crop["irrigation_source"]),
            language,
        )

        crop["land_classification"] = translate_choice(
            crop_labels,
            "select_one_classified",
            clean_odk_value(crop["land_classification"]),
            language,
        )

        crop["kharif_crops"] = translate_multiple_choices(
            crop_labels,
            "select_multiple_cropping_kharif",
            clean_odk_value(crop["kharif_crops"]),
            language,
        )

        crop["rabi_crops"] = translate_multiple_choices(
            crop_labels,
            "select_multiple_cropping_Rabi",
            clean_odk_value(crop["rabi_crops"]),
            language,
        )

        crop["zaid_crops"] = translate_multiple_choices(
            crop_labels,
            "select_multiple_cropping_Zaid",
            clean_odk_value(crop["zaid_crops"]),
            language,
        )

        crop["cropping_intensity"] = translate_choice(
            crop_labels,
            "select_one_productivity",
            clean_odk_value(crop["cropping_intensity"]),
            language,
        )
    return {
        "socio_eco": settlement_data,
        "mgnrega": settlement_data,
        "crop_info": crop_data,
        "livestock_info": get_livestock_data(plan.id),
    }


def get_section_d_data(plan, settlement_mws_ids, mws_gdf, language):
    unique_mws_ids = sorted({item["mws_id"] for item in settlement_mws_ids})

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

    well_labels = load_form_labels("Add_well_form_V1.0.1")
    water_body_labels_repair = load_form_labels(
        "NRM_form_NRM_form_Waterbody_Screen_V1.0.0"
    )
    water_body_labels = load_form_labels("Add_Waterbodies_Form_V1.0.3")
    return {
        "mws": get_mws_table_data(unique_mws_ids, mws_gdf),
        "well_summary": get_well_summary_data(all_wells_with_mws),
        "wells": get_detailed_well_data(all_wells_with_mws, well_labels, language),
        "water_summary": get_waterbody_summary_data(
            all_waterbodies_with_mws, water_body_labels, language
        ),
        "water_structures": get_detailed_waterbody_data(
            all_waterbodies_with_mws,
            water_body_labels,
            language,
            water_body_labels_repair,
        ),
    }


def get_mws_table_data(unique_mws_ids, mws_gdf):

    data = []

    for mws_id in unique_mws_ids:

        matching_feature = mws_gdf[mws_gdf["uid"] == mws_id]

        centroid = None

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


def get_detailed_well_data(all_wells_with_mws, labels, language):

    rows = []

    all_wells_with_mws_sorted = sorted(
        all_wells_with_mws,
        key=lambda x: (
            not x[0].beneficiary_settlement or x[0].beneficiary_settlement == "NA",
            (x[0].beneficiary_settlement or "").lower(),
        ),
    )

    for well, mws_id in all_wells_with_mws_sorted:

        well_usage = None

        if well.data_well and "Well_usage" in well.data_well:

            usage = well.data_well["Well_usage"]

            used = ensure_str(usage.get("select_one_well_used"))

            other = usage.get("select_one_well_used_other")

            if used and used.lower() == "other" and other:
                well_usage = f"Other: {other}"

            elif used:
                well_usage = translate_choice(
                    labels,
                    "select_one_well_used",
                    used,
                    language,
                )

        repair_activities = None

        if well.data_well and "Well_usage" in well.data_well:

            usage = well.data_well["Well_usage"]

            repairs = ensure_str(usage.get("repairs_type"))
            repairs_other = usage.get("repairs_type_other")

            if repairs and repairs.lower() == "other" and repairs_other:
                repair_activities = repairs_other

            elif repairs:
                repair_activities = translate_multiple_choices(
                    labels,
                    "repairs_type",
                    repairs,
                    language,
                )
        rows.append(
            {
                "mws_id": mws_id,
                "settlement": well.beneficiary_settlement,
                "well_type": translate_choice(
                    labels,
                    "select_one_well_type",
                    well.data_well.get("select_one_well_type"),
                    language,
                ),
                "owner": translate_choice(
                    labels,
                    "select_one_owns",
                    well.owner,
                    language,
                ),
                "beneficiary_name": well.data_well.get("Beneficiary_name") or None,
                "father_name": well.data_well.get("ben_father") or None,
                "water_availability": translate_choice(
                    labels,
                    "select_one_year",
                    well.data_well.get("select_one_year"),
                    language,
                ),
                "households_benefitted": well.households_benefitted,
                "caste_uses": translate_multiple_choices(
                    labels,
                    "select_multiple_caste_use",
                    well.caste_uses,
                    language,
                ),
                "well_usage": well_usage,
                "need_maintenance": translate_choice(
                    labels,
                    "is_maintenance_required",
                    well.need_maintenance,
                    language,
                ),
                "repair_activities": repair_activities,
                "latitude": well.latitude,
                "longitude": well.longitude,
            }
        )

    return rows


def get_waterbody_summary_data(all_waterbodies_with_mws, water_body_labels, language):

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
                "structure_type": translate_choice(
                    water_body_labels,
                    "select_one_water_structure",
                    structure_type,
                    language,
                ),
                "count": waterbody_count[(settlement, structure_type)],
                "households": households_count[(settlement, structure_type)],
            }
        )

    return rows


def get_detailed_waterbody_data(
    all_waterbodies_with_mws, waterbody_labels, language, water_body_labels_repair
):

    rows = []

    for (
        waterbody,
        mws_id,
    ) in sorted(
        all_waterbodies_with_mws,
        key=lambda x: sort_key(x[0].beneficiary_settlement),
    ):

        who_manages = waterbody.who_manages or None

        if who_manages:

            if who_manages.lower() == "other":
                who_manages = "Other: " + (waterbody.specify_other_manager or "")

            else:
                who_manages = translate_choice(
                    waterbody_labels,
                    "select_one_manages",
                    who_manages.lower(),
                    language,
                )

        structure_type = waterbody.water_structure_type or None
        structure_type_eng = structure_type

        if structure_type:

            if structure_type.lower() == "other":
                structure_type = "Other: " + (waterbody.water_structure_other or "")
                structure_type_eng = structure_type

            else:
                structure_type = translate_choice(
                    waterbody_labels,
                    "select_one_water_structure",
                    structure_type,
                    language,
                )
        repair_activities = get_waterbody_repair_activities(
            waterbody.data_waterbody,
            structure_type_eng,
        )
        repair_label_key = REPAIR_LABEL_MAPPING.get(
            str(waterbody.water_structure_type).strip().lower()
        )
        wb_owner = waterbody.owner
        wb_owner = classify_demand_type(wb_owner.lower())
        rows.append(
            {
                "mws_id": mws_id,
                "settlement": waterbody.beneficiary_settlement,
                "owner": translate_choice(
                    water_body_labels_repair,
                    "demand_type",
                    wb_owner,
                    language,
                ),
                "beneficiary_name": waterbody.data_waterbody.get("Beneficiary_name")
                or None,
                "father_name": waterbody.data_waterbody.get("ben_father") or None,
                "who_manages": who_manages,
                "caste_uses": translate_multiple_choices(
                    waterbody_labels,
                    "select_multiple_caste_use",
                    waterbody.caste_who_uses,
                    language,
                ),
                "households_benefitted": waterbody.household_benefitted,
                "structure_type": structure_type,
                "usage": translate_multiple_choices(
                    waterbody_labels,
                    "select_multiple_uses_structure",
                    waterbody.data_waterbody.get("select_multiple_uses_structure"),
                    language,
                ),
                "need_maintenance": translate_choice(
                    waterbody_labels,
                    "select_one_maintenance",
                    waterbody.need_maintenance,
                    language,
                ),
                "repair_activities": translate_multiple_choices(
                    water_body_labels_repair,
                    repair_label_key,
                    repair_activities,
                    language,
                ),
                "latitude": waterbody.latitude,
                "longitude": waterbody.longitude,
            }
        )

    return rows


def get_section_e_data(plan, language):
    populate_maintenance_from_waterbody(plan)
    gw_data = get_maintenance_data(plan.id, "gw")
    agri_data = get_maintenance_data(plan.id, "agri")
    swb_data = get_maintenance_data(plan.id, "swb")
    swb_rs_data = get_maintenance_data(plan.id, "swb_rs")
    gw_label = load_form_labels(
        "Propose_Maintenance_on_Existing_Water_Recharge_Structures_V1.1.1"
    )
    agri_label = load_form_labels(
        "Propose_Maintenance_on_Existing_Irrigation_Structures_V1.1.1"
    )
    swb_rs_label = load_form_labels("PM_Remote_Sensed_Surface_Water_structure_V1.0.0")
    swb_label = load_form_labels("NRM_form_NRM_form_Waterbody_Screen_V1.0.0")

    for row in gw_data:

        row["demand_type"] = translate_choice(
            gw_label,
            "demand_type",
            row["demand_type"],
            language,
        )

        row["structure_type"] = translate_choice(
            gw_label,
            "select_one_recharge_structure",
            row["structure_type"],
            language,
        )
    for row in agri_data:
        demand_type = classify_demand_type(row["demand_type"].lower())
        row["demand_type"] = translate_choice(
            agri_label,
            "demand_type".lower().replace(" ", "_"),
            demand_type,
            language,
        )
        row["structure_type"] = translate_choice(
            agri_label,
            "select_one_irrigation_structure",
            row["structure_type"],
            language,
        )
    for row in swb_data:
        row["demand_type"] = translate_choice(
            swb_label,
            "demand_type",
            row["demand_type"],
            language,
        )
        row["structure_type"] = translate_choice(
            swb_rs_label,
            "TYPE_OF_WORK",
            row["structure_type"],
            language,
        )
    for row in swb_rs_data:
        row["demand_type"] = translate_choice(
            swb_rs_label,
            "demand_type",
            row["demand_type"],
            language,
        )

        original_structure = row["structure_type"]
        original_structure = str(original_structure).strip().lower()
        repair_key = RS_WATER_STRUCTIRE_REVERSE_MAPPING.get(original_structure)
        if repair_key and row.get("repair_activities"):
            row["repair_activities"] = translate_choice(
                swb_rs_label,
                repair_key,
                clean_odk_value(row["repair_activities"]),
                language,
            )
        row["structure_type"] = translate_choice(
            swb_rs_label,
            "TYPE_OF_WORK",
            row["structure_type"],
            language,
        )
    return {
        "gw": gw_data,
        "agri": agri_data,
        "swb": swb_data,
        "swb_rs": swb_rs_data,
    }


def get_section_f_data(plan, language):
    gw_labels = load_form_labels("NRM_form_propose_new_recharge_structure_V1.0.0")
    agri_labels = load_form_labels("NRM_form_Agri_Screen_V1.0.0")
    works = get_nrm_works_data(plan.id)
    for row in works:
        if row["work_category"] == "Recharge Structure":
            row["demand_type"] = translate_choice(
                gw_labels,
                "demand_type",
                row["demand_type"],
                language,
            )
            row["work_demand"] = translate_choice(
                gw_labels,
                "TYPE_OF_WORK_ID",
                row["work_demand"],
                language,
            )
            row["gender"] = translate_choice(
                gw_labels,
                "select_gender",
                row["gender"],
                language,
            )
            row["work_category"] = translate_work_category(
                row["work_category"],
                language,
            )
        elif row["work_category"] == "Irrigation Work":
            row["demand_type"] = translate_choice(
                agri_labels,
                "demand_type_irrigation",
                row["demand_type"],
                language,
            )
            row["work_demand"] = translate_choice(
                agri_labels,
                "TYPE_OF_WORK_ID",
                row["work_demand"],
                language,
            )
            row["gender"] = translate_choice(
                agri_labels,
                "gender",
                row["gender"],
                language,
            )
            row["work_category"] = translate_work_category(
                row["work_category"],
                language,
            )
    return {"works": works}


def get_section_g_data(plan, language):
    all_livelihood = get_livelihood_data(plan.id)
    agro_labels = load_form_labels("Agrohorticulture")
    livelihood_labels = load_form_labels("NRM Livelihood Form")

    livestock_fisheries = [
        r for r in all_livelihood if r["livelihood_work"] in ("Livestock", "Fisheries")
    ]
    for row in livestock_fisheries:
        livelihood_work = row.get("livelihood_work")

        if livelihood_work == "Livestock":
            row["demand_type"] = translate_choice(
                livelihood_labels,
                "livestock_demand",
                row.get("demand_type"),
                language,
            )

            row["work_demand"] = translate_choice(
                livelihood_labels,
                "demands_promoting_livestock",
                row.get("work_demand"),
                language,
            )
            row["gender"] = translate_choice(
                livelihood_labels,
                "gender_livestock",
                row.get("gender"),
                language,
            )

        elif livelihood_work == "Fisheries":
            row["demand_type"] = translate_choice(
                livelihood_labels,
                "demand_type_fisheries",
                row.get("demand_type"),
                language,
            )

            row["work_demand"] = translate_choice(
                livelihood_labels,
                "select_one_promoting_fisheries",
                row.get("work_demand"),
                language,
            )
            row["gender"] = translate_choice(
                livelihood_labels,
                "gender_fisheries",
                row.get("gender"),
                language,
            )

    plantations_etc = [
        r
        for r in all_livelihood
        if r["livelihood_work"] not in ("Livestock", "Fisheries")
    ]
    for row in plantations_etc:
        livelihood_work = row.get("livelihood_work")

        if livelihood_work == "Plantations":

            row["demand_type"] = translate_choice(
                agro_labels,
                "demand_type_plantations",
                row.get("demand_type"),
                language,
            )
            row["gender"] = translate_choice(
                agro_labels,
                "gender",
                row.get("gender"),
                language,
            )

            species = row.get("work_demand")

            if species:
                translated_species = []

                for item in species.split():
                    translated_species.append(
                        translate_choice(
                            agro_labels,
                            "select_multiple_species",
                            item.strip().lower(),
                            language,
                        )
                    )

                row["work_demand"] = ", ".join(translated_species)

        elif livelihood_work == "Kitchen Garden":

            row["demand_type"] = translate_choice(
                livelihood_labels,
                "demand_type_kitchen_garden",
                row.get("demand_type"),
                language,
            )
            row["gender"] = translate_choice(
                livelihood_labels,
                "gender_kitchen_gardens",
                row.get("gender"),
                language,
            )
    for row in livestock_fisheries:
        row["livelihood_work"] = translate_livelihood_work(
            row["livelihood_work"],
            language,
        )

    for row in plantations_etc:
        row["livelihood_work"] = translate_livelihood_work(
            row["livelihood_work"],
            language,
        )
    return {
        "livestock_fisheries": livestock_fisheries,
        "plantations": plantations_etc,
    }


def clean_odk_value(value):
    if value is None:
        return None

    value = str(value).strip()

    if value.upper() == "NA":
        return None

    return value


REPAIR_LABEL_MAPPING = {
    "community pond": "select_one_community_pond",
    "large water body": "select_one_repair_large_water_body",
    "farm pond": "select_one_farm_pond",
    "canal": "select_one_repair_canal",
    "check dam": "select_one_check_dam",
    "percolation tank": "select_one_percolation_tank",
    "rock fill dam": "select_one_rock_fill_dam",
    "loose boulder structure": "select_one_loose_boulder_structure",
    "5% model structure": "select_one_model5_structure",
    "30-40 model structure": "select_one_model30_40_structure",
}
REPAIR_ACTIVITY_MAPPING = {
    "check dam": "select_one_check_dam",
    "percolation tank": "select_one_percolation_tank",
    "earthen gully plug": "select_one_earthen_gully_plug",
    "drainage/soakage channels": "select_one_drainage_soakage_channels",
    "recharge pits": "select_one_recharge_pits",
    "sokage pits": "select_one_sokage_pits",
    "trench cum bund network": "select_one_trench_cum_bund_network",
    "continuous contour trenches (cct)": "select_one_continuous_contour_trenches",
    "staggered contour trenches(sct)": "select_one_staggered_contour_trenches",
    "water absorption trenches(wat)": "select_one_water_absorption_trenches",
    "loose boulder structure": "select_one_loose_boulder_structure",
    "rock fill dam": "select_one_rock_fill_dam",
    "stone bunding": "select_one_stone_bunding",
    "diversion drains": "select_one_diversion_drains",
    "bunding:contour bunds/ graded bunds": "select_one_bunding",
    "5% model structure": "select_one_model5_structure",
    "30-40 model structure": "select_one_model30_40_structure",
}


def get_maintenance_data(plan_id, maintenance_type):
    pid = str(plan_id)
    result = []

    if maintenance_type == "gw":
        for m in GW_maintenance.objects.filter(plan_id=pid).exclude(is_deleted=True):
            d = m.data_gw_maintenance or {}
            structure_type = d.get("select_one_recharge_structure") or None
            repair = _resolve_repair_activity(
                d,
                structure_type,
                RECHARGE_STRUCTURE_MAPPING,
            )
            result.append(
                {
                    "id": m.gw_maintenance_id,
                    "demand_type": d.get("demand_type"),
                    "beneficiary_settlement": d.get("beneficiary_settlement"),
                    "beneficiary_name": d.get("Beneficiary_Name"),
                    "gender": d.get("select_gender"),
                    "beneficiary_father_name": d.get("ben_father"),
                    "structure_type": structure_type,
                    "repair_activities": repair,
                    "latitude": m.latitude,
                    "longitude": m.longitude,
                }
            )

    elif maintenance_type == "agri":
        for m in Agri_maintenance.objects.filter(plan_id=pid).exclude(is_deleted=True):
            d = m.data_agri_maintenance or {}
            structure_type = (
                d.get("select_one_water_structure")
                or d.get("select_one_irrigation_structure")
                or "NA"
            )
            repair = _resolve_repair_activity(
                d, structure_type, IRRIGATION_STRUCTURE_REVERSE_MAPPING
            )
            result.append(
                {
                    "id": m.agri_maintenance_id,
                    "demand_type": d.get("demand_type"),
                    "beneficiary_settlement": d.get("beneficiary_settlement"),
                    "beneficiary_name": d.get("Beneficiary_Name"),
                    "beneficiary_father_name": d.get("ben_father"),
                    "structure_type": structure_type,
                    "repair_activities": repair,
                    "latitude": m.latitude,
                    "longitude": m.longitude,
                }
            )

    elif maintenance_type == "swb":
        for m in SWB_maintenance.objects.filter(plan_id=pid).exclude(is_deleted=True):
            d = m.data_swb_maintenance or {}
            structure_type = (
                d.get("TYPE_OF_WORK") or d.get("select_one_water_structure") or "NA"
            )
            repair = _resolve_repair_activity(
                d, structure_type, WATER_STRUCTURE_REVERSE_MAPPING
            )
            result.append(
                {
                    "id": m.swb_maintenance_id,
                    "demand_type": d.get("demand_type"),
                    "beneficiary_settlement": d.get("beneficiary_settlement"),
                    "beneficiary_name": d.get("Beneficiary_Name"),
                    "gender": d.get("select_gender"),
                    "beneficiary_father_name": d.get("ben_father"),
                    "structure_type": structure_type,
                    "repair_activities": repair,
                    "latitude": m.latitude,
                    "longitude": m.longitude,
                }
            )

    elif maintenance_type == "swb_rs":
        for m in SWB_RS_maintenance.objects.filter(plan_id=pid).exclude(
            is_deleted=True
        ):
            d = m.data_swb_rs_maintenance or {}
            structure_type = d.get("TYPE_OF_WORK") or "NA"
            repair = _resolve_repair_activity(
                d, structure_type, RS_WATER_STRUCTIRE_REVERSE_MAPPING
            )

            result.append(
                {
                    "id": m.swb_rs_maintenance_id,
                    "demand_type": d.get("demand_type"),
                    "beneficiary_settlement": d.get("beneficiary_settlement"),
                    "beneficiary_name": d.get("Beneficiary_Name"),
                    "gender": d.get("select_gender"),
                    "beneficiary_father_name": d.get("ben_father"),
                    "structure_type": structure_type,
                    "repair_activities": repair,
                    "latitude": m.latitude,
                    "longitude": m.longitude,
                }
            )

    return result


def _resolve_repair_activity(
    data, structure_type, mapping, fallback_key="select_one_activities"
):
    repair_activities = None

    structure_key = str(structure_type).strip().lower()

    repair_key = mapping.get(structure_key)

    if repair_key:
        repair_activities = data.get(repair_key)

        if repair_activities == "other":
            repair_activities = data.get(f"{repair_key}_other")

    if not repair_activities:
        repair_activities = data.get(fallback_key)

    return repair_activities


RECHARGE_STRUCTURE_MAPPING = {
    "Check dam": "select_one_check_dam",
    "Percolation tank": "select_one_percolation_tank",
    "Earthen gully plug": "select_one_earthen_gully_plug",
    "Drainage/soakage channels": "select_one_drainage_soakage_channels",
    "Recharge pits": "select_one_recharge_pits",
    "Sokage pits": "select_one_sokage_pits",
    "Trench cum bund network": "select_one_trench_cum_bund_network",
    "Continuous contour trenches (CCT)": "select_one_continuous_contour_trenches",
    "Staggered Contour trenches(SCT)": "select_one_staggered_contour_trenches",
    "Water absorption trenches(WAT)": "select_one_water_absorption_trenches",
    "Loose boulder structure": "select_one_loose_boulder_structure",
    "Rock fill dam": "select_one_rock_fill_dam",
    "Stone bunding": "select_one_stone_bunding",
    "Diversion drains": "select_one_diversion_drains",
    "Bunding:Contour bunds/ graded bunds": "select_one_bunding",
    "5% model structure": "select_one_model5_structure",
    "30-40 model structure": "select_one_model30_40_structure",
}


def get_livelihood_data(plan_id):
    pid = str(plan_id)
    result = []

    for record in (
        ODK_livelihood.objects.filter(plan_id=pid)
        .exclude(status_re="rejected")
        .exclude(is_deleted=True)
    ):
        dl = record.data_livelihood or {}

        livestock_group = dl.get("Livestock") or {}
        fisheries_group = dl.get("fisheries") or {}
        plantation_group = dl.get("plantations") or {}
        kitchen_garden_group = dl.get("kitchen_gardens") or {}

        is_livestock = (
            ensure_str(livestock_group.get("is_demand_livestock", "")).lower() == "yes"
            or ensure_str(dl.get("select_one_demand_promoting_livestock", "")).lower()
            == "yes"
        )
        if is_livestock:
            demands = ensure_str(livestock_group.get("demands_promoting_livestock"))
            if demands and demands.lower() == "other":
                demands = livestock_group.get("demands_promoting_livestock_other")
            if not demands:
                demands = ensure_str(dl.get("select_one_promoting_livestock"))
                if demands and demands.lower() == "other":
                    demands = dl.get("select_one_promoting_livestock_other")
            result.append(
                {
                    "livelihood_work": "Livestock",
                    "demand_type": livestock_group.get("livestock_demand"),
                    "work_demand": demands,
                    "beneficiary_settlement": record.beneficiary_settlement,
                    "beneficiary_name": dl.get("beneficiary_name")
                    or livestock_group.get("ben_livestock"),
                    "gender": livestock_group.get("gender_livestock"),
                    "beneficiary_father_name": livestock_group.get(
                        "ben_father_livestock"
                    ),
                    "latitude": record.latitude,
                    "longitude": record.longitude,
                }
            )

        is_fisheries = (
            ensure_str(fisheries_group.get("is_demand_fisheris", "")).lower() == "yes"
            or ensure_str(dl.get("select_one_demand_promoting_fisheries", "")).lower()
            == "yes"
        )
        if is_fisheries:
            demands = ensure_str(fisheries_group.get("select_one_promoting_fisheries"))
            if demands and demands.lower() == "other":
                demands = fisheries_group.get("select_one_promoting_fisheries_other")
            if not demands:
                demands = ensure_str(dl.get("select_one_promoting_fisheries"))
                if demands and demands.lower() == "other":
                    demands = dl.get("select_one_promoting_fisheries_other")
            result.append(
                {
                    "livelihood_work": "Fisheries",
                    "demand_type": fisheries_group.get("demand_type_fisheries"),
                    "work_demand": demands,
                    "beneficiary_settlement": record.beneficiary_settlement,
                    "beneficiary_name": dl.get("beneficiary_name")
                    or fisheries_group.get("ben_fisheries"),
                    "gender": fisheries_group.get("gender_fisheries"),
                    "beneficiary_father_name": fisheries_group.get(
                        "ben_father_fisheries"
                    ),
                    "latitude": record.latitude,
                    "longitude": record.longitude,
                }
            )

        is_plantation = (
            ensure_str(dl.get("select_one_demand_plantation", "")).lower() == "yes"
            or ensure_str(plantation_group.get("select_plantation_demands", "")).lower()
            == "yes"
        )
        if is_plantation:
            result.append(
                {
                    "livelihood_work": "Plantations",
                    "demand_type": plantation_group.get("demand_type_plantations"),
                    "work_demand": dl.get("Plantation")
                    or plantation_group.get("crop_name"),
                    "beneficiary_settlement": record.beneficiary_settlement,
                    "beneficiary_name": dl.get("beneficiary_name")
                    or plantation_group.get("ben_plantation"),
                    "gender": plantation_group.get("gender"),
                    "beneficiary_father_name": plantation_group.get("ben_father"),
                    "total_acres": dl.get("Plantation_crop")
                    or plantation_group.get("crop_area"),
                    "latitude": record.latitude,
                    "longitude": record.longitude,
                }
            )

        is_kitchen_garden = (
            ensure_str(dl.get("indi_assets", "")).lower() == "yes"
            or ensure_str(kitchen_garden_group.get("assets_kg", "")).lower() == "yes"
        )
        if is_kitchen_garden:
            result.append(
                {
                    "livelihood_work": "Kitchen Garden",
                    "demand_type": kitchen_garden_group.get(
                        "demand_type_kitchen_garden"
                    ),
                    "work_demand": dl.get("Plantation"),
                    "beneficiary_settlement": record.beneficiary_settlement,
                    "beneficiary_name": dl.get("beneficiary_name")
                    or kitchen_garden_group.get("ben_kitchen_gardens"),
                    "gender": kitchen_garden_group.get("gender_kitchen_gardens"),
                    "beneficiary_father_name": kitchen_garden_group.get(
                        "ben_father_kitchen_gardens"
                    ),
                    "total_acres": dl.get("area_didi_badi")
                    or kitchen_garden_group.get("area_kg"),
                    "latitude": record.latitude,
                    "longitude": record.longitude,
                }
            )

    for agrohorti in (
        ODK_agrohorticulture.objects.filter(plan_id=pid)
        .exclude(status_re="rejected")
        .exclude(is_deleted=True)
    ):
        data = agrohorti.data_agohorticulture or {}
        species_parts = filter(
            None,
            [
                data.get("select_multiple_species"),
                data.get("select_multiple_species_other"),
            ],
        )
        species = " ".join(species_parts) or None
        result.append(
            {
                "livelihood_work": "Plantations",
                "demand_type": data.get("demand_type_plantations"),
                "work_demand": species,
                "beneficiary_settlement": data.get("beneficiary_settlement"),
                "beneficiary_name": data.get("beneficiary_name"),
                "gender": data.get("gender"),
                "beneficiary_father_name": data.get("ben_father"),
                "total_acres": data.get("crop_area"),
                "latitude": agrohorti.latitude,
                "longitude": agrohorti.longitude,
            }
        )

    return result


def translate_livelihood_work(value, language):
    translations = {
        "hi": {
            "Livestock": "पशुपालन",
            "Fisheries": "मत्स्य पालन",
            "Plantations": "पौधारोपण",
            "Kitchen Garden": "रसोई बाड़ी",
        },
        "od": {
            "Livestock": "ପଶୁପାଳନ",
            "Fisheries": "ମତ୍ସ୍ୟଚାଷ",
            "Plantations": "ବୃକ୍ଷରୋପଣ",
            "Kitchen Garden": "ରୋଷେଇ ବଗିଚା",
        },
    }

    return translations.get(language, {}).get(value, value)


def get_nrm_works_data(
    plan_id,
):
    pid = str(plan_id)
    result = []

    for structure in (
        ODK_groundwater.objects.filter(plan_id=pid)
        .exclude(status_re="rejected")
        .exclude(is_deleted=True)
    ):
        dg = structure.data_groundwater or {}
        result.append(
            {
                "work_category": "Recharge Structure",
                "demand_type": dg.get("demand_type"),
                "work_demand": structure.work_type,
                "beneficiary_settlement": structure.beneficiary_settlement,
                "beneficiary_name": dg.get("Beneficiary_Name"),
                "gender": dg.get("select_gender"),
                "beneficiary_father_name": dg.get("ben_father"),
                "latitude": structure.latitude,
                "longitude": structure.longitude,
            }
        )

    for irr in (
        ODK_agri.objects.filter(plan_id=pid)
        .exclude(status_re="rejected")
        .exclude(is_deleted=True)
    ):
        da = irr.data_agri or {}
        work_demand = irr.work_type
        if (irr.work_type or "").lower() == "other":
            work_demand = da.get("TYPE_OF_WORK_ID_other") or "Other (unspecified)"
        result.append(
            {
                "work_category": "Irrigation Work",
                "demand_type": da.get("demand_type_irrigation"),
                "work_demand": work_demand,
                "beneficiary_settlement": irr.beneficiary_settlement,
                "beneficiary_name": da.get("Beneficiary_Name"),
                "gender": da.get("gender"),
                "beneficiary_father_name": da.get("ben_father"),
                "latitude": irr.latitude,
                "longitude": irr.longitude,
            }
        )

    return result


def translate_work_category(value, language):

    translations = {
        "hi": {
            "Recharge Structure": "पुनर्भरण संरचना",
            "Irrigation Work": "सिंचाई कार्य",
        },
        "od": {
            "Recharge Structure": "ପୁନର୍ଭରଣ ସଂରଚନା",
            "Irrigation Work": "ସିଚାଇ କାର୍ଯ୍ୟ",
        },
    }

    return translations.get(language, {}).get(value, value)


_COMMUNITY_DEMAND_VALUES = {
    "community",
    "community well",
    "community demand",
    "public",
    "public well",
    "shared among families",
}
_INDIVIDUAL_DEMAND_VALUES = {"private", "privately owned", "individual demand"}


def classify_demand_type(raw_value):
    if not raw_value:
        return raw_value
    normalized = raw_value.strip().lower().replace("_", " ")
    if normalized in _COMMUNITY_DEMAND_VALUES:
        return "community_demand"
    if normalized in _INDIVIDUAL_DEMAND_VALUES:
        return "individual_demand"
    return raw_value


IRRIGATION_STRUCTURE_MAPPING = {
    "select_one_farm_pond": "Farm pond",
    "select_one_community_pond": "Community Pond",
    "select_one_well": "Well",
    "select_one_canal": "Canal",
    "select_one_farm_bund": "Farm bund",
}

IRRIGATION_STRUCTURE_REVERSE_MAPPING = {
    v.lower(): k for k, v in IRRIGATION_STRUCTURE_MAPPING.items()
}

RS_WATER_STRUCTURE_MAPPING = {
    "select_one_farm_pond": "Farm pond",
    "select_one_community_pond": "Community Pond",
    "select_one_repair_large_water_body": "Large water body",
    "select_one_repair_canal": "Canal",
    "select_one_check_dam": "Check dam",
    "select_one_percolation_tank": "Percolation tank",
    "select_one_rock_fill_dam": "Rock fill dam",
    "select_one_loose_boulder_structure": "Loose boulder structure",
    "select_one_model5_structure": "5% Model structure",
    "select_one_Model30_40_structure": "30-40 Model structure",
}

RS_WATER_STRUCTIRE_REVERSE_MAPPING = {
    v.lower(): k for k, v in RS_WATER_STRUCTURE_MAPPING.items()
}


WATER_STRUCTURE_MAPPING = {
    "select_one_farm_pond": "Farm pond",
    "select_one_community_pond": "Community Pond",
    "select_one_repair_large_water_body": "Large water body",
    "select_one_repair_canal": "Canal",
    "select_one_check_dam": "Check dam",
    "select_one_percolation_tank": "Percolation tank",
    "select_one_rock_fill_dam": "Rock fill dam",
    "select_one_loose_boulder_structure": "Loose boulder structure",
    "select_one_model5_structure": "5% Model structure",
    "select_one_Model30_40_structure": "30-40 Model structure",
}

WATER_STRUCTURE_REVERSE_MAPPING = {
    v.lower(): k for k, v in WATER_STRUCTURE_MAPPING.items()
}
