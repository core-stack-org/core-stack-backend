from collections import defaultdict

from .mapping import (
    IRRIGATION_STRUCTURE_REVERSE_MAPPING,
    RECHARGE_STRUCTURE_REVERSE_MAPPING,
    RS_WATER_STRUCTIRE_REVERSE_MAPPING,
    WATER_STRUCTURE_REVERSE_MAPPING,
    classify_demand_type,
)
from .models import (
    DEMAND_STATUS_CHOICES,
    Agri_maintenance,
    GW_maintenance,
    ODK_agri,
    ODK_agrohorticulture,
    ODK_crop,
    ODK_groundwater,
    ODK_livelihood,
    ODK_settlement,
    ODK_waterbody,
    ODK_well,
    SWB_RS_maintenance,
    SWB_maintenance,
)
from .utils import ensure_str, format_text, get_waterbody_repair_activities


def _active_settlements(plan_id):
    return ODK_settlement.objects.filter(plan_id=plan_id).exclude(
        status_re="rejected"
    ).exclude(is_deleted=True)


def _active_qs(model, plan_id, data_field=None):
    qs = model.objects.filter(plan_id=plan_id).exclude(is_deleted=True)
    if data_field:
        qs = qs.exclude(status_re="rejected")
    return qs


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def get_dpr_summary(plan_id):
    pid = str(plan_id)
    return {
        "plan_id": plan_id,
        "sections": {
            "settlements": _active_settlements(pid).count(),
            "crops": ODK_crop.objects.filter(plan_id=pid).exclude(status_re="rejected").exclude(is_deleted=True).count(),
            "wells": ODK_well.objects.filter(plan_id=pid).exclude(status_re="rejected").exclude(is_deleted=True).count(),
            "waterbodies": ODK_waterbody.objects.filter(plan_id=pid).exclude(status_re="rejected").exclude(is_deleted=True).count(),
            "maintenance": {
                "gw": GW_maintenance.objects.filter(plan_id=pid).exclude(is_deleted=True).count(),
                "agri": Agri_maintenance.objects.filter(plan_id=pid).exclude(is_deleted=True).count(),
                "swb": SWB_maintenance.objects.filter(plan_id=pid).exclude(is_deleted=True).count(),
                "swb_rs": SWB_RS_maintenance.objects.filter(plan_id=pid).exclude(is_deleted=True).count(),
            },
            "nrm_works": {
                "recharge": ODK_groundwater.objects.filter(plan_id=pid).exclude(status_re="rejected").exclude(is_deleted=True).count(),
                "irrigation": ODK_agri.objects.filter(plan_id=pid).exclude(status_re="rejected").exclude(is_deleted=True).count(),
            },
            "livelihood": ODK_livelihood.objects.filter(plan_id=pid).exclude(status_re="rejected").exclude(is_deleted=True).count(),
            "agrohorticulture": ODK_agrohorticulture.objects.filter(plan_id=pid).exclude(status_re="rejected").exclude(is_deleted=True).count(),
        },
    }


# ---------------------------------------------------------------------------
# Section A – Team Details
# ---------------------------------------------------------------------------

def get_team_details(plan):
    return {
        "organization": plan.organization.name if plan.organization else None,
        "project": plan.project.name if plan.project else None,
        "plan": plan.plan,
        "facilitator": plan.facilitator_name,
        "process": "PRA, Gram Sabha, Transect Walk, GIS Mapping",
    }


# ---------------------------------------------------------------------------
# Section B – Village Brief
# ---------------------------------------------------------------------------

def get_village_brief(plan):
    pid = str(plan.id)
    return {
        "village_name": plan.village_name,
        "gram_panchayat": plan.gram_panchayat,
        "tehsil": plan.tehsil_soi.tehsil_name if plan.tehsil_soi else None,
        "district": plan.district_soi.district_name if plan.district_soi else None,
        "state": plan.state_soi.state_name if plan.state_soi else None,
        "total_settlements": _active_settlements(pid).count(),
        "latitude": float(plan.latitude) if plan.latitude else None,
        "longitude": float(plan.longitude) if plan.longitude else None,
    }


# ---------------------------------------------------------------------------
# Section C – Socio-Economic Profile
# ---------------------------------------------------------------------------

def get_settlements_data(plan_id):
    result = []
    for item in _active_settlements(str(plan_id)):
        ds = item.data_settlement or {}
        ff = item.farmer_family or {}
        largest_caste = (item.largest_caste or "").lower()

        if largest_caste == "single caste group":
            caste_group_detail = item.smallest_caste
        elif largest_caste == "mixed caste group":
            caste_group_detail = item.settlement_status
        else:
            caste_group_detail = None

        result.append({
            "settlement_id": item.settlement_id,
            "settlement_name": item.settlement_name,
            "number_of_households": item.number_of_households,
            "settlement_type": item.largest_caste,
            "caste_group_detail": caste_group_detail,
            "caste_counts": {
                "sc": ds.get("count_sc"),
                "st": ds.get("count_st"),
                "obc": ds.get("count_obc"),
                "general": ds.get("count_general"),
            },
            "marginal_farmers": ff.get("marginal_farmers"),
            "nrega_job_applied": item.nrega_job_applied,
            "nrega_job_card": item.nrega_job_card,
            "nrega_work_days": item.nrega_work_days,
            "nrega_past_work": item.nrega_past_work,
            "nrega_demand": item.nrega_demand,
            "nrega_issues": item.nrega_issues,
            "latitude": item.latitude,
            "longitude": item.longitude,
        })
    return result


def get_crops_data(plan_id):
    result = []
    for crop in ODK_crop.objects.filter(plan_id=str(plan_id)).exclude(status_re="rejected").exclude(is_deleted=True):
        dc = crop.data_crop or {}

        def _to_acres(key):
            raw = dc.get(key)
            if raw in (None, "NA", ""):
                return None
            try:
                return round(float(raw) * 2.47105, 4)
            except (ValueError, TypeError):
                return None

        result.append({
            "crop_grid_id": crop.crop_grid_id,
            "beneficiary_settlement": crop.beneficiary_settlement,
            "irrigation_source": crop.irrigation_source,
            "land_classification": crop.land_classification,
            "kharif_crops": crop.cropping_patterns_kharif,
            "kharif_acres": _to_acres("total_area_cultivation_kharif"),
            "rabi_crops": crop.cropping_patterns_rabi,
            "rabi_acres": _to_acres("total_area_cultivation_Rabi"),
            "zaid_crops": crop.cropping_patterns_zaid,
            "zaid_acres": _to_acres("total_area_cultivation_Zaid"),
            "cropping_intensity": crop.agri_productivity,
        })
    return result


def get_livestock_data(plan_id):
    result = []
    livestock_types = ["Goats", "Sheep", "Cattle", "Piggery", "Poultry"]

    def _fmt(val):
        return None if val in (None, "", "0", 0, "None") else str(val)

    for item in _active_settlements(str(plan_id)):
        lc = item.livestock_census or {}
        result.append({
            "settlement_id": item.settlement_id,
            "settlement_name": item.settlement_name,
            **{lt.lower(): _fmt(lc.get(lt)) for lt in livestock_types},
        })
    return result


# ---------------------------------------------------------------------------
# Section D – Wells and Water Structures
# ---------------------------------------------------------------------------

def _extract_well_fields(well):
    dw = well.data_well or {}

    well_type = dw.get("select_one_well_type")
    beneficiary_name = dw.get("Beneficiary_name")
    beneficiary_father_name = dw.get("ben_father")
    water_availability = dw.get("select_one_year")

    well_usage = None
    usage_data = dw.get("Well_usage", {})
    if usage_data:
        sow = ensure_str(usage_data.get("select_one_well_used"))
        sow_other = usage_data.get("select_one_well_used_other")
        if sow and sow.lower() == "other" and sow_other:
            well_usage = f"Other: {sow_other}"
        elif sow:
            well_usage = sow

    repair_activities = None
    if usage_data:
        repairs = ensure_str(usage_data.get("repairs_type"))
        repairs_other = usage_data.get("repairs_type_other")
        if repairs and repairs.lower() == "other" and repairs_other:
            repair_activities = f"Other: {repairs_other}"
        elif repairs:
            repair_activities = repairs.replace("_", " ")

    if not repair_activities:
        condition_data = dw.get("Well_condition", {})
        if condition_data:
            repairs = ensure_str(condition_data.get("select_one_repairs_well"))
            repairs_other = condition_data.get("select_one_repairs_well_other")
            if repairs and repairs.lower() == "other" and repairs_other:
                repair_activities = f"Other: {repairs_other}"
            elif repairs:
                repair_activities = repairs.replace("_", " ")

    return {
        "well_id": well.well_id,
        "beneficiary_settlement": well.beneficiary_settlement,
        "well_type": well_type,
        "owner": well.owner,
        "beneficiary_name": beneficiary_name,
        "beneficiary_father_name": beneficiary_father_name,
        "water_availability": water_availability,
        "households_benefitted": well.households_benefitted,
        "caste_uses": well.caste_uses,
        "well_usage": well_usage,
        "need_maintenance": well.need_maintenance,
        "repair_activities": repair_activities,
        "latitude": well.latitude,
        "longitude": well.longitude,
    }


def get_wells_data(plan_id):
    wells = ODK_well.objects.filter(plan_id=str(plan_id)).exclude(
        status_re="rejected"
    ).exclude(is_deleted=True)
    return [_extract_well_fields(w) for w in wells]


def _extract_waterbody_fields(wb):
    dw = wb.data_waterbody or {}

    who_manages = wb.who_manages or None
    if who_manages and who_manages.lower() == "other":
        who_manages = "Other: " + (wb.specify_other_manager or "")

    water_structure_type = wb.water_structure_type or None
    if water_structure_type and water_structure_type.lower() == "other":
        water_structure_type = "Other: " + (wb.water_structure_other or "")

    repair_activities = get_waterbody_repair_activities(dw, water_structure_type or "")

    usage_raw = dw.get("select_multiple_uses_structure")
    usage = format_text(usage_raw).strip() if usage_raw else None

    return {
        "waterbody_id": wb.waterbody_id,
        "beneficiary_settlement": wb.beneficiary_settlement,
        "owner": wb.owner,
        "beneficiary_name": dw.get("Beneficiary_name"),
        "beneficiary_father_name": dw.get("ben_father"),
        "who_manages": who_manages,
        "caste_who_uses": wb.caste_who_uses,
        "households_benefitted": wb.household_benefitted,
        "water_structure_type": water_structure_type,
        "usage": usage,
        "need_maintenance": wb.need_maintenance,
        "repair_activities": repair_activities,
        "latitude": wb.latitude,
        "longitude": wb.longitude,
    }


def get_waterbodies_data(plan_id):
    wbs = ODK_waterbody.objects.filter(plan_id=str(plan_id)).exclude(
        status_re="rejected"
    ).exclude(is_deleted=True)
    return [_extract_waterbody_fields(wb) for wb in wbs]


# ---------------------------------------------------------------------------
# Section E – Maintenance
# ---------------------------------------------------------------------------

def _resolve_repair_activity(data, structure_type, reverse_mapping, fallback_key="select_one_activities"):
    repair_activities = None
    if structure_type and structure_type != "NA" and structure_type in reverse_mapping:
        repair_key = reverse_mapping[structure_type]
        repair_val = ensure_str(data.get(repair_key))
        if repair_val and repair_val.lower() == "other":
            repair_activities = data.get(f"{repair_key}_other")
        else:
            repair_activities = repair_val
    if not repair_activities:
        repair_activities = data.get(fallback_key)
    return repair_activities


def get_maintenance_data(plan_id, maintenance_type):
    pid = str(plan_id)
    result = []

    if maintenance_type == "gw":
        for m in GW_maintenance.objects.filter(plan_id=pid).exclude(is_deleted=True):
            d = m.data_gw_maintenance or {}
            structure_type = d.get("select_one_recharge_structure") or d.get("select_one_water_structure") or "NA"
            repair = _resolve_repair_activity(d, structure_type, RECHARGE_STRUCTURE_REVERSE_MAPPING)
            result.append({
                "id": m.gw_maintenance_id,
                "demand_type": classify_demand_type(d.get("demand_type")),
                "beneficiary_settlement": d.get("beneficiary_settlement"),
                "beneficiary_name": d.get("Beneficiary_Name"),
                "beneficiary_father_name": d.get("ben_father"),
                "structure_type": structure_type,
                "repair_activities": repair,
                "latitude": m.latitude,
                "longitude": m.longitude,
            })

    elif maintenance_type == "agri":
        for m in Agri_maintenance.objects.filter(plan_id=pid).exclude(is_deleted=True):
            d = m.data_agri_maintenance or {}
            structure_type = d.get("select_one_water_structure") or d.get("select_one_irrigation_structure") or "NA"
            repair = _resolve_repair_activity(d, structure_type, IRRIGATION_STRUCTURE_REVERSE_MAPPING)
            result.append({
                "id": m.agri_maintenance_id,
                "demand_type": classify_demand_type(d.get("demand_type")),
                "beneficiary_settlement": d.get("beneficiary_settlement"),
                "beneficiary_name": d.get("Beneficiary_Name"),
                "beneficiary_father_name": d.get("ben_father"),
                "structure_type": structure_type,
                "repair_activities": repair,
                "latitude": m.latitude,
                "longitude": m.longitude,
            })

    elif maintenance_type == "swb":
        for m in SWB_maintenance.objects.filter(plan_id=pid).exclude(is_deleted=True):
            d = m.data_swb_maintenance or {}
            structure_type = d.get("TYPE_OF_WORK") or d.get("select_one_water_structure") or "NA"
            repair = _resolve_repair_activity(d, structure_type, WATER_STRUCTURE_REVERSE_MAPPING)
            result.append({
                "id": m.swb_maintenance_id,
                "demand_type": classify_demand_type(d.get("demand_type")),
                "beneficiary_settlement": d.get("beneficiary_settlement"),
                "beneficiary_name": d.get("Beneficiary_Name"),
                "beneficiary_father_name": d.get("ben_father"),
                "structure_type": structure_type,
                "repair_activities": repair,
                "latitude": m.latitude,
                "longitude": m.longitude,
            })

    elif maintenance_type == "swb_rs":
        for m in SWB_RS_maintenance.objects.filter(plan_id=pid).exclude(is_deleted=True):
            d = m.data_swb_rs_maintenance or {}
            structure_type = d.get("TYPE_OF_WORK") or "NA"
            repair = _resolve_repair_activity(d, structure_type, RS_WATER_STRUCTIRE_REVERSE_MAPPING)
            result.append({
                "id": m.swb_rs_maintenance_id,
                "demand_type": classify_demand_type(d.get("demand_type")),
                "beneficiary_settlement": d.get("beneficiary_settlement"),
                "beneficiary_name": d.get("Beneficiary_Name"),
                "beneficiary_father_name": d.get("ben_father"),
                "structure_type": structure_type,
                "repair_activities": repair,
                "latitude": m.latitude,
                "longitude": m.longitude,
            })

    return result


# ---------------------------------------------------------------------------
# Section F – New NRM Works
# ---------------------------------------------------------------------------

def get_nrm_works_data(plan_id):
    pid = str(plan_id)
    result = []

    for structure in ODK_groundwater.objects.filter(plan_id=pid).exclude(status_re="rejected").exclude(is_deleted=True):
        dg = structure.data_groundwater or {}
        result.append({
            "work_category": "Recharge Structure",
            "demand_type": classify_demand_type(dg.get("demand_type")),
            "work_demand": structure.work_type,
            "beneficiary_settlement": structure.beneficiary_settlement,
            "beneficiary_name": dg.get("Beneficiary_Name"),
            "gender": dg.get("select_gender"),
            "beneficiary_father_name": dg.get("ben_father"),
            "latitude": structure.latitude,
            "longitude": structure.longitude,
        })

    for irr in ODK_agri.objects.filter(plan_id=pid).exclude(status_re="rejected").exclude(is_deleted=True):
        da = irr.data_agri or {}
        work_demand = irr.work_type
        if (irr.work_type or "").lower() == "other":
            work_demand = da.get("TYPE_OF_WORK_ID_other") or "Other (unspecified)"
        result.append({
            "work_category": "Irrigation Work",
            "demand_type": classify_demand_type(da.get("demand_type_irrigation")),
            "work_demand": work_demand,
            "beneficiary_settlement": irr.beneficiary_settlement,
            "beneficiary_name": da.get("Beneficiary_Name"),
            "gender": da.get("gender"),
            "beneficiary_father_name": da.get("ben_father"),
            "latitude": irr.latitude,
            "longitude": irr.longitude,
        })

    return result


# ---------------------------------------------------------------------------
# Section G – Livelihood
# ---------------------------------------------------------------------------

def get_livelihood_data(plan_id):
    pid = str(plan_id)
    result = []

    for record in ODK_livelihood.objects.filter(plan_id=pid).exclude(status_re="rejected").exclude(is_deleted=True):
        dl = record.data_livelihood or {}

        livestock_group = dl.get("Livestock") or {}
        fisheries_group = dl.get("fisheries") or {}
        plantation_group = dl.get("plantations") or {}
        kitchen_garden_group = dl.get("kitchen_gardens") or {}

        is_livestock = (
            ensure_str(livestock_group.get("is_demand_livestock", "")).lower() == "yes"
            or ensure_str(dl.get("select_one_demand_promoting_livestock", "")).lower() == "yes"
        )
        if is_livestock:
            demands = ensure_str(livestock_group.get("demands_promoting_livestock"))
            if demands and demands.lower() == "other":
                demands = livestock_group.get("demands_promoting_livestock_other")
            if not demands:
                demands = ensure_str(dl.get("select_one_promoting_livestock"))
                if demands and demands.lower() == "other":
                    demands = dl.get("select_one_promoting_livestock_other")
            result.append({
                "livelihood_work": "Livestock",
                "demand_type": livestock_group.get("livestock_demand"),
                "work_demand": format_text(demands).strip() if demands else None,
                "beneficiary_settlement": record.beneficiary_settlement,
                "beneficiary_name": dl.get("beneficiary_name") or livestock_group.get("ben_livestock"),
                "gender": livestock_group.get("gender_livestock"),
                "beneficiary_father_name": livestock_group.get("ben_father_livestock"),
                "latitude": record.latitude,
                "longitude": record.longitude,
            })

        is_fisheries = (
            ensure_str(fisheries_group.get("is_demand_fisheris", "")).lower() == "yes"
            or ensure_str(dl.get("select_one_demand_promoting_fisheries", "")).lower() == "yes"
        )
        if is_fisheries:
            demands = ensure_str(fisheries_group.get("select_one_promoting_fisheries"))
            if demands and demands.lower() == "other":
                demands = fisheries_group.get("select_one_promoting_fisheries_other")
            if not demands:
                demands = ensure_str(dl.get("select_one_promoting_fisheries"))
                if demands and demands.lower() == "other":
                    demands = dl.get("select_one_promoting_fisheries_other")
            result.append({
                "livelihood_work": "Fisheries",
                "demand_type": fisheries_group.get("demand_type_fisheries"),
                "work_demand": format_text(demands).strip() if demands else None,
                "beneficiary_settlement": record.beneficiary_settlement,
                "beneficiary_name": dl.get("beneficiary_name") or fisheries_group.get("ben_fisheries"),
                "gender": fisheries_group.get("gender_fisheries"),
                "beneficiary_father_name": fisheries_group.get("ben_father_fisheries"),
                "latitude": record.latitude,
                "longitude": record.longitude,
            })

        is_plantation = (
            ensure_str(dl.get("select_one_demand_plantation", "")).lower() == "yes"
            or ensure_str(plantation_group.get("select_plantation_demands", "")).lower() == "yes"
        )
        if is_plantation:
            result.append({
                "livelihood_work": "Plantations",
                "demand_type": classify_demand_type(plantation_group.get("demand_type_plantations")),
                "work_demand": dl.get("Plantation") or plantation_group.get("crop_name"),
                "beneficiary_settlement": record.beneficiary_settlement,
                "beneficiary_name": dl.get("beneficiary_name") or plantation_group.get("ben_plantation"),
                "gender": plantation_group.get("gender"),
                "beneficiary_father_name": plantation_group.get("ben_father"),
                "total_acres": dl.get("Plantation_crop") or plantation_group.get("crop_area"),
                "latitude": record.latitude,
                "longitude": record.longitude,
            })

        is_kitchen_garden = (
            ensure_str(dl.get("indi_assets", "")).lower() == "yes"
            or ensure_str(kitchen_garden_group.get("assets_kg", "")).lower() == "yes"
        )
        if is_kitchen_garden:
            result.append({
                "livelihood_work": "Kitchen Garden",
                "demand_type": kitchen_garden_group.get("demand_type_kitchen_garden"),
                "work_demand": dl.get("Plantation"),
                "beneficiary_settlement": record.beneficiary_settlement,
                "beneficiary_name": dl.get("beneficiary_name") or kitchen_garden_group.get("ben_kitchen_gardens"),
                "gender": kitchen_garden_group.get("gender_kitchen_gardens"),
                "beneficiary_father_name": kitchen_garden_group.get("ben_father_kitchen_gardens"),
                "total_acres": dl.get("area_didi_badi") or kitchen_garden_group.get("area_kg"),
                "latitude": record.latitude,
                "longitude": record.longitude,
            })

    for agrohorti in ODK_agrohorticulture.objects.filter(plan_id=pid).exclude(status_re="rejected").exclude(is_deleted=True):
        data = agrohorti.data_agohorticulture or {}
        species_parts = filter(None, [data.get("select_multiple_species"), data.get("select_multiple_species_other")])
        species = " ".join(species_parts) or None
        result.append({
            "livelihood_work": "Plantations",
            "demand_type": classify_demand_type(data.get("demand_type_plantations")),
            "work_demand": species,
            "beneficiary_settlement": data.get("beneficiary_settlement"),
            "beneficiary_name": data.get("beneficiary_name"),
            "gender": data.get("gender"),
            "beneficiary_father_name": data.get("ben_father"),
            "total_acres": data.get("crop_area"),
            "latitude": agrohorti.latitude,
            "longitude": agrohorti.longitude,
        })

    return result


# ---------------------------------------------------------------------------
# DPR Status Tracking & Demand Status Updates
# ---------------------------------------------------------------------------

RESOURCE_TYPE_MAP = {
    "settlement": (ODK_settlement, "settlement_id", "settlement_demand_status"),
    "well": (ODK_well, "well_id", "well_demand_status"),
    "waterbody": (ODK_waterbody, "waterbody_id", "waterbody_demand_status"),
    "crop": (ODK_crop, "crop_grid_id", "crop_pattern_demand_status"),
}

DEMAND_TYPE_MAP = {
    "groundwater": (ODK_groundwater, "recharge_structure_id", "recharge_structure_demand_status"),
    "agri": (ODK_agri, "irrigation_work_id", "irrigation_work_demand_status"),
    "livelihood": (ODK_livelihood, "livelihood_id", "livelihood_demand_status"),
    "agrohorticulture": (ODK_agrohorticulture, "agrohorticulture_id", "agrohorticulture_demand_status"),
    "gw_maintenance": (GW_maintenance, "gw_maintenance_id", "recharge_structure_maintenance_status"),
    "swb_rs_maintenance": (SWB_RS_maintenance, "swb_rs_maintenance_id", "swb_rs_maintenance_status"),
    "swb_maintenance": (SWB_maintenance, "swb_maintenance_id", "swb_maintenance_status"),
    "agri_maintenance": (Agri_maintenance, "agri_maintenance_id", "irrigation_structure_maintenance_status"),
}

ALL_TYPE_MAP = {**RESOURCE_TYPE_MAP, **DEMAND_TYPE_MAP}

VALID_DEMAND_STATUSES = {c[0] for c in DEMAND_STATUS_CHOICES}


def _count_by_status(type_map, plan_id, target_status):
    pid = str(plan_id)
    total = 0
    for _model, _pk, demand_field in type_map.values():
        total += _model.objects.filter(plan_id=pid, **{demand_field: target_status}).exclude(is_deleted=True).count()
    return total


def get_dpr_status_tracking(plan_id):
    return {
        "statuses": [
            {
                "key": "SUBMITTED",
                "label": "Submitted",
                "sub_sections": [
                    {
                        "key": "RESOURCES_SUBMITTED",
                        "label": "Resources Submitted",
                        "count": _count_by_status(RESOURCE_TYPE_MAP, plan_id, "SUBMITTED"),
                    },
                    {
                        "key": "DEMANDS_SUBMITTED",
                        "label": "Demands Submitted",
                        "count": _count_by_status(DEMAND_TYPE_MAP, plan_id, "SUBMITTED"),
                    },
                ],
            },
            {
                "key": "APPROVED",
                "label": "Approved",
                "count": _count_by_status(ALL_TYPE_MAP, plan_id, "APPROVED"),
            },
            {
                "key": "REJECTED",
                "label": "Rejected",
                "count": _count_by_status(ALL_TYPE_MAP, plan_id, "REJECTED"),
            },
        ]
    }


def update_demand_status(plan_id, resource_type, resource_id, new_status):
    if resource_type not in ALL_TYPE_MAP:
        return None, f"Invalid resource_type. Choose from: {', '.join(sorted(ALL_TYPE_MAP))}"

    if new_status not in VALID_DEMAND_STATUSES:
        return None, f"Invalid status. Choose from: {', '.join(sorted(VALID_DEMAND_STATUSES))}"

    model, pk_field, demand_field = ALL_TYPE_MAP[resource_type]
    try:
        obj = model.objects.get(**{pk_field: resource_id, "plan_id": str(plan_id)})
    except model.DoesNotExist:
        return None, "Resource not found"

    setattr(obj, demand_field, new_status)
    obj.save(update_fields=[demand_field])
    return {
        "resource_type": resource_type,
        "resource_id": str(resource_id),
        "status": new_status,
    }, None
