from django.db.models import Q

from dpr.models import (
    ODK_settlement,
    ODK_well,
    ODK_waterbody,
    ODK_groundwater,
    ODK_agri,
    ODK_livelihood,
    ODK_crop,
    Agri_maintenance,
    GW_maintenance,
    SWB_maintenance,
    SWB_RS_maintenance,
)
from dpr.utils import determine_caste_fields


def extract_lat_lon_from_gps(gps):
    if not gps:
        return None, None
    for key in ("point_mapsappearance", "point_mapappearance"):
        point = gps.get(key)
        if point and isinstance(point, dict):
            coords = point.get("coordinates", [])
            if coords and len(coords) >= 2:
                return coords[1], coords[0]
    return None, None


def sync_edited_updated_settlement(sub):
    settlement_id = sub.get("Settlements_id")
    if ODK_settlement.objects.filter(
        settlement_id=settlement_id
    ).filter(Q(is_moderated=True) | Q(is_deleted=True)).exists():
        return

    system = sub.get("__system", {})
    gps = sub.get("GPS_point", {})
    nrega = sub.get("MNREGA_INFORMATION", {})
    farmer_family = sub.get("farmer_family", {})
    Livestock_Census = sub.get("Livestock_Census", {})
    lat, lon = extract_lat_lon_from_gps(gps)
    largest_caste, smallest_caste, settlement_status, use_existing_logic = (
        determine_caste_fields(sub)
    )

    nrega_issues = nrega.get("select_multiple_issues", "") or "NA"
    if isinstance(nrega_issues, str) and "other" in nrega_issues.lower():
        other_text = nrega.get("select_multiple_issues_other", "")
        if other_text:
            nrega_issues = f"{nrega_issues}: {other_text}" if nrega_issues.lower() != "other" else other_text

    mapped = {
        "settlement_name": sub.get("Settlements_name"),
        "submission_time": system.get("submissionDate"),
        "submitted_by": system.get("submitterName"),
        "status_re": system.get("reviewState") or "in progress",
        "latitude": lat,
        "longitude": lon,
        "block_name": sub.get("block_name"),
        "number_of_households": sub.get("number_households") or 0,
        "largest_caste": largest_caste,
        "smallest_caste": smallest_caste,
        "settlement_status": settlement_status,
        "plan_id": sub.get("plan_id") or "NA",
        "plan_name": sub.get("plan_name") or "NA",
        "uuid": sub.get("__id") or "NA",
        "system": system,
        "gps_point": gps,
        "farmer_family": farmer_family,
        "livestock_census": Livestock_Census,
        "nrega_job_aware": nrega.get("NREGA_aware", "") or 0,
        "nrega_job_applied": nrega.get("NREGA_applied", "") or 0,
        "nrega_job_card": nrega.get("NREGA_have_job_card", "") or 0,
        "nrega_without_job_card": nrega.get("total_household", "") or 0,
        "nrega_work_days": nrega.get("NREGA_work_days", "") or 0,
        "nrega_past_work": nrega.get("work_demands", "") or "NA",
        "nrega_raise_demand": nrega.get("select_one_Y_N", "") or "NA",
        "nrega_demand": nrega.get("select_one_demands", "") or "NA",
        "nrega_issues": nrega_issues,
        "nrega_community": nrega.get("select_one_contributions", "") or "NA",
        "data_settlement": sub,
    }

    ODK_settlement.objects.update_or_create(
        settlement_id=settlement_id, defaults=mapped
    )


def sync_edited_updated_well(well_submission):
    well_id = well_submission.get("well_id")
    if ODK_well.objects.filter(well_id=well_id).filter(
        Q(is_moderated=True) | Q(is_deleted=True)
    ).exists():
        return

    Well_usage = well_submission.get("Well_usage", {})
    gps = well_submission.get("GPS_point", {})
    Well_condition = well_submission.get("Well_condition", {})
    system = well_submission.get("__system", {})
    lat, lon = extract_lat_lon_from_gps(gps)

    need_maintenance = Well_usage.get("is_maintenance_required", "") or "NA"
    if need_maintenance == "NA":
        need_maintenance = Well_condition.get("select_one_maintenance") or "NA"

    mapped = {
        "uuid": well_submission.get("__id") or "NA",
        "submission_time": system.get("submissionDate"),
        "beneficiary_settlement": well_submission.get("beneficiary_settlement") or "NA",
        "block_name": well_submission.get("block_name") or "NA",
        "owner": well_submission.get("select_one_owns") or "NA",
        "households_benefitted": well_submission.get("households_benefited") or 0,
        "caste_uses": well_submission.get("select_multiple_caste_use") or "NA",
        "is_functional": Well_usage.get("select_one_Functional_Non_functional") or "NA",
        "need_maintenance": need_maintenance,
        "plan_id": well_submission.get("plan_id") or "NA",
        "plan_name": well_submission.get("plan_name") or "NA",
        "status_re": system.get("reviewState") or "in progress",
        "latitude": lat,
        "longitude": lon,
        "system": system,
        "gps_point": gps,
        "data_well": well_submission,
    }

    ODK_well.objects.update_or_create(well_id=well_id, defaults=mapped)


def sync_edited_updated_waterbody(waterbody_submission):
    waterbody_id = waterbody_submission.get("waterbodies_id")
    if ODK_waterbody.objects.filter(waterbody_id=waterbody_id).filter(
        Q(is_moderated=True) | Q(is_deleted=True)
    ).exists():
        return

    gps = waterbody_submission.get("GPS_point", {})
    system = waterbody_submission.get("__system", {})
    lat, lon = extract_lat_lon_from_gps(gps)

    mapped = {
        "uuid": waterbody_submission.get("__id") or "NA",
        "submission_time": system.get("submissionDate"),
        "block_name": waterbody_submission.get("block_name") or "NA",
        "beneficiary_settlement": waterbody_submission.get("beneficiary_settlement") or "NA",
        "beneficiary_contact": waterbody_submission.get("Beneficiary_contact_number") or "NA",
        "who_manages": waterbody_submission.get("select_one_manages") or "NA",
        "specify_other_manager": waterbody_submission.get("text_one_manages") or "NA",
        "owner": waterbody_submission.get("select_one_owns") or "NA",
        "caste_who_uses": waterbody_submission.get("select_multiple_caste_use") or "NA",
        "household_benefitted": waterbody_submission.get("households_benefited") or 0,
        "water_structure_type": waterbody_submission.get("select_one_water_structure") or "NA",
        "water_structure_other": waterbody_submission.get("select_one_water_structure_other") or "NA",
        "identified_by": waterbody_submission.get("select_one_identified") or "No Data Provided",
        "need_maintenance": waterbody_submission.get("select_one_maintenance") or "No Data Provided",
        "plan_id": waterbody_submission.get("plan_id") or "0",
        "plan_name": waterbody_submission.get("plan_name") or "0",
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState") or "in progress",
        "system": system,
        "gps_point": gps,
        "data_waterbody": waterbody_submission,
    }

    ODK_waterbody.objects.update_or_create(waterbody_id=waterbody_id, defaults=mapped)


def sync_edited_updated_gw(gw_submission):
    recharge_structure_id = gw_submission.get("work_id")
    if ODK_groundwater.objects.filter(recharge_structure_id=recharge_structure_id).filter(
        Q(is_moderated=True) | Q(is_deleted=True)
    ).exists():
        return

    gps = gw_submission.get("GPS_point", {})
    system = gw_submission.get("__system", {})
    lat, lon = extract_lat_lon_from_gps(gps)
    status_re = system.get("reviewState") or "in progress"

    if status_re.lower() == "rejected":
        return

    mapped = {
        "uuid": gw_submission.get("__id") or "0",
        "submission_time": system.get("submissionDate"),
        "beneficiary_settlement": gw_submission.get("beneficiary_settlement") or "NA",
        "block_name": gw_submission.get("block_name") or "NA",
        "work_type": gw_submission.get("TYPE_OF_WORK_ID") or "NA",
        "plan_id": gw_submission.get("plan_id") or "NA",
        "plan_name": gw_submission.get("plan_name") or "NA",
        "latitude": lat,
        "longitude": lon,
        "status_re": status_re,
        "system": system,
        "gps_point": gps,
        "data_groundwater": gw_submission,
    }

    obj, created = ODK_groundwater.objects.update_or_create(
        recharge_structure_id=recharge_structure_id, defaults=mapped
    )
    work_types = ["Check_dam", "Loose_Boulder_Structure", "Trench_cum_bunds"]
    for work_type in work_types:
        if work_type in gw_submission:
            obj.update_work_dimensions(work_type=work_type, work_details=gw_submission[work_type])


def sync_edited_updated_agri(agri_submission):
    irrigation_work_id = agri_submission.get("work_id")
    if ODK_agri.objects.filter(irrigation_work_id=irrigation_work_id).filter(
        Q(is_moderated=True) | Q(is_deleted=True)
    ).exists():
        return

    gps = agri_submission.get("GPS_point", {})
    system = agri_submission.get("__system", {})
    lat, lon = extract_lat_lon_from_gps(gps)

    mapped = {
        "uuid": agri_submission.get("__id") or "0",
        "submission_time": system.get("submissionDate"),
        "beneficiary_settlement": agri_submission.get("beneficiary_settlement") or "0",
        "block_name": agri_submission.get("block_name") or "",
        "work_type": agri_submission.get("TYPE_OF_WORK_ID") or "",
        "plan_id": agri_submission.get("plan_id") or "0",
        "plan_name": agri_submission.get("plan_name") or "0",
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState") or "in progress",
        "system": system,
        "gps_point": gps,
        "data_agri": agri_submission,
    }

    obj, created = ODK_agri.objects.update_or_create(
        irrigation_work_id=irrigation_work_id, defaults=mapped
    )
    work_types = ["new_well", "Land_leveling", "Farm_pond"]
    for work_type in work_types:
        if work_type in agri_submission:
            obj.update_work_dimensions(work_type=work_type, work_details=agri_submission[work_type])


def sync_edited_updated_livelihood(livelihood_submission):
    livelihood_id = livelihood_submission.get("work_id")
    if ODK_livelihood.objects.filter(livelihood_id=livelihood_id).filter(
        Q(is_moderated=True) | Q(is_deleted=True)
    ).exists():
        return

    gps = livelihood_submission.get("GPS_point", {})
    system = livelihood_submission.get("__system", {})
    lat, lon = extract_lat_lon_from_gps(gps)

    mapped = {
        "uuid": livelihood_submission.get("__id") or "0",
        "beneficiary_settlement": livelihood_submission.get("beneficiary_settlement") or "0",
        "block_name": livelihood_submission.get("block_name") or "0",
        "beneficiary_contact": livelihood_submission.get("Beneficiary_Contact_Number") or "0",
        "livestock_development": livelihood_submission.get("select_one_promoting_livestock") or "0",
        "submission_time": system.get("submissionDate"),
        "fisheries": livelihood_submission.get("select_one_promoting_fisheries") or "0",
        "common_asset": livelihood_submission.get("select_one_common_asset") or "0",
        "plan_id": livelihood_submission.get("plan_id") or "0",
        "plan_name": livelihood_submission.get("plan_name") or "0",
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState") or "in progress",
        "system": system,
        "gps_point": gps,
        "data_livelihood": livelihood_submission,
    }

    ODK_livelihood.objects.update_or_create(livelihood_id=livelihood_id, defaults=mapped)


def sync_edited_updated_cropping_pattern(cp_submission):
    uuid_val = cp_submission.get("__id", "") or "NA"
    crop_grid_id = cp_submission.get("crop_Grid_id", "")
    crop_grid_id = crop_grid_id if crop_grid_id and crop_grid_id != "undefined" else uuid_val

    if ODK_crop.objects.filter(crop_grid_id=crop_grid_id).filter(
        Q(is_moderated=True) | Q(is_deleted=True)
    ).exists():
        return

    system = cp_submission.get("__system", {})

    def get_crop_pattern(field_name, other_field_name):
        crops = cp_submission.get(field_name, "")
        if crops and "other" in crops.lower():
            other = cp_submission.get(other_field_name, "")
            if other:
                return f"{crops}: {other}"
        return crops or "NA"

    mapped = {
        "uuid": uuid_val,
        "beneficiary_settlement": cp_submission.get("beneficiary_settlement") or "NA",
        "irrigation_source": cp_submission.get("select_multiple_widgets") or "NA",
        "submission_time": system.get("submissionDate"),
        "land_classification": cp_submission.get("select_one_classified") or "NA",
        "cropping_patterns_kharif": get_crop_pattern("select_multiple_cropping_kharif", "select_multiple_cropping_kharif_other"),
        "cropping_patterns_rabi": get_crop_pattern("select_multiple_cropping_Rabi", "select_multiple_cropping_Rabi_other"),
        "cropping_patterns_zaid": get_crop_pattern("select_multiple_cropping_Zaid", "select_multiple_cropping_Zaid_other"),
        "agri_productivity": cp_submission.get("select_one_productivity") or "NA",
        "plan_id": cp_submission.get("plan_id") or "NA",
        "plan_name": cp_submission.get("plan_name") or "NA",
        "status_re": system.get("reviewState") or "in progress",
        "system": system,
        "data_crop": cp_submission,
    }

    ODK_crop.objects.update_or_create(crop_grid_id=crop_grid_id, defaults=mapped)


def sync_edited_updated_agri_maintenance(am_submission):
    uuid_val = am_submission.get("__id") or "0"
    if Agri_maintenance.objects.filter(uuid=uuid_val).filter(
        Q(is_moderated=True) | Q(is_deleted=True)
    ).exists():
        return

    gps = am_submission.get("GPS_point", {})
    system = am_submission.get("__system", {})
    lat, lon = extract_lat_lon_from_gps(gps)

    mapped = {
        "plan_id": am_submission.get("plan_id") or "0",
        "plan_name": am_submission.get("plan_name") or "0",
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState") or "in progress",
        "work_id": am_submission.get("work_id") or "0",
        "corresponding_work_id": am_submission.get("corresponding_work_id") or "0",
        "submission_time": system.get("submissionDate"),
        "data_agri_maintenance": am_submission,
    }

    Agri_maintenance.objects.update_or_create(uuid=uuid_val, defaults=mapped)


def sync_edited_updated_gw_maintenance(gwm_submission):
    uuid_val = gwm_submission.get("__id") or "0"
    if GW_maintenance.objects.filter(uuid=uuid_val).filter(
        Q(is_moderated=True) | Q(is_deleted=True)
    ).exists():
        return

    gps = gwm_submission.get("GPS_point", {})
    system = gwm_submission.get("__system", {})
    lat, lon = extract_lat_lon_from_gps(gps)

    mapped = {
        "plan_id": gwm_submission.get("plan_id") or "0",
        "plan_name": gwm_submission.get("plan_name") or "0",
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState") or "in progress",
        "work_id": gwm_submission.get("work_id") or "0",
        "corresponding_work_id": gwm_submission.get("corresponding_work_id") or "0",
        "submission_time": system.get("submissionDate"),
        "data_gw_maintenance": gwm_submission,
    }

    GW_maintenance.objects.update_or_create(uuid=uuid_val, defaults=mapped)


def sync_edited_updated_swb_maintenance(swbm_submission):
    uuid_val = swbm_submission.get("__id") or "0"
    if SWB_maintenance.objects.filter(uuid=uuid_val).filter(
        Q(is_moderated=True) | Q(is_deleted=True)
    ).exists():
        return

    gps = swbm_submission.get("GPS_point", {})
    system = swbm_submission.get("__system", {})
    lat, lon = extract_lat_lon_from_gps(gps)

    mapped = {
        "plan_id": swbm_submission.get("plan_id") or "0",
        "plan_name": swbm_submission.get("plan_name") or "0",
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState") or "in progress",
        "work_id": swbm_submission.get("work_id") or "0",
        "corresponding_work_id": swbm_submission.get("corresponding_work_id") or "0",
        "submission_time": system.get("submissionDate"),
        "data_swb_maintenance": swbm_submission,
    }

    SWB_maintenance.objects.update_or_create(uuid=uuid_val, defaults=mapped)


def sync_edited_updated_swb_rs_maintenance(swb_rs_submission):
    uuid_val = swb_rs_submission.get("__id") or "0"
    if SWB_RS_maintenance.objects.filter(uuid=uuid_val).filter(
        Q(is_moderated=True) | Q(is_deleted=True)
    ).exists():
        return

    gps = swb_rs_submission.get("GPS_point", {})
    system = swb_rs_submission.get("__system", {})
    lat, lon = extract_lat_lon_from_gps(gps)

    mapped = {
        "plan_id": swb_rs_submission.get("plan_id") or "0",
        "plan_name": swb_rs_submission.get("plan_name") or "0",
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState") or "in progress",
        "work_id": swb_rs_submission.get("work_id") or "0",
        "corresponding_work_id": swb_rs_submission.get("corresponding_work_id") or "0",
        "submission_time": system.get("submissionDate"),
        "data_swb_rs_maintenance": swb_rs_submission,
    }

    SWB_RS_maintenance.objects.update_or_create(uuid=uuid_val, defaults=mapped)


def _extract_settlement_fields(data):
    nrega = data.get("MNREGA_INFORMATION", {})
    largest_caste, smallest_caste, settlement_status, use_existing = (
        determine_caste_fields(data)
    )
    nrega_issues = nrega.get("select_multiple_issues", "") or "NA"
    if isinstance(nrega_issues, str) and "other" in nrega_issues.lower():
        other_text = nrega.get("select_multiple_issues_other", "")
        if other_text:
            nrega_issues = (
                f"{nrega_issues}: {other_text}"
                if nrega_issues.lower() != "other"
                else other_text
            )
    fields = {
        "settlement_name": data.get("Settlements_name") or "",
        "block_name": data.get("block_name") or "",
        "number_of_households": data.get("number_households") or 0,
        "farmer_family": data.get("farmer_family", {}),
        "livestock_census": data.get("Livestock_Census", {}),
        "nrega_job_aware": nrega.get("NREGA_aware", "") or 0,
        "nrega_job_applied": nrega.get("NREGA_applied", "") or 0,
        "nrega_job_card": nrega.get("NREGA_have_job_card", "") or 0,
        "nrega_without_job_card": nrega.get("total_household", "") or 0,
        "nrega_work_days": nrega.get("NREGA_work_days", "") or 0,
        "nrega_past_work": nrega.get("work_demands", "") or "NA",
        "nrega_raise_demand": nrega.get("select_one_Y_N", "") or "NA",
        "nrega_demand": nrega.get("select_one_demands", "") or "NA",
        "nrega_issues": nrega_issues,
        "nrega_community": nrega.get("select_one_contributions", "") or "NA",
    }
    if not use_existing:
        fields["largest_caste"] = largest_caste
        fields["smallest_caste"] = smallest_caste
        fields["settlement_status"] = settlement_status
    return fields


def _extract_well_fields(data):
    well_usage = data.get("Well_usage", {})
    well_condition = data.get("Well_condition", {})
    need_maintenance = well_usage.get("is_maintenance_required", "") or "NA"
    if need_maintenance == "NA":
        need_maintenance = well_condition.get("select_one_maintenance") or "NA"
    return {
        "beneficiary_settlement": data.get("beneficiary_settlement") or "NA",
        "block_name": data.get("block_name") or "NA",
        "owner": data.get("select_one_owns") or "NA",
        "households_benefitted": data.get("households_benefited") or 0,
        "caste_uses": data.get("select_multiple_caste_use") or "NA",
        "is_functional": well_usage.get("select_one_Functional_Non_functional") or "NA",
        "need_maintenance": need_maintenance,
    }


def _extract_waterbody_fields(data):
    return {
        "beneficiary_settlement": data.get("beneficiary_settlement") or "NA",
        "block_name": data.get("block_name") or "NA",
        "who_manages": data.get("select_one_manages") or "NA",
        "specify_other_manager": data.get("text_one_manages") or "NA",
        "owner": data.get("select_one_owns") or "NA",
        "caste_who_uses": data.get("select_multiple_caste_use") or "NA",
        "household_benefitted": data.get("households_benefited") or 0,
        "water_structure_type": data.get("select_one_water_structure") or "NA",
        "water_structure_other": data.get("select_one_water_structure_other") or "NA",
        "identified_by": data.get("select_one_identified") or "No Data Provided",
        "need_maintenance": data.get("select_one_maintenance") or "No Data Provided",
    }


def _extract_groundwater_fields(data):
    return {
        "beneficiary_settlement": data.get("beneficiary_settlement") or "NA",
        "block_name": data.get("block_name") or "NA",
        "work_type": data.get("TYPE_OF_WORK_ID") or "NA",
    }


def _extract_agri_fields(data):
    return {
        "beneficiary_settlement": data.get("beneficiary_settlement") or "0",
        "block_name": data.get("block_name") or "",
        "work_type": data.get("TYPE_OF_WORK_ID") or "",
    }


def _extract_livelihood_fields(data):
    return {
        "beneficiary_settlement": data.get("beneficiary_settlement") or "0",
        "block_name": data.get("block_name") or "0",
        "beneficiary_contact": data.get("Beneficiary_Contact_Number") or "0",
        "livestock_development": data.get("select_one_promoting_livestock") or "0",
        "fisheries": data.get("select_one_promoting_fisheries") or "0",
        "common_asset": data.get("select_one_common_asset") or "0",
    }


def _extract_crop_fields(data):
    def _crop_pattern(field, other_field):
        crops = data.get(field, "")
        if crops and "other" in crops.lower():
            other = data.get(other_field, "")
            if other:
                return f"{crops}: {other}"
        return crops or "NA"

    return {
        "beneficiary_settlement": data.get("beneficiary_settlement") or "NA",
        "irrigation_source": data.get("select_multiple_widgets") or "NA",
        "land_classification": data.get("select_one_classified") or "NA",
        "cropping_patterns_kharif": _crop_pattern(
            "select_multiple_cropping_kharif", "select_multiple_cropping_kharif_other"
        ),
        "cropping_patterns_rabi": _crop_pattern(
            "select_multiple_cropping_Rabi", "select_multiple_cropping_Rabi_other"
        ),
        "cropping_patterns_zaid": _crop_pattern(
            "select_multiple_cropping_Zaid", "select_multiple_cropping_Zaid_other"
        ),
        "agri_productivity": data.get("select_one_productivity") or "NA",
    }


MODEL_FIELD_EXTRACTORS = {
    ODK_settlement: _extract_settlement_fields,
    ODK_well: _extract_well_fields,
    ODK_waterbody: _extract_waterbody_fields,
    ODK_groundwater: _extract_groundwater_fields,
    ODK_agri: _extract_agri_fields,
    ODK_livelihood: _extract_livelihood_fields,
    ODK_crop: _extract_crop_fields,
}
