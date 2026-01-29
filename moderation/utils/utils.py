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
    # Agrohorticulture,
)
from dpr.utils import determine_caste_fields


def sync_edited_updated_settlement(sub):
    system = sub.get("__system", {})
    gps = sub.get("GPS_point", {})
    meta = sub.get("meta", {})
    nrega = sub.get("MNREGA_INFORMATION", {})
    farmer_family = sub.get("farmer_family", {})
    Livestock_Census = sub.get("Livestock_Census", {})
    lat = None
    lon = None
    try:
        coords = gps.get("point_mapsappearance", {}).get("coordinates", [])
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    except Exception:
        pass
    largest_caste, smallest_caste, settlement_status, use_existing_logic = (
        determine_caste_fields(sub)
    )
    mapped = {
        "settlement_name": sub.get("Settlements_name"),
        "submission_time": system.get("submissionDate"),
        "submitted_by": system.get("submitterName"),
        "status_re": system.get("reviewState"),
        "latitude": lat,
        "longitude": lon,
        "block_name": sub.get("block_name"),
        "number_of_households": sub.get("number_households"),
        "largest_caste": largest_caste,
        "smallest_caste": smallest_caste,
        "settlement_status": settlement_status,
        "plan_id": sub.get("plan_id"),
        "plan_name": sub.get("plan_name"),
        "uuid": sub.get("__id"),
        "system": system,
        "gps_point": gps,
        "farmer_family": farmer_family,
        "livestock_census": Livestock_Census,
        "nrega_job_aware": nrega.get("NREGA_aware", 0),
        "nrega_job_applied": nrega.get("NREGA_applied", 0),
        "nrega_job_card": nrega.get("NREGA_have_job_card", 0),
        "nrega_without_job_card": sub.get("number_households", 0),
        "nrega_work_days": nrega.get("NREGA_work_days", 0),
        "nrega_past_work": nrega.get("NREGA_past_work", 0) or "NA",
        "nrega_raise_demand": nrega.get("NREGA_raise_demand"),
        "nrega_demand": nrega.get("work_demands"),
        "nrega_issues": nrega.get("select_multiple_issues"),
        "nrega_community": nrega.get("select_one_contributions"),
        "data_settlement": sub,
    }

    # Save using update_or_create
    ODK_settlement.objects.update_or_create(
        settlement_id=sub.get("Settlements_id"), defaults=mapped
    )


def sync_edited_updated_well(well_submission):
    Well_usage = well_submission.get("Well_usage", {})
    gps = well_submission.get("GPS_point", {})
    Well_condition = well_submission.get("Well_condition", {})
    system = well_submission.get("__system", {})
    lat = None
    lon = None
    try:
        coords = gps.get("point_mapsappearance", {}).get("coordinates", [])
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    except Exception:
        pass
    mapped = {
        "uuid": well_submission.get("__id"),
        "submission_time": system.get("submissionDate"),
        "beneficiary_settlement": well_submission.get("beneficiary_settlement"),
        "block_name": well_submission.get("block_name"),
        "owner": well_submission.get("select_one_owns"),
        "households_benefitted": well_submission.get("households_benefited"),
        "caste_uses": well_submission.get("select_multiple_caste_use"),
        "is_functional": Well_usage.get("select_one_Functional_Non_functional"),
        "need_maintenance": Well_usage.get("is_maintenance_required"),
        "plan_id": well_submission.get("plan_id"),
        "plan_name": well_submission.get("plan_name"),
        "status_re": system.get("reviewState"),
        "latitude": lat,
        "longitude": lon,
        "system": system,
        "gps_point": gps,
        "data_well": well_submission,
    }

    ODK_well.objects.update_or_create(
        well_id=well_submission.get("well_id"), defaults=mapped
    )


def sync_edited_updated_waterbody(waterbody_submission):
    gps = waterbody_submission.get("GPS_point", {})
    system = waterbody_submission.get("__system", {})
    lat = None
    lon = None
    try:
        coords = gps.get("point_mapsappearance", {}).get("coordinates", [])
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    except Exception:
        pass
    mapped = {
        "uuid": waterbody_submission.get("__id"),
        "submission_time": system.get("submissionDate"),
        "block_name": waterbody_submission.get("block_name"),
        "beneficiary_settlement": waterbody_submission.get("beneficiary_settlement"),
        "beneficiary_contact": waterbody_submission.get("Beneficiary_contact_number"),
        "who_manages": waterbody_submission.get("select_one_manages"),
        "specify_other_manager": waterbody_submission.get("text_one_manages"),
        "owner": waterbody_submission.get("select_one_owns"),
        "caste_who_uses": waterbody_submission.get("select_multiple_caste_use"),
        "household_benefitted": waterbody_submission.get("households_benefited"),
        "water_structure_type": waterbody_submission.get("select_one_water_structure"),
        "water_structure_other": waterbody_submission.get(
            "select_one_water_structure_other"
        ),
        "need_maintenance": waterbody_submission.get("select_one_maintenance"),
        "plan_id": waterbody_submission.get("plan_id"),
        "plan_name": waterbody_submission.get("plan_name"),
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState"),
        "system": system,
        "gps_point": gps,
        "data_waterbody": waterbody_submission,
    }

    ODK_waterbody.objects.update_or_create(
        waterbody_id=waterbody_submission.get("waterbodies_id"), defaults=mapped
    )


def sync_edited_updated_gw(gw_submissions):
    gps = gw_submissions.get("GPS_point", {})
    system = gw_submissions.get("__system", {})
    lat = None
    lon = None
    try:
        coords = gps.get("point_mapsappearance", {}).get("coordinates", [])
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    except Exception:
        pass

    mapped = {
        "uuid": gw_submissions.get("__id"),
        "submission_time": system.get("submissionDate"),
        "beneficiary_settlement": gw_submissions.get("beneficiary_settlement"),
        "block_name": gw_submissions.get("block_name"),
        "work_type": gw_submissions.get("TYPE_OF_WORK_ID"),
        "plan_id": gw_submissions.get("plan_id"),
        "plan_name": gw_submissions.get("plan_name"),
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState"),
        "system": system,
        "gps_point": gps,
        "work_dimensions": "",
        "data_groundwater": gw_submissions,
    }

    ODK_groundwater.objects.update_or_create(
        recharge_structure_id=gw_submissions.get("work_id"), defaults=mapped
    )


def sync_edited_updated_agri(agri_submission):
    gps = agri_submission.get("GPS_point", {})
    system = agri_submission.get("__system", {})
    lat = None
    lon = None
    try:
        coords = gps.get("point_mapsappearance", {}).get("coordinates", [])
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    except Exception:
        pass

    mapped = {
        "uuid": agri_submission.get("__id"),
        "submission_time": system.get("submissionDate"),
        "beneficiary_settlement": agri_submission.get("beneficiary_settlement"),
        "block_name": agri_submission.get("block_name"),
        "work_type": agri_submission.get("TYPE_OF_WORK_ID"),
        "plan_id": agri_submission.get("plan_id"),
        "plan_name": agri_submission.get("plan_name"),
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState"),
        "system": system,
        "gps_point": gps,
        "work_dimensions": "",
        "data_agri": agri_submission,
    }

    ODK_agri.objects.update_or_create(
        irrigation_work_id=agri_submission.get("work_id"), defaults=mapped
    )


def sync_edited_updated_livelihhod(livelihood_submission):
    gps = livelihood_submission.get("GPS_point", {})
    system = livelihood_submission.get("__system", {})
    lat = None
    lon = None
    try:
        coords = gps.get("point_mapsappearance", {}).get("coordinates", [])
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    except Exception:
        pass

    mapped = {
        "uuid": livelihood_submission.get("__id"),
        "beneficiary_settlement": livelihood_submission.get("beneficiary_settlement"),
        "block_name": livelihood_submission.get("block_name"),
        "beneficiary_contact": "",
        "livestock_development": livelihood_submission.get(
            "select_one_promoting_livestock "
        ),
        "submission_time": system.get("submissionDate"),
        "fisheries": livelihood_submission.get("select_one_promoting_fisheries"),
        "common_asset": "",
        "plan_id": livelihood_submission.get("plan_id"),
        "plan_name": livelihood_submission.get("plan_name"),
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState"),
        "system": system,
        "gps_point": gps,
        "data_livelihood": livelihood_submission,
    }
    ODK_livelihood.objects.update_or_create(
        livelihood_id=livelihood_submission.get("work_id"), defaults=mapped
    )


def sync_edited_updated_cropping_pattern(cp_submission):
    system = cp_submission.get("__system", {})
    uuid = cp_submission.get("__id")
    crop_grid_id = cp_submission.get("crop_Grid_id")
    if not crop_grid_id:
        crop_grid_id = uuid
    mapped = {
        "uuid": uuid,
        "beneficiary_settlement": cp_submission.get("beneficiary_settlement"),
        "irrigation_source": cp_submission.get("select_multiple_widgets"),
        "submission_time": system.get("submissionDate"),
        "land_classification": cp_submission.get("select_one_classified"),
        "cropping_patterns_kharif": cp_submission.get(
            "select_multiple_cropping_kharif"
        ),
        "cropping_patterns_rabi": cp_submission.get("select_multiple_cropping_Rabi"),
        "cropping_patterns_zaid": cp_submission.get("select_multiple_cropping_Zaid"),
        "agri_productivity": cp_submission.get("select_one_productivity"),
        "plan_id": cp_submission.get("plan_id"),
        "plan_name": cp_submission.get("plan_name"),
        "status_re": system.get("reviewState"),
        "system": system,
        "data_crop": cp_submission,
    }
    ODK_crop.objects.update_or_create(crop_grid_id=crop_grid_id, defaults=mapped)


def sync_edited_updated_agri_maintenance(am_submission):
    gps = am_submission.get("GPS_point", {})
    system = am_submission.get("__system", {})
    lat = None
    lon = None
    try:
        coords = gps.get("point_mapsappearance", {}).get("coordinates", [])
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    except Exception:
        pass
    work_id = am_submission.get("work_id")
    mapped = {
        "uuid": am_submission.get("__id"),
        "plan_id": am_submission.get("plan_id"),
        "plan_name": am_submission.get("plan_name"),
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState"),
        "work_id": work_id,
        "corresponding_work_id": am_submission.get("corresponding_work_id"),
        "data_agri_maintenance": am_submission,
    }
    Agri_maintenance.objects.update_or_create(work_id=work_id, defaults=mapped)


def sync_edited_updated_gw_maintenance(gwm_submission):
    gps = gwm_submission.get("GPS_point", {})
    system = gwm_submission.get("__system", {})
    lat = None
    lon = None
    try:
        coords = gps.get("point_mapsappearance", {}).get("coordinates", [])
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    except Exception:
        pass
    work_id = gwm_submission.get("work_id")
    mapped = {
        "uuid": gwm_submission.get("__id"),
        "plan_id": gwm_submission.get("plan_id"),
        "plan_name": gwm_submission.get("plan_name"),
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState"),
        "work_id": work_id,
        "corresponding_work_id": gwm_submission.get("corresponding_work_id"),
        "data_gw_maintenance": gwm_submission,
    }
    # need to ask id
    GW_maintenance.objects.update_or_create(work_id=work_id, defaults=mapped)


def sync_edited_updated_swb_maintenance(swbm_submission):
    gps = swbm_submission.get("GPS_point", {})
    system = swbm_submission.get("__system", {})
    lat = None
    lon = None
    try:
        coords = gps.get("point_mapsappearance", {}).get("coordinates", [])
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    except Exception:
        pass
    work_id = swbm_submission.get("work_id")
    mapped = {
        "uuid": swbm_submission.get("__id"),
        "plan_id": swbm_submission.get("plan_id"),
        "plan_name": swbm_submission.get("plan_name"),
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState"),
        "work_id": work_id,
        "corresponding_work_id": swbm_submission.get("corresponding_work_id"),
        "data_swb_maintenance": swbm_submission,
    }
    SWB_maintenance.objects.update_or_create(work_id=work_id, defaults=mapped)


def sync_edited_updated_swb_rs_maintenance(swb_rs_submission):
    gps = swb_rs_submission.get("GPS_point", {})
    system = swb_rs_submission.get("__system", {})
    lat = None
    lon = None
    try:
        coords = gps.get("point_mapsappearance", {}).get("coordinates", [])
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    except Exception:
        pass
    work_id = swb_rs_submission.get("work_id")
    mapped = {
        "uuid": swb_rs_submission.get("__id"),
        "plan_id": swb_rs_submission.get("plan_id"),
        "plan_name": swb_rs_submission.get("plan_name"),
        "latitude": lat,
        "longitude": lon,
        "status_re": system.get("reviewState"),
        "work_id": work_id,
        "corresponding_work_id": swb_rs_submission.get("corresponding_work_id"),
        "data_swb_rs_maintenance": swb_rs_submission,
    }
    SWB_RS_maintenance.objects.update_or_create(work_id=work_id, defaults=mapped)


def sync_edited_updated_agrohorticulture(agrohorticulture_submission):
    pass
