from dpr.models import (
    Agri_maintenance,
    GW_maintenance,
    ODK_waterbody,
    ODK_well,
    SWB_maintenance,
)
from dpr.utils import ensure_str
from utilities.logger import setup_logger

logger = setup_logger(__name__)

RECHARGE_STRUCTURE_MAPPING = {
    "select_one_check_dam": "Check dam",
    "select_one_percolation_tank": "Percolation tank",
    "select_one_earthen_gully_plug": "Earthen gully plug",
    "select_one_drainage_soakage_channels": "Drainage/soakage channels",
    "select_one_recharge_pits": "Recharge pits",
    "select_one_sokage_pits": "Sokage pits",
    "select_one_trench_cum_bund_network": "Trench cum bund network",
    "select_one_continuous_contour_trenches": "Continuous contour trenches (CCT)",
    "select_one_staggered_contour_trenches": "Staggered Contour trenches(SCT)",
    "select_one_water_absorption_trenches": "Water absorption trenches(WAT)",
    "select_one_loose_boulder_structure": "Loose boulder structure",
    "select_one_rock_fill_dam": "Rock fill dam",
    "select_one_stone_bunding": "Stone bunding",
    "select_one_diversion_drains": "Diversion drains",
    "select_one_bunding": "Bunding:Contour bunds/ graded bunds",
    "select_one_model5_structure": "5% model structure",
    "select_one_model30_40_structure": "30-40 model structure",
}

RECHARGE_STRUCTURE_REVERSE_MAPPING = {
    v: k for k, v in RECHARGE_STRUCTURE_MAPPING.items()
}

IRRIGATION_STRUCTURE_MAPPING = {
    "select_one_farm_pond": "Farm pond",
    "select_one_community_pond": "Community Pond",
    "select_one_well": "Well",
    "select_one_canal": "Canal",
    "select_one_farm_bund": "Farm bund",
}

IRRIGATION_STRUCTURE_REVERSE_MAPPING = {
    v: k for k, v in IRRIGATION_STRUCTURE_MAPPING.items()
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

WATER_STRUCTURE_REVERSE_MAPPING = {v: k for k, v in WATER_STRUCTURE_MAPPING.items()}

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
    v: k for k, v in RS_WATER_STRUCTURE_MAPPING.items()
}

STRUCTURE_TO_REPAIR_MAPPING = {
    "farm pond": "Repair_of_farm_ponds",
    "community pond": "Repair_of_community_pond",
    "large water body": "Repair_of_large_water_body",
    "large water bodies": "Repair_of_large_water_body",
    "canal": "Repair_of_canal",
    "check dam": "Repair_of_check_dam",
    "percolation tank": "Repair_of_percolation_tank",
    "earthen gully plug": "Repair_of_earthen_gully_plug",
    "earthern gully plugs": "Repair_of_earthen_gully_plug",
    "drainage/soakage channels": "Repair_of_drainage_soakage_channels",
    "recharge pits": "Repair_of_recharge_pits",
    "soakage pits": "Repair_of_soakage_pits",
    "sokage pits": "Repair_of_soakage_pits",
    "trench cum bund network": "Repair_of_trench_cum_bund_network",
    "continuous contour trenches (cct)": "Repair_of_Continuous_contour_trenches",
    "staggered contour trenches(sct)": "Repair_of_Staggered_contour_trenches",
    "water absorption trenches(wat)": "Repair_of_Water_absorption_trenches",
    "loose boulder structure": "Repair_of_loose_boulder_structure",
    "rock fill dam": "Repair_of_rock_fill_dam",
    "stone bunding": "Repair_of_stone_bunding",
    "diversion drains": "Repair_of_diversion_drains",
    "contour bunds/graded bunds": "Repair_of_bunding",
    "bunding:contour bunds/ graded bunds": "Repair_of_bunding",
    "farm bund": "Repair_of_farm_bund",
    "5% model structure": "Repair_of_model5_structure",
    "30-40 model": "Repair_of_30_40_model_structure",
    "30-40 model structure": "Repair_of_30_40_model_structure",
}

all_water_structures = [
    "Farm pond",
    "Canal",
    "Check dam",
    "Percolation Tank",
    "Earthern Gully plugs",
    "Drainage/Soakage channels",
    "Recharge pits",
    "Sokage pits",
    "Trench cum bund Network",
    "Large Water bodies",
    "Large Water Body",
    "Irrigation Channel",
    "Continuous contour trenches (CCT)",
    "Staggered Contour trenches(SCT)",
    "Water absorption trenches(WAT)",
    "Rock fill Dam",
    "Loose Boulder Structure",
    "Stone bunding",
    "Diversion drains",
    "Contour bunds/graded bunds",
    "Bunding:Contour bunds/ graded bunds",
    "Farm bund",
    "Well",
    "5% model structure",
    "30-40 Model",
    "Community pond",
]

recharge_structures = [
    "Check dam",
    "Percolation Tank",
    "Earthern Gully plugs",
    "Drainage/Soakage channels",
    "Recharge pits",
    "Sokage pits",
    "Trench cum bund Network",
    "Continuous contour trenches (CCT)",
    "Staggered Contour trenches(SCT)",
    "Water absorption trenches(WAT)",
    "Rock fill Dam",
    "Loose Boulder Structure",
    "Stone bunding",
    "Diversion drains",
    "Contour bunds/graded bunds",
    "Bunding:Contour bunds/ graded bunds",
    "5% model structure",
    "30-40 model structure",
]

irrigation_structures = ["Farm pond", "Canal", "Farm bund", "Well", "Community pond"]

surface_waterbodies = [
    "Farm pond",
    "Canal",
    "Check dam",
    "Percolation Tank",
    "Large Water bodies",
    "Large Water Body",
    "Irrigation Channel",
    "Rock fill Dam",
    "Loose Boulder Structure",
    "Community pond",
]

_COMMUNITY_DEMAND_VALUES = {
    "community",
    "community well",
    "community demand",
    "public",
    "public well",
    "shared among families",
}
_INDIVIDUAL_DEMAND_VALUES = {"private", "privately owned", "individual demand"}

DEMAND_TYPE_LABELS = {
    "community_demand": "Community Demand",
    "individual_demand": "Individual Demand",
}


def classify_demand_type_code(raw_value):
    """
    Normalize a raw ODK ownership value into the machine-readable demand-type
    code ("community_demand" / "individual_demand"), e.g. for form-label
    translation lookups keyed by that code. Returns raw_value unchanged if
    it isn't a recognized community/individual variant.
    """
    if not raw_value:
        return raw_value
    normalized = raw_value.strip().lower().replace("_", " ")
    if normalized in _COMMUNITY_DEMAND_VALUES:
        return "community_demand"
    if normalized in _INDIVIDUAL_DEMAND_VALUES:
        return "individual_demand"
    return raw_value


def classify_demand_type(raw_value):
    """Return the display-ready demand-type label (e.g. "Community Demand")."""
    code = classify_demand_type_code(raw_value)
    return DEMAND_TYPE_LABELS.get(code, code)


def _extract_repair_value(section, key, other_key):
    """
    Read a repair-activity choice from an ODK data section, resolving the
    "other" free-text follow-up when present. Returns None if the section
    doesn't have a value for `key` at all.
    """
    value = ensure_str(section.get(key))
    if not value:
        return None
    if value.lower() == "other":
        other_value = section.get(other_key)
        return other_value if other_value else value
    return value


def get_activity_type_from_waterbody(waterbody):
    """
    Extract the activity type VALUE from waterbody based on structure type and data_waterbody content.

    Args:
        waterbody: ODK_waterbody instance

    Returns:
        str: The value of the appropriate activity type (e.g., "yes", "no", etc.) or 'Maintenance'
    """
    structure_type = waterbody.water_structure_type.lower().strip()
    data = waterbody.data_waterbody

    expected_repair_key = STRUCTURE_TO_REPAIR_MAPPING.get(structure_type)
    if expected_repair_key:
        repair_value = _extract_repair_value(
            data, expected_repair_key, f"{expected_repair_key}_other"
        )
        if repair_value:
            return repair_value

    return "Maintenance"


def get_activity_type_from_well(well):
    """
    Extract the activity type VALUE from well based on data_well content.
    Checks Well_usage.repairs_type first (the field the ODK form primarily
    populates), then falls back to Well_condition.select_one_repairs_well.

    Args:
        well: ODK_well instance

    Returns:
        str: The value of the repair activity or 'Maintenance'
    """
    data = well.data_well

    well_usage = data.get("Well_usage", {})
    repair_value = _extract_repair_value(
        well_usage, "repairs_type", "repairs_type_other"
    )
    if repair_value:
        return repair_value

    well_condition = data.get("Well_condition", {})
    repair_value = _extract_repair_value(
        well_condition, "select_one_repairs_well", "select_one_repairs_well_other"
    )
    if repair_value:
        return repair_value

    return "Maintenance"


def _build_modified_data(**raw_values):
    """Keep only the raw (pre-transform) values that were actually present."""
    return {k: v for k, v in raw_values.items() if v is not None}


def _preserve_source_modified_data(instance, modified_data_field, raw_values):
    """
    Snapshot raw pre-transform values (e.g. the raw select_one_owns/repair-
    activity choice) onto the ODK_waterbody/ODK_well record itself -- that's
    where this data actually originates; the maintenance record is just a
    derived copy of it. Only fills in keys not already preserved, and never
    overwrites an existing snapshot.
    """
    modified_data = getattr(instance, modified_data_field) or {}
    changed = False
    for key, value in raw_values.items():
        if key not in modified_data:
            modified_data[key] = value
            changed = True

    if not changed:
        return

    setattr(instance, modified_data_field, modified_data)
    instance.save(update_fields=[modified_data_field])


def _sync_maintenance_activity(
    existing, data_field, modified_data_field, common_data, common_modified_data
):
    """
    Refresh the derived (demand_type, select_one_activities) values on an
    already-created maintenance record, so mapping/classification fixes made
    after the record was created (e.g. a corrected repair-activity lookup)
    still reach it on the next regeneration. Records already moderated or
    soft-deleted are left untouched.
    """
    if existing.is_moderated or existing.is_deleted:
        return False

    data = getattr(existing, data_field) or {}
    changed = False
    for key in ("demand_type", "select_one_activities"):
        if data.get(key) != common_data[key]:
            data[key] = common_data[key]
            changed = True

    if not changed:
        return False

    modified_data = getattr(existing, modified_data_field) or {}
    modified_data.update(common_modified_data)

    setattr(existing, data_field, data)
    setattr(existing, modified_data_field, modified_data)
    existing.save(update_fields=[data_field, modified_data_field])
    print(f"Refreshed {data_field} for existing record {existing.pk}")
    return True


# MARK: Water Structures Data and Well Data
def populate_maintenance_from_waterbody(plan, create_missing=True):
    """
    Filter ODK_waterbody and ODK_well records by water structure type and populate the appropriate maintenance tables
    (GW_maintenance, Agri_maintenance, SWB_maintenance) based on the structure type.

    Does the same for wells maintenance -- populating the irrigation table

    Args:
        plan: Plan object containing plan details
        create_missing: when False, only refresh derived fields on maintenance
            records that already exist -- never create new ones. Used by the
            backfill_maintenance_activity management command to safely re-sync
            historical data without generating Section E for plans that never
            had it.

    Returns:
        dict with "created" and "refreshed" counts.
    """
    stats = {"created": 0, "refreshed": 0}
    # Get all waterbody records for the plan
    waterbodies = ODK_waterbody.objects.filter(plan_id=plan.id).exclude(
        status_re="rejected"
    )
    wells = ODK_well.objects.filter(plan_id=plan.id).exclude(status_re="rejected")
    print(f"Found {waterbodies.count()} waterbody records for plan {plan.id}")
    print(f"Found {wells.count()} well records for plan {plan.id}")

    for waterbody in waterbodies:
        structure_type = waterbody.water_structure_type

        # Skip if no maintenance needed
        if waterbody.need_maintenance.lower() != "yes":
            continue

        # Get the dynamic activity type VALUE based on structure type and data_waterbody
        activity_type = get_activity_type_from_waterbody(waterbody)
        raw_owner = waterbody.data_waterbody.get("select_one_owns")

        common_data = {
            "demand_type": classify_demand_type(raw_owner),
            "beneficiary_settlement": waterbody.beneficiary_settlement,
            "Beneficiary_Name": waterbody.data_waterbody.get("Beneficiary_name"),
            "ben_father": waterbody.data_waterbody.get("ben_father"),
            # Keep the raw (underscore-code) activity value here -- translation
            # lookups need the original ODK choice code. format_text() is only
            # applied at display time, after translation has been attempted.
            "select_one_activities": activity_type,
        }
        # Preserve the raw values before classify_demand_type() normalizes them
        common_modified_data = _build_modified_data(
            demand_type=raw_owner,
            select_one_activities=activity_type,
        )
        _preserve_source_modified_data(
            waterbody, "modified_data_waterbody", common_modified_data
        )

        work_id = waterbody.waterbody_id
        print("Work ID:", work_id)

        structure_type_lower = structure_type.lower()
        recharge_structures_lower = [s.lower() for s in recharge_structures]
        irrigation_structures_lower = [s.lower() for s in irrigation_structures]
        surface_waterbodies_lower = [s.lower() for s in surface_waterbodies]

        if structure_type_lower in recharge_structures_lower:
            existing = (
                GW_maintenance.objects.filter(
                    plan_id=plan.id,
                    work_id=work_id,
                )
                .exclude(status_re="rejected")
                .first()
            )

            print(
                f"GW Maintenance - Existing record check: {'Found' if existing else 'Not found'}"
            )

            if not existing:
                if create_missing:
                    maintenance_data = common_data.copy()
                    maintenance_data["select_one_recharge_structure"] = structure_type

                    GW_maintenance.objects.create(
                        uuid=waterbody.uuid,
                        plan_id=plan.id,
                        plan_name=plan.plan,
                        latitude=waterbody.latitude,
                        longitude=waterbody.longitude,
                        status_re=waterbody.status_re,
                        work_id=work_id,
                        corresponding_work_id=waterbody.waterbody_id,
                        data_gw_maintenance=maintenance_data,
                        modified_data_gw_maintenance=common_modified_data,
                    )
                    stats["created"] += 1
                    print(f"GW Maintenance record created successfully for {work_id}")
            elif _sync_maintenance_activity(
                existing,
                "data_gw_maintenance",
                "modified_data_gw_maintenance",
                common_data,
                common_modified_data,
            ):
                stats["refreshed"] += 1

        elif structure_type_lower in irrigation_structures_lower:
            existing = (
                Agri_maintenance.objects.filter(
                    plan_id=plan.id,
                    work_id=work_id,
                )
                .exclude(status_re="rejected")
                .first()
            )

            print(
                f"Agri Maintenance - Existing record check: {'Found' if existing else 'Not found'}"
            )

            if not existing:
                if create_missing:
                    maintenance_data = common_data.copy()
                    maintenance_data["select_one_irrigation_structure"] = structure_type

                    print(f"Creating Agri Maintenance record for {work_id}")
                    print(f"Maintenance data: {maintenance_data}")
                    print(f"Plan name: {plan.plan}")

                    Agri_maintenance.objects.create(
                        uuid=waterbody.uuid,
                        plan_id=plan.id,
                        plan_name=plan.plan,
                        latitude=waterbody.latitude,
                        longitude=waterbody.longitude,
                        status_re=waterbody.status_re,
                        work_id=work_id,
                        corresponding_work_id=waterbody.waterbody_id,
                        data_agri_maintenance=maintenance_data,
                        modified_data_agri_maintenance=common_modified_data,
                    )
                    stats["created"] += 1
                    print(f"Agri Maintenance record created successfully for {work_id}")
            elif _sync_maintenance_activity(
                existing,
                "data_agri_maintenance",
                "modified_data_agri_maintenance",
                common_data,
                common_modified_data,
            ):
                stats["refreshed"] += 1

        elif structure_type_lower in surface_waterbodies_lower:
            existing = SWB_maintenance.objects.filter(
                plan_id=plan.id,
                work_id=work_id,
            ).first()

            print(
                f"SWB Maintenance - Existing record check: {'Found' if existing else 'Not found'}"
            )

            if not existing:
                if create_missing:
                    maintenance_data = common_data.copy()
                    maintenance_data["TYPE_OF_WORK"] = structure_type

                    print(f"Creating SWB Maintenance record for {work_id}")
                    print(f"Maintenance data: {maintenance_data}")

                    SWB_maintenance.objects.create(
                        uuid=waterbody.uuid,
                        plan_id=plan.id,
                        plan_name=plan.plan,
                        latitude=waterbody.latitude,
                        longitude=waterbody.longitude,
                        status_re=waterbody.status_re,
                        work_id=work_id,
                        corresponding_work_id=waterbody.waterbody_id,
                        data_swb_maintenance=maintenance_data,
                        modified_data_swb_maintenance=common_modified_data,
                    )
                    stats["created"] += 1
                    print(f"SWB Maintenance record created successfully for {work_id}")
            elif _sync_maintenance_activity(
                existing,
                "data_swb_maintenance",
                "modified_data_swb_maintenance",
                common_data,
                common_modified_data,
            ):
                stats["refreshed"] += 1

    for well in wells:
        if well.need_maintenance.lower() != "yes":
            continue

        # Get the dynamic activity type VALUE from well data
        activity_type = get_activity_type_from_well(well)
        raw_owner = well.data_well.get("select_one_owns")

        common_data = {
            "demand_type": classify_demand_type(raw_owner),
            "beneficiary_settlement": well.beneficiary_settlement,
            "Beneficiary_Name": well.data_well.get("Beneficiary_name"),
            "ben_father": well.data_well.get("ben_father"),
            # Raw (underscore-code) value -- see comment in the waterbody loop above.
            "select_one_activities": activity_type,
        }
        common_modified_data = _build_modified_data(
            demand_type=raw_owner,
            select_one_activities=activity_type,
        )
        _preserve_source_modified_data(well, "modified_data_well", common_modified_data)

        well_id = well.well_id

        existing_well = (
            Agri_maintenance.objects.filter(
                plan_id=plan.id,
                work_id=well_id,
            )
            .exclude(status_re="rejected")
            .first()
        )

        if not existing_well:
            if create_missing:
                maintenance_data = common_data.copy()
                maintenance_data["select_one_irrigation_structure"] = "Well"

                Agri_maintenance.objects.create(
                    uuid=well.uuid,
                    plan_id=plan.id,
                    plan_name=plan.plan,
                    latitude=well.latitude,
                    longitude=well.longitude,
                    status_re=well.status_re,
                    work_id=well_id,
                    corresponding_work_id=well.well_id,
                    data_agri_maintenance=maintenance_data,
                    modified_data_agri_maintenance=common_modified_data,
                )
                stats["created"] += 1
                print(f"Well maintenance record created successfully for {well_id}")
        else:
            print(f"Well maintenance record already exists for {well_id}")
            if _sync_maintenance_activity(
                existing_well,
                "data_agri_maintenance",
                "modified_data_agri_maintenance",
                common_data,
                common_modified_data,
            ):
                stats["refreshed"] += 1

    print("Maintenance records created successfully")
    return stats


# yuktdhara kml icon mapping
ICON_MAP = {
    "Farm pond": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_farm_pond.png",
    "Community Pond": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_community_pond.png",
    "Large water body": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_large_water_body.png",
    "Canal": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_canal.png",
    "Check dam": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_check_dam.png",
    "Percolation tank": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_percolation_tank.png",
    "Earthen gully plug": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_earthen_gully_plug.png",
    "Drainage/soakage channels": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_drainage_soakage_channels.png",
    "Recharge pits": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_recharge_pits.png",
    "Soakage pits": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_soakage_pits.png",
    "Trench cum bund network": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_trench_cum_bund_network.png",
    "Continuous contour trenches (CCT)": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_continuous_contour_trenches_cct.png",
    "Staggered Contour trenches(SCT)": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_staggered_contour_trenches_sct.png",
    "Water absorption trenches(WAT)": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_water_absorption_trenches_wat.png",
    "Loose boulder structure": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_loose_boulder_structure.png",
    "Rock fill dam": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_rock_fill_dam.png",
    "Stone bunding": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_stone_bunding.png",
    "Bunding:Contour bunds/ graded bunds": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_bunding_contour_bunds_graded_bunds.png",
    "Farm bund": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_farm_bund.png",
    "5% model structure": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_5_model_structure.png",
    "30-40 model structure": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_30-40_model_structure.png",
    "Farm pond": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_farm_pond.png",
    "Community Pond": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_community_pond.png",
    "Large water body": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_large_water_body.png",
    "Canal": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_canal.png",
    "Check dam": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_check_dam.png",
    "Percolation tank": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_percolation_tank.png",
    "Earthen gully plug": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_earthen_gully_plug.png",
    "Drainage/soakage channels": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_drainage_soakage_channels.png",
    "Recharge pits": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_recharge_pits.png",
    "Soakage pits": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_soakage_pits.png",
    "Trench cum bund network": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_trench_cum_bund_network.png",
    "Continuous contour trenches (CCT)": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_continuous_contour_trenches_cct.png",
    "Staggered Contour trenches(SCT)": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_staggered_contour_trenches_sct.png",
    "Water absorption trenches(WAT)": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_water_absorption_trenches_wat.png",
    "Bunding:Contour bunds/ graded bunds": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_bunding_contour_bunds_graded_bunds.png",
    "Farm bund": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_farm_bund.png",
    "5% model structure": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_5_model_structure.png",
    "30-40 model structure": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/wb_icons_maintenance_30-40_model_structure.png",
    "Check dam": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_check_dam.png",
    "Percolation tank": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_percolation_tank.png",
    "Earthen gully plug": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_earthen_gully_plug.png",
    "Drainage/soakage channels": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_drainage_soakage_channels.png",
    "Recharge pits": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_recharge_pits.png",
    "Sokage pits": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_sokage_pits.png",
    "Trench cum bund network": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_trench_cum_bund_network.png",
    "Continuous contour trenches (CCT)": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_continuous_contour_trenches_cct.png",
    "Staggered Contour trenches(SCT)": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_staggered_contour_trenches_sct.png",
    "Water absorption trenches(WAT)": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_water_absorption_trenches_wat.png",
    "Loose boulder structure": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_loose_boulder_structure.png",
    "Rock fill dam": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_rock_fill_dam.png",
    "Stone bunding": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_stone_bunding.png",
    "Bunding:Contour bunds/ graded bunds": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_bunding_contour_bunds_graded_bunds.png",
    "5% model structure": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_5_model_structure.png",
    "30-40 model structure": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/recharge_icons_30-40_model_structure.png",
    "approved": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/socialmapping_icons_well_approved.png",
    "rejected": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/socialmapping_icons_well_rejected.png",
    "proposed": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/socialmapping_icons_well_proposed.png",
    "maintenance": "https://raw.githubusercontent.com/core-stack-org/core-stack-backend/refs/heads/main/dpr/icons/socialmapping_icons_well_maintenance.png",
}
