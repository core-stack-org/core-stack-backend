from dpr.models import (
    Agri_maintenance,
    GW_maintenance,
    ODK_waterbody,
    ODK_well,
    SWB_maintenance,
)
from dpr.utils import format_text
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
    print(f"Expected repair key: {expected_repair_key}")

    if expected_repair_key:
        repair_value = data.get(expected_repair_key)

        if repair_value and repair_value.lower() == "other":
            other_value = data.get(f"{expected_repair_key}_other")
            if other_value:
                print(
                    f"Found repair activity '{expected_repair_key}' with value '{other_value}' for waterbody {waterbody.waterbody_id}"
                )
                return other_value
            else:
                print(
                    f"Repair activity '{expected_repair_key}' is 'other' but no 'other' value specified for waterbody {waterbody.waterbody_id}"
                )
        elif repair_value:
            print(
                f"Found repair activity '{expected_repair_key}' with value '{repair_value}' for waterbody {waterbody.waterbody_id}"
            )
            return repair_value
        else:
            print(
                f"Repair activity '{expected_repair_key}' is not 'other' but no value specified for waterbody {waterbody.waterbody_id}"
            )
            return "Maintenance"


def get_activity_type_from_well(well):
    """
    Extract the activity type VALUE from well based on data_well content.

    Args:
        well: ODK_well instance

    Returns:
        str: The value of the repair activity or 'Maintenance'
    """
    data = well.data_well

    # Navigate to the Well_condition section
    well_condition = data.get("Well_condition", {})

    repair_type = well_condition.get("select_one_repairs_well")
    print(f"Repair type well: {repair_type}")

    if repair_type:
        if repair_type and repair_type.lower() == "other":
            other_value = well_condition.get("select_one_repairs_well_other")
            if other_value:
                print(
                    f"Found well repair type 'other' with value '{other_value}' for well {well.well_id}"
                )
                return other_value
            else:
                print(
                    f"Well repair type is 'other' but no 'other' value specified for well {well.well_id}"
                )
                return repair_type
        else:
            print(f"Found well repair type '{repair_type}' for well {well.well_id}")
            return repair_type

    print(f"No repair type found for well {well.well_id}, using 'Maintenance'")
    return "Maintenance"


def populate_maintenance_from_waterbody(plan):
    """
    Filter ODK_waterbody records by water structure type and populate the appropriate maintenance tables
    (GW_maintenance, Agri_maintenance, SWB_maintenance) based on the structure type.

    Does the same for wells maintenance -- populating the irrigation table

    Args:
        plan: Plan object containing plan details
    """
    # Get all waterbody records for the plan
    waterbodies = ODK_waterbody.objects.filter(plan_id=plan.id).exclude(
        status_re="rejected"
    )
    wells = ODK_well.objects.filter(plan_id=plan.id).exclude(status_re="rejected")
    print(f"Found {waterbodies.count()} waterbody records for plan {plan.id}")

    for waterbody in waterbodies:
        structure_type = waterbody.water_structure_type

        # Skip if no maintenance needed
        if waterbody.need_maintenance.lower() != "yes":
            continue

        # Get the dynamic activity type VALUE based on structure type and data_waterbody
        activity_type = get_activity_type_from_waterbody(waterbody)

        common_data = {
            "beneficiary_settlement": waterbody.beneficiary_settlement,
            "Beneficiary_Name": waterbody.data_waterbody.get("Beneficiary_name"),
            "select_one_activities": format_text(activity_type),
        }

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
                maintenance_data = common_data.copy()
                maintenance_data["select_one_water_structure"] = structure_type

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
                )
                print(f"GW Maintenance record created successfully for {work_id}")

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
                )
                print(f"Agri Maintenance record created successfully for {work_id}")

        elif structure_type_lower in surface_waterbodies_lower:
            existing = SWB_maintenance.objects.filter(
                plan_id=plan.id,
                work_id=work_id,
            ).first()

            print(
                f"SWB Maintenance - Existing record check: {'Found' if existing else 'Not found'}"
            )

            if not existing:
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
                )
                print(f"SWB Maintenance record created successfully for {work_id}")

    for well in wells:
        if well.need_maintenance.lower() != "yes":
            continue

        # Get the dynamic activity type VALUE from well data
        activity_type = get_activity_type_from_well(well)

        common_data = {
            "beneficiary_settlement": well.beneficiary_settlement,
            "Beneficiary_Name": well.data_well.get("Beneficiary_name"),
            "select_one_activities": format_text(activity_type),
        }

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
            )
            print(f"Well maintenance record created successfully for {well_id}")
        else:
            print(f"Well maintenance record already exists for {well_id}")

    print("Maintenance records created successfully")
