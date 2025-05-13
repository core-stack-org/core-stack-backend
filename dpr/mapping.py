from dpr.models import (
    ODK_waterbody,
    GW_maintenance,
    Agri_maintenance,
    SWB_maintenance,
)
from utilities.logger import setup_logger

logger = setup_logger(__name__)

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
    "Contour bunds/graded bunds",  # Under "Bunding:"
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
    "Community pond"
]


def populate_maintenance_from_waterbody(plan):
    """
    Filter ODK_waterbody records by water structure type and populate the appropriate maintenance tables
    (GW_maintenance, Agri_maintenance, SWB_maintenance) based on the structure type.

    Args:
        plan: Plan object containing plan details
    """
    # Get all waterbody records for the plan
    waterbodies = ODK_waterbody.objects.filter(plan_id=plan.plan_id)
    print(f"Found {waterbodies.count()} waterbody records for plan {plan.plan_id}")

    for waterbody in waterbodies:
        print("*******************  JUST FOR A CHECK  **********************")
        print("Waterbody:", waterbody)
        structure_type = waterbody.water_structure_type
        print("Water Structure Type:", structure_type)

        # Skip if no maintenance needed
        print("Maintenance needed:", waterbody.need_maintenance)
        if waterbody.need_maintenance.lower() != "yes":
            continue

        common_data = {
            "beneficiary_settlement": waterbody.beneficiary_settlement,
            "Beneficiary_Name": waterbody.data_waterbody.get("Beneficiary_name"),
            "select_one_activities": "Maintenance",
        }


        work_id = waterbody.waterbody_id
        print("Work ID:", work_id)


        structure_type_lower = structure_type.lower()
        recharge_structures_lower = [s.lower() for s in recharge_structures]
        irrigation_structures_lower = [s.lower() for s in irrigation_structures]
        surface_waterbodies_lower = [s.lower() for s in surface_waterbodies]

        if structure_type_lower in recharge_structures_lower:
            existing = GW_maintenance.objects.filter(
                plan_id=plan.plan_id,
                work_id=work_id,
            ).first()

            print(
                f"GW Maintenance - Existing record check: {'Found' if existing else 'Not found'}"
            )

            if not existing:
                maintenance_data = common_data.copy()
                maintenance_data["select_one_water_structure"] = structure_type

                print(f"Creating GW Maintenance record for {work_id}")
                print(f"Maintenance data: {maintenance_data}")

                GW_maintenance.objects.create(
                    uuid=waterbody.uuid,
                    plan_id=plan.plan_id,
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
            existing = Agri_maintenance.objects.filter(
                plan_id=plan.plan_id,
                work_id=work_id,
            ).first()

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
                    plan_id=plan.plan_id,
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
                plan_id=plan.plan_id,
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
                    plan_id=plan.plan_id,
                    plan_name=plan.plan,
                    latitude=waterbody.latitude,
                    longitude=waterbody.longitude,
                    status_re=waterbody.status_re,
                    work_id=work_id,
                    corresponding_work_id=waterbody.waterbody_id,
                    data_swb_maintenance=maintenance_data,
                )
                print(f"SWB Maintenance record created successfully for {work_id}")
