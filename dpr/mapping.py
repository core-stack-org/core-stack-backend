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
    "Irrigation Channel",
    "Continuous contour trenches (CCT)",
    "Staggered Contour trenches(SCT)",
    "Water absorption trenches(WAT)",
    "Rock fill Dam",
    "Loose Boulder Structure",
    "Stone bunding",
    "Diversion drains",
    "Contour bunds/graded bunds",  # Under "Bunding:"
    "Farm bund",
    "Well",
    "5% Model",
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
    "5% Model",
    "30-40 Model",
]

irrigation_structures = ["Farm pond", "Canal", "Farm bund", "Well", "Community pond"]

surface_waterbodies = [
    "Farm pond",
    "Canal",
    "Check dam",
    "Percolation Tank",
    "Large Water bodies",
    "Irrigation Channel",
    "Rock fill Dam",
    "Loose Boulder Structure",
    "5% Model",
    "30-40 Model",
    "Community pond",
]


def populate_maintenance_from_waterbody(plan):
    """
    Filter ODK_waterbody records by water structure type and populate the appropriate maintenance tables
    (GW_maintenance, Agri_maintenance, SWB_maintenance) based on the structure type.

    Args:
        plan: Plan object containing plan details
    """
    print("HERE WE GO")
    # Get all waterbody records for the plan
    waterbodies = ODK_waterbody.objects.filter(plan_id=plan.plan_id)
    logger.info(f"Found {waterbodies.count()} waterbody records for plan {plan.plan_id}")

    for waterbody in waterbodies:
        print("*******************  JUST FOR A CHECK  **********************")
        print("Waterbody:", waterbody)
        structure_type = waterbody.water_structure_type
        logger.info("Water Structure Type:", structure_type)

        # Skip if no maintenance needed
        if waterbody.need_maintenance.lower() != "yes":
            continue

        common_data = {
            "beneficiary_settlement": waterbody.beneficiary_settlement,
            "Beneficiary_Name": waterbody.data_waterbody.get("Beneficiary_name"),
            "select_one_activities": "Maintenance",
        }

        logger.info("Common Data:", common_data)

        work_id = waterbody.waterbody_id

        logger.info("Work ID:", work_id)

        if structure_type in recharge_structures:
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

        elif structure_type in irrigation_structures:
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

        elif structure_type in surface_waterbodies:
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
                    plan_name=plan.plan_name,
                    latitude=waterbody.latitude,
                    longitude=waterbody.longitude,
                    status_re=waterbody.status_re,
                    work_id=work_id,
                    corresponding_work_id=waterbody.waterbody_id,
                    data_swb_maintenance=maintenance_data,
                )
                print(f"SWB Maintenance record created successfully for {work_id}")
