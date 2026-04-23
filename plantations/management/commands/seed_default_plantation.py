import uuid

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand

from organization.models import Organization
from plantations.models import PlantationProfile
from projects.models import AppType, Project

CFPT_ORG_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
DEFAULT_PROJECT_NAME = "Default Plantation Project"
SYSTEM_USERNAME = "corestack_system"

CONFIG_VARIABLES = {
    "AWC": {"labels": "1,1,2,2,3,4,5,5", "thresholds": "1,2,3,4,5,6,7,0"},
    "LULC": {"labels": "5,5,5,5,5,5,5,1,5,5,5,5,1", "thresholds": "0,1,2,3,4,5,6,7,8,9,10,11,12"},
    "NDVI": {"labels": "1,2,3,4,5", "thresholds": "0.6-0.9,0.4-0.6,0.2-0.4,0.1-0.2,negInf-0.1"},
    "slope": {"labels": "1,2,3,4,5", "thresholds": "0-5,5-10,10-15,15-20,20-posInf"},
    "aspect": {"labels": "3,1,3", "thresholds": "0-67.5,67.5-292.5,292.5-360"},
    "drainage": {"labels": "1,2,2,2,3,4,4", "thresholds": "4,3,5,6,2,1,0"},
    "elevation": {"labels": "1,2,3,4,5", "thresholds": "negInf-1500,1500-2200,2200-3500,3500-4500,4500-posInf"},
    "subsoilBD": {"labels": "1,2,3,4,5", "thresholds": "negInf-1.2,1.2-1.4,1.4-1.58,1.58-1.66,1.66-posInf"},
    "subsoilOC": {"labels": "1,2,3,4,5", "thresholds": "0.6-1.03,0.4-0.6,0.2-0.4,negInf-0.2,1.03-posInf"},
    "subsoilPH": {"labels": "1,2,2,3,5,5", "thresholds": "5.5-7.2,7.2-7.4,4.6-5.5,7.4-7.6,negInf-4.6,7.6-posInf"},
    "topsoilBD": {"labels": "1,2,3,4", "thresholds": "negInf-1.4,1.4-1.44,1.44-1.48,1.48-posInf"},
    "topsoilOC": {"labels": "1,2,3,4,5", "thresholds": "2-3.71,1-2,0.6-1,negInf-0.6,3.71-posInf"},
    "topsoilPH": {"labels": "1,2,2,3,5,5", "thresholds": "5.5-7.2,7.2-7.4,4.6-5.5,7.4-7.6,negInf-4.6,7.6-posInf"},
    "distToRoad": {"labels": "1,2,3,4,5", "thresholds": "0-60,60-120,120-180,180-240,240-posInf"},
    "subsoilCEC": {"labels": "1,2,3,4", "thresholds": "10-posInf,5-10,1-5,negInf-1"},
    "topsoilCEC": {"labels": "1,2,3,4,5", "thresholds": "13-26,10-13,5-10,negInf-5,26-posInf"},
    "aridityIndex": {"labels": "1,2,3,4,5", "thresholds": "35000-posInf,15000-35000,10000-15000,5000-10000,negInf-5000"},
    "distToDrainage": {"labels": "1,2,3,4,5", "thresholds": "0-60,60-120,120-180,180-240,240-posInf"},
    "subsoilTexture": {
        "labels": "1,1,1,2,2,3,3,3,5,5,5,5,5,5",
        "thresholds": "5,7,9,4,12,1,3,10,0,2,6,8,11,13",
    },
    "topsoilTexture": {"labels": "1,2,3,5", "thresholds": "3,2,1,0"},
    "distToSettlements": {"labels": "1,2,3,4,5", "thresholds": "0-60,60-120,120-180,180-240,240-posInf"},
    "annualPrecipitation": {"labels": "1,2,3,4,5", "thresholds": "2000-posInf,1500-2000,1300-1500,1000-1300,negInf-1000"},
    "meanAnnualTemperature": {"labels": "1,2,3,4,5", "thresholds": "20-posInf,15-20,10-15,5-10,negInf-5"},
    "referenceEvapoTranspiration": {
        "labels": "1,2,3,4,5",
        "thresholds": "1450-posInf,1250-1450,1100-1250,1000-1100,negInf-1000",
    },
}

CONFIG_WEIGHT = {
    "AWC": 0.2,
    "LULC": 0.5,
    "NDVI": 0.5,
    "Soil": 0.2,
    "slope": 0.4,
    "aspect": 0.2,
    "Climate": 0.35,
    "Ecology": 0.1,
    "drainage": 0.2,
    "elevation": 0.4,
    "Topography": 0.25,
    "distToRoad": 0.34,
    "rcSubsoilBD": 0.25,
    "rcSubsoilPH": 0.25,
    "rcTopsoilBD": 0.25,
    "rcTopsoilPH": 0.25,
    "snSubsoilOC": 0.25,
    "snSubsoilPH": 0.25,
    "tnTopsoilOC": 0.25,
    "tnTopsoilPH": 0.25,
    "aridityIndex": 0.15,
    "snSubsoilCEC": 0.25,
    "tnTopsoilCEC": 0.25,
    "Socioeconomic": 0.1,
    "distToDrainage": 0.33,
    "subsoilNutrient": 0.2,
    "topsoilNutrient": 0.2,
    "rootingCondition": 0.2,
    "snSubsoilTexture": 0.25,
    "tnTopsoilTexture": 0.25,
    "distToSettlements": 0.33,
    "annualPrecipitation": 0.35,
    "meanAnnualTemperature": 0.35,
    "referenceEvapoTranspiration": 0.15,
}

CONFIG_USER_INPUT = {"": ""}


class Command(BaseCommand):
    help = "Seed the default CFPT plantation org, project, and plantation profile (idempotent)"

    def handle(self, *args, **options):
        User = get_user_model()

        system_user, created = User.objects.get_or_create(
            username=SYSTEM_USERNAME,
            defaults={
                "is_active": False,
                "is_staff": False,
                "is_superuser": False,
            },
        )
        if created:
            system_user.set_unusable_password()
            system_user.save(update_fields=["password"])
            self.stdout.write(f"Created system user: {SYSTEM_USERNAME}")

        org, created = Organization.objects.get_or_create(
            id=CFPT_ORG_ID,
            defaults={"name": "CFPT"},
        )
        if created:
            self.stdout.write("Created organization: CFPT")
        else:
            self.stdout.write("Organization CFPT already exists.")

        project, created = Project.objects.get_or_create(
            name=DEFAULT_PROJECT_NAME,
            organization=org,
            defaults={
                "app_type": AppType.PLANTATION,
                "created_by": system_user,
                "updated_by": system_user,
            },
        )
        if created:
            self.stdout.write(f"Created project: {DEFAULT_PROJECT_NAME}")
        else:
            self.stdout.write(f"Project '{DEFAULT_PROJECT_NAME}' already exists.")

        profile, created = PlantationProfile.objects.get_or_create(
            project=project,
            defaults={
                "config_variables": CONFIG_VARIABLES,
                "config_weight": CONFIG_WEIGHT,
                "config_user_input": CONFIG_USER_INPUT,
            },
        )
        if created:
            self.stdout.write(
                f"Created plantation profile (id={profile.profile_id}) "
                f"for project '{DEFAULT_PROJECT_NAME}'."
            )
        else:
            self.stdout.write(
                f"Plantation profile for '{DEFAULT_PROJECT_NAME}' already exists (id={profile.profile_id})."
            )

        self.stdout.write(self.style.SUCCESS("Default plantation seed complete."))
