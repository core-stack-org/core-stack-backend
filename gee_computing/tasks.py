from gee_computing.compute import ComputeOnGEE as cog
from celery import shared_task


"""
path: projects/ee-water-management/assets/...

variables folder:
- precipitation
- runoff
- ET
- SM
- deltaG
"""


@shared_task(name="tasks.UploadAssetsToGEE")
def upload_assets_GEE():
    """
    Periodically upload data to GEE Assets for ET and SM every fortnitely
    """
    pass


@shared_task(name="tasks.ComputeSEVariables")
def compute_se_variables():
    """Periodic computation of the SE variables which repeats after 14 days"""
    cog.precipitation()
    cog.run_off()
    cog.evapo_transpiration()
    # cog.soil_moisture()


@shared_task(name="tasks.ComputeDeltaG")
def compute_delta_g():
    """
    Periodic calculation of Delta G
    """
