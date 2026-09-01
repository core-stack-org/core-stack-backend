import logging

from nrm_app.celery import app

from computing.STAC_specs.stac_collection import generate_stac_collection_task
from computing.bulk_layer_generation import run_pipeline
from computing.mws.et_download import et_download
from computing.mws.generate_hydrology_local import (
    generate_hydrology,
    generate_hydrology_base_layer,
)
from computing.mws.runoff_gpu import generate_runoff_gpu


logger = logging.getLogger(__name__)


@app.task(name="computing.tasks.bulk_generate_layer")
def bulk_generate_layer(
    pipeline,
    location,
    overwrite=True,
    compute="local",
    start_year=None,
    end_year=None,
    gee_account_id=None,
):
    logger.info(
        "Running bulk pipeline %s for %s/%s/%s",
        pipeline,
        location["state"],
        location["district"],
        location["block"],
    )
    return run_pipeline(
        pipeline,
        location,
        overwrite=overwrite,
        compute=compute,
        start_year=start_year,
        end_year=end_year,
        gee_account_id=gee_account_id,
    )


__all__ = ["bulk_generate_layer", 
           "generate_stac_collection_task", 
           "et_download",
           "generate_hydrology",
           "generate_hydrology_base_layer",
           "generate_runoff_gpu",]
