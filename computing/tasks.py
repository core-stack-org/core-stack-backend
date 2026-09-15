import logging

from nrm_app.celery import app

from computing.STAC_specs.stac_collection import generate_stac_collection_task
from computing.bulk_layer_generation import run_pipeline


logger = logging.getLogger(__name__)
GEOSERVER_QUEUE = "geoserver"


class GeoServerPublishError(Exception):
    pass


@app.task(
    bind=True,
    name="computing.tasks.publish_local_layer",
    autoretry_for=(GeoServerPublishError,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
    max_retries=5,
    acks_late=True,
    reject_on_worker_lost=True,
)
def publish_local_layer(
    self,
    layer_type,
    path,
    layer_name,
    workspace,
    style_name=None,
    file_type="gpkg",
    layer_id=None,
    verify=False,
):
    from computing.local_compute_helper import (
        push_local_raster_to_geoserver,
        push_local_vector_to_geoserver,
    )
    from computing.utils import update_layer_sync_status

    if layer_type == "raster":
        response = push_local_raster_to_geoserver(
            file_path=path,
            layer_name=layer_name,
            workspace=workspace,
            style_name=style_name,
            verify=verify,
        )
    elif layer_type == "vector":
        response = push_local_vector_to_geoserver(
            path=path,
            layer_name=layer_name,
            workspace=workspace,
            file_type=file_type,
        )
    else:
        raise ValueError(f"Unsupported GeoServer layer type: {layer_type}")

    if response.get("status_code") not in (200, 201):
        raise GeoServerPublishError(
            f"Failed to publish {workspace}:{layer_name}: {response}"
        )

    if layer_id and update_layer_sync_status(
        layer_id=layer_id,
        sync_to_geoserver=True,
        is_stac_specs_generated=False,
    ) is None:
        raise GeoServerPublishError(
            f"Published {workspace}:{layer_name}, but metadata update failed"
        )

    logger.info(
        "Published %s layer %s:%s from task %s",
        layer_type,
        workspace,
        layer_name,
        self.request.id,
    )
    return response


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


__all__ = [
    "bulk_generate_layer",
    "generate_stac_collection_task",
    "publish_local_layer",
]
