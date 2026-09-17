from nrm_app.celery import app

from computing.lulc_X_terrain.lulc_on_plain_cluster_local import (
    run_lulc_on_plain_cluster_local,
)
from computing.lulc_X_terrain.lulc_on_slope_cluster_local import (
    run_lulc_on_slope_cluster_local,
)
from computing.terrain_descriptor.terrain_clusters_local import (
    run_terrain_clusters_local,
)
from computing.terrain_descriptor.terrain_raster_fabdem_local import (
    run_terrain_raster_fabdem_local,
)


def _run_step(step_name, step_func, **kwargs):
    print(f"Starting step: {step_name}")
    step_result = step_func(**kwargs)
    print(f"Completed step: {step_name} -> {step_result}")
    if step_result is False:
        raise RuntimeError(f"{step_name} failed")
    return step_result


def _is_missing_block(block):
    if block is None:
        return True
    return str(block).strip().lower() in {"", "null", "none"}


def _run_terrain_compute_for_block(
    state,
    district,
    block,
    start_year,
    end_year,
):
    state = str(state).strip()
    district = str(district).strip()
    block = str(block).strip()
    start_year = int(start_year)
    end_year = int(end_year)

    results = {}

    results["terrain_raster"] = _run_step(
        "terrain_raster",
        run_terrain_raster_fabdem_local,
        state=state,
        district=district,
        block=block,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )
    results["terrain_vector"] = _run_step(
        "terrain_vector",
        run_terrain_clusters_local,
        state=state,
        district=district,
        block=block,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )
    results["terrain_lulc_slope"] = _run_step(
        "terrain_lulc_slope",
        run_lulc_on_slope_cluster_local,
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )
    results["terrain_lulc_plain"] = _run_step(
        "terrain_lulc_plain",
        run_lulc_on_plain_cluster_local,
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )

    print(
        f"Completed local terrain compute-all flow for {state}/{district}/{block}: {results}"
    )
    return results


def run_terrain_compute_all_local(
    state,
    district,
    block,
    start_year,
    end_year,
):
    state = str(state).strip()
    district = str(district).strip()
    start_year = int(start_year)
    end_year = int(end_year)

    if _is_missing_block(block):
        raise ValueError(
            "block is null. Please provide a block value for terrain compute-all."
        )

    return _run_terrain_compute_for_block(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
    )


def _generate_terrain_compute_all_local_task(
    state,
    district,
    block,
    start_year,
    end_year,
    gee_account_id=None,
):
    _ = gee_account_id
    return run_terrain_compute_all_local(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
    )


@app.task(bind=True)
def generate_terrain_compute_all(
    self,
    state,
    district,
    block,
    start_year,
    end_year,
    gee_account_id=None,
):
    _ = self
    return _generate_terrain_compute_all_local_task(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        gee_account_id=gee_account_id,
    )
