from pathlib import Path

from nrm_app.celery import app

from computing.local_compute_helper import PRECOMPUTED_TEHSIL_WATERSHED_DIR
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
from utilities.gee_utils import valid_gee_text


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


def _resolve_blocks_for_district(
    state,
    district,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
):
    district_dir = (
        Path(precomputed_roi_dir)
        / (valid_gee_text(str(state).strip().lower()) or "unknown_state")
        / (valid_gee_text(str(district).strip().lower()) or "unknown_district")
    )
    if not district_dir.exists():
        raise FileNotFoundError(
            f"District watershed directory not found: {district_dir}"
        )

    asset_block_slugs = sorted(
        {
            path.stem
            for path in district_dir.iterdir()
            if path.is_file() and path.suffix.lower() in {".gpkg", ".geojson"}
        }
    )
    if not asset_block_slugs:
        raise FileNotFoundError(
            f"No watershed files found for district: {state}/{district}"
        )

    canonical_block_names = {}
    try:
        from geoadmin.models import DistrictSOI, StateSOI, TehsilSOI

        state_obj = StateSOI.objects.get(state_name__iexact=state)
        district_obj = DistrictSOI.objects.get(
            district_name__iexact=district,
            state=state_obj,
        )
        tehsil_names = TehsilSOI.objects.filter(district=district_obj).values_list(
            "tehsil_name",
            flat=True,
        )
        canonical_block_names = {
            (
                valid_gee_text(str(tehsil_name).strip().lower())
                or "unknown_block"
            ): tehsil_name
            for tehsil_name in tehsil_names
        }
    except Exception as error:
        print(
            "Unable to resolve canonical block names from DB. "
            f"Falling back to asset slugs. Error: {error}"
        )

    return [
        canonical_block_names.get(block_slug, block_slug)
        for block_slug in asset_block_slugs
    ]


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
        block_names = _resolve_blocks_for_district(
            state=state,
            district=district,
        )
        district_results = {}
        success_count = 0

        for block_name in block_names:
            try:
                district_results[block_name] = _run_terrain_compute_for_block(
                    state=state,
                    district=district,
                    block=block_name,
                    start_year=start_year,
                    end_year=end_year,
                )
                success_count += 1
            except Exception as error:
                print(
                    f"Failed district-wide terrain compute-all for block {block_name}: {error}"
                )
                district_results[block_name] = {"error": str(error)}

        summary = {
            "scope": "district",
            "state": state,
            "district": district,
            "total_blocks": len(block_names),
            "successful_blocks": success_count,
            "failed_blocks": len(block_names) - success_count,
            "blocks": district_results,
        }
        print(f"Completed district-wide terrain compute-all flow: {summary}")

        if success_count == 0:
            raise RuntimeError(
                f"Terrain compute-all failed for every block in {state}/{district}"
            )
        return summary

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
