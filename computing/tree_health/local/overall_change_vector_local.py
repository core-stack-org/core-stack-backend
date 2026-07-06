from pathlib import Path

from computing.config_loader import PROJECT_ROOT
from computing.local_compute_helper import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    build_output_vector_path,
    compute_categorical_raster_areas_for_watersheds,
    load_precomputed_watersheds,
    push_local_vector_to_geoserver,
    read_validated_vector_file,
    write_vector_output,
)
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text

from computing.tree_health.overall_change_local import (
    LOCAL_OUTPUT_BASE_DIR as OVERALL_CHANGE_RASTER_DIR,
)


LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/tree_health"
GEOSERVER_WORKSPACE = "tree_overall_vector"

OVERALL_CHANGE_CLASSES = [
    {"value": -2, "label": "Deforestation"},
    {"value": -1, "label": "Degradation"},
    {"value": 0, "label": "No_Change"},
    {"value": 1, "label": "Improvement"},
    {"value": 2, "label": "Afforestation"},
    {"values": [3, 4], "label": "Partially_Degraded"},
    {"value": 5, "label": "Missing Data"},
]


def _slug(value, fallback):
    if value is None:
        return fallback
    return valid_gee_text(str(value).strip().lower()) or fallback


def _resolve_overall_change_raster(asset_suffix, state=None, district=None, block=None):
    raster_name = f"overall_change_raster_{asset_suffix}.tif"
    if state and district and block:
        path = (
            Path(OVERALL_CHANGE_RASTER_DIR)
            / _slug(state, "unknown_state")
            / _slug(district, "unknown_district")
            / _slug(block, "unknown_block")
            / raster_name
        )
    else:
        path = Path(OVERALL_CHANGE_RASTER_DIR) / asset_suffix / raster_name

    if path.exists():
        return str(path)

    raise FileNotFoundError(
        f"Local overall change raster not found for vectorisation: {path}. "
        "Run overall_change_local.py first."
    )

@app.task(bind=True)
def tree_health_overall_change_vector_local(
    self,
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    state = str(state).strip().lower() if state else None
    district = str(district).strip().lower() if district else None
    block = str(block).strip().lower() if block else None

    # Vector outputs are generated over watershed polygons, same as reduceRegions in GEE.
    if state and district and block:
        asset_suffix = (
            f"{_slug(district, 'unknown_district')}_"
            f"{_slug(block, 'unknown_block')}"
        )
        result_gdf, _ = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
    else:
        if not roi or not asset_suffix:
            raise ValueError(
                "For non state/district/block runs, both `roi` and "
                "`asset_suffix` are required."
            )
        asset_suffix = _slug(asset_suffix, "custom")
        result_gdf = read_validated_vector_file(
            roi,
            f"ROI file has no valid geometries: {roi}",
        )

    raster_path = _resolve_overall_change_raster(
        asset_suffix=asset_suffix,
        state=state,
        district=district,
        block=block,
    )
    print(f"Computing local overall change vector columns: {raster_path}")
    result_gdf = compute_categorical_raster_areas_for_watersheds(
        watersheds_gdf=result_gdf,
        raster_path=raster_path,
        class_definitions=OVERALL_CHANGE_CLASSES,
    )

    layer_name = f"overall_change_vector_{asset_suffix}"
    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        custom_subdir=asset_suffix,
    )
    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local overall tree change vector: {asset_id}")

    if push_to_geoserver:
        res = push_local_vector_to_geoserver(
            path=asset_id,
            layer_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            file_type="gpkg",
        )
        print(f"GeoServer response for {layer_name}: {res}")
        if not isinstance(res, dict) or res.get("status_code") not in (200, 201):
            return False

    if sync_layer_metadata and state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Tree Overall Change Vector",
            misc={"is_generated_locally": True},
            algorithm="local_tree_overall_change_vector",
            algorithm_version="local-1.0",
        )
        if layer_id and push_to_geoserver:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)

    return True
