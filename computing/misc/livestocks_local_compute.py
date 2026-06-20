import os
import geopandas as gpd

from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from computing.local_compute_helper import (
    PROJECT_ROOT,
    build_output_vector_path,
    load_precomputed_panchayat,
    read_validated_vector_file,
    write_vector_output,
    validate_geometry,
)
from computing.config_loader import (
    PAN_INDIA_LIVESTOCKS,
    LOCAL_LIVESTOCKS_OUTPUT,
)

GEOSERVER_WORKSPACE = "livestocks"

def _compute_livestocks_for_panchayat(panchayat_gdf, livestocks_gdf):
    if livestocks_gdf.empty:
        return livestocks_gdf

    outer_boundary = panchayat_gdf.geometry.unary_union

    # Clip Antyodaya geometries to the panchayat boundary
    livestocks_in_roi = gpd.clip(livestocks_gdf, outer_boundary).copy()
    livestocks_in_roi = livestocks_in_roi[~livestocks_in_roi.geometry.is_empty]
    livestocks_in_roi = livestocks_in_roi[livestocks_in_roi.geometry.is_valid]
    livestocks_in_roi = livestocks_in_roi[livestocks_in_roi.geometry.notna()]

    return livestocks_in_roi


@app.task(bind=True)
def generate_livestocks_data_local(
    self,
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi_path=None,
    gee_account_id=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    if state and district and block:
        layer_name = f"livestocks_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
        panchayat_gdf, watershed_source = load_precomputed_panchayat(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
    else:
        if not roi_path or not asset_suffix:
            raise ValueError("ROI path and asset_suffix are required for custom runs.")
        layer_name = f"livestocks_{valid_gee_text(asset_suffix).lower()}"
        panchayat_gdf = read_validated_vector_file(roi_path, f"Invalid ROI file: {roi_path}")
        print(f"ROI source: {roi_path}")

    if not os.path.exists(PAN_INDIA_LIVESTOCKS):
        raise FileNotFoundError(f"PAN INDIA Livestocks file not found at {PAN_INDIA_LIVESTOCKS}")

    print("Loading Livestocks data overlapping ROI...")
    livestocks_gdf = gpd.read_file(PAN_INDIA_LIVESTOCKS, mask=panchayat_gdf)
    livestocks_gdf = validate_geometry(livestocks_gdf)
    if livestocks_gdf.empty:
        print("Warning: PAN INDIA Livestocks file has no valid geometries overlapping ROI")
    else:
        print(f"Loaded {len(livestocks_gdf)} Livestock features")

    result_gdf = _compute_livestocks_for_panchayat(
        panchayat_gdf=panchayat_gdf,
        livestocks_gdf=livestocks_gdf,
    )
    print(f"Final valid Livestock features after spatial filter: {len(result_gdf)}")

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_LIVESTOCKS_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local Livestock vector: {asset_id}")

    layer_at_geoserver = False

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
        )
        print(f"GeoServer response: {geoserver_response}")
        if geoserver_response and geoserver_response.get("status_code") in (200, 201):
            layer_at_geoserver = True

    if sync_layer_metadata and state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Livestock Census 2019",
            misc={"is_generated_locally": True},
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for Livestock vector")

    return layer_at_geoserver
