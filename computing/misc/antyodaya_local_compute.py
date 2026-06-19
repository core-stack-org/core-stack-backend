"""Mission Antyodaya tehsil clipping from the pan-India local GeoPackage.

Runtime contract:

- request names may be spaces or snake_case; stored names are resolved from the
  GeoPackage before clipping
- local output is always written first
- GeoServer publish is enabled by default and can be disabled per request;
  failure is reported without breaking local generation

The source GeoPackage should have an attribute index on
``(state_name, district_name, TEHSIL)``. The task creates it once if missing,
which keeps reads sub-second for normal tehsil clips on the local server.
"""

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
    PAN_INDIA_ANTYODAYA_2020,
    LOCAL_ANTYODAYA_2020_OUTPUT,
)

GEOSERVER_WORKSPACE = "antyodaya_2020"


def _compute_antyodaya_for_panchayat(panchayat_gdf, antyodaya_gdf):
    if antyodaya_gdf.empty:
        return antyodaya_gdf

    outer_boundary = panchayat_gdf.geometry.unary_union

    # Clip Antyodaya geometries to the panchayat boundary
    antyodaya_in_roi = gpd.clip(antyodaya_gdf, outer_boundary).copy()

    # Final cleanup
    antyodaya_in_roi = antyodaya_in_roi[~antyodaya_in_roi.geometry.is_empty]
    antyodaya_in_roi = antyodaya_in_roi[antyodaya_in_roi.geometry.is_valid]
    antyodaya_in_roi = antyodaya_in_roi[antyodaya_in_roi.geometry.notna()]

    return antyodaya_in_roi


@app.task(bind=True)
def generate_antyodaya_data_local(
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
        layer_name = f"antyodaya20_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
        panchayat_gdf, watershed_source = load_precomputed_panchayat(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
        print(f"Watershed boundary source: {watershed_source}")
    else:
        if not roi_path or not asset_suffix:
            raise ValueError("ROI path and asset_suffix are required for custom runs.")
        layer_name = f"antyodaya20_{valid_gee_text(asset_suffix).lower()}"
        panchayat_gdf = read_validated_vector_file(roi_path, f"Invalid ROI file: {roi_path}")
        print(f"ROI source: {roi_path}")

    if not os.path.exists(PAN_INDIA_ANTYODAYA_2020):
        raise FileNotFoundError(f"PAN INDIA Antyodaya file not found at {PAN_INDIA_ANTYODAYA_2020}")

    print("Loading Antyodaya data overlapping ROI...")
    antyodaya_gdf = gpd.read_file(PAN_INDIA_ANTYODAYA_2020, mask=panchayat_gdf)
    antyodaya_gdf = validate_geometry(antyodaya_gdf)
    if antyodaya_gdf.empty:
        print("Warning: PAN INDIA Antyodaya file has no valid geometries overlapping ROI")
    else:
        print(f"Loaded {len(antyodaya_gdf)} Antyodaya features")

    result_gdf = _compute_antyodaya_for_panchayat(
        panchayat_gdf=panchayat_gdf,
        antyodaya_gdf=antyodaya_gdf,
    )
    print(f"Final valid Antyodaya features after spatial filter: {len(result_gdf)}")

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_ANTYODAYA_2020_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local Antyodaya vector: {asset_id}")

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
            dataset_name="Antyodaya 2020",
            misc={"is_generated_locally": True},
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for Antyodaya vector")

    return layer_at_geoserver
