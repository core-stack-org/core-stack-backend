import os
import json
import datetime
import pandas as pd
import geopandas as gpd
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text
from computing.local_compute_helper import (
    PROJECT_ROOT,
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    build_output_vector_path,
    get_watershed_areas_in_hectares,
    load_precomputed_watersheds,
    read_validated_vector_file,
    validate_geometry,
    write_vector_output,
)
from computing.config_loader import (
    PAN_INDIA_MWS_CONNECTIVITY_PATH,
    LOCAL_MWS_CONNECTIVITY_OUTPUT,
)
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
    fix_invalid_geometry_in_gdf,
)

GEOSERVER_WORKSPACE = "mws_connectivity"

from shapely.ops import unary_union


def _compute_mws_connectivity_for_watersheds(watersheds_gdf, mws_gdf):
    if mws_gdf.empty:
        print("No MWS connectivity found within the outer boundary.")
        return mws_gdf

    # Step 1: Clip to the outer boundary of watersheds
    outer_boundary = watersheds_gdf.geometry.unary_union
    mws_in_roi = gpd.clip(mws_gdf, outer_boundary).copy()

    # Step 2: Drop empty/invalid geometries
    mws_in_roi = fix_invalid_geometry_in_gdf(mws_in_roi)
    mws_in_roi = mws_in_roi[
        mws_in_roi.geometry.notna()
        & ~mws_in_roi.geometry.is_empty
        & mws_in_roi.geometry.is_valid
    ]

    print(f"Final valid MWS connectivity: {len(mws_in_roi)}")
    return mws_in_roi


@app.task(bind=True)
def mws_connectivity_vector(
    self,
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    if state and district and block:
        layer_name = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_mws_connectivity"
        watersheds_gdf, watershed_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
        print(f"Watershed boundary source: {watershed_source}")
    else:
        if not roi or not asset_suffix:
            raise ValueError(
                "For non state/district/block runs, both `roi` and `asset_suffix` are required."
            )
        layer_name = f"{asset_suffix}_mws_connectivity".lower()
        watersheds_gdf = read_validated_vector_file(
            roi,
            f"ROI file has no valid geometries: {roi}",
        )
        print(f"ROI source: {roi}")

    if not os.path.exists(PAN_INDIA_MWS_CONNECTIVITY_PATH):
        raise FileNotFoundError(f"PAN INDIA MWS connectivity file not found")

    mws_gdf = gpd.read_file(PAN_INDIA_MWS_CONNECTIVITY_PATH, mask=watersheds_gdf)
    mws_gdf = validate_geometry(mws_gdf)
    if mws_gdf.empty:
        print("Warning: PAN INDIA MWS connectivity file has no valid geometries overlapping ROI")

    result_gdf = _compute_mws_connectivity_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        mws_gdf=mws_gdf,
    )

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_MWS_CONNECTIVITY_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local MWS connectivity vector: {asset_id}")

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
        )
        print(f"GeoServer response: {geoserver_response}")

    if sync_layer_metadata and state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Mws Connectivity",
            misc={"is_generated_locally": True},
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for MWS connectivity vector")

    return True
