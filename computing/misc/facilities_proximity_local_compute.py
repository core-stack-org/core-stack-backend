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
    load_precomputed_watersheds,
    read_validated_vector_file,
    write_vector_output,
    validate_geometry,
)
from computing.config_loader import (
    PAN_INDIA_FACILITIES_PATH,
    LOCAL_FACILITIES_OUTPUT,
)

GEOSERVER_WORKSPACE = "facilities"


def _compute_proximity_for_watersheds(watersheds_gdf, facilities_gdf):
    """
    Filters facilities to strictly those intersecting the watershed boundaries.
    """
    if facilities_gdf.empty:
        return facilities_gdf

    # Ensure CRS matches
    if (
        watersheds_gdf.crs
        and facilities_gdf.crs
        and watersheds_gdf.crs != facilities_gdf.crs
    ):
        facilities_gdf = facilities_gdf.to_crs(watersheds_gdf.crs)

    outer_boundary = watersheds_gdf.geometry.unary_union

    # Precise intersection check (since load-time mask is just bounding box)
    facilities_in_roi = facilities_gdf[facilities_gdf.intersects(outer_boundary)].copy()

    # Final cleanup
    facilities_in_roi = facilities_in_roi[~facilities_in_roi.geometry.is_empty]
    facilities_in_roi = facilities_in_roi[facilities_in_roi.geometry.is_valid]
    facilities_in_roi = facilities_in_roi[facilities_in_roi.geometry.notna()]

    return facilities_in_roi


@app.task(bind=True)
def generate_facilities_proximity_local(
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
        layer_name = f"facilities_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_27may"
        watersheds_gdf, watershed_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
        print(f"Watershed boundary source: {watershed_source}")
    else:
        if not roi_path or not asset_suffix:
            raise ValueError("ROI path and asset_suffix are required for custom runs.")
        layer_name = f"facilities_{valid_gee_text(asset_suffix).lower()}"
        watersheds_gdf = read_validated_vector_file(
            roi_path, f"Invalid ROI file: {roi_path}"
        )
        print(f"ROI source: {roi_path}")

    if not os.path.exists(PAN_INDIA_FACILITIES_PATH):
        raise FileNotFoundError(
            f"PAN INDIA Facilities file not found at {PAN_INDIA_FACILITIES_PATH}"
        )

    print("Loading Facilities data overlapping ROI...")
    # Load using bounding box mask to save memory
    facilities_gdf = gpd.read_file(PAN_INDIA_FACILITIES_PATH, mask=watersheds_gdf)
    facilities_gdf = validate_geometry(facilities_gdf)
    if facilities_gdf.empty:
        print(
            "Warning: PAN INDIA Facilities file has no valid geometries overlapping ROI"
        )
    print(f"Loaded {len(facilities_gdf)} Facilities features")

    result_gdf = _compute_proximity_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        facilities_gdf=facilities_gdf,
    )
    print(f"Final valid Facilities features after spatial filter: {len(result_gdf)}")

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_FACILITIES_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local Facilities vector: {asset_id}")

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
            dataset_name="Facilities",
            misc={"is_generated_locally": True},
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for Facilities vector")

    return layer_at_geoserver
