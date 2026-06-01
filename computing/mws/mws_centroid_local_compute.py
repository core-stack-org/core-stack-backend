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
)
from computing.config_loader import LOCAL_MWS_CENTROID_OUTPUT

GEOSERVER_WORKSPACE = "mws_centroid"


def _compute_mws_centroids(watersheds_gdf):
    """
    Computes the centroid of each watershed polygon and extracts lat/lon.
    """
    if watersheds_gdf.empty:
        return watersheds_gdf

    # Create a copy so we don't modify the original
    centroids_gdf = watersheds_gdf.copy()

    # Ensure WGS84 for correct lat/lon coordinate extraction
    if centroids_gdf.crs != "EPSG:4326":
        centroids_gdf = centroids_gdf.to_crs("EPSG:4326")

    # Replace polygon geometry with point centroid
    centroids_gdf["geometry"] = centroids_gdf.geometry.centroid

    # Extract coordinates
    centroids_gdf["centroid_lon"] = centroids_gdf.geometry.x
    centroids_gdf["centroid_lat"] = centroids_gdf.geometry.y

    return centroids_gdf


@app.task(bind=True)
def generate_mws_centroid_data_local(
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
    _ = self, gee_account_id
    if state and district and block:
        layer_name = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_mws_centroid_27may"
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
        layer_name = f"{valid_gee_text(asset_suffix).lower()}_mws_centroid"
        watersheds_gdf = read_validated_vector_file(
            roi_path, f"Invalid ROI file: {roi_path}"
        )
        print(f"ROI source: {roi_path}")

    print("Computing MWS centroids...")
    result_gdf = _compute_mws_centroids(watersheds_gdf=watersheds_gdf)
    print(f"Computed centroids for {len(result_gdf)} features")

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_MWS_CENTROID_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local MWS centroid vector: {asset_id}")

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
            dataset_name="Mws Centroid",
            misc={"is_generated_locally": True},
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for MWS Centroid vector")

    return layer_at_geoserver
