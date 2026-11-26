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
    build_output_vector_path,
    load_precomputed_watersheds,
    read_validated_vector_file,
    write_vector_output,
    validate_geometry,
    clip_layer_to_micro_watersheds,
)
from computing.config_loader import (
    PAN_INDIA_SOLAR_POWER_TRACKER_PATH,
    LOCAL_SOLAR_POWER_TRACKER_OUTPUT,
)

GEOSERVER_WORKSPACE = "solar_power"


@app.task(bind=True)
def generate_solar_power_local(
    self,
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi_path=None,
    gee_account_id=None,
    precomputed_roi_dir=None,
    push_to_geoserver=False,
    sync_layer_metadata=True,
):
    if state and district and block:
        layer_name = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_solar_power_tracker"
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
        layer_name = f"{valid_gee_text(asset_suffix).lower()}_solar_power_tracker"
        watersheds_gdf = read_validated_vector_file(roi_path, f"Invalid ROI file: {roi_path}")
        print(f"ROI source: {roi_path}")

    if not os.path.exists(PAN_INDIA_SOLAR_POWER_TRACKER_PATH):
        raise FileNotFoundError(f"PAN INDIA Solar Power Tracker file not found at {PAN_INDIA_SOLAR_POWER_TRACKER_PATH}")

    print("Loading Solar Power Tracker data overlapping ROI...")
    solar_power_gdf = gpd.read_file(PAN_INDIA_SOLAR_POWER_TRACKER_PATH, mask=watersheds_gdf)
    solar_power_gdf = validate_geometry(solar_power_gdf)
    if solar_power_gdf.empty:
        print("Warning: PAN INDIA Solar Power Tracker file has no valid geometries overlapping ROI")
    print(f"Loaded {len(solar_power_gdf)} Solar Power Tracker features")

    result_gdf = clip_layer_to_micro_watersheds(
        watersheds_gdf=watersheds_gdf,
        source_gdf=solar_power_gdf,
    )
    print(f"Final valid Solar Power Tracker features after spatial join: {len(result_gdf)}")

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_SOLAR_POWER_TRACKER_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local Solar Power Tracker vector: {asset_id}")

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
            dataset_name="Solar Power Tracker",
            misc={"is_generated_locally": True},
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for Solar Power Tracker vector")

    return layer_at_geoserver



