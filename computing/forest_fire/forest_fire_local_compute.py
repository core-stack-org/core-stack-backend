import os
import geopandas as gpd
from nrm_app.celery import app

from utilities.gee_utils import valid_gee_text
from computing.local_compute_helper import (
    clip_vector_to_mws,
    load_precomputed_watersheds,
    read_validated_vector_file,
    validate_geometry,
    write_vector_output,
    build_output_vector_path,
)
from computing.utils import (
    save_layer_info_to_db,
    update_layer_sync_status,
    push_shape_to_geoserver,
)

from computing.config_loader import (
    PAN_INDIA_FOREST_FIRE,
    LOCAL_FOREST_FIRE_OUTPUT,
)


@app.task(bind=True)
def generate_forest_fire_local(
    self,
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi_path=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    if state and district and block:
        layer_name = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_forest_fire"
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
        layer_name = f"{valid_gee_text(asset_suffix).lower()}_forest_fire"
        watersheds_gdf = read_validated_vector_file(
            roi_path, f"Invalid ROI file: {roi_path}"
        )
        print(f"ROI source: {roi_path}")

    if not os.path.exists(PAN_INDIA_FOREST_FIRE):
        raise FileNotFoundError(
            f"PAN INDIA forest fire file not found at {PAN_INDIA_FOREST_FIRE}"
        )

    print("Loading forest fire data overlapping ROI...")
    forest_fire_gdf = gpd.read_file(PAN_INDIA_FOREST_FIRE, mask=watersheds_gdf)
    forest_fire_gdf = validate_geometry(forest_fire_gdf)
    if forest_fire_gdf.empty:
        print(
            "Warning: PAN INDIA forest fire file has no valid geometries overlapping ROI"
        )
    print(f"Loaded {len(forest_fire_gdf)} forest fire features")

    result_gdf = clip_vector_to_mws(
        watersheds_gdf=watersheds_gdf,
        source_gdf=forest_fire_gdf,
    )
    print(f"Final valid forest fire features after clipping: {len(result_gdf)}")

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_FOREST_FIRE_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local forest fire vector: {asset_id}")

    layer_at_geoserver = False

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
            workspace="forest_fire",
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
            dataset_name="Forest Fire",
            misc={"is_generated_locally": True},
        )
        if layer_id and layer_at_geoserver:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for Forest Fire vector")

    return layer_at_geoserver or not push_to_geoserver
