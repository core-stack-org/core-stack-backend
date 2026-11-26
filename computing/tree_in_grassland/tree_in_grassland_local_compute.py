import os
import geopandas as gpd
from nrm_app.celery import app

from utilities.gee_utils import valid_gee_text
from computing.local_compute_helper import (
    clip_vector_to_mws,
    get_roi,
    read_validated_vector_file,
    resolve_indexed_vector_path,
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
    PAN_INDIA_TREE_IN_GRASSLAND,
    LOCAL_TREE_IN_GRASSLAND_OUTPUT,
)


@app.task(bind=True)
def generate_tree_in_grassland_local(
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
        state = str(state).strip().lower()
        district = str(district).strip().lower()
        block = str(block).strip().lower()

    # Uses precomputed tehsil ROI when state/district/block are given,
    # otherwise falls back to the provided `roi_path` file and `asset_suffix`.
    asset_suffix, watersheds_gdf = get_roi(
        asset_suffix,
        block,
        district,
        roi_path,
        state,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    layer_name = f"{valid_gee_text(asset_suffix).lower()}_tree_in_grassland"

    tree_in_grassland_path = resolve_indexed_vector_path(PAN_INDIA_TREE_IN_GRASSLAND)
    if not tree_in_grassland_path.exists():
        raise FileNotFoundError(
            f"PAN INDIA tree in grassland file not found at {tree_in_grassland_path}"
        )

    print(
        f"Loading tree in grassland data overlapping ROI from {tree_in_grassland_path}..."
    )
    tree_in_grassland_gdf = gpd.read_file(
        tree_in_grassland_path, mask=watersheds_gdf
    )
    tree_in_grassland_gdf = validate_geometry(tree_in_grassland_gdf)
    if tree_in_grassland_gdf.empty:
        print(
            "Warning: PAN INDIA tree in grassland file has no valid geometries overlapping ROI"
        )
    print(f"Loaded {len(tree_in_grassland_gdf)} tree in grassland features")

    result_gdf = clip_vector_to_mws(
        watersheds_gdf=watersheds_gdf,
        source_gdf=tree_in_grassland_gdf,
    )
    print(
        f"Final valid tree in grassland features after spatial join: {len(result_gdf)}"
    )

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_TREE_IN_GRASSLAND_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local tree in grassland vector: {asset_id}")

    layer_at_geoserver = False

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
            workspace="tree_in_grassland",
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
            dataset_name="Tree in Grassland",
            misc={"is_generated_locally": True},
        )
        if layer_id and layer_at_geoserver:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for Tree in Grassland vector")

    return layer_at_geoserver or not push_to_geoserver
