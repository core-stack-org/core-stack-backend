import os
import geopandas as gpd
from nrm_app.celery import app

from utilities.gee_utils import valid_gee_text
from computing.local_compute_helper import (
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
    PAN_INDIA_TREE_IN_GRASSLAND,
    LOCAL_TREE_IN_GRASSLAND_OUTPUT,
)


def _compute_tree_in_grassland_for_watersheds(watersheds_gdf, tree_in_grassland_gdf):
    """
    Spatially joins tree in grassland features with watershed polygons.
    Equivalent to the GEE Join.saveFirst() with spatial intersection.
    """
    if tree_in_grassland_gdf.empty:
        return tree_in_grassland_gdf

    if (
        watersheds_gdf.crs
        and tree_in_grassland_gdf.crs
        and watersheds_gdf.crs != tree_in_grassland_gdf.crs
    ):
        tree_in_grassland_gdf = tree_in_grassland_gdf.to_crs(watersheds_gdf.crs)

    # We only need the 'uid' from watersheds
    target_watersheds = watersheds_gdf[["uid", "geometry"]].copy()

    joined_gdf = gpd.sjoin(
        tree_in_grassland_gdf, target_watersheds, how="inner", predicate="intersects"
    )

    # To mimic ee.Join.saveFirst(), drop duplicates based on the original feature index
    joined_gdf = joined_gdf[~joined_gdf.index.duplicated(keep="first")]

    if "index_right" in joined_gdf.columns:
        joined_gdf = joined_gdf.drop(columns=["index_right"])

    return joined_gdf


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
        layer_name = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_tree_in_grassland"
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
        layer_name = f"{valid_gee_text(asset_suffix).lower()}_tree_in_grassland"
        watersheds_gdf = read_validated_vector_file(
            roi_path, f"Invalid ROI file: {roi_path}"
        )
        print(f"ROI source: {roi_path}")

    if not os.path.exists(PAN_INDIA_TREE_IN_GRASSLAND):
        raise FileNotFoundError(
            f"PAN INDIA tree in grassland file not found at {PAN_INDIA_TREE_IN_GRASSLAND}"
        )

    print("Loading tree in grassland data overlapping ROI...")
    tree_in_grassland_gdf = gpd.read_file(
        PAN_INDIA_TREE_IN_GRASSLAND, mask=watersheds_gdf
    )
    tree_in_grassland_gdf = validate_geometry(tree_in_grassland_gdf)
    if tree_in_grassland_gdf.empty:
        print(
            "Warning: PAN INDIA tree in grassland file has no valid geometries overlapping ROI"
        )
    print(f"Loaded {len(tree_in_grassland_gdf)} tree in grassland features")

    result_gdf = _compute_tree_in_grassland_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        tree_in_grassland_gdf=tree_in_grassland_gdf,
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
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for Tree in Grassland vector")

    return layer_at_geoserver
