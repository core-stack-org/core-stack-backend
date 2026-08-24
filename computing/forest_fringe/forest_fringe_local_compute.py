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

from computing.config_loader import PAN_INDIA_FOREST_FRINGE, LOCAL_FOREST_FRINGE_OUTPUT


def _compute_forest_fringe_for_watersheds(watersheds_gdf, forest_fringe_gdf):
    """
    Spatially joins forest fringe features with watershed polygons.
    Equivalent to the GEE Join.saveFirst() with spatial intersection.
    """
    if forest_fringe_gdf.empty:
        return forest_fringe_gdf

    if (
        watersheds_gdf.crs
        and forest_fringe_gdf.crs
        and watersheds_gdf.crs != forest_fringe_gdf.crs
    ):
        forest_fringe_gdf = forest_fringe_gdf.to_crs(watersheds_gdf.crs)

    # We only need the 'uid' from watersheds
    target_watersheds = watersheds_gdf[["uid", "geometry"]].copy()

    joined_gdf = gpd.sjoin(
        forest_fringe_gdf, target_watersheds, how="inner", predicate="intersects"
    )

    # To mimic ee.Join.saveFirst(), drop duplicates based on the original feature index
    joined_gdf = joined_gdf[~joined_gdf.index.duplicated(keep="first")]

    if "index_right" in joined_gdf.columns:
        joined_gdf = joined_gdf.drop(columns=["index_right"])

    return joined_gdf


@app.task(bind=True)
def generate_forest_fringe_local(
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
        layer_name = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_forest_fringe"
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
        layer_name = f"{valid_gee_text(asset_suffix).lower()}_forest_fringe"
        watersheds_gdf = read_validated_vector_file(
            roi_path, f"Invalid ROI file: {roi_path}"
        )
        print(f"ROI source: {roi_path}")

    if not os.path.exists(PAN_INDIA_FOREST_FRINGE):
        raise FileNotFoundError(
            f"PAN INDIA forest fringe file not found at {PAN_INDIA_FOREST_FRINGE}"
        )

    print("Loading forest fringe data overlapping ROI...")
    forest_fringe_gdf = gpd.read_file(PAN_INDIA_FOREST_FRINGE, mask=watersheds_gdf)
    forest_fringe_gdf = validate_geometry(forest_fringe_gdf)
    if forest_fringe_gdf.empty:
        print(
            "Warning: PAN INDIA forest fringe file has no valid geometries overlapping ROI"
        )
    print(f"Loaded {len(forest_fringe_gdf)} forest fringe features")

    result_gdf = _compute_forest_fringe_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        forest_fringe_gdf=forest_fringe_gdf,
    )
    print(
        f"Final valid forest fringe features after spatial join: {len(forest_fringe_gdf)}"
    )

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_FOREST_FRINGE_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=forest_fringe_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local forest fringe vector: {asset_id}")

    layer_at_geoserver = False

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
            workspace="forest_fringes",
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
            dataset_name="Forest Fringe",
            misc={"is_generated_locally": True},
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for Green Credit vector")

    return layer_at_geoserver
