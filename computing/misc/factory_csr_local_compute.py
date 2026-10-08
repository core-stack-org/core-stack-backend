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
    PAN_INDIA_FACTORY_CSR_PATH,
    LOCAL_FACTORY_CSR_OUTPUT,
)

GEOSERVER_WORKSPACE = "factory_csr"


def _compute_factory_csr_for_watersheds(watersheds_gdf, factory_gdf):
    """
    Spatially joins Factory CSR features with watershed polygons.
    Equivalent to the GEE Join.saveFirst() with spatial intersection.
    """
    if factory_gdf.empty:
        return factory_gdf
        
    if watersheds_gdf.crs and factory_gdf.crs and watersheds_gdf.crs != factory_gdf.crs:
        factory_gdf = factory_gdf.to_crs(watersheds_gdf.crs)

    # We only need the 'uid' from watersheds
    target_watersheds = watersheds_gdf[["uid", "geometry"]].copy()
    
    joined_gdf = gpd.sjoin(
        factory_gdf,
        target_watersheds,
        how="inner",
        predicate="intersects"
    )

    # To mimic ee.Join.saveFirst(), drop duplicates based on the original feature index
    joined_gdf = joined_gdf[~joined_gdf.index.duplicated(keep="first")]
    
    if "index_right" in joined_gdf.columns:
        joined_gdf = joined_gdf.drop(columns=["index_right"])

    rename_map = {}
    for col in joined_gdf.columns:
        cl = col.lower().strip()
        if cl in ["company_na", "company na", "company name", "company_name"]: rename_map[col] = "COMPANY NA"
        elif cl in ["location n", "location_n", "location t", "location_t", "location type", "location_type"]: rename_map[col] = "LOCATION T"
        elif cl == "address": rename_map[col] = "ADDRESS"
        elif cl in ["level 1", "level_1"]: rename_map[col] = "LEVEL 1"
        elif cl in ["level 2", "level_2"]: rename_map[col] = "LEVEL 2"
        elif cl in ["level 3", "level_3"]: rename_map[col] = "LEVEL 3"
        elif cl == "uuid": rename_map[col] = "UUID"
        elif col == "" or "unnamed" in cl: rename_map[col] = "Unnamed_ 9"
        elif cl == "lat": rename_map[col] = "LAT"
        elif cl in ["lng", "lon", "long", "longitude"]: rename_map[col] = "LNG"
        
    joined_gdf = joined_gdf.rename(columns=rename_map)

    if "LAT" not in joined_gdf.columns and not joined_gdf.empty:
        joined_gdf["LAT"] = joined_gdf.geometry.y
    if "LNG" not in joined_gdf.columns and not joined_gdf.empty:
        joined_gdf["LNG"] = joined_gdf.geometry.x

    target_cols = [
        "ADDRESS", "COMPANY NA", "LAT", "LEVEL 1", "LEVEL 2",
        "LEVEL 3", "LNG", "LOCATION T", "UUID", "Unnamed_ 9",
        "uid", "geometry"
    ]
    cols_to_keep = [col for col in target_cols if col in joined_gdf.columns]
    joined_gdf = joined_gdf[cols_to_keep]

    return joined_gdf


@app.task(bind=True)
def generate_factory_csr_data_local(
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
        layer_name = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_factory_csr"
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
        layer_name = f"{valid_gee_text(asset_suffix).lower()}_factory_csr"
        watersheds_gdf = read_validated_vector_file(roi_path, f"Invalid ROI file: {roi_path}")
        print(f"ROI source: {roi_path}")

    if not os.path.exists(PAN_INDIA_FACTORY_CSR_PATH):
        raise FileNotFoundError(f"PAN INDIA Factory CSR file not found at {PAN_INDIA_FACTORY_CSR_PATH}")

    print("Loading Factory CSR data overlapping ROI...")
    factory_gdf = gpd.read_file(PAN_INDIA_FACTORY_CSR_PATH, mask=watersheds_gdf)
    factory_gdf = validate_geometry(factory_gdf)
    if factory_gdf.empty:
        print("Warning: PAN INDIA Factory CSR file has no valid geometries overlapping ROI")
    print(f"Loaded {len(factory_gdf)} Factory CSR features")

    result_gdf = _compute_factory_csr_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        factory_gdf=factory_gdf,
    )
    print(f"Final valid Factory CSR features after spatial join: {len(result_gdf)}")

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_FACTORY_CSR_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local Factory CSR vector: {asset_id}")

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
            dataset_name="Factory CSR",
            misc={"is_generated_locally": True},
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for Factory CSR vector")

    return layer_at_geoserver



