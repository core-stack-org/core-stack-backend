import os
from pathlib import Path

from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from computing.local_compute_helper import (
    PROJECT_ROOT,
    load_precomputed_watersheds,
    read_validated_vector_file,
    clip_raster_with_roi,
    push_local_raster_to_geoserver,
    compute_categorical_raster_areas_for_watersheds,
    build_output_raster_path,
    build_output_vector_path,
    write_vector_output,
)
from computing.STAC_specs import generate_STAC_layerwise


from computing.config_loader import (
    PAN_INDIA_RESTORATION_PATH,
    LOCAL_RESTORATION_OUTPUT,
)

GEOSERVER_WORKSPACE = "restoration"
RESTORATION_STYLE_NAME = "restoration_style"


RESTORATION_CLASSES = [
    {"value": 0, "label": "Excluded A"},
    {"value": 1, "label": "Mosaic Res"},
    {"value": 2, "label": "Wide-scale"},
    {"value": 3, "label": "Protection"},
]


def run_raster_restoration_local(
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
        layer_name = f"restoration_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_raster"
        watersheds_gdf, watershed_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
        print(f"Loaded watersheds from {watershed_source}")
    else:
        if not roi_path or not asset_suffix:
            raise ValueError("ROI path and asset_suffix are required for custom runs.")
        layer_name = f"{asset_suffix}_restoration_raster".lower()
        watersheds_gdf = read_validated_vector_file(roi_path, f"Invalid ROI file: {roi_path}")

    # ==========================================
    # 1. Raster Processing
    # ==========================================
    output_raster_path = build_output_raster_path(
        layer_name=layer_name,
        output_base_dir=LOCAL_RESTORATION_OUTPUT,
        state=state,
        district=district,
        block=block,
    )

    print("Clipping restoration raster...")
    clipped_raster_path = clip_raster_with_roi(
        roi_gdf=watersheds_gdf,
        raster_path=PAN_INDIA_RESTORATION_PATH,
        output_path=output_raster_path,
        raster_label="WRI Restoration Raster",
    )
    print(f"Saved clipped restoration raster to: {clipped_raster_path}")

    if push_to_geoserver:
        push_local_raster_to_geoserver(
            file_path=str(clipped_raster_path),
            layer_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            style_name=RESTORATION_STYLE_NAME,
        )
        print(f"Pushed raster {layer_name} to GeoServer.")

    if sync_layer_metadata and state and district and block:
        raster_layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=str(clipped_raster_path),
            dataset_name="Restoration Raster",
            misc={"is_generated_locally": True},
        )
        if raster_layer_id:
            update_layer_sync_status(layer_id=raster_layer_id, sync_to_geoserver=True)
            print(f"Database record updated for raster layer_id: {raster_layer_id}")
            
            # STAC Specs for Raster
            # try:
            #     layer_STAC_generated = generate_STAC_layerwise.generate_raster_stac(
            #         state=state,
            #         district=district,
            #         block=block,
            #         layer_name="wri_restoration_raster",
            #     )
            #     update_layer_sync_status(
            #         layer_id=raster_layer_id, is_stac_specs_generated=layer_STAC_generated
            #     )
            #     print("STAC metadata updated for restoration raster")
            # except Exception as e:
            #     print(f"Error generating STAC for raster: {e}")

    return True, clipped_raster_path


def run_vector_restoration_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi_path=None,
    raster_path=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    if not raster_path:
        raise ValueError("`raster_path` is required for vector stage.")

    if state and district and block:
        layer_name = f"restoration_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_vector"
        watersheds_gdf, watershed_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
    else:
        layer_name = f"{asset_suffix}_restoration_vector".lower()
        watersheds_gdf = read_validated_vector_file(roi_path, f"Invalid ROI file: {roi_path}")

    # ==========================================
    # 2. Vector Processing
    # ==========================================
    print("Computing restoration class areas per watershed...")
    vector_result_gdf = compute_categorical_raster_areas_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        raster_path=raster_path,
        class_definitions=RESTORATION_CLASSES,
    )

    desired_cols = [
        "uid", 
        "id", 
        "area_in_ha", 
        "Excluded A", 
        "Mosaic Res", 
        "Protection", 
        "Wide-scale", 
        "geometry"
    ]
    keep_cols = [c for c in desired_cols if c in vector_result_gdf.columns]
    vector_result_gdf = vector_result_gdf[keep_cols]

    output_vector_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_RESTORATION_OUTPUT,
    )

    vector_asset_id = write_vector_output(
        gdf=vector_result_gdf,
        output_path=output_vector_path,
        layer_name=layer_name,
    )
    print(f"Saved local restoration vector: {vector_asset_id}")

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(vector_asset_id)[0],
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
        )
        print(f"GeoServer response for vector: {geoserver_response}")

    if sync_layer_metadata and state and district and block:
        vector_layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=vector_asset_id,
            dataset_name="Restoration Vector",
            misc={"is_generated_locally": True},
        )
        if vector_layer_id:
            update_layer_sync_status(layer_id=vector_layer_id, sync_to_geoserver=True)
            print(f"Database record updated for vector layer_id: {vector_layer_id}")

            # try:
            #     layer_STAC_generated = generate_STAC_layerwise.generate_vector_stac(
            #         state=state,
            #         district=district,
            #         block=block,
            #         layer_name="wri_restoration_vector",
            #     )
            #     update_layer_sync_status(
            #         layer_id=vector_layer_id,
            #         is_stac_specs_generated=layer_STAC_generated,
            #     )
            #     print("STAC metadata updated for restoration vector")
            # except Exception as e:
            #     print(f"Error generating STAC for vector: {e}")

    return True


@app.task(bind=True)
def generate_restoration_opportunity_local(
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

    raster_ok, clipped_raster_path = run_raster_restoration_local(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        roi_path=roi_path,
        precomputed_roi_dir=precomputed_roi_dir,
        push_to_geoserver=push_to_geoserver,
        sync_layer_metadata=sync_layer_metadata,
    )

    if not raster_ok or not clipped_raster_path:
        print("Raster stage failed — skipping vector stage.")
        return False

    vector_ok = run_vector_restoration_local(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        roi_path=roi_path,
        raster_path=clipped_raster_path,
        precomputed_roi_dir=precomputed_roi_dir,
        push_to_geoserver=push_to_geoserver,
        sync_layer_metadata=sync_layer_metadata,
    )

    return raster_ok and vector_ok
