import os
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text
from computing.utils import (
    save_layer_info_to_db,
    update_layer_sync_status,
)
from computing.local_compute_helper import (
    PROJECT_ROOT,
    build_output_raster_path,
    load_precomputed_watersheds,
    read_validated_vector_file,
    clip_raster_with_roi,
    push_local_raster_to_geoserver,
)
from computing.STAC_specs import generate_STAC_layerwise
from computing.config_loader import (
    PAN_INDIA_NATURALDEPRESSION_PATH,
    LOCAL_NATURALDEPRESSION_OUTPUT,
)

GEOSERVER_WORKSPACE = "natural_depression"
NATURAL_DEPRESSION_STYLE_NAME = "natural_depression"


@app.task(bind=True)
def generate_natural_depression_data_local(
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
        layer_name_base = f"natural_depression_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_raster"
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
        layer_name_base = (
            f"natural_depression_{valid_gee_text(asset_suffix).lower()}_raster"
        )
        watersheds_gdf = read_validated_vector_file(
            roi_path, f"Invalid ROI file: {roi_path}"
        )
        print(f"ROI source: {roi_path}")

    # Raster Processing
    raster_layer_name = f"{layer_name_base}"
    output_raster_path = build_output_raster_path(
        layer_name=raster_layer_name,
        output_base_dir=LOCAL_NATURALDEPRESSION_OUTPUT,
        state=state,
        district=district,
        block=block,
    )

    print("Clipping Natural Depression raster...")
    clipped_raster_path = clip_raster_with_roi(
        roi_gdf=watersheds_gdf,
        raster_path=PAN_INDIA_NATURALDEPRESSION_PATH,
        output_path=output_raster_path,
        raster_label="Natural Depression Raster",
    )
    print(f"Saved clipped Natural Depression raster to: {clipped_raster_path}")

    layer_at_geoserver = False
    if push_to_geoserver:
        push_local_raster_to_geoserver(
            file_path=str(clipped_raster_path),
            layer_name=raster_layer_name,
            workspace=GEOSERVER_WORKSPACE,
            style_name=NATURAL_DEPRESSION_STYLE_NAME,
        )
        print(f"Pushed raster {raster_layer_name} to GeoServer.")
        layer_at_geoserver = True

    if sync_layer_metadata and state and district and block:
        raster_layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=raster_layer_name,
            asset_id=str(clipped_raster_path),
            dataset_name="Natural Depression",
            misc={"is_generated_locally": True},
        )
        if raster_layer_id:
            update_layer_sync_status(layer_id=raster_layer_id, sync_to_geoserver=True)
            print(f"Database record updated for raster layer_id: {raster_layer_id}")

            # STAC Specs for Raster
            try:
                layer_STAC_generated = generate_STAC_layerwise.generate_raster_stac(
                    state=state,
                    district=district,
                    block=block,
                    layer_name="natural_depression_raster",
                )
                update_layer_sync_status(
                    layer_id=raster_layer_id,
                    is_stac_specs_generated=layer_STAC_generated,
                )
                print("STAC metadata updated for Natural Depression raster")
            except Exception as e:
                print(f"Error generating STAC for raster: {e}")

    return layer_at_geoserver
