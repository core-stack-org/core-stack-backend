from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text
from computing.utils import save_layer_info_to_db
from computing.local_compute_helper import (
    load_precomputed_watersheds,
    clip_raster_with_roi,
    queue_local_raster_for_geoserver,
    build_output_raster_path,
)
from computing.config_loader import (
    PAN_INDIA_SITE_SUITABILITY_PATH,
    LOCAL_SITE_SUITABILITY_OUTPUT,
)

GEOSERVER_WORKSPACE = "plantation"


@app.task(bind=True)
def generate_site_suitability_local(
    self,
    state=None,
    district=None,
    block=None,
    gee_account_id=None,
    project_id=None,
    start_year=None,
    end_year=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    """Clip the pan-India site suitability COG to a tehsil."""
    _ = (self, gee_account_id, project_id, start_year, end_year)
    if not (state and district and block):
        raise ValueError("state, district and block are required.")

    layer_name = (
        f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
        "_site_suitability_raster"
    )
    watersheds_gdf, watershed_source = load_precomputed_watersheds(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    print(f"Loaded watersheds from {watershed_source}")

    output_raster_path = build_output_raster_path(
        layer_name=layer_name,
        output_base_dir=LOCAL_SITE_SUITABILITY_OUTPUT,
        state=state,
        district=district,
        block=block,
    )
    clipped_raster_path = clip_raster_with_roi(
        roi_gdf=watersheds_gdf,
        raster_path=PAN_INDIA_SITE_SUITABILITY_PATH,
        output_path=output_raster_path,
        raster_label="Site Suitability Raster",
    )
    print(f"Saved clipped site suitability raster to: {clipped_raster_path}")

    layer_id = None
    if sync_layer_metadata:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=str(clipped_raster_path),
            dataset_name="Site Suitability",
            misc={"is_generated_locally": True},
        )

    if push_to_geoserver:
        response = queue_local_raster_for_geoserver(
            file_path=str(clipped_raster_path),
            layer_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            layer_id=layer_id,
        )
        if response.get("status_code") != 202:
            return False
        print(f"Queued raster {layer_name} for GeoServer.")

    return True
