from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text
from computing.utils import save_layer_info_to_db
from computing.local_compute_helper import (
    get_roi,
    clip_raster_with_roi,
    queue_local_raster_for_geoserver,
    build_output_raster_path,
)
from computing.config_loader import (
    PAN_INDIA_SITE_SUITABILITY_PATH,
    LOCAL_SITE_SUITABILITY_OUTPUT,
)

GEOSERVER_WORKSPACE = "plantation"
GEOSERVER_STYLE = "site_suitability"


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
    asset_suffix=None,
    roi=None,
):
    """Clip the pan-India site suitability COG to a tehsil."""
    _ = (self, gee_account_id, project_id, start_year, end_year)

    # Uses precomputed tehsil ROI when state/district/block are given,
    # otherwise falls back to the provided `roi` file and `asset_suffix`.
    asset_suffix, watersheds_gdf = get_roi(
        asset_suffix,
        block,
        district,
        roi,
        state,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    layer_name = f"{valid_gee_text(asset_suffix.lower())}_site_suitability_raster"

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
    if sync_layer_metadata and state and district and block:
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
            style_name=GEOSERVER_STYLE,
            layer_id=layer_id,
        )
        if response.get("status_code") != 202:
            return False
        print(f"Queued raster {layer_name} for GeoServer.")

    return True
