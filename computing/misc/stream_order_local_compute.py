import os

from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text
from computing.utils import save_layer_info_to_db
from computing.local_compute_helper import (
    build_output_raster_path,
    build_output_vector_path,
    clip_raster_with_roi,
    compute_categorical_raster_percentages_for_watersheds,
    get_roi,
    queue_local_raster_for_geoserver,
    queue_local_vector_for_geoserver,
    write_vector_output,
)
from computing.config_loader import (
    LOCAL_STREAM_ORDER_OUTPUT,
    PAN_INDIA_STREAM_ORDER_PATH,
)

GEOSERVER_WORKSPACE = "stream_order"
STREAM_ORDER_STYLE_NAME = "stream_order"
STREAM_ORDER_DATASET_NAME = "Stream Order"

STREAM_ORDER_CLASSES = [
    {"value": order, "label": str(order)} for order in range(1, 12)
]


def _normalize_location(state, district, block):
    if state and district and block:
        return (
            str(state).strip().lower(),
            str(district).strip().lower(),
            str(block).strip().lower(),
            True,
        )
    return state, district, block, False


def _stream_order_layer_name(asset_suffix, use_precomputed, kind):
    if use_precomputed:
        return f"stream_order_{asset_suffix}_{kind}"
    return f"stream_order_{valid_gee_text(str(asset_suffix).lower())}_{kind}"


def run_raster_stream_order_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi_path=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
    source_raster_path=None,
):
    state, district, block, use_precomputed = _normalize_location(
        state, district, block
    )

    asset_suffix, watersheds_gdf = get_roi(
        asset_suffix,
        block,
        district,
        roi_path,
        state,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    layer_name = _stream_order_layer_name(asset_suffix, use_precomputed, "raster")

    output_raster_path = build_output_raster_path(
        layer_name=layer_name,
        output_base_dir=LOCAL_STREAM_ORDER_OUTPUT,
        state=state,
        district=district,
        block=block,
    )

    print("Clipping stream order raster...")
    clipped_raster_path = clip_raster_with_roi(
        roi_gdf=watersheds_gdf,
        raster_path=source_raster_path or PAN_INDIA_STREAM_ORDER_PATH,
        output_path=output_raster_path,
        raster_label="Stream Order Raster",
    )
    print(f"Saved clipped stream order raster to: {clipped_raster_path}")

    raster_layer_id = None
    if sync_layer_metadata and state and district and block:
        raster_layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=str(clipped_raster_path),
            dataset_name=STREAM_ORDER_DATASET_NAME,
            misc={"is_generated_locally": True},
        )

    if push_to_geoserver:
        response = queue_local_raster_for_geoserver(
            file_path=str(clipped_raster_path),
            layer_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            style_name=STREAM_ORDER_STYLE_NAME,
            layer_id=raster_layer_id,
        )
        if response.get("status_code") != 202:
            return False, clipped_raster_path
        print(f"Queued raster {layer_name} for GeoServer.")

    return True, clipped_raster_path


def run_vector_stream_order_local(
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

    state, district, block, use_precomputed = _normalize_location(
        state, district, block
    )

    asset_suffix, watersheds_gdf = get_roi(
        asset_suffix,
        block,
        district,
        roi_path,
        state,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    layer_name = _stream_order_layer_name(asset_suffix, use_precomputed, "vector")

    print("Computing stream order class area percentages per watershed...")
    vector_result_gdf = compute_categorical_raster_percentages_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        raster_path=raster_path,
        class_definitions=STREAM_ORDER_CLASSES,
    )
    if vector_result_gdf.crs is not None and vector_result_gdf.crs.to_epsg() != 4326:
        vector_result_gdf = vector_result_gdf.to_crs("EPSG:4326")

    output_vector_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_STREAM_ORDER_OUTPUT,
    )

    vector_asset_id = write_vector_output(
        gdf=vector_result_gdf,
        output_path=output_vector_path,
        layer_name=layer_name,
    )
    print(f"Saved local stream order vector: {vector_asset_id}")

    vector_layer_id = None
    if sync_layer_metadata and state and district and block:
        vector_layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=vector_asset_id,
            dataset_name=STREAM_ORDER_DATASET_NAME,
            misc={"is_generated_locally": True},
        )

    if push_to_geoserver:
        response = queue_local_vector_for_geoserver(
            path=os.path.splitext(vector_asset_id)[0],
            layer_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            file_type="gpkg",
            layer_id=vector_layer_id,
        )
        if response.get("status_code") != 202:
            return False
        print(f"Queued vector {layer_name} for GeoServer.")

    return True


@app.task(bind=True)
def generate_stream_order_local(
    self,
    state=None,
    district=None,
    block=None,
    gee_account_id=None,
    asset_suffix=None,
    roi_path=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
    source_raster_path=None,
):
    raster_ok, clipped_raster_path = run_raster_stream_order_local(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        roi_path=roi_path,
        precomputed_roi_dir=precomputed_roi_dir,
        push_to_geoserver=push_to_geoserver,
        sync_layer_metadata=sync_layer_metadata,
        source_raster_path=source_raster_path,
    )

    if not raster_ok or not clipped_raster_path:
        print("Raster stage failed — skipping vector stage.")
        return False

    vector_ok = run_vector_stream_order_local(
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
