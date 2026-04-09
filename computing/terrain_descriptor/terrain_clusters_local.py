import os
import shutil
import tempfile
import zipfile

from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text

from computing.local_compute_helper import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    build_output_vector_path,
    compute_terrain_properties_for_watersheds,
    load_precomputed_watersheds,
    resolve_clipped_terrain_raster_path,
    write_vector_output,
)


CLIPPED_TERRAIN_RASTER_DIR = "data/terrain/fabdem_local"
LOCAL_CLUSTER_OUTPUT_DIR = "data/terrain/terrain_clusters_local"
GEOSERVER_WORKSPACE = "terrain"


def _push_vector_to_geoserver(gdf, layer_name):
    from utilities.geoserver_utils import Geoserver

    temp_dir = tempfile.mkdtemp(prefix=f"{layer_name}_")
    gpkg_path = os.path.join(temp_dir, f"{layer_name}.gpkg")
    zip_path = os.path.join(temp_dir, f"{layer_name}.zip")

    try:
        gdf.to_file(gpkg_path, driver="GPKG", layer=layer_name)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(gpkg_path, arcname=os.path.basename(gpkg_path))

        geo = Geoserver()
        geo.delete_vector_store(workspace=GEOSERVER_WORKSPACE, store=layer_name)
        response = geo.create_shp_datastore(
            path=zip_path,
            store_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            file_extension="gpkg",
        )
        return response
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def run_terrain_clusters_local(
    state,
    district,
    block,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    clipped_raster_dir=CLIPPED_TERRAIN_RASTER_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=False,
):
    state = str(state).strip()
    district = str(district).strip()
    block = str(block).strip()

    layer_name = (
        f"{valid_gee_text(str(district).strip().lower())}_"
        f"{valid_gee_text(str(block).strip().lower())}_cluster"
    )

    watersheds_gdf, watershed_source = load_precomputed_watersheds(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    clipped_raster_path = resolve_clipped_terrain_raster_path(
        state=state,
        district=district,
        block=block,
        clipped_raster_dir=clipped_raster_dir,
    )
    print(f"Using clipped terrain raster: {clipped_raster_path}")

    terrain_clusters_gdf = compute_terrain_properties_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        raster_path=clipped_raster_path,
    )
    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_CLUSTER_OUTPUT_DIR,
        block_fallback="unknown_tehsil",
    )
    local_vector_path = write_vector_output(
        gdf=terrain_clusters_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local terrain cluster vector: {local_vector_path}")

    if push_to_geoserver:
        try:
            geoserver_response = _push_vector_to_geoserver(
                gdf=terrain_clusters_gdf,
                layer_name=layer_name,
            )
            print(f"GeoServer response: {geoserver_response}")
        except Exception as error:
            print(f"Failed to sync terrain clusters vector to GeoServer: {error}")
            return False

    if sync_layer_metadata:
        from computing.STAC_specs import generate_STAC_layerwise
        from computing.utils import save_layer_info_to_db, update_layer_sync_status

        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=local_vector_path,
            dataset_name="Terrain Vector",
            algorithm="FABDEM",
            algorithm_version="2.0",
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            layer_stac_generated = generate_STAC_layerwise.generate_vector_stac(
                state=state,
                district=district,
                block=block,
                layer_name="terrain_vector",
            )
            update_layer_sync_status(
                layer_id=layer_id,
                is_stac_specs_generated=layer_stac_generated,
            )

    print(f"Completed terrain cluster computation for {state}/{district}/{block}")
    print(f"Watershed boundary source: {watershed_source}")
    return True


def _generate_terrain_clusters_task(
    state,
    district,
    block,
    gee_account_id=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    clipped_raster_dir=CLIPPED_TERRAIN_RASTER_DIR,
):
    _ = gee_account_id
    return run_terrain_clusters_local(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
        clipped_raster_dir=clipped_raster_dir,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )


@app.task(bind=True)
def generate_terrain_clusters(
    self,
    state,
    district,
    block,
    gee_account_id=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    clipped_raster_dir=CLIPPED_TERRAIN_RASTER_DIR,
):
    _ = self
    return _generate_terrain_clusters_task(
        state=state,
        district=district,
        block=block,
        gee_account_id=gee_account_id,
        precomputed_roi_dir=precomputed_roi_dir,
        clipped_raster_dir=clipped_raster_dir,
    )
