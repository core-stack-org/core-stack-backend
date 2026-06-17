from pathlib import Path

from computing.config_loader import PROJECT_ROOT
from computing.local_compute_helper import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    build_output_vector_path,
    compute_categorical_raster_areas_for_watersheds,
    load_precomputed_watersheds,
    push_local_vector_to_geoserver,
    read_validated_vector_file,
    write_vector_output,
)
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text

from computing.tree_health.ccd_local import LOCAL_OUTPUT_BASE_DIR as CCD_RASTER_DIR


LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/tree_health/ccd_vector_local"
GEOSERVER_WORKSPACE = "ccd"

CCD_CLASSES = [
    {"value": 0, "label_prefix": "Low_Density_"},
    {"value": 1, "label_prefix": "High_Density_"},
    {"value": 2, "label_prefix": "Missing_Data_"},
]


def _slug(value, fallback):
    if value is None:
        return fallback
    return valid_gee_text(str(value).strip().lower()) or fallback


def _resolve_ccd_output_raster(asset_suffix, year, state=None, district=None, block=None):
    raster_name = f"ccd_raster_{asset_suffix}_{year}.tif"
    if state and district and block:
        path = (
            Path(CCD_RASTER_DIR)
            / _slug(state, "unknown_state")
            / _slug(district, "unknown_district")
            / _slug(block, "unknown_block")
            / raster_name
        )
    else:
        path = Path(CCD_RASTER_DIR) / asset_suffix / raster_name

    if path.exists():
        return str(path)

    raise FileNotFoundError(
        f"Local CCD raster not found for vectorisation: {path}. "
        "Run ccd_local.py first."
    )


def tree_health_ccd_vector_local(
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    start_year=None,
    end_year=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    state = str(state).strip().lower() if state else None
    district = str(district).strip().lower() if district else None
    block = str(block).strip().lower() if block else None
    start_year = int(start_year)
    end_year = int(end_year)

    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    # Vector outputs are generated over watershed polygons, same as reduceRegions in GEE.
    if state and district and block:
        asset_suffix = (
            f"{_slug(district, 'unknown_district')}_"
            f"{_slug(block, 'unknown_block')}"
        )
        result_gdf, _ = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
    else:
        if not roi or not asset_suffix:
            raise ValueError(
                "For non state/district/block runs, both `roi` and "
                "`asset_suffix` are required."
            )
        asset_suffix = _slug(asset_suffix, "custom")
        result_gdf = read_validated_vector_file(
            roi,
            f"ROI file has no valid geometries: {roi}",
        )

    for year in range(start_year, end_year + 1):
        raster_path = _resolve_ccd_output_raster(
            asset_suffix=asset_suffix,
            year=year,
            state=state,
            district=district,
            block=block,
        )
        class_definitions = [
            {"value": item["value"], "label": f"{item['label_prefix']}{year}"}
            for item in CCD_CLASSES
        ]
        print(f"Computing local CCD vector columns for {year}: {raster_path}")
        year_gdf = compute_categorical_raster_areas_for_watersheds(
            watersheds_gdf=result_gdf,
            raster_path=raster_path,
            class_definitions=class_definitions,
        )
        for definition in class_definitions:
            result_gdf[definition["label"]] = year_gdf[definition["label"]]

    layer_name = f"ccd_vector_{asset_suffix}_{start_year}_{end_year}"
    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        custom_subdir=asset_suffix,
    )
    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local CCD vector: {asset_id}")

    if push_to_geoserver:
        res = push_local_vector_to_geoserver(
            path=asset_id,
            layer_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            file_type="gpkg",
        )
        print(f"GeoServer response for {layer_name}: {res}")
        if not isinstance(res, dict) or res.get("status_code") not in (200, 201):
            return False

    if sync_layer_metadata and state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Ccd Vector",
            misc={
                "start_year": start_year,
                "end_year": end_year,
                "is_generated_locally": True,
            },
            algorithm="local_ccd_vector",
            algorithm_version="local-1.0",
        )
        if layer_id and push_to_geoserver:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)

    return True
