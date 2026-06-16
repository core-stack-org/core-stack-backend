from pathlib import Path

import geopandas as gpd
from django.conf import settings
from utilities.gee_utils import valid_gee_text

from computing.config_loader import PRECOMPUTED_TEHSIL_WATERSHED_DIR

PRECOMPUTED_ROI_EXTENSIONS = (".gpkg", ".geojson")
VALID_COMPUTE_TYPES = {"gee", "local"}


def _slug(value, fallback):
    return valid_gee_text(str(value).strip().lower()) or fallback


def validate_geometry(gdf):
    gdf = gdf[gdf.geometry.notna()].copy()
    if gdf.empty:
        return gdf
    invalid = ~gdf.is_valid
    if invalid.any():
        gdf.loc[invalid, "geometry"] = gdf.loc[invalid, "geometry"].buffer(0)
    return gdf[~gdf.geometry.is_empty].copy()


def read_validated_vector_file(path, empty_message):
    gdf = validate_geometry(gpd.read_file(path))
    if gdf.empty:
        raise ValueError(empty_message)
    return gdf


def resolve_precomputed_vector_file(
    state,
    district,
    block,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    extensions=PRECOMPUTED_ROI_EXTENSIONS,
    missing_file_label="Precomputed vector file",
):
    roi_dir = Path(precomputed_roi_dir or PRECOMPUTED_TEHSIL_WATERSHED_DIR)
    state_slug = _slug(state, "unknown_state")
    district_slug = _slug(district, "unknown_district")
    block_slug = _slug(block, "unknown_tehsil")

    expected_paths = [
        roi_dir / state_slug / district_slug / f"{block_slug}{ext}"
        for ext in extensions
    ]
    for path in expected_paths:
        if path.exists():
            return path

    raise FileNotFoundError(
        f"{missing_file_label} not found. "
        f"state={state}, district={district}, block={block}. "
        f"Expected one of: {[str(path) for path in expected_paths]}"
    )


def load_precomputed_roi(
    state,
    district,
    block,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
):
    from utilities.download_gpkg_from_geoserver import generate_gpkg

    try:
        roi_path = resolve_precomputed_vector_file(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
            missing_file_label="Precomputed tehsil watershed file",
        )
    except FileNotFoundError:
        print(f"Precomputed ROI not found for {state}/{district}/{block}. Downloading...")
        generate_gpkg(state=state, district=district, block=block, workspace="mws")
        roi_path = resolve_precomputed_vector_file(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
            missing_file_label="Generated tehsil watershed file",
        )

    roi_gdf = read_validated_vector_file(
        roi_path,
        f"Precomputed ROI file has no valid geometries: {roi_path}",
    )
    print(f"Loaded precomputed ROI file: {roi_path}")
    return roi_gdf


def _build_output_dir(
    output_base_dir,
    state=None,
    district=None,
    block=None,
    custom_subdir="custom",
    block_fallback="unknown_tehsil",
):
    output_base_dir = Path(output_base_dir)
    if state and district and block:
        output_dir = (
            output_base_dir
            / _slug(state, "unknown_state")
            / _slug(district, "unknown_district")
            / _slug(block, block_fallback)
        )
    else:
        output_dir = output_base_dir / custom_subdir
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def build_output_vector_path(
    layer_name,
    state,
    district,
    block,
    output_base_dir,
    custom_subdir="custom",
    block_fallback="unknown_block",
):
    output_dir = _build_output_dir(
        output_base_dir=output_base_dir,
        state=state,
        district=district,
        block=block,
        custom_subdir=custom_subdir,
        block_fallback=block_fallback,
    )
    return output_dir / f"{layer_name}.gpkg"


def write_vector_output(gdf, output_path, layer_name):
    gdf.to_file(output_path, driver="GPKG", layer=layer_name)
    return str(output_path)


def _default_compute_mode():
    return "local" if getattr(settings, "SYNC_LAYER", False) else "gee"


def get_compute_mode(request, default=None):
    if default is None:
        default = _default_compute_mode()
    compute = str(request.data.get("compute") or default).strip().lower()
    if compute not in VALID_COMPUTE_TYPES:
        raise ValueError("compute must be either 'gee' or 'local'")
    return compute


def select_compute_task(compute, gee_task, local_task):
    return gee_task if compute == "gee" else local_task
