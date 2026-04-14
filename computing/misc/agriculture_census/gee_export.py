"""
GEE Export for Agriculture Census

Takes the matched agriculture census data, joins it with SOI tehsil boundary
geometries, and publishes the enriched vector layer as an Earth Engine asset.

The output FeatureCollection has per-tehsil polygons with properties:
    - crop_name, area_hectares (for each crop)
    - matched_tehsil, match_score
    - state, district, tehsil

This produces a tehsil-level vectorized crop map that can be used
in the Know Your Landscape dashboard and other downstream apps.
"""

import ee
import geopandas as gpd
import pandas as pd

from utilities.gee_utils import (
    ee_initialize,
    gdf_to_ee_fc,
    export_vector_asset_to_gee,
    check_task_status,
    is_gee_asset_exists,
    make_asset_public,
    valid_gee_text,
    get_gee_asset_path,
)
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from nrm_app.celery import app


def enrich_tehsil_boundaries(matched_csv_path, boundary_geojson_path):
    """Join matched agriculture census data with SOI tehsil boundaries.

    Args:
        matched_csv_path: Path to matched agriculture census CSV
            (output of pipeline.py with matched_tehsil column)
        boundary_geojson_path: Path to SOI tehsil boundary GeoJSON

    Returns:
        GeoDataFrame with tehsil polygons enriched with crop attributes
    """
    census_df = pd.read_csv(matched_csv_path)
    boundaries_gdf = gpd.read_file(boundary_geojson_path)

    # Standardize boundary column names
    boundaries_gdf.columns = [
        c.strip().lower().replace(" ", "_") for c in boundaries_gdf.columns
    ]

    # Find tehsil name column in boundaries
    tehsil_col = None
    for col in boundaries_gdf.columns:
        if "tehsil" in col.lower():
            tehsil_col = col
            break

    if tehsil_col is None:
        raise ValueError(
            "Could not find tehsil column in boundary file. "
            f"Available: {list(boundaries_gdf.columns)}"
        )

    # Filter to matched records only
    matched = census_df[census_df["match_type"].isin(["exact", "fuzzy"])].copy()

    if matched.empty:
        print("No matched records found.")
        return gpd.GeoDataFrame()

    # Normalize for join
    matched["_join_key"] = matched["matched_tehsil"].str.strip().str.lower()
    boundaries_gdf["_join_key"] = boundaries_gdf[tehsil_col].str.strip().str.lower()

    # Join
    enriched = boundaries_gdf.merge(matched, on="_join_key", how="inner")
    enriched = enriched.drop(columns=["_join_key"], errors="ignore")

    # Ensure EPSG:4326
    if enriched.crs is None:
        enriched = enriched.set_crs("EPSG:4326")
    elif enriched.crs.to_epsg() != 4326:
        enriched = enriched.to_crs("EPSG:4326")

    print(f"Enriched {len(enriched)} tehsil polygons with crop data")
    return enriched


def export_to_geojson(enriched_gdf, output_path):
    """Export enriched GeoDataFrame to GeoJSON."""
    enriched_gdf.to_file(output_path, driver="GeoJSON")
    print(f"Exported GeoJSON to {output_path}")


@app.task(bind=True)
def publish_agri_census_to_gee(
    self,
    matched_csv_path,
    boundary_geojson_path,
    state,
    district,
    block,
    gee_account_id,
):
    """Celery task to publish agriculture census as a GEE vector asset.

    Workflow:
        1. Enrich tehsil boundaries with crop data
        2. Convert to ee.FeatureCollection
        3. Export to GEE as vector asset
        4. Sync to GeoServer
        5. Save layer info to DB
    """
    ee_initialize(gee_account_id)

    description = (
        f"agri_census_{valid_gee_text(district)}_{valid_gee_text(block)}"
    )
    asset_id = get_gee_asset_path(state, district, block) + description

    if is_gee_asset_exists(asset_id):
        print(f"Asset already exists: {asset_id}")
        return

    # Step 1: Enrich boundaries
    print("Enriching tehsil boundaries with crop data...")
    enriched_gdf = enrich_tehsil_boundaries(
        matched_csv_path, boundary_geojson_path
    )

    if enriched_gdf.empty:
        print("No matched tehsils found. Skipping.")
        return

    # Step 2: Convert to FeatureCollection
    print("Converting to Earth Engine FeatureCollection...")
    fc = gdf_to_ee_fc(enriched_gdf)

    # Step 3: Export to GEE
    print(f"Exporting to GEE asset: {asset_id}")
    task_id = export_vector_asset_to_gee(fc, description, asset_id)

    if task_id:
        check_task_status(task_id)
        make_asset_public(asset_id)
        print(f"Published agriculture census asset: {asset_id}")

    # Step 4: Sync to GeoServer
    layer_name = (
        valid_gee_text(district) + "_" + valid_gee_text(block) + "_agri_census"
    )
    sync_fc_to_geoserver(asset_id, layer_name)

    # Step 5: Save to DB
    save_layer_info_to_db(
        state=state,
        district=district,
        block=block,
        layer_name=layer_name,
        dataset_name="Agriculture Census",
        metadata={
            "source": "agcensus.da.gov.in",
            "description": "Tehsil-level crop type and area data",
        },
    )
    update_layer_sync_status(layer_name, status="synced")
    print("Done.")
