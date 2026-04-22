"""
GEE Export for Livestock Census

Takes the matched livestock census data, joins it with village boundary
geometries, and publishes the enriched vector layer as an Earth Engine asset.

The output FeatureCollection has per-village polygons with properties:
    - cattle, buffalo, sheep, goat, pig, total_livestock
    - census2011_id, lgd_village_id
    - state_name, district_name, block_name, village_name

This enables filtering/aggregation by livestock type in GEE and
downstream apps like Know Your Landscape.
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

LIVESTOCK_TYPES = ["cattle", "buffalo", "sheep", "goat", "pig"]


def enrich_village_boundaries(matched_csv_path, boundary_shapefile_path):
    """Join matched livestock data with village boundary geometries.

    Args:
        matched_csv_path: Path to the matched livestock CSV
            (output of pipeline.py with census2011_id column)
        boundary_shapefile_path: Path to village boundary shapefile/geojson

    Returns:
        GeoDataFrame with village polygons enriched with livestock attributes
    """
    livestock_df = pd.read_csv(matched_csv_path)
    boundaries_gdf = gpd.read_file(boundary_shapefile_path)

    # Standardize boundary column names
    boundaries_gdf.columns = [
        c.strip().lower().replace(" ", "_") for c in boundaries_gdf.columns
    ]

    # Find the census ID column in boundary data
    census_col = None
    for col in boundaries_gdf.columns:
        if "census2011" in col or "census_2011" in col:
            census_col = col
            break

    if census_col is None:
        raise ValueError(
            "Could not find census2011 ID column in boundary shapefile. "
            f"Available columns: {list(boundaries_gdf.columns)}"
        )

    # Filter to only matched records
    matched = livestock_df[livestock_df["match_type"].isin(["exact", "fuzzy"])].copy()
    matched["census2011_id"] = matched["census2011_id"].astype(str)
    boundaries_gdf[census_col] = boundaries_gdf[census_col].astype(str)

    # Aggregate livestock counts per census ID (in case of duplicates)
    agg_cols = {col: "sum" for col in LIVESTOCK_TYPES if col in matched.columns}
    agg_cols["total_livestock"] = "sum"
    agg_cols["state_name"] = "first"
    agg_cols["district_name"] = "first"
    agg_cols["block_name"] = "first"
    agg_cols["village_name"] = "first"

    livestock_agg = matched.groupby("census2011_id", as_index=False).agg(agg_cols)

    # Join with boundaries
    enriched = boundaries_gdf.merge(
        livestock_agg,
        left_on=census_col,
        right_on="census2011_id",
        how="inner",
    )

    # Ensure CRS is EPSG:4326
    if enriched.crs is None:
        enriched = enriched.set_crs("EPSG:4326")
    elif enriched.crs.to_epsg() != 4326:
        enriched = enriched.to_crs("EPSG:4326")

    # Keep only relevant columns + geometry
    keep_cols = [
        "geometry", "census2011_id", "state_name", "district_name",
        "block_name", "village_name",
    ] + [c for c in LIVESTOCK_TYPES if c in enriched.columns] + ["total_livestock"]

    enriched = enriched[[c for c in keep_cols if c in enriched.columns]]

    print(f"Enriched {len(enriched)} village polygons with livestock data")
    return enriched


def export_to_geojson(enriched_gdf, output_path):
    """Export enriched GeoDataFrame to GeoJSON for local use.

    Args:
        enriched_gdf: GeoDataFrame from enrich_village_boundaries()
        output_path: Path to write GeoJSON file
    """
    enriched_gdf.to_file(output_path, driver="GeoJSON")
    print(f"Exported GeoJSON to {output_path}")


@app.task(bind=True)
def publish_livestock_census_to_gee(
    self,
    matched_csv_path,
    boundary_shapefile_path,
    state,
    district,
    block,
    gee_account_id,
):
    """Celery task to publish livestock census as a GEE vector asset.

    Workflow:
        1. Enrich village boundaries with livestock data
        2. Convert to ee.FeatureCollection
        3. Export to GEE as a vector asset
        4. Sync to GeoServer
        5. Save layer info to DB

    Args:
        matched_csv_path: Path to matched livestock CSV
        boundary_shapefile_path: Path to village boundary shapefile
        state, district, block: Location identifiers
        gee_account_id: GEE service account ID
    """
    ee_initialize(gee_account_id)

    description = (
        f"livestock_census_{valid_gee_text(district)}_{valid_gee_text(block)}"
    )
    asset_id = (
        get_gee_asset_path(state, district, block) + description
    )

    if is_gee_asset_exists(asset_id):
        print(f"Asset already exists: {asset_id}")
        return

    # Step 1: Enrich boundaries
    print("Enriching village boundaries with livestock data...")
    enriched_gdf = enrich_village_boundaries(
        matched_csv_path, boundary_shapefile_path
    )

    if enriched_gdf.empty:
        print("No matched villages found for this location. Skipping.")
        return

    # Step 2: Convert to ee.FeatureCollection
    print("Converting to Earth Engine FeatureCollection...")
    fc = gdf_to_ee_fc(enriched_gdf)

    # Step 3: Export to GEE
    print(f"Exporting to GEE asset: {asset_id}")
    task_id = export_vector_asset_to_gee(fc, description, asset_id)

    if task_id:
        check_task_status(task_id)
        make_asset_public(asset_id)
        print(f"Published livestock census asset: {asset_id}")

    # Step 4: Sync to GeoServer
    layer_name = (
        valid_gee_text(district) + "_" + valid_gee_text(block) + "_livestock_census"
    )
    sync_fc_to_geoserver(asset_id, layer_name)

    # Step 5: Save to DB
    save_layer_info_to_db(
        state=state,
        district=district,
        block=block,
        layer_name=layer_name,
        dataset_name="Livestock Census",
        metadata={
            "source": "DAHD 20th Livestock Census",
            "livestock_types": LIVESTOCK_TYPES,
            "year": 2020,
            "resolution": "village_level",
        },
    )
    update_layer_sync_status(layer_name, status="synced")
    print("Done.")
