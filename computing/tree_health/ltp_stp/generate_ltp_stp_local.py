import os
import re

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
from rasterio.features import shapes, rasterize
from shapely.geometry import shape
from rasterio.enums import Resampling
from rasterio.warp import reproject
from computing.config_loader import LULC_BASE_DIR, PROJECT_ROOT
from rasterio.merge import merge
from glob import glob
from math import floor
from rasterio.transform import Affine

from nrm_app.celery import app

"""
Generate Long-Term Tree Patches (LTP) and Short-Term Tree Patches (STP) rasters.

This script processes satellite imagery and land use data to identify and classify 
tree patches based on size thresholds. It follows this workflow:
    Year
      -> ACZ (Agroclimatic Zone)
          -> District
              -> Clip LULC (Land Use Land Cover) data
              -> Extract tree mask
              -> Polygonize tree patches
              -> Compute patch area in hectares
              -> Classify as LTP (Large Tree Patches) or STP (Small Tree Patches)
              -> Rasterize classified patches
              -> Save GeoTIFF output
"""


# LULC classification value for tree/forest cover
TREE_CLASS = 6

# Minimum area threshold (in hectares) to classify a patch as Large Tree Patch (LTP)
# Patches below this are classified as Short-term Tree Patches (STP)
AREA_THRESHOLD_HA = 1.0

# Base output directory for LTP/STP raster files
LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/base_layers/tree_health/ltp_stp"


# Dictionary mapping Agroclimatic Zones (ACZ) to their acronyms
# Other ACZs are commented out but available for use

ACZS = {
    "Eastern Plateau & Hills Region": "EPAHR",
    "Southern Plateau and Hills Region": "SPAHR",
    "East Coast Plains & Hills Region": "ECPHR",
    "Western Plateau and Hills Region": "WPAHR",
    "Central Plateau & Hills Region": "CPAHR",
    "Lower Gangetic Plain Region": "LGPR",
    "Middle Gangetic Plain Region": "MGPR",
    "Eastern Himalayan Region": "EHR",
    "Western Himalayan Region": "WHR",
    "Upper Gangetic Plain Region": "UGPR",
    "Trans Gangetic Plain Region": "TGPR",
    "West Coast Plains & Ghat Region": "WCPGR",
    "Gujarat Plains & Hills Region": "GPHR",
    "Western Dry Region": "WDR",
}


@app.task(bind=True)
def generate_ltp_stp_local(self, start_year, end_year, scale=25):
    """
    Main function to generate LTP/STP classification rasters.

    Processes LULC data for a range of years across multiple ACZs and districts.
    Creates both starting year and ending year datasets using modal LULC values
    from a 3-year window (current year and +/- 1 year for robustness).

    Args:
        start_year: Beginning year for analysis period
        end_year: End year for analysis period
        scale: Scaling factor for LTP/STP
    """
    # Load district boundary geometries from GeoJSON
    district_boundaries = gpd.read_file(
        "data/base_layers/india_district_boundaries.geojson"
    )
    print(
        "Loaded DISTRICTS:",
        len(district_boundaries),
        "rows from data/base_layers/india_district_boundaries.geojson",
    )

    # Create 3-year windows for modal LULC calculation (current year ± 1 year)
    # This provides temporal stability in the LULC classification
    start_years = [start_year, start_year + 1, start_year + 2]
    end_years = [end_year, end_year - 1, end_year - 2]

    # Process both start and end year datasets
    for lulc_years in [start_years, end_years]:
        year = lulc_years[0]

        # Iterate through each Agroclimatic Zone
        for acz, acronym in ACZS.items():

            print(f"\nProcessing {acz}")

            # Load district names that belong to this ACZ
            district_csv = (
                f"data/base_layers/tree_health/Agroclimatic_regions/{acz}.csv"
            )
            district_names = pd.read_csv(district_csv)["Name"].tolist()

            # Create output directory for this ACZ and year
            output_dir = os.path.join(
                LOCAL_OUTPUT_BASE_DIR,
                f"{scale}",
                f"ltp_{year}",
                acronym,
            )
            os.makedirs(output_dir, exist_ok=True)

            # Open LULC source files for the 3-year window
            lulc_sources = []

            for y in lulc_years:
                lulc_file = f"data/base_layers/lulc/lulc_v3_{y}_{y+1}.tif"
                lulc_sources.append(rasterio.open(lulc_file))

            # Generate LTP/STP rasters for each district in the ACZ
            generate_district_tiff(
                acronym,
                district_boundaries,
                district_names,
                lulc_sources,
                output_dir,
                year,
                scale,
            )

            # Merge individual district rasters into single ACZ mosaic
            merge_district_tiffs(acz, output_dir, year, acronym)

            # Close all LULC source files
            for src in lulc_sources:
                src.close()


def get_lulc_mode(lulc_sources, district, scale):
    """
    Extract and process LULC data for a district.

    Computes the modal (most common) LULC class from 3-year LULC stack,
    resamples to 25m resolution, and extracts tree class pixels.

    Args:
        lulc_sources: List of opened rasterio LULC source files (3 files)
        district: GeoDataFrame containing the district geometry
        scale: Scaling factor for LTP/STP

    Returns:
        tree: Binary array (1 = tree, 0 = non-tree)
        transform: Affine transform of the output raster
        profile: Rasterio profile of the source data
    """
    lulc_arrays = []

    profile = None
    transform = None
    nodata = None
    crs = None
    source_transform = None

    # Clip each LULC source to district boundaries and collect arrays
    for src in lulc_sources:
        roi = district
        # Reproject district to match source CRS if needed
        if roi.crs != src.crs:
            roi = roi.to_crs(src.crs)

        # Clip LULC raster to district boundaries
        clipped, transform = mask(
            src,
            roi.geometry,
            crop=True,
            filled=False,
            indexes=1,
        )

        # Extract metadata from first source
        if profile is None:
            profile = src.profile.copy()
            nodata = src.nodata if src.nodata is not None else 255
            crs = src.crs
            source_transform = src.transform

        # Fill masked pixels with nodata value
        arr = clipped.filled(nodata)
        lulc_arrays.append(arr)

    # Stack the three LULC arrays
    stack = np.stack(lulc_arrays)

    # Compute modal LULC using the three-year stack
    # Returns the most frequent class value; uses tie-breaking logic
    a = stack[0]
    b = stack[1]
    c = stack[2]

    # Nested where conditions to find the mode:
    # If a==b, use a; else if a==c, use a; else if b==c, use b; else use b as tie-breaker
    modal = np.where(
        a == b,
        a,
        np.where(
            a == c,
            a,
            np.where(
                b == c,
                b,
                b,  # tie breaker - default to middle year
            ),
        ),
    ).astype(np.uint8)

    if scale == 25:
        # Resample modal LULC from 10m to 25m resolution
        modal, transform = resample_tiff(
            modal,
            transform,
            source_transform,
            crs,
            nodata,
            source_resolution_m=10,
            target_resolution_m=25,
        )

    # Extract only tree class pixels as binary mask
    tree = (modal == TREE_CLASS).astype(np.uint8)
    return tree, transform, profile


def resample_tiff(
    clipped,
    transform,
    source_transform,
    src_crs,
    nodata,
    source_resolution_m,
    target_resolution_m,
):
    """
    Resample raster using MODE resampling.

    Snaps the output to the original LULC grid to ensure alignment with
    other datasets. This maintains consistency across multiple processing runs.

    Args:
        clipped: Input raster array
        transform: Current Affine transform of clipped data
        source_transform: Original Affine transform of LULC source
        src_crs: Coordinate reference system
        nodata: Nodata value
        source_resolution_m: Source resolution in m
        target_resolution_m: Target resolution in m
    Returns:
        dst: Resampled raster array
        dst_transform: Affine transform of output raster
    """
    # Extract coordinates from current transform
    left = transform.c
    top = transform.f

    # Calculate boundaries of the clipped area
    right = left + clipped.shape[1] * transform.a
    bottom = top + clipped.shape[0] * transform.e

    # Extract coordinates from original LULC grid
    # (ensures alignment with source resolution)
    src_left = source_transform.c
    src_top = source_transform.f

    # Calculate resampling scale factor (2.5x for 10m -> 25m)
    scale = target_resolution_m / source_resolution_m

    # Calculate target pixel resolution
    target_res_x = abs(source_transform.a) * scale
    target_res_y = abs(source_transform.e) * scale

    # Snap to the original LULC grid - align new pixels to source grid
    new_left = src_left + floor((left - src_left) / target_res_x) * target_res_x
    new_top = src_top - floor((src_top - top) / target_res_y) * target_res_y

    # Calculate output raster dimensions
    dst_width = int(np.ceil((right - new_left) / target_res_x))
    dst_height = int(np.ceil((new_top - bottom) / target_res_y))

    # Create output Affine transform
    dst_transform = Affine(
        target_res_x,
        0,
        new_left,
        0,
        -target_res_y,
        new_top,
    )

    # Initialize output array filled with nodata value
    dst = np.full(
        (dst_height, dst_width),
        nodata,
        dtype=clipped.dtype,
    )

    # Reproject and resample using MODE resampling
    # MODE resampling preserves the most common value in each output pixel
    reproject(
        source=clipped,
        destination=dst,
        src_transform=transform,
        src_crs=src_crs,
        dst_transform=dst_transform,
        dst_crs=src_crs,
        src_nodata=nodata,
        dst_nodata=nodata,
        resampling=Resampling.mode,
    )
    return dst, dst_transform


def generate_district_tiff(
    acronym, district_boundaries, district_names, lulc_sources, output_dir, year, scale
):
    """
    Generate LTP/STP classification rasters for each district.

    For each district, extracts tree pixels, polygonizes them, calculates
    patch areas, and classifies based on size threshold into LTP (Large)
    or STP (Small) tree patches.

    Args:
        acronym: ACZ acronym for file naming
        district_boundaries: GeoDataFrame of all district boundaries
        district_names: List of district names to process
        lulc_sources: List of opened LULC rasterio objects
        output_dir: Directory to save output GeoTIFFs
        year: Year for file naming
        scale: Scale factor for patch size
    """
    for district_name in district_names:
        print("  Processing district:", district_name)

        # Extract the specific district from boundary data
        district = district_boundaries[district_boundaries["Name"] == district_name]

        if district.empty:
            print("  ERROR: District not found in boundary file.")
            continue

        # Get modal LULC tree mask for the district
        tree, transform, profile = get_lulc_mode(lulc_sources, district, scale)

        # Polygonize tree patches - convert raster to vector polygons
        # shapes() returns (geometry, value) tuples for connected regions
        polygons = []

        for geom, value in shapes(
            tree,
            mask=tree == 1,  # Only polygonize tree pixels
            transform=transform,
            connectivity=8,  # Use 8-connectivity (diagonal neighbors included)
        ):
            if value == 1:
                polygons.append(shape(geom))

        if len(polygons) == 0:
            print("No tree patches found for this district.")
            continue

        # Create GeoDataFrame from polygons
        gdf = gpd.GeoDataFrame(
            geometry=polygons,
            crs=profile["crs"],
        )

        # Convert to UTM for accurate area calculation
        utm = gdf.estimate_utm_crs()
        projected = gdf.to_crs(utm)

        # Calculate patch area in hectares
        gdf["area_ha"] = projected.area / 10000.0

        # Classify patches as Large Tree Patch (1) or Small Tree Patch (0)
        # based on area threshold
        gdf["large_tree_patch"] = (gdf["area_ha"] >= AREA_THRESHOLD_HA).astype(np.uint8)

        # Convert back to original CRS
        gdf = gdf.to_crs(profile["crs"])

        # Rasterize the LTP/STP classification back to raster format
        ltp = rasterize(
            (
                (geom, value)
                for geom, value in zip(
                    gdf.geometry,
                    gdf.large_tree_patch,
                )
            ),
            out_shape=tree.shape,
            transform=transform,
            fill=0,
            dtype=np.uint8,
        )

        # Create output raster:
        # 255 = nodata, 1 = large tree patch, 0 = small tree patch/non-tree
        output = np.full(tree.shape, 255, dtype=np.uint8)
        output[tree == 1] = ltp[tree == 1]

        # Update raster profile with output specifications
        profile.update(
            driver="GTiff",
            height=output.shape[0],
            width=output.shape[1],
            transform=transform,
            count=1,
            dtype="uint8",
            nodata=255,
            compress="lzw",
        )

        # Generate output filename with district name
        outfile = os.path.join(
            output_dir,
            f"ltp_{year}_{acronym}_{re.sub('[^A-Za-z0-9]', '', district_name)}.tif",
        )

        # Write raster to GeoTIFF file
        with rasterio.open(outfile, "w", **profile) as dst:
            dst.write(output, 1)

        print("Saved:", outfile)


def merge_district_tiffs(acz, output_dir, year, acronym):
    """
    Merge individual district LTP/STP rasters into a single ACZ mosaic.

    Combines all district GeoTIFFs for an ACZ and year into a single
    seamless raster using the 'first' method (no priority/overlap handling).

    Args:
        acz: Full name of Agroclimatic Zone (for logging)
        output_dir: Directory containing district rasters
        year: Year for file naming
        acronym: ACZ acronym for output filename
    """
    print(f"\nMerging district rasters for {acz}...")

    # Find all district raster files in the output directory
    district_tiffs = sorted(glob(os.path.join(output_dir, "*.tif")))

    if len(district_tiffs) == 0:
        print("No district rasters found.")
        return

    # Open all district rasters
    src_files = [rasterio.open(fp) for fp in district_tiffs]

    # Merge rasters using the 'first' method
    # (uses first valid pixel from overlapping areas)
    mosaic, out_transform = merge(
        src_files,
        method="first",
    )

    # Copy metadata from first source file
    out_meta = src_files[0].meta.copy()

    # Update metadata for merged output
    out_meta.update(
        {
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_transform,
            "compress": "lzw",
        }
    )

    # Generate output filename for ACZ-level mosaic
    acz_output = os.path.join(
        output_dir,
        f"ltp_{year}_{acronym}.tif",
    )

    # Write merged raster to GeoTIFF
    with rasterio.open(acz_output, "w", **out_meta) as dst:
        dst.write(mosaic)

    # Close all source files
    for src in src_files:
        src.close()

    print(f"Saved ACZ raster: {acz_output}")
