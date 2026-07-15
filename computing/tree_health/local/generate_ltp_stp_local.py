"""
This is written to closely follow the original GEE notebook:
    Year
      -> ACZ
          -> District
              -> Clip LULC
              -> Tree mask
              -> Polygonize
              -> Compute patch area
              -> Classify LTP/STP
              -> Rasterize
              -> Save GeoTIFF
"""

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
from rasterio.warp import calculate_default_transform, reproject
from computing.config_loader import LULC_BASE_DIR, PROJECT_ROOT
from rasterio.merge import merge
from glob import glob

TREE_CLASS = 6
AREA_THRESHOLD_HA = 1.0
LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/tree_health/ltp_stp"

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
}


def generate_ltp_stp_local(year):
    """
    Generate LTP/STP rasters for each year, ACZ, and district.
    """

    lulc_file = f"data/base_layers/lulc/lulc_v3_{year}_{year+1}.tif"
    print(f"\nYear: {year} -> LULC file: {lulc_file}")
    if not os.path.exists(lulc_file):
        print("ERROR: LULC file does not exist:", lulc_file)
        return

    district_boundaries = gpd.read_file(
        "data/base_layers/india_district_boundaries.geojson"
    )
    print(
        "Loaded DISTRICTS:",
        len(district_boundaries),
        "rows from data/base_layers/india_district_boundaries.geojson",
    )

    for acz, acronym in ACZS.items():

        print(f"\nProcessing {acz}")

        district_csv = f"data/base_layers/tree_health/Agroclimatic_regions/{acz}.csv"
        district_names = pd.read_csv(district_csv)["Name"].tolist()

        output_dir = os.path.join(
            LOCAL_OUTPUT_BASE_DIR,
            f"ltp_{year}",
            acronym,
        )
        os.makedirs(output_dir, exist_ok=True)

        generate_district_tiff(
            acronym, district_boundaries, district_names, lulc_file, output_dir, year
        )

        merge_district_tiffs(acz, output_dir, year, acronym)


def resample_to_25m(
    clipped,
    transform,
    src_crs,
    nodata,
):
    """
    Resample clipped raster from its native resolution
    to approximately 25 m using MODE resampling.
    """

    # Approximate 25 m in degrees
    target_res = 25.0 / 111320.0

    left = transform.c
    top = transform.f

    right = left + clipped.shape[1] * transform.a
    bottom = top + clipped.shape[0] * transform.e

    dst_transform, dst_width, dst_height = calculate_default_transform(
        src_crs,
        src_crs,
        clipped.shape[1],
        clipped.shape[0],
        left,
        bottom,
        right,
        top,
        resolution=target_res,
    )

    dst = np.full(
        (dst_height, dst_width),
        nodata,
        dtype=clipped.dtype,
    )

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
    acronym, district_boundaries, district_names, lulc_file, output_dir, year
):

    for district_name in district_names:

        print("  Processing district:", district_name)
        district = district_boundaries[district_boundaries["Name"] == district_name]

        if district.empty:
            print("  ERROR: District not found in boundary file.")
            continue

        with rasterio.open(lulc_file) as src:
            # print("Opened LULC raster. CRS:", src.crs)
            # print("District CRS:", district.crs)

            if district.crs != src.crs:
                district = district.to_crs(src.crs)
                # print("Reprojected district to raster CRS.")

            clipped, transform = mask(
                src,
                district.geometry,
                crop=True,
                filled=False,
                indexes=1,
            )
            # print("Clipped raster shape:", clipped.shape)
            # print("Masked pixels count:", np.count_nonzero(~clipped.mask))

            profile = src.profile.copy()

        nodata = src.nodata
        if nodata is None:
            nodata = 255

        # print("Original shape:", clipped.shape)
        # print("Original resolution:", src.res)

        resampled, transform = resample_to_25m(
            clipped.filled(nodata),
            transform,
            src.crs,
            nodata,
        )

        tree = (resampled == TREE_CLASS).astype(np.uint8)

        # print("Resampled shape:", tree.shape)
        # print("Tree pixels:", np.count_nonzero(tree))
        # print("Tree pixels count:", np.count_nonzero(tree == 1))

        polygons = []

        for geom, value in shapes(
            tree,
            mask=tree == 1,
            transform=transform,
            connectivity=8,
        ):
            if value == 1:
                polygons.append(shape(geom))

        # print("Polygonized tree patches:", len(polygons))
        if len(polygons) == 0:
            print("No tree patches found for this district.")
            continue

        gdf = gpd.GeoDataFrame(
            geometry=polygons,
            crs=profile["crs"],
        )

        projected = gdf.to_crs(gdf.estimate_utm_crs())

        gdf["area_ha"] = projected.area / 10000.0

        gdf["large_tree_patch"] = (gdf["area_ha"] >= AREA_THRESHOLD_HA).astype(np.uint8)

        gdf = gdf.to_crs(profile["crs"])

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

        output = np.full(tree.shape, 255, dtype=np.uint8)
        output[tree == 1] = ltp[tree == 1]

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

        outfile = os.path.join(
            output_dir,
            f"ltp_{year}_{acronym}_{re.sub('[^A-Za-z0-9]', '', district_name)}.tif",
        )

        with rasterio.open(outfile, "w", **profile) as dst:
            dst.write(output, 1)

        print("Saved:", outfile)


def merge_district_tiffs(acz, output_dir, year, acronym):
    print(f"\nMerging district rasters for {acz}...")

    district_tiffs = sorted(glob(os.path.join(output_dir, "*.tif")))

    if len(district_tiffs) == 0:
        print("No district rasters found.")
        return

    src_files = [rasterio.open(fp) for fp in district_tiffs]

    mosaic, out_transform = merge(
        src_files,
        method="first",
    )

    out_meta = src_files[0].meta.copy()

    out_meta.update(
        {
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_transform,
            "compress": "lzw",
        }
    )

    acz_output = os.path.join(
        output_dir,
        f"ltp_{year}_{acronym}.tif",
    )

    with rasterio.open(acz_output, "w", **out_meta) as dst:
        dst.write(mosaic)

    for src in src_files:
        src.close()

    print(f"Saved ACZ raster: {acz_output}")
