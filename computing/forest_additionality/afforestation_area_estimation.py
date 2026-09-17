"""Observed afforestation area estimation.

This module calculates afforestation area only from the observed afforestation raster,
without using any modelled density or probability surface.
"""

import numpy as np
import pandas as pd
import rasterio


def get_afforestation_area_estimation(
    district_name, start_year, mid_pt, end_year, DATA_DIR
):
    """Return observed afforestation area in hectares for the selected period.

    The afforestation raster is generated from the forest-cover change analysis and marks
    pixels that transitioned from non-forest to forest during the period. The function
    restricts the assessment to pixels that were non-forest at the start of the period and
    fall within the valid jurisdiction mask.
    """
    gt_tif = DATA_DIR + f"/afforestation_{start_year}_{end_year}.tif"
    forest_start_tif = DATA_DIR + f"/{district_name}_{start_year}.tif"
    jurisdiction_tif = DATA_DIR + f"/{district_name}_jurisdiction_mask.tif"

    with (
        rasterio.open(gt_tif) as gt_src,
        rasterio.open(forest_start_tif) as forest_src,
        rasterio.open(jurisdiction_tif) as jurisdiction_src,
    ):
        raster_sources = {
            "observed afforestation": gt_src,
            "baseline non-forest mask": forest_src,
            "jurisdiction mask": jurisdiction_src,
        }
        reference_shape = (gt_src.height, gt_src.width)

        for source_name, source in raster_sources.items():
            if (
                source.crs != gt_src.crs
                or (source.height, source.width) != reference_shape
                or not source.transform.almost_equals(gt_src.transform)
            ):
                raise ValueError(
                    f"{source_name} must use the same CRS, shape, and transform "
                    "as the observed-afforestation raster."
                )

        baseline_forest = forest_src.read(1)
        jurisdiction = jurisdiction_src.read(1)
        gt = gt_src.read(1)

    pixel_area_ha = abs(gt_src.transform.a * gt_src.transform.e) / 10_000

    # Only pixels that were non-forest at the beginning of the period and are within the
    # valid jurisdiction are eligible for observed afforestation area estimation.
    valid_mask = (baseline_forest == 0) & (jurisdiction > 0) & np.isfinite(gt)

    total_pixels = np.sum(valid_mask)
    total_area_ha = total_pixels * pixel_area_ha

    gt_bin = (gt == 1).astype(np.uint8)
    observed_afforestation_area_ha = np.sum(gt_bin[valid_mask] * pixel_area_ha)

    observed_fraction = (
        observed_afforestation_area_ha / total_area_ha if total_area_ha > 0 else 0.0
    )

    result = {
        "district_name": district_name,
        "start_year": start_year,
        "mid_pt": mid_pt,
        "end_year": end_year,
        "total_eligible_area_ha": float(total_area_ha),
        "observed_afforestation_area_ha": float(observed_afforestation_area_ha),
        "observed_afforestation_fraction": float(observed_fraction),
    }

    out_csv = DATA_DIR + "/observed_afforestation_area.csv"
    pd.DataFrame([result]).to_csv(out_csv, index=False)

    print("\n=== Observed Afforestation Area Estimates (ha) ===")
    print(result)

    return result
