"""Estimate deforestation area by comparing observed loss masks with model output rasters.

The script loads the relevant raster layers, keeps only pixels that are both forested at
period start and within the administrative jurisdiction, and then computes the area in
hectares represented by observed and predicted deforestation.
"""

import rasterio
import numpy as np
import pandas as pd
from dataclasses import dataclass

from rasterio.warp import reproject, Resampling


def get_deforestation_area_estimation(
    state_name, start_year, mid_pt, end_year, DATA_DIR
):
    # Directory that contains the generated prediction and mask rasters.
    PRED_DIR = f"{DATA_DIR}/outputs"  # BASE_DIR / "outputs" / "predictions"
    LABEL_BAND = "remapped"  # "9_deforestation"

    @dataclass
    class RunConfig:
        # Configuration for a specific model run being compared against ground truth.
        name: str
        ex_ante: bool
        counterfactual: bool
        udef_arp: bool

    RUNS = [
        # These are historical alternatives that were not active for this run.
        #     RunConfig("rf_ex_ante", True, False, False),
        #     RunConfig("rf_ex_post", False, False, False),
        #     RunConfig("counterfactual_ex_ante", True, True, False),
        #     RunConfig("counterfactual_ex_post", False, True, False),
        # The active scenario currently evaluated for this workflow.
        RunConfig("udef_arp", False, False, True),
    ]

    def evaluate_area(cfg: RunConfig) -> dict:
        # TODO: this year tag is currently hard-coded until the full model pipeline is unified.
        predict_year = "2010_15"

        if cfg.ex_ante:
            suffix = "ex_ante"
        else:
            suffix = "ex_post"

        if cfg.udef_arp:
            pred_tif = PRED_DIR + "/Acre_Adjucted_Density_Map_VP.tif"

        # elif cfg.counterfactual:
        #     pred_tif = PRED_DIR + f"counterfactual_prediction_FULL_{predict_year}_ex_{'ante' if cfg.ex_ante else 'post'}.tif"

        else:
            pred_tif = (
                PRED_DIR + f"/deforestation_prob_{predict_year}_full_{suffix}.tif"
            )

        # gt_tif = (
        #     DATA_DIR + f"/deforestation_map_{start_year}_{mid_pt}_gd.tif"  # TODO
        # )  # f"training_data_x_{predict_year[:4]}_y_{predict_year}.tif"

        # Ground-truth deforestation layer for the evaluation period.
        gt_tif = PRED_DIR + f"/deforestation_map_{mid_pt}_{end_year}.tif"

        # A pixel can only be evaluated if it was forest at the start of the
        # evaluation period (T2) and is within the jurisdiction.
        forest_start_tif = PRED_DIR + f"/{state_name}_{mid_pt}.tif"
        jurisdiction_tif = PRED_DIR + f"/{state_name}_jurisidiction_mask.tif"

        with rasterio.open(gt_tif) as gt_src, rasterio.open(pred_tif) as pred_src:

            with (
                rasterio.open(forest_start_tif) as forest_src,
                rasterio.open(jurisdiction_tif) as jurisdiction_src,
            ):
                # All rasters must align spatially before any aggregation is done.
                raster_sources = {
                    "observed deforestation": gt_src,
                    "prediction density": pred_src,
                    "forest at T2": forest_src,
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
                            f"{source_name} must use the same CRS, shape, and "
                            "transform as the observed-deforestation raster."
                        )

                forest_at_period_start = forest_src.read(1)
                jurisdiction = jurisdiction_src.read(1)

            # Convert raster pixel size to hectares for all area calculations.
            pixel_area_ha = abs(gt_src.transform.a * gt_src.transform.e) / 10_000

            # label_idx = list(gt_src.descriptions).index(LABEL_BAND)
            # gt = gt_src.read(label_idx + 1)
            gt = gt_src.read(1)
            # print(f"Ground truth shape: {gt}")

            # valid_mask = ~np.isnan(gt)

            # Restrict the assessment to forest available for loss at T2, within
            # the jurisdiction. Prediction densities use negative values (e.g. -1)
            # as nodata, which must not enter either the area or the sums.
            pred_for_mask = pred_src.read(1)
            eligible_mask = (
                (forest_at_period_start == 1)
                & (jurisdiction > 0)
                & np.isfinite(gt)
                & np.isfinite(pred_for_mask)
                & (pred_for_mask >= 0)
            )
            valid_mask = eligible_mask

            total_pixels = np.sum(valid_mask)
            total_area_ha = total_pixels * pixel_area_ha

            # Convert the observed loss mask to a binary indicator and sum it in ha.
            gt_bin = (gt == 1).astype(np.uint8)
            gt_area = np.sum(gt_bin[valid_mask] * pixel_area_ha)

            if cfg.udef_arp:
                # The UDEF/ARP output is stored as a continuous density value.
                pred_vals = pred_src.read(1)

                if pred_vals.shape != gt.shape:
                    # Reproject the prediction raster to the ground-truth grid if needed.
                    aligned = np.empty_like(gt, dtype=np.float32)
                    print(pred_src.crs, gt_src.crs)
                    reproject(
                        source=rasterio.band(pred_src, 1),
                        destination=aligned,
                        src_transform=pred_src.transform,
                        src_crs=pred_src.crs,
                        dst_transform=gt_src.transform,
                        dst_crs=gt_src.crs,
                        resampling=Resampling.nearest,
                    )

                    pred_vals = aligned

                # Treat negative values as missing data and exclude them from area estimates.
                pred_vals[pred_vals < 0] = 0

                predicted_area = np.nansum(pred_vals[valid_mask])

            else:
                # Standard probability outputs use a band selection that may vary between
                # counterfactual and non-counterfactual model runs.
                if cfg.counterfactual:
                    band_names = list(pred_src.descriptions)
                    if "y_cf" in band_names:
                        band_idx = band_names.index("y_cf") + 1
                    else:
                        band_idx = 2
                else:
                    band_idx = 1

                prob = pred_src.read(band_idx).astype(np.float32)

                prob[np.isnan(prob)] = 0
                prob = np.clip(prob, 0, 1)

                # Sum the probability-weighted footprint over valid pixels to estimate area.
                predicted_area = np.sum(prob[valid_mask] * pixel_area_ha)

            # Compare model-estimated area against the observed deforestation area.
            diff = predicted_area - gt_area
            rel_error = diff / gt_area if gt_area > 0 else 0

            gt_fraction = gt_area / total_area_ha
            pred_fraction = predicted_area / total_area_ha

        return {
            "run": cfg.name,
            "total_area_ha": total_area_ha,
            "predicted_area_ha": predicted_area,
            "ground_truth_area_ha": gt_area,
            "area_difference_ha": diff,
            "relative_error": rel_error,
            "gt_fraction_of_total": gt_fraction,
            "pred_fraction_of_total": pred_fraction,
        }

    results = []

    # Run each configured scenario and store the summary metrics for comparison.
    for cfg in RUNS:
        print(f"Running: {cfg.name}")
        res = evaluate_area(cfg)
        results.append(res)

    df = pd.DataFrame(results)

    # Save the summary metrics to disk for downstream analysis or reporting.
    out_csv = DATA_DIR + "/deforestation_area_estimates.csv"

    df.to_csv(out_csv, index=False)

    print("\n=== Deforestation Area Estimates (ha) ===")
    print(df)
