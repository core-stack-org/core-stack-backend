import rasterio
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from dataclasses import dataclass

from rasterio.warp import reproject, Resampling

# if len(sys.argv) > 2:
#     state_name = sys.argv[2]
#     start_year = int(sys.argv[3])
#     mid_pt = int(sys.argv[4])
#     end_year = int(sys.argv[5])
# else:
#     state_name = "Dhenkanal"
#     start_year = 2010
#     mid_pt = 2015
#     end_year = 2020


def get_area_estimation(state_name, start_year, mid_pt, end_year, DATA_DIR):
    PRED_DIR = f"{DATA_DIR}"  # BASE_DIR / "outputs" / "predictions"
    LABEL_BAND = "remapped"  # "9_deforestation"

    PIXEL_AREA_HA = 0.09

    @dataclass
    class RunConfig:
        name: str
        ex_ante: bool
        counterfactual: bool
        udef_arp: bool

    RUNS = [
        #     RunConfig("rf_ex_ante", True, False, False),
        #     RunConfig("rf_ex_post", False, False, False),
        #     RunConfig("counterfactual_ex_ante", True, True, False),
        #     RunConfig("counterfactual_ex_post", False, True, False),
        RunConfig("udef_arp", False, False, True),
    ]

    def evaluate_area(cfg: RunConfig) -> dict:
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

        gt_tif = (
            DATA_DIR + f"/deforestation_map_{start_year}_{mid_pt}_gd.tif"
        )  # f"training_data_x_{predict_year[:4]}_y_{predict_year}.tif"

        with rasterio.open(gt_tif) as gt_src, rasterio.open(pred_tif) as pred_src:

            pixel_area_ha = PIXEL_AREA_HA

            label_idx = list(gt_src.descriptions).index(LABEL_BAND)
            gt = gt_src.read(label_idx + 1)
            # gt = gt_src.read()
            # print(f"Ground truth shape: {gt}")

            valid_mask = ~np.isnan(gt)

            total_pixels = np.sum(valid_mask)
            total_area_ha = total_pixels * pixel_area_ha

            gt_bin = (gt == 1).astype(np.uint8)
            gt_area = np.sum(gt_bin[valid_mask] * pixel_area_ha)

            if cfg.udef_arp:
                pred_vals = pred_src.read(1)

                if pred_vals.shape != gt.shape:
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

                pred_vals[pred_vals < 0] = 0

                predicted_area = np.nansum(pred_vals[valid_mask])

            else:
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

                predicted_area = np.sum(prob[valid_mask] * pixel_area_ha)

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

    for cfg in RUNS:
        print(f"Running: {cfg.name}")
        res = evaluate_area(cfg)
        results.append(res)

    df = pd.DataFrame(results)

    out_csv = DATA_DIR + "/deforestation_area_estimates_32633.csv"
    # out_parquet = BASE_DIR / "deforestation_area_estimates.parquet"

    df.to_csv(out_csv, index=False)
    # df.to_parquet(out_parquet)

    print("\n=== Deforestation Area Estimates (ha) ===")
    print(df)
