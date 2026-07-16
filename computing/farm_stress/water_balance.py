"""Water balance (P - PET) for SPEI-3 (plan.md Step 1 / Script 01a Part C).

Pure local computation - no GEE involved. Rainfall (spi_spei_export.py's
export_gsmap_*) and PET (export_modis_pet_*) are both already downloaded
locally on the same 11km grid (same India bbox, same scale, same period
labels), verified by direct comparison of their rasterio transforms.
"""

import os

import numpy as np
import rasterio

from computing.farm_stress.helper import generate_28day_periods
from computing.farm_stress.config import (
    LOCAL_DIR_GSMAP_MONTHLY,
    LOCAL_DIR_MODIS_PET_MONTHLY,
    LOCAL_DIR_WATER_BALANCE_MONTHLY,
)


def compute_water_balance_archive(
    start_year=2000,
    end_year=2025,
    precip_dir=LOCAL_DIR_GSMAP_MONTHLY,
    pet_dir=LOCAL_DIR_MODIS_PET_MONTHLY,
    output_dir=LOCAL_DIR_WATER_BALANCE_MONTHLY,
    overwrite=False,
):
    """Compute water_balance_mm = precip_mm - pet_mm for every 28-day period,
    reading the already-downloaded rainfall and PET rasters and writing one
    water-balance GeoTIFF per period. Can be negative (PET > rainfall).

    PET's NoData is real NaN (masked over ocean); rainfall's NoData is 0
    (colliding with real zero-rainfall, per the earlier QGIS investigation -
    not a genuine mask). Subtracting propagates PET's NaN through
    automatically (anything - NaN = NaN), so the output is correctly masked
    over ocean without any extra masking logic, while land pixels with
    legitimately zero rainfall subtract normally.

    Safe to interrupt and re-run: files already on disk are skipped unless
    overwrite=True.
    """
    periods = generate_28day_periods(start_year, end_year)
    precip_dir = precip_dir.rstrip("/")
    pet_dir = pet_dir.rstrip("/")
    output_dir = output_dir.rstrip("/")
    print(f"{len(periods)} periods to process ({start_year}-{end_year})")

    computed, skipped, missing_input = [], [], []
    for i, period in enumerate(periods, start=1):
        label = period["label"]
        output_path = f"{output_dir}/wb_{label}.tif"
        if os.path.exists(output_path) and not overwrite:
            skipped.append(output_path)
            continue

        precip_path = f"{precip_dir}/precip_{label}.tif"
        pet_path = f"{pet_dir}/pet_{label}.tif"
        if not (os.path.exists(precip_path) and os.path.exists(pet_path)):
            missing_input.append(label)
            continue

        with rasterio.open(precip_path) as precip_src, rasterio.open(pet_path) as pet_src:
            precip = precip_src.read(1).astype(np.float64)
            pet = pet_src.read(1).astype(np.float64)
            profile = pet_src.profile

        water_balance = precip - pet

        os.makedirs(output_dir, exist_ok=True)
        profile.update(dtype="float64", count=1, nodata=np.nan)
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(water_balance, 1)

        print(f"[{i}/{len(periods)}] {label} -> {output_path}")
        computed.append(output_path)

    print(
        f"Done. Computed {len(computed)}, skipped {len(skipped)}, "
        f"missing input for {len(missing_input)} period(s)."
    )
    if missing_input:
        print(f"Periods missing rainfall/PET input: {missing_input}")
    return {"computed": computed, "skipped": skipped, "missing_input": missing_input}
