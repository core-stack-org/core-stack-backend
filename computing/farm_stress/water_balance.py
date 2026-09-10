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


def compute_water_balance_archive_banded(
    precip_paths_by_year,
    pet_paths_by_year,
    start_year=2000,
    end_year=2025,
    output_dir=None,
    overwrite=False,
    row_chunk=500,
):
    """Yearly multi-band version of compute_water_balance_archive, for
    the 500m SPEI-3 pipeline where rainfall/PET are stored as one
    multi-band file per year (one band per 28-day period) rather than
    one file per period - avoids ever materialising the ~340 tiny
    per-period files the 11km pipeline uses, which matters at 500m scale
    where disk space is a real constraint (this was built specifically
    because of that - see conversation).

    Row-tiled (read/subtract/write a horizontal strip at a time, not the
    whole year at once) - subtraction has no cross-pixel dependency, so
    this is a zero-cost way to bound memory, unlike the float32 downcast
    tried first here: reading a full year (13 bands, 6569x7110) of
    precip AND pet AND the output simultaneously actually OOM-killed the
    process in practice (~19.7GB RSS, confirmed via dmesg) - the earlier
    assumption that one year was "small enough" to hold whole was wrong
    on a machine already under memory pressure from other things. Row
    tiling fixes that at the root without trading away precision the way
    float32 did (measured ~0.09 divergence in final SPEI-3 values on a
    synthetic test - not an acceptable tradeoff when tiling is free).

    precip_paths_by_year / pet_paths_by_year: {year: file_path} - passed
    in explicitly rather than assumed from a fixed naming pattern,
    because the actual files on disk came from a manual terminal
    workflow (gdalbuildvrt/gdal_translate merges) with naming that
    doesn't perfectly follow one convention - baking a fragile
    path-guessing pattern into this function risks yet another
    silent-mismatch bug like the last two.

    Band order within each year's file must match
    spi_spei_export._periods_by_year's chronological ordering for that
    year - true for anything produced by this project's export/merge
    path, since both sides derive from the same
    generate_28day_periods() call.

    Output: one wb_{year}.tif per year, same band layout as the inputs.

    Safe to interrupt and re-run: years already on disk are skipped
    unless overwrite=True. Doesn't resume a partially-written year if
    interrupted mid-strip - re-run that year with overwrite=True.
    """
    from rasterio.windows import Window

    from computing.farm_stress.config import LOCAL_DIR_WATER_BALANCE_500M
    from computing.farm_stress.spi_spei_export import _periods_by_year

    output_dir = (output_dir or LOCAL_DIR_WATER_BALANCE_500M).rstrip("/")
    os.makedirs(output_dir, exist_ok=True)
    by_year = _periods_by_year(start_year, end_year)
    print(f"{len(by_year)} year(s) to process ({start_year}-{end_year})")

    computed, skipped, missing_input = [], [], []
    for year in sorted(by_year):
        periods = by_year[year]
        out_path = f"{output_dir}/wb_{year}.tif"
        if os.path.exists(out_path) and not overwrite:
            skipped.append(out_path)
            continue

        precip_path = precip_paths_by_year.get(year)
        pet_path = pet_paths_by_year.get(year)
        if not (precip_path and pet_path and os.path.exists(precip_path) and os.path.exists(pet_path)):
            print(f"{year}: missing precip and/or pet file, skipping")
            missing_input.append(year)
            continue

        with rasterio.open(precip_path) as precip_src, rasterio.open(pet_path) as pet_src:
            if precip_src.count != len(periods) or pet_src.count != len(periods):
                raise ValueError(
                    f"{year}: expected {len(periods)} bands, got "
                    f"{precip_src.count} (precip) / {pet_src.count} (pet)"
                )
            rows, cols = precip_src.height, precip_src.width
            profile = precip_src.profile.copy()
            profile.update(dtype="float64", count=len(periods), nodata=np.nan)

            n_strips = (rows + row_chunk - 1) // row_chunk
            with rasterio.open(out_path, "w", **profile) as dst:
                for s in range(n_strips):
                    row_off = s * row_chunk
                    strip_h = min(row_chunk, rows - row_off)
                    window = Window(0, row_off, cols, strip_h)

                    precip_strip = precip_src.read(window=window).astype(np.float64)
                    pet_strip = pet_src.read(window=window).astype(np.float64)
                    dst.write(precip_strip - pet_strip, window=window)

        print(f"{year}: wrote {out_path} ({len(periods)} bands, {n_strips} strip(s))")
        computed.append(out_path)

    print(
        f"Done. Computed {len(computed)}, skipped {len(skipped)}, "
        f"missing input for {len(missing_input)} year(s)."
    )
    if missing_input:
        print(f"Years missing rainfall/PET input: {missing_input}")
    return {"computed": computed, "skipped": skipped, "missing_input": missing_input}
