"""Local 500m MAI (Moisture Availability Index) = ET / PET, per 8-day composite.

Inputs are the yearly 46-band rasters exported from MOD16A2GF by
export_gee_assets.export_modis_8day_500m_archive (ET and PET, mm per 8-day,
band i = 8-day composite i of the year, DOY 1, 9, ..., 361). Output is one
yearly 46-band COG per year on the same grid, so band i of MAI lines up with
band i of ET and PET (and with the 8-day VCI periods).

MAI per pixel = ET / PET, NaN where either input is NaN or PET <= 0, then
clipped to [0, 1] (plan.md Script 02, Part B). The 0.1 scale factor cancels
in the ratio, so it doesn't matter that it was applied at export time - but
it only cancels if ET and PET were scaled the same way; the per-year summary
this prints (share of pixels that needed clipping at 1) is the check for
that.
"""

import os

import numpy as np
import rasterio

from computing.farm_stress.config import LOCAL_DIR_MAI_500M, N_8DAY_PERIODS


def compute_mai_archive_banded(
    et_paths_by_year,
    pet_paths_by_year,
    start_year=2000,
    end_year=2025,
    output_dir=None,
    overwrite=False,
    row_chunk=500,
):
    """Yearly multi-band MAI = ET / PET, row-tiled (a horizontal strip at a
    time, so memory stays bounded however large the year is).

    et_paths_by_year / pet_paths_by_year: {year: file_path}, passed in
    explicitly rather than guessed from a naming pattern - same reasoning
    as water_balance.compute_water_balance_archive_banded.

    Output: one mai_500m_{year}.tif per year (46 float32 bands, NaN =
    no data, Cloud Optimized GeoTIFF). The year is written to a temp file
    and only moved into place once complete and converted, so a file
    called mai_500m_{year}.tif is always a finished one. Re-running skips
    years already on disk unless overwrite=True.

    Raises if the two inputs of a year differ in band count or grid
    (width/height/transform/CRS) - they would otherwise silently divide
    unrelated pixels.

    Returns {"computed": [...], "skipped": [...], "missing_input": [...],
    "stats": {year: {...}}}.
    """
    from rasterio.windows import Window

    from computing.farm_stress.export_gee_assets import _to_cog

    output_dir = (output_dir or LOCAL_DIR_MAI_500M).rstrip("/")
    os.makedirs(output_dir, exist_ok=True)
    years = list(range(start_year, end_year + 1))
    print(f"{len(years)} year(s) to process ({start_year}-{end_year})")

    computed, skipped, missing_input, stats = [], [], [], {}
    for year in years:
        out_path = f"{output_dir}/mai_500m_{year}.tif"
        if os.path.exists(out_path) and not overwrite:
            skipped.append(out_path)
            continue

        et_path = et_paths_by_year.get(year)
        pet_path = pet_paths_by_year.get(year)
        if not (et_path and pet_path and os.path.exists(et_path) and os.path.exists(pet_path)):
            print(f"{year}: missing ET and/or PET file, skipping")
            missing_input.append(year)
            continue

        tmp_path = out_path + ".tmp"
        try:
            with rasterio.open(et_path) as et_src, rasterio.open(pet_path) as pet_src:
                if et_src.count != N_8DAY_PERIODS or pet_src.count != N_8DAY_PERIODS:
                    raise ValueError(
                        f"{year}: expected {N_8DAY_PERIODS} bands, got "
                        f"{et_src.count} (ET) / {pet_src.count} (PET)"
                    )
                if (et_src.width, et_src.height, et_src.transform, et_src.crs) != (
                    pet_src.width, pet_src.height, pet_src.transform, pet_src.crs,
                ):
                    raise ValueError(
                        f"{year}: ET and PET are not on the same grid "
                        f"({et_src.width}x{et_src.height} vs {pet_src.width}x{pet_src.height})"
                    )
                rows, cols = et_src.height, et_src.width
                profile = et_src.profile.copy()
                profile.update(
                    driver="GTiff", dtype="float32", count=N_8DAY_PERIODS, nodata=np.nan,
                    tiled=True, blockxsize=512, blockysize=512, compress="deflate", BIGTIFF="YES",
                )

                n_valid = n_clipped_high = 0
                sum_mai = 0.0
                n_strips = (rows + row_chunk - 1) // row_chunk
                with rasterio.open(tmp_path, "w", **profile) as dst:
                    for s in range(n_strips):
                        row_off = s * row_chunk
                        window = Window(0, row_off, cols, min(row_chunk, rows - row_off))
                        et = et_src.read(window=window).astype(np.float32)
                        pet = pet_src.read(window=window).astype(np.float32)

                        ok = np.isfinite(et) & np.isfinite(pet) & (pet > 0)
                        mai = np.full(et.shape, np.nan, dtype=np.float32)
                        np.divide(et, pet, out=mai, where=ok)
                        n_clipped_high += int((mai[ok] > 1).sum())
                        mai = np.clip(mai, 0.0, 1.0)  # NaN stays NaN
                        n_valid += int(ok.sum())
                        sum_mai += float(np.nansum(mai[ok], dtype=np.float64))
                        dst.write(mai, window=window)

            _to_cog(tmp_path)
            os.replace(tmp_path, out_path)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        stats[year] = {
            "valid_values": n_valid,
            "mean_mai": (sum_mai / n_valid) if n_valid else float("nan"),
            "clipped_high_frac": (n_clipped_high / n_valid) if n_valid else float("nan"),
        }
        print(
            f"{year}: wrote {out_path} ({N_8DAY_PERIODS} bands, {n_strips} strip(s)); "
            f"mean MAI {stats[year]['mean_mai']:.3f}, "
            f"{stats[year]['clipped_high_frac']:.2%} of values were >1 and clipped"
        )
        computed.append(out_path)

    print(
        f"Done. Computed {len(computed)}, skipped {len(skipped)}, "
        f"missing input for {len(missing_input)} year(s)."
    )
    if missing_input:
        print(f"Years missing ET/PET input: {missing_input}")
    return {"computed": computed, "skipped": skipped, "missing_input": missing_input, "stats": stats}
