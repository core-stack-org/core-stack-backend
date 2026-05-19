# =============================================================================
# SPEI Pipeline - Step 1 (Local P-PET)
#
# Uses GeoTIFFs downloaded by download_chirps_local.py instead of reading:
#   ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")
#   ee.ImageCollection("MODIS/061/MOD16A2GF").select("PET")
#
# Output: one local multiband GeoTIFF with one P-PET band per month.
# Band names follow the original script: y{year}_m{month}, e.g. y2015_m06.
# =============================================================================

from __future__ import annotations

from pathlib import Path

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject


# --- CONFIG ---
state_name = "Madhya_Pradesh"
start_year = 2004
end_year = 2023

input_root = Path("data/drought_inputs")
output_dir = Path("data/drought_inputs") / state_name / "monthly" / "ppet"
output_path = output_dir / f"P_PET_{state_name}_monthly_multiband.tif"
OUTPUT_NODATA = -9999.0


def find_monthly_file(dataset_dir: Path, prefix: str, year: int, month: int) -> Path:
    """Find a downloaded monthly GeoTIFF, allowing nested folders from old runs."""
    label = f"{year}{month:02d}"
    matches = sorted(dataset_dir.rglob(f"{prefix}_{label}.tif"))
    if not matches:
        raise FileNotFoundError(
            f"Missing {prefix}_{label}.tif under {dataset_dir}. "
            "Run download_chirps_local.py first."
        )
    return matches[0]


def read_masked_band(dataset: rasterio.DatasetReader) -> np.ma.MaskedArray:
    """Read band 1 and mask nodata plus any nan/inf values."""
    data = dataset.read(1, masked=True).astype("float32")
    data = np.ma.masked_invalid(data)
    return np.ma.masked_where(np.abs(data) > 1.0e20, data)


def reproject_modis_to_chirps_grid(
    modis_path: Path,
    chirps_dataset: rasterio.DatasetReader,
    log_metadata: bool = False,
) -> np.ma.MaskedArray:
    """Reproject/resample MODIS PET onto the CHIRPS projection and pixel grid."""
    with rasterio.open(modis_path) as modis_src:
        if log_metadata:
            print(
                "  MODIS -> CHIRPS grid:",
                f"modis_crs={modis_src.crs}",
                f"chirps_crs={chirps_dataset.crs}",
                f"modis_shape=({modis_src.height}, {modis_src.width})",
                f"chirps_shape=({chirps_dataset.height}, {chirps_dataset.width})",
            )

        if (
            modis_src.crs == chirps_dataset.crs
            and modis_src.transform == chirps_dataset.transform
            and modis_src.width == chirps_dataset.width
            and modis_src.height == chirps_dataset.height
        ):
            return read_masked_band(modis_src)

        pet_source = read_masked_band(modis_src)
        pet_on_chirps_grid = np.full(
            (chirps_dataset.height, chirps_dataset.width),
            OUTPUT_NODATA,
            dtype="float32",
        )

        # MODIS PET is finer (~500m) than CHIRPS (~5500m), so average is the
        # right downsampling behavior for a monthly total/continuous variable.
        reproject(
            source=pet_source.filled(OUTPUT_NODATA),
            destination=pet_on_chirps_grid,
            src_transform=modis_src.transform,
            src_crs=modis_src.crs,
            dst_transform=chirps_dataset.transform,
            dst_crs=chirps_dataset.crs,
            dst_width=chirps_dataset.width,
            dst_height=chirps_dataset.height,
            src_nodata=OUTPUT_NODATA,
            dst_nodata=OUTPUT_NODATA,
            init_dest_nodata=True,
            resampling=Resampling.average,
        )
        return np.ma.masked_where(
            (pet_on_chirps_grid == OUTPUT_NODATA) | ~np.isfinite(pet_on_chirps_grid),
            pet_on_chirps_grid,
        )


def main(
    state: str = state_name,
    start: int = start_year,
    end: int = end_year,
    data_root: Path | str = input_root,
    output: Path | str | None = output_path,
) -> Path:
    data_root = Path(data_root)
    chirps_dir = data_root / state / "monthly" / "chirps"
    modis_dir = data_root / state / "monthly" / "modis_pet"

    output_file = Path(output) if output else data_root / state / "ppet" / (
        f"P_PET_{state}_monthly_multiband.tif"
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)

    months = [(year, month) for year in range(start, end + 1) for month in range(1, 13)]
    first_chirps = find_monthly_file(chirps_dir, "CHIRPS", *months[0])

    with rasterio.open(first_chirps) as template:
        profile = template.profile.copy()
        profile.update(
            count=len(months),
            dtype="float32",
            nodata=OUTPUT_NODATA,
            compress="lzw",
            BIGTIFF="IF_SAFER",
        )

        with rasterio.open(output_file, "w", **profile) as dst:
            for band_index, (year, month) in enumerate(months, start=1):
                chirps_path = find_monthly_file(chirps_dir, "CHIRPS", year, month)
                modis_path = find_monthly_file(modis_dir, "MODIS_PET", year, month)

                with rasterio.open(chirps_path) as chirps_src:
                    precipitation = read_masked_band(chirps_src)

                    # MODIS PET downloaded by download_chirps_local.py already has
                    # the 0.1 scale factor applied, so do not multiply again here.
                    pet = reproject_modis_to_chirps_grid(
                        modis_path, chirps_src, log_metadata=(band_index == 1)
                    )

                precipitation_data = precipitation.filled(np.nan).astype("float32")
                pet_data = pet.filled(np.nan).astype("float32")

                valid_mask = np.isfinite(precipitation_data) & np.isfinite(pet_data)
                ppet_data = np.full(
                    precipitation_data.shape, OUTPUT_NODATA, dtype="float32"
                )
                with np.errstate(invalid="ignore", over="ignore"):
                    ppet_data[valid_mask] = (
                        precipitation_data[valid_mask] - pet_data[valid_mask]
                    )

                ppet = np.ma.masked_where(
                    (ppet_data == OUTPUT_NODATA) | ~np.isfinite(ppet_data), ppet_data
                )
                band_name = f"y{year}_m{month:02d}"
                dst.write(ppet.filled(OUTPUT_NODATA).astype("float32"), band_index)
                dst.set_band_description(band_index, band_name)
                invalid_count = int(np.ma.count_masked(ppet))
                print(f"Prepared: {band_name} nodata_pixels={invalid_count}")

    print(f"\nWrote {len(months)} P-PET band(s): {output_file}")
    return output_file
