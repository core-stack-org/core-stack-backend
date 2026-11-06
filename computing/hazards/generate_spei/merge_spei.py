import glob
import rasterio
from rasterio.merge import merge


def merge_SPEI_rasters(spei_type):
    # Input rasters
    input_dir = f"data/base_layers/spei/outputs/SPEI_{spei_type}"
    input_files = sorted(glob.glob(f"{input_dir}/SPEI{spei_type}_*.tif"))

    if not input_files:
        raise RuntimeError("No input files found.")

    srcs = [rasterio.open(f) for f in input_files]

    # Validate all rasters have same number of bands
    band_count = srcs[0].count
    for src in srcs:
        if src.count != band_count:
            raise RuntimeError(
                f"{src.name} has {src.count} bands, expected {band_count}"
            )

    # Output metadata
    meta = srcs[0].meta.copy()

    # Merge first band to determine output size/transform
    first_band, out_transform = merge(srcs, indexes=1, nodata=-9999, method="first")

    meta.update(
        driver="GTiff",
        height=first_band.shape[1],
        width=first_band.shape[2],
        transform=out_transform,
        count=band_count,
        compress="LZW",
        tiled=True,
        BIGTIFF="YES",
        nodata=-9999,
    )

    output_file = f"{input_dir}/SPEI{spei_type}.tif"

    with rasterio.open(output_file, "w", **meta) as dst:

        # Copy band descriptions
        dst.descriptions = srcs[0].descriptions

        for band in range(1, band_count + 1):
            print(f"Merging band {band}/{band_count}")

            mosaic, _ = merge(srcs, indexes=[band], nodata=-9999, method="first")

            dst.write(mosaic[0], band)

    # Close inputs
    for src in srcs:
        src.close()

    print(f"Finished: {output_file}")


"""
    After uploading the merged SPEI files on GEE, the band names gets renamed to b1, b2,..,etc. 
    To rename them back to relevant names, use the below GEE script:
    https://code.earthengine.google.co.in/370dfca36f228fbd09892708949cf381
"""
