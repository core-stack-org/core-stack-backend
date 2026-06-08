import numpy as np
import rasterio
from rasterio.mask import mask
from shapely.geometry import mapping

from computing.local_compute_helper import ensure_file_exists


def nutrient_stats_for_geometries(roi_gdf, raster_path, percentiles, nutrient):
    ensure_file_exists(raster_path, "Clipped soil health raster")

    with rasterio.open(raster_path) as src:
        # Raster masking expects geometries in the raster CRS, so reproject a
        # working copy and preserve the original ROI CRS for the output vector.
        working_gdf = roi_gdf.copy()
        if working_gdf.crs is None:
            raise ValueError(
                "ROI CRS is missing; cannot align with soil health raster."
            )
        if src.crs and working_gdf.crs != src.crs:
            working_gdf = working_gdf.to_crs(src.crs)

        nodata = src.nodata
        rows = []
        total = len(working_gdf)
        for index, row in enumerate(working_gdf.itertuples(index=False), start=1):
            geom = row.geometry
            if geom is None or geom.is_empty:
                values = np.array([], dtype=np.float64)
            else:
                # Clip the raster to one ROI/watershed polygon. `filled=False`
                # keeps rasterio's mask, which lets us drop pixels outside the geometry.
                try:
                    clipped, _ = mask(
                        src,
                        [mapping(geom)],
                        crop=True,
                        filled=False,
                    )
                except ValueError:
                    clipped = None

                if clipped is None or clipped.size == 0:
                    values = np.array([], dtype=np.float64)
                else:
                    data = clipped[0]
                    valid_mask = ~np.ma.getmaskarray(data)
                    values = np.asarray(data, dtype=np.float64)

                    # Ignore pixels outside the polygon, nodata pixels, and any
                    # non-finite values before calculating nutrient statistics.
                    valid_mask &= np.isfinite(values)
                    if nodata is not None and not np.isnan(nodata):
                        valid_mask &= values != float(nodata)
                    values = values[valid_mask]

            stats = {f"{nutrient}_count": int(values.size)}
            if values.size == 0:
                # Use NULLs in the vector table when a polygon has no valid raster pixels.
                stats[f"{nutrient}_mean"] = None
                for percentile in percentiles:
                    stats[f"{nutrient}_p{percentile:02d}"] = None
            else:
                stats[f"{nutrient}_mean"] = float(np.mean(values))
                percentile_values = np.percentile(values, percentiles)
                print(f"Percentile values: {percentile_values}")
                for percentile, value in zip(percentiles, percentile_values):
                    print(f"Percentile: {percentile}, Value: {value}")
                    stats[f"{nutrient}_p{percentile:02d}"] = float(value)
            rows.append(stats)

            if index % 200 == 0 or index == total:
                print(f"Computed soil health stats for {index}/{total} geometries")

    result = roi_gdf.copy()
    # Attach the computed columns back to the original ROI geometries for output.
    for column in rows[0] if rows else []:
        result[column] = [row[column] for row in rows]
    return result
