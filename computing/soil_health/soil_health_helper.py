import rasterio
from rasterio.mask import mask
from shapely.geometry import mapping


from computing.local_compute_helper import ensure_file_exists

from pyproj import Geod
import numpy as np


def _compute_row_area(transform, height):
    """
    Compute the geodesic area (m²) of one pixel for every raster row.

    Since the raster is north-up in EPSG:4326, every pixel in the same
    row has identical area. Only latitude changes between rows.

    Returns
    -------
    np.ndarray
        Shape = (height,)
        Area (m²) of one pixel for each row.
    """

    geod = Geod(ellps="WGS84")

    row_area = np.zeros(height, dtype=np.float64)

    pixel_width = transform.a  # degrees
    pixel_height = abs(transform.e)  # degrees

    west = transform.c
    east = west + pixel_width

    for row in range(height):

        north = transform.f + row * transform.e
        south = north + transform.e

        area, _ = geod.polygon_area_perimeter(
            [west, east, east, west],
            [north, north, south, south],
        )

        row_area[row] = abs(area)

    return row_area


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
                for percentile, value in zip(percentiles, percentile_values):
                    stats[f"{nutrient}_p{percentile:02d}"] = float(value)
            rows.append(stats)

            if index % 200 == 0 or index == total:
                print(f"Computed soil health stats for {index}/{total} geometries")

    result = roi_gdf.copy()
    # Attach the computed columns back to the original ROI geometries for output.
    for column in rows[0] if rows else []:
        result[column] = [row[column] for row in rows]
    return result


def lulc_area_stats_for_geometries(
    roi_gdf,
    lulc_mode,
    lulc_meta,
):
    rows = []
    row_area = _compute_row_area(
        lulc_meta["transform"],
        lulc_mode.shape[0],
    )
    transform = lulc_meta["transform"]

    for geom in roi_gdf.geometry:

        mask_arr = rasterio.features.geometry_mask(
            [mapping(geom)],
            out_shape=lulc_mode.shape,
            transform=transform,
            invert=True,
        )

        # crop_pixels = np.count_nonzero(mask_arr & np.isin(lulc_mode, [8, 9, 10, 11]))

        # tree_pixels = np.count_nonzero(mask_arr & np.isin(lulc_mode, [6, 12]))

        crop_mask = mask_arr & np.isin(lulc_mode, [8, 9, 10, 11])

        tree_mask = mask_arr & np.isin(lulc_mode, [6, 12])

        crop_area = 0.0
        tree_area = 0.0

        height = lulc_mode.shape[0]

        for row in range(height):

            crop_pixels = np.count_nonzero(crop_mask[row])

            tree_pixels = np.count_nonzero(tree_mask[row])

            crop_area += crop_pixels * row_area[row]

            tree_area += tree_pixels * row_area[row]

        crop_area /= 10000.0
        tree_area /= 10000.0

        rows.append(
            {
                "crop_cover_area": crop_area,
                "tree_shrub_area": tree_area,
            }
        )

    result = roi_gdf.copy()

    result["crop_cover_area"] = [r["crop_cover_area"] for r in rows]

    result["tree_shrub_area"] = [r["tree_shrub_area"] for r in rows]

    return result
