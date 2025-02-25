import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_origin
import fiona
import os


def rasterize_vector(vector_path, output_raster_path, attribute_column):
    # Define the resolution (30m x 30m)
    resolution = 0.000278  # in meters

    # Read the vector shapefile
    with fiona.open(vector_path, "r") as shapefile:
        shapes = [
            (feature["geometry"], feature["properties"][attribute_column])
            for feature in shapefile
        ]
        bounds = shapefile.bounds
        if bounds is None:
            # Calculate bounds manually
            all_geometries = [feature["geometry"] for feature in shapefile]
            bounds = fiona.collection.bounds(all_geometries)
    # Define the raster metadata
    minx, miny, maxx, maxy = bounds
    width = int((maxx - minx) / resolution)
    height = int((maxy - miny) / resolution)
    transform = from_origin(minx, maxy, resolution, resolution)
    # Rasterize the vector shapes based on the attribute column
    raster = rasterize(
        shapes,
        out_shape=(height, width),
        fill=0,  # Background value
        transform=transform,
        all_touched=True,
        dtype=rasterio.float32,  # or another appropriate data type
    )

    # Save the raster to a file
    with rasterio.open(
        output_raster_path,
        "w",
        driver="GTiff",
        height=raster.shape[0],
        width=raster.shape[1],
        count=1,
        dtype=raster.dtype,
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(raster, 1)

    print(f"Rasterized shapefile saved as {output_raster_path}")
