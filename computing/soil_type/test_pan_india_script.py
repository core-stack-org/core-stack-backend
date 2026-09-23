import tempfile
import sqlite3
from pathlib import Path
from unittest import TestCase

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from computing.scripts.generate_pan_india_soil_type import (
    OUTPUT_NODATA,
    VECTOR_LAYER_NAME,
    build_pan_india_soil_raster,
    build_pan_india_soil_vector,
)
from computing.soil_type.soil_type_local import SOIL_PROPERTY_SPECS


class PanIndiaSoilTypeScriptTests(TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)
        self.transform = from_origin(0, 4, 1, 1)
        # Use WKT directly so this test does not depend on the host's PROJ
        # authority database being compatible with Rasterio's GDAL build.
        self.crs_wkt = (
            'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",'
            '6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",'
            '0.0174532925199433],AXIS["Latitude",NORTH],AXIS["Longitude",EAST]]'
        )
        self.boundary = gpd.GeoDataFrame(
            {
                "name": ["test boundary"],
                "excluded": [["test island one", "test island two"]],
            },
            geometry=[box(0, 0, 2, 4)],
            crs=self.crs_wkt,
        )
        self.raster_paths = {}
        for index, spec in enumerate(SOIL_PROPERTY_SPECS, start=1):
            path = self.root / f"{spec['column']}.tif"
            with rasterio.open(
                path,
                "w",
                driver="GTiff",
                width=4,
                height=4,
                count=1,
                dtype="float32",
                crs=self.crs_wkt,
                transform=self.transform,
            ) as destination:
                destination.write(
                    np.full((4, 4), index, dtype=np.float32),
                    1,
                )
            self.raster_paths[spec["column"]] = path

    def test_builds_named_multiband_raster_clipped_to_boundary(self):
        output_path = self.root / "pan_india_soil_type.tif"

        build_pan_india_soil_raster(
            boundary_gdf=self.boundary,
            raster_paths=self.raster_paths,
            output_path=output_path,
        )

        with rasterio.open(output_path) as result:
            self.assertEqual(result.count, len(SOIL_PROPERTY_SPECS))
            self.assertEqual(result.width, 2)
            self.assertEqual(result.height, 4)
            self.assertEqual(
                result.descriptions,
                tuple(spec["column"] for spec in SOIL_PROPERTY_SPECS),
            )
            np.testing.assert_array_equal(result.read(1), np.ones((4, 2)))
            self.assertEqual(result.nodata, OUTPUT_NODATA)

    def test_builds_vector_with_all_soil_property_columns(self):
        combined_raster_path = self.root / "pan_india_soil_type.tif"
        mws_path = self.root / "pan_india_mws.gpkg"
        output_path = self.root / "pan_india_soil_type.gpkg"
        with rasterio.open(
            self.raster_paths["available_water_capacity"],
            "r+",
        ) as source:
            values = source.read(1)
            values[:, 2:] = 2
            source.write(values, 1)

        full_boundary = self.boundary.copy()
        full_boundary.geometry = [box(0, 0, 4, 4)]
        build_pan_india_soil_raster(
            boundary_gdf=full_boundary,
            raster_paths=self.raster_paths,
            output_path=combined_raster_path,
        )
        gpd.GeoDataFrame(
            {
                "uid": ["mws_left", "mws_right"],
                "id": [1, 2],
            },
            geometry=[box(0, 0, 2, 4), box(2, 0, 4, 4)],
            crs=self.crs_wkt,
        ).to_file(mws_path, driver="GPKG", layer="mws")

        build_pan_india_soil_vector(
            raster_path=combined_raster_path,
            mws_path=mws_path,
            output_path=output_path,
        )

        result = gpd.read_file(output_path, layer=VECTOR_LAYER_NAME)
        self.assertEqual(len(result), 2)
        self.assertEqual(set(result["available_water_capacity"]), {125, 150})
        self.assertEqual(set(result["uid"]), {"mws_left", "mws_right"})
        for spec in SOIL_PROPERTY_SPECS:
            self.assertIn(spec["column"], result.columns)
        with sqlite3.connect(output_path) as connection:
            indexes = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        self.assertIn(
            (f"idx_{VECTOR_LAYER_NAME}_uid",),
            indexes,
        )
        self.assertIn(
            (f"idx_{VECTOR_LAYER_NAME}_id",),
            indexes,
        )

    def test_refuses_to_replace_existing_output_without_overwrite(self):
        output_path = self.root / "pan_india_soil_type.tif"
        output_path.touch()

        with self.assertRaises(FileExistsError):
            build_pan_india_soil_raster(
                boundary_gdf=self.boundary,
                raster_paths=self.raster_paths,
                output_path=output_path,
            )
