from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import geopandas as gpd
from django.test import SimpleTestCase
from shapely.geometry import box

from computing.forest_fire.forest_fire_local_compute import (
    generate_forest_fire_local,
)
from computing.forest_fringe.forest_fringe_local_compute import (
    generate_forest_fringe_local,
)
from computing.local_compute_helper import resolve_indexed_vector_path
from computing.tree_in_grassland.tree_in_grassland_local_compute import (
    generate_tree_in_grassland_local,
)


def _write_vector(path: Path, uid: str, marker: str, driver: str) -> None:
    gdf = gpd.GeoDataFrame(
        {"uid": [uid], "source_marker": [marker]},
        geometry=[box(77.0, 12.0, 77.1, 12.1)],
        crs="EPSG:4326",
    )
    gdf.to_file(path, driver=driver)


class ResolveIndexedVectorPathTests(SimpleTestCase):
    def test_prefers_fgb_then_gpkg_then_original(self):
        with TemporaryDirectory() as tmpdir:
            base = Path(tmpdir) / "layer.geojson"
            gpkg = base.with_suffix(".gpkg")
            fgb = base.with_suffix(".fgb")

            # Nothing exists yet -> returns original path
            self.assertEqual(resolve_indexed_vector_path(base), base)

            # Only geojson exists
            base.touch()
            self.assertEqual(resolve_indexed_vector_path(base), base)

            # Sibling gpkg preferred over geojson
            gpkg.touch()
            self.assertEqual(resolve_indexed_vector_path(base), gpkg)

            # Sibling fgb preferred over gpkg and geojson
            fgb.touch()
            self.assertEqual(resolve_indexed_vector_path(base), fgb)

    def test_uses_indexed_sibling_even_if_configured_geojson_missing(self):
        with TemporaryDirectory() as tmpdir:
            base = Path(tmpdir) / "layer.geojson"
            gpkg = base.with_suffix(".gpkg")
            fgb = base.with_suffix(".fgb")

            gpkg.touch()
            self.assertEqual(resolve_indexed_vector_path(base), gpkg)

            fgb.touch()
            self.assertEqual(resolve_indexed_vector_path(base), fgb)


class LocalVectorComputeIndexedSiblingTests(SimpleTestCase):
    CASES = (
        (
            "forest_fire",
            generate_forest_fire_local,
            "computing.forest_fire.forest_fire_local_compute.PAN_INDIA_FOREST_FIRE",
            "computing.forest_fire.forest_fire_local_compute.LOCAL_FOREST_FIRE_OUTPUT",
            "test_roi_forest_fire.gpkg",
        ),
        (
            "forest_fringe",
            generate_forest_fringe_local,
            "computing.forest_fringe.forest_fringe_local_compute.PAN_INDIA_FOREST_FRINGE",
            "computing.forest_fringe.forest_fringe_local_compute.LOCAL_FOREST_FRINGE_OUTPUT",
            "test_roi_forest_fringe.gpkg",
        ),
        (
            "tree_in_grassland",
            generate_tree_in_grassland_local,
            "computing.tree_in_grassland.tree_in_grassland_local_compute.PAN_INDIA_TREE_IN_GRASSLAND",
            "computing.tree_in_grassland.tree_in_grassland_local_compute.LOCAL_TREE_IN_GRASSLAND_OUTPUT",
            "test_roi_tree_in_grassland.gpkg",
        ),
    )

    def _make_roi(self, tmpdir: Path) -> Path:
        roi_path = tmpdir / "roi.gpkg"
        roi = gpd.GeoDataFrame(
            {"uid": ["u1"]},
            geometry=[box(76.95, 11.95, 77.15, 12.15)],
            crs="EPSG:4326",
        )
        roi.to_file(roi_path, driver="GPKG")
        return roi_path

    def test_prefers_sibling_fgb_then_gpkg_then_geojson(self):
        for label, task, source_attr, output_attr, out_filename in self.CASES:
            with self.subTest(layer=label), TemporaryDirectory() as tmpdir:
                tmp = Path(tmpdir)
                roi_path = self._make_roi(tmp)
                output_dir = tmp / "out"
                output_file = output_dir / "custom" / out_filename
                geojson_path = tmp / f"{label}.geojson"
                gpkg_path = geojson_path.with_suffix(".gpkg")
                fgb_path = geojson_path.with_suffix(".fgb")

                with patch(source_attr, geojson_path), patch(output_attr, output_dir):
                    # Missing all files -> FileNotFoundError
                    with self.assertRaises(FileNotFoundError):
                        task.run(
                            asset_suffix="test_roi",
                            roi_path=str(roi_path),
                            push_to_geoserver=False,
                            sync_layer_metadata=False,
                        )

                    # Only geojson -> reads geojson
                    _write_vector(geojson_path, "u1", "from_geojson", "GeoJSON")
                    self.assertTrue(
                        task.run(
                            asset_suffix="test_roi",
                            roi_path=str(roi_path),
                            push_to_geoserver=False,
                            sync_layer_metadata=False,
                        )
                    )
                    out_gdf = gpd.read_file(output_file)
                    self.assertEqual(list(out_gdf["source_marker"]), ["from_geojson"])

                    # Add sibling gpkg -> prefers gpkg
                    _write_vector(gpkg_path, "u1", "from_gpkg", "GPKG")
                    self.assertTrue(
                        task.run(
                            asset_suffix="test_roi",
                            roi_path=str(roi_path),
                            push_to_geoserver=False,
                            sync_layer_metadata=False,
                        )
                    )
                    out_gdf = gpd.read_file(output_file)
                    self.assertEqual(list(out_gdf["source_marker"]), ["from_gpkg"])

                    # Add sibling fgb -> prefers fgb over gpkg and geojson
                    _write_vector(fgb_path, "u1", "from_fgb", "FlatGeobuf")
                    self.assertTrue(
                        task.run(
                            asset_suffix="test_roi",
                            roi_path=str(roi_path),
                            push_to_geoserver=False,
                            sync_layer_metadata=False,
                        )
                    )
                    out_gdf = gpd.read_file(output_file)
                    self.assertEqual(list(out_gdf["source_marker"]), ["from_fgb"])

                    # Remove geojson & gpkg -> still works with only sibling fgb
                    geojson_path.unlink()
                    gpkg_path.unlink()
                    self.assertTrue(
                        task.run(
                            asset_suffix="test_roi",
                            roi_path=str(roi_path),
                            push_to_geoserver=False,
                            sync_layer_metadata=False,
                        )
                    )
                    out_gdf = gpd.read_file(output_file)
                    self.assertEqual(list(out_gdf["source_marker"]), ["from_fgb"])
