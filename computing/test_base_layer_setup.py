import tempfile
from pathlib import Path
from unittest.mock import patch

from django.test import SimpleTestCase

from computing.base_layer_setup import (
    ensure_tehsil_watershed,
    ensure_tehsil_watersheds,
    with_tehsil_watershed,
)


class TehsilWatershedDecoratorTests(SimpleTestCase):
    @patch("computing.base_layer_setup.ensure_tehsil_watershed")
    def test_local_compute_ensures_requested_tehsil(self, ensure):
        @with_tehsil_watershed
        def generate(state, district, block, compute="gee"):
            return "generated"

        result = generate("Bihar", "Banka", "Banka", compute="local")

        self.assertEqual(result, "generated")
        ensure.assert_called_once_with(
            state="Bihar",
            district="Banka",
            tehsil="Banka",
        )

    @patch("computing.base_layer_setup.ensure_tehsil_watershed")
    def test_gee_compute_does_not_ensure_local_tehsil(self, ensure):
        @with_tehsil_watershed
        def generate(state, district, block, compute="gee"):
            return "generated"

        generate("Bihar", "Banka", "Banka")

        ensure.assert_not_called()


class EnsureTehsilWatershedTests(SimpleTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = Path(self.temp_dir.name)
        patcher = patch(
            "computing.base_layer_setup.TEHSIL_WATERSHEDS_DIR", self.output_dir
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        self.temp_dir.cleanup()

    def _touch(self, name):
        path = self.output_dir / "bihar" / "banka" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
        return path

    def test_geojson_is_resolved(self):
        path = self._touch("banka.geojson")
        self.assertEqual(ensure_tehsil_watershed("Bihar", "Banka", "Banka"), path)

    def test_gpkg_preferred_over_geojson(self):
        self._touch("banka.geojson")
        path = self._touch("banka.gpkg")
        self.assertEqual(ensure_tehsil_watershed("Bihar", "Banka", "Banka"), path)

    def test_missing_file_raises_without_network(self):
        with self.assertRaises(FileNotFoundError):
            ensure_tehsil_watershed("Bihar", "Banka", "Banka")

    @patch("computing.base_layer_setup._active_tehsil_locations")
    def test_ensure_all_reports_missing(self, active):
        active.return_value = [("Bihar", "Banka", "Banka")]
        with self.assertRaisesRegex(FileNotFoundError, "Bihar/Banka/Banka"):
            ensure_tehsil_watersheds()
        self._touch("banka.geojson")
        ensure_tehsil_watersheds()
