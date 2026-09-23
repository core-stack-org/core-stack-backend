import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from computing.base_layer_setup import (
    _download_active_tehsil_watersheds,
    with_tehsil_watershed,
)


class GeoServerTehsilWatershedSetupTests(SimpleTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = Path(self.temp_dir.name)
        self.location = [("Bihar", "Banka", "Banka")]

    def tearDown(self):
        self.temp_dir.cleanup()

    @patch("computing.base_layer_setup.requests.get")
    @patch("computing.base_layer_setup._active_tehsil_locations")
    def test_existing_active_tehsil_is_skipped(self, active_locations, get):
        active_locations.return_value = self.location
        destination = self.output_dir / "bihar" / "banka" / "banka.gpkg"
        destination.parent.mkdir(parents=True)
        destination.write_bytes(b"existing")

        with patch(
            "computing.base_layer_setup.TEHSIL_WATERSHEDS_DIR",
            self.output_dir,
        ):
            _download_active_tehsil_watersheds()

        get.assert_not_called()
        self.assertEqual(destination.read_bytes(), b"existing")

    @patch("geopandas.GeoDataFrame.from_features")
    @patch("computing.base_layer_setup.requests.get")
    @patch("computing.base_layer_setup._active_tehsil_locations")
    def test_force_replaces_active_tehsil_gpkg(
        self,
        active_locations,
        get,
        from_features,
    ):
        active_locations.return_value = self.location
        response = Mock()
        response.json.return_value = {
            "type": "FeatureCollection",
            "features": [{"type": "Feature", "properties": {}, "geometry": None}],
        }
        get.return_value = response

        watersheds = Mock()
        watersheds.empty = False
        watersheds.to_file.side_effect = lambda path, **kwargs: Path(path).write_bytes(
            b"replacement"
        )
        from_features.return_value = watersheds

        destination = self.output_dir / "bihar" / "banka" / "banka.gpkg"
        destination.parent.mkdir(parents=True)
        destination.write_bytes(b"existing")

        with patch(
            "computing.base_layer_setup.TEHSIL_WATERSHEDS_DIR",
            self.output_dir,
        ):
            _download_active_tehsil_watersheds(force=True)

        self.assertEqual(destination.read_bytes(), b"replacement")
        get.assert_called_once()
        self.assertEqual(
            get.call_args.kwargs["params"]["typeName"],
            "mws:mws_banka_banka",
        )
        watersheds.to_file.assert_called_once_with(
            destination.with_suffix(".tmp.gpkg"),
            layer="watersheds",
            driver="GPKG",
        )

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
