import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from computing.base_layer_setup import (
    _download_active_tehsil_watersheds,
    ensure_tehsil_watershed,
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


class EnsureTehsilWatershedTests(SimpleTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    @patch("computing.base_layer_setup.requests.get")
    def test_unpublished_layer_raises_clear_error(self, get):
        response = Mock()
        response.json.side_effect = ValueError("not json")
        get.return_value = response

        with patch(
            "computing.base_layer_setup.TEHSIL_WATERSHEDS_DIR",
            self.output_dir,
        ):
            with self.assertRaisesRegex(ValueError, "mws:mws_banka_banka.*not available"):
                ensure_tehsil_watershed("Bihar", "Banka", "Banka")

        self.assertFalse((self.output_dir / "bihar" / "banka" / "banka.gpkg").exists())

    @patch("computing.base_layer_setup._download_tehsil_watershed")
    def test_existing_file_is_not_downloaded(self, download):
        destination = self.output_dir / "bihar" / "banka" / "banka.gpkg"
        destination.parent.mkdir(parents=True)
        destination.write_bytes(b"existing")

        with patch(
            "computing.base_layer_setup.TEHSIL_WATERSHEDS_DIR",
            self.output_dir,
        ):
            result = ensure_tehsil_watershed("Bihar", "Banka", "Banka")

        self.assertEqual(result, destination)
        download.assert_not_called()

    def test_concurrent_callers_download_once(self):
        from concurrent.futures import ThreadPoolExecutor

        calls = []

        def fake_download(destination, layer_name):
            calls.append(layer_name)
            destination.write_bytes(b"downloaded")

        with patch(
            "computing.base_layer_setup.TEHSIL_WATERSHEDS_DIR",
            self.output_dir,
        ), patch(
            "computing.base_layer_setup._download_tehsil_watershed",
            side_effect=fake_download,
        ):
            with ThreadPoolExecutor(max_workers=4) as pool:
                list(
                    pool.map(
                        lambda _: ensure_tehsil_watershed("Bihar", "Banka", "Banka"),
                        range(4),
                    )
                )

        self.assertEqual(calls, ["mws:mws_banka_banka"])
