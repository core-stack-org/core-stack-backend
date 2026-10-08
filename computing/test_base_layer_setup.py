import tempfile
from pathlib import Path
from unittest.mock import patch

from django.test import SimpleTestCase

from computing.base_layer_setup import (
    ensure_tehsil_watershed,
    ensure_tehsil_watersheds,
)


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
        path = self.output_dir / "bihar" / "banka" / "banka" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
        return path

    def test_geojson_is_resolved(self):
        path = self._touch("filtered_mws_banka_banka_uid.geojson")
        self.assertEqual(ensure_tehsil_watershed("Bihar", "Banka", "Banka"), path)

    def test_gpkg_preferred_over_geojson(self):
        self._touch("filtered_mws_banka_banka_uid.geojson")
        path = self._touch("filtered_mws_banka_banka_uid.gpkg")
        self.assertEqual(ensure_tehsil_watershed("Bihar", "Banka", "Banka"), path)

    def test_missing_file_raises_without_network(self):
        with self.assertRaises(FileNotFoundError):
            ensure_tehsil_watershed("Bihar", "Banka", "Banka")

    @patch("computing.base_layer_setup._active_tehsil_locations")
    def test_ensure_all_reports_missing(self, active):
        active.return_value = [("Bihar", "Banka", "Banka")]
        with self.assertRaisesRegex(FileNotFoundError, "Bihar/Banka/Banka"):
            ensure_tehsil_watersheds()
        self._touch("filtered_mws_banka_banka_uid.geojson")
        ensure_tehsil_watersheds()


class TehsilFileCandidatesTests(SimpleTestCase):
    STEM = staticmethod(lambda d, t: f"{d}_{t}")

    def _resolve(self, relative):
        from computing.base_layer_setup import tehsil_file_candidates

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / relative
            path.parent.mkdir(parents=True)
            path.write_text("{}")
            candidates = tehsil_file_candidates(
                root,
                "West Bengal",
                "North Twenty-Four Parganas",
                "Basirhat-II",
                self.STEM,
                (".geojson",),
            )
            found = [p for p in candidates if p.exists()]
            return [p.relative_to(root) for p in found]

    def test_all_hyphen_and_underscore_combinations_resolve(self):
        for dirs in ("north_twenty-four_parganas/basirhat-ii",
                     "north_twenty_four_parganas/basirhat_ii"):
            for name in ("north_twenty-four_parganas_basirhat-ii",
                         "north_twenty_four_parganas_basirhat_ii"):
                relative = Path("west_bengal") / dirs / f"{name}.geojson"
                with self.subTest(relative=str(relative)):
                    self.assertEqual(self._resolve(relative), [relative])

    def test_names_without_hyphens_have_single_candidate(self):
        from computing.base_layer_setup import tehsil_file_candidates

        candidates = tehsil_file_candidates(
            Path("/r"), "Bihar", "Banka", "Banka", self.STEM, (".geojson",)
        )
        self.assertEqual(candidates, [Path("/r/bihar/banka/banka/banka_banka.geojson")])
