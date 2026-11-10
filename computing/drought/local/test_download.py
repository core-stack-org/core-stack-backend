"""Tests for the input download's checks and locks (no network needed).

Run from the repo root:
    python -m unittest computing.drought.local.test_download -v
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import requests

from computing.drought.local import download as dl


class FakeListing:
    def __init__(self, text, status_code=200):
        self.text, self.status_code = text, status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


class ChirpsListingTests(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.object(dl.time, "sleep")
        self.sleep = patcher.start()
        self.addCleanup(patcher.stop)

    def test_days_come_from_the_moved_folders_and_cog_wins(self):
        listings = {f"{dl.CHIRPS_TIF_URL}/2001/": '"chirps-v2.0.2001.01.01.tif.gz" "chirps-v2.0.2001.01.02.tif.gz"',
                    f"{dl.CHIRPS_COG_URL}/2001/": '"chirps-v2.0.2001.01.01.cog"'}
        http = mock.Mock(get=lambda url, timeout: FakeListing(listings[url]))
        days = dl.chirps_days_on_server(http, 2001)
        self.assertTrue(all("/products/CHIRPS/v2.0/global_daily/" in url for url in days.values()))
        self.assertTrue(days[dl.dt.date(2001, 1, 1)].endswith(".cog"))
        self.assertTrue(days[dl.dt.date(2001, 1, 2)].endswith(".tif.gz"))
        self.sleep.assert_called_with(dl.CHIRPS_LISTING_PAUSE)  # a pause before each listing

    def test_refused_listing_leaves_the_rest_for_the_next_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = SimpleNamespace(chirps_start_year=2001, end_year=2003, dry_run=False, chirps_workers=1)
            with mock.patch.object(dl, "chirps_days_on_server", side_effect=requests.ConnectionError("dropped")) as listing:
                failed = dl.run_chirps(Path(tmp), None, args)
            self.assertEqual(failed, 1)
            self.assertEqual(listing.call_count, 1)  # stops at the first year instead of trying every year
            manifest = json.loads((Path(tmp) / "rainfall_chirps" / "manifest.json").read_text())
            self.assertIn("listing 2001", manifest["failed"])

    def test_chirps_session_retries_refusals(self):
        retry = dl.chirps_session().get_adapter("https://").max_retries
        self.assertIn(403, retry.status_forcelist)
        self.assertNotIn(403, dl.http_session().get_adapter("https://").max_retries.status_forcelist)


class DownloadTests(unittest.TestCase):
    def test_unknown_product(self):
        with self.assertRaises(ValueError):
            dl.run_download(products=["chirps", "foo"])

    def test_missing_data_folder(self):
        with self.assertRaises(FileNotFoundError):
            dl.run_download(products=["chirps"], data_dir="/nonexistent/drought-data")

    def test_lock_is_reported_and_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / "base_layers" / "periodic" / "rainfall_chirps"
            self.assertFalse(dl.download_in_progress(tmp))
            with dl.folder_lock(folder):
                self.assertTrue(dl.download_in_progress(tmp))
                with self.assertRaises(dl.DownloadError):
                    with dl.folder_lock(folder):
                        pass
            self.assertFalse(dl.download_in_progress(tmp))

    def test_gdal_settings_last_only_for_the_run(self):
        key = "GDAL_DISABLE_READDIR_ON_OPEN"
        before = os.environ.pop(key, None)
        try:
            with dl.gdal_settings():
                self.assertEqual(os.environ.get(key), "EMPTY_DIR")
            self.assertNotIn(key, os.environ)
        finally:
            if before is not None:
                os.environ[key] = before


if __name__ == "__main__":
    unittest.main()
