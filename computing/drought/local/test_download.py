"""Tests for the input download's checks and locks (no network needed).

Run from the repo root:
    python -m unittest computing.drought.local.test_download -v
"""

import os
import tempfile
import unittest
from pathlib import Path

from computing.drought.local import download as dl


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
