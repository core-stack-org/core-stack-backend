"""Tests for the stage runner's helpers and the run lock (no data files needed).

Run from the repo root:
    python -m unittest computing.drought.local.test_pipeline -v
"""

import os
import tempfile
import unittest
from pathlib import Path

from computing.drought.local import cropland, mws, rainfall, vegetation
from computing.drought.local import pipeline as pl
from computing.drought.local.config import Paths
from computing.drought.local.store import NotWritable, RunInProgress, not_writable, run_in_progress, run_lock


class StageListTests(unittest.TestCase):
    def test_run_order_and_spelling(self):
        self.assertEqual(pl.stage_list("mws, Classify,rain"), ["rain", "classify", "mws"])
        self.assertEqual(pl.stage_list(["mai", "mai"]), ["mai"])

    def test_unknown_stage(self):
        with self.assertRaises(ValueError):
            pl.stage_list("rain,foo")


class RerunTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.paths = Paths(Path(tmp.name), Path(tmp.name))

    def touch(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
        return path

    def test_deletes_only_the_seasons_own_files(self):
        own = [self.touch(rainfall.season_file(self.paths, 2023)), self.touch(cropland.share_file(self.paths, 2023)),
               self.touch(vegetation.vci_file(self.paths, 2023)), self.touch(mws.layer_file(self.paths, 2023))]
        other_season = self.touch(rainfall.season_file(self.paths, 2022))
        shared = [self.touch(rainfall.weekly_file(self.paths, 2023)), self.touch(cropland.normal_file(self.paths)),
                  self.touch(vegetation.window_max_file(self.paths, 2023)), self.touch(mws.watersheds_file(self.paths))]
        removed = pl.clear_outputs(self.paths, ["rain", "cropland", "vegetation", "mws"], [2023])
        self.assertEqual(sorted(removed), sorted(map(str, own)))
        self.assertFalse(any(p.exists() for p in own))
        self.assertTrue(other_season.exists() and all(p.exists() for p in shared))

    def test_rerun_must_be_among_the_stages(self):
        with self.assertRaises(ValueError):
            pl.run_pipeline(seasons=[2023], stages=["rain"], rerun_stages=["mws"], data_dir=self.paths.data_dir)


class LockTests(unittest.TestCase):
    def test_second_run_is_refused_while_the_first_holds_the_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = Paths(Path(tmp), Path(tmp))
            self.assertFalse(run_in_progress(paths))
            with run_lock(paths):
                self.assertTrue(run_in_progress(paths))
                with self.assertRaises(RunInProgress):
                    with run_lock(paths):
                        pass
            self.assertFalse(run_in_progress(paths))


@unittest.skipIf(os.geteuid() == 0, "root can write everywhere")
class WritableTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.paths = Paths(Path(tmp.name), Path(tmp.name))
        self.locked = self.paths.version_dir / "moisture"
        self.locked.mkdir(parents=True)
        self.locked.chmod(0o555)  # as if made by another user
        self.addCleanup(self.locked.chmod, 0o755)

    def test_message_names_the_folder_and_the_fix(self):
        message = not_writable(self.paths.base)
        self.assertIn(str(self.locked), message)
        self.assertIn("chown -R", message)
        self.assertIsNone(not_writable(self.paths.version_dir / "rain"))

    def test_run_is_refused_before_it_starts(self):
        with self.assertRaises(NotWritable):
            pl.run_pipeline(seasons=[2023], stages=["mai"], data_dir=self.paths.data_dir)


if __name__ == "__main__":
    unittest.main()
