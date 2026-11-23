"""Tests for the pan-India LULC v4 temporal correction: the rules, the six-year and incremental
runs and the state they keep, and the two API views.

Run in the container:
    docker compose exec backend python manage.py test computing.lulc.v4.temporal_correction.test_temporal_correction

FixtureTests compares with the reference outputs of 4_temporal_correction.py when the handoff
test fixture is in data/base_layers/periodic/lulc_v4_temporal_correction_fixture/ (its inland/
and border/ folders); it is skipped otherwise.
"""

import json
import tempfile
from pathlib import Path
from unittest import TestCase, skipUnless
from unittest.mock import MagicMock, patch

import numpy as np
import rasterio
from django.test import override_settings
from django.urls import reverse
from rasterio.transform import from_origin
from rest_framework.test import APISimpleTestCase

from computing.config_loader import DATA_DIR
from computing.lulc.v4.temporal_correction import rules, runner

FIXTURE_DIR = DATA_DIR / "base_layers/periodic/lulc_v4_temporal_correction_fixture"

BUILT_UP, WATER, CROP, CROP_DOUBLE, PLANTATION = 1, 2, 8, 9, 13


def px(*values):
    """One 1 x 1 uint8 array per value: a single pixel's history."""
    return [np.array([[v]], np.uint8) for v in values]


def write_year(folder, year, label, cluster, name="test_lulc"):
    path = Path(folder) / f"{name}_{runner.span_for(year)}.tif"
    label = np.asarray(label, np.uint8)
    profile = dict(driver="GTiff", dtype="uint8", count=2, nodata=0, width=label.shape[1], height=label.shape[0],
                   crs="EPSG:4326", transform=from_origin(84.0, 27.0, 0.0001, 0.0001))
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(label, 1)
        dst.write(np.asarray(cluster, np.uint8), 2)
    return path


def write_inputs(folder, years, labels=None):
    """Raw 2 x 2 inputs; labels maps year -> 2 x 2 labels (default all crop)."""
    Path(folder).mkdir(parents=True, exist_ok=True)
    for year in years:
        label = (labels or {}).get(year, np.full((2, 2), CROP))
        write_year(folder, year, label, np.full((2, 2), CROP_DOUBLE))


def read(path):
    with rasterio.open(path) as src:
        return src.read(1)


class RuleTests(TestCase):
    def test_crop_built_up_crop_blip_is_corrected(self):
        out = rules.correct_block_6yr(px(CROP, CROP, BUILT_UP, CROP, CROP, CROP), px(*[CROP_DOUBLE] * 6))
        # a pixel corrected to crop takes its subclass from predicted_cluster; crop pixels keep theirs
        self.assertEqual([int(a[0, 0]) for a in out], [CROP, CROP, CROP_DOUBLE, CROP, CROP, CROP])

    def test_disallowed_flip_is_left_alone(self):
        out = rules.correct_block_6yr(px(WATER, WATER, CROP, WATER, WATER, WATER), px(*[CROP_DOUBLE] * 6))
        self.assertEqual(int(out[2][0, 0]), CROP)

    def test_incremental_aba_centre(self):
        (prev, cur, nxt), cluster = px(CROP, PLANTATION, CROP), px(CROP_DOUBLE)[0]
        final, flipped = rules.correct_block_incremental(prev, cur, cluster, nxt)
        self.assertEqual(int(final[0, 0]), CROP_DOUBLE)
        self.assertTrue(flipped[0, 0])


class RunnerTests(TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.inputs, self.out = Path(tmp.name) / "in", Path(tmp.name) / "out"

    def states(self):
        return runner.year_states(self.out)

    def test_six_year_then_incremental_one_year_at_a_time(self):
        # pixel (0, 0) has a plantation blip in 2023, which only the incremental step can see
        blip = np.full((2, 2), CROP)
        blip[0, 0] = PLANTATION
        write_inputs(self.inputs, range(2017, 2025), {2023: blip})

        runner.run_six_year(self.inputs, self.out, workers=1)
        self.assertEqual(self.states(), {**{y: "final" for y in range(2017, 2022)}, 2022: "provisional"})

        result = runner.run_incremental(self.inputs, self.out, workers=1)
        self.assertEqual((result["final"], result["provisional"]), ([2022], 2023))
        self.assertEqual(self.states()[2023], "provisional")
        # the provisional year is written through uncorrected
        self.assertEqual(int(read(runner.out_path(self.out, "test_lulc", 2023))[0, 0]), PLANTATION)

        result = runner.run_incremental(self.inputs, self.out, all_years=True, workers=1)
        self.assertEqual((result["final"], result["provisional"]), ([2023], 2024))
        self.assertEqual(int(read(runner.out_path(self.out, "test_lulc", 2023))[0, 0]), CROP_DOUBLE)
        self.assertEqual(result["flips"]["2023"], {"flipped": 1, "valid": 4})

        with self.assertRaisesRegex(runner.TemporalCorrectionError, "2024 stays provisional"):
            runner.run_incremental(self.inputs, self.out, workers=1)
        manifest = json.loads(runner.manifest_path(self.out).read_text())
        self.assertEqual([h["mode"] for h in manifest["history"]], ["batch6", "incremental", "incremental"])

    def test_no_data_is_never_filled(self):
        hole = np.full((2, 2), CROP)
        hole[1, 1] = 0
        write_inputs(self.inputs, range(2017, 2023), {2019: hole})
        runner.run_six_year(self.inputs, self.out, workers=1)
        self.assertEqual(int(read(runner.out_path(self.out, "test_lulc", 2019))[1, 1]), 0)

    def test_six_year_needs_six_input_years(self):
        write_inputs(self.inputs, [2017, 2018, 2019, 2020, 2022, 2023])
        with self.assertRaisesRegex(runner.TemporalCorrectionError, r"missing input years: \[2021\]"):
            runner.run_six_year(self.inputs, self.out, workers=1)

    def test_incremental_needs_the_six_year_run(self):
        write_inputs(self.inputs, range(2017, 2024))
        with self.assertRaisesRegex(runner.TemporalCorrectionError, "run the six-year API first"):
            runner.run_incremental(self.inputs, self.out, workers=1)

    def test_one_run_at_a_time(self):
        with runner.run_lock(self.out):
            self.assertTrue(runner.run_in_progress(self.out))
            with self.assertRaises(runner.RunInProgress):
                with runner.run_lock(self.out):
                    pass
        self.assertFalse(runner.run_in_progress(self.out))


@skipUnless((FIXTURE_DIR / "inland/expected").is_dir(), f"no test fixture in {FIXTURE_DIR}")
class FixtureTests(TestCase):
    """The six-year run, then incremental with all_years, must match 4_temporal_correction.py."""

    def check(self, area):
        expected = {int(runner.INPUT_RE.search(p.name)["y1"]): p for p in (FIXTURE_DIR / area / "expected").glob("*.tif")}
        with tempfile.TemporaryDirectory() as out:
            runner.run_six_year(FIXTURE_DIR / area / "inputs", out, workers=2, block=512)
            runner.run_incremental(FIXTURE_DIR / area / "inputs", out, all_years=True, workers=2, block=512)
            for year, path in sorted(expected.items()):
                got = read(runner.out_path(out, f"fixture_{area}", year))
                self.assertEqual(int((got != read(path)).sum()), 0, f"{area} {year}")

    def test_inland(self):
        self.check("inland")

    def test_border(self):
        self.check("border")


@override_settings(HEAVY_WORKER_ENABLED=True, HEAVY_TASK_QUEUE="heavy")
class TemporalCorrectionAPITests(APISimpleTestCase):
    def setUp(self):
        self.client.force_authenticate(user=MagicMock(is_authenticated=True))
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.data_dir = Path(tmp.name).resolve()
        self.inputs, self.out = self.data_dir / "in", self.data_dir / "out"
        for name, value in (("CONFIG_DATA_DIR", self.data_dir), ("LULC_TEMPORAL_CORRECTION_INPUT_DIR", self.inputs),
                            ("LULC_TEMPORAL_CORRECTION_OUTPUT_DIR", self.out)):
            patcher = patch(f"computing.api.{name}", value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def post(self, url_name, payload):
        return self.client.post(reverse(url_name), payload, format="json")

    @patch("computing.api.lulc_temporal_correction_six_year_task.apply_async")
    def test_six_year_is_queued_on_the_heavy_queue(self, apply_async):
        apply_async.return_value = MagicMock(id="task-1")
        write_inputs(self.inputs, range(2017, 2025))
        response = self.post("pan_india_lulc_temporal_correction_six_year", {"workers": 4})
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual((response.data["years"], response.data["provisional"]), (list(range(2017, 2023)), 2022))
        apply_async.assert_called_once_with(
            kwargs={"input_dir": str(self.inputs), "output_dir": str(self.out), "start_year": 2017, "workers": 4},
            queue="heavy")

    @patch("computing.api.lulc_temporal_correction_six_year_task.apply_async")
    def test_six_year_does_not_redo_a_finished_run_unless_asked(self, apply_async):
        apply_async.return_value = MagicMock(id="task-1")
        write_inputs(self.inputs, range(2017, 2023))
        runner.run_six_year(self.inputs, self.out, workers=1)
        response = self.post("pan_india_lulc_temporal_correction_six_year", {})
        self.assertEqual(response.status_code, 400)
        self.assertIn("overwrite=true", response.data["Exception"])
        apply_async.assert_not_called()
        self.assertEqual(self.post("pan_india_lulc_temporal_correction_six_year", {"overwrite": True}).status_code, 200)

    @patch("computing.api.lulc_temporal_correction_six_year_task.apply_async")
    def test_bad_six_year_requests_are_refused(self, apply_async):
        write_inputs(self.inputs, range(2017, 2022))  # five years only
        for payload in ({}, {"start_year": 2016}, {"input_dir": "/etc"}, {"output_dir": "../elsewhere"},
                        {"workers": 0}, {"district": "raichur"}, {"pan_india": True}, {"compute": "gee"}):
            self.assertEqual(self.post("pan_india_lulc_temporal_correction_six_year", payload).status_code, 400, payload)
        apply_async.assert_not_called()

    @patch("computing.api.lulc_temporal_correction_incremental_task.apply_async")
    def test_incremental_is_queued_after_the_six_year_run(self, apply_async):
        apply_async.return_value = MagicMock(id="task-2")
        write_inputs(self.inputs, range(2017, 2026))
        self.assertEqual(self.post("pan_india_lulc_temporal_correction_incremental", {}).status_code, 400)

        runner.run_six_year(self.inputs, self.out, workers=1)
        response = self.post("pan_india_lulc_temporal_correction_incremental", {"all": True})
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual((response.data["finalising"], response.data["provisional"]), ([2022, 2023, 2024], 2025))
        apply_async.assert_called_once_with(
            kwargs={"input_dir": str(self.inputs), "output_dir": str(self.out), "all_years": True,
                    "workers": runner.DEFAULT_WORKERS},
            queue="heavy")

    @patch("computing.api.lulc_temporal_correction_incremental_task.apply_async")
    def test_running_correction_is_409(self, apply_async):
        with patch("computing.api.temporal_correction.run_in_progress", return_value=True):
            self.assertEqual(self.post("pan_india_lulc_temporal_correction_incremental", {}).status_code, 409)
        apply_async.assert_not_called()

    @override_settings(HEAVY_WORKER_ENABLED=False)
    def test_without_a_heavy_worker_is_503(self):
        self.assertEqual(self.post("pan_india_lulc_temporal_correction_six_year", {}).status_code, 503)
