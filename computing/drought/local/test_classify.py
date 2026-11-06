"""Tests for the classification stage's decision logic (no data files needed).

Run from the repo root:
    python -m unittest computing.drought.local.test_classify -v
"""

import unittest

import numpy as np

from computing.drought.local import classify as cf
from computing.drought.local import rules as r

N = len(cf.WINDOW_STARTS)  # 22 windows, weeks 19-40


def run(onset, trigger_windows=(), vci=80.0, mai=90.0, sown=100.0, crop=True, fallback_windows=()):
    one = lambda v: np.full((1, 1), v, dtype=float)
    triggers = np.zeros((N, 1, 1))
    for i in trigger_windows:
        triggers[i] = 1
    fallback = np.zeros((N, 1, 1))
    for i in fallback_windows:
        fallback[i] = 1
    labels, summary = cf.classify_arrays(
        onset=one(onset), triggers=triggers, vci=np.full((N, 1, 1), vci), mai=np.full((N, 1, 1), mai),
        sown=one(sown), crop=np.full((1, 1), crop), fallback=fallback, dry_total=one(5), dry_longest=one(4))
    return labels[:, 0, 0].tolist(), dict(zip(cf.SUMMARY_NAMES, summary[:, 0, 0].tolist()))


class ClassifyTests(unittest.TestCase):
    def test_no_trigger_means_no_drought(self):
        labels, s = run(onset=19, vci=10, mai=10, sown=10)
        self.assertEqual((s["weeks_none"], s["season_weeks"]), (25, 25))

    def test_severe_window_covers_four_weeks(self):
        labels, s = run(onset=19, trigger_windows=[0, 1], vci=10, mai=10, sown=10)
        self.assertEqual(labels[:6], [r.SEVERE] * 5 + [r.NONE])  # windows at weeks 19 and 20 cover 19-23
        self.assertEqual((s["weeks_severe"], s["weeks_none"]), (5, 20))

    def test_weeks_before_onset_do_not_count(self):
        labels, s = run(onset=25, trigger_windows=range(N), vci=10, mai=10, sown=10)
        self.assertEqual(s["season_weeks"], 43 - 25 + 1)
        self.assertEqual(s["weeks_severe"], 19)
        self.assertEqual(labels[:6], [255] * 6)  # weeks 19-24 are outside the season

    def test_mild_and_moderate(self):
        _, s = run(onset=19, trigger_windows=[5], vci=80, mai=90, sown=100)
        self.assertEqual(s["weeks_mild"], 4)
        _, s = run(onset=19, trigger_windows=[5], vci=50, mai=60, sown=100)
        self.assertEqual(s["weeks_moderate"], 4)

    def test_missing_vci_counts_as_normal(self):
        _, s = run(onset=19, trigger_windows=[5], vci=np.nan, mai=10, sown=10)
        self.assertEqual((s["weeks_moderate"], s["weeks_severe"]), (4, 0))

    def test_not_cropland(self):
        labels, s = run(onset=19, trigger_windows=[0], vci=10, mai=10, sown=10, crop=False)
        self.assertEqual(s["weeks_severe"], 255)
        self.assertEqual((s["dry_spell_weeks"], s["longest_dry_spell_weeks"], s["onset_week"]), (5, 4, 19))
        self.assertTrue(all(v == 255 for v in labels))

    def test_cloud_fallback_counted_only_in_season(self):
        _, s = run(onset=21, fallback_windows=[0, 1, 2, 3])
        self.assertEqual(s["windows_cloud_fallback"], 2)  # windows starting at weeks 21 and 22


if __name__ == "__main__":
    unittest.main()
