"""Tests for the rainfall stage's maths (no data files needed).

Run from the repo root:
    python -m unittest computing.drought.local.test_rainfall -v
"""

import unittest

import numpy as np
from scipy import stats

from computing.drought.local import rainfall as rf


class SpiTests(unittest.TestCase):
    def test_fit_recovers_gamma_and_spi_is_standard_normal(self):
        rng = np.random.default_rng(0)
        totals = rng.gamma(shape=2.0, scale=50.0, size=(300, 200))  # 300 years, 200 pixels
        alpha, beta, zero = rf.gamma_fit(totals)
        self.assertAlmostEqual(float(np.median(alpha)), 2.0, delta=0.2)
        self.assertAlmostEqual(float(np.median(alpha * beta)), 100.0, delta=8)
        self.assertEqual(float(zero.max()), 0.0)
        values = rf.spi(totals, alpha, beta, zero)
        self.assertAlmostEqual(float(values.mean()), 0.0, delta=0.05)
        self.assertAlmostEqual(float(values.std()), 1.0, delta=0.05)

    def test_matches_scipy_gamma_cdf(self):
        alpha, beta = np.array([2.0]), np.array([50.0])
        for x in (10.0, 100.0, 300.0):
            expected = stats.norm.ppf(stats.gamma.cdf(x, 2.0, scale=50.0))
            self.assertAlmostEqual(float(rf.spi(np.array([x]), alpha, beta, np.array([0.0]))[0]), expected, places=6)

    def test_zero_totals(self):
        totals = np.array([0.0] * 10 + list(np.linspace(10, 200, 20)))[:, None]
        alpha, beta, zero = rf.gamma_fit(totals)
        self.assertAlmostEqual(float(zero[0]), 1 / 3)
        self.assertAlmostEqual(float(rf.spi(np.array([0.0]), alpha, beta, zero)[0]), float(stats.norm.ppf(1 / 3)), places=6)

    def test_too_few_rainy_years_gives_no_spi(self):
        totals = np.array([0.0] * 28 + [5.0, 7.0])[:, None]
        alpha, beta, zero = rf.gamma_fit(totals)
        self.assertTrue(np.isnan(rf.spi(np.array([6.0]), alpha, beta, zero)[0]))


class OnsetTests(unittest.TestCase):
    def setUp(self):
        # Normals: 2 mm a week until week 22, then 60 mm a week to week 39, then 5 mm.
        self.normals = np.full((52, 1), 2.0)
        self.normals[21:39] = 60.0
        self.normals[39:] = 5.0
        self.threshold = rf.onset_threshold(self.normals, np.array([50.0]))  # median of the 52 normals: 5 mm

    def onset(self, this_year, history, land=None, lon=None, lat=None):
        cells = this_year.shape[-1]
        land = np.ones(cells, bool) if land is None else land
        lon = np.arange(cells, dtype=float) * 0.05 if lon is None else lon
        lat = np.full(cells, 20.0) if lat is None else lat
        threshold = np.repeat(self.threshold, cells)
        week, source = rf.season_onset(this_year[rf.SEASON_SPAN], history[:, rf.SEASON_SPAN], threshold, land, lon, lat)
        return week.tolist(), source.tolist()

    def year_with_onset(self, week, cells=1):
        weekly = np.full((52, cells), 1.0)
        weekly[week - 1:] = 60.0
        return weekly

    def test_threshold_is_the_zone_percentile_of_the_52_normals(self):
        self.assertEqual(self.threshold.tolist(), [5.0])

    def test_this_year(self):
        history = np.stack([self.year_with_onset(22)])
        self.assertEqual(self.onset(self.year_with_onset(26), history), ([26.0], [rf.ONSET_THIS_YEAR]))

    def test_no_onset_takes_the_median_of_past_onsets(self):
        dry = np.full((52, 1), 1.0)
        history = np.stack([self.year_with_onset(w) for w in (22, 30, 24)] + [dry])  # dry year left out
        self.assertEqual(self.onset(dry, history), ([24.0], [rf.ONSET_PAST_MEDIAN]))

    def test_even_count_takes_the_later_middle_week(self):
        dry = np.full((52, 1), 1.0)
        history = np.stack([self.year_with_onset(w) for w in (22, 30, 24, 26)])
        self.assertEqual(self.onset(dry, history), ([26.0], [rf.ONSET_PAST_MEDIAN]))

    def test_no_onset_ever_copies_the_nearest_cell(self):
        # Cells along a line 0.05 degrees apart; cell 2 never gets rain above the threshold.
        this_year = np.full((52, 4), 1.0)
        this_year[24:, 0] = this_year[27:, 1] = this_year[29:, 3] = 60.0  # onsets 25, 28, -, 30
        history = np.stack([this_year])
        week, source = self.onset(this_year, history)
        # Cells 1 and 3 are equally near: the later of their weeks (28, 30).
        self.assertEqual(week, [25.0, 28.0, 30.0, 30.0])
        self.assertEqual(source, [1, 1, rf.ONSET_NEAREST, 1])

    def test_outside_land_has_no_onset(self):
        history = np.stack([self.year_with_onset(22, cells=2)])
        week, source = self.onset(self.year_with_onset(26, cells=2), history, land=np.array([True, False]))
        self.assertEqual(source, [rf.ONSET_THIS_YEAR, 0])
        self.assertTrue(np.isnan(week[1]))

    def test_upper_median(self):
        values = np.array([[3, 1, np.nan], [2, np.nan, np.nan], [9, 4, np.nan], [1, np.nan, np.nan]])
        self.assertEqual(rf.upper_median(values, axis=0)[:2].tolist(), [3.0, 4.0])  # [1,2,3,9] -> 3; [1,4] -> 4
        self.assertTrue(np.isnan(rf.upper_median(values, axis=0)[2]))

    def test_zones(self):
        pct = rf.zone_percentile(np.array([76.95, 73.0, 77.0, 88.0, 82.10, 68.0]),
                                 np.array([16.35, 30.0, 22.0, 25.0, 22.0, 35.0]))
        # Devadurga southern, Punjab northern, central, eastern, gap -> nearest (eastern), west of A -> northern
        self.assertEqual(pct.tolist(), [50, 75, 69, 65, 65, 75])


class HistoryTests(unittest.TestCase):
    def test_history_window_totals(self):
        history = np.arange(1, 26, dtype=float)[None, :, None]  # weeks 19-43 hold 1..25
        self.assertEqual(rf.history_window_totals(history, 19)[0].tolist(), [1 + 2 + 3 + 4])
        self.assertEqual(rf.history_window_totals(history, 40)[0].tolist(), [22 + 23 + 24 + 25])


class WindowTotalTests(unittest.TestCase):
    def test_window_totals(self):
        weekly = np.arange(1, 53, dtype=float)[:, None]
        totals = rf.window_totals(weekly, starts=[19, 40])
        self.assertEqual(totals[:, 0].tolist(), [19 + 20 + 21 + 22, 40 + 41 + 42 + 43])


if __name__ == "__main__":
    unittest.main()
