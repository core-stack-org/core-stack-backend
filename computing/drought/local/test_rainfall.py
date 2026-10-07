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
        # Normal year: 2 mm a week until week 22, then 60 mm a week to week 39, then 5 mm.
        self.normals = np.full((52, 1), 2.0)
        self.normals[21:39] = 60.0
        self.normals[39:] = 5.0
        self.pct = np.array([50.0])

    def test_this_year(self):
        weekly = self.normals.copy()
        weekly[18:25] = 1.0  # dry until week 25
        week, source = rf.onset_week(weekly, self.normals, self.pct)
        self.assertEqual((int(week[0]), int(source[0])), (26, rf.ONSET_THIS_YEAR))

    def test_failed_monsoon_falls_back_to_normal_onset(self):
        weekly = np.full((52, 1), 1.0)
        week, source = rf.onset_week(weekly, self.normals, self.pct)
        self.assertEqual((int(week[0]), int(source[0])), (22, rf.ONSET_NORMAL))

    def test_zones(self):
        pct = rf.zone_percentile(np.array([76.95, 73.0, 77.0, 88.0, 82.10, 68.0]),
                                 np.array([16.35, 30.0, 22.0, 25.0, 22.0, 35.0]))
        # Devadurga southern, Punjab northern, central, eastern, gap -> nearest (eastern), west of A -> northern
        self.assertEqual(pct.tolist(), [50, 75, 69, 65, 65, 75])


class WindowTotalTests(unittest.TestCase):
    def test_window_totals(self):
        weekly = np.arange(1, 53, dtype=float)[:, None]
        totals = rf.window_totals(weekly, starts=[19, 40])
        self.assertEqual(totals[:, 0].tolist(), [19 + 20 + 21 + 22, 40 + 41 + 42 + 43])


if __name__ == "__main__":
    unittest.main()
