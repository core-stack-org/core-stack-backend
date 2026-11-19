"""Tests for the rainfall stage's maths (no data files needed).

Run from the repo root:
    python -m unittest computing.drought.local.test_rainfall -v
"""

import datetime as dt
import unittest

import numpy as np
from rasterio.transform import from_origin
from scipy import stats

from computing.drought.local import onset
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
    def days(self, first=(5, 7), n=60, year=2023):
        start = dt.date(year, *first)
        return [start + dt.timedelta(days=i) for i in range(n)]

    def test_kerala_date_needs_6_stations_on_2_days_from_22_may(self):
        days = self.days()
        rain = np.zeros((len(days), 12))
        i = {d: k for k, d in enumerate(days)}
        rain[i[dt.date(2023, 5, 15)]:i[dt.date(2023, 5, 17)], :8] = 20.0  # before 22 May
        rain[i[dt.date(2023, 5, 25)]:i[dt.date(2023, 5, 27)], :5] = 20.0  # only 5 stations
        rain[i[dt.date(2023, 6, 1)], :6] = 20.0  # a single day
        rain[i[dt.date(2023, 6, 7)]:i[dt.date(2023, 6, 9)], :6] = 15.0
        self.assertEqual(onset.kerala_date(rain, days), dt.date(2023, 6, 8).timetuple().tm_yday)

    def test_kerala_pair_may_start_on_21_may(self):
        days = self.days()
        rain = np.zeros((len(days), 12))
        rain[14:16, :] = 30.0  # 21 and 22 May
        self.assertEqual(onset.kerala_date(rain, days), dt.date(2023, 5, 22).timetuple().tm_yday)

    def test_first_pair_from_the_earliest_day(self):
        area = np.zeros((40, 4), np.float32)
        area[5:7] = 3.0  # a pair before the earliest day
        area[20] = 3.0  # a single wet day
        area[25:27] = 3.0
        threshold = np.array([2.5, 2.5, 5.0, np.nan], np.float32)
        day, source = onset.first_pair(area, 100, threshold, np.array([109.2, 110, 110, 110]))
        self.assertEqual(day.tolist(), [126, 126, 110, 110])  # cell 2: no pair reaches 5 mm -> earliest day
        self.assertEqual(source.tolist(), [onset.SOURCE_RAIN, onset.SOURCE_RAIN, onset.SOURCE_FALLBACK,
                                           onset.SOURCE_NO_RAIN_TEST])

    def test_pair_on_the_earliest_day_counts(self):
        area = np.zeros((40, 1), np.float32)
        area[9:11] = 1.0
        day, _ = onset.first_pair(area, 100, np.array([1.0], np.float32), np.array([110.0]))
        self.assertEqual(day.tolist(), [110])

    def test_latitude_limit(self):
        self.assertEqual(onset.latitude_limit(150, np.array([8.0, 10.0, 25.0]), 0.75).tolist(), [150, 150, 170])

    def test_area_rain_uses_india_cells_only(self):
        india = np.zeros((30, 30), bool)
        india[:, :15] = True
        daily = np.where(india, 4.0, 100.0)[None].astype(np.float32)  # heavy rain outside India
        area = onset.area_rain(daily, india)
        self.assertAlmostEqual(float(area[0, 15, 14]), 4.0, places=4)  # on the border
        self.assertAlmostEqual(float(area[0, 15, 20]), 4.0, places=4)  # outside, India cells in its box
        self.assertTrue(np.isnan(area[0, 15, 29]))  # no India cell in its box

    def test_region_settings(self):
        threshold, speed, margin = onset.region_settings(np.array([1, 4, 7, 0]))
        self.assertEqual(threshold[:2].tolist(), [2.5, 0.5])
        self.assertTrue(np.isnan(threshold[2]))  # Ladakh: no rain test
        self.assertEqual(speed[:3].tolist(), [0.75, 0.5, 0.75])
        self.assertEqual(margin[:3].tolist(), [3, 5, 0])

    def test_regions_by_state_with_nearest_fill(self):
        transform = from_origin(70.0, 30.0, 0.05, 0.05)
        box = lambda x0, x1: {"type": "Polygon", "coordinates": [[(x0, 29.5), (x1, 29.5), (x1, 30.0), (x0, 30.0), (x0, 29.5)]]}  # noqa: E731
        tehsils = [{"geometry": box(70.0, 70.25), "properties": {"STATE": "RAJASTHAN"}},
                   {"geometry": box(70.25, 70.4), "properties": {"STATE": "Disputed (West Bengal)"}},
                   {"geometry": box(70.4, 70.5), "properties": {"STATE": "DISPUTED"}}]
        land = np.ones((10, 10), bool)
        land[:, 9] = False
        region, india = onset.build_regions(tehsils, [{"geometry": box(70.0, 70.3)}], transform, (10, 10), land)
        self.assertEqual(region[0].tolist(), [3] * 5 + [2] * 3 + [2, 0])  # DISPUTED: nearest; sea: 0
        self.assertEqual(india[0].tolist(), [True] * 6 + [False] * 4)

    def test_station_cells_on_the_chirps_window(self):
        rows, cols = onset.station_cells(from_origin(67.60, 38.00, 0.05, 0.05))
        names = list(onset.KERALA_STATIONS)
        k, t = names.index("Kochi"), names.index("Thiruvananthapuram")
        self.assertEqual((rows[t], cols[t]), (590, 186))
        self.assertEqual((rows[k], cols[k]), (560, 173))


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
