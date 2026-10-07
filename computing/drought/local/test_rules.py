"""Tests for the calendar, composite and decision rules of the local drought layer.

Run from the repo root:
    python -m unittest computing.drought.local.test_rules -v
"""

import datetime as dt
import math
import unittest

import numpy as np
from rasterio.transform import Affine

from computing.drought.local import rules as r
from computing.drought.local.grid import EARTH_RADIUS, Grid, lookup_index, pixel_lonlat, tile_window
from computing.drought.local.weeks import (
    composite_end,
    composite_starts,
    mai_weights,
    season_composites,
    season_window_starts,
    vci_composites,
    week_end,
    week_of,
    week_start,
    window_dates,
)

D = dt.date
N, M, S = r.NORMAL, r.IMPACT_MODERATE, r.IMPACT_SEVERE


class WeekTests(unittest.TestCase):
    def test_known_weeks(self):
        self.assertEqual((week_start(2023, 1), week_end(2023, 1)), (D(2023, 1, 1), D(2023, 1, 7)))
        self.assertEqual((week_start(2023, 9), week_end(2023, 9)), (D(2023, 2, 26), D(2023, 3, 4)))
        self.assertEqual((week_start(2024, 9), week_end(2024, 9)), (D(2024, 2, 26), D(2024, 3, 4)))
        self.assertEqual((week_end(2024, 9) - week_start(2024, 9)).days + 1, 8)  # takes 29 February
        self.assertEqual(week_start(2023, 10), D(2023, 3, 5))
        self.assertEqual(week_start(2024, 10), D(2024, 3, 5))
        self.assertEqual((week_start(2023, 52), week_end(2023, 52)), (D(2023, 12, 24), D(2023, 12, 31)))
        self.assertEqual((week_start(2023, 19), week_end(2023, 19)), (D(2023, 5, 7), D(2023, 5, 13)))
        self.assertEqual((week_start(2023, 43), week_end(2023, 43)), (D(2023, 10, 22), D(2023, 10, 28)))

    def test_weeks_cover_every_day_once(self):
        for year in (2023, 2024):
            day = D(year, 1, 1)
            for week in range(1, 53):
                self.assertEqual(week_start(year, week), day)
                while day <= week_end(year, week):
                    self.assertEqual(week_of(day), week)
                    day += dt.timedelta(days=1)
            self.assertEqual(day, D(year + 1, 1, 1))

    def test_windows(self):
        self.assertEqual(week_of(D(2023, 7, 2)), 27)
        self.assertEqual(window_dates(2023, 27), (D(2023, 7, 2), D(2023, 7, 29)))
        starts = season_window_starts(19)
        self.assertEqual((starts[0], starts[-1]), (19, 40))  # last window is weeks 40-43
        self.assertEqual(season_window_starts(26)[0], 26)
        self.assertEqual(season_window_starts(10)[0], 19)  # onset search starts in week 19


class CompositeTests(unittest.TestCase):
    def test_composite_dates(self):
        self.assertEqual(composite_starts(2023)[22], D(2023, 6, 26))  # day 177
        self.assertEqual(composite_end(D(2023, 12, 27)), D(2023, 12, 31))  # last one is short
        first, last = season_composites(2023)[0], season_composites(2023)[-1]
        self.assertEqual((first, last), (D(2023, 5, 1), D(2023, 10, 24)))

    def test_method_note_example(self):
        """Weeks 27-30 of 2023 (2-29 July), as in the method note."""
        start, end = window_dates(2023, 27)
        self.assertEqual(
            mai_weights(start, end),
            {D(2023, 6, 26): 0.25, D(2023, 7, 4): 1, D(2023, 7, 12): 1, D(2023, 7, 20): 1, D(2023, 7, 28): 0.25},
        )
        self.assertEqual(vci_composites(start, end), [D(2023, 7, 4), D(2023, 7, 12), D(2023, 7, 20)])

    def test_weights_cover_the_window_exactly(self):
        for year in (2023, 2024):
            for week in season_window_starts(19):
                start, end = window_dates(year, week)
                covered = sum(w * ((composite_end(s) - s).days + 1) for s, w in mai_weights(start, end).items())
                self.assertAlmostEqual(covered, (end - start).days + 1)
                self.assertGreaterEqual(len(vci_composites(start, end)), 3)


class IndicatorTests(unittest.TestCase):
    def test_vci(self):
        self.assertEqual(r.vci_class([39.9, 40, 59.9, 60, np.nan]).tolist(), [S, M, M, N, N])

    def test_mai(self):
        self.assertEqual(r.mai_class([50, 50.1, 75, 75.1, np.nan]).tolist(), [S, M, M, N, N])

    def test_area_sown(self):
        self.assertEqual(r.sown_class([75, 75.1, 84.9, 85, np.nan]).tolist(), [S, M, M, N, N])

    def test_rain_trigger(self):
        got = r.rain_trigger([True, False, False, False, False], [0, -60, -59, 0, np.nan], [0, 0, 0, -1.6, np.nan])
        self.assertEqual(got.tolist(), [True, True, False, True, False])

    def test_dry_weeks(self):
        got = r.dry_weeks([4, 5, 0, np.nan], [10, 10, 0, 10])
        self.assertEqual(got.tolist(), [True, False, False, False])


class WindowClassTests(unittest.TestCase):
    CASES = [
        # trigger, vci, mai, sown -> class
        (False, S, S, S, r.NONE),
        (True, N, N, N, r.MILD),
        (True, S, S, S, r.SEVERE),
        (True, S, S, M, r.SEVERE),
        (True, M, S, S, r.SEVERE),
        (True, S, S, N, r.MODERATE),  # two severe + one normal: moderate (decided reading)
        (True, M, M, N, r.MODERATE),
        (True, S, M, N, r.MODERATE),
        (True, M, M, M, r.MODERATE),
        (True, S, N, N, r.MILD),
        (True, M, N, N, r.MILD),
    ]

    def test_table(self):
        for trigger, vci, mai, sown, expected in self.CASES:
            with self.subTest(trigger=trigger, vci=vci, mai=mai, sown=sown):
                self.assertEqual(int(r.window_class(trigger, vci, mai, sown)), expected)

    def test_missing_indicator_counts_as_normal(self):
        vci = r.vci_class(np.nan)
        self.assertEqual(int(r.window_class(True, vci, r.mai_class(40), r.sown_class(70))), r.MODERATE)

    def test_arrays(self):
        got = r.window_class(np.array([[True, False], [True, True]]), np.array([[S, S], [N, M]]), S, S)
        self.assertEqual(got.tolist(), [[r.SEVERE, r.NONE], [r.MODERATE, r.SEVERE]])


class SeasonTests(unittest.TestCase):
    def test_weekly_labels(self):
        self.assertEqual(r.weekly_labels([1, 0, 3, 0]).tolist(), [1, 1, 3, 3, 3, 3, 0])

    def test_dry_spell_weeks(self):
        T, F = True, False
        self.assertEqual(int(r.dry_spell_weeks([F, T, T, T, T, F, T, T, T, F])), 4)
        self.assertEqual(int(r.dry_spell_weeks([T] * 6)), 6)
        self.assertEqual(int(r.dry_spell_weeks([T, T, T, F, T, T, T, T])), 4)
        self.assertEqual(int(r.dry_spell_weeks([T, T, T])), 0)
        self.assertTrue(bool(r.dry_spell_in_window([T, T, T, T])))
        self.assertFalse(bool(r.dry_spell_in_window([T, F, T, T])))

    def test_season_counts(self):
        counts = r.season_counts(np.array([0, 1, 1, 3, 2, 0, 0]))
        self.assertEqual([int(c) for c in counts], [3, 2, 1, 1])


class GridTests(unittest.TestCase):
    # The real India grid: origin of periodic/modis_india_mask.tif.
    PIXEL = 1111950.5196666666 / 2400
    GRID = Grid(Affine(PIXEL, 0, 6498424.162018612, 0, -PIXEL, 4133212.7441443047), None, 6751, 7005,
                np.zeros((1, 1), bool))

    def test_tile_window(self):
        w = tile_window(self.GRID, "h25v07")
        self.assertEqual((w.col_off, w.row_off, w.width, w.height), (2774, 4121, 2400, 2400))
        with self.assertRaises(ValueError):
            tile_window(self.GRID, "h40v01")

    def test_pixel_lonlat_round_trip(self):
        lon, lat = 76.95, 16.35  # Devadurga
        x = EARTH_RADIUS * math.radians(lon) * math.cos(math.radians(lat))
        y = EARTH_RADIUS * math.radians(lat)
        col = int((x - self.GRID.transform.c) // self.PIXEL)
        row = int((self.GRID.transform.f - y) // self.PIXEL)
        from rasterio.windows import Window

        plon, plat = pixel_lonlat(self.GRID, Window(col, row, 1, 1))
        self.assertAlmostEqual(float(plon[0, 0]), lon, delta=0.005)
        self.assertAlmostEqual(float(plat[0, 0]), lat, delta=0.005)

    def test_lookup_index(self):
        chirps = Affine(0.05, 0, 67.6, 0, -0.05, 38.0)
        row, col = lookup_index(np.array([76.96, 60.0]), np.array([16.34, 16.34]), chirps, (640, 636))
        self.assertEqual((int(row[0]), int(col[0])), (433, 187))
        self.assertEqual((int(row[1]), int(col[1])), (-1, -1))


if __name__ == "__main__":
    unittest.main()
