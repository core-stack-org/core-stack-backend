"""Tests for the vegetation stage (no data files needed).

Run from the repo root:
    python -m unittest computing.drought.local.test_vegetation -v
"""

import unittest

import numpy as np

from computing.drought.local import vegetation as vg


class CloudTests(unittest.TestCase):
    def test_clear_mask(self):
        states = {
            0: True,        # clear
            1: False,       # cloudy
            2: False,       # mixed
            3: True,        # not set, assumed clear
            4: False,       # cloud shadow
            1024: False,    # internal cloud flag
            8: True,        # land (land/water bits only)
            256: True,      # small cirrus is not masked
            8192: True,     # next to a cloud is not masked
            65535: False,   # no data
        }
        got = vg.clear_mask(np.array(list(states)))
        self.assertEqual(got.tolist(), list(states.values()))


class IndexTests(unittest.TestCase):
    def test_normalized_difference(self):
        got = vg.normalized_difference(np.array([3000, 3000, -28672, 0]), np.array([1000, -28672, 1000, 0]))
        self.assertAlmostEqual(float(got[0]), 0.5)
        self.assertTrue(np.isnan(got[1:]).all())

    def test_window_max_prefers_clear_and_falls_back(self):
        values = [np.array([0.6, 0.3, np.nan]), np.array([0.4, 0.7, np.nan]), np.array([0.5, np.nan, np.nan])]
        clear = [np.array([False, False, True]), np.array([True, False, True]), np.array([True, False, True])]
        best, fallback = vg.window_max(values, clear)
        self.assertAlmostEqual(float(best[0]), 0.5)  # highest clear, not the cloudy 0.6
        self.assertAlmostEqual(float(best[1]), 0.7)  # nothing clear: highest of all
        self.assertTrue(np.isnan(best[2]))           # no value at all
        self.assertEqual(fallback.tolist(), [False, True, False])

    def test_vci(self):
        history = np.array([[0.2, 0.3, 0.3], [0.6, 0.3, 0.6], [0.4, 0.3, 0.1]])
        got = vg.vci(np.array([0.4, 0.3, 0.1]), history)
        self.assertAlmostEqual(float(got[0]), 50.0, places=4)
        self.assertTrue(np.isnan(got[1]))  # no range in the history
        self.assertAlmostEqual(float(got[2]), 0.0)


if __name__ == "__main__":
    unittest.main()
