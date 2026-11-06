"""Tests for the MAI stage (no data files needed).

Run from the repo root:
    python -m unittest computing.drought.local.test_moisture -v
"""

import unittest

import numpy as np

from computing.drought.local import moisture as mo


class MaiTests(unittest.TestCase):
    def test_method_note_example(self):
        """ET 16, 24, 28, 30, 32 and PET 40, 40, 44, 44, 48 mm with weights 2/8, 1, 1, 1, 2/8 give MAI 63."""
        et = [np.array([v * 10]) for v in (16, 24, 28, 30, 32)]  # stored in 0.1 mm units
        pet = [np.array([v * 10]) for v in (40, 40, 44, 44, 48)]
        got = mo.window_mai(et, pet, [0.25, 1, 1, 1, 0.25])
        self.assertAlmostEqual(float(got[0]), 94 / 150 * 100)

    def test_codes_and_missing_files_give_no_mai(self):
        et = [np.array([100, 32766, 100]), np.array([100, 100, 100])]
        pet = [np.array([200, 200, 0]), np.array([200, 200, 0])]
        got = mo.window_mai(et, pet, [1, 1])
        self.assertAlmostEqual(float(got[0]), 50.0)
        self.assertTrue(np.isnan(got[1]))  # water/urban/barren code
        self.assertTrue(np.isnan(got[2]))  # PET 0
        self.assertTrue(np.isnan(mo.window_mai([np.array([100]), None], [np.array([200]), None], [1, 1])[0]))

    def test_window_sums_are_kept(self):
        """The MWS stage divides sums of these, so they carry the same weights and gaps as MAI."""
        et = [np.array([100, 32766]), np.array([300, 100])]
        pet = [np.array([400, 400]), np.array([400, 400])]
        et_sum, pet_sum = mo.window_sums(et, pet, [1, 0.5])
        self.assertAlmostEqual(float(et_sum[0]), 10 + 15)
        self.assertAlmostEqual(float(pet_sum[0]), 40 + 20)
        self.assertTrue(np.isnan(et_sum[1]) and np.isnan(pet_sum[1]))


if __name__ == "__main__":
    unittest.main()
