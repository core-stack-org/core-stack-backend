"""Tests for the MWS stage's aggregation and columns (no data files needed).

Run from the repo root:
    python -m unittest computing.drought.local.test_mws -v
"""

import datetime as dt
import json
import unittest

import numpy as np
from rasterio.transform import Affine
from scipy import sparse
from shapely.geometry import box

from computing.drought.local import mws
from computing.drought.local import rules as r

N = len(mws.WINDOW_STARTS)  # 22 windows, weeks 19-40
SEASON = 2023


def rain_inputs(n=1, dry_weeks=(), onset_day=152):
    """Weekly rain of 20 mm against a normal of 20 mm, 0 mm in dry_weeks; onset on day onset_day (1 June)."""
    weekly = np.full((52, n), 20.0)
    weekly[[w - 1 for w in dry_weeks]] = 0
    return dict(onset_day=np.full(n, float(onset_day)), weekly=weekly, normal=np.full((52, n), 20.0),
                spi=np.zeros((N, n)))


def classes(rain, vci=10.0, mai=10.0, sown=10.0, crop=True):
    n = len(rain["onset_week"])
    return mws.mws_classes(rain, np.full((N, n), vci), np.full((N, n), mai), np.full(n, sown), np.full(n, crop))


class AggregationTests(unittest.TestCase):
    def test_group_mean_skips_missing(self):
        got = mws.group_mean(np.array([0, 0, 1, 1]), np.array([10.0, np.nan, np.nan, np.nan]), 3)
        self.assertEqual(got[0], 10)
        self.assertTrue(np.isnan(got[1:]).all())

    def test_mai_is_a_ratio_of_sums(self):
        """ET 10 and 30 mm, PET 100 and 50 mm: 40 / 150, not the mean of 10% and 60%."""
        got = mws.group_ratio(np.array([0, 0]), np.array([10.0, 30.0]), np.array([100.0, 50.0]), 1)
        self.assertAlmostEqual(got[0] * 100, 40 / 150 * 100)

    def test_weighted_median(self):
        groups = np.array([0, 0, 0, 1, 1, 2])
        values = np.array([160.0, 150.0, 170.0, 150.0, 170.0, np.nan])
        weights = np.array([1, 1, 1, 1, 3, 1])
        got = mws.weighted_median(groups, values, weights, 4)
        self.assertEqual(got[0], 160)  # plain median
        self.assertEqual(got[1], 170)  # most pixels lie in the cell with day 170
        self.assertTrue(np.isnan(got[2:]).all())  # no onset / no pixels

    def test_weighted_median_even_count_takes_the_lower_day(self):
        got = mws.weighted_median(np.array([0, 0]), np.array([170.0, 150.0]), np.array([1, 1]), 1)
        self.assertEqual(got[0], 150)

    def test_cell_mean_counts_pixels_per_cell(self):
        """MWS 0 has 3 pixels in cell 0 (10 mm) and 1 in cell 1 (30 mm); MWS 1 lies only in a sea cell."""
        weights = sparse.csr_matrix((np.ones(5), ([0, 0, 0, 0, 1], [0, 0, 0, 1, 2])), shape=(2, 3))
        values = np.array([[[10.0, 30.0, np.nan]]])  # (bands, rows, cols)
        got = mws.cell_mean(weights, values)
        self.assertEqual(got.shape, (1, 2))
        self.assertAlmostEqual(got[0, 0], 15)
        self.assertTrue(np.isnan(got[0, 1]))


class PixelTests(unittest.TestCase):
    def test_pixel_centres_and_touched_pixels(self):
        """A 4 x 4 grid of 1 m pixels: a 2 x 2 MWS, and an MWS too small to hold a pixel centre."""
        transform = Affine(1, 0, 0, 0, -1, 4)
        big = box(0, 2, 2, 4)  # pixels (0, 0), (0, 1), (1, 0), (1, 1)
        tiny = box(2.6, 0.6, 2.9, 0.9)  # inside pixel (3, 2), away from its centre
        outside = box(10, 10, 11, 11)
        groups, pixels = mws.mws_pixels(np.array([big, tiny, outside], dtype=object), transform, (4, 4))
        got = {g: sorted(pixels[groups == g].tolist()) for g in range(3)}
        self.assertEqual(got, {0: [0, 1, 4, 5], 1: [14], 2: []})


class RainTests(unittest.TestCase):
    def test_onset_week_and_date(self):
        rain = mws.mws_rain(SEASON, **rain_inputs(onset_day=152))  # 1 June 2023: week 22 (28 May - 3 June)
        self.assertEqual(rain["onset_week"][0], 22)
        cols = dict((name, values) for name, _, values in
                    mws.layer_columns(SEASON, rain, *classes(rain), np.array([1.5]), np.array([90.0])))
        self.assertEqual(cols["m_ons_2023"], ["2023-6-1"])
        self.assertEqual(cols["t_wks_2023"], [43 - 22 + 1])

    def test_dry_spell_from_onset(self):
        rain = mws.mws_rain(SEASON, **rain_inputs(dry_weeks=range(28, 34)))  # weeks 28-33 dry
        self.assertEqual((rain["dry_spell_weeks"][0], rain["longest_dry_spell"][0]), (6, 6))
        triggered = [w for w, t in zip(mws.WINDOW_STARTS, rain["triggers"][:, 0]) if t]
        # 28-30: dry spell; 27 and 31: 3 of 4 weeks dry, 75% below normal (scanty)
        self.assertEqual(triggered, [27, 28, 29, 30, 31])
        self.assertEqual(rain["deviation"][mws.WINDOW_STARTS.index(28), 0], -100)
        self.assertEqual(rain["deviation"][mws.WINDOW_STARTS.index(27), 0], -75)

    def test_dry_weeks_before_onset_are_not_a_dry_spell(self):
        rain = mws.mws_rain(SEASON, **rain_inputs(dry_weeks=range(19, 25), onset_day=180))  # onset week 26
        self.assertEqual(rain["dry_spell_weeks"][0], 0)

    def test_no_rainfall_data(self):
        inputs = rain_inputs()
        inputs["onset_day"][:] = np.nan
        rain = mws.mws_rain(SEASON, **inputs)
        self.assertTrue(np.isnan(rain["onset_week"][0]))
        labels, summary = classes(rain)
        self.assertTrue((labels == mws.NODATA).all())


class ColumnTests(unittest.TestCase):
    def columns(self, rain, crop=True, **indicators):
        labels, summary = classes(rain, crop=crop, **indicators)
        n = len(rain["onset_week"])
        return {name: values for name, _, values in
                mws.layer_columns(SEASON, rain, labels, summary, np.full(n, 2.5), np.full(n, 70.0))}

    def test_severe_dry_spell(self):
        rain = mws.mws_rain(SEASON, **rain_inputs(dry_weeks=range(28, 34)))
        cols = self.columns(rain)
        labels = json.loads(cols["drlb_2023"][0])
        self.assertEqual(len(labels), cols["t_wks_2023"][0])
        self.assertEqual(labels.count(r.SEVERE), 8)  # windows 27-31 cover weeks 27-34
        self.assertEqual(cols["w_sev_2023"], [labels.count(r.SEVERE)])
        self.assertEqual(cols["w_no_2023"], [labels.count(r.NONE)])
        self.assertEqual(cols["drysp_2023"], [6])
        self.assertEqual(cols["dsw_2023"], [6])
        self.assertEqual(cols["frth3_2023"], [labels.count(r.SEVERE)])
        self.assertEqual(cols["frth0_2023"], [len(labels)])
        self.assertAlmostEqual(cols["inth0_2023"][0], round(sum(labels) / len(labels), 3))
        self.assertEqual(cols["inth3_2023"], [3.0])
        self.assertEqual((cols["kh_cr_2023"], cols["pcr_k_2023"]), ([2.5], [70.0]))

    def test_no_cropland_keeps_rain_columns_only(self):
        rain = mws.mws_rain(SEASON, **rain_inputs(dry_weeks=range(28, 34)))
        cols = self.columns(rain, crop=False)
        for name in ("drlb_2023", "w_sev_2023", "frth0_2023", "inth0_2023"):
            self.assertEqual(cols[name], [None], name)
        self.assertEqual((cols["drysp_2023"], cols["t_wks_2023"], cols["m_ons_2023"]), ([6], [22], ["2023-6-1"]))

    def test_rainfall_deviation_columns_are_named_by_window_start(self):
        rain = mws.mws_rain(SEASON, **rain_inputs())
        cols = self.columns(rain)
        rd = [name for name in cols if name.startswith("rd")]
        self.assertEqual(len(rd), N)
        start = dt.date(2023, 5, 7)  # week 19
        self.assertEqual(rd[0], f"rd23-{start.month}-{start.day}")
        self.assertEqual(cols[rd[0]], [0.0])

    def test_same_classes_as_a_pixel_with_the_same_values(self):
        """An MWS whose pixels all have the same values gets that pixel's classes."""
        from computing.drought.local import classify as cf

        rain = mws.mws_rain(SEASON, **rain_inputs(dry_weeks=range(28, 34)))
        labels, summary = classes(rain, vci=50.0, mai=60.0, sown=100.0)
        one = lambda v: np.full((1, 1), v, dtype=float)
        pixel_labels, pixel_summary = cf.classify_arrays(
            onset=one(rain["onset_week"][0]), triggers=rain["triggers"][:, :, None].astype(float),
            vci=np.full((N, 1, 1), 50.0), mai=np.full((N, 1, 1), 60.0), sown=one(100), crop=np.full((1, 1), True),
            fallback=np.zeros((N, 1, 1)), dry_total=one(6), dry_longest=one(6))
        np.testing.assert_array_equal(labels[:, 0], pixel_labels[:, 0, 0])
        np.testing.assert_array_equal(summary[:9, 0], pixel_summary[:9, 0, 0])


if __name__ == "__main__":
    unittest.main()
