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
        self.assertEqual(cols["monsoon_onset_2023"], ["2023-6-1"])
        self.assertEqual(cols["total_weeks_2023"], [43 - 22 + 1])

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
        labels = json.loads(cols["drought_labels_2023"][0])
        self.assertEqual(len(labels), cols["total_weeks_2023"][0])
        self.assertEqual(labels.count(r.SEVERE), 8)  # windows 27-31 cover weeks 27-34
        self.assertEqual(cols["number_of_weeks_in_severe_drought_2023"], [labels.count(r.SEVERE)])
        self.assertEqual(cols["number_of_weeks_in_no_drought_2023"], [labels.count(r.NONE)])
        self.assertEqual(cols["dryspell_length_2023"], [6])
        self.assertEqual(cols["total_dryspell_weeks_2023"], [6])
        self.assertEqual(cols["freq_of_drought_2023_at_threshold_3"], [labels.count(r.SEVERE)])
        self.assertEqual(cols["freq_of_drought_2023_at_threshold_0"], [len(labels)])
        self.assertAlmostEqual(cols["intensity_of_drought_2023_at_threshold_0"][0], round(sum(labels) / len(labels), 3))
        self.assertEqual(cols["intensity_of_drought_2023_at_threshold_3"], [3.0])
        self.assertEqual((cols["kharif_cropped_sqkm_2023"], cols["percent_of_area_cropped_kharif_2023"]), ([2.5], [70.0]))

    def test_season_columns_have_the_gee_yearly_asset_names(self):
        cols = self.columns(mws.mws_rain(SEASON, **rain_inputs()))
        self.assertEqual(sorted(cols), sorted(
            ["drought_labels_2023", "dryspell_length_2023", "total_dryspell_weeks_2023"]
            + [f"freq_of_drought_2023_at_threshold_{k}" for k in range(4)]
            + [f"intensity_of_drought_2023_at_threshold_{k}" for k in range(4)]
            + [f"number_of_weeks_in_{c}_drought_2023" for c in ("no", "mild", "moderate", "severe")]
            + ["kharif_cropped_sqkm_2023", "monsoon_onset_2023", "percent_of_area_cropped_kharif_2023", "total_weeks_2023"]))

    def test_no_cropland_keeps_rain_columns_only(self):
        rain = mws.mws_rain(SEASON, **rain_inputs(dry_weeks=range(28, 34)))
        cols = self.columns(rain, crop=False)
        for name in ("drought_labels_2023", "number_of_weeks_in_severe_drought_2023",
                     "freq_of_drought_2023_at_threshold_0", "intensity_of_drought_2023_at_threshold_0"):
            self.assertEqual(cols[name], [None], name)
        self.assertEqual((cols["dryspell_length_2023"], cols["total_weeks_2023"], cols["monsoon_onset_2023"]),
                         ([6], [22], ["2023-6-1"]))

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


class WindowColumnTests(unittest.TestCase):
    """The per-window values for drought causality, named as in the GEE yearly drought asset."""

    def columns(self, rain, crop=True, vci=50.0, mai=60.0):
        labels, _ = classes(rain, vci=vci, mai=mai, crop=crop)
        n = len(rain["onset_week"])
        full = lambda v: np.full((N, n), v if crop else np.nan)
        return {name: values for name, _, values in
                mws.window_columns(SEASON, rain, labels, full(vci), full(30.0), full(50.0), full(mai))}

    def test_names_follow_the_gee_yearly_asset(self):
        cols = self.columns(mws.mws_rain(SEASON, **rain_inputs()))
        self.assertEqual(len(cols), len(mws.WINDOW_COLUMNS) * 20)  # 10 values x 20 windows
        self.assertEqual([c for c in cols if c.startswith("vci_")][:2], ["vci_2023-5-21", "vci_2023-5-28"])
        for name in ("weekly_label_2023-5-21", "rfdev_class_2023-6-4", "meteorological_drought_2023-10-1",
                     "et_2023-7-9", "pet_2023-7-9", "mai_2023-7-9", "dryspell_2023-8-6", "spi_2023-9-3",
                     "monthly_rainfall_deviation_2023-5-21", "monthly_rainfall_deviation_2023-10-1"):
            self.assertIn(name, cols)

    def test_windows_that_can_never_be_counted_have_no_columns(self):
        # onset is never before 22 May (the Kerala date), week 21: the windows of 7 and 14 May never count
        self.assertEqual(mws.EARLIEST_ONSET_WEEK, 21)
        self.assertEqual(mws.LAYER_WINDOWS, list(range(21, 41)))
        self.assertEqual([mws.window_date(year, 21) for year in (2023, 2024)], ["2023-5-21", "2024-5-21"])
        cols = self.columns(mws.mws_rain(SEASON, **rain_inputs()))
        self.assertFalse([c for c in cols if c.endswith(("_2023-5-7", "_2023-5-14"))])

    def test_onset_before_the_first_window_is_an_error(self):
        rain = mws.mws_rain(SEASON, **rain_inputs(onset_day=130))  # 10 May 2023: week 19
        with self.assertRaisesRegex(ValueError, "onset before week 21"):
            self.columns(rain)

    def test_geoserver_carries_the_season_columns_and_rainfall_deviation(self):
        rain = mws.mws_rain(SEASON, **rain_inputs())
        labels, summary = classes(rain)
        season = [name for name, _, _ in mws.layer_columns(SEASON, rain, labels, summary, np.ones(1), np.ones(1))]
        windows = list(self.columns(rain))
        self.assertEqual(len(season), 19)
        self.assertTrue(all(mws.geoserver_column(c) for c in season))
        self.assertEqual([c for c in windows if mws.geoserver_column(c)],
                         [c for c in windows if c.startswith("monthly_rainfall_deviation_")])
        self.assertEqual(sum(mws.geoserver_column(c) for c in windows), len(mws.LAYER_WINDOWS))
        self.assertFalse(any(mws.geoserver_column(c) for c in mws.ATTRIBUTES))
        self.assertFalse(any(mws.geoserver_column(c) for c in ("rd23-5-7", "drlb_2023", "severe_moderate_drought_causality_2023")))

    def test_rainfall_deviation_is_empty_before_onset(self):
        rain = mws.mws_rain(SEASON, **rain_inputs())  # onset 1 June, week 22 (28 May)
        cols = self.columns(rain)
        self.assertEqual(cols["monthly_rainfall_deviation_2023-5-21"], [None])
        self.assertEqual(cols["monthly_rainfall_deviation_2023-5-28"], [0.0])

    def test_windows_before_onset_are_empty(self):
        rain = mws.mws_rain(SEASON, **rain_inputs(onset_day=180))  # onset week 26, which starts 25 June
        cols = self.columns(rain)
        for name in mws.WINDOW_COLUMNS:
            self.assertEqual(cols[f"{name}_2023-6-18"], [None], name)  # week 25
            self.assertIsNotNone(cols[f"{name}_2023-6-25"][0], name)  # week 26

    def test_dry_spell_windows(self):
        rain = mws.mws_rain(SEASON, **rain_inputs(dry_weeks=range(28, 34)))  # onset week 22
        cols = self.columns(rain)
        # week 28 (9 July): dry spell; week 27 (2 July): 3 of 4 weeks dry, 75% below normal; week 22: normal
        self.assertEqual([cols[f"{v}_2023-7-9"][0] for v in ("dryspell", "meteorological_drought", "rfdev_class")],
                         [1, 1, "Scanty rf"])
        self.assertEqual([cols[f"{v}_2023-7-2"][0] for v in ("dryspell", "meteorological_drought", "rfdev_class")],
                         [0, 1, "Scanty rf"])
        self.assertEqual([cols[f"{v}_2023-5-28"][0] for v in ("dryspell", "meteorological_drought", "rfdev_class")],
                         [0, 0, "Normal rf"])
        drlb = json.loads(dict((n, v) for n, _, v in mws.layer_columns(
            SEASON, rain, *classes(rain, vci=50.0, mai=60.0), np.ones(1), np.ones(1)))["drought_labels_2023"][0])
        self.assertEqual(cols["weekly_label_2023-7-9"], [drlb[28 - 22]])
        self.assertEqual((cols["vci_2023-7-9"], cols["et_2023-7-9"], cols["pet_2023-7-9"], cols["mai_2023-7-9"]),
                         ([50.0], [30.0], [50.0], [60.0]))

    def test_no_cropland_keeps_the_rain_values(self):
        cols = self.columns(mws.mws_rain(SEASON, **rain_inputs()), crop=False)
        for name in ("weekly_label", "vci", "et", "pet", "mai"):
            self.assertEqual(cols[f"{name}_2023-7-9"], [None], name)
        self.assertEqual([cols[f"{v}_2023-7-9"][0] for v in ("dryspell", "meteorological_drought", "rfdev_class", "spi")],
                         [0, 0, "Normal rf", 0.0])

    def test_values_next_to_a_threshold_are_stored_as_computed(self):
        # rounded to 2 decimals these would read -1.50, 40.00, 50.00, -60.00 and 75.00: on the
        # threshold, contradicting the trigger and classes worked out from the unrounded values
        inputs = rain_inputs()
        w29 = mws.WINDOW_STARTS.index(29)  # the window of 16 July
        inputs["spi"][w29] = -1.5004
        inputs["weekly"][28:32] = 7.9992  # weeks 29-32: 31.9968 mm against a normal of 80, -60.004%
        rain = mws.mws_rain(SEASON, **inputs)
        cols = self.columns(rain, vci=39.9996, mai=50.0004)
        self.assertEqual(cols["spi_2023-7-16"], [-1.5004])
        deviation = cols["monthly_rainfall_deviation_2023-7-16"][0]
        self.assertEqual(deviation, rain["deviation"][w29, 0])
        self.assertLess(deviation, -60)
        self.assertEqual((cols["rfdev_class_2023-7-16"], cols["meteorological_drought_2023-7-16"]), (["Scanty rf"], [1]))
        self.assertEqual((cols["vci_2023-7-16"], cols["mai_2023-7-16"]), ([39.9996], [50.0004]))
        self.assertEqual((cols["et_2023-7-16"], cols["pet_2023-7-16"]), ([30.0], [50.0]))  # not compared: 2 decimals
        season = {name: values for name, _, values in
                  mws.layer_columns(SEASON, rain, *classes(rain), np.ones(1), np.array([74.9996]))}
        self.assertEqual(season["percent_of_area_cropped_kharif_2023"], [74.9996])

    def test_rainfall_class_thresholds(self):
        """Scanty rf is the 2020 trigger, -60% or worse, so the class always agrees with the trigger."""
        got = mws.rfdev_class(np.array([25.0, -19.0, -19.5, -59.0, -59.5, -60.0, -100.0, np.nan]))
        self.assertEqual(got.tolist(), ["Normal rf", "Normal rf", "Deficit rf", "Deficit rf", "Deficit rf",
                                        "Scanty rf", "Scanty rf", None])


def causality(rain, vci, mai, sown=80.0, crop=True):
    """The two causality columns for one MWS; vci and mai per window (a number or 22 values)."""
    vci = np.broadcast_to(np.asarray(vci, dtype=float), (N,))[:, None]
    mai = np.broadcast_to(np.asarray(mai, dtype=float), (N,))[:, None]
    sown = np.array([sown])
    _, summary = mws.mws_classes(rain, vci, mai, sown, np.array([crop]))
    cols = {name: values[0] for name, _, values in mws.causality_columns(SEASON, rain, vci, mai, sown, summary)}
    parse = lambda v: None if v is None else json.loads(v)
    return parse(cols["severe_moderate_drought_causality_2023"]), parse(cols["mild_drought_causality_2023"])


def per_window(default, **weeks):
    """22 window values: default, with {"w28": value} for the window starting in that week."""
    values = np.full(N, float(default))
    for key, v in weeks.items():
        values[mws.WINDOW_STARTS.index(int(key[1:]))] = v
    return values


class CausalityTests(unittest.TestCase):
    """Drought causality: the windows the 2020 rule classes moderate/severe (or mild), counted by
    class, triggers that fired and each impact's class; only in a drought year."""

    def setUp(self):
        # weeks 28-33 dry: windows 28-30 are a dry spell and 100% below normal, 27 and 31 75% below
        self.rain = mws.mws_rain(SEASON, **rain_inputs(dry_weeks=range(28, 34)))

    def test_counts_combinations_of_the_severe_windows(self):
        # VCI 30 severe, MAI 40 severe, area sown 80% moderate: windows 27-31 severe, weeks 27-34
        severe_moderate, mild = causality(self.rain, vci=30, mai=40)
        self.assertEqual(severe_moderate, {
            "severe: dry spell + scanty rain; VCI severe, MAI severe, area sown moderate": 3,
            "severe: scanty rain; VCI severe, MAI severe, area sown moderate": 2})
        self.assertEqual(mild, {})

    def test_mild_windows_in_a_drought_year(self):
        # windows 28-30 severe (weeks 28-33: 6 severe weeks); 27 and 31 have only area sown moderate: mild
        vci = per_window(70, w28=30, w29=30, w30=30)
        mai = per_window(80, w28=40, w29=40, w30=40)
        severe_moderate, mild = causality(self.rain, vci=vci, mai=mai)
        self.assertEqual(severe_moderate, {"severe: dry spell + scanty rain; VCI severe, MAI severe, area sown moderate": 3})
        self.assertEqual(mild, {"mild: scanty rain; VCI normal, MAI normal, area sown moderate": 2})

    def test_moderate_window_lists_every_impact(self):
        # VCI 50 moderate, MAI 60 moderate, area sown 100% normal: moderate (2 of 3 at least moderate)
        severe_moderate, _ = causality(self.rain, vci=50, mai=60, sown=100)
        self.assertEqual(severe_moderate, {
            "moderate: dry spell + scanty rain; VCI moderate, MAI moderate, area sown normal": 3,
            "moderate: scanty rain; VCI moderate, MAI moderate, area sown normal": 2})

    def test_spi_is_listed_with_the_other_triggers(self):
        inputs = rain_inputs(dry_weeks=range(28, 34))
        inputs["spi"][mws.WINDOW_STARTS.index(29)] = -2.0
        severe_moderate, _ = causality(mws.mws_rain(SEASON, **inputs), vci=30, mai=40)
        self.assertEqual(severe_moderate, {
            "severe: dry spell + scanty rain; VCI severe, MAI severe, area sown moderate": 2,
            "severe: scanty rain; VCI severe, MAI severe, area sown moderate": 2,
            "severe: dry spell + scanty rain + SPI; VCI severe, MAI severe, area sown moderate": 1})

    def test_not_a_drought_year(self):
        # all five triggered windows mild: no moderate or severe week, so no causality at all
        self.assertEqual(causality(self.rain, vci=70, mai=80, sown=100), (None, None))

    def test_five_drought_weeks_are_not_a_drought_year(self):
        # only window 28 severe (weeks 28-31) and 29 (29-32): 5 severe weeks; the rest mild
        vci = per_window(70, w28=30, w29=30)
        mai = per_window(80, w28=40, w29=40)
        self.assertEqual(causality(self.rain, vci=vci, mai=mai), (None, None))
        vci = per_window(70, w28=30, w29=30, w30=30)  # 6 severe weeks
        mai = per_window(80, w28=40, w29=40, w30=40)
        self.assertIsNotNone(causality(self.rain, vci=vci, mai=mai)[0])

    def test_no_cropland_no_causality(self):
        self.assertEqual(causality(self.rain, vci=30, mai=40, crop=False), (None, None))

    def test_windows_before_onset_do_not_count(self):
        # dry weeks 20-25 before an onset in week 26: no window counts, so no drought weeks
        rain = mws.mws_rain(SEASON, **rain_inputs(dry_weeks=range(20, 26), onset_day=180))
        self.assertEqual(causality(rain, vci=30, mai=40), (None, None))

    def test_top_three_and_ties(self):
        code = lambda cls, bits, v, m, s: (((cls * 8 + bits) * 3 + v) * 3 + m) * 3 + s
        S, M, N_ = r.IMPACT_SEVERE, r.IMPACT_MODERATE, r.NORMAL
        entries = [(code(r.MODERATE, 2, M, M, N_), 2), (code(r.SEVERE, 2, S, S, M), 2),
                   (code(r.MODERATE, 4, M, M, N_), 2), (code(r.MODERATE, 6, S, M, N_), 1)]
        self.assertEqual(list(mws._top(entries)), [
            "severe: scanty rain; VCI severe, MAI severe, area sown moderate",  # same count: worse class first
            "moderate: SPI; VCI moderate, MAI moderate, area sown normal",  # then by text
            "moderate: scanty rain; VCI moderate, MAI moderate, area sown normal"])

    def test_same_classes_as_the_drought_layer(self):
        """Every window causality counts is one the drought layer classes the same way."""
        from computing.drought.local import classify as cf

        vci, mai = per_window(70, w28=30, w29=30, w30=30), per_window(80, w28=40, w29=40, w30=40)
        classes = cf.window_classes(self.rain["onset_week"], self.rain["triggers"], vci[:, None], mai[:, None],
                                    np.array([80.0]))[:, 0]
        severe_moderate, mild = causality(self.rain, vci=vci, mai=mai)
        self.assertEqual(sum(severe_moderate.values()), int((classes >= r.MODERATE).sum()))
        self.assertEqual(sum(mild.values()), int((classes == r.MILD).sum()))

    def test_causality_columns(self):
        self.assertTrue(mws.causality_column("severe_moderate_drought_causality_2023"))
        self.assertTrue(mws.causality_column("mild_drought_causality_2023"))
        self.assertFalse(mws.causality_column("se_mo_2023"))
        self.assertFalse(mws.geoserver_column("mild_drought_causality_2023"))


if __name__ == "__main__":
    unittest.main()
