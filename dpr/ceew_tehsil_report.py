import json
import math
import os
import re
import unicodedata
from decimal import ROUND_HALF_UP, Decimal
from functools import lru_cache
from pathlib import Path

import pymannkendall as mk
from django.conf import settings
from scipy.stats import theilslopes

from utilities.logger import setup_logger

logger = setup_logger(__name__)

OBSERVED_START = 1981
OBSERVED_END = 2024
BASELINE_END = 2016
RECENT_START = 2017
PROJECTED_START = 2025
PROJECTED_END = 2099
NEAR_END = 2049
LATE_START = 2075
MIN_COVERAGE = 0.80


def _slug(value):
    value = unicodedata.normalize("NFKD", str(value or ""))
    value = value.encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z0-9]+", "_", value).strip("_")


def _profile_path(state, district):
    state_slug = _slug(state)
    district_slug = _slug(district)
    district_dir = Path(settings.CEEW_DISTRICT_DATA_DIR) / state_slug / district_slug
    expected = district_dir / f"{state_slug}_{district_slug}_profile.json"
    if expected.is_file():
        return expected

    candidates = sorted(district_dir.glob("*_profile.json"))
    if len(candidates) == 1:
        return candidates[0]
    return None


@lru_cache(maxsize=16)
def _load_profile_cached(file_path, modified_time):
    with open(file_path, "r", encoding="utf-8") as profile_file:
        return json.load(profile_file)


def _load_profile(state, district):
    file_path = _profile_path(state, district)
    if file_path is None:
        logger.warning(
            "No district climate profile found for state=%s district=%s in %s",
            state,
            district,
            settings.CEEW_DISTRICT_DATA_DIR,
        )
        return None

    try:
        return _load_profile_cached(str(file_path), os.path.getmtime(file_path))
    except (OSError, json.JSONDecodeError) as error:
        logger.warning(
            "Could not read district climate profile %s: %s", file_path, error
        )
        return None


def _nested_value(data, keys):
    current = data
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _series(profile, scenario, metric_path, start_year, end_year):
    values = _nested_value(
        profile,
        (
            "climate",
            "rcpssp",
            scenario,
            *metric_path,
            "annual",
            "yearly_series",
            "scenario",
        ),
    )
    if not isinstance(values, dict):
        return {}

    result = {}
    for year in range(start_year, end_year + 1):
        value = values.get(str(year))
        if (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
        ):
            result[str(year)] = float(value)
    return result


def _coverage(series, start_year, end_year):
    expected = end_year - start_year + 1
    return len(series) / expected if expected else 0


def _available(series, start_year, end_year):
    return _coverage(series, start_year, end_year) >= MIN_COVERAGE


def _period_values(series, start_year, end_year):
    return [
        series[str(year)]
        for year in range(start_year, end_year + 1)
        if str(year) in series
    ]


def _mean(series, start_year, end_year):
    values = _period_values(series, start_year, end_year)
    return sum(values) / len(values) if values else None


def _format_one_decimal(value):
    if value is None:
        return ""
    rounded = Decimal(str(value)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    return f"{rounded:.1f}"


def _trend_class(series, start_year, end_year):
    valid = [
        (year, series[str(year)])
        for year in range(start_year, end_year + 1)
        if str(year) in series
    ]
    if len(valid) < 10 or len(valid) / (end_year - start_year + 1) < MIN_COVERAGE:
        return "insufficient_data"

    years = [year for year, _ in valid]
    values = [value for _, value in valid]
    if min(values) == max(values):
        return "unchanged"

    try:
        mann_kendall = mk.trend_free_pre_whitening_modification_test(values, alpha=0.05)
        slope = theilslopes(values, years, alpha=0.95)
    except (ValueError, ZeroDivisionError):
        return "insufficient_data"

    if (
        mann_kendall.p <= 0.05
        and mann_kendall.trend == "increasing"
        and slope.low_slope > 0
    ):
        return "increasing"
    if (
        mann_kendall.p <= 0.05
        and mann_kendall.trend == "decreasing"
        and slope.high_slope < 0
    ):
        return "decreasing"
    return "no_clear_monotonic_trend"


def _direction_phrase(trend_class):
    return {
        "increasing": "shows a clear increasing trend",
        "decreasing": "shows a clear declining trend",
        "unchanged": "remains unchanged",
        "no_clear_monotonic_trend": "does not show a clear increasing or declining trend",
        "insufficient_data": "",
    }[trend_class]


def _projected_count_phrase(trend_class, series):
    values = list(series.values())
    if values and min(values) == 0 and max(values) == 0:
        return "almost no"
    return {
        "increasing": "an increasing number of",
        "decreasing": "a declining number of",
        "unchanged": "largely unchanged numbers of",
        "no_clear_monotonic_trend": "no clear increasing or declining trend in",
        "insufficient_data": "",
    }[trend_class]


def _observed_word(baseline_mean, recent_mean):
    if baseline_mean is None or recent_mean is None:
        return ""
    if recent_mean > baseline_mean:
        return "more"
    if recent_mean < baseline_mean:
        return "fewer"
    return "the same number of"


def _metric_series(profile, metric_path):
    return {
        "observed": _series(profile, "imd", metric_path, OBSERVED_START, OBSERVED_END),
        "rcp45": _series(
            profile, "RCP45", metric_path, PROJECTED_START, PROJECTED_END
        ),
        "rcp85": _series(
            profile, "RCP85", metric_path, PROJECTED_START, PROJECTED_END
        ),
    }


def _build_drought_context(profile):
    drought = _metric_series(
        profile, ("precipitation", "derived_metrics", "6_month_drought_severity")
    )
    dry_spell = _metric_series(
        profile, ("precipitation", "derived_metrics", "longest_dry_spell")
    )
    available = all(
        (
            _available(drought["observed"], OBSERVED_START, OBSERVED_END),
            _available(drought["rcp45"], PROJECTED_START, PROJECTED_END),
            _available(drought["rcp85"], PROJECTED_START, PROJECTED_END),
            _available(dry_spell["observed"], OBSERVED_START, OBSERVED_END),
            _available(dry_spell["rcp45"], PROJECTED_START, PROJECTED_END),
            _available(dry_spell["rcp85"], PROJECTED_START, PROJECTED_END),
        )
    )
    if not available:
        return {"available": False}

    drought_trend = _trend_class(drought["rcp85"], PROJECTED_START, PROJECTED_END)
    dry_spell_trend = _trend_class(
        dry_spell["rcp85"], PROJECTED_START, PROJECTED_END
    )
    if drought_trend == "decreasing" and dry_spell_trend == "decreasing":
        projected_text = (
            "This district's RCP8.5 modelling seems to predict a continuously "
            "bettering scenario, as shown by largely declining drought severity, "
            "and smaller periods without rain."
        )
    elif drought_trend == "increasing" and dry_spell_trend == "increasing":
        projected_text = (
            "This district's RCP8.5 modelling seems to predict a worsening "
            "scenario, as shown by increasing drought severity and longer periods "
            "without rain."
        )
    elif "insufficient_data" not in (drought_trend, dry_spell_trend):
        projected_text = (
            "The two modelled district drought measures do not show the same clear "
            f"direction. Drought severity {_direction_phrase(drought_trend)}, while "
            f"the longest dry spell {_direction_phrase(dry_spell_trend)}."
        )
    elif drought_trend != "insufficient_data":
        projected_text = (
            "The modelled district drought-severity series "
            f"{_direction_phrase(drought_trend)}."
        )
    elif dry_spell_trend != "insufficient_data":
        projected_text = (
            "The modelled district longest dry-spell series "
            f"{_direction_phrase(dry_spell_trend)}."
        )
    else:
        projected_text = ""

    return {
        "available": True,
        "baseline_mean": _format_one_decimal(
            _mean(drought["observed"], OBSERVED_START, BASELINE_END)
        ),
        "recent_mean": _format_one_decimal(
            _mean(drought["observed"], RECENT_START, OBSERVED_END)
        ),
        "projected_text": projected_text,
        "chart": {
            "drought": drought,
            "dry_spell": dry_spell,
        },
    }


def _build_temperature_context(profile):
    metric_paths = {
        "maximum": ("temperature", "maximum_temperature"),
        "average": ("temperature", "average_temperature"),
        "minimum": ("temperature", "minimum_temperature"),
    }
    metrics = {}
    classes = {}
    for name, path in metric_paths.items():
        values = _metric_series(profile, path)
        modelled_reference = _series(profile, "RCP85", path, RECENT_START, OBSERVED_END)
        values["modelled_reference"] = modelled_reference
        metrics[name] = values
        classes[name] = _trend_class(
            values["rcp85"], PROJECTED_START, PROJECTED_END
        )

    available = all(
        _available(metric["observed"], OBSERVED_START, OBSERVED_END)
        and _available(metric["modelled_reference"], RECENT_START, OBSERVED_END)
        and _available(metric["rcp45"], PROJECTED_START, PROJECTED_END)
        and _available(metric["rcp85"], PROJECTED_START, PROJECTED_END)
        for metric in metrics.values()
    )
    if not available:
        return {"available": False}

    if all(trend == "increasing" for trend in classes.values()):
        projected_phrase = (
            "all three annual temperature series show clear increasing trends"
        )
    elif all(trend == "decreasing" for trend in classes.values()):
        projected_phrase = (
            "all three annual temperature series show clear declining trends"
        )
    elif "insufficient_data" in classes.values():
        projected_phrase = ""
    else:
        projected_phrase = (
            "the three annual temperature series do not show one common clear direction"
        )

    context = {
        "available": True,
        "projected_phrase": projected_phrase,
        "chart": {
            **metrics,
            "y_min": math.floor(
                min(
                    [
                        *metrics["minimum"]["observed"].values(),
                        *metrics["minimum"]["rcp45"].values(),
                        *metrics["minimum"]["rcp85"].values(),
                    ]
                )
                - 1
            ),
            "y_max": math.ceil(
                max(
                    [
                        *metrics["maximum"]["observed"].values(),
                        *metrics["maximum"]["rcp45"].values(),
                        *metrics["maximum"]["rcp85"].values(),
                    ]
                )
                + 1
            ),
        },
    }
    for name, metric in metrics.items():
        observed_reference = _mean(metric["observed"], RECENT_START, OBSERVED_END)
        modelled_reference = _mean(
            metric["modelled_reference"], RECENT_START, OBSERVED_END
        )
        near_mean = _mean(metric["rcp85"], PROJECTED_START, NEAR_END)
        late_mean = _mean(metric["rcp85"], LATE_START, PROJECTED_END)
        context[name] = {
            "observed_reference": _format_one_decimal(observed_reference),
            "near_change": _format_one_decimal(near_mean - modelled_reference),
            "late_change": _format_one_decimal(late_mean - modelled_reference),
        }
    return context


def _build_unusual_temperature_context(profile):
    definitions = {
        "hot": (
            "unusually hot days",
            ("hot_weather", "unusually_hot_days"),
        ),
        "warm": (
            "unusually warm nights",
            ("hot_weather", "unusually_warm_nights"),
        ),
        "cold_day": (
            "unusually cold days",
            ("cold_weather", "unusually_cold_days"),
        ),
        "cold_night": (
            "unusually cold nights",
            ("cold_weather", "unusually_cold_nights"),
        ),
    }
    metrics = {}
    for name, (label, path) in definitions.items():
        values = _metric_series(profile, path)
        baseline_mean = _mean(values["observed"], OBSERVED_START, BASELINE_END)
        recent_mean = _mean(values["observed"], RECENT_START, OBSERVED_END)
        trend = _trend_class(values["rcp85"], PROJECTED_START, PROJECTED_END)
        near_mean = _mean(values["rcp85"], PROJECTED_START, NEAR_END)
        late_mean = _mean(values["rcp85"], LATE_START, PROJECTED_END)
        metrics[name] = {
            "label": label,
            "observed": values["observed"],
            "rcp45": values["rcp45"],
            "rcp85": values["rcp85"],
            "baseline_mean": _format_one_decimal(baseline_mean),
            "recent_mean": _format_one_decimal(recent_mean),
            "observed_word": _observed_word(baseline_mean, recent_mean),
            "projected_phrase": _projected_count_phrase(trend, values["rcp85"]),
            "trend": trend,
            "near_mean": near_mean,
            "late_mean": late_mean,
        }

    available = all(
        _available(metric["observed"], OBSERVED_START, OBSERVED_END)
        and _available(metric["rcp45"], PROJECTED_START, PROJECTED_END)
        and _available(metric["rcp85"], PROJECTED_START, PROJECTED_END)
        for metric in metrics.values()
    )
    if not available:
        return {"available": False}

    hot_trend = metrics["hot"]["trend"]
    warm_trend = metrics["warm"]["trend"]
    if "insufficient_data" in (hot_trend, warm_trend):
        overall_phrase = ""
    elif hot_trend == "increasing" and warm_trend == "increasing":
        overall_phrase = "hotter conditions"
    elif hot_trend == "decreasing" and warm_trend == "decreasing":
        overall_phrase = "fewer heat extremes"
    else:
        overall_phrase = "mixed changes in temperature extremes"

    ranked = sorted(
        metrics.values(),
        key=lambda metric: abs(metric["late_mean"] - metric["near_mean"]),
        reverse=True,
    )
    priorities = [
        {
            "label": metric["label"],
            "near_mean": _format_one_decimal(metric["near_mean"]),
            "late_mean": _format_one_decimal(metric["late_mean"]),
        }
        for metric in ranked[:2]
    ]

    return {
        "available": True,
        "overall_phrase": overall_phrase,
        "metrics": metrics,
        "priorities": priorities,
        "chart": {
            name: {
                "label": metric["label"],
                "observed": metric["observed"],
                "rcp45": metric["rcp45"],
                "rcp85": metric["rcp85"],
            }
            for name, metric in metrics.items()
        },
    }


def _build_rainfall_context(profile):
    values = _metric_series(
        profile, ("precipitation", "time_series_metrics", "total_rainfall")
    )
    available = _available(
        values["observed"], OBSERVED_START, OBSERVED_END
    ) and _available(
        values["rcp45"], PROJECTED_START, PROJECTED_END
    ) and _available(values["rcp85"], PROJECTED_START, PROJECTED_END)
    if not available:
        return {"available": False}

    return {
        "available": True,
        "baseline_mean": _format_one_decimal(
            _mean(values["observed"], OBSERVED_START, BASELINE_END)
        ),
        "recent_mean": _format_one_decimal(
            _mean(values["observed"], RECENT_START, OBSERVED_END)
        ),
        "near_mean": _format_one_decimal(
            _mean(values["rcp85"], PROJECTED_START, NEAR_END)
        ),
        "late_mean": _format_one_decimal(
            _mean(values["rcp85"], LATE_START, PROJECTED_END)
        ),
        "projected_phrase": _direction_phrase(
            _trend_class(values["rcp85"], PROJECTED_START, PROJECTED_END)
        ),
        "chart": values,
    }


def _build_unusual_rainfall_context(profile):
    values = _metric_series(
        profile,
        ("precipitation", "derived_metrics", "unusually_heavy_rainy_days"),
    )
    available = _available(
        values["observed"], OBSERVED_START, OBSERVED_END
    ) and _available(
        values["rcp45"], PROJECTED_START, PROJECTED_END
    ) and _available(values["rcp85"], PROJECTED_START, PROJECTED_END)
    if not available:
        return {"available": False}

    baseline_mean = _mean(values["observed"], OBSERVED_START, BASELINE_END)
    recent_mean = _mean(values["observed"], RECENT_START, OBSERVED_END)
    trend = _trend_class(values["rcp85"], PROJECTED_START, PROJECTED_END)
    return {
        "available": True,
        "baseline_mean": _format_one_decimal(baseline_mean),
        "recent_mean": _format_one_decimal(recent_mean),
        "near_mean": _format_one_decimal(
            _mean(values["rcp85"], PROJECTED_START, NEAR_END)
        ),
        "late_mean": _format_one_decimal(
            _mean(values["rcp85"], LATE_START, PROJECTED_END)
        ),
        "observed_word": _observed_word(baseline_mean, recent_mean),
        "projected_phrase": _projected_count_phrase(trend, values["rcp85"]),
        "chart": values,
    }


def get_ceew_tehsil_context(state, district):
    """Build district-scale climate context for a tehsil report."""
    profile = _load_profile(state, district)
    if profile is None:
        return {"available": False}

    district_name = _nested_value(profile, ("location", "district_name")) or district
    drought = _build_drought_context(profile)
    temperature = _build_temperature_context(profile)
    unusual_temperature = _build_unusual_temperature_context(profile)
    rainfall = _build_rainfall_context(profile)
    unusual_rainfall = _build_unusual_rainfall_context(profile)
    climate_available = any(
        section.get("available", False)
        for section in (
            temperature,
            unusual_temperature,
            rainfall,
            unusual_rainfall,
        )
    )
    available = drought.get("available", False) or climate_available

    return {
        "available": available,
        "climate_available": climate_available,
        "district_name": district_name,
        "drought": drought,
        "temperature": temperature,
        "unusual_temperature": unusual_temperature,
        "rainfall": rainfall,
        "unusual_rainfall": unusual_rainfall,
        "charts": {
            "drought": drought.get("chart"),
            "temperature": temperature.get("chart"),
            "unusual_temperature": unusual_temperature.get("chart"),
            "rainfall": rainfall.get("chart"),
            "unusual_rainfall": unusual_rainfall.get("chart"),
        },
    }
