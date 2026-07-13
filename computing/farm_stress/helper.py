"""Temporal alignment helpers for the Farm Stress Detection System.

This module holds only logic that is genuinely new to the codebase: how the
weekly operational run resolves period/composite boundaries for indices that
update on different cadences (28-day SPI/SPEI, 8-day MAI, 16-day VCI). GEE
plumbing (asset export, GCS sync, task polling, account init) is not
reimplemented here - use utilities.gee_utils and utilities.constants.GEE_PATHS
directly, following the pattern in computing/et_downscale and computing/drought.
"""

from datetime import datetime, timedelta

from computing.farm_stress.config import EPOCH_ANCHOR


def _parse_date(value):
    if isinstance(value, datetime):
        return value
    return datetime.strptime(value, "%Y-%m-%d")


def get_completed_period(run_date, epoch_anchor=EPOCH_ANCHOR):
    """Resolve the most recently completed 28-day period for a run date.

    SPI-1/SPEI-3 must never be computed on an incomplete accumulation window -
    the partial rainfall total has a different statistical distribution from
    the fitted gamma/log-logistic. Returns the completed period along with
    the currently accumulating (incomplete) period, used for the
    partial-period departure leading indicator.
    """
    run_date = _parse_date(run_date)
    anchor = _parse_date(epoch_anchor)

    days_since_anchor = (run_date - anchor).days
    current_period_n = days_since_anchor // 28
    completed_n = current_period_n - 1

    period_start = anchor + timedelta(days=completed_n * 28)
    period_end = anchor + timedelta(days=(completed_n + 1) * 28 - 1)
    days_stale = (run_date - period_end).days  # 1 to 28

    partial_period_start = anchor + timedelta(days=current_period_n * 28)
    days_elapsed_in_period = (run_date - partial_period_start).days  # 0 to 27

    return {
        "completed_n": completed_n,
        "period_start": period_start,
        "period_end": period_end,
        "days_stale": days_stale,
        "current_period_n": current_period_n,
        "partial_period_start": partial_period_start,
        "days_elapsed_in_period": days_elapsed_in_period,
    }


def doy_to_8day_period_index(doy):
    """Map a day-of-year to the MOD16A2 8-day composite period index (0-45)."""
    return (int(doy) - 1) // 8


def doy_to_16day_period_index(doy):
    """Map a day-of-year to the MOD13A1 16-day composite period index (0-22)."""
    return (int(doy) - 1) // 16
