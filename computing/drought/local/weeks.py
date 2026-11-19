"""Standard meteorological weeks, 4-week windows, and how MODIS 8-day composites fit them.

Standard meteorological weeks (IMD): 52 a year, week 1 = 1-7 January. Week 9
(26 February - 4 March) takes 29 February in leap years and week 52 (24-31
December) has 8 days, so from week 10 on a week has the same dates every year.
"""

import datetime as dt

from computing.drought.local.config import (
    ONSET_FIRST_WEEK,
    SEASON_LAST_WEEK,
    VCI_HISTORY_START,
    WINDOW_WEEKS,
)

WEEKS_PER_YEAR = 52
COMPOSITE_DAYS = 8
VCI_MIN_DAYS_INSIDE = 4  # a composite counts for VCI when at least 4 of its 8 days are in the window


def _check_week(week):
    if not 1 <= week <= WEEKS_PER_YEAR:
        raise ValueError(f"week must be 1-{WEEKS_PER_YEAR}, got {week}")


def week_start(year, week):
    _check_week(week)
    if week <= 9:
        return dt.date(year, 1, 1) + dt.timedelta(days=7 * (week - 1))
    return dt.date(year, 3, 5) + dt.timedelta(days=7 * (week - 10))


def week_end(year, week):
    _check_week(week)
    if week == WEEKS_PER_YEAR:
        return dt.date(year, 12, 31)
    return week_start(year, week + 1) - dt.timedelta(days=1)


def week_of(day):
    if day < dt.date(day.year, 3, 5):
        return min((day - dt.date(day.year, 1, 1)).days // 7 + 1, 9)
    return min((day - dt.date(day.year, 3, 5)).days // 7 + 10, WEEKS_PER_YEAR)


def window_dates(year, start_week):
    """First and last day of the 4-week window that starts in start_week."""
    return week_start(year, start_week), week_end(year, start_week + WINDOW_WEEKS - 1)


def season_window_starts(onset_week):
    """Start weeks of the season's windows: from onset, ending by SEASON_LAST_WEEK."""
    return range(max(onset_week, ONSET_FIRST_WEEK), SEASON_LAST_WEEK - WINDOW_WEEKS + 2)


def composite_starts(year):
    """MODIS 8-day composites start on day 1, 9, 17, ... 361 of every year."""
    return [dt.date(year, 1, 1) + dt.timedelta(days=COMPOSITE_DAYS * k) for k in range(46)]


def composite_end(start):
    """Last day a composite covers; the year's last composite stops at 31 December."""
    return min(start + dt.timedelta(days=COMPOSITE_DAYS - 1), dt.date(start.year, 12, 31))


def days_inside(start, end, window_start, window_end):
    return max(0, (min(end, window_end) - max(start, window_start)).days + 1)


def _composites_near(window_start, window_end):
    years = range(window_start.year, window_end.year + 1)
    return [s for year in years for s in composite_starts(year)]


def mai_weights(window_start, window_end):
    """Composite start date -> share of its ET and PET totals that falls in the window."""
    weights = {}
    for start in _composites_near(window_start, window_end):
        end = composite_end(start)
        inside = days_inside(start, end, window_start, window_end)
        if inside:
            weights[start] = inside / ((end - start).days + 1)
    return weights


def vci_composites(window_start, window_end):
    """Composites whose clearest-day values count for the window's VCI."""
    return [
        start
        for start in _composites_near(window_start, window_end)
        if days_inside(start, composite_end(start), window_start, window_end) >= VCI_MIN_DAYS_INSIDE
    ]


def season_composites(year):
    """Composites that touch the weeks a season can use (onset search to season end)."""
    first, last = week_start(year, ONSET_FIRST_WEEK), week_end(year, SEASON_LAST_WEEK)
    return [s for s in composite_starts(year) if days_inside(s, composite_end(s), first, last)]


def vci_history_years(season):
    """VCI compares a season with 2000 up to the season itself, never later years."""
    return range(VCI_HISTORY_START, season + 1)
