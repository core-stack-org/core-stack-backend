import datetime
import re

import ee
from utilities.gee_utils import load_gee_asset

_HYDROLOGY_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_HYDROLOGY_YEAR_RANGE_RE = re.compile(r"^\d{4}_\d{4}$")


def is_hydrology_period_column(col_name):
    col = str(col_name)
    return bool(
        _HYDROLOGY_ISO_DATE_RE.match(col) or _HYDROLOGY_YEAR_RANGE_RE.match(col)
    )


def parse_hydrology_period_start(col_name):
    """Return period start (July 1) for ISO dates or hydrological year labels."""
    col = str(col_name)
    if _HYDROLOGY_YEAR_RANGE_RE.match(col):
        start_year = int(col.split("_")[0])
        return datetime.datetime(start_year, 7, 1)
    return datetime.datetime.strptime(col, "%Y-%m-%d")


def hydrology_period_end(period_start, is_annual):
    if is_annual:
        return period_start + datetime.timedelta(days=364)
    return period_start + datetime.timedelta(days=14)


def hydrology_period_label(col_name, is_annual):
    """Normalize a period column name to the annual hydrological-year key."""
    col = str(col_name)
    if _HYDROLOGY_YEAR_RANGE_RE.match(col):
        return col
    period_start = parse_hydrology_period_start(col)
    if is_annual:
        return f"{period_start.year}_{period_start.year + 1}"
    return col


def hydrology_period_columns(col_names):
    cols = [col for col in col_names if is_hydrology_period_column(col)]
    return sorted(cols, key=parse_hydrology_period_start)


def parse_hydrology_end_date(value):
    """Parse layer misc end_date or the last period column name."""
    text = str(value)
    if _HYDROLOGY_YEAR_RANGE_RE.match(text):
        end_year = int(text.split("_")[1])
        return datetime.datetime(end_year, 6, 30)
    return datetime.datetime.strptime(text, "%Y-%m-%d")


def get_last_date(asset_id, is_annual, layer_obj):
    if layer_obj:
        existing_end_date = parse_hydrology_end_date(layer_obj.misc["end_date"])
    else:
        fc = load_gee_asset(asset_id)
        col_names = fc.first().propertyNames().getInfo()
        period_cols = hydrology_period_columns(col_names)
        if not period_cols:
            raise ValueError(
                f"No hydrology period columns found on asset {asset_id}"
            )
        period_start = parse_hydrology_period_start(period_cols[-1])
        existing_end_date = hydrology_period_end(period_start, is_annual)

    return existing_end_date
