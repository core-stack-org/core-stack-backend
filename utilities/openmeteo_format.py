"""
Open-Meteo-style API payloads: aligned time arrays + units, shared across apps.
"""
import re

from utilities.renderers import round_floats

YEAR_SUFFIX_RE = re.compile(
    r"^(?P<metric>.+)_(?P<period>\d{2}-\d{2}|\d{4}(?:[-_]\d{4})?|\d{4})$"
)


def error_envelope(message, details=None):
    payload = {"status": "error", "error_message": message, "error": message}
    if details is not None:
        payload["details"] = details
    return round_floats(payload, precision=2)


def success_envelope(data_block):
    return round_floats(
        {"status": "success", "error_message": None, "data": data_block},
        precision=2,
    )


def _period_sort_key(period_label):
    if "-" in period_label:
        first = period_label.split("-")[0]
        if len(first) == 2:
            return int(f"20{first}")
        return int(first)
    return int(period_label)


def _normalize_period_label(period):
    if len(period) == 5 and period[2] == "-":
        start = int(period[:2])
        end = int(period[3:])
        return f"{2000 + start}-{2000 + end}"
    if len(period) == 9 and period[4] == "-":
        return period
    if len(period) == 9 and period[4] == "_":
        start = int(period[:4])
        end = int(period[5:])
        return f"{start}-{end}"
    if len(period) == 4:
        year = int(period)
        return f"{year}-{year + 1}"
    return period


def _has_timeseries_keys(payload):
    if not isinstance(payload, dict):
        return False
    return any(YEAR_SUFFIX_RE.match(str(key)) for key in payload.keys())


def annual_structure_from_dict(item):
    """Fold metric_2017, metric_17-18, etc. into annual.time + aligned arrays."""
    if not isinstance(item, dict):
        return round_floats(
            {
                "metadata": item,
                "annual": {"time": []},
                "annual_units": {"time": "period"},
            },
            precision=2,
        )

    grouped = {}
    metadata = {}

    for key, value in item.items():
        match = YEAR_SUFFIX_RE.match(key)
        if not match:
            metadata[key] = _convert_nested_timeseries(value)
            continue

        metric = match.group("metric")
        period_raw = match.group("period")
        period_label = _normalize_period_label(period_raw)
        grouped.setdefault(period_label, {})[metric] = value

    if not grouped:
        return round_floats(
            {
                "metadata": metadata,
                "annual": {"time": []},
                "annual_units": {"time": "period"},
            },
            precision=2,
        )

    periods = sorted(grouped.keys(), key=_period_sort_key)
    metric_names = sorted({m for period in periods for m in grouped[period].keys()})

    annual = {"time": periods}
    annual_units = {"time": "period"}
    for metric in metric_names:
        annual[metric] = [grouped[period].get(metric) for period in periods]
        annual_units[metric] = "unknown"

    return round_floats(
        {
            "metadata": metadata,
            "annual": annual,
            "annual_units": annual_units,
        },
        precision=2,
    )


def _convert_nested_timeseries(value):
    if isinstance(value, dict):
        if _has_timeseries_keys(value):
            return annual_structure_from_dict(value)
        return {k: _convert_nested_timeseries(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_convert_nested_timeseries(v) for v in value]
    return value


def hourly_structure_from_mws(payload):
    """GeoServer fortnight JSON -> Open-Meteo hourly-style arrays."""
    if not isinstance(payload, dict):
        return round_floats(
            {
                "metadata": payload,
                "hourly": {"time": []},
                "hourly_units": {"time": "iso8601"},
            },
            precision=2,
        )

    if not isinstance(payload.get("time_series"), list):
        return round_floats(
            {
                "metadata": payload,
                "hourly": {"time": []},
                "hourly_units": {"time": "iso8601"},
            },
            precision=2,
        )

    rows = payload.get("time_series", [])
    hourly = {
        "time": [row.get("date") for row in rows],
        "et": [row.get("et") for row in rows],
        "runoff": [row.get("runoff") for row in rows],
        "precipitation": [row.get("precipitation") for row in rows],
    }
    return round_floats(
        {
            "metadata": {"mws_id": payload.get("mws_id")},
            "hourly": hourly,
            "hourly_units": {
                "time": "iso8601",
                "et": "mm",
                "runoff": "mm",
                "precipitation": "mm",
            },
        },
        precision=2,
    )


def metadata_only(payload):
    """Non time-series responses: metadata + empty hourly (Open-Meteo compatible)."""
    if isinstance(payload, dict):
        meta = dict(payload)
    else:
        meta = {"value": payload}
    return round_floats(
        {
            "metadata": meta,
            "hourly": {"time": []},
            "hourly_units": {"time": "iso8601"},
        },
        precision=2,
    )


def normalize_payload(payload):
    """
    Best-effort Open-Meteo shape for arbitrary API results.
    """
    if isinstance(payload, dict) and "metadata" in payload and (
        ("hourly" in payload and isinstance(payload.get("hourly"), dict))
        or ("annual" in payload and isinstance(payload.get("annual"), dict))
    ):
        return round_floats(payload, precision=2)
    if isinstance(payload, dict) and isinstance(
        payload.get("time_series"), list
    ):
        return hourly_structure_from_mws(payload)
    if isinstance(payload, list):
        normalized_items = []
        for x in payload:
            if isinstance(x, dict):
                normalized_items.append(annual_structure_from_dict(x))
            else:
                normalized_items.append(metadata_only(x))
        return round_floats(
            {
                "metadata": {"items": normalized_items},
                "hourly": {"time": []},
                "hourly_units": {"time": "iso8601"},
            },
            precision=2,
        )
    if isinstance(payload, dict):
        return annual_structure_from_dict(payload)
    return metadata_only(payload)
