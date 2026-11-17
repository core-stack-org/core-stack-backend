import math
import re
import threading
from collections import OrderedDict
from pathlib import Path

from openpyxl import load_workbook


MAX_SCATTER_POINTS = 10000
MAX_MULTI_PROPERTIES = 16
MAX_MULTI_ROWS = 300
# Number of block analyses kept in memory per process (least recently used
# entries are evicted first).
ANALYSIS_CACHE_SIZE = 16
_ANALYSIS_CACHE = OrderedDict()
_ANALYSIS_CACHE_LOCK = threading.Lock()
_SAMPLE_ROWS = 100
_UNMATCHED_SAMPLE = 50
_EXACT_TOLERANCE = 1e-9
_PERIOD_SUFFIX = re.compile(r"^(.+?)_((?:19|20)\d{2}(?:[-_](?:19|20)?\d{2})?)$")
# Identifier / geometry-size columns excluded from multi-property class views.
_META_METRICS = frozenset(
    {
        "areainha",
        "mwsareainha",
        "suminha",
        "stateid",
        "districtid",
        "tehsilid",
        "villageid",
        "gpid",
        "index",
        "terrainclusterid",
        "latitude",
        "longitude",
        "watershedcode",
        "basincode",
        "subbasincode",
    }
)
# Relative tolerances (percent of the old value) for which agreement counts
# are precomputed, so the page can switch tolerance without reloading data.
TOLERANCE_LEVELS = (0, 0.1, 0.5, 1, 2, 5, 10, 25, 50)
DEFAULT_TOLERANCE = 5
_IDENTIFIER_HEADERS = (
    "uid",
    "mwsid",
    "mwsuid",
    "villid",
    "villageid",
    "censuscode2011",
    "statecensusid",
    "id",
)


def _normalize(value):
    return re.sub(r"[^a-z0-9]", "", str(value or "").casefold())


def _natural_key(value):
    """Case-insensitive sort key that orders "sheet 2" before "sheet 10"."""
    return [
        (0, int(part), "") if part.isdigit() else (1, 0, part)
        for part in re.split(r"(\d+)", str(value).casefold())
        if part
    ]


def _tolerance_key(level):
    return f"{level:g}"


def _cell_number(value):
    if isinstance(value, bool) or value is None:
        return None
    if not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _split_header(rows):
    headers = rows[0] if rows else ()
    headers = [str(value).strip() if value is not None else "" for value in headers]
    return headers, rows[1:]


def _read_sheet(worksheet):
    """Read a whole worksheet once so every metric can reuse the rows."""
    return _split_header(list(worksheet.iter_rows(values_only=True)))


def _headers_and_sample(worksheet):
    rows = worksheet.iter_rows(values_only=True)
    headers = next(rows, ())
    headers = [str(value).strip() if value is not None else "" for value in headers]
    sample = []
    for row_index, row in enumerate(rows):
        sample.append(row)
        if row_index + 1 >= _SAMPLE_ROWS:
            break
    return headers, sample


def _columns_by_normalized_name(headers):
    return {
        _normalize(header): index
        for index, header in enumerate(headers)
        if _normalize(header)
    }


def _choose_join_key(common_columns):
    for preferred in _IDENTIFIER_HEADERS:
        if preferred in common_columns:
            return preferred
    return None


def _numeric_common_columns(old_headers, new_headers, old_sample, new_sample, join_key):
    old_columns = _columns_by_normalized_name(old_headers)
    new_columns = _columns_by_normalized_name(new_headers)
    metrics = []

    for normalized_name in old_columns.keys() & new_columns.keys():
        if normalized_name == join_key:
            continue
        old_index = old_columns[normalized_name]
        new_index = new_columns[normalized_name]
        old_has_numbers = any(
            old_index < len(row) and _cell_number(row[old_index]) is not None
            for row in old_sample
        )
        new_has_numbers = any(
            new_index < len(row) and _cell_number(row[new_index]) is not None
            for row in new_sample
        )
        if old_has_numbers and new_has_numbers:
            metrics.append(old_headers[old_index])

    return sorted(metrics, key=str.casefold), old_columns, new_columns


def _metadata(name, old_headers, old_sample, new_headers, new_sample):
    old_columns = _columns_by_normalized_name(old_headers)
    new_columns = _columns_by_normalized_name(new_headers)
    common_columns = old_columns.keys() & new_columns.keys()
    join_key = _choose_join_key(common_columns)
    metrics, _, _ = _numeric_common_columns(
        old_headers, new_headers, old_sample, new_sample, join_key
    )
    if not metrics:
        return None

    return {
        "name": name,
        "metrics": metrics,
        "match_key": old_headers[old_columns[join_key]] if join_key else "Row number",
        "match_method": "column" if join_key else "row_order",
    }


def _sheet_metadata(old_sheet, new_sheet):
    old_headers, old_sample = _headers_and_sample(old_sheet)
    new_headers, new_sample = _headers_and_sample(new_sheet)
    return _metadata(old_sheet.title, old_headers, old_sample, new_headers, new_sample)


def _excel_roots():
    from django.conf import settings

    base_path = Path(settings.EXCEL_PATH) / "data"
    return base_path / "stats_excel_files_gee", base_path / "stats_excel_files"


def _matching_children(parent, name):
    if not parent.is_dir():
        return []
    expected = _normalize(name)
    return sorted(
        (
            child
            for child in parent.iterdir()
            if child.is_dir() and _normalize(child.name) == expected
        ),
        key=lambda child: child.name.casefold(),
    )


def _workbook_path(root, state_name, district_name, block_name):
    expected_stem = _normalize(district_name) + _normalize(block_name)
    for state_dir in _matching_children(root, state_name):
        for district_dir in _matching_children(state_dir, district_name):
            for candidate in district_dir.glob("*.xlsx"):
                if _normalize(candidate.stem) == expected_stem and candidate.is_file():
                    return candidate
    return None


def _location_workbooks(state_name, district_name, block_name):
    old_root, new_root = _excel_roots()
    old_path = _workbook_path(old_root, state_name, district_name, block_name)
    new_path = _workbook_path(new_root, state_name, district_name, block_name)
    if not old_path or not new_path:
        return None, None
    return old_path, new_path


class _SheetComparison:
    """Old/new rows of one sheet held in memory for repeated metric pairing."""

    def __init__(self, name, old_headers, old_rows, new_headers, new_rows):
        self.name = name
        self.old_rows = old_rows
        self.new_rows = new_rows
        self.old_columns = _columns_by_normalized_name(old_headers)
        self.new_columns = _columns_by_normalized_name(new_headers)
        self.metadata = _metadata(
            name,
            old_headers,
            old_rows[:_SAMPLE_ROWS],
            new_headers,
            new_rows[:_SAMPLE_ROWS],
        )
        self.old_key_index = self.new_key_index = None
        if self.metadata and self.metadata["match_method"] == "column":
            key = _normalize(self.metadata["match_key"])
            self.old_key_index = self.old_columns[key]
            self.new_key_index = self.new_columns[key]

    @classmethod
    def from_worksheets(cls, old_sheet, new_sheet):
        old_headers, old_rows = _read_sheet(old_sheet)
        new_headers, new_rows = _read_sheet(new_sheet)
        return cls(old_sheet.title, old_headers, old_rows, new_headers, new_rows)

    @property
    def keyed(self):
        return self.old_key_index is not None

    def metric_pairs(self, metric):
        """Return (pairs sorted by |diff| desc, old-only ids, new-only ids)."""
        old_metric_index = self.old_columns[_normalize(metric)]
        new_metric_index = self.new_columns[_normalize(metric)]
        old_only = []
        new_only = []

        if self.keyed:
            old_values = _keyed_values(self.old_rows, self.old_key_index, old_metric_index)
            new_values = _keyed_values(self.new_rows, self.new_key_index, new_metric_index)
            pairs = []
            for row_key in sorted(old_values.keys() & new_values.keys(), key=str.casefold):
                for old_value, new_value in zip(old_values[row_key], new_values[row_key]):
                    pairs.append({"id": row_key, "old": old_value, "new": new_value})
            old_only = sorted(old_values.keys() - new_values.keys(), key=str.casefold)
            new_only = sorted(new_values.keys() - old_values.keys(), key=str.casefold)
        else:
            pairs = _positional_pairs(
                self.old_rows, self.new_rows, old_metric_index, new_metric_index
            )

        for pair in pairs:
            pair.update(_difference(pair["old"], pair["new"]))

        # Most divergent MWS first so truncation never hides them.
        pairs.sort(key=lambda pair: abs(pair["diff"]), reverse=True)
        return pairs, old_only, new_only

    def key_coverage(self):
        """MWS ids present in only one workbook, regardless of metric values."""
        if not self.keyed:
            return [], []
        old_keys = _row_keys(self.old_rows, self.old_key_index)
        new_keys = _row_keys(self.new_rows, self.new_key_index)
        return (
            sorted(old_keys - new_keys, key=str.casefold),
            sorted(new_keys - old_keys, key=str.casefold),
        )


def _unmatched_payload(old_only, new_only):
    return {
        "old_only_count": len(old_only),
        "new_only_count": len(new_only),
        "old_only": old_only[:_UNMATCHED_SAMPLE],
        "new_only": new_only[:_UNMATCHED_SAMPLE],
    }


def _scatter_payload(comparison, metric, pairs, old_only, new_only):
    return {
        "sheet": comparison.name,
        "metric": metric,
        "match_key": comparison.metadata["match_key"],
        "points": pairs[:MAX_SCATTER_POINTS],
        "truncated": len(pairs) > MAX_SCATTER_POINTS,
        "summary": summarize_pairs(pairs),
        "unmatched": _unmatched_payload(old_only, new_only),
    }


def _metric_severity(metric):
    """Sort key: least-agreeing metrics first, empty metrics last."""
    summary = metric["summary"]
    count = summary["count"]
    if not count:
        return (1, 0.0, 0.0, 0.0)
    within = summary["within_tolerance"][_tolerance_key(DEFAULT_TOLERANCE)] / count
    r_squared = summary["r_squared"] if summary["r_squared"] is not None else 1.0
    return (0, within, r_squared, -summary["rmse"])


def _analyse_sheet(comparison):
    metrics = []
    pairs_by_metric = {}
    for metric in comparison.metadata["metrics"]:
        pairs_by_metric[metric] = comparison.metric_pairs(metric)
        metrics.append(
            {"name": metric, "summary": summarize_pairs(pairs_by_metric[metric][0])}
        )
    metrics.sort(key=_metric_severity)

    default_metric = metrics[0]["name"]
    old_only, new_only = comparison.key_coverage()
    return {
        "name": comparison.name,
        "match_key": comparison.metadata["match_key"],
        "match_method": comparison.metadata["match_method"],
        "metrics": metrics,
        "unmatched": _unmatched_payload(old_only, new_only),
        "default_metric": default_metric,
        "detail": _scatter_payload(
            comparison, default_metric, *pairs_by_metric[default_metric]
        ),
        "multi_property": _build_multi_property(metrics, pairs_by_metric),
    }


_LABEL_SUFFIXES = (
    "_area_in_ha",
    "_in_ha",
    "_area_percent",
    "_in_percent",
    "_percent",
    "_unit_less",
    "_in_mm",
    "_in_bcm",
    "_in_m",
    "_in_km",
    "_in_weeks",
    "_count",
)


def _short_property_label(name):
    label = str(name).strip()
    lower = label.casefold()
    for suffix in _LABEL_SUFFIXES:
        if lower.endswith(suffix) and len(label) > len(suffix):
            label = label[: -len(suffix)]
            break
    if label.casefold().startswith("principle_aq_") and len(label) > 13:
        label = label[13:]
    return label.replace("_", " ").strip() or str(name)


def _unit_kind(metric_name):
    lower = metric_name.casefold()
    if ("_in_ha" in lower or lower.endswith("_ha")) and "_per_ha" not in lower:
        return "ha"
    if "percent" in lower or lower.endswith("_pct"):
        return "%"
    if "_in_mm" in lower:
        return "mm"
    if "_in_bcm" in lower:
        return "bcm"
    if "_in_weeks" in lower:
        return "weeks"
    if lower.endswith("_count") or "_count_" in lower:
        return "count"
    return None


def _is_aggregate_column(metric_name):
    lower = metric_name.casefold()
    return lower.startswith(("total_", "all_", "sum_")) or lower.startswith(
        "area_under_"
    )


def _build_multi_slice(prop_specs, pairs_by_metric, period=None):
    """Align MWS rows across ``prop_specs`` [(metric_name, label), ...]."""
    prop_specs = list(prop_specs)[:MAX_MULTI_PROPERTIES]
    if not prop_specs:
        return None

    # Index pairs by (mws_id, occurrence) so even non-unique keys align safely.
    by_row = {}
    col_scales = []
    units = [_unit_kind(metric) for metric, _ in prop_specs]
    same_unit = units[0] is not None and all(u == units[0] for u in units)
    non_negative = True

    for col_idx, (metric, _) in enumerate(prop_specs):
        pairs = pairs_by_metric[metric][0]
        seen_counts = {}
        max_mag = 1.0
        for pair in pairs:
            row_id = pair["id"]
            occ = seen_counts.get(row_id, 0)
            seen_counts[row_id] = occ + 1
            key = (row_id, occ)
            entry = by_row.get(key)
            if entry is None:
                entry = {
                    "id": row_id,
                    "old": [None] * len(prop_specs),
                    "new": [None] * len(prop_specs),
                }
                by_row[key] = entry
            old_val = pair["old"]
            new_val = pair["new"]
            entry["old"][col_idx] = old_val
            entry["new"][col_idx] = new_val
            if old_val < -1e-9 or new_val < -1e-9:
                non_negative = False
            mag = max(abs(old_val), abs(new_val))
            if mag > max_mag:
                max_mag = mag
        col_scales.append(max_mag)

    compositional = (
        same_unit
        and units[0] in ("ha", "%")
        and non_negative
        and not any(_is_aggregate_column(m) for m, _ in prop_specs)
    )

    rows = []
    for entry in by_row.values():
        l1 = 0.0
        norm_score = 0.0
        for idx, (old_val, new_val) in enumerate(zip(entry["old"], entry["new"])):
            if old_val is None or new_val is None:
                continue
            abs_d = abs(new_val - old_val)
            l1 += abs_d
            norm_score += abs_d / col_scales[idx]
        entry["l1"] = l1
        entry["_score"] = l1 if same_unit else norm_score
        rows.append(entry)

    rows.sort(key=lambda r: (r["_score"], r["l1"]), reverse=True)
    total_rows = len(rows)
    trimmed = rows[:MAX_MULTI_ROWS]
    for row in trimmed:
        row.pop("_score", None)

    return {
        "period": period,
        "unit": units[0] if same_unit else None,
        "same_unit": same_unit,
        "compositional": compositional,
        "properties": [
            {"metric": metric, "label": label} for metric, label in prop_specs
        ],
        "rows": trimmed,
        "total_rows": total_rows,
        "truncated_rows": total_rows > MAX_MULTI_ROWS,
    }


def _group_temporal_metrics(candidates):
    """Detect <base>_<period> columns; return (ordered_bases, periods, grid) or None."""
    raw_grid = {}
    for item in candidates:
        match = _PERIOD_SUFFIX.match(item["name"])
        if not match:
            continue
        base, period = match.group(1), match.group(2)
        if _normalize(base) in _META_METRICS:
            continue
        raw_grid.setdefault(base, {})[period] = item

    # Keep bases present in >=2 periods, then periods covering >=2 such bases.
    multi_bases = {b: pmap for b, pmap in raw_grid.items() if len(pmap) >= 2}
    if len(multi_bases) < 2:
        return None
    period_counts = {}
    for pmap in multi_bases.values():
        for period in pmap:
            period_counts[period] = period_counts.get(period, 0) + 1
    periods = sorted(
        (p for p, cnt in period_counts.items() if cnt >= 2), key=_natural_key
    )
    if len(periods) < 2:
        return None

    # Order bases so the classes with the worst single-period agreement come first.
    ordered_bases = sorted(
        multi_bases.keys(),
        key=lambda b: min(
            (_metric_severity(multi_bases[b][p]) for p in periods if p in multi_bases[b]),
            default=(1, 0.0, 0.0, 0.0),
        ),
    )
    return ordered_bases, periods, multi_bases


def _worst_period(periods, multi_bases):
    tol_key = _tolerance_key(DEFAULT_TOLERANCE)

    def severity(period):
        items = [
            pmap[period]["summary"]
            for pmap in multi_bases.values()
            if period in pmap and pmap[period]["summary"]["count"]
        ]
        if not items:
            return (1.0, 0.0)
        mean_within = sum(
            s["within_tolerance"][tol_key] / s["count"] for s in items
        ) / len(items)
        mean_mae = sum(s["mae"] for s in items) / len(items)
        return (mean_within, -mean_mae)

    return min(periods, key=severity)


def _build_multi_property(metrics, pairs_by_metric):
    candidates = [
        m
        for m in metrics
        if m["summary"]["count"] > 0 and _normalize(m["name"]) not in _META_METRICS
    ]
    temporal = _group_temporal_metrics(candidates)
    if temporal is not None:
        ordered_bases, periods, multi_bases = temporal
        default_period = _worst_period(periods, multi_bases)
        specs = [
            (multi_bases[b][default_period]["name"], _short_property_label(b))
            for b in ordered_bases
            if default_period in multi_bases[b]
        ]
        return {
            "mode": "temporal",
            "periods": periods,
            "default_period": default_period,
            "grid": [
                {
                    "key": b,
                    "label": _short_property_label(b),
                    "metrics": {
                        p: multi_bases[b][p]["name"]
                        for p in periods
                        if p in multi_bases[b]
                    },
                }
                for b in ordered_bases
            ],
            "truncated_properties": len(ordered_bases) > MAX_MULTI_PROPERTIES,
            "slice": _build_multi_slice(specs, pairs_by_metric, period=default_period),
        }

    if len(candidates) < 3:
        return None

    specs = [(m["name"], _short_property_label(m["name"])) for m in candidates]
    return {
        "mode": "flat",
        "truncated_properties": len(candidates) > MAX_MULTI_PROPERTIES,
        "total_properties": len(candidates),
        "slice": _build_multi_slice(specs, pairs_by_metric, period=None),
    }


def get_comparable_sheets(state_name, district_name, block_name):
    old_path, new_path = _location_workbooks(state_name, district_name, block_name)
    if not old_path or not new_path:
        return {"sheets": [], "error": "A current or old workbook was not found for this location."}

    old_book = load_workbook(old_path, read_only=True, data_only=True)
    new_book = load_workbook(new_path, read_only=True, data_only=True)
    try:
        sheets = []
        for sheet_name in old_book.sheetnames:
            if sheet_name not in new_book.sheetnames:
                continue
            metadata = _sheet_metadata(old_book[sheet_name], new_book[sheet_name])
            if metadata:
                sheets.append(metadata)
        return {"sheets": sheets}
    finally:
        old_book.close()
        new_book.close()


def _file_signature(path):
    stat = path.stat()
    return (str(path), stat.st_mtime_ns, stat.st_size)


def _cached_analysis(cache_key):
    with _ANALYSIS_CACHE_LOCK:
        result = _ANALYSIS_CACHE.get(cache_key)
        if result is not None:
            _ANALYSIS_CACHE.move_to_end(cache_key)
        return result


def _store_analysis(cache_key, result):
    with _ANALYSIS_CACHE_LOCK:
        _ANALYSIS_CACHE[cache_key] = result
        _ANALYSIS_CACHE.move_to_end(cache_key)
        while len(_ANALYSIS_CACHE) > ANALYSIS_CACHE_SIZE:
            _ANALYSIS_CACHE.popitem(last=False)


def clear_analysis_cache():
    with _ANALYSIS_CACHE_LOCK:
        _ANALYSIS_CACHE.clear()


def get_workbook_analysis(state_name, district_name, block_name):
    """Compare every shared numeric metric of every shared sheet in one pass.

    Sheets are returned in natural alphabetical order of their names. Each
    carries agreement statistics for all of its metrics (worst first) plus
    full scatter data for the worst metric, so the dashboard can render a
    complete single-page report without further requests.

    Results are cached per process, keyed on both workbooks' path,
    modification time and size, so replacing either file invalidates the
    entry automatically. The returned dict is shared; do not mutate it.
    """
    old_path, new_path = _location_workbooks(state_name, district_name, block_name)
    if not old_path or not new_path:
        return {"sheets": [], "error": "A current or old workbook was not found for this location."}

    cache_key = (_file_signature(old_path), _file_signature(new_path))
    cached = _cached_analysis(cache_key)
    if cached is not None:
        return cached

    result = _build_workbook_analysis(old_path, new_path)
    _store_analysis(cache_key, result)
    return result


def _build_workbook_analysis(old_path, new_path):
    old_book = load_workbook(old_path, read_only=True, data_only=True)
    new_book = load_workbook(new_path, read_only=True, data_only=True)
    try:
        sheets = []
        for sheet_name in old_book.sheetnames:
            if sheet_name not in new_book.sheetnames:
                continue
            comparison = _SheetComparison.from_worksheets(
                old_book[sheet_name], new_book[sheet_name]
            )
            if comparison.metadata:
                sheets.append(_analyse_sheet(comparison))
        sheets.sort(key=lambda sheet: _natural_key(sheet["name"]))
        return {
            "sheets": sheets,
            "tolerance_levels": [_tolerance_key(level) for level in TOLERANCE_LEVELS],
            "default_tolerance": _tolerance_key(DEFAULT_TOLERANCE),
        }
    finally:
        old_book.close()
        new_book.close()


def get_scatter_points(state_name, district_name, block_name, sheet_name, metric):
    old_path, new_path = _location_workbooks(state_name, district_name, block_name)
    if not old_path or not new_path:
        raise ValueError("A current or old workbook was not found for this location.")

    old_book = load_workbook(old_path, read_only=True, data_only=True)
    new_book = load_workbook(new_path, read_only=True, data_only=True)
    try:
        if sheet_name not in old_book.sheetnames or sheet_name not in new_book.sheetnames:
            raise ValueError("The selected sheet is not present in both workbooks.")
        comparison = _SheetComparison.from_worksheets(
            old_book[sheet_name], new_book[sheet_name]
        )
        if not comparison.metadata or metric not in comparison.metadata["metrics"]:
            raise ValueError("The selected metric is not comparable for this sheet.")
        return _scatter_payload(comparison, metric, *comparison.metric_pairs(metric))
    finally:
        old_book.close()
        new_book.close()


def get_multi_metric_slice(
    state_name, district_name, block_name, sheet_name, period=None
):
    """Return the aligned multi-property MWS slice for ``sheet_name`` (and optional ``period``)."""
    old_path, new_path = _location_workbooks(state_name, district_name, block_name)
    if not old_path or not new_path:
        raise ValueError("A current or old workbook was not found for this location.")

    old_book = load_workbook(old_path, read_only=True, data_only=True)
    new_book = load_workbook(new_path, read_only=True, data_only=True)
    try:
        if sheet_name not in old_book.sheetnames or sheet_name not in new_book.sheetnames:
            raise ValueError("The selected sheet is not present in both workbooks.")
        comparison = _SheetComparison.from_worksheets(
            old_book[sheet_name], new_book[sheet_name]
        )
        if not comparison.metadata:
            raise ValueError("The selected sheet has no comparable numeric metrics.")

        pairs_by_metric = {
            m: comparison.metric_pairs(m) for m in comparison.metadata["metrics"]
        }
        metrics = [
            {"name": m, "summary": summarize_pairs(pairs_by_metric[m][0])}
            for m in comparison.metadata["metrics"]
        ]
        metrics.sort(key=_metric_severity)

        if period:
            candidates = [
                m
                for m in metrics
                if m["summary"]["count"] > 0
                and _normalize(m["name"]) not in _META_METRICS
            ]
            temporal = _group_temporal_metrics(candidates)
            if temporal is None:
                raise ValueError("Sheet does not have period-grouped metrics.")
            ordered_bases, periods, multi_bases = temporal
            if period not in periods:
                raise ValueError(f"No comparable metrics found for period {period!r}.")
            specs = [
                (multi_bases[b][period]["name"], _short_property_label(b))
                for b in ordered_bases
                if period in multi_bases[b]
            ]
            return _build_multi_slice(specs, pairs_by_metric, period=period)

        multi = _build_multi_property(metrics, pairs_by_metric)
        if not multi:
            raise ValueError("Sheet does not have enough comparable properties.")
        return multi["slice"]
    finally:
        old_book.close()
        new_book.close()


def _difference(old_value, new_value):
    diff = new_value - old_value
    pct_diff = (diff * 100 / abs(old_value)) if old_value else None
    return {"diff": diff, "pct_diff": pct_diff}


def _is_exact(old_value, new_value):
    scale = max(abs(old_value), abs(new_value), 1.0)
    return abs(new_value - old_value) <= _EXACT_TOLERANCE * scale


def is_within_tolerance(old_value, new_value, tolerance_pct):
    """True when new is within tolerance_pct percent of old (or identical).

    The dashboard JavaScript mirrors this rule; keep the two in sync.
    """
    if _is_exact(old_value, new_value):
        return True
    return old_value != 0 and abs(new_value - old_value) <= (
        tolerance_pct / 100 * abs(old_value)
    )


def summarize_pairs(pairs):
    """Agreement statistics between old (GEE) and new (local) values."""
    count = len(pairs)
    if not count:
        return {"count": 0}

    olds = [pair["old"] for pair in pairs]
    news = [pair["new"] for pair in pairs]
    diffs = [new - old for old, new in zip(olds, news)]

    mean_old = sum(olds) / count
    mean_new = sum(news) / count
    var_old = sum((value - mean_old) ** 2 for value in olds)
    var_new = sum((value - mean_new) ** 2 for value in news)
    covariance = sum(
        (old - mean_old) * (new - mean_new) for old, new in zip(olds, news)
    )

    pearson_r = None
    if var_old > 0 and var_new > 0:
        pearson_r = covariance / math.sqrt(var_old * var_new)

    slope = intercept = None
    if var_old > 0:
        slope = covariance / var_old
        intercept = mean_new - slope * mean_old

    abs_diffs = [abs(diff) for diff in diffs]
    worst_index = max(range(count), key=abs_diffs.__getitem__)
    exact = [_is_exact(old, new) for old, new in zip(olds, news)]

    return {
        "count": count,
        "mean_old": mean_old,
        "mean_new": mean_new,
        "bias": sum(diffs) / count,
        "mae": sum(abs_diffs) / count,
        "rmse": math.sqrt(sum(diff * diff for diff in diffs) / count),
        "max_abs_diff": abs_diffs[worst_index],
        "max_abs_diff_id": pairs[worst_index]["id"],
        "pearson_r": pearson_r,
        "r_squared": pearson_r * pearson_r if pearson_r is not None else None,
        "slope": slope,
        "intercept": intercept,
        "exact_matches": sum(exact),
        "higher_in_new": sum(
            diff > 0 and not is_exact for diff, is_exact in zip(diffs, exact)
        ),
        "lower_in_new": sum(
            diff < 0 and not is_exact for diff, is_exact in zip(diffs, exact)
        ),
        "within_tolerance": {
            _tolerance_key(level): sum(
                is_within_tolerance(old, new, level) for old, new in zip(olds, news)
            )
            for level in TOLERANCE_LEVELS
        },
    }


def _keyed_values(rows, key_index, value_index):
    values = {}
    for row in rows:
        if key_index >= len(row) or value_index >= len(row):
            continue
        row_key = row[key_index]
        value = _cell_number(row[value_index])
        if row_key is None or value is None:
            continue
        values.setdefault(str(row_key).strip(), []).append(value)
    return values


def _rows_by_key(worksheet, key_index, value_index):
    return _keyed_values(
        worksheet.iter_rows(min_row=2, values_only=True), key_index, value_index
    )


def _row_keys(rows, key_index):
    return {
        str(row[key_index]).strip()
        for row in rows
        if key_index < len(row) and row[key_index] is not None
    }


def _positional_pairs(old_rows, new_rows, old_metric_index, new_metric_index):
    pairs = []
    for row_number, (old_row, new_row) in enumerate(zip(old_rows, new_rows), start=2):
        if old_metric_index >= len(old_row) or new_metric_index >= len(new_row):
            continue
        old_value = _cell_number(old_row[old_metric_index])
        new_value = _cell_number(new_row[new_metric_index])
        if old_value is not None and new_value is not None:
            pairs.append({"id": str(row_number), "old": old_value, "new": new_value})
    return pairs
