import math
import re
from pathlib import Path

from openpyxl import load_workbook


MAX_SCATTER_POINTS = 10000
_SAMPLE_ROWS = 100
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


def _sheet_metadata(old_sheet, new_sheet):
    old_headers, old_sample = _headers_and_sample(old_sheet)
    new_headers, new_sample = _headers_and_sample(new_sheet)
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
        "name": old_sheet.title,
        "metrics": metrics,
        "match_key": old_headers[old_columns[join_key]] if join_key else "Row number",
        "match_method": "column" if join_key else "row_order",
    }


def _excel_roots():
    from django.conf import settings

    base_path = Path(settings.EXCEL_PATH) / "data"
    return base_path / "stats_excel_files_bk_1jul", base_path / "stats_excel_files"


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


def get_scatter_points(state_name, district_name, block_name, sheet_name, metric):
    old_path, new_path = _location_workbooks(state_name, district_name, block_name)
    if not old_path or not new_path:
        raise ValueError("A current or old workbook was not found for this location.")

    old_book = load_workbook(old_path, read_only=True, data_only=True)
    new_book = load_workbook(new_path, read_only=True, data_only=True)
    try:
        if sheet_name not in old_book.sheetnames or sheet_name not in new_book.sheetnames:
            raise ValueError("The selected sheet is not present in both workbooks.")
        old_sheet = old_book[sheet_name]
        new_sheet = new_book[sheet_name]
        metadata = _sheet_metadata(old_sheet, new_sheet)
        if not metadata or metric not in metadata["metrics"]:
            raise ValueError("The selected metric is not comparable for this sheet.")

        old_headers, _ = _headers_and_sample(old_sheet)
        new_headers, _ = _headers_and_sample(new_sheet)
        old_columns = _columns_by_normalized_name(old_headers)
        new_columns = _columns_by_normalized_name(new_headers)
        old_metric_index = old_columns[_normalize(metric)]
        new_metric_index = new_columns[_normalize(metric)]

        if metadata["match_method"] == "column":
            key = _normalize(metadata["match_key"])
            old_key_index = old_columns[key]
            new_key_index = new_columns[key]
            old_rows = _rows_by_key(old_sheet, old_key_index, old_metric_index)
            new_rows = _rows_by_key(new_sheet, new_key_index, new_metric_index)
            pairs = []
            for row_key in sorted(old_rows.keys() & new_rows.keys(), key=str.casefold):
                for old_value, new_value in zip(old_rows[row_key], new_rows[row_key]):
                    pairs.append({"id": row_key, "old": old_value, "new": new_value})
        else:
            pairs = _rows_by_position(old_sheet, new_sheet, old_metric_index, new_metric_index)

        return {
            "sheet": sheet_name,
            "metric": metric,
            "match_key": metadata["match_key"],
            "points": pairs[:MAX_SCATTER_POINTS],
            "truncated": len(pairs) > MAX_SCATTER_POINTS,
        }
    finally:
        old_book.close()
        new_book.close()


def _rows_by_key(worksheet, key_index, value_index):
    rows = {}
    for row in worksheet.iter_rows(min_row=2, values_only=True):
        if key_index >= len(row) or value_index >= len(row):
            continue
        row_key = row[key_index]
        old_value = _cell_number(row[value_index])
        if row_key is None or old_value is None:
            continue
        rows.setdefault(str(row_key).strip(), []).append(old_value)
    return rows


def _rows_by_position(old_sheet, new_sheet, old_metric_index, new_metric_index):
    old_rows = old_sheet.iter_rows(min_row=2, values_only=True)
    new_rows = new_sheet.iter_rows(min_row=2, values_only=True)
    pairs = []
    for row_number, (old_row, new_row) in enumerate(zip(old_rows, new_rows), start=2):
        if old_metric_index >= len(old_row) or new_metric_index >= len(new_row):
            continue
        old_value = _cell_number(old_row[old_metric_index])
        new_value = _cell_number(new_row[new_metric_index])
        if old_value is not None and new_value is not None:
            pairs.append({"id": str(row_number), "old": old_value, "new": new_value})
    return pairs
