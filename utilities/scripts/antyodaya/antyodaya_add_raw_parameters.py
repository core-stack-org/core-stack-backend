#!/usr/bin/env python3
"""Add lean, directly named raw Antyodaya parameters to the Pan-India output.

The existing category/feature columns are copied without modification. Each
unique raw dependency is inserted once, at the first feature that uses it in
the configured category/feature order. Later features reference that same
canonical column through the generated schema and hierarchy files.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
import warnings
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import PerformanceWarning


REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = REPO_ROOT / "data" / "antyodaya"
DEFAULT_INPUT = DATA_DIR / "output" / "antyodaya_feature_category_values_cluster.csv"
DEFAULT_OUTPUT = DATA_DIR / "output" / "antyodaya_2020_cluster_analysis_full.csv"
DEFAULT_SCHEMA_OUTPUT = DATA_DIR / "output" / "antyodaya_2020_cluster_analysis_full_schema.csv"
DEFAULT_MAPPING_CONFIG = DATA_DIR / "mappings" / "antyodaya_variable_mapping.json"
DEFAULT_RAW_COLUMN_FLOW = DATA_DIR / "mappings" / "antyodaya_raw_column_flow.csv"

GROUP_COLUMNS = (
    "state_code",
    "district_code",
    "sub_district_code",
    "village_code",
)
VALID_VILLAGE_KEY = re.compile(r"^\d+:\d+:\d+:\d+$")
WRITE_BATCH_SIZE = 5_000


@dataclass(frozen=True)
class RawColumn:
    name: str
    description: str
    data_type: str
    value_codes: str
    code_meanings: dict[str, str]
    unit: str
    source_unit: str
    aggregation: str
    value_representation: str


@dataclass(frozen=True)
class RawOutputColumn:
    feature_id: str
    category_id: str
    feature_display_name: str
    raw_order: int
    raw: RawColumn
    output_column: str
    column_owner_feature_id: str
    column_owner_feature_display_name: str
    column_owner_value_column: str
    is_column_emitted_here: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Copy the combined Antyodaya category/feature output and insert "
            "the raw parameter values and units used by each feature."
        )
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--schema-output", type=Path, default=DEFAULT_SCHEMA_OUTPUT)
    parser.add_argument("--mapping-config", type=Path, default=DEFAULT_MAPPING_CONFIG)
    parser.add_argument("--raw-column-flow", type=Path, default=DEFAULT_RAW_COLUMN_FLOW)
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help="Override the raw 2020 CSV directory from the mapping config.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing output files.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_code_meanings(value_codes: str) -> dict[str, str]:
    if not value_codes:
        return {}
    matches = re.finditer(
        r"(?:^|,\s*)(-?\d+(?:\.\d+)?)\s*=\s*(.*?)(?=,\s*-?\d+(?:\.\d+)?\s*=|$)",
        value_codes,
    )
    return {
        normalize_code_key(match.group(1)): match.group(2).strip()
        for match in matches
    }


def normalize_code_key(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value).strip()
    if numeric.is_integer():
        return str(int(numeric))
    return format(numeric, ".15g")


def normalized_unit(source_unit: str, data_type: str) -> str:
    if data_type == "Categorical":
        return ""
    if source_unit in {"", "Count"}:
        return ""
    if source_unit == "Currency (INR)":
        return "INR"
    return source_unit


def is_simple_yes_no(raw_name: str, code_meanings: dict[str, str], config: dict[str, Any]) -> bool:
    presence_codes = config["processing"].get("direct_binary_presence", {}).get(raw_name)
    return (
        presence_codes == [1]
        and code_meanings == {"1": "Yes", "2": "No"}
    )


def value_representation_for(
    *,
    raw_name: str,
    data_type: str,
    code_meanings: dict[str, str],
    config: dict[str, Any],
) -> str:
    if data_type != "Categorical":
        return "numeric"
    if is_simple_yes_no(raw_name, code_meanings, config):
        return "standard_binary_availability"
    return "decoded_categorical_label"


def load_raw_columns_for_config(path: Path, config: dict[str, Any]) -> dict[str, RawColumn]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))

    raw_columns: dict[str, RawColumn] = {}
    for row in rows:
        name = row["column_name"].strip()
        data_type = row["data_type"].strip()
        source_unit = row["unit"].strip()
        code_meanings = parse_code_meanings(row["value_codes"].strip())
        raw_columns[name] = RawColumn(
            name=name,
            description=row["description"].strip(),
            data_type=data_type,
            value_codes=row["value_codes"].strip(),
            code_meanings=code_meanings,
            unit=normalized_unit(source_unit, data_type),
            source_unit=source_unit,
            aggregation=row["aggregation"].strip(),
            value_representation=value_representation_for(
                raw_name=name,
                data_type=data_type,
                code_meanings=code_meanings,
                config=config,
            ),
        )
    return raw_columns


def feature_output_stem(feature_id: str) -> str:
    if not feature_id.endswith("_feature"):
        raise ValueError(f"Unexpected feature ID: {feature_id}")
    return feature_id[: -len("_feature")] + "_feat"


def build_feature_layout(
    config: dict[str, Any],
    raw_columns: dict[str, RawColumn],
) -> tuple[dict[str, list[RawOutputColumn]], dict[str, list[RawOutputColumn]], list[str]]:
    emitted_by_value_column: dict[str, list[RawOutputColumn]] = {}
    all_by_value_column: dict[str, list[RawOutputColumn]] = {}
    dependency_names: list[str] = []
    owner_by_raw_name: dict[str, tuple[str, str, str]] = {}

    for feature in config["features"]:
        feature_id = feature["feature_id"]
        stem = feature_output_stem(feature_id)
        feature_value_column = f"{stem}_value"
        entries: list[RawOutputColumn] = []
        for raw_order, raw_name in enumerate(feature["raw_dependencies"], start=1):
            if raw_name not in raw_columns:
                raise ValueError(f"Raw dependency is missing from column flow: {raw_name}")
            if raw_name not in owner_by_raw_name:
                owner_by_raw_name[raw_name] = (
                    feature_id,
                    feature["display_name"],
                    feature_value_column,
                )
                dependency_names.append(raw_name)
            owner_feature_id, owner_display_name, owner_value_column = owner_by_raw_name[raw_name]
            is_column_emitted_here = owner_feature_id == feature_id
            entries.append(
                RawOutputColumn(
                    feature_id=feature_id,
                    category_id=feature["category_id"],
                    feature_display_name=feature["display_name"],
                    raw_order=raw_order,
                    raw=raw_columns[raw_name],
                    output_column=raw_name,
                    column_owner_feature_id=owner_feature_id,
                    column_owner_feature_display_name=owner_display_name,
                    column_owner_value_column=owner_value_column,
                    is_column_emitted_here=is_column_emitted_here,
                )
            )
        all_by_value_column[feature_value_column] = entries
        emitted = [entry for entry in entries if entry.is_column_emitted_here]
        if emitted:
            emitted_by_value_column[feature_value_column] = emitted

    return emitted_by_value_column, all_by_value_column, dependency_names


def build_output_header(
    input_header: list[str],
    emitted_feature_layout: dict[str, list[RawOutputColumn]],
) -> list[str]:
    output_header: list[str] = []
    found_feature_values: set[str] = set()

    for column in input_header:
        output_header.append(column)
        entries = emitted_feature_layout.get(column)
        if not entries:
            continue
        found_feature_values.add(column)
        for entry in entries:
            output_header.append(entry.output_column)

    missing = sorted(set(emitted_feature_layout) - found_feature_values)
    if missing:
        raise ValueError(
            "Feature value columns are missing from the combined input: " + ", ".join(missing)
        )
    if len(output_header) != len(set(output_header)):
        raise ValueError("Generated output header contains duplicate column names")
    return output_header


def parse_code_values(value_codes: str) -> list[float]:
    codes: list[float] = []
    for match in re.finditer(r"(?:^|,\s*)(-?\d+(?:\.\d+)?)\s*=", value_codes):
        codes.append(float(match.group(1)))
    return codes


def score_source_definitions(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    definitions = config["processing"]["derived_variables"]["scores"]
    score_names = config["processing"]["aggregation_groups"]["derived_score_then_max"]
    return {
        definitions[name]["source_column"]: definitions[name]
        for name in score_names
    }


def preferred_code_ranks(
    raw: RawColumn,
    config: dict[str, Any],
    score_sources: dict[str, dict[str, Any]],
) -> tuple[dict[float, int], dict[int, float]]:
    known_codes = parse_code_values(raw.value_codes)
    if raw.aggregation == "presence_flag_then_max":
        presence_codes = {
            float(code)
            for code in config["processing"]["direct_binary_presence"][raw.name]
        }
        ordered_codes = sorted(presence_codes)
        ordered_codes.extend(code for code in sorted(known_codes) if code not in presence_codes)
    elif raw.aggregation == "row_level_score_input":
        definition = score_sources[raw.name]
        scores = {
            float(code): float(score)
            for code, score in definition["code_scores"].items()
        }
        ordered_codes = sorted(scores, key=lambda code: (-scores[code], code))
    else:
        raise ValueError(f"Cannot build preferred-code ranks for {raw.name}")

    code_to_rank = {code: rank for rank, code in enumerate(ordered_codes)}
    rank_to_code = {rank: code for code, rank in code_to_rank.items()}
    return code_to_rank, rank_to_code


def aggregate_raw_file(
    path: Path,
    dependency_names: list[str],
    raw_columns: dict[str, RawColumn],
    config: dict[str, Any],
    score_sources: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    required_columns = [*GROUP_COLUMNS, *dependency_names]
    header = set(pd.read_csv(path, nrows=0, encoding="utf-8-sig").columns)
    missing = sorted(set(required_columns) - header)
    if missing:
        raise ValueError(f"{path.name} is missing raw columns: {', '.join(missing)}")

    source = pd.read_csv(path, usecols=required_columns, low_memory=False)
    processed: dict[str, pd.Series] = {
        column: source[column] for column in GROUP_COLUMNS
    }
    aggregations: dict[str, str] = {}
    rank_decoders: dict[str, dict[int, float]] = {}

    for name in dependency_names:
        raw = raw_columns[name]
        values = pd.to_numeric(source[name], errors="coerce")
        if raw.aggregation == "sum":
            processed[name] = values.fillna(0)
            aggregations[name] = "sum"
        elif raw.aggregation == "max":
            processed[name] = values.fillna(0)
            aggregations[name] = "max"
        elif raw.aggregation in {
            "min_categorical_code_then_derive",
            "min_categorical_code_then_presence_flag",
        }:
            processed[name] = values
            aggregations[name] = "min"
        elif raw.aggregation in {"presence_flag_then_max", "row_level_score_input"}:
            code_to_rank, rank_to_code = preferred_code_ranks(
                raw, config, score_sources
            )
            processed[name] = values.map(code_to_rank)
            aggregations[name] = "min"
            rank_decoders[name] = rank_to_code
        else:
            raise ValueError(
                f"Unsupported aggregation '{raw.aggregation}' for raw dependency {name}"
            )

    processed_frame = pd.DataFrame(processed).copy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", PerformanceWarning)
        grouped = processed_frame.groupby(
            list(GROUP_COLUMNS),
            as_index=False,
            sort=False,
            dropna=False,
        ).agg(aggregations)

    for name, rank_to_code in rank_decoders.items():
        grouped[name] = grouped[name].map(rank_to_code)

    village_keys = (
        grouped["state_code"].map(format_identifier)
        + ":"
        + grouped["district_code"].map(format_identifier)
        + ":"
        + grouped["sub_district_code"].map(format_identifier)
        + ":"
        + grouped["village_code"].map(format_identifier)
    )
    return pd.concat(
        [
            village_keys.rename("village_key"),
            grouped[dependency_names],
        ],
        axis=1,
    )


def format_identifier(value: Any) -> str:
    if pd.isna(value):
        return ""
    numeric = float(value)
    if numeric.is_integer():
        return str(int(numeric))
    return format(numeric, ".15g")


def format_numeric_value(value: Any, data_type: str) -> str:
    if pd.isna(value):
        return ""
    numeric = float(value)
    if not math.isfinite(numeric):
        return ""
    if data_type == "Integer" and numeric.is_integer():
        return str(int(numeric))
    return format(numeric, ".15g")


def format_raw_value(value: Any, raw: RawColumn, config: dict[str, Any]) -> str:
    if pd.isna(value):
        return ""
    if raw.data_type != "Categorical":
        return format_numeric_value(value, raw.data_type)

    code_key = normalize_code_key(value)
    if raw.value_representation == "standard_binary_availability":
        presence_codes = {
            normalize_code_key(code)
            for code in config["processing"]["direct_binary_presence"][raw.name]
        }
        return "1" if code_key in presence_codes else "0"
    return raw.code_meanings.get(code_key, code_key)


def iter_aggregated_raw_rows(
    raw_dir: Path,
    dependency_names: list[str],
    raw_columns: dict[str, RawColumn],
    config: dict[str, Any],
) -> Iterator[tuple[str, dict[str, str]]]:
    state_files = sorted(raw_dir.glob("*.csv"))
    if not state_files:
        raise FileNotFoundError(f"No raw CSV files found under {raw_dir}")

    score_sources = score_source_definitions(config)
    for path in state_files:
        print(f"Aggregating raw parameters: {path.name}", file=sys.stderr, flush=True)
        grouped = aggregate_raw_file(
            path,
            dependency_names,
            raw_columns,
            config,
            score_sources,
        )
        column_positions = {name: index + 1 for index, name in enumerate(dependency_names)}
        for values in grouped.itertuples(index=False, name=None):
            raw_values = {
                name: format_raw_value(
                    values[column_positions[name]],
                    raw_columns[name],
                    config,
                )
                for name in dependency_names
            }
            yield str(values[0]), raw_values


def valid_input_rows(
    reader: csv.reader,
    input_width: int,
    skipped_rows: list[int],
) -> Iterator[tuple[int, list[str]]]:
    for row_number, row in enumerate(reader, start=2):
        if len(row) != input_width:
            raise ValueError(
                f"Input row {row_number:,} has {len(row)} columns; expected {input_width}"
            )
        if not row or not VALID_VILLAGE_KEY.fullmatch(row[0]):
            skipped_rows.append(row_number)
            continue
        yield row_number, row


def augment_row(
    input_header: list[str],
    input_row: list[str],
    raw_values: dict[str, str],
    emitted_feature_layout: dict[str, list[RawOutputColumn]],
) -> list[str]:
    output_row: list[str] = []
    for column, value in zip(input_header, input_row, strict=True):
        output_row.append(value)
        for entry in emitted_feature_layout.get(column, []):
            output_row.append(raw_values[entry.raw.name])
    return output_row


def write_schema(
    path: Path,
    input_header: list[str],
    all_feature_layout: dict[str, list[RawOutputColumn]],
) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "category_id",
        "feature_id",
        "feature_display_name",
        "feature_value_column",
        "raw_order",
        "raw_parameter",
        "output_column",
        "column_owner_feature_id",
        "column_owner_feature_display_name",
        "column_owner_value_column",
        "is_column_emitted_here",
        "description",
        "data_type",
        "value_codes",
        "code_meanings_json",
        "unit",
        "source_unit",
        "value_representation",
        "aggregation",
    ]
    row_count = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for feature_value_column in input_header:
            for entry in all_feature_layout.get(feature_value_column, []):
                writer.writerow(
                    {
                        "category_id": entry.category_id,
                        "feature_id": entry.feature_id,
                        "feature_display_name": entry.feature_display_name,
                        "feature_value_column": feature_value_column,
                        "raw_order": entry.raw_order,
                        "raw_parameter": entry.raw.name,
                        "output_column": entry.output_column,
                        "column_owner_feature_id": entry.column_owner_feature_id,
                        "column_owner_feature_display_name": entry.column_owner_feature_display_name,
                        "column_owner_value_column": entry.column_owner_value_column,
                        "is_column_emitted_here": str(entry.is_column_emitted_here),
                        "description": entry.raw.description,
                        "data_type": entry.raw.data_type,
                        "value_codes": entry.raw.value_codes,
                        "code_meanings_json": json.dumps(
                            entry.raw.code_meanings,
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        "unit": entry.raw.unit,
                        "source_unit": entry.raw.source_unit,
                        "value_representation": entry.raw.value_representation,
                        "aggregation": entry.raw.aggregation,
                    }
                )
                row_count += 1
    return row_count


def ensure_output_paths(args: argparse.Namespace) -> None:
    for path in (args.output, args.schema_output):
        if path.exists() and not args.overwrite:
            raise FileExistsError(f"{path} already exists; pass --overwrite to replace it")
        path.parent.mkdir(parents=True, exist_ok=True)


def run(args: argparse.Namespace) -> dict[str, Any]:
    ensure_output_paths(args)
    config = load_json(args.mapping_config)
    raw_columns = load_raw_columns_for_config(args.raw_column_flow, config)
    emitted_feature_layout, all_feature_layout, dependency_names = build_feature_layout(
        config,
        raw_columns,
    )
    raw_dir = args.raw_dir or Path(config["source"]["raw_files_dir"])

    temporary_output = args.output.with_suffix(args.output.suffix + ".tmp")
    if temporary_output.exists():
        temporary_output.unlink()

    skipped_rows: list[int] = []
    output_rows = 0
    try:
        with (
            args.input.open(newline="", encoding="utf-8") as input_handle,
            temporary_output.open("w", newline="", encoding="utf-8") as output_handle,
        ):
            reader = csv.reader(input_handle)
            input_header = next(reader)
            output_header = build_output_header(input_header, emitted_feature_layout)
            writer = csv.writer(output_handle)
            writer.writerow(output_header)

            input_rows = valid_input_rows(reader, len(input_header), skipped_rows)
            batch: list[list[str]] = []
            for village_key, raw_values in iter_aggregated_raw_rows(
                raw_dir,
                dependency_names,
                raw_columns,
                config,
            ):
                try:
                    input_row_number, input_row = next(input_rows)
                except StopIteration as exc:
                    raise ValueError(
                        f"Combined input ended before raw village {village_key}"
                    ) from exc
                if input_row[0] != village_key:
                    raise ValueError(
                        "Village order/key mismatch: "
                        f"raw={village_key}, input={input_row[0]} at row {input_row_number:,}"
                    )
                batch.append(
                    augment_row(
                        input_header,
                        input_row,
                        raw_values,
                        emitted_feature_layout,
                    )
                )
                output_rows += 1
                if len(batch) >= WRITE_BATCH_SIZE:
                    writer.writerows(batch)
                    batch.clear()
            if batch:
                writer.writerows(batch)

            try:
                extra_row_number, extra_row = next(input_rows)
            except StopIteration:
                pass
            else:
                raise ValueError(
                    "Combined input contains a valid village absent from the raw aggregation: "
                    f"{extra_row[0]} at row {extra_row_number:,}"
                )

        temporary_output.replace(args.output)
        schema_rows = write_schema(args.schema_output, input_header, all_feature_layout)
    except Exception:
        if temporary_output.exists():
            temporary_output.unlink()
        raise

    return {
        "input": str(args.input),
        "raw_dir": str(raw_dir),
        "output": str(args.output),
        "schema_output": str(args.schema_output),
        "rows": output_rows,
        "input_columns": len(input_header),
        "raw_dependency_occurrences": schema_rows,
        "unique_raw_parameters": len(dependency_names),
        "output_columns": len(output_header),
        "skipped_non_village_input_rows": len(skipped_rows),
        "skipped_input_row_numbers": skipped_rows,
    }


def main() -> int:
    summary = run(parse_args())
    for key, value in summary.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
