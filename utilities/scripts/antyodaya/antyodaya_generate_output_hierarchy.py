#!/usr/bin/env python3
"""Generate human-readable and detailed mappings for the enriched output."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import OrderedDict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = REPO_ROOT / "data" / "antyodaya"
DEFAULT_OUTPUT_CSV = (
    DATA_DIR / "output" / "antyodaya_2020_cluster_analysis_full.csv"
)
DEFAULT_RAW_SCHEMA = (
    DATA_DIR / "output" / "antyodaya_2020_cluster_analysis_full_schema.csv"
)
DEFAULT_MAPPING_CONFIG = DATA_DIR / "mappings" / "antyodaya_variable_mapping.json"
DEFAULT_CLUSTER_METADATA = DATA_DIR / "output" / "antyodaya_cluster_metadata.json"
DEFAULT_CLUSTER_RAW_MAPPING = DATA_DIR / "antyodaya_cluster_raw_mapping.csv"
DEFAULT_YAML_OUTPUT = DATA_DIR / "category_feature_raw_output_mapping.yaml"
DEFAULT_JSON_OUTPUT = DATA_DIR / "category_feature_raw_output_mapping.json"

IDENTIFIER_COLUMNS = [
    {
        "column": "village_key",
        "display_name": "Village Key",
        "data_type": "String",
        "source_column": None,
        "description": (
            "Composite village identifier in "
            "state_code:district_code:sub_district_code:village_id form."
        ),
    },
    {
        "column": "state_code",
        "display_name": "State Code",
        "data_type": "Integer",
        "source_column": "state_code",
        "description": "Mission Antyodaya state code.",
    },
    {
        "column": "district_code",
        "display_name": "District Code",
        "data_type": "Integer",
        "source_column": "district_code",
        "description": "Mission Antyodaya district code.",
    },
    {
        "column": "sub_district_code",
        "display_name": "Sub-district Code",
        "data_type": "Integer",
        "source_column": "sub_district_code",
        "description": "Mission Antyodaya sub-district code.",
    },
    {
        "column": "village_id",
        "display_name": "Village ID",
        "data_type": "Integer",
        "source_column": "village_code",
        "description": "Mission Antyodaya village code, renamed to village_id in this output.",
    },
    {
        "column": "state_name",
        "display_name": "State Name",
        "data_type": "String",
        "source_column": "state_name",
        "description": "State name.",
    },
    {
        "column": "district_name",
        "display_name": "District Name",
        "data_type": "String",
        "source_column": "district_name",
        "description": "District name.",
    },
    {
        "column": "sub_district_name",
        "display_name": "Sub-district Name",
        "data_type": "String",
        "source_column": "sub_district_name",
        "description": "Sub-district name.",
    },
    {
        "column": "village_name",
        "display_name": "Village Name",
        "data_type": "String",
        "source_column": "village_name",
        "description": "Village name.",
    },
    {
        "column": "block_code_nunique",
        "display_name": "Unique Block Code Count",
        "data_type": "Integer",
        "source_column": "block_code",
        "description": "Number of distinct block codes found in raw rows for the village.",
    },
    {
        "column": "block_name",
        "display_name": "Block Name",
        "data_type": "String",
        "source_column": "block_name",
        "description": "First block name retained within the aggregated village record.",
    },
    {
        "column": "gp_code_nunique",
        "display_name": "Unique Gram Panchayat Code Count",
        "data_type": "Integer",
        "source_column": "gp_code",
        "description": "Number of distinct Gram Panchayat codes found for the village.",
    },
    {
        "column": "gp_name",
        "display_name": "Gram Panchayat Name",
        "data_type": "String",
        "source_column": "gp_name",
        "description": "First Gram Panchayat name retained within the aggregated village record.",
    },
]

AGGREGATION_SEMANTICS = {
    "sum": "Sum values across duplicate raw rows for the same village.",
    "max": "Keep the maximum numeric value across duplicate raw rows.",
    "presence_flag_then_max": (
        "Prefer a code representing presence across duplicate raw rows, then retain "
        "the corresponding original categorical code."
    ),
    "row_level_score_input": (
        "Choose the original categorical code whose configured row-level score is highest."
    ),
    "min_categorical_code_then_derive": (
        "Keep the minimum observed categorical code before deriving feature helpers."
    ),
    "min_categorical_code_then_presence_flag": (
        "Keep the minimum observed categorical code before deriving a presence flag."
    ),
}

VALUE_REPRESENTATION_DESCRIPTIONS = {
    "numeric": "Numeric value aggregated at village level.",
    "standard_binary_availability": "Standardized availability flag: 1 = available, 0 = unavailable.",
    "decoded_categorical_label": "Human-readable categorical label decoded from the retained raw code.",
}

SAFE_YAML_SCALAR = re.compile(r"^[A-Za-z_][A-Za-z0-9_./-]*$")
YAML_RESERVED = {
    "null",
    "true",
    "false",
    "yes",
    "no",
    "on",
    "off",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a compact YAML hierarchy and detailed JSON data dictionary "
            "for the Antyodaya raw-parameter output."
        )
    )
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--raw-schema", type=Path, default=DEFAULT_RAW_SCHEMA)
    parser.add_argument("--mapping-config", type=Path, default=DEFAULT_MAPPING_CONFIG)
    parser.add_argument("--cluster-metadata", type=Path, default=DEFAULT_CLUSTER_METADATA)
    parser.add_argument(
        "--cluster-raw-mapping",
        type=Path,
        default=DEFAULT_CLUSTER_RAW_MAPPING,
    )
    parser.add_argument("--yaml-output", type=Path, default=DEFAULT_YAML_OUTPUT)
    parser.add_argument("--json-output", type=Path, default=DEFAULT_JSON_OUTPUT)
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def output_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as handle:
        return next(csv.reader(handle))


def relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path.resolve())


def feature_stem(feature_id: str) -> str:
    if not feature_id.endswith("_feature"):
        raise ValueError(f"Unexpected feature ID: {feature_id}")
    return feature_id[: -len("_feature")] + "_feat"


def category_output_columns(category_id: str) -> dict[str, dict[str, Any]]:
    return {
        "cluster": {
            "column": f"{category_id}_cat_cluster",
            "data_type": "String",
            "values": ["Low", "Medium", "High"],
            "description": "Final qualitative category cluster.",
        },
        "value": {
            "column": f"{category_id}_cat_value",
            "data_type": "Float",
            "range": [0.0, 1.0],
            "description": "Normalized category value before final class assignment.",
        },
    }


def feature_output_columns(
    feature_id: str,
    display_name: str,
) -> dict[str, dict[str, Any]]:
    stem = feature_stem(feature_id)
    return {
        "cluster": {
            "column": f"{stem}_cluster",
            "data_type": "String",
            "values": ["Low", "Medium", "High"],
            "description": f"Final qualitative cluster for {display_name}.",
        },
        "value": {
            "column": f"{stem}_value",
            "data_type": "Float",
            "range": [0.0, 1.0],
            "description": f"Normalized 0-1 feature value for {display_name}.",
        },
    }


def parse_code_meanings(value_codes: str) -> dict[str, str]:
    if not value_codes:
        return {}
    matches = re.finditer(
        r"(?:^|,\s*)(-?\d+(?:\.\d+)?)\s*=\s*(.*?)(?=,\s*-?\d+(?:\.\d+)?\s*=|$)",
        value_codes,
    )
    return OrderedDict((match.group(1), match.group(2).strip()) for match in matches)


def parse_json_mapping(value: str) -> dict[str, str]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return OrderedDict((str(key), str(item)) for key, item in parsed.items())


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def raw_output_data_type(row: dict[str, str]) -> str:
    representation = row.get("value_representation", "")
    if representation == "standard_binary_availability":
        return "Integer"
    if representation == "decoded_categorical_label":
        return "String"
    return row["data_type"]


def calculation_lines(value: str) -> list[tuple[str, str]]:
    parsed: list[tuple[str, str]] = []
    for line in value.splitlines():
        line = line.strip()
        match = re.match(r"CALC::([^ ]+)\s*:=", line)
        if match:
            parsed.append((match.group(1), line))
    return parsed


def calculations_for_feature(
    calculations: list[tuple[str, str]],
    feature: dict[str, Any],
) -> list[str]:
    symbols = {
        feature["feature_id"],
        *feature.get("derived_dependencies", []),
    }
    return [line for symbol, line in calculations if symbol in symbols]


def category_calculation(
    calculations: list[tuple[str, str]],
    category_column: str,
) -> str:
    for symbol, line in calculations:
        if symbol == category_column:
            return line
    return (
        f"CALC::{category_column} := mean(feature class scores); "
        "CLASS_SCORE::Low=0, Medium=0.5, High=1"
    )


def raw_trail_for_feature(raw_trail: str, feature_id: str) -> list[str]:
    return [
        line.strip()
        for line in raw_trail.splitlines()
        if line.strip() and f"FEATURE::{feature_id}" in line
    ]


def cluster_details(
    metadata_by_id: dict[str, dict[str, Any]],
    key: str,
) -> dict[str, Any]:
    row = metadata_by_id.get(key, {})
    return {
        "cluster_count": row.get("n_clusters"),
        "label_distribution": row.get("label_distribution", {}),
        "variance": row.get("variance"),
        "shannon_entropy_bits": row.get("shannon_entropy_bits"),
    }


def build_mapping(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    header = output_header(args.output_csv)
    config = load_json(args.mapping_config)
    cluster_metadata = load_json(args.cluster_metadata)
    raw_schema_rows = load_csv(args.raw_schema)
    raw_mapping_rows = load_csv(args.cluster_raw_mapping)

    raw_schema_by_feature: dict[str, list[dict[str, str]]] = {}
    for row in raw_schema_rows:
        raw_schema_by_feature.setdefault(row["feature_id"], []).append(row)
    for rows in raw_schema_by_feature.values():
        rows.sort(key=lambda row: int(row["raw_order"]))

    raw_mapping_by_category = {
        row["category"]: row
        for row in raw_mapping_rows
    }
    features_by_id = {
        feature["feature_id"]: feature
        for feature in config["features"]
    }
    feature_cluster_by_id = {
        row["feature_id"]: row
        for row in cluster_metadata["feature_cluster_columns"]
    }
    category_cluster_by_id = {
        row["category_id"]: row
        for row in cluster_metadata["category_cluster_columns"]
    }

    represented_columns = [item["column"] for item in IDENTIFIER_COLUMNS]
    detailed_categories: list[dict[str, Any]] = []
    concise_categories: list[dict[str, Any]] = []
    unique_raw_parameters: set[str] = set()

    for category_order, category in enumerate(config["categories"], start=1):
        category_id = category["category_id"]
        display_name = category["display_name"]
        category_columns = category_output_columns(category_id)
        represented_columns.extend(
            [
                category_columns["cluster"]["column"],
                category_columns["value"]["column"],
            ]
        )

        raw_mapping = raw_mapping_by_category.get(display_name, {})
        calculations = calculation_lines(raw_mapping.get("calculations_formulae", ""))
        detailed_features: list[dict[str, Any]] = []
        concise_features: list[dict[str, Any]] = []
        category_column_sequence = [
            category_columns["cluster"]["column"],
            category_columns["value"]["column"],
        ]
        category_raw_parameters: list[str] = []

        for feature_order, feature_id in enumerate(category["feature_ids"], start=1):
            feature = features_by_id[feature_id]
            feature_columns = feature_output_columns(feature_id, feature["display_name"])
            feature_sequence = [
                feature_columns["cluster"]["column"],
                feature_columns["value"]["column"],
            ]
            represented_columns.extend(feature_sequence)

            raw_parameters: list[dict[str, Any]] = []
            concise_raw_parameters: list[dict[str, Any]] = []
            schema_rows = raw_schema_by_feature.get(feature_id, [])
            expected_dependencies = feature.get("raw_dependencies", [])
            actual_dependencies = [row["raw_parameter"] for row in schema_rows]
            if actual_dependencies != expected_dependencies:
                raise ValueError(
                    f"Raw schema order differs from mapping for {feature_id}: "
                    f"{actual_dependencies} != {expected_dependencies}"
                )

            for row in schema_rows:
                value_column = row["output_column"]
                emitted_here = parse_bool(row.get("is_column_emitted_here", ""))
                if emitted_here:
                    feature_sequence.append(value_column)
                    represented_columns.append(value_column)
                raw_name = row["raw_parameter"]
                unique_raw_parameters.add(raw_name)
                if raw_name not in category_raw_parameters:
                    category_raw_parameters.append(raw_name)

                code_meanings = parse_json_mapping(
                    row.get("code_meanings_json", "")
                ) or parse_code_meanings(row["value_codes"])
                value_representation = row.get("value_representation", "")
                unit = row.get("unit") or None
                raw_detail = {
                    "raw_order": int(row["raw_order"]),
                    "parameter": raw_name,
                    "description": row["description"],
                    "data_type": row["data_type"],
                    "output_data_type": raw_output_data_type(row),
                    "unit": unit,
                    "source_unit": row.get("source_unit") or None,
                    "value_representation": value_representation,
                    "value_representation_description": (
                        VALUE_REPRESENTATION_DESCRIPTIONS.get(
                            value_representation,
                            value_representation,
                        )
                    ),
                    "value_codes": row["value_codes"] or None,
                    "code_meanings": code_meanings,
                    "aggregation": row["aggregation"],
                    "aggregation_semantics": AGGREGATION_SEMANTICS.get(
                        row["aggregation"],
                        row["aggregation"],
                    ),
                    "output_column": {
                        "column": value_column,
                        "data_type": raw_output_data_type(row),
                        "unit": unit,
                        "emitted_at_this_feature": emitted_here,
                        "stored_under_feature_id": row["column_owner_feature_id"],
                        "stored_under_feature_name": row[
                            "column_owner_feature_display_name"
                        ],
                        "stored_after_feature_value_column": row[
                            "column_owner_value_column"
                        ],
                        "description": (
                            f"Village-level aggregated raw value for {raw_name}."
                        ),
                    },
                }
                raw_parameters.append(raw_detail)

                concise_raw = {
                    "parameter": raw_name,
                    "column": value_column,
                    "stored_under_feature": row["column_owner_feature_id"],
                    "emitted_here": emitted_here,
                    "value_representation": value_representation,
                }
                if unit:
                    concise_raw["unit"] = unit
                if code_meanings:
                    concise_raw["codes"] = code_meanings
                concise_raw_parameters.append(concise_raw)

            category_column_sequence.extend(feature_sequence)
            detailed_feature = {
                "feature_order": feature_order,
                "feature_id": feature_id,
                "display_name": feature["display_name"],
                "description": feature.get("description", ""),
                "method": feature.get("method_label", feature.get("method")),
                "inverse": bool(feature.get("inverse", False)),
                "output_columns": feature_columns,
                "output_column_sequence": feature_sequence,
                "input_variables": feature.get("input_variables", []),
                "derived_variables": feature.get("derived_dependencies", []),
                "calculations": calculations_for_feature(calculations, feature),
                "raw_to_category_trail": raw_trail_for_feature(
                    raw_mapping.get("raw_to_category_maps", ""),
                    feature_id,
                ),
                "cluster_statistics": cluster_details(
                    feature_cluster_by_id,
                    feature_id,
                ),
                "raw_parameters": raw_parameters,
            }
            detailed_features.append(detailed_feature)
            concise_features.append(
                {
                    "feature_id": feature_id,
                    "name": feature["display_name"],
                    "method": detailed_feature["method"],
                    "columns": {
                        "cluster": feature_columns["cluster"]["column"],
                        "value": feature_columns["value"]["column"],
                    },
                    "raw_parameters": concise_raw_parameters,
                }
            )

        cluster_rule = category.get("category_cluster_rule")
        detailed_categories.append(
            {
                "category_order": category_order,
                "category_id": category_id,
                "display_name": display_name,
                "description": (
                    f"{display_name} category value is the equal-weight mean of its "
                    "feature class scores, where Low=0, Medium=0.5, and High=1."
                ),
                "output_columns": category_columns,
                "output_column_sequence": category_column_sequence,
                "calculation": category_calculation(
                    calculations,
                    category["category_column"],
                ),
                "cluster_rule": cluster_rule
                or {
                    "n_clusters": 3,
                    "reason": (
                        "Default three-class clustering when the observed data "
                        "supports Low, Medium, and High classes."
                    ),
                },
                "cluster_statistics": cluster_details(
                    category_cluster_by_id,
                    category_id,
                ),
                "village_cluster_share": raw_mapping.get(
                    "village_cluster_share",
                    "",
                ),
                "raw_parameters": category_raw_parameters,
                "features": detailed_features,
            }
        )
        concise_categories.append(
            {
                "category_id": category_id,
                "name": display_name,
                "columns": {
                    "cluster": category_columns["cluster"]["column"],
                    "value": category_columns["value"]["column"],
                },
                "features": concise_features,
            }
        )

    if represented_columns != header:
        mismatch_index = next(
            (
                index
                for index, (represented, actual) in enumerate(
                    zip(represented_columns, header)
                )
                if represented != actual
            ),
            min(len(represented_columns), len(header)),
        )
        represented = (
            represented_columns[mismatch_index]
            if mismatch_index < len(represented_columns)
            else "<missing>"
        )
        actual = header[mismatch_index] if mismatch_index < len(header) else "<missing>"
        raise ValueError(
            "Hierarchy does not match the enriched output header at position "
            f"{mismatch_index + 1}: hierarchy={represented}, output={actual}"
        )

    metadata = {
        "schema_version": "1.0",
        "dataset": "Mission Antyodaya 2020 feature/category output with lean raw parameters",
        "output_file": relative_path(args.output_csv),
        "row_grain": "One aggregated Mission Antyodaya record per village.",
        "record_count": cluster_metadata["input"]["aggregated_row_count"],
        "column_count": len(header),
        "identifier_column_count": len(IDENTIFIER_COLUMNS),
        "category_count": len(config["categories"]),
        "feature_count": len(config["features"]),
        "raw_dependency_occurrences": len(raw_schema_rows),
        "unique_raw_parameter_count": len(unique_raw_parameters),
        "hierarchy": "identifiers -> categories -> features -> raw parameters",
        "column_order": (
            "Identifier columns come first. Each category then contains category "
            "cluster/value columns followed by each feature's cluster/value columns "
            "and raw columns first introduced by that feature. Shared raw "
            "dependencies are stored once and referenced by later features."
        ),
        "source_files": [
            relative_path(args.mapping_config),
            relative_path(args.cluster_metadata),
            relative_path(args.cluster_raw_mapping),
            relative_path(args.raw_schema),
        ],
    }

    detailed = {
        "metadata": metadata,
        "column_conventions": {
            "*_cat_cluster": "Qualitative category class.",
            "*_cat_value": "Normalized category value on a 0-1 scale.",
            "*_feat_cluster": "Qualitative feature class.",
            "*_feat_value": "Normalized feature value on a 0-1 scale.",
            "<raw_parameter_name>": (
                "Aggregated raw parameter column, directly named from the raw "
                "source variable and emitted once at its first feature dependency."
            ),
        },
        "identifier_columns": IDENTIFIER_COLUMNS,
        "categories": detailed_categories,
        "output_column_sequence": header,
    }
    concise = {
        "schema_version": metadata["schema_version"],
        "dataset": metadata["dataset"],
        "output_file": metadata["output_file"],
        "row_grain": metadata["row_grain"],
        "record_count": metadata["record_count"],
        "column_count": metadata["column_count"],
        "hierarchy": metadata["hierarchy"],
        "identifier_columns": [
            {
                "column": row["column"],
                "meaning": row["display_name"],
            }
            for row in IDENTIFIER_COLUMNS
        ],
        "categories": concise_categories,
    }
    return concise, detailed


def yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if (
        SAFE_YAML_SCALAR.fullmatch(text)
        and text.lower() not in YAML_RESERVED
    ):
        return text
    return json.dumps(text, ensure_ascii=False)


def is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def emit_yaml(value: Any, indent: int = 0) -> list[str]:
    prefix = " " * indent
    if isinstance(value, dict):
        lines: list[str] = []
        for key, item in value.items():
            if is_scalar(item):
                lines.append(f"{prefix}{key}: {yaml_scalar(item)}")
            elif not item:
                empty = "[]" if isinstance(item, list) else "{}"
                lines.append(f"{prefix}{key}: {empty}")
            else:
                lines.append(f"{prefix}{key}:")
                lines.extend(emit_yaml(item, indent + 2))
        return lines
    if isinstance(value, list):
        lines = []
        for item in value:
            if is_scalar(item):
                lines.append(f"{prefix}- {yaml_scalar(item)}")
                continue
            if isinstance(item, dict) and item:
                entries = list(item.items())
                first_key, first_value = entries[0]
                if is_scalar(first_value):
                    lines.append(
                        f"{prefix}- {first_key}: {yaml_scalar(first_value)}"
                    )
                else:
                    lines.append(f"{prefix}- {first_key}:")
                    lines.extend(emit_yaml(first_value, indent + 4))
                for key, nested in entries[1:]:
                    if is_scalar(nested):
                        lines.append(
                            f"{' ' * (indent + 2)}{key}: {yaml_scalar(nested)}"
                        )
                    elif not nested:
                        empty = "[]" if isinstance(nested, list) else "{}"
                        lines.append(f"{' ' * (indent + 2)}{key}: {empty}")
                    else:
                        lines.append(f"{' ' * (indent + 2)}{key}:")
                        lines.extend(emit_yaml(nested, indent + 4))
                continue
            lines.append(f"{prefix}-")
            lines.extend(emit_yaml(item, indent + 2))
        return lines
    return [f"{prefix}{yaml_scalar(value)}"]


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    args = parse_args()
    concise, detailed = build_mapping(args)
    yaml_text = "\n".join(emit_yaml(concise)) + "\n"
    json_text = json.dumps(detailed, ensure_ascii=False, indent=2) + "\n"
    write_text_atomic(args.yaml_output, yaml_text)
    write_text_atomic(args.json_output, json_text)

    print(f"yaml_output: {args.yaml_output}")
    print(f"json_output: {args.json_output}")
    print(f"columns_documented: {detailed['metadata']['column_count']}")
    print(f"categories: {detailed['metadata']['category_count']}")
    print(f"features: {detailed['metadata']['feature_count']}")
    print(
        "raw_dependency_occurrences: "
        f"{detailed['metadata']['raw_dependency_occurrences']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
