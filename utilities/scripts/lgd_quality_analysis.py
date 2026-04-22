#!/usr/bin/env python3
"""Analyze raw LGD JSON dumps for datatype quality and hierarchy consistency.

This script is intentionally focused on the source files in `data/lgd/lgd_*.json`.
It answers questions such as:

- are numeric identifiers stored as ints, strings, or `*.0` strings?
- how many raw rows collapse to unique codes after cleaning?
- where do parent hierarchy files lag the village file?
- how well does the panchayat mapping table join to villages?

The output is a JSON report and an optional Markdown summary.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import re
import sys
from typing import Any, Iterable


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utilities.scripts.admin_resolve import (  # noqa: E402
    DEFAULT_LGD_DIR,
    LGDHierarchyIndex,
    clean_text,
    iter_json_array,
    load_small_json_rows,
    normalize_code_text,
    parse_int,
    utc_now_iso,
)


FILE_CONFIG = {
    "lgd_state.json": {
        "row_loader": "small",
        "primary_code_field": "state_code",
        "code_fields": ["state_code", "state_census2011_code"],
    },
    "lgd_district.json": {
        "row_loader": "small",
        "primary_code_field": "district_code",
        "code_fields": [
            "state_code",
            "state_census2011_code",
            "district_code",
            "district_census2011_code",
        ],
    },
    "lgd_subdistrict.json": {
        "row_loader": "small",
        "primary_code_field": "subdistrict_code",
        "code_fields": [
            "state_code",
            "state_census2011_code",
            "district_code",
            "district_census2011_code",
            "subdistrict_code",
            "subdistrict_census2011_code",
        ],
    },
    "lgd_village.json": {
        "row_loader": "stream",
        "primary_code_field": "villageCode",
        "code_fields": [
            "stateCode",
            "stateCensus2011Code",
            "districtCode",
            "districtCensus2011Code",
            "subdistrictCode",
            "subdistrictCensus2011Code",
            "villageCode",
            "villageCensus2011Code",
        ],
    },
    "lgd_panchayat.json": {
        "row_loader": "stream",
        "primary_code_field": "localBodyCode",
        "code_fields": [
            "stateCode",
            "stateCensus2011Code",
            "localBodyCode",
            "localBodyCensus2011Code",
            "localBodyTypeCode",
            "entityCode",
        ],
    },
}


FLOAT_LIKE_RE = re.compile(r"^-?\d+\.0$")
INT_LIKE_RE = re.compile(r"^-?\d+$")


def get_rows(path: Path, loader: str) -> Iterable[dict[str, Any]]:
    if loader == "small":
        return load_small_json_rows(path)
    return iter_json_array(path)


def raw_value_style(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float_integer" if value.is_integer() else "float_non_integer"
    text = str(value).strip()
    if text == "":
        return "blank"
    if FLOAT_LIKE_RE.fullmatch(text):
        return "str_float_integer"
    if INT_LIKE_RE.fullmatch(text):
        return "str_integer"
    return "other"


def analyze_file(path: Path, config: dict[str, Any]) -> dict[str, Any]:
    primary_code_field = config["primary_code_field"]
    code_fields = config["code_fields"]
    row_count = 0
    seen_codes: dict[int, tuple[Any, ...]] = {}
    duplicate_rows = 0
    conflicting_duplicate_rows = 0
    code_field_counters = {field: Counter() for field in code_fields}

    for row in get_rows(path, config["row_loader"]):
        row_count += 1
        for field in code_fields:
            raw = row.get(field)
            style = raw_value_style(raw)
            code_field_counters[field][style] += 1
            normalized = normalize_code_text(raw)
            if normalized is not None:
                code_field_counters[field]["normalized_non_empty"] += 1
            parsed = parse_int(raw)
            if parsed is not None:
                code_field_counters[field]["coercible_int"] += 1
            elif normalized is not None:
                code_field_counters[field]["non_integer_non_empty"] += 1

        primary_code = parse_int(row.get(primary_code_field))
        if primary_code is None:
            continue
        signature = tuple(row.get(key) for key in sorted(row.keys()))
        existing = seen_codes.get(primary_code)
        if existing is not None:
            duplicate_rows += 1
            if existing != signature:
                conflicting_duplicate_rows += 1
        else:
            seen_codes[primary_code] = signature

    return {
        "path": str(path),
        "rows": row_count,
        "unique_primary_codes": len(seen_codes),
        "duplicate_primary_code_rows": duplicate_rows,
        "conflicting_duplicate_primary_code_rows": conflicting_duplicate_rows,
        "primary_code_field": primary_code_field,
        "code_field_profiles": {
            field: dict(counter) for field, counter in code_field_counters.items()
        },
    }


def sample_missing_parents(index: LGDHierarchyIndex, limit: int = 25) -> dict[str, list[dict[str, Any]]]:
    canonical_district_codes = {
        parse_int(row.get("districtCode"))
        for row in iter_json_array(DEFAULT_LGD_DIR / "lgd_village.json")
        if parse_int(row.get("districtCode")) is not None
    }
    canonical_subdistrict_codes = {
        parse_int(row.get("subdistrictCode"))
        for row in iter_json_array(DEFAULT_LGD_DIR / "lgd_village.json")
        if parse_int(row.get("subdistrictCode")) is not None
    }
    raw_district_codes = {record.code for record in index.districts_by_code.values()}
    raw_subdistrict_codes = {record.code for record in index.subdistricts_by_code.values()}

    missing_districts = []
    missing_subdistricts = []
    raw_district_table_codes = {
        parse_int(row.get("district_code") or row.get("districtCode"))
        for row in load_small_json_rows(DEFAULT_LGD_DIR / "lgd_district.json")
    }
    raw_subdistrict_table_codes = {
        parse_int(row.get("subdistrict_code") or row.get("subdistrictCode"))
        for row in load_small_json_rows(DEFAULT_LGD_DIR / "lgd_subdistrict.json")
    }

    for code in sorted(canonical_district_codes - raw_district_table_codes):
        if code is None:
            continue
        record = index.districts_by_code.get(code)
        if record and len(missing_districts) < limit:
            missing_districts.append(
                {
                    "district_code": code,
                    "district_name_english": record.name_english,
                    "state_code": record.state_code,
                }
            )

    for code in sorted(canonical_subdistrict_codes - raw_subdistrict_table_codes):
        if code is None:
            continue
        record = index.subdistricts_by_code.get(code)
        if record and len(missing_subdistricts) < limit:
            missing_subdistricts.append(
                {
                    "subdistrict_code": code,
                    "subdistrict_name_english": record.name_english,
                    "district_code": record.district_code,
                    "state_code": record.state_code,
                }
            )

    return {
        "districts_missing_from_parent_table_but_present_in_villages": missing_districts,
        "subdistricts_missing_from_parent_table_but_present_in_villages": missing_subdistricts,
        "district_count": len(canonical_district_codes - raw_district_table_codes),
        "subdistrict_count": len(canonical_subdistrict_codes - raw_subdistrict_table_codes),
    }


def analyze_panchayat_links(index: LGDHierarchyIndex) -> dict[str, Any]:
    entity_type_counter: Counter[str] = Counter()
    local_body_type_counter: Counter[str] = Counter()
    village_entity_rows = 0
    village_entity_rows_matched = 0
    unmatched_examples: list[dict[str, Any]] = []

    for row in iter_json_array(DEFAULT_LGD_DIR / "lgd_panchayat.json"):
        entity_type = clean_text(row.get("entityType")) or ""
        entity_type_counter[entity_type] += 1
        local_body_type = clean_text(row.get("localBodyTypeName")) or ""
        local_body_type_counter[local_body_type] += 1

        if entity_type != "Village":
            continue
        village_entity_rows += 1
        entity_code = parse_int(row.get("entityCode"))
        if entity_code is not None and entity_code in index.villages_by_code:
            village_entity_rows_matched += 1
        elif len(unmatched_examples) < 20:
            unmatched_examples.append(
                {
                    "entityCode": row.get("entityCode"),
                    "entityName": row.get("entityName"),
                    "localBodyCode": row.get("localBodyCode"),
                    "localBodyNameEnglish": row.get("localBodyNameEnglish"),
                    "localBodyTypeName": row.get("localBodyTypeName"),
                    "coverageType": row.get("coverageType"),
                }
            )

    return {
        "entity_type_distribution": dict(entity_type_counter),
        "local_body_type_distribution_top_20": dict(local_body_type_counter.most_common(20)),
        "village_entity_rows": village_entity_rows,
        "village_entity_rows_matched_to_cleaned_village_codes": village_entity_rows_matched,
        "village_entity_match_rate": round(village_entity_rows_matched / village_entity_rows, 6)
        if village_entity_rows
        else None,
        "unmatched_village_entity_examples": unmatched_examples,
    }


def build_report(lgd_dir: Path) -> dict[str, Any]:
    index = LGDHierarchyIndex.from_disk(lgd_dir)
    files = {
        name: analyze_file(lgd_dir / name, config)
        for name, config in FILE_CONFIG.items()
    }
    report = {
        "generated_at": utc_now_iso(),
        "lgd_dir": str(lgd_dir),
        "cleaned_index_summary": index.metadata(),
        "file_profiles": files,
        "hierarchy_parent_gap_analysis": sample_missing_parents(index),
        "panchayat_link_analysis": analyze_panchayat_links(index),
        "conclusions": [
            "The loader coerces float-like integer strings such as '645017.0' to integer 645017 before matching.",
            "Invalid village codes in downstream audits are therefore reference-gap issues or source-conflict issues, not a '*.0' parsing issue.",
            "The village file is the most complete hierarchy source and should be treated as the canonical fallback for missing district/subdistrict parent codes.",
            "VillageCode is usable as the primary operational identifier only after canonical de-duplication because raw LGD village rows contain duplicate and conflicting entries.",
        ],
    }
    return report


def render_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# LGD Raw Source Analysis")
    lines.append("")
    lines.append(f"Generated: `{report['generated_at']}`")
    lines.append("")
    lines.append("## Key Conclusions")
    lines.append("")
    for item in report["conclusions"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## Cleaned Index Summary")
    lines.append("")
    for key, value in sorted(report["cleaned_index_summary"].items()):
        lines.append(f"- `{key}`: `{value}`")
    lines.append("")
    lines.append("## File Profiles")
    lines.append("")
    for name, profile in report["file_profiles"].items():
        lines.append(f"### `{name}`")
        lines.append("")
        lines.append(f"- Rows: `{profile['rows']}`")
        lines.append(f"- Unique primary codes: `{profile['unique_primary_codes']}`")
        lines.append(f"- Duplicate primary-code rows: `{profile['duplicate_primary_code_rows']}`")
        lines.append(
            f"- Conflicting duplicate primary-code rows: `{profile['conflicting_duplicate_primary_code_rows']}`"
        )
        lines.append(f"- Primary code field: `{profile['primary_code_field']}`")
        lines.append("")
        lines.append("| Code field | Coercible int | str `*.0` | str int | int | float int | non-integer non-empty |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for field, counters in sorted(profile["code_field_profiles"].items()):
            lines.append(
                "| "
                f"`{field}` | "
                f"{counters.get('coercible_int', 0)} | "
                f"{counters.get('str_float_integer', 0)} | "
                f"{counters.get('str_integer', 0)} | "
                f"{counters.get('int', 0)} | "
                f"{counters.get('float_integer', 0)} | "
                f"{counters.get('non_integer_non_empty', 0)} |"
            )
        lines.append("")
    lines.append("## Parent Gaps")
    lines.append("")
    gap_report = report["hierarchy_parent_gap_analysis"]
    lines.append(
        f"- Districts present in villages but absent from raw district table: `{gap_report['district_count']}`"
    )
    lines.append(
        f"- Subdistricts present in villages but absent from raw subdistrict table: `{gap_report['subdistrict_count']}`"
    )
    lines.append("")
    lines.append("## Panchayat Link Analysis")
    lines.append("")
    panchayat = report["panchayat_link_analysis"]
    lines.append(f"- Village entity rows: `{panchayat['village_entity_rows']}`")
    lines.append(
        "- Matched to cleaned village codes: "
        f"`{panchayat['village_entity_rows_matched_to_cleaned_village_codes']}`"
    )
    lines.append(f"- Match rate: `{panchayat['village_entity_match_rate']}`")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze raw LGD JSON dumps.")
    parser.add_argument(
        "--lgd-dir",
        default=str(DEFAULT_LGD_DIR),
        help=f"Directory containing lgd_*.json files. Default: {DEFAULT_LGD_DIR}",
    )
    parser.add_argument(
        "--out-json",
        default="data/lgd/lgd_source_analysis.json",
        help="Path for the JSON report.",
    )
    parser.add_argument(
        "--out-md",
        default="data/lgd/lgd_source_analysis.md",
        help="Path for the Markdown report.",
    )
    args = parser.parse_args(argv)

    lgd_dir = Path(args.lgd_dir)
    report = build_report(lgd_dir)

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    out_md = Path(args.out_md)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(render_markdown(report), encoding="utf-8")

    print(f"JSON report written to {out_json}")
    print(f"Markdown report written to {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
