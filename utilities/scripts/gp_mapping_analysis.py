#!/usr/bin/env python3
"""Analyze gp_mapping CSV quality against lgd_panchayat.json and canonical LGD villages."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
import sqlite3
import sys
from typing import Any, Iterable


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utilities.scripts.admin_resolve import clean_text, iter_json_array, parse_int, utc_now_iso  # noqa: E402


DEFAULT_LGD_DIR = Path("data/lgd")
DEFAULT_HYBRID_SQLITE_PATH = DEFAULT_LGD_DIR / "admin_lookup_hybrid.sqlite3"
DEFAULT_OUT_JSON = DEFAULT_LGD_DIR / "gp_mapping_analysis.json"
DEFAULT_OUT_MD = DEFAULT_LGD_DIR / "gp_mapping_analysis.md"


CSV_COLUMN_MAP = {
    "S.No.": "serial_no",
    "District Code": "district_code",
    "District Name (In English)": "district_name_english",
    "District Census 2011 Code": "district_census_2011_code",
    "District Census 2001 Code": "district_census_2001_code",
    "Subdistrict Code": "subdistrict_code",
    "Subdistrict Name (In English)": "subdistrict_name_english",
    "Subdistrict Census 2011 Code": "subdistrict_census_2011_code",
    "Subdistrict Census 2001 Code": "subdistrict_census_2001_code",
    "Village Code": "village_code",
    "Village Name (In English)": "village_name_english",
    "Village Census 2011 Code": "village_census_2011_code",
    "Village Census 2001 Code": "village_census_2001_code",
    "Local Body Code": "local_body_code",
    "Local Body Name (In English)": "local_body_name_english",
    "State Code": "state_code",
    "State Name": "state_name",
}


def normalize_gp_mapping_row(row: dict[str, str]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for source_key, target_key in CSV_COLUMN_MAP.items():
        value = row.get(source_key)
        if target_key.endswith("_code") or target_key in {"serial_no"}:
            normalized[target_key] = parse_int(value)
        else:
            normalized[target_key] = clean_text(value)
    return normalized


def render_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# GP Mapping Analysis")
    lines.append("")
    lines.append(f"Generated: `{report['generated_at']}`")
    lines.append("")
    lines.append("## Recommendation")
    lines.append("")
    lines.append(f"- decision: `{report['recommendation']['decision']}`")
    lines.append(f"- rationale: {report['recommendation']['rationale']}")
    lines.append("")
    lines.append("## Topline Comparison")
    lines.append("")
    for label in ("gp_mapping_csv", "lgd_panchayat_json"):
        source = report["sources"][label]
        lines.append(f"### {label}")
        lines.append("")
        for key in (
            "rows_total",
            "rows_with_nonzero_local_body_code",
            "link_rows",
            "link_rows_matched_to_canonical_villages",
            "matched_village_count",
            "unique_local_body_codes",
            "rows_with_nonzero_local_body_code_but_missing_name",
        ):
            if key in source:
                lines.append(f"- `{key}`: `{source[key]}`")
        if "link_match_rate" in source:
            lines.append(f"- `link_match_rate`: `{source['link_match_rate']}`")
        lines.append("")
    lines.append("## Statewise Coverage Gaps")
    lines.append("")
    for label in ("gp_mapping_csv", "lgd_panchayat_json"):
        gaps = report["statewise"].get(label, {}).get("states_with_zero_named_local_body_rows", [])
        if gaps:
            lines.append(f"- `{label}` zero-named/nonzero-local-body states: {', '.join(gaps[:20])}")
        else:
            lines.append(f"- `{label}` zero-named/nonzero-local-body states: none")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    for note in report["notes"]:
        lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)


def build_canonical_village_set(sqlite_path: Path) -> set[int]:
    connection = sqlite3.connect(sqlite_path)
    try:
        return {row[0] for row in connection.execute("SELECT village_code FROM villages")}
    finally:
        connection.close()


def analyze_gp_mapping(csv_path: Path, canonical_villages: set[int]) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    state_named_rows: Counter[str] = Counter()
    state_linked_rows: Counter[str] = Counter()
    matched_villages: set[int] = set()
    local_body_codes: set[int] = set()
    missing_name_examples: list[dict[str, Any]] = []
    conflicting_name_by_code: defaultdict[int, set[str]] = defaultdict(set)
    conflicting_state_by_code: defaultdict[int, set[int]] = defaultdict(set)

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            row = normalize_gp_mapping_row(raw_row)
            counts["rows_total"] += 1
            state_name = row["state_name"] or "<missing>"
            local_body_code = row["local_body_code"]
            village_code = row["village_code"]
            local_body_name = row["local_body_name_english"]

            if local_body_code is not None and local_body_code != 0:
                counts["rows_with_nonzero_local_body_code"] += 1
                local_body_codes.add(local_body_code)
                conflicting_state_by_code[local_body_code].add(row["state_code"] or -1)
                if local_body_name:
                    state_named_rows[state_name] += 1
                    conflicting_name_by_code[local_body_code].add(local_body_name)
                else:
                    counts["rows_with_nonzero_local_body_code_but_missing_name"] += 1
                    if len(missing_name_examples) < 20:
                        missing_name_examples.append(row)

            if local_body_code is not None and local_body_code != 0 and village_code is not None:
                counts["link_rows"] += 1
                if village_code in canonical_villages:
                    counts["link_rows_matched_to_canonical_villages"] += 1
                    matched_villages.add(village_code)
                    state_linked_rows[state_name] += 1

    return {
        "rows_total": counts["rows_total"],
        "rows_with_nonzero_local_body_code": counts["rows_with_nonzero_local_body_code"],
        "rows_with_nonzero_local_body_code_but_missing_name": counts[
            "rows_with_nonzero_local_body_code_but_missing_name"
        ],
        "link_rows": counts["link_rows"],
        "link_rows_matched_to_canonical_villages": counts["link_rows_matched_to_canonical_villages"],
        "link_match_rate": round(
            counts["link_rows_matched_to_canonical_villages"] / counts["link_rows"], 6
        )
        if counts["link_rows"]
        else None,
        "matched_village_count": len(matched_villages),
        "unique_local_body_codes": len(local_body_codes),
        "local_body_codes_with_conflicting_names": sum(
            1 for names in conflicting_name_by_code.values() if len(names) > 1
        ),
        "local_body_codes_with_conflicting_states": sum(
            1 for state_codes in conflicting_state_by_code.values() if len(state_codes) > 1
        ),
        "missing_name_examples": missing_name_examples,
        "statewise_named_local_body_rows": dict(state_named_rows),
        "statewise_matched_link_rows": dict(state_linked_rows),
    }


def analyze_lgd_panchayat(json_path: Path, canonical_villages: set[int]) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    state_named_rows: Counter[str] = Counter()
    state_linked_rows: Counter[str] = Counter()
    matched_villages: set[int] = set()
    local_body_codes: set[int] = set()
    conflicting_name_by_code: defaultdict[int, set[str]] = defaultdict(set)
    conflicting_state_by_code: defaultdict[int, set[int]] = defaultdict(set)

    for row in iter_json_array(json_path):
        counts["rows_total"] += 1
        local_body_code = parse_int(row.get("localBodyCode") or row.get("local_body_code"))
        village_code = parse_int(row.get("entityCode") or row.get("entity_code"))
        local_body_name = clean_text(
            row.get("localBodyNameEnglish") or row.get("local_body_name_english")
        )
        state_name = clean_text(row.get("stateNameEnglish") or row.get("state_name_english")) or "<missing>"
        state_code = parse_int(row.get("stateCode") or row.get("state_code"))

        if local_body_code is not None and local_body_code != 0:
            counts["rows_with_nonzero_local_body_code"] += 1
            local_body_codes.add(local_body_code)
            conflicting_state_by_code[local_body_code].add(state_code or -1)
            if local_body_name:
                state_named_rows[state_name] += 1
                conflicting_name_by_code[local_body_code].add(local_body_name)
            else:
                counts["rows_with_nonzero_local_body_code_but_missing_name"] += 1

        if clean_text(row.get("entityType")) != "Village":
            continue
        if local_body_code is not None and local_body_code != 0 and village_code is not None:
            counts["link_rows"] += 1
            if village_code in canonical_villages:
                counts["link_rows_matched_to_canonical_villages"] += 1
                matched_villages.add(village_code)
                state_linked_rows[state_name] += 1

    return {
        "rows_total": counts["rows_total"],
        "rows_with_nonzero_local_body_code": counts["rows_with_nonzero_local_body_code"],
        "rows_with_nonzero_local_body_code_but_missing_name": counts[
            "rows_with_nonzero_local_body_code_but_missing_name"
        ],
        "link_rows": counts["link_rows"],
        "link_rows_matched_to_canonical_villages": counts["link_rows_matched_to_canonical_villages"],
        "link_match_rate": round(
            counts["link_rows_matched_to_canonical_villages"] / counts["link_rows"], 6
        )
        if counts["link_rows"]
        else None,
        "matched_village_count": len(matched_villages),
        "unique_local_body_codes": len(local_body_codes),
        "local_body_codes_with_conflicting_names": sum(
            1 for names in conflicting_name_by_code.values() if len(names) > 1
        ),
        "local_body_codes_with_conflicting_states": sum(
            1 for state_codes in conflicting_state_by_code.values() if len(state_codes) > 1
        ),
        "statewise_named_local_body_rows": dict(state_named_rows),
        "statewise_matched_link_rows": dict(state_linked_rows),
    }


def states_with_zero_named_rows(statewise_named_rows: dict[str, int], all_states: Iterable[str]) -> list[str]:
    return sorted(state for state in all_states if statewise_named_rows.get(state, 0) == 0)


def recommend_source(csv_report: dict[str, Any], json_report: dict[str, Any]) -> dict[str, str]:
    csv_score = (
        csv_report["matched_village_count"],
        -csv_report["rows_with_nonzero_local_body_code_but_missing_name"],
        -csv_report["local_body_codes_with_conflicting_states"],
        -csv_report["local_body_codes_with_conflicting_names"],
    )
    json_score = (
        json_report["matched_village_count"],
        -json_report["rows_with_nonzero_local_body_code_but_missing_name"],
        -json_report["local_body_codes_with_conflicting_states"],
        -json_report["local_body_codes_with_conflicting_names"],
    )
    if csv_score > json_score:
        decision = "prefer_gp_mapping_csv_with_json_fallback_for_missing_names"
        rationale = (
            "The gp_mapping CSV links more canonical villages and/or exposes a cleaner nonzero "
            "local-body footprint, but still needs JSON fallback where local-body names are blank or missing."
        )
    elif csv_score == json_score:
        decision = "keep_hybrid_merge"
        rationale = (
            "Neither source dominates strongly enough to justify exclusive use. A merged strategy is safer."
        )
    else:
        decision = "keep_lgd_panchayat_json_primary"
        rationale = (
            "The current lgd_panchayat.json remains more coherent for named local-body linkage."
        )
    return {"decision": decision, "rationale": rationale}


def build_report(*, lgd_dir: Path, hybrid_sqlite_path: Path) -> dict[str, Any]:
    canonical_villages = build_canonical_village_set(hybrid_sqlite_path)
    csv_report = analyze_gp_mapping(lgd_dir / "gp_mapping.01Apr2026.csv", canonical_villages)
    json_report = analyze_lgd_panchayat(lgd_dir / "lgd_panchayat.json", canonical_villages)

    all_states = sorted(
        set(csv_report["statewise_named_local_body_rows"])
        | set(json_report["statewise_named_local_body_rows"])
        | set(csv_report["statewise_matched_link_rows"])
        | set(json_report["statewise_matched_link_rows"])
    )

    report = {
        "generated_at": utc_now_iso(),
        "lgd_dir": str(lgd_dir),
        "hybrid_sqlite_path": str(hybrid_sqlite_path),
        "column_normalization": dict(CSV_COLUMN_MAP),
        "sources": {
            "gp_mapping_csv": csv_report,
            "lgd_panchayat_json": json_report,
        },
        "statewise": {
            "gp_mapping_csv": {
                "states_with_zero_named_local_body_rows": states_with_zero_named_rows(
                    csv_report["statewise_named_local_body_rows"],
                    all_states,
                ),
            },
            "lgd_panchayat_json": {
                "states_with_zero_named_local_body_rows": states_with_zero_named_rows(
                    json_report["statewise_named_local_body_rows"],
                    all_states,
                ),
            },
        },
        "recommendation": recommend_source(csv_report, json_report),
        "notes": [
            "The CSV uses flat non-standard headers and is normalized to snake_case before any internal use.",
            "A row contributes to linkage only when local_body_code is nonzero and village_code is present.",
            "Canonical matching is measured against the currently built admin_lookup_hybrid.sqlite3 village table.",
        ],
    }
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lgd-dir", default=str(DEFAULT_LGD_DIR))
    parser.add_argument("--hybrid-sqlite-path", default=str(DEFAULT_HYBRID_SQLITE_PATH))
    parser.add_argument("--out-json", default=str(DEFAULT_OUT_JSON))
    parser.add_argument("--out-md", default=str(DEFAULT_OUT_MD))
    args = parser.parse_args(argv)

    report = build_report(
        lgd_dir=Path(args.lgd_dir),
        hybrid_sqlite_path=Path(args.hybrid_sqlite_path),
    )
    Path(args.out_json).write_text(
        json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    Path(args.out_md).write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps(report["recommendation"], indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
