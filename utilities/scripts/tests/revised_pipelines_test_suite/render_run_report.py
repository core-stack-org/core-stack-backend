#!/usr/bin/env python
"""Render a human-readable markdown report from an active-locations test run.

    python utilities/scripts/tests/revised_pipelines_test_suite/render_run_report.py \
        data/local_pipeline_test_runs/<run> [--output report.md]
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _pct(part: int, whole: int) -> str:
    return f"{100 * part / whole:.1f}%" if whole else "n/a"


def render(run_dir: Path) -> str:
    records = _load_jsonl(run_dir / "pipeline_cases.jsonl")
    manifest = json.loads((run_dir / "run_manifest.json").read_text()) if (run_dir / "run_manifest.json").exists() else {}
    api_smoke = json.loads((run_dir / "api_normalization_smoke.json").read_text()) if (run_dir / "api_normalization_smoke.json").exists() else []
    resolution_path = run_dir / "admin_resolution_report.csv"
    resolution = list(csv.DictReader(resolution_path.open())) if resolution_path.exists() else []

    ok = [r for r in records if r.get("ok")]
    failed = [r for r in records if not r.get("ok")]
    lines = [
        f"# Local Pipeline Test Run: `{run_dir.name}`",
        "",
        f"Rendered: `{datetime.now(timezone.utc).replace(microsecond=0).isoformat()}`",
        "",
        "## Overall",
        "",
        f"- Cases: **{len(records)}**, passed: **{len(ok)}** ({_pct(len(ok), len(records))}), failed: **{len(failed)}**",
        f"- Pipelines: {', '.join(manifest.get('pipelines', []))}",
        f"- Locations available after filters: {manifest.get('available_locations')}",
        "",
    ]

    if resolution:
        resolved = [r for r in resolution if r.get("ok") == "True"]
        unresolved = [r for r in resolution if r.get("ok") != "True"]
        lines += [
            "## Active-Location Coverage (admin boundary resolution)",
            "",
            f"- {len(resolved)}/{len(resolution)} active locations resolve against `cs_admin_standard.gpkg`.",
            "",
        ]
        if unresolved:
            lines += ["Unresolved locations (frontend labels not matching boundary names):", ""]
            lines += [
                f"- {r['state_name']} / {r['district_name']} / {r['tehsil_name']}"
                for r in unresolved
            ]
            lines.append("")

    if api_smoke:
        lines += ["## API Contract Smoke", "", "| Pipeline | Body | Expected | Result |", "| --- | --- | --- | --- |"]
        for r in api_smoke:
            expected = "400 rejected, not queued" if r.get("body_type") == "legacy_rejected" else "200 queued"
            lines.append(
                f"| {r.get('pipeline')} | {r.get('body_type')} | {expected} | "
                f"{'PASS' if r.get('ok') else 'FAIL'} (HTTP {r.get('status_code')}) |"
            )
        lines.append("")

    lines += ["## Cases by Pipeline and Scope", "", "| Pipeline | Scope | Cases | Passed | Median wall s | Max wall s |", "| --- | --- | ---: | ---: | ---: | ---: |"]
    groups = Counter((r["pipeline"], r["scope_level"]) for r in records)
    for (pipeline, scope), count in sorted(groups.items()):
        subset = [r for r in records if r["pipeline"] == pipeline and r["scope_level"] == scope]
        walls = [r.get("elapsed_seconds_wall") or 0 for r in subset]
        lines.append(
            f"| {pipeline} | {scope} | {count} | {sum(1 for r in subset if r.get('ok'))} | "
            f"{median(walls):.1f} | {max(walls):.1f} |"
        )
    lines.append("")

    cached = [r for r in records if r.get("use_pregenerated")]
    cache_hits = [r for r in cached if (r.get("result") or {}).get("cache_hit") or r.get("cache_hit") or r.get("status") == "cached"]
    if cached:
        lines += [
            "## Cache Behaviour",
            "",
            f"- Repeat requests with `use_pregenerated`: {len(cached)}, served from cache: {len(cache_hits)}.",
            "",
        ]

    checked = [r for r in ok if isinstance(r.get("checks"), dict) and r["checks"].get("csv_exists")]
    if checked:
        contract_ok = [r for r in checked if r["checks"].get("csv_contract_ok")]
        prefix_ok = [r for r in checked if r["checks"].get("csv_admin_prefix_ok")]
        status_ok = [r for r in checked if r["checks"].get("csv_status_after_admin")]
        machine_leaks = [r for r in checked if r["checks"].get("csv_machine_columns")]
        feature_leaks = [r for r in checked if r["checks"].get("csv_feature_columns")]
        missing_desc = [
            r
            for r in checked
            if (r["checks"].get("metadata") or {}).get("csv_columns_missing_description")
        ]
        lines += [
            "## Report CSV Data-Quality Checks",
            "",
            f"- CSVs checked: {len(checked)}",
            f"- Admin columns lead the file: {len(prefix_ok)}/{len(checked)}",
            f"- Status column right after admin columns: {len(status_ok)}/{len(checked)}",
            f"- Dataset column contract satisfied: {len(contract_ok)}/{len(checked)}",
            f"- CSVs leaking machine columns (`l2_*`/`l3_*`): {len(machine_leaks)}",
            f"- CSVs leaking feature columns (`*_feat_value`): {len(feature_leaks)}",
            f"- Runs with undocumented CSV columns in metadata: {len(missing_desc)}",
            "",
        ]

    match_rates = [
        (r["pipeline"], r["state_name"], r["district_name"], r["tehsil_name"],
         (r.get("result") or {}).get("join_coverage"))
        for r in ok
        if (r.get("result") or {}).get("join_coverage") is not None and r["scope_level"] == "tehsil"
    ]
    if match_rates:
        lows = sorted(match_rates, key=lambda item: item[4])[:8]
        rates = [item[4] for item in match_rates]
        lines += [
            "## Join Coverage (matched villages / admin villages)",
            "",
            f"- Tehsil-level joins measured: {len(match_rates)}; mean {mean(rates):.3f}, median {median(rates):.3f}.",
            "",
            "Lowest coverage (places to look at first):",
            "",
        ]
        lines += [
            f"- {p}: {s} / {d} / {t} -> {rate:.3f}" for p, s, d, t, rate in lows
        ]
        lines.append("")

    if failed:
        lines += ["## Failures", "", "| Pipeline | Scope | Location | Error |", "| --- | --- | --- | --- |"]
        for r in failed:
            location = f"{r.get('state_name')}/{r.get('district_name')}/{r.get('tehsil_name')}"
            error = str(r.get("error") or "").replace("|", "\\|")[:140]
            lines.append(f"| {r.get('pipeline')} | {r.get('scope_level')} | {location} | {r.get('error_type')}: {error} |")
        lines.append("")
    else:
        lines += ["## Failures", "", "None.", ""]

    lines += [
        "## Artifacts",
        "",
        f"- Full per-case records: `{run_dir}/pipeline_cases.jsonl`",
        f"- Case summary CSV: `{run_dir}/pipeline_cases_summary.csv`",
        f"- Manifest: `{run_dir}/run_manifest.json`",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    report = render(args.run_dir)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report)
        print(f"wrote {args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()
