"""Run sanity configs against fetched tehsil data and report coverage.

For each tehsil folder under the cache (from fetch_tehsil.py) and each config,
validates vector/<config>.geojson (vector rules) and the config's sheet in
stats.xlsx (excel rules), then reports:

  status / errors    what the framework itself reports
  vacuous rules      rules that matched no field in this data, so they pass
                     without checking anything (usually a stale field name or
                     pattern, or a rule meant for the other context)
  unchecked fields   data columns no rule looks at (candidates for new rules)
  pair               the config's pair rules (applies_to: [pair]), which compare
                     the vector with the Excel sheet: same ids, same shared
                     values (a mismatch usually means a stale sheet)

Run inside the conda env, from the repo root:
  conda run -n corestackenv python .claude/skills/sanity-config/scripts/validate.py \
      [--configs cropping_intensity,terrain] [--tehsils jharkhand__dumka__masalia]
"""

import argparse
import contextlib
import fnmatch
import io
import logging
import sys
from pathlib import Path

import pandas as pd

SKILL_DIR = Path(__file__).resolve().parents[1]
REPO_DIR = SKILL_DIR.parents[2]
CONFIG_DIR = REPO_DIR / "computing" / "sanity" / "configs"
DEFAULT_CACHE = Path.home() / ".cache" / "core-stack-sanity"

sys.path.insert(0, str(REPO_DIR / "computing" / "sanity"))
logging.disable(logging.CRITICAL)
from report import summarize  # noqa: E402
from rules import PAIR_RULE_MAP  # noqa: E402
from run_validation import _load_config, _resolve_sheet_name, _summarize_frame  # noqa: E402
from validator import VectorValidator  # noqa: E402

IGNORED_COLUMNS = {"geometry", "id", "fid", "uid", "UID"}
def expression_groups(rule, df: pd.DataFrame) -> list:
    """Columns referenced by an expression rule, one list per suffix it expands to."""
    columns = [str(c) for c in df.columns]
    groups = []
    for suffix in rule._suffixes(df):
        expression = rule.value.format(**suffix) if suffix else rule.value
        referenced = rule._referenced(expression, columns)
        if referenced:
            groups.append(referenced)
    return groups


def rule_fields(rule, cfg: dict, df: pd.DataFrame) -> list:
    """Columns a rule actually inspects in df (required_* rules included)."""
    if cfg["type"] == "required_any_field_pattern":
        patterns = [p.lower() for p in cfg.get("value") or []]
        return [c for c in df.columns if any(fnmatch.fnmatchcase(c.lower(), p) for p in patterns)]
    if cfg["type"] == "expression":
        return [c for group in expression_groups(rule, df) for c in group]
    if cfg["type"] == "required_fields":
        wanted = cfg.get("value") or cfg.get("fields") or []
        return [c for c in wanted if c in df.columns]
    return [c for c in rule.get_fields(df) if c in df.columns]


def coverage(df: pd.DataFrame, config: dict, context: str):
    validator = VectorValidator(config=config, validation_context=context)
    cfgs = [
        c for c in config["rules"]
        if validator._rule_applies_to_context(c) and c["type"] not in PAIR_RULE_MAP
    ]
    rules = validator.build_rules()
    vacuous, checked = [], set()
    for cfg, rule in zip(cfgs, rules):
        fields = rule_fields(rule, cfg, df)
        if not fields:
            vacuous.append(cfg["name"])
        checked.update(fields)
    unchecked = [c for c in df.columns if c not in checked and c not in IGNORED_COLUMNS]
    return len(rules), vacuous, unchecked


def report(label: str, df: pd.DataFrame, config: dict, context: str) -> None:
    with contextlib.redirect_stdout(io.StringIO()):
        result = _summarize_frame(df, config, context)
    n_rules, vacuous, unchecked = coverage(df, config, context)
    print(f"  {label:6} {result['status']:6} rows={len(df)} rules={n_rules}")
    for err in result["errors"]:
        sample = (err.get("invalid_uids") or [])[:3]
        msg = f" ({err['message']})" if err.get("message") else ""
        print(f"         ERROR {err['rule']} field={err.get('field')} invalid={err.get('invalid_count')}{msg} row ids e.g. {sample}")
    if vacuous:
        print(f"         vacuous rules ({len(vacuous)}): {', '.join(vacuous)}")
    if unchecked:
        more = f" ... +{len(unchecked) - 8}" if len(unchecked) > 8 else ""
        print(f"         unchecked fields ({len(unchecked)}): {', '.join(unchecked[:8])}{more}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--configs", help="Comma-separated config stems (default: all)")
    parser.add_argument("--tehsils", help="Comma-separated tehsil folder names (default: all in cache)")
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    args = parser.parse_args()

    stems = args.configs.split(",") if args.configs else sorted(p.stem for p in CONFIG_DIR.glob("*.yaml"))
    tehsil_dirs = (
        [args.cache / t for t in args.tehsils.split(",")]
        if args.tehsils
        else sorted(p for p in args.cache.iterdir() if (p / "metadata").is_dir())
    )

    import geopandas as gpd

    for stem in stems:
        config = _load_config(stem)
        print(f"== {stem}")
        for tehsil_dir in tehsil_dirs:
            print(f" {tehsil_dir.name}")
            vector_df = excel_df = None
            vector = tehsil_dir / "vector" / f"{stem}.geojson"
            if vector.is_file():
                vector_df = gpd.read_file(vector)
                report("vector", vector_df, config, "vector")
            else:
                print("  vector (no data)")
            workbook = tehsil_dir / "stats.xlsx"
            if not workbook.is_file():
                print("  excel  (no data: no stats.xlsx in this folder)")
            else:
                try:
                    xls = pd.ExcelFile(workbook)
                    sheet = _resolve_sheet_name(xls, stem, config, None)
                    excel_df = pd.read_excel(xls, sheet_name=sheet)
                    report("excel", excel_df, config, "excel")
                except Exception as exc:  # sheet not in this tehsil's data
                    print(f"  excel  (no data: {exc})")
            pair_rules = [c for c in config["rules"] if c["type"] in PAIR_RULE_MAP]
            if pair_rules and vector_df is not None and excel_df is not None:
                validator = VectorValidator(config=config, validation_context="pair")
                result = summarize(validator.validate_pair(vector_df, excel_df))
                print(f"  pair   {result['status']:6} rules={len(pair_rules)}")
                for err in result["errors"]:
                    print(f"         ERROR {err['rule']} field={err.get('field')} invalid={err.get('invalid_count')}"
                          f" ({err.get('message')}) ids e.g. {(err.get('invalid_uids') or [])[:3]}")

if __name__ == "__main__":
    main()
