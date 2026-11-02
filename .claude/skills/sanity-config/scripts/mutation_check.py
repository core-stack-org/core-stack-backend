"""Prove each sanity rule can fail: break real data once per rule and re-run it.

A rule that passes on good data proves little; it may be checking nothing. For
every rule that matches fields in the fetched data, this makes a copy of the
data violating that rule (value below min, above max, a null, a wrong type, an
unknown category, or the required fields removed) and checks the rule now fails.

  FIRES      the rule caught the injected problem in every field it matches
  SILENT     the rule passed broken data in at least one field: its fields,
             pattern or value are wrong for those (they are listed)
  NO FIELDS  the rule matched nothing in this data, so it can't be tested here

Each matched field is broken separately, so one good field can't hide others
that a broad pattern matches by accident. For required_fields, each field is
dropped on its own; for required_any_field_pattern, all matches are dropped
together (one match is enough to pass, by design).

Run inside the conda env, from the repo root:
  conda run -n corestackenv python .claude/skills/sanity-config/scripts/mutation_check.py \
      --configs cropping_intensity [--tehsils jharkhand__dumka__masalia]
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

SKILL_DIR = Path(__file__).resolve().parents[1]
REPO_DIR = SKILL_DIR.parents[2]
CONFIG_DIR = REPO_DIR / "computing" / "sanity" / "configs"
DEFAULT_CACHE = Path.home() / ".cache" / "core-stack-sanity"

sys.path.insert(0, str(REPO_DIR / "computing" / "sanity"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
logging.disable(logging.CRITICAL)
from run_validation import _load_config, _resolve_sheet_name  # noqa: E402
from validator import VectorValidator  # noqa: E402
from validate import rule_fields  # noqa: E402


def mutate(df: pd.DataFrame, cfg: dict, fields: list) -> pd.DataFrame:
    """Return a copy of df that should violate the rule for fields."""
    # Keep geometry so rules see the same columns as in validate.py.
    broken = df.copy()
    field, kind = fields[0], cfg["type"]
    if kind in ("required_fields", "required_any_field_pattern"):
        return broken.drop(columns=fields)
    row = broken.index[0]
    if kind == "min_value":
        broken[field] = pd.to_numeric(broken[field], errors="coerce").astype(float)
        broken.loc[row, field] = float(cfg["value"]) - 1
    elif kind == "max_value":
        broken[field] = pd.to_numeric(broken[field], errors="coerce").astype(float)
        broken.loc[row, field] = float(cfg["value"]) + 1
    elif kind == "not_null":
        broken[field] = broken[field].astype(object)
        broken.loc[row, field] = None
    elif kind == "allowed_values":
        broken[field] = broken[field].astype(object)
        broken.loc[row, field] = "__not_an_allowed_value__"
    elif kind == "data_type":
        if str(cfg.get("expected_type", "")).lower() == "string":
            broken[field] = range(len(broken))
        else:
            broken[field] = broken[field].astype(object)
            broken.loc[row, field] = "not_a_number"
    else:
        raise ValueError(f"no mutation for rule type {kind!r}")
    return broken


def check(label: str, df: pd.DataFrame, config: dict, context: str) -> list:
    validator = VectorValidator(config=config, validation_context=context)
    cfgs = [c for c in config["rules"] if validator._rule_applies_to_context(c)]
    problems = []
    for cfg, rule in zip(cfgs, validator.build_rules()):
        fields = rule_fields(rule, cfg, df)
        silent = []
        if not fields:
            outcome = "NO FIELDS"
        else:
            # One mutation per field, except required_any (any match passes).
            groups = [fields] if cfg["type"] == "required_any_field_pattern" else [[f] for f in fields]
            for group in groups:
                result = rule.validate(mutate(df, cfg, group))
                if all(r["passed"] for r in result["results"]):
                    silent += group
            outcome = "SILENT" if silent else "FIRES"
        if outcome != "FIRES":
            problems.append(f"{label}:{cfg['name']}={outcome}")
        detail = f" silent on: {', '.join(silent[:6])}" if silent else ""
        print(f"    {label:6} {outcome:9} {cfg['name']} ({cfg['type']}, {len(fields)} fields){detail}")
    return problems


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

    summary = {}
    for stem in stems:
        config = _load_config(stem)
        print(f"== {stem}")
        problems = []
        for tehsil_dir in tehsil_dirs:
            print(f"  {tehsil_dir.name}")
            vector = tehsil_dir / "vector" / f"{stem}.geojson"
            if vector.is_file():
                problems += check("vector", gpd.read_file(vector), config, "vector")
            try:
                xls = pd.ExcelFile(tehsil_dir / "stats.xlsx")
                sheet = _resolve_sheet_name(xls, stem, config, None)
                problems += check("excel", pd.read_excel(xls, sheet_name=sheet), config, "excel")
            except Exception:
                pass
        summary[stem] = problems
    print("\n== summary (rules that didn't fire on at least one tehsil)")
    for stem, problems in summary.items():
        names = {}
        for p in problems:
            key, outcome = p.rsplit("=", 1)
            names.setdefault(key, set()).add(outcome)
        print(f"  {stem}: " + (", ".join(f"{k}({'/'.join(sorted(v))})" for k, v in names.items()) or "all rules fire"))


if __name__ == "__main__":
    main()
