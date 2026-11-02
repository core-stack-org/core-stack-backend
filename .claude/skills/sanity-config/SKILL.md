---
name: sanity-config
description: Add, review, tighten or test data sanity configs (computing/sanity/configs/*.yaml) for core-stack-backend pipelines against real prod data. Use this whenever the user wants to check whether a layer's sanity rules are right, add rules for a pipeline (cropping intensity, terrain, hydrology, SWB, LULC, NREGA, ...), find gaps in a sanity config, validate a layer or stats Excel sheet, or asks "what else should this config check" — even if they don't say "skill" or name the framework.
---

# Sanity config workflow

The sanity framework (`computing/sanity/`) validates pipeline outputs in two
forms: the **vector layer** a pipeline publishes and the **stats-generator Excel
sheet** derived from it. Each YAML config lists rules with `applies_to: [vector]`,
`[excel]` or both, because the same data has different column names in each form
(e.g. `cropping_intensity_2017` vs `cropping_intensity_unit_less_2017-2018`).

A config that passes on real data is only half the story. Rules commonly pass
because they check nothing: a hard-coded year in a field name, a pattern that
matches no column, or `allow_missing_fields: true` on every rule so a layer
missing its key fields sails through. This workflow is built to catch that.

## Before you start

- Run Python as `~/miniconda3/bin/conda run -n corestackenv python ...` (conda
  is not on PATH in non-interactive shells). Calling the env's python directly
  breaks `geopandas`/`rtree`. `conda run` swallows stdin, so run script files,
  not `python - <<EOF`.
- Prod API key: `_rundata/core-stack-api-key.txt` (local deployment data,
  untracked). The scripts pass it via the environment. Never print it.
- Data goes to `~/.cache/core-stack-sanity/` — outside the repo, because the
  repo's `tmp/` is not gitignored. Treat downloaded files as data, not code.
- Config files are UTF-8 with a BOM and CRLF line endings. Keep both, or the
  diff becomes the whole file.

## How the framework behaves (read before writing rules)

Rule types (`computing/sanity/rules.py`): `data_type`
(integer/float/numeric/string/boolean, judged on the whole column's dtype),
`min_value`, `max_value`, `not_null`, `allowed_values`, `required_fields`,
`required_any_field_pattern`. Fields are picked with `field`, `fields` or
`field_pattern` (fnmatch, case-insensitive) plus `exclude_field_patterns`.
Quirks that have bitten before:

- **Presence:** for fixed column names use `required_fields` — every listed
  field must exist. `required_any_field_pattern` passes if *any one* pattern
  matches, so a layer missing nearly all its columns still passes; use it only
  for year-suffixed names or naming variants, with one rule per field family.
  It is also the only case-insensitive presence check.
- `field:` (singular) ignores `allow_missing_fields`: a missing column fails and
  `data_type` then reports every row as invalid. `fields: [...]` honours the
  flag, and without it doubles as a presence check.
- `allow_missing_fields: true` on everything makes a config pass on a layer that
  lacks the fields entirely. Use it only for fields that legitimately exist in
  some layer versions and not others.
- `not_null` does not see empty strings `""`.
- Broad patterns like `"*"` catch id/code columns (`bacode`, `sbcode`, `wsconc`
  are strings) — list fields or narrow the pattern instead.
- GeoJSON values that look like JSON (e.g. hydrology's per-year metric blobs)
  load as dicts via `gpd.read_file`, so their dtype depends on file format;
  don't type-check them, check presence/not-null.
- Row ids in error output come only from columns named UID/uid/MWS UID/mws_id/
  vill_id/village_id; other layers report no sample ids.
- There is no cross-field, uniqueness or "value or no-data sentinel" rule
  (e.g. SOGE uses -9999 for no data, so its quantities can only be `>= -9999`).

## 1. Learn what the pipeline actually writes

Read, for the config's layer:
- the config itself (`computing/sanity/configs/<stem>.yaml`);
- the pipeline code that names the output fields — usually both a local
  (`*_local.py`) and a GEE version, since prod layers can come from either and
  may carry leftovers like a GEE `sum` field;
- the Excel export in `stats_generator/utils.py` (search for the sheet name in
  `sheet_names`). Prod runs the `dev` branch, so compare with
  `git show upstream/dev:stats_generator/utils.py` if your branch is older —
  the export format has drifted before (SOGE year-suffixed columns). Note its
  quirks: it often fills missing values with `0` or `""`
  (`properties.get(key, 0)`), which no rule can distinguish from real values.

Write down the field families (with their year/season suffixes), units, and
the physical bounds each must respect. This is what you judge the config against.

## 2. Fetch real data

Use at least two tehsils from different regions so year ranges and layer
versions differ. List active ones with:

```bash
PUBLIC_API_X_API_KEY="$(tr -d '[:space:]' < _rundata/core-stack-api-key.txt)" \
  ~/miniconda3/bin/conda run -n corestackenv python installation/public_api_client.py locations
```

Then fetch (repeat per tehsil):

```bash
~/miniconda3/bin/conda run -n corestackenv python .claude/skills/sanity-config/scripts/fetch_tehsil.py \
  --state jharkhand --district dumka --tehsil masalia [--configs <stem>,...]
```

This writes `metadata/` (layer catalog + `tehsil_data.json`, which holds every
stats sheet as JSON), `vector/<stem>.geojson` and `stats.xlsx`. Which vector
layer belongs to which config lives in `references/layer_map.yaml` (it also
supports picking the newest year-suffixed layer and fetching layers missing
from the catalog straight from GeoServer WFS). If a config reports `NOT FOUND`,
check the tehsil's `metadata/generated_layer_urls.json` — either prod lacks the
layer for that tehsil (fine, use another tehsil) or the map needs updating.
Jharkhand/Dumka/Masalia and Andhra Pradesh/Ananthapur/Raptadu are known-good
choices. To test a file you fetched by hand, drop it in as
`<cache>/<folder>/vector/<stem>.geojson` (next to a `metadata/` folder) and
pass `--cache`/`--tehsils`.

Two things about the Excel side that change how you write rules:
- `stats.xlsx` is rebuilt from the API's `tehsil_data`, which **lowercases
  every header** (`UID`→`uid`, `MWS UID`→`mws uid`). The real workbook from
  `stats_generator` keeps the original case. Exact `field`/`fields` names that
  contain capitals pass on one and fail on the other, so use (case-insensitive)
  patterns for those.
- The sheets can come from an **older generation** of the data than the current
  vector (different MWS ids, row counts, even stale columns). A mismatch between
  the forms is often stale prod data, not a config bug — validate.py reports
  uid coverage differences so you can tell.

## 3. Validate and read the coverage report

```bash
~/miniconda3/bin/conda run -n corestackenv python .claude/skills/sanity-config/scripts/validate.py --configs <stem>
```

For each tehsil and form it prints the framework's status and errors, plus:
- **vacuous rules** — matched no field here. Either the field name/pattern is
  stale, or the rule targets the other form and should change `applies_to`.
- **unchecked fields** — columns no rule inspects. Decide per field whether it
  deserves a rule; leftovers and metadata usually don't.
- **uid coverage** — ids present in only one of the two forms.

The "row ids e.g." in error lines are the failing rows' ids, not the bad values;
look the rows up in the data to see what's wrong.

Errors on prod data are findings in their own right: either the data is wrong
(report it with the sample UIDs) or the rule is too strict (check the pipeline
code before loosening it).

## 4. Improve the config

Typical improvements, in rough order of value:
- **Require the key fields** (`required_fields` for fixed names, one
  `required_any_field_pattern` per family for suffixed names), so a layer
  missing them fails instead of passing vacuously.
- **Split vector and Excel rules** when the names differ. A common bug is a
  rule with `applies_to: [excel, vector]` that only uses Excel names, so the
  vector side checks nothing.
- **Patterns over hard-coded years**: `total_cropable_area_ever_hydroyear_*`, not
  `..._2017_2023`. Years move with every regeneration.
- **Bounds** from the pipeline's maths: areas `>= 0`, fractions `0..1`,
  percentages `0..100`, intensities `0..3`, categorical codes via
  `allowed_values`.
- **Drop redundant rules** that a broader pattern already covers (duplicates
  report the same failure twice).

Keep the config's style: a short comment above each group saying what it
checks and why. Keep vector and Excel rules separate when names differ.

## 5. Prove the rules can fail

```bash
~/miniconda3/bin/conda run -n corestackenv python .claude/skills/sanity-config/scripts/mutation_check.py --configs <stem>
```

For every rule it breaks a copy of the real data in exactly the way that rule
should catch — each matched field separately — and re-runs it. Every rule
should print `FIRES`. `SILENT` lists the fields where the rule let broken data
through (wrong field, pattern or bound, or a pattern catching columns it
shouldn't); `NO FIELDS` means it matched nothing in this data — fix it or
justify why (e.g. the field only exists in older layer versions). Re-run step 3
afterwards: real data must still pass, apart from genuine data problems.

## 6. Cross-field checks (optional, when the pipeline has arithmetic)

Relationships like "parts sum to total" or "cropped area <= croppable area"
can't be expressed as rules. Derive them from the pipeline code and test them
with a short script; `references/cross_field_example.py` shows the pattern for
cropping intensity. Report violations as pipeline bugs; if a relationship is
important and holds, suggest it as a future rule type rather than hacking it in.

## 7. Report and commit

Summarise per config: what changed and why, validation results per tehsil,
mutation results, real data problems found (with row ids), and anything the
config can't catch (Excel zero-filling, cross-field gaps). When a rule fails on
real data because the data is wrong, keep the rule strict and report the data;
don't loosen rules to make prod pass. Show the diff. Commit only the config
files, and only when the user asks — the branch may have unrelated work in
progress.

Many configs at once: the per-config work is independent, so it splits well
across parallel agents, one group of related configs each (e.g. hydrology,
change detection, terrain/drainage, land use/MWS, socio-economic). Have each
edit only its own configs, leave git and this skill alone, and report data
problems and skill issues back; then re-run steps 3 and 5 over everything.
