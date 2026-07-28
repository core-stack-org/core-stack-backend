---
name: ceew-report-context
description: Maintain YAML-driven CEEW/CRAVIS District Context integration in Core Stack backend and landscape-explorer Tehsil, MWS, and Village reports.
---

# CEEW Report Context

Use this skill when adding, reviewing, or maintaining CEEW/CRAVIS District
Context in Core Stack Tehsil, MWS, or Village reports.

## Required Reading

Read these before editing code:

1. `data/ceew_cravis/docs/ceew_methodology.md`
2. `data/ceew_cravis/config/report_context_integration.yaml`
3. `data/ceew_cravis/docs/report_context_yaml_methodology.md`
4. The target backend handler/template and frontend caller listed in the YAML

Do not use `data/ceew_cravis/llm/` outputs for production report integration.

## Repositories

Backend:

```text
/mnt/y/core-stack-org/core-stack-backend
```

Frontend:

```text
/mnt/y/core-stack-org/landscape-explorer
```

Expected branch bases:

```text
core-stack-backend: dev
landscape-explorer: development
```

## Core Rules

- Treat CEEW/CRAVIS as district-scale context in sub-district reports.
- Keep existing report evidence primary.
- Make metric choice configurable in `data/ceew_cravis/config/report_context_integration.yaml`.
- Use `computing.misc.ceew.pipeline` as the read-only CEEW profile access layer. Build it on the lines of local pipelines `computing.misc.antyodaya` and `computing.misc.livestocks`, while utilising `utilities.` without altering them, as they are core modules for three working layers.
- Add or change metrics in YAML first; keep code generic.
- Use risk scores as provided.
- Use light derived values only when they are traceable from profile period summaries.
- Fail closed if CEEW config or profile data is missing.
- Keep frontend changes limited to report URL generation unless explicitly asked.

## Current Report Paths

Backend routes:

```text
dpr/urls.py
/api/v1/generate_tehsil_report  -> dpr/api.py::generate_tehsil_report
/api/v1/generate_mws_report     -> dpr/api.py::generate_mws_report
/api/v1/generate_village_report -> dpr/api.py::generate_village_report
```

Templates:

```text
templates/block-report.html
templates/mws-report.html
templates/village-report.html
templates/village-report-unavailable.html
```

Frontend callers:

```text
landscape-explorer/src/components/kyl_rightSidebar.jsx
landscape-explorer/src/components/kyl_MWSProfilePanel.jsx
landscape-explorer/src/pages/PlanViewPage.jsx
```

## Default Implementation Shape

Add a backend helper:

```text
dpr/ceew_report_context.py::build_ceew_report_context
```

It should:

1. Load `report_context_integration.yaml`.
2. Resolve state and district.
3. Load one district bundle with `computing.misc.ceew.get_ceew_data`.
4. Select configured climate cards, risk chips, and optional section notes.
5. Return `ceew_district_context`.
6. Return an empty disabled context on any missing-data condition.

Template insertion should use a partial, for example:

```text
templates/partials/ceew_district_context.html
```

## Validation

Run at least one report of each kind after implementation:

```text
http://localhost:8000/api/v1/generate_tehsil_report/?state=bihar&district=jamui&block=jamui
http://localhost:8000/api/v1/generate_mws_report/?state=bihar&district=jamui&block=jamui&uid=12_325505
http://localhost:8000/api/v1/generate_village_report/?state=bihar&district=jamui&block=jamui&villageId=259074
```

Check:

- CEEW District Context appears once.
- It is labelled as district-scale context.
- Existing report sections still render.
- Setting `enabled: false` in YAML removes CEEW context without breaking reports.
- Missing CEEW profile data does not break the report.
