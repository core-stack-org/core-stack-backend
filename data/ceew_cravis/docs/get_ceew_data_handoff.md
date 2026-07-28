# CEEW `get_ceew_data` Handoff

Date: 2026-07-28

This handoff restarts the CEEW/CRAVIS backend work around one public API name:

```text
GET /api/v1/get_ceew_data/
```

The main scientific reference remains:

```text
data/ceew_cravis/docs/ceew_methodology.md
```

## What Was Finalised

The CEEW read path is now centred on:

```text
computing/misc/ceew/pipeline.py::get_ceew_data
```

It reads generated local artifacts only. It does not download or regenerate
CEEW data.

The Django endpoint is:

```text
computing/api.py::get_ceew_data
computing/urls.py -> path("get_ceew_data/", api.get_ceew_data, name="get_ceew_data")
```

The endpoint supports four read modes:

```text
/api/v1/get_ceew_data/?state=Bihar&district=Jamui
/api/v1/get_ceew_data/?state=Bihar&district=Jamui&include_metadata=true
/api/v1/get_ceew_data/?latitude=25.6&longitude=85.1
/api/v1/get_ceew_data/?bbox=83.5,24.3,88.5,27.5
/api/v1/get_ceew_data/?map_index=true
```

Optional flags:

```text
include_metadata=true|false
include_summary=true|false
include_file_info=true|false
raw=true|false for map_index only
```

Default behavior:

```text
include_metadata=false
include_summary=true
include_file_info=true
```

## Key Files

Runtime:

```text
computing/api.py
computing/urls.py
computing/misc/ceew/__init__.py
computing/misc/ceew/pipeline.py
```

Configuration and future report insertion:

```text
data/ceew_cravis/config/report_context_integration.yaml
data/ceew_cravis/docs/report_context_yaml_methodology.md
```

Reusable Codex skill:

```text
.codex/skills/ceew-report-context/SKILL.md
```

Generated data expected on the backend server:

```text
data/ceew_cravis/output/district_profiles/
data/ceew_cravis/output/grid_point_profiles/
data/ceew_cravis/output/grid_bbox_profiles/
data/ceew_cravis/output/map_layers/district_map_index.json
data/ceew_cravis/output/ceew_cravis_metadata_config.json
```

## Validation Commands

Run from repository root:

```bash
python3 -m py_compile computing/misc/ceew/pipeline.py computing/api.py
python3 computing/misc/ceew/pipeline.py district --state Bihar --district Jamui --summary-only
python3 - <<'PY'
from computing.misc.ceew import get_ceew_data
print(get_ceew_data("Bihar", "Jamui", include_metadata=False)["summary"]["data_status"])
print(get_ceew_data(latitude=25.6, longitude=85.1, include_metadata=False)["summary"]["grid_count"])
print(get_ceew_data(bbox="83.5,24.3,88.5,27.5", include_metadata=False)["summary"]["grid_count"])
PY
```

Postman/local Django examples:

```text
http://localhost:8000/api/v1/get_ceew_data/?state=Bihar&district=Jamui&include_metadata=false
http://localhost:8000/api/v1/get_ceew_data/?state=Andhra%20Pradesh&district=Guntur&include_metadata=false
http://localhost:8000/api/v1/get_ceew_data/?latitude=25.6&longitude=85.1&include_metadata=false
http://localhost:8000/api/v1/get_ceew_data/?bbox=83.5,24.3,88.5,27.5&include_metadata=false
http://localhost:8000/api/v1/get_ceew_data/?map_index=true
```

## Next Work

Use the `ceew-report-context` skill in a new Codex chat.

Start by reading:

```text
data/ceew_cravis/docs/ceew_methodology.md
data/ceew_cravis/config/report_context_integration.yaml
data/ceew_cravis/docs/report_context_yaml_methodology.md
.codex/skills/ceew-report-context/SKILL.md
```

Then implement report insertion:

1. Create `dpr/ceew_report_context.py`.
2. Read `report_context_integration.yaml`.
3. Use `computing.misc.ceew.get_ceew_data()` to load one district profile.
4. Return a compact `ceew_district_context` object.
5. Add that context to:
   - `dpr/api.py::generate_tehsil_report`
   - `dpr/api.py::generate_mws_report`
   - `dpr/api.py::generate_village_report`
6. Add a compact template partial, likely:
   - `templates/partials/ceew_district_context.html`
7. Include it in:
   - `templates/block-report.html`
   - `templates/mws-report.html`
   - `templates/village-report.html`
8. Keep every value labelled as district-scale CEEW/CRAVIS context.

Frontend cleanup can happen separately in `landscape-explorer`:

```text
src/components/kyl_rightSidebar.jsx
src/components/kyl_MWSProfilePanel.jsx
src/pages/PlanViewPage.jsx
```

The frontend should use `REACT_APP_API_URL` consistently for report URLs.

## Constraints

- Do not use `data/ceew_cravis/llm/` outputs for production reports.
- Do not edit `data/ceew_cravis/docs/ceew_methodology.md` unless explicitly asked.
- Do not load all 734 full district profiles in request-time report code.
- Do not present district CEEW values as village, MWS, block, or tehsil measurements.
