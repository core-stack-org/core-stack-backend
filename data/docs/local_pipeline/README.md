# Local Pipeline Knowledgebase

This directory is the team knowledgebase for the Core Stack **local data
pipelines**: the runtime system that serves village-level datasets
(facility proximity, Mission Antyodaya 2020, 20th Livestock Census) for any
requested geography from local GeoPackages and CSV sidecars — without Google
Earth Engine, without precomputed per-district files, and with a standard,
documented output bundle.

It is written for two audiences at once: engineers who need to run, extend,
and troubleshoot the system, and everyone else in the organisation who needs
to understand what the system does, why it is shaped this way, and what its
outputs mean.

## Contents

| Document | What it covers |
| --- | --- |
| [architecture.md](architecture.md) | The core methodology and moving parts: request lifecycle, module map, the shared core package, caching, and publishing — with flowcharts. |
| [data_structures.md](data_structures.md) | The full data structure and schema: the standard request, output options, status columns, the on-disk output bundle, per-dataset column contracts, and the SQLite sidecar format. |
| [troubleshooting.md](troubleshooting.md) | Failure scenarios, what they look like, why they happen, and how to fix them. |
| [testing.md](testing.md) | The test suites: the interactive CLI runner for exercising parts of a pipeline, and the active-locations matrix run for end-to-end data quality. |

## The One-Paragraph Mental Model

Every dataset pipeline is the same machine with different data plugged in: a
**standard request** (scope + outputs + publish) arrives from the API, CLI,
or a batch file; the **admin source** resolves the scope to village rows in
`cs_admin_standard.gpkg` using SQLite indexes created on first use; the
dataset's **source reader** fetches only the matching rows (keyed lookup in a
CSV-backed SQLite sidecar, or spatial/bbox reads from a GeoPackage); the
pipeline joins, derives, and validates; and an **output bundle** writer emits
a fixed set of artifacts — CSV for humans, GPKG for GIS, README for context,
run-metadata JSON with a column dictionary and EDA for machines, a STAC
fragment for catalogs, and optionally a GeoServer layer published from the
GPKG. A cache manifest makes repeat requests cheap.

## Dataset Knowledge Articles

Each dataset package carries its own knowledge article covering provenance,
methodology, interpretation, and cautions:

- Facilities: `computing/misc/facilities/README.md`
- Mission Antyodaya 2020: `computing/misc/antyodaya/README.md`
- 20th Livestock Census: `computing/misc/livestocks/README.md`
- Shared core: `computing/misc/local_pipeline/README.md`

## Why These Docs Live Under `data/`

The `data/` tree is where the pipeline's inputs and outputs live on every
deployment, so this documentation sits next to the thing it describes. The
`.gitignore` rule `data/*` + `!data/docs/` keeps payloads untracked while
versioning this knowledgebase.
