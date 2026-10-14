# GBIF Plans — Comparison & Recommendation

> **Question:** we now have two competing designs for the GBIF species layer. Which do we implement?
> This doc compares them head-to-head, lists pros/cons, says which reuses the stack best, which has
> more impact, and which is easier to build — then recommends a path.

---

## The three things on the table (so we don't confuse them)

1. **Root plan** — [`README.md`](README.md): the *original* design. National-first, GeoPandas
   point-in-polygon, static richness. This is the "v1" the `uk/` docs refer to.
2. **Current scaffold (mine)** — [`config.py`](config.py), [`gbif_download.py`](gbif_download.py),
   [`gbif_clean.py`](gbif_clean.py), [`gbif_richness.py`](gbif_richness.py),
   [`gbif_species_change.py`](gbif_species_change.py), [`species_task.py`](species_task.py) +
   [`PLAN_B_IMPLEMENTATION.md`](PLAN_B_IMPLEMENTATION.md). **Per-taxon**, GeoPandas compute,
   **Level A (richness) + Level B (change over time with rarefaction)**. Already written & compiling.
3. **UK plan** — [`uk/implementation.md`](uk/implementation.md) + [`uk/output_design.md`](uk/output_design.md):
   a "v2" redesign. **Block-first**, **GEE-native** (`ee.Join.saveAll` + `aggregate_count_distinct`),
   static richness with a **large indicator set**. Change detection explicitly deferred to v3.

The real contest is **#2 (current scaffold) vs #3 (UK plan)**. #1 is their shared ancestor.

---

## The single most important difference

**Where the per-MWS computation runs.**

| | Current scaffold (#2) | UK plan (#3) |
| --- | --- | --- |
| Species→MWS aggregation | **Python / GeoPandas** `sjoin` on the local machine | **GEE server-side** `ee.Join.saveAll()` + `aggregate_count_distinct('taxonKey')` |
| GBIF points live as | a pandas DataFrame in the worker | a **GEE FeatureCollection asset** (uploaded via GCS → CLI) |
| Fits "compute in GEE like every other layer"? | ⚠️ No — compute is off-GEE | ✅ **Yes** — same engine as LULC/terrain/change-detection |

This matters because of the exact concern raised earlier: *"don't build a separate pipeline that's
incompatible with the others."* Every existing layer computes in GEE. **The UK plan keeps compute in
GEE; the current scaffold moves it to Python.** Both reuse the same *integration* plumbing
(`sync_layer_to_geoserver`, `save_layer_info_to_db`, GeoServer, Excel/KYL/reports) — so neither is
"incompatible" downstream — but the UK plan is the more *architecturally native* of the two.

---

## Full comparison

| Dimension | Current scaffold (#2) | UK plan (#3) | Winner |
| --- | --- | --- | --- |
| **Architecture fit** (compute in GEE) | Python/GeoPandas — off-GEE | GEE-native join — matches all layers | **UK** |
| **Scope of the input** | one **taxon** per run | **all taxa** in the block | depends on goal |
| **Answers "how rich is this area?"** | Yes (Level A) | Yes — with far more indicators | **UK** |
| **Answers "how did species change over time?"** | **Yes** (Level B, rarefied) | **No** — deferred to v3 | **Scaffold** |
| **Indicator richness** | 4–6 per-MWS fields | ~19 fields (Shannon, Simpson, Pielou, threatened, rare, per-class taxonomy, category, density…) | **UK** |
| **Effort/bias handling** | rarefaction + effort mask (change), data_poor | occurrence_count + data_poor + rare-species caveat | tie (different depths) |
| **Reuses existing helpers** | integration helpers only | integration **and** compute (GEE) + a status model like other trackers | **UK** |
| **Documentation depth** | 1 build doc | 2 exhaustive docs: 11 stages, per-stage I/O tables, failure cases, dashboard mockups, roadmap | **UK** |
| **Code state today** | **written & compiles** | spec only (no code yet) | **Scaffold** |
| **New moving parts** | 6 files, no new model | 10 module files + `GBIFBlockDownload` model + migration + mgmt command | **Scaffold** |
| **Known scaling caveats** | GeoPandas `sjoin` fine at block scale | GEE `ee.Filter.inList` for zero-record MWS breaks >1000 MWS (flagged in their doc) | tie |

---

## Pros & cons

### Current scaffold (#2) — per-taxon, Python, Level A + B

**Pros**
- **Already exists and compiles** — fastest to a first run.
- **Only design that delivers change-over-time** (Level B), which was the original goal of this whole thread.
- Fewest moving parts: no GEE ingestion round-trip, no new DB model.
- Rarefaction gives a scientifically honest change signal.

**Cons**
- **Compute is off-GEE** — the one way it deviates from every other layer's pattern.
- Per-taxon input is narrower than "all biodiversity in this area."
- Thin indicator set; no taxonomy breakdown, threatened-species, or diversity-index spread.
- GeoPandas `sjoin` on a Celery worker is heavier on the app server than pushing work to GEE.

### UK plan (#3) — block-first, GEE-native, rich static layer

**Pros**
- **Most architecturally native** — compute in GEE, block-first exactly like the change-detection task; this is the direct answer to "fit the existing architecture."
- **Richest output** — ~19 indicators incl. threatened species (the most actionable conservation signal), full taxonomic breakdown, and three diversity indices.
- **Best documented by far** — 11 stages with input→operation→output tables, failure cases, validation steps, a DB status model, management command, and a milestone roadmap. Lowest ambiguity to hand to a developer.
- Block-first means you can test one block in ~30 min and roll out incrementally.

**Cons**
- **No change-over-time** — deferred to v3; doesn't answer the original "changes in species" ask yet.
- **Most code to write** — 10 modules + model + migration + management command, none written yet.
- GEE-join edge cases (the `inList` >1000-MWS limit; zero-record MWS merge) need care.
- Per-taxon filtering isn't the default (it's a dev shortcut), so "pick a species" needs adding.

---

## Which is easier to implement?

Two senses of "easier":

- **Easier to get *something* working now:** **the current scaffold.** The code exists, compiles, has
  no new DB model, and skips the GEE-ingestion round-trip. Level A could run on a pilot block as soon
  as `pygbif` is installed + credentials are set + the Phase-0 Dataset row exists.
- **Easier to implement *correctly and to completion*:** **the UK plan.** It's more total code, but
  every stage is specified to the line — inputs, outputs, failure modes, validation. There is almost
  no design guesswork left, which is usually what actually sinks an implementation. The scaffold, by
  contrast, still has open scientific decisions in Level B (rarefaction params, window split).

So: **fewer lines = scaffold; less risk-per-line = UK plan.**

---

## Recommendation

**Adopt the UK plan's architecture as the backbone, and fold in the scaffold's change-over-time as a
later phase.** Concretely:

1. **Build the UK plan's Level-A first** (block-first, GEE-native, static richness + its indicator
   set). It's the most stack-native, highest-impact, best-specified, and it's the defensible
   "how rich is this area?" deliverable. This supersedes both the root README (national-first) and my
   scaffold's Level A (Python compute).
2. **Keep the scaffold's Level B (rarefied change over time) as the v2/v3 increment** — it's the only
   design that addresses the original "species change detection" goal, and the UK plan's clean
   per-block GEE FeatureCollection asset is actually a *good* foundation to add temporal windows to
   later (filter the same FC by a `year` property for two windows, then diff).
3. **Salvage from the scaffold into the UK plan:** the `year`-aware download predicate and the
   rarefaction logic ([`gbif_species_change.py`](gbif_species_change.py)) — they're the pieces the UK
   plan is missing for change detection.

Rationale: the UK plan wins on architecture fit, reuse, impact, and specification — the four things
that matter for landing a production layer. The scaffold wins only on "already typed" and on the
change dimension — and the change dimension is exactly what we bolt on afterward.

### If the mentor says "change detection is the priority, not a rich snapshot"

Then invert the order: start from the **scaffold's Level B**, but **migrate its compute into GEE**
(upload points as a FeatureCollection per the UK plan's Stage 3–4, run the join + a per-window
`aggregate_count_distinct`, diff server-side). That gives change detection *and* keeps it native. It's
more work than running the scaffold as-is, but it avoids shipping the one off-pattern piece.

---

## One-line summary for the mentor

> Two designs: my **current scaffold** (per-taxon, Python compute, and the only one that does
> change-over-time) vs the **UK plan** (block-first, GEE-native, a rich static richness layer with ~19
> indicators, deferring change to v3). The UK plan fits our architecture better, reuses more, has more
> impact, and is far better documented — so I propose we **build the UK plan's static layer first**,
> then **add change detection** by reusing my rarefaction logic on top of its per-block GEE asset.
> Which should I prioritize — the rich static layer, or change detection first?
</content>
