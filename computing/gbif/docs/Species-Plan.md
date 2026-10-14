# Species (GBIF) Integration — Plans, Feasibility & Recommendation

> **Goal:** bring **species data from GBIF** into the CoRE Stack the same way we already handle
> water / forest / built-up — first as a **"how species-rich is this area?"** layer for a selected
> location, and then as **change detection at the species level** (how species change over time).
>
> This doc lists every plan I explored, whether each is feasible, which reuses the most existing
> pipeline, and which has the most impact — so we can decide where to focus.

---

## Background: how the current change detection works

Our existing change detection (LULC / water / forest / built-up) is **100% Google Earth Engine +
LULC rasters**:

- Input = annual `..._LULCmap_10m` GEE assets — a **single-band categorical raster** where every
  10 m pixel is **one** of 12 land-cover classes.
- Each detector remaps the class codes, takes the **mode of the early years ("then") vs later years
  ("now")**, and encodes specific **class→class transitions** (e.g. forest→built-up = deforestation).
- Vectorization uses `reduceRegions()` over the MWS polygons, keyed on the MWS `uid`.

**Key assumption:** every pixel is exactly one land-cover class, and change = one class label
flipping to another. Everything downstream depends on this.

GBIF is the opposite: **sparse point records** ("species X seen at this lat/lon on this date"), not a
dense wall-to-wall raster, and not in GEE.

---

## The plans I explored

### Plan 1 — Reuse the existing pipeline (merge species into the LULC raster)

**Idea:** build a species `.tif`, align it to the LULC `.tif` by lat/lon, **merge** them into one
raster containing "species + water + forest + built-up", and feed that to the **existing** change
detection so species rides along with the current layers.

**Feasibility: ❌ Not feasible.** Three independent reasons, any one fatal:

1. **Species is not a land-cover class.** LULC says "this pixel *is* forest." GBIF says "someone
   *recorded* species X here." Absence in GBIF ≠ "not present" — it means "not observed." It can't be
   slotted into the 1–12 class scheme or `mode()`-d like a land class.
2. **The detectors are hard-coded to LULC codes.** The functions only read a single LULC band and test
   fixed transitions like `then==3 AND now==1`. A merged "species band" is simply not read — so
   "merge and reuse" delivers **zero reuse**; you'd write new logic anyway.
3. **Resolution mismatch.** LULC is dense at 10 m; GBIF points are sparse — most 10 m pixels and most
   MWS have **zero records**. A 10 m species raster is ~99.9% empty. Species data is only meaningful at
   **coarse cells** (~km scale) aggregated over multi-year windows — a different grid from LULC.

**Verdict:** don't merge into LULC and don't route through the existing detectors.

---

### Plan 2 — Separate species pipeline (own raster → species change detection → vectorize)

**Idea:** build our **own** species raster, run a **species-specific** change detection on it,
vectorize per MWS, and show it as a **separate companion layer** next to the LULC change layers.

**Feasibility: ✅ Feasible — this is the correct route.** It mirrors the existing pipeline's *shape*
(then/now → classify → `reduceRegions` per MWS `uid`) while respecting that species data is sparse
points, not dense LULC. It also **reuses all the plumbing** (GEE auth, GCS/GeoServer sync, DB
registration, the `reduceRegions`-per-MWS pattern) — only the classification logic is new.

---

## The two feature levels (what "species" can mean)

Independently of Plan 1 vs 2, there are two things a user might want. They build on each other:

### Level A — Species richness of a selected area (snapshot)

"Input a location → show how many species / which species are there → how rich is it." This is the
**static richness** layer. It is **already designed** in [`README.md`](README.md) (Phases 3–4):
per-MWS `species_richness`, `occurrence_count` (sampling effort), `shannon_diversity_index`,
`dominant_taxon_group`, and a `data_poor` flag — all keyed on MWS `uid`, flowing into Excel → KYL →
reports like every other layer.

**Feasibility: ✅ High.** This is the most defensible and the natural first deliverable.

### Level B — Species change over time (richness change / occurrence change)

"How did the species / richness change between then and now." Mechanically it's **Level A run on two
year windows, then diff on `uid`** — but it carries a serious scientific caveat (below).

**Feasibility: 🟡 Feasible only with effort normalization.**

---

## ⚠️ The scientific caveat that gates Level B

GBIF change over time mostly measures **who uploaded observations**, not real ecological change:

- Species "appearing" is almost always **a new upload**, not a range expansion (GBIF-India uploads grew
  hugely with eBird / iNaturalist).
- You **cannot prove disappearance** from presence-only data ("0 records now" may just mean nobody looked).
- A **raw richness diff rises almost everywhere** — that's the upload curve, not biodiversity.

To make Level B defensible we must **control for sampling effort**:

- **Minimum:** always show `Δrichness` next to `Δoccurrence_count` (effort).
- **Recommended:** **rarefaction** — down-sample both windows to equal record count before counting
  species ("richness at equal effort").
- **Best:** coverage-based estimators (Chao1 / iNEXT).
- Only assess where effort is adequate in both windows; elsewhere output `data_poor = cannot assess`.

Without this, a species-change layer is a **sampling-effort map wearing a biodiversity costume**.

---

## Feasibility & impact summary

| Plan / Level | What it delivers | Feasible? | Reuses existing pipeline? | Impact |
| --- | --- | --- | --- | --- |
| **Plan 1** — merge into LULC | Species inside the existing change detection | ❌ No | No (false reuse) | — |
| **Plan 2** — separate pipeline | A species layer built the right way | ✅ Yes | ✅ High (all plumbing; new classifier only) | Enables everything below |
| **Level A** — richness snapshot | "How rich is this area" per location | ✅ High | ✅ Reuses README Phases 3–4 | High, defensible, ship first |
| **Level B** — change over time | Species/richness change then→now | 🟡 With rarefaction | ✅ Runs Level A twice + diff | High, but needs effort control |

**What's reusable:** `ee_initialize`, `get_gee_asset_path`, `upload_tif_to_gcs`,
`gcs_to_gee_asset_cli`, `sync_raster_to_gcs` / `sync_raster_gcs_to_geoserver`,
`export_raster_asset_to_gee`, `save_layer_info_to_db`, `sync_layer_to_geoserver`,
`update_layer_sync_status`, and the `reduceRegions`-over-`filtered_mws_..._uid` **pattern**.
**Genuinely new:** the taxon+window download tweak, the species classifier
(`gbif_species_change.py`), one Celery task, one API endpoint, and registry rows.

---

## Recommendation

1. **Drop Plan 1** — confirmed not feasible; merging into LULC gives no real reuse.
2. **Proceed with Plan 2** (separate pipeline). It's the correct architecture and reuses the most.
3. **Build Level A first** (richness snapshot for a selected location) — it's already designed, most
   defensible, and immediately useful.
4. **Add Level B on top** (change over time) **with rarefaction/effort normalization built in from the
   start**, once Level A is shipped. Level B = Level A on two windows + diff, so it's an increment, not
   a rewrite.

This staged path gives a working, honest species layer early and grows into change detection without
throwing anything away.

---

## Message to ask the mentor

> **Subject: GBIF species integration — which plan to focus on?**
>
> I explored bringing GBIF species data into the stack the way we do water/forest/built-up.
>
> **Plan 1** was to merge a species raster into the existing LULC `.tif` and reuse the current change
> detection. After digging into the code, this is **not feasible** — the change detection is hard-wired
> to LULC's categorical classes (fixed remap + then/now transitions), and species data is sparse points,
> not a land-cover class. Merging gives no real reuse.
>
> **Plan 2** is a **separate species pipeline** (own raster → species-specific change detection →
> vectorize per MWS). This is feasible and still reuses all our plumbing (GEE/GCS/GeoServer sync, DB
> registration, the reduceRegions-per-MWS pattern) — only the classification logic is new.
>
> My proposal is to **go with Plan 2**, and build it in two levels:
> - **Level A — species richness of a selected location** (snapshot: how rich is the area). This is
>   already designed and is the most defensible. Ship this first.
> - **Level B — species change over time**, added on top of A (run it on two windows + diff). Caveat:
>   GBIF change is heavily confounded by sampling effort (more uploads over time ≠ more species), so
>   this needs effort normalization (rarefaction) to be trustworthy.
>
> **Question:** do you agree we drop Plan 1 and proceed with Plan 2, starting with **Level A (richness
> snapshot)** and then adding **Level B (change over time)** with rarefaction? Or do you want us to
> prioritize the change-detection angle first?

---

### Reference docs

- [`README.md`](README.md) — full plan for the static richness layer (Phases 0–7).
- [`SPECIES_CHANGE_DETECTION_FEASIBILITY.md`](SPECIES_CHANGE_DETECTION_FEASIBILITY.md) — detailed
  feasibility of the change-detection route, existing-function reuse, and the sampling-bias analysis.
- Existing pipeline: [`change_detection.py`](change_detection.py),
  [`change_detection_vector.py`](change_detection_vector.py).
</content>
