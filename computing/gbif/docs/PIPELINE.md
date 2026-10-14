# Biodiversity (GBIF) Module — Architecture & Pipeline

> **Status:** primary architecture document for the Biodiversity module.
> **Audience:** anyone — mentor, reviewer, or a new developer who has never seen the code.
> **Promise:** by the end you can explain what the module does, how every stage transforms the data,
> and *why* each design decision was made — without reading the source first.

> **One-paragraph summary.** A user picks a **location** (state → district → block). The system
> downloads every **species-occurrence record** GBIF has inside that block, cleans out bad
> coordinates, adds each species' IUCN threat status, then — inside Google Earth Engine — assigns
> every occurrence point to the **micro-watershed (MWS)** it falls in and computes ~16 biodiversity
> **indicators per MWS** (species richness, taxonomy breakdown, diversity indices, threatened-species
> count, data-quality flags). The result is published as a per-MWS map layer + Excel + dashboard
> filters + report sections — using the **exact same infrastructure every other CoRE Stack layer uses**.

---

## Table of contents

1. [Project overview](#1-project-overview)
2. [Complete end-to-end architecture](#2-complete-end-to-end-architecture)
3. [Data types through the pipeline](#3-data-types-through-the-pipeline)
4. [User input](#4-user-input)
5. [GBIF download](#5-gbif-download)
6. [Cleaning pipeline](#6-cleaning-pipeline)
7. [IUCN enrichment](#7-iucn-enrichment)
8. [Earth Engine upload](#8-earth-engine-upload)
9. [MWS spatial join](#9-mws-spatial-join)
10. [Biodiversity indicators](#10-biodiversity-indicators)
11. [Earth Engine computation flow](#11-earth-engine-computation-flow)
12. [Export pipeline](#12-export-pipeline)
13. [GeoServer integration](#13-geoserver-integration)
14. [Excel generation](#14-excel-generation)
15. [KYL integration](#15-kyl-integration)
16. [Report generation](#16-report-generation)
17. [Final outputs](#17-final-outputs)
18. [Complete data flow](#18-complete-data-flow)
19. [Why we chose this architecture](#19-why-we-chose-this-architecture)
20. [Repository mapping](#20-repository-mapping)
21. [End-to-end walkthrough](#21-end-to-end-walkthrough)

---

## 1. Project overview

### The problem

CoRE Stack already tells planners about **water, forest, built-up, cropping, terrain, groundwater** for
each micro-watershed. It says nothing about **biodiversity** — how many species live there, whether
any are threatened, whether the area is ecologically rich or degraded. Planners need this to prioritise
conservation, flag sensitive areas before interventions, and target field surveys.

This module adds a **biodiversity layer**: for each micro-watershed, "how species-rich is this area,
what kinds of species, and are any of them threatened?"

### Why GBIF

**GBIF (Global Biodiversity Information Facility)** is the world's largest free, open aggregator of
**species-occurrence records** — one record = "this species was observed at this lat/lon on this date."
It aggregates eBird, iNaturalist, museum specimens, herbaria, etc. For India it holds tens of millions
of records. It has a public **Download API** and requires only a free account. No other single source
gives comparable species coverage for free.

### Why one block at a time (block-first)

A block (tehsil) is the unit CoRE Stack already computes everything else on. Processing per block means:

- **Small, fast downloads** (hundreds–tens of thousands of records instead of tens of millions).
- **Testable in minutes**, not a multi-hour national job.
- **Independent failure/rerun** — one block failing doesn't affect others.
- **Same trigger shape** as every other layer (`state, district, block, gee_account_id`).

### Why Google Earth Engine (GEE)

Every other CoRE Stack layer computes in GEE, and the **micro-watershed polygons already live in GEE**
as an asset. Doing the spatial work in GEE means we (a) reuse the exact MWS geometry every other layer
uses, (b) run the heavy point-in-polygon join on Google's servers, and (c) plug into the identical
"export → GeoServer → Excel → reports" chain. Staying in GEE is what makes this feel like a native
CoRE Stack layer rather than a bolt-on.

### Why per-MWS indicators, not raw points

Showing 20,000 raw dots on a map answers nothing for a planner. The stack is organised around the
**micro-watershed** as the decision unit. So we **summarise** the points into per-MWS numbers
("this watershed has 47 species, 3 threatened") that can be compared, filtered, coloured on a
choropleth, put in a spreadsheet, and written into a report — exactly like every other indicator.

---

## 2. Complete end-to-end architecture

```
   USER (picks state / district / block)
     │   input: 3 location strings
     ▼
   API   POST /api/v1/generate_biodiversity_layer/            [Django REST view]
     │   validates + fires an async task; returns immediately
     ▼
   CELERY  generate_biodiversity_block  (queue="nrm")         [background worker]
     │   orchestrates every stage below
     ▼
   GBIF DOWNLOAD                                              [local worker + GBIF servers]
     │   in: block bounding box (WKT)   out: raw occurrences CSV (+ provenance)
     ▼
   CLEANING                                                   [pandas, local]
     │   in: raw CSV   out: clean DataFrame (bad coords removed, deduped)
     ▼
   IUCN ENRICHMENT                                            [GBIF species API, local, cached]
     │   in: clean DataFrame   out: same + iucnRedListCategory column
     ▼
   EARTH ENGINE UPLOAD  (gdf_to_ee_fc)                        [local → GEE object]
     │   in: GeoDataFrame of points   out: ee.FeatureCollection (in memory)
     ▼
   SPATIAL JOIN  (ee.Join.saveAll)                            [GEE servers]
     │   in: points FC + MWS polygons FC   out: each MWS carries its contained points
     ▼
   INDICATORS  (per-MWS statistics)                           [GEE servers]
     │   in: MWS + contained points   out: MWS FC with ~16 indicator properties
     ▼
   EXPORT  (export_vector_asset_to_gee → getInfo)             [GEE servers → local]
     │   in: indicator FC   out: GEE asset, then a Python GeoJSON dict
     ▼
   ENRICH + REGISTER + SYNC                                   [local + DB + GeoServer]
     │   add dominant_class/category/density; Layer row (+misc); publish vector layer
     ▼
   GEOSERVER vector layer  biodiversity:<district>_<block>_biodiversity   (map tiles + WFS)
     ▼
   EXCEL  (biodiversity sheet)                                [stats_generator]
     ▼
   KYL  (dashboard filter keys)                               [stats_generator]
     ▼
   REPORTS  (MWS + tehsil HTML sections)                      [dpr]
```

**Reading every arrow** (input → output, where it runs, why):

| Arrow                | Input             | Output                   | Runs where        | Why this transformation                                                     |
| -------------------- | ----------------- | ------------------------ | ----------------- | --------------------------------------------------------------------------- |
| User → API          | 3 strings         | HTTP request             | browser → Django | the location is the only thing a user must choose                           |
| API → Celery        | request           | queued task              | Django → broker  | GBIF + GEE take minutes; the request must not block                         |
| Celery → GBIF       | block bbox (WKT)  | raw CSV                  | worker → GBIF    | GBIF is the species data source; bbox restricts to the block                |
| GBIF → Cleaning     | raw CSV           | clean DataFrame          | pandas (local)    | raw records contain wrong/imprecise coordinates that would corrupt richness |
| Cleaning → IUCN     | clean DataFrame   | + threat status          | GBIF species API  | SIMPLE_CSV has no threat status; it's a per-species attribute               |
| IUCN → EE upload    | GeoDataFrame      | ee.FeatureCollection     | local → GEE      | GEE can only join data that lives as an EE object                           |
| EE upload → Join    | points + polygons | points-per-MWS           | GEE servers       | assign every observation to the watershed it falls in                       |
| Join → Indicators   | points-per-MWS    | per-MWS numbers          | GEE servers       | turn raw points into decision-ready statistics                              |
| Indicators → Export | indicator FC      | asset → GeoJSON         | GEE → local      | persist results + bring them to Python for publishing                       |
| Export → Sync       | GeoJSON           | GeoServer layer + DB row | local + GeoServer | make it a queryable, drawable, catalogued layer                             |
| Sync → Excel        | WFS layer         | spreadsheet              | stats_generator   | KYL + reports read Excel, not GeoServer directly                            |
| Excel → KYL         | sheet             | filter JSON              | stats_generator   | dashboard filters MWS by indicator                                          |
| Excel → Reports     | sheet             | HTML sections            | dpr               | human-readable per-MWS and per-block narrative                              |

---

## 3. Data types through the pipeline

| Stage           | Input type           | Output type                     | Concrete example                                                                   |
| --------------- | -------------------- | ------------------------------- | ---------------------------------------------------------------------------------- |
| User input      | —                   | 3 strings                       | `("bihar","jamui","jamui")`                                                      |
| Block bbox      | MWS GEE asset        | WKT polygon string              | `POLYGON((85.77 24.33, 86.65 24.33, …))`                                        |
| GBIF download   | WKT + predicates     | tab-separated CSV file          | `occurrences_raw.csv` (21,643 rows)                                              |
| Cleaning        | CSV path             | `pandas.DataFrame`            | 8,921 rows × ~12 columns                                                          |
| IUCN enrichment | DataFrame            | DataFrame (+1 column)           | adds`iucnRedListCategory` = `LC/NT/VU/EN/…`                                   |
| To geometry     | DataFrame            | `geopandas.GeoDataFrame`      | points with`geometry = Point(lon,lat)`                                           |
| EE upload       | GeoDataFrame         | `ee.FeatureCollection`        | 8,921`ee.Feature` points (in memory)                                             |
| MWS polygons    | GEE asset            | `ee.FeatureCollection`        | 324 MultiPolygon features with`uid`                                              |
| Spatial join    | 2 FeatureCollections | `ee.FeatureCollection`        | each MWS + a list of its points                                                    |
| Indicators      | joined FC            | `ee.FeatureCollection`        | 324 features × ~16 scalar properties                                              |
| Export to asset | FC                   | GEE**table asset**        | `…/biodiversity_jamui_jamui`                                                    |
| getInfo         | asset                | Python`dict` (GeoJSON)        | `{"type":"FeatureCollection","features":[…]}`                                   |
| Enrich          | GeoJSON dict         | GeoJSON dict (+3 fields)        | adds`dominant_class`, `biodiversity_category`, `observation_density_per_km2` |
| DB register     | GeoJSON + meta       | `Layer` row (+`Layer.misc`) | one row, keyed on state/district/block                                             |
| GeoServer sync  | GeoJSON              | vector layer (WFS/WMS)          | `biodiversity:jamui_jamui_biodiversity`                                          |
| Excel           | WFS GeoJSON          | `.xlsx` sheet                 | `biodiversity` sheet, 18 columns                                                 |
| KYL             | Excel sheet          | JSON per MWS                    | 7 keys per`uid`                                                                  |
| Reports         | Excel sheet          | HTML context                    | narrative + tables                                                                 |

The through-line: **points (many rows) → per-MWS rows (324) → one map layer + one sheet + filters + report**.

---

## 4. User input

The user provides exactly what every other CoRE Stack layer needs — **no biodiversity-specific input**:

```json
POST /api/v1/generate_biodiversity_layer/
{
  "state": "bihar",
  "district": "jamui",
  "block": "jamui",
  "gee_account_id": 1
}
```

- **state / district / block** — the location. These name the MWS asset and the output layer.
- **gee_account_id** — which stored Earth Engine service account to authenticate with.

**How the location becomes a GBIF query:** the block name maps to an existing MWS GEE asset
`…/<state>/<district>/<block>/filtered_mws_<district>_<block>_uid`. We load that asset, take its
**bounding box** (`roi.geometry().bounds()`), and turn it into a WKT polygon. That polygon is the
`geometry` predicate in the GBIF download. So "jamui" → the MWS asset → a bbox → a GBIF geometry filter.
**There is one source of truth for "where is this block": the MWS asset.** Nothing is hard-coded.

*(Note: the download is all-taxa. There is deliberately no `taxon_key` input — richness and the
per-class taxonomy counts only make sense over all taxa. See §19.)*

---

## 5. GBIF download

**API used:** the GBIF **Download API** (asynchronous), via the `pygbif` library. Not the search API —
the search endpoint caps at 100k records and isn't citable; the Download API has no cap and returns a
citable **DOI**.

**The request (predicates):**

```
hasCoordinate      = TRUE          # must have lat/lon
hasGeospatialIssue = FALSE         # GBIF's own coarse geo-sanity flag
occurrenceStatus   = PRESENT       # observed present (not absence records)
basisOfRecord in [HUMAN_OBSERVATION, PRESERVED_SPECIMEN, MACHINE_OBSERVATION, OBSERVATION]
geometry within POLYGON((...block bbox...))
format = SIMPLE_CSV
```

It's asynchronous: we submit → GBIF prepares a zip → we poll `download_meta(key)` until `SUCCEEDED` →
`download_get` → unzip → a single tab-separated CSV.

**Example response (one row, abbreviated):**

```
gbifID    taxonKey  species                 kingdom  class  decimalLatitude decimalLongitude basisOfRecord      ...
39...     2493145   Acrocephalus dumetorum  Animalia Aves   24.71           86.12            HUMAN_OBSERVATION  ...
```

**Fields we use from SIMPLE_CSV, and why:**

| Field                                     | Why it's needed                                                                                                                                                             |
| ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `taxonKey`                              | GBIF's stable species id —**the** key for all distinct-species counts (richness, taxonomy, threatened). Two records with the same `taxonKey` are the same species. |
| `species`                               | human-readable name (display / debugging)                                                                                                                                   |
| `kingdom`                               | plant vs animal grouping → drives`plant_species_count` (kingdom = Plantae)                                                                                               |
| `class`                                 | Aves / Mammalia / Reptilia / Amphibia / Insecta → the per-class taxonomy counts                                                                                            |
| `decimalLatitude`, `decimalLongitude` | the point location → used to build geometry for the spatial join                                                                                                           |
| `coordinateUncertaintyInMeters`         | cleaning: drop points too imprecise for MWS-scale work                                                                                                                      |
| `basisOfRecord`                         | keep genuine observations/specimens (already filtered in the predicate)                                                                                                     |
| `stateProvince`                         | context/QA (Indian state)                                                                                                                                                   |
| `year`                                  | provenance / potential future temporal use                                                                                                                                  |

**Fields present but *not* used:** `order`, `family`, `genus`, `phylum` — GBIF populates `class` and
`kingdom` reliably; lower ranks (order/family/genus) have higher null rates, so we intentionally do
taxonomy at the `class`/`kingdom` level.

**Not in SIMPLE_CSV:** `iucnRedListCategory` — see §7 (this was a real discovery; the field is *not*
in the CSV and must be fetched separately).

**Provenance captured** (for `Layer.misc`): the `download_key`, the citable `doi`, the `download_date`,
and the raw record count — all read from `download_meta` and cached to `meta.txt`.

---

## 6. Cleaning pipeline

GBIF is opportunistic citizen/collection data; **dirty coordinates inflate species richness**, so
cleaning is not optional. Five filters run in order (all in pandas, fast, local). Example counts are
from the real Jamui pilot (**21,643 → 8,921**, ~59% dropped — high because the birds pilot had many
duplicate eBird checklists).

| # | Filter                                                                     | Why it exists                                                                 | If skipped                                     |
| - | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | ---------------------------------------------- |
| 1 | **drop rows missing** `species` / `taxonKey` / `lat` / `lon` | can't count or place a record with no id or location                          | crashes / meaningless "species"                |
| 2 | **inside India bbox & not (0,0)**                                    | (0,0) and out-of-country points are data-entry errors                         | phantom species appear in wrong watersheds     |
| 3 | **coordinate uncertainty ≤ 10 km** (unknown allowed)                | a point known only to ±50 km can't be assigned to a ~km-scale MWS            | points land in the wrong MWS → wrong richness |
| 4 | **drop "centroid piles"** (>1000 records on one exact coordinate)    | a giant pile on one coordinate = a country/province centroid, not a real spot | one fake hotspot dominates a watershed         |
| 5 | **de-duplicate** identical `(species, lat, lon)`                   | the same species repeatedly logged at the same spot inflates counts           | occurrence_count and evenness skewed           |

**Before → after (illustrative):**

```
BEFORE (raw)                                   AFTER (clean)
taxonKey lat      lon      uncert  species     taxonKey lat     lon     species
212      0.0      0.0      —       House Crow   2493145 24.71   86.12   Acrocephalus...
2493145  24.71    86.12    120     Blyth's...   ...     (row with (0,0) dropped)
2493145  24.71    86.12    120     Blyth's...   ...     (exact duplicate dropped)
9999     24.7     86.1     80000   (uncertain)  ...     (>10km uncertainty dropped)
```

Output: a clean `DataFrame` where every row is a trustworthy "species X at (lat,lon)".

---

## 7. IUCN enrichment

**The gap:** the "most actionable" biodiversity signal for a planner is **how many threatened species**
are present. But GBIF's SIMPLE_CSV **does not contain** the IUCN Red List category. (An early plan
assumed it did — it doesn't; verified on real downloads.)

**Why:** the IUCN category is not a property of a single *observation* — it's a global property of the
*species*. GBIF serves it from its **species API**, not the occurrence CSV.

**How enrichment works** (`gbif_iucn.py`):

1. Take the **distinct** `taxonKey`s in the cleaned data (240 in the Jamui pilot).
2. For each, call `GET https://api.gbif.org/v1/species/{taxonKey}/iucnRedListCategory`.
3. Normalise the long-form answer to the standard short code:
   `LEAST_CONCERN→LC`, `NEAR_THREATENED→NT`, `VULNERABLE→VU`, `ENDANGERED→EN`, `CRITICALLY_ENDANGERED→CR`, …
4. **Cache** results on disk keyed by `taxonKey` — the category is global, so it's fetched once ever and
   reused across all blocks (`_cache/iucn_by_taxonkey.json`).
5. Attach an `iucnRedListCategory` column to the DataFrame.

**Field added:** `iucnRedListCategory` (short code). **Threatened** = `VU`, `EN`, or `CR`.
Pilot result: of 240 bird species, **6 were threatened** (VU/EN).

---

## 8. Earth Engine upload

**Why convert to a GeoDataFrame first:** Earth Engine works on *geometry objects*, not lat/lon columns.
A `geopandas.GeoDataFrame` pairs each row with a proper `Point` geometry, which converts cleanly to an
Earth Engine geometry.

**Why a FeatureCollection:** GEE's spatial join needs both sides to be **FeatureCollections** (the MWS
polygons already are one). A "Feature" = geometry + properties; a "FeatureCollection" = many Features.

**How `gdf_to_ee_fc()` works (conceptually):** it walks each GeoDataFrame row and builds one
`ee.Feature(geometry, properties)`, then wraps them in an `ee.FeatureCollection`. This is the **shared
CoRE Stack helper** already used by the plantation and NREGA modules — we reuse it rather than invent an
upload path.

**What we upload (properties per point):** deliberately **only the four fields the indicators need**,
NaN-free and typed:

```
taxonKey (string)  kingdom (string)  class (string)  iucnRedListCategory (string)
```

We do *not* upload lat/lon as properties (they're already the geometry) or uncertainty/year (unused
downstream) — a smaller payload and no serialization surprises.

**What the geometry looks like:** `ee.Geometry.Point([lon, lat])` per record.

```
DataFrame row                         →   ee.Feature
taxonKey=2493145, class=Aves,             geometry: Point([86.12, 24.71])
lon=86.12, lat=24.71                      properties: {taxonKey:"2493145", class:"Aves",
                                                        kingdom:"Animalia", iucnRedListCategory:"LC"}
```

---

## 9. MWS spatial join

This is the heart of the module: deciding **which micro-watershed each observation belongs to**.

```
   GBIF POINTS (8,921)                 MWS POLYGONS (324)
   .   .    .   .                      ┌───────┬───────┐
     .   .    .        +               │  A    │  B    │
   .    .  .    .                      ├───────┼───────┤
      .    .                          │  C    │  D    │
                                       └───────┴───────┘
                         │
                         ▼  ee.Join.saveAll  (point intersects polygon)
                         │
   ┌──────────────────────────────────────────────────────┐
   │  MWS A  ← [pt, pt, pt, …]   (list of its contained points) │
   │  MWS B  ← [pt, pt]                                          │
   │  MWS C  ← [ ] (none)                                        │
   │  MWS D  ← [pt, pt, pt, pt, …]                               │
   └──────────────────────────────────────────────────────┘
                         │
                         ▼  map: compute indicators from each MWS's point list
   ┌──────────────────────────────────────────────────────┐
   │  MWS A → {richness: 16, occurrence: 18, birds: 16, …}      │
   │  MWS C → {richness: 0,  occurrence: 0,  data_poor: true}   │
   └──────────────────────────────────────────────────────┘
```

**`ee.Join.saveAll` explained:** it's a spatial join where, for each **primary** feature (an MWS
polygon), it *saves all* matching **secondary** features (the GBIF points that intersect it) into a list
property (`gbif_occurrences`) on that MWS. So after the join, each MWS "carries" the points inside it,
and we can compute statistics on exactly those points.

- The join keeps MWS that **have** points. MWS with **zero** points are added back separately (a second
  branch) as all-zero, `data_poor=True` features, so **every** MWS appears in the output — a watershed
  with no records is shown honestly as "no data", not dropped.
- Each output feature is rebuilt **fresh** (`ee.Feature(geometry, indicator_props)`) so it carries only
  clean scalar indicators — not the internal point list (which can't be exported to a table).

### Why a join, not rasterization

The obvious alternative is "paint points onto a raster grid, then `reduceRegions`" (how LULC-style
layers work). **That destroys species identity.** Once you rasterise, a pixel just says "2 records
here" — you can never recover "how many *distinct* species". Species **richness = count of distinct
`taxonKey`**, which requires keeping the individual points. The join preserves each point's `taxonKey`,
so `aggregate_count_distinct('taxonKey')` gives true richness. (This join pattern is itself native —
the LULC cropland module also uses `ee.Join.saveAll`.)

---

## 10. Biodiversity indicators

All indicators below are computed **per MWS**. Groups 1–4 run **server-side in Earth Engine**; group 5
("derived") is cheap **local** post-processing after the results come back (a few hundred rows).
`p_i = n_i / N` = proportion of records that are species *i*; `N` = total records; `S` = richness.
Worked numbers are the real Jamui MWS `12_312011` (18 records, 16 species, all birds).

### 10.1 Species Richness

- **Definition:** number of distinct species recorded in the MWS.
- **Formula:** count of distinct `taxonKey`.
- **EE:** `occurrences.aggregate_count_distinct("taxonKey")`
- **Example:** `16`. **Type:** Integer ≥ 0.

### 10.2 Occurrence Count

- **Definition:** total observation records in the MWS (survey effort).
- **Formula:** count of records.
- **EE:** `occurrences.size()`
- **Example:** `18`. **Type:** Integer ≥ 0. *Always shown next to richness — see §19.*

### 10.3 Shannon Diversity Index

- **Definition:** richness **and** evenness combined; higher = more diverse and even.
- **Formula:** `H = −Σ pᵢ · ln(pᵢ)`  (0 if N ≤ 1).
- **EE:** histogram → proportions → `−Σ p·ln(p)`, guarded by `ee.Algorithms.If(n>1, …, 0)`.
- **Example:** `2.736`. **Type:** Float (stored formatted to 3 dp).

### 10.4 Simpson Diversity Index

- **Definition:** probability two random records are *different* species (0–1; intuitive).
- **Formula:** `D = 1 − Σ pᵢ²`.
- **EE:** reuses the same histogram: `1 − Σ p²`.
- **Example:** `0.932`. **Type:** Float.

### 10.5 Pielou Evenness

- **Definition:** how evenly records spread across species (0 = one dominates, 1 = perfectly even).
- **Formula:** `J = H / ln(S)`  (0 if S ≤ 1).
- **EE:** `shannon / log(richness)`, guarded.
- **Example:** `0.987`. **Type:** Float.

### 10.6 Rare Species Count

- **Definition:** species seen exactly once (singletons); a data-reliability signal.
- **Formula:** count of histogram bins equal to 1.
- **EE:** `histogram.values().map(c → c==1).reduce(sum)`.
- **Example:** `14`. **Type:** Integer.

### 10.7 Threatened Species Count

- **Definition:** distinct species classified IUCN **VU / EN / CR**.
- **Formula:** distinct `taxonKey` where `iucnRedListCategory ∈ {VU,EN,CR}`.
- **EE:** `occurrences.filter(inList(iucnRedListCategory,[VU,EN,CR])).aggregate_count_distinct("taxonKey")`.
- **Example:** `2`. **Type:** Integer. *Depends on §7 enrichment.*

### 10.8–10.13 Taxonomy counts (bird / mammal / reptile / amphibian / insect / plant)

- **Definition:** distinct species within each taxonomic group.
- **Formula:** distinct `taxonKey` after filtering by `class` (or `kingdom` for plants).
- **EE:** e.g. `occurrences.filter(ee.Filter.eq("class","Aves")).aggregate_count_distinct("taxonKey")`.
  Plants use `ee.Filter.eq("kingdom","Plantae")`.
- **Example:** `bird_species_count = 16`, others `0` (birds-only pilot). **Type:** Integer each.

### 10.14 Data Poor flag

- **Definition:** `True` if the MWS has fewer than 20 records — "cannot assess", not "no biodiversity".
- **Formula:** `occurrence_count < 20`.
- **EE:** `n.lt(20)` (stored as 0/1).
- **Example:** `1` (18 < 20). **Type:** Boolean/int.

### 10.15 Dominant Taxonomic Class  *(derived, local)*

- **Definition:** the taxonomic group with the most species in the MWS.
- **Formula:** argmax over the six per-class counts (0 → `"Unknown"`).
- **Where:** Python, after `getInfo`.
- **Example:** `"Aves"`. **Type:** String.

### 10.16 Biodiversity Category  *(derived, local)*

- **Definition:** plain-language band of richness.
- **Formula:** `<10 Very Low · <25 Low · <50 Moderate · <100 High · else Very High`.
- **Where:** Python.
- **Example:** richness 16 → `"Low"`. **Type:** String.

### 10.17 Observation Density (per km²)  *(derived, local)*

- **Definition:** records per km² — normalises effort for watershed size.
- **Formula:** `occurrence_count / (area_in_ha / 100)`  (null if area unavailable).
- **Where:** Python (uses the MWS `area_in_ha` carried through the join).
- **Type:** Float or null.

---

## 11. Earth Engine computation flow

**Server-side (runs on Google's servers, lazily):**

1. Load MWS polygons: `ee.FeatureCollection(<filtered_mws asset>)`.
2. Build the points FC in memory and hand it to GEE.
3. `ee.Join.saveAll` — attach each MWS's contained points.
4. `.map(compute_stats)` — per MWS: richness, occurrence, shannon/simpson/pielou, rare, threatened,
   6 taxonomy counts, data_poor. Zero-record MWS merged back in.
5. `.select(_OUTPUT_PROPERTIES)` — project to the fixed scalar schema.
6. `Export.table.toAsset` — materialise the result to a GEE asset (this is when it actually computes).

**Local (Python, after results return):**
7. `ee.FeatureCollection(asset).getInfo()` — pull the 324 features to the client as a dict.
8. `enrich_and_clean` — add `dominant_class`, `biodiversity_category`, `observation_density`; coerce the
   formatted-string indices to floats; NaN-fill.
9. Register `Layer` + publish to GeoServer.

**Execution order note:** Earth Engine is lazy — steps 1–5 only *describe* a computation graph; the
actual work happens at step 6 (export) and step 7 (getInfo). That's why the export is the slow (~1–3 min)
part, and why we persist to an asset first (so `getInfo` reads a finished table, not a live computation).

---

## 12. Export pipeline

```
   indicator FeatureCollection (in GEE, lazy)
      │  export_vector_asset_to_gee  →  ee.batch.Export.table.toAsset
      ▼
   GEE ASSET  …/biodiversity_<district>_<block>   (materialised table)
      │  check_task_status  (poll until the export finishes)
      ▼
   ee.FeatureCollection(asset_id).getInfo()
      ▼
   Python dict  {"type":"FeatureCollection","features":[… 324 …]}
      │  enrich_and_clean  (dominant_class / category / density / NaN-fill)
      ▼
   clean GeoJSON dict  →  handed to GeoServer + DB
```

**Why this mirrors Change Detection:** `change_detection_vector` does exactly this —
`export_vector_asset_to_gee` → `ee.FeatureCollection(asset_id).getInfo()` → `sync_layer_to_geoserver`.
We use the identical helpers and order, so the biodiversity layer persists, becomes public, and is
consumable exactly like every other vector layer. (An earlier version exported to GCS and re-downloaded
— that was non-native and was removed.)

---

## 13. GeoServer integration

Four existing CoRE Stack objects; **no new models** were introduced.

| Object                   | Role for biodiversity                                                                                                                                                                 |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`Dataset`**    | one seed row`"Biodiversity Occurrence"` (layer_type `vector`, workspace `biodiversity`). Defines the *type* of layer.                                                         |
| **`Layer`**      | one row per block: links to the`Dataset`, stores `layer_name`, `gee_asset_path`, `algorithm`, sync flags, and **`Layer.misc`**. Created by `save_layer_info_to_db`. |
| **`Layer.misc`** | small JSON of GBIF provenance that can't be derived elsewhere (see §17). Matches the repo convention (change_detection stores`start_year/end_year` here).                          |
| **`LayerInfo`**  | registry row (in`stats_generator`) that tells the Excel/KYL step to generate a sheet for this layer. Admin/seed-managed like every other vector layer.                              |

**Synchronisation** (inline in the task, mirroring `change_detection_vector`):

```
save_layer_info_to_db(..., dataset_name="Biodiversity Occurrence", misc={…})   # DB row
make_asset_public(asset_id)                                                    # asset visibility
res = sync_layer_to_geoserver(state, geojson, layer_name, "biodiversity")      # publish vector
if res["status_code"] == 201: update_layer_sync_status(layer_id, sync_to_geoserver=True)
```

**Why reuse:** these helpers already handle GeoServer auth, shapefile packaging, DB versioning, and the
prod/remote split. Re-implementing any of it would be duplicate, divergent infrastructure.

---

## 14. Excel generation

The Excel sheet is the bridge between GeoServer and KYL/reports (they read Excel, not GeoServer).
`create_excel_for_biodiversity` (in `stats_generator/utils.py`, dispatched when `workspace == "biodiversity"`) flattens the layer's per-MWS properties into the **`biodiversity`** sheet:

| Column                                                                          | Source     | Why                       |
| ------------------------------------------------------------------------------- | ---------- | ------------------------- |
| `UID`                                                                         | MWS uid    | join key for KYL/reports  |
| `species_richness`                                                            | GEE        | primary metric            |
| `occurrence_count`                                                            | GEE        | survey effort             |
| `threatened_species_count`                                                    | GEE (IUCN) | conservation signal       |
| `rare_species_count`                                                          | GEE        | reliability caveat        |
| `shannon_diversity_index` / `simpson_diversity_index` / `pielou_evenness` | GEE        | ecological depth          |
| `bird_/mammal_/plant_/reptile_/amphibian_/insect_species_count`               | GEE        | taxonomy breakdown        |
| `dominant_class`                                                              | local      | one-word character        |
| `biodiversity_category`                                                       | local      | plain-language band       |
| `observation_density_per_km2`                                                 | local      | effort normalised by area |
| `data_poor`                                                                   | GEE        | quality flag              |

---

## 15. KYL integration

**KYL** ("Know Your Landscape") is the dashboard filter layer. `stats_generator/mws_indicators.py`
reads the `biodiversity` sheet per MWS and emits these keys into the KYL JSON:

```
species_richness, occurrence_count, threatened_species_count,
shannon_diversity_index, dominant_taxon_group, biodiversity_category, biodiversity_data_poor
```

**Filtering:** the frontend discovers these keys and lets a user filter watersheds (e.g. "show MWS with
≥1 threatened species", "biodiversity_category = High"). **Colouring:** a chosen key drives the choropleth
— each MWS polygon is shaded by its value (e.g. richness) via the GeoServer style, so the map reads as a
heatmap of biodiversity across the block.

---

## 16. Report generation

**MWS report** (`dpr/gen_mws_report.py` → `get_biodiversity_data`): a per-watershed "Biodiversity"
section — a short narrative ("Low biodiversity: 16 species across 18 records, dominated by Aves;
Shannon 2.74"), an indicator table, a **conservation note** when threatened species exist, and an
**under-surveyed caveat** when `data_poor`.

**Tehsil/block report** (`dpr/gen_tehsil_report.py` → `get_biodiversity_summary_data`): a block-level
rollup — total MWS, how many have data, how many are data-poor, average/median richness, count of MWS
with threatened species, and a **top-5 richest watersheds** table.

Both always pair richness with survey effort and a caveat that GBIF absence ≠ true absence.

---

## 17. Final outputs

**Per-MWS GeoJSON feature (published to GeoServer):**

```json
{ "type":"Feature", "geometry": {"type":"MultiPolygon","coordinates":[...]},
  "properties": {
    "uid":"12_312011", "area_in_ha":2775.6,
    "species_richness":16, "occurrence_count":18,
    "shannon_diversity_index":2.736, "simpson_diversity_index":0.932, "pielou_evenness":0.987,
    "rare_species_count":14, "threatened_species_count":2,
    "bird_species_count":16, "mammal_species_count":0, "plant_species_count":0,
    "reptile_species_count":0, "amphibian_species_count":0, "insect_species_count":0,
    "dominant_class":"Aves", "biodiversity_category":"Low",
    "observation_density_per_km2":0.65, "data_poor":true } }
```

**`Layer.misc` (provenance — one per block):**

```json
{ "gbif_doi":"10.15468/dl.xxxxxxx", "download_key":"0009612-260623161305970",
  "taxon_scope":"all", "raw_record_count":21643, "clean_record_count":8921,
  "download_date":"2026-07-02" }
```

- `gbif_doi` — required GBIF citation. `download_key` — re-fetch/debug handle. `taxon_scope` — qualifies
  what the numbers mean. `raw_/clean_record_count` — data-quality (drop rate). `download_date` — data vintage.

**Excel `biodiversity` sheet:** one row per MWS, the 18 columns from §14.

**KYL JSON (per MWS):** the 7 keys from §15.

**Report context:** the narrative + tables from §16.

**Map output:** a choropleth of 324 MWS polygons shaded by a chosen indicator, queryable via WFS.

---

## 18. Complete data flow

```
   (bihar, jamui, jamui)                              ← user picks a block
        │
        ▼  API → Celery
   MWS GEE asset  ──►  bounding box (WKT)
        │
        ▼  GBIF Download API (geometry + basisOfRecord predicates)
   occurrences_raw.csv            21,643 records
        │
        ▼  clean_occurrences  (5 filters)
   clean DataFrame               8,921 records, 240 species
        │
        ▼  enrich_with_iucn  (species API, cached)
   DataFrame + iucnRedListCategory
        │
        ▼  GeoDataFrame → gdf_to_ee_fc
   ee.FeatureCollection (points)  8,921 features
        │                         + MWS FC (324 polygons)
        ▼  ee.Join.saveAll  +  compute_stats  +  merge zeros  +  select
   ee.FeatureCollection (indicators)  324 features × 16 props
        │
        ▼  export_vector_asset_to_gee  →  check_task_status
   GEE asset  …/biodiversity_jamui_jamui
        │
        ▼  getInfo  →  enrich_and_clean
   GeoJSON dict (324 features, +dominant_class/category/density)
        │
        ▼  save_layer_info_to_db(misc) + make_asset_public + sync_layer_to_geoserver + update flag
   Layer row (+misc)  +  GeoServer layer  biodiversity:jamui_jamui_biodiversity
        │
        ├──►  Excel  (biodiversity sheet, 18 cols)
        │        └──►  KYL  (7 filter keys)  ──►  dashboard filters + choropleth
        └──►  Reports  (MWS section + tehsil summary)
```

---

## 19. Why we chose this architecture

| Decision                                            | Why                                                                                                                                                            |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **No rasterization**                          | rasterising points destroys species identity → distinct-species richness becomes impossible. The join keeps`taxonKey`.                                      |
| **FeatureCollections (not images)**           | species data is inherently point/vector; the MWS units are polygons; a FC↔FC join is the correct primitive and preserves per-record attributes.               |
| **Block-first**                               | small/fast/testable downloads, independent reruns, and the same trigger shape as every other layer.                                                            |
| **Earth Engine**                              | the MWS polygons already live in GEE; the heavy join runs on Google's servers; and it plugs into the existing export→GeoServer→Excel→reports chain.         |
| **`Layer.misc` for provenance**             | the repo's established slot for "qualifying parameters that aren't columns" (change_detection stores years there). Minimal, flat, non-duplicative.             |
| **Reuse Change Detection's flow**             | it already solved export→getInfo→GeoServer→register for a per-MWS vector layer; copying it makes biodiversity a native peer.                                |
| **No `GBIFBlockDownload` model**            | no GEE layer uses a status model;`Layer` + `is_gee_asset_exists` already provide idempotency + registration. Adding one would be a new, divergent pattern. |
| **No custom GeoServer pipeline**              | `sync_layer_to_geoserver` handles auth/packaging/prod-split already.                                                                                         |
| **No custom GCS upload pipeline**             | `gdf_to_ee_fc` gets points into GEE in one call (as plantation/nrega do). Above `POINTS_INLINE_LIMIT` the points are exported in chunks to intermediate GEE assets (GEE caps a request at 10 MB); no GCS needed. |
| **IUCN enrichment as a separate step**        | the threat category isn't in the occurrence CSV; it's a per-species attribute fetched (and cached) from the species API.                                       |
| **All-taxa (no taxon filter)**                | richness and per-class taxonomy only make sense over all taxa; filtering to one species would make most indicators meaningless.                                |
| **Always carry occurrence_count + data_poor** | GBIF is opportunistic — "low richness" often means "under-surveyed". Effort and the data-poor flag prevent misreading.                                        |

---

## 20. Repository mapping

| Pipeline stage   | File                                                                                          | Responsibility                                                                                  |
| ---------------- | --------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Orchestration    | `computing/gbif/biodiversity_task.py`                                                       | the Celery task; runs every stage; idempotency guard; registration + sync; builds`Layer.misc` |
| Download         | `computing/gbif/gbif_download.py`                                                           | block bbox from MWS asset; GBIF Download API; poll/fetch/unzip; provenance                      |
| Cleaning         | `computing/gbif/gbif_clean.py`                                                              | the 5 coordinate-cleaning filters                                                               |
| IUCN             | `computing/gbif/gbif_iucn.py`                                                               | per-species IUCN category lookup (cached) + short-code normalisation                            |
| Indicators       | `computing/gbif/gbif_mws_stats.py`                                                          | MWS load,`Join.saveAll`, per-MWS indicators, export-to-asset                                  |
| Local enrichment | `computing/gbif/gbif_export.py`                                                             | `dominant_class` / `biodiversity_category` / `observation_density` + NaN-fill             |
| Config           | `computing/gbif/config.py`                                                                  | thresholds, IUCN categories, basis-of-record, dataset names, asset-id helper                    |
| API + route      | `computing/api.py`, `computing/urls.py`                                                   | `generate_biodiversity_layer` endpoint                                                        |
| Excel            | `stats_generator/utils.py`                                                                  | `create_excel_for_biodiversity` + `workspace=="biodiversity"` dispatch                      |
| KYL              | `stats_generator/mws_indicators.py`                                                         | 7 biodiversity keys per MWS                                                                     |
| Reports          | `dpr/gen_mws_report.py`, `dpr/gen_tehsil_report.py`, `dpr/api.py`, `templates/*.html` | report sections                                                                                 |
| Registry         | `installation/seed/seed_data.json`                                                          | `Dataset` seed row                                                                            |

**Reused CoRE Stack helpers (not reimplemented):** `ee_initialize`, `get_gee_asset_path`,
`valid_gee_text`, `gdf_to_ee_fc`, `export_vector_asset_to_gee`, `check_task_status`,
`is_gee_asset_exists`, `make_asset_public`, `sync_layer_to_geoserver`, `save_layer_info_to_db`,
`update_layer_sync_status`.

---

## 21. End-to-end walkthrough

**User selects:** state `bihar`, district `jamui`, block `jamui`, `gee_account_id = 1`.
*(This is the real pilot; numbers below are measured, from a birds-only validation download — a full
all-taxa run would show more taxa and higher plant/insect counts.)*

1. **API** receives the POST, fires `generate_biodiversity_block.apply_async(["bihar","jamui","jamui",1], queue="nrm")`, returns `{"Success": "biodiversity task initiated"}` instantly.
2. **Task starts**, `ee_initialize(1)` authenticates to Earth Engine.
3. **Bounding box:** loads `…/bihar/jamui/jamui/filtered_mws_jamui_jamui_uid` (324 MWS), takes its bounds →
   `POLYGON((85.77 24.33, 86.65 24.33, 86.65 25.02, 85.77 25.02, 85.77 24.33))`.
4. **Download:** submits the GBIF download for that polygon → key `0009612-260623161305970`; polls until
   `SUCCEEDED`; fetches → `occurrences_raw.csv` with **21,643** records; records DOI + date.
5. **Clean:** 5 filters → **8,921** records, **240** distinct species.
6. **IUCN:** looks up 240 taxa (cached) → adds `iucnRedListCategory`; **6** species are VU/EN.
7. **To GEE:** builds a GeoDataFrame of 8,921 points (props: taxonKey/kingdom/class/iucn) →
   `gdf_to_ee_fc` → an `ee.FeatureCollection`.
8. **Idempotency check:** `is_gee_asset_exists(…/biodiversity_jamui_jamui)` → false → proceed.
9. **Join + indicators:** `ee.Join.saveAll` attaches points to each of the 324 MWS; `compute_stats`
   produces per-MWS indicators; zero-record MWS merged in; projected to the fixed schema.
   *E.g. MWS `12_312011` → richness 16, occurrence 18, birds 16, threatened 2, shannon 2.736,
   simpson 0.932, pielou 0.987, rare 14, data_poor true.*
10. **Export:** `export_vector_asset_to_gee` → asset `…/biodiversity_jamui_jamui`; `check_task_status`
    waits (~1–3 min) until it's materialised.
11. **Read back:** `getInfo` → a GeoJSON dict of **324** features (~8 MB); `enrich_and_clean` adds
    `dominant_class="Aves"`, `biodiversity_category="Low"`, `observation_density_per_km2`, coerces floats.
12. **Register + publish:** `save_layer_info_to_db(dataset="Biodiversity Occurrence", misc={doi, key, taxon_scope:"all", raw:21643, clean:8921, date})` → a `Layer` row; `make_asset_public`;
    `sync_layer_to_geoserver` → `biodiversity:jamui_jamui_biodiversity`; sync flag set.
13. **Downstream (separate triggers):** the stats step writes the `biodiversity` Excel sheet → KYL emits
    the 7 filter keys → the MWS/tehsil reports render their Biodiversity sections.
14. **On the map:** the block's 324 micro-watersheds appear as a choropleth — shade by `species_richness`
    to see rich vs poor watersheds, filter by `threatened_species_count ≥ 1` to spot conservation
    priorities, with under-surveyed (`data_poor`) watersheds flagged so nobody mistakes "no data" for
    "no biodiversity".

**Result:** from three location strings, the planner gets a biodiversity layer for every micro-watershed
in the block — richness, taxonomy, threatened species, diversity, and data quality — built with the
same infrastructure, and readable the same way, as every other CoRE Stack layer.

---

### Appendix — current status & known caveats (for honesty)

- **Validated live:** GBIF auth, download, cleaning, IUCN enrichment, and the full GEE join + indicators
  + export + getInfo, on the real 324-MWS Jamui block (birds subset).
- **Pending:** one full **all-taxa** production run end-to-end; the `biodiversity` **`LayerInfo`** registry
  row (needed for Excel/KYL) is admin/seed-managed and not yet added; GeoServer `biodiversity` workspace +
  style must exist.
- **Scale handling:** Earth Engine rejects any request over 10 MB (~50k inline points). Blocks above
  `config.POINTS_INLINE_LIMIT` are exported in `POINTS_CHUNK_SIZE` chunks to intermediate assets
  (`biodiversity_points_<district>_<block>_partN`, deleted after the stats export) and merged server-side;
  validated on tamil nadu/chennai/guindy (56,809 points).
- **Scientific caveat:** GBIF is opportunistic; richness is confounded by survey effort. This is why
  `occurrence_count` and `data_poor` travel with every richness number.
