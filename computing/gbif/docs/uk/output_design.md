# GBIF Biodiversity Module — Output Design

> **Scope:** This document redesigns the output layer of the biodiversity
> module. The pipeline architecture (download → clean → GEE → GeoServer → Excel
> → KYL → reports) is unchanged. Every indicator proposed for V1 is computable
> purely from GBIF's SIMPLE_CSV fields with no external datasets.
>
> **One small pipeline change required for V1:** Add `iucnRedListCategory` to
> `OUTPUT_COLS` in `clean.py`. This field is already present in every GBIF
> SIMPLE_CSV download — it requires no external API and enables threatened
> species counts directly from the same data.

---

## Table of Contents

1. [Core Indicators (MVP)](#1-core-indicators-mvp)
2. [Taxonomic Indicators](#2-taxonomic-indicators)
3. [Biodiversity Quality Indicators](#3-biodiversity-quality-indicators)
4. [Ecological Indicators](#4-ecological-indicators)
5. [Dashboard Output — KYL Section Design](#5-dashboard-output--kyl-section-design)
6. [GeoJSON Output](#6-geojson-output)
7. [Excel Output](#7-excel-output)
8. [MWS Report Section](#8-mws-report-section)
9. [Tehsil Report Section](#9-tehsil-report-section)
10. [Future Enhancements](#10-future-enhancements)
11. [Priority Tables](#11-priority-tables)

---

## Available GBIF Fields (V1 Pipeline)

Before designing outputs, here is every field available per GBIF record after
Stage 2 cleaning. These are the only inputs available to GEE computation.

| Field | Source | Used for |
|---|---|---|
| `taxonKey` | GBIF | Species identity — all distinct-species counts |
| `species` | GBIF | Human-readable species name for display |
| `kingdom` | GBIF | Plant/Animal/Fungi/etc. grouping |
| `class` | GBIF | Aves, Mammalia, Reptilia, Amphibia, Insecta, etc. |
| `stateProvince` | GBIF | Indian state — not used in per-MWS computation |
| `iucnRedListCategory` | GBIF *(add to OUTPUT_COLS)* | Threatened species count |
| geometry | GBIF (lat/lon → Point) | Spatial join with MWS polygons |

**Fields NOT available in V1 (future pipeline changes needed):**
- `eventDate` / `year` / `month` — not in current OUTPUT_COLS (needed for temporal analysis)
- `order` — needed for finer insect/invertebrate classification
- Any elevation, habitat, or climate data

---

## 1. Core Indicators (MVP)

These five indicators are the non-negotiable minimum. Every other indicator
in this document is built on top of these. All are computed in GEE via
`aggregate_count_distinct` or histogram math on the joined sub-FeatureCollection.

---

### 1.1 Species Richness

**What it means:** The number of distinct species recorded in this watershed.
One species observed 100 times still counts as 1.

**Why it is useful:** The most direct biodiversity metric an environmental
planner understands. "This watershed has 47 species on record." Enables
spatial comparison across watersheds: which areas are biodiversity-rich?

**How it is computed:**

```python
richness = occurrences.aggregate_count_distinct("taxonKey")
```

`aggregate_count_distinct` counts unique `taxonKey` values in the per-MWS
sub-collection. `taxonKey` is GBIF's species-level identifier — two records
with the same `taxonKey` are the same species. Two records with different
`taxonKey` are different species even if the `species` name string varies.

**GBIF fields required:** `taxonKey`

**Expected output type:** Integer ≥ 0

**Example output:** `47`

---

### 1.2 Occurrence Count

**What it means:** Total number of individual occurrence records within this
watershed. One species observed by 10 different people on 10 different days
= 10 occurrences.

**Why it is useful:** The primary proxy for survey effort. A watershed with
species_richness = 47 and occurrence_count = 312 is more trustworthy than
one with richness = 47 and occurrence_count = 52. Always displayed alongside
richness so the user can judge the underlying data quality.

**How it is computed:**

```python
occurrence_count = occurrences.size()
```

**GBIF fields required:** None (count of features)

**Expected output type:** Integer ≥ 0

**Example output:** `312`

---

### 1.3 Shannon Diversity Index

**What it means:** A measure of both richness AND evenness. A watershed with
47 species where one species accounts for 98% of records has a low Shannon
index (uneven). A watershed where all 47 species have similar observation
frequencies has a high Shannon index (even).

**Why it is useful:** Corrects for the bias that species richness gives equal
weight to a species with 1 sighting and a species with 200 sightings. More
informative than richness alone for understanding ecosystem health.

**Formula:** H = −∑ pᵢ · ln(pᵢ), where pᵢ = nᵢ / N (proportion of taxonKey i)

**How it is computed:**

```python
histogram = occurrences.aggregate_histogram("taxonKey")  # {taxonKey: count}
counts    = histogram.values()                           # ee.List of counts
total     = counts.reduce(ee.Reducer.sum())
shannon   = ee.Algorithms.If(
    n.gt(1),
    ee.Number(counts.map(
        lambda c: ee.Number(c).divide(total)
                  .multiply(ee.Number(c).divide(total).log())
    ).reduce(ee.Reducer.sum())).multiply(-1),
    ee.Number(0),
)
```

**GBIF fields required:** `taxonKey`

**Expected output type:** Float (typically 0.0–5.0 for Indian block data)

**Example output:** `3.21` (moderate-high diversity)

**Interpretation guide:**
- 0–1: Very low diversity (few dominant species)
- 1–2: Low diversity
- 2–3: Moderate diversity
- 3–4: High diversity
- 4+: Very high diversity (rare at block scale)

---

### 1.4 Data Poor Flag

**What it means:** Boolean flag. `True` if this watershed has fewer than 20
occurrence records. `False` if ≥ 20 records.

**Why it is useful:** Prevents users from drawing conclusions from statistically
meaningless data. A watershed with 3 records and species_richness = 3 is not
meaningfully different from a watershed with 3 records and richness = 1. The
flag forces the UI to show a warning before the user acts on the data.

**Threshold rationale:** 20 is a widely-used minimum for species diversity
indices (Shannon, Simpson) to be statistically meaningful. Below 20, all
diversity metrics are unreliable.

**How it is computed:**

```python
data_poor = n.lt(20)
```

**Expected output type:** Boolean

**Example output:** `false`

---

### 1.5 Threatened Species Count

**What it means:** Number of distinct species in this watershed that are
classified as Vulnerable (VU), Endangered (EN), or Critically Endangered (CR)
on the IUCN Red List.

**Why it is useful:** This is the single most actionable output for a watershed
manager. "This watershed contains 3 IUCN-threatened species" triggers
conservation planning, land-use restrictions, or detailed field surveys in a
way that a Shannon index does not.

**Important:** This is NOT an external dataset lookup. GBIF embeds the
`iucnRedListCategory` field directly in each occurrence record in its
SIMPLE_CSV format. Adding it to `OUTPUT_COLS` in `clean.py` is the only
pipeline change needed.

**How it is computed:**

```python
threatened = occurrences.filter(
    ee.Filter.inList("iucnRedListCategory", ["VU", "EN", "CR"])
)
threatened_species_count = threatened.aggregate_count_distinct("taxonKey")
```

**GBIF fields required:** `iucnRedListCategory`, `taxonKey`

**Pipeline change required:** Add `iucnRedListCategory` to `OUTPUT_COLS` in
`clean.py`. No other changes.

**Expected output type:** Integer ≥ 0

**Example output:** `3`

**IUCN categories in GBIF:**
- `LC` — Least Concern (not counted as threatened)
- `NT` — Near Threatened (borderline — display but do not count as threatened)
- `VU` — Vulnerable ← counted
- `EN` — Endangered ← counted
- `CR` — Critically Endangered ← counted
- `EW` — Extinct in the Wild (rarely in GBIF India)
- `EX` — Extinct
- `DD` — Data Deficient
- (blank) — Not evaluated

---

## 2. Taxonomic Indicators

Taxonomic breakdown tells the watershed manager *what kind of biodiversity*
exists in the watershed — not just how much. "47 species: 18 birds, 12 plants,
8 mammals, 5 reptiles, 4 amphibians" is far more informative than "47 species."

All taxonomic counts are computed in GEE by filtering the joined
sub-FeatureCollection by `class` or `kingdom`, then calling
`aggregate_count_distinct("taxonKey")` on each filtered subset.

**Important:** These use `class` (not `order` or `family`) because `class` is
consistently populated in GBIF India data. Lower-level taxonomy (order, family)
has high null rates in GBIF occurrence records.

---

### 2.1 Bird Species Count

**Class filter:** `class = "Aves"`

**Why:** Birds are the best-documented vertebrate group in GBIF India due to
eBird integration. Bird richness is a well-established biodiversity surrogate.
"Number of bird species" is immediately interpretable by a non-scientist.

**Also meaningful because:** Western Ghats bird diversity is a key conservation
indicator. A watershed manager in Karnataka can directly act on "15 bird
species including 3 threatened" — it suggests the watershed supports complex
forest or wetland habitat.

```python
birds = occurrences.filter(ee.Filter.eq("class", "Aves"))
bird_species_count = birds.aggregate_count_distinct("taxonKey")
```

**Example output:** `18`

---

### 2.2 Mammal Species Count

**Class filter:** `class = "Mammalia"`

**Why:** Mammals are strong indicators of habitat health and corridor
connectivity. Large mammal presence (deer, leopard, elephant) indicates
intact forest. Even small mammal diversity indicates good ground cover and
prey base.

**Caveat:** Mammals are under-represented in GBIF compared to birds. Low
mammal counts may reflect survey gaps, not absence.

```python
mammals = occurrences.filter(ee.Filter.eq("class", "Mammalia"))
mammal_species_count = mammals.aggregate_count_distinct("taxonKey")
```

**Example output:** `8`

---

### 2.3 Plant Species Count

**Kingdom filter:** `kingdom = "Plantae"`

**Why:** Plants are the foundation of terrestrial food webs. Plant richness
is directly relevant to watershed functions: soil stabilization, water
retention, carbon storage. A watershed with high plant diversity has more
complex vegetation structure.

**Note:** We filter at kingdom level (not class) because plant classes in
GBIF span Magnoliopsida, Liliopsida, Polypodiopsida, etc. Using kingdom
captures all vascular and non-vascular plants consistently.

```python
plants = occurrences.filter(ee.Filter.eq("kingdom", "Plantae"))
plant_species_count = plants.aggregate_count_distinct("taxonKey")
```

**Example output:** `12`

---

### 2.4 Reptile Species Count

**Class filter:** `class = "Reptilia"`

**Why:** Reptiles, especially snakes, are key indicators of grassland and
scrubland health. Herpetofauna are sensitive to habitat degradation and
pesticide use — high reptile diversity indicates a less-degraded watershed.

```python
reptiles = occurrences.filter(ee.Filter.eq("class", "Reptilia"))
reptile_species_count = reptiles.aggregate_count_distinct("taxonKey")
```

**Example output:** `5`

---

### 2.5 Amphibian Species Count

**Class filter:** `class = "Amphibia"`

**Why:** Amphibians are the most sensitive vertebrate group to water quality,
habitat fragmentation, and microclimate change. High amphibian diversity is a
strong positive indicator of watershed health — frogs and toads require both
aquatic breeding habitat and terrestrial foraging habitat. Their presence
indicates intact riparian zones.

```python
amphibians = occurrences.filter(ee.Filter.eq("class", "Amphibia"))
amphibian_species_count = amphibians.aggregate_count_distinct("taxonKey")
```

**Example output:** `4`

---

### 2.6 Insect Species Count

**Class filter:** `class = "Insecta"`

**Why:** Insects are pollinators and decomposers — critical for agricultural
watersheds. High insect diversity (especially bees, butterflies) indicates
intact flowering vegetation and low pesticide pressure. In practice, insect
data in GBIF India is sparse outside of butterfly surveys.

**Caveat:** GBIF insect data is heavily biased toward butterflies and beetles
(charismatic groups). Low insect counts likely reflect survey gaps. Always
show alongside occurrence count.

```python
insects = occurrences.filter(ee.Filter.eq("class", "Insecta"))
insect_species_count = insects.aggregate_count_distinct("taxonKey")
```

**Example output:** `6`

---

### 2.7 Dominant Class

**What it means:** The taxonomic class with the highest number of distinct
species in this watershed. Tells the manager which biological group best
characterizes this watershed's biodiversity.

**Examples:**
- `"Aves"` → bird-rich watershed, likely forest or wetland
- `"Mammalia"` → large mammal corridor
- `"Plantae"` → botanical survey focus area
- `"Insecta"` → high insect survey effort (butterflies)

**How it is computed:** Local post-processing (not GEE). After GEE exports
the per-class species counts, compute the argmax in Python:

```python
# In prepare_geojson_for_geoserver() or add_dominant_taxon_group()
CLASS_MAP = {
    "Aves":      "bird_species_count",
    "Mammalia":  "mammal_species_count",
    "Plantae":   "plant_species_count",    # kingdom-level, stored as plant_species
    "Reptilia":  "reptile_species_count",
    "Amphibia":  "amphibian_species_count",
    "Insecta":   "insect_species_count",
}
for feature in geojson["features"]:
    props = feature["properties"]
    dominant = max(CLASS_MAP.items(), key=lambda kv: props.get(kv[1], 0))
    props["dominant_class"] = dominant[0] if props.get(dominant[1], 0) > 0 else "Unknown"
```

**Expected output type:** String

**Example output:** `"Aves"`

---

## 3. Biodiversity Quality Indicators

Quality indicators answer: "Can I trust the species richness number for this
watershed?" They protect the user from drawing conclusions from sparse or
geographically biased data.

---

### 3.1 Data Poor Flag (see Core Indicators 1.4)

Already described. `True` if occurrence_count < 20. Shown prominently in
the UI as a warning.

---

### 3.2 Observation Density (per km²)

**What it means:** Occurrence count per square kilometre of watershed area.
Normalises for watershed size — a large watershed with 100 records is more
data-poor than a small watershed with 100 records.

**Why it is useful:** Directly answers "how intensively was this watershed
surveyed?" Low density means any high richness values are probably
under-estimates.

**How it is computed:** Local post-processing. The MWS GeoServer WFS response
includes `area_ha` as a property. After GEE export:

```python
area_ha  = props.get("area_ha", 0)
area_km2 = area_ha / 100 if area_ha else None
obs_density = round(props["occurrence_count"] / area_km2, 2) if area_km2 else None
props["observation_density_per_km2"] = obs_density
```

**Expected output type:** Float (observations per km²), nullable if area
is unavailable.

**Example output:** `4.8`

**Interpretation:**
- < 1 per km² → Very low survey effort
- 1–5 per km² → Moderate survey effort
- > 5 per km² → High survey effort (well-documented area)

---

### 3.3 Rare Species Count

**What it means:** Number of species observed only once (singletons) within
this watershed. High rare_species_count relative to species_richness suggests
either genuinely rare species or opportunistic single visits by surveyors.

**Why it is useful:** If rare_species_count / species_richness > 0.5, more
than half the "species" in the richness count have only one record. This
warrants caution — a single sighting could be a misidentification.

**How it is computed in GEE:**

```python
histogram  = occurrences.aggregate_histogram("taxonKey")  # {taxonKey: count}
counts     = histogram.values()
rare_count = counts.map(lambda c: ee.Number(c).eq(1)).reduce(ee.Reducer.sum())
```

This maps each count to 1 if it equals 1, else 0, then sums → count of
singleton species. Fully server-side in GEE.

**GBIF fields required:** `taxonKey`

**Expected output type:** Integer ≥ 0

**Example output:** `14` (out of 47 species richness → 30% are singletons)

---

### 3.4 Biodiversity Category

**What it means:** A plain-language classification of this watershed's
species richness relative to fixed thresholds.

| Category | Species Richness Range | Interpretation |
|---|---|---|
| Very Low | 0–9 | Very few species recorded; data gaps likely |
| Low | 10–24 | Below-average biodiversity for Indian blocks |
| Moderate | 25–49 | Typical for partially-forested Indian block |
| High | 50–99 | Above-average; intact or recovering habitat |
| Very High | 100+ | Exceptional; likely Western Ghats / forest fringe |

**How it is computed:** Local post-processing after GEE export:

```python
def get_biodiversity_category(species_richness: int) -> str:
    if species_richness < 10:   return "Very Low"
    if species_richness < 25:   return "Low"
    if species_richness < 50:   return "Moderate"
    if species_richness < 100:  return "High"
    return "Very High"
```

**Note on thresholds:** These are initial estimates based on typical GBIF
coverage for Indian blocks. After running the pipeline on 10–20 blocks, review
the distribution and recalibrate these thresholds to reflect actual observed
ranges.

**Expected output type:** String

**Example output:** `"Moderate"`

---

## 4. Ecological Indicators

---

### 4.1 Species Richness (V1 ✓)

Already described in Section 1.1.

---

### 4.2 Shannon Diversity Index (V1 ✓)

Already described in Section 1.3.

---

### 4.3 Simpson Diversity Index

**What it means:** The probability that two randomly selected individuals
from the watershed belong to different species. Ranges from 0 (no diversity)
to 1 (maximum diversity). More intuitive than Shannon for non-scientists.

**Formula:** D = 1 − ∑ pᵢ²

**How it is computed in GEE:**

```python
simpson = ee.Algorithms.If(
    n.gt(1),
    ee.Number(1).subtract(
        ee.Number(counts.map(
            lambda c: ee.Number(c).divide(total).pow(2)
        ).reduce(ee.Reducer.sum()))
    ),
    ee.Number(0),
)
```

**Version:** V1. Computed from the same histogram already built for Shannon —
zero additional GEE work.

**Example output:** `0.87` (high: randomly picking 2 individuals, 87% chance
they are different species)

---

### 4.4 Pielou's Evenness (J)

**What it means:** How evenly are observations distributed across species?
0 = one species dominates completely. 1 = all species observed equally often.

**Formula:** J = H / ln(S), where H = Shannon index, S = species richness

**Why it is useful:** A watershed with evenness = 0.9 and richness = 30 is
ecologically healthier than one with evenness = 0.2 and richness = 30 (the
second is dominated by one opportunistic species).

**How it is computed in GEE:**

```python
evenness = ee.Algorithms.If(
    richness.gt(1),
    ee.Number(shannon).divide(ee.Number(richness).log()),
    ee.Number(0),
)
```

**Version:** V1. Derived from Shannon and richness, which are already computed.

**Example output:** `0.84` (high evenness)

---

### 4.5 Dominant Species

**What it means:** The species name with the most occurrence records in this
watershed.

**Why it is useful:** If dominant_species = "Passer domesticus" (House
Sparrow), the watershed is likely urban or peri-urban with low ecological
value. If dominant_species = "Tectona grandis" (Teak), it is a teak
plantation, not natural forest.

**How it is computed:** This requires finding the key (taxonKey) corresponding
to the maximum value in the histogram. GEE's Dictionary API does not natively
support "argmax" — finding the key for the max value. The cleanest approach
for V1:

1. In GEE export, include `"species"` in the selectors — this is already
   in the FC properties.
2. After GEE export, in the local post-processing step, iterate over all
   features across the block and compute a cross-MWS occurrence counter
   per species, then map back to each MWS.

**Practical V1 approach:** Set `dominant_species = "Unknown"` as placeholder
in V1 (already done in current plan). Implement properly in V2 by including
occurrence counts per species as a separate GEE Export.table (one row per
species per MWS) and joining locally.

**Version:** V2. Mark as placeholder in V1.

---

### 4.6 Rare Species Count (V1 ✓)

Already described in Section 3.3.

---

## 5. Dashboard Output — KYL Section Design

Below is the complete KYL JSON output for one MWS entry and a mock UI layout.

### KYL JSON Keys (per MWS)

```json
{
  "uid": "KA_RAMANAGARA_CHANNAPATNA_MWS_042",

  "species_richness":          47,
  "occurrence_count":          312,
  "threatened_species_count":  3,
  "rare_species_count":        14,

  "shannon_diversity_index":   3.21,
  "simpson_diversity_index":   0.87,
  "pielou_evenness":           0.84,

  "bird_species_count":        18,
  "mammal_species_count":      8,
  "plant_species_count":       12,
  "reptile_species_count":     5,
  "amphibian_species_count":   4,
  "insect_species_count":      6,

  "dominant_class":            "Aves",
  "biodiversity_category":     "Moderate",
  "observation_density_per_km2": 4.8,

  "data_poor":                 false,
  "biodiversity_data_poor":    false
}
```

### Mock Dashboard Card (text layout)

```
╔══════════════════════════════════════════════════════════════════╗
║  BIODIVERSITY — Channapatna MWS 042                              ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  Category:  MODERATE  ●●●○○                                      ║
║                                                                  ║
║  ─────────────────────────────────────────────────────────────  ║
║  SPECIES SUMMARY                                                 ║
║  Total Species Recorded    47                                    ║
║  ⚠ Threatened Species       3  (VU/EN/CR)                        ║
║  Rare Species (singletons) 14  (30% of total)                    ║
║  Total Observations       312                                    ║
║                                                                  ║
║  ─────────────────────────────────────────────────────────────  ║
║  TAXONOMIC BREAKDOWN                                             ║
║  🐦 Birds           18 species    [▓▓▓▓▓▓▓▓▓▓░░░░] 38%           ║
║  🌿 Plants          12 species    [▓▓▓▓▓▓░░░░░░░░] 26%           ║
║  🦁 Mammals          8 species    [▓▓▓▓░░░░░░░░░░] 17%           ║
║  🦎 Reptiles         5 species    [▓▓▓░░░░░░░░░░░] 11%           ║
║  🐸 Amphibians       4 species    [▓▓░░░░░░░░░░░░]  9%           ║
║  🦋 Insects          6 species    (incl. butterflies)            ║
║  Dominant Group:    Birds (Aves)                                 ║
║                                                                  ║
║  ─────────────────────────────────────────────────────────────  ║
║  DIVERSITY INDICES                                               ║
║  Shannon Diversity    3.21  (High)                               ║
║  Simpson Diversity    0.87  (High)                               ║
║  Pielou Evenness      0.84  (Even)                               ║
║                                                                  ║
║  ─────────────────────────────────────────────────────────────  ║
║  DATA QUALITY                                                    ║
║  Observation Density  4.8 / km²   (Moderate coverage)           ║
║  Survey Confidence    MODERATE                                   ║
║  Data Source          GBIF · DOI: 10.15468/dl.xxxxx             ║
║  Last Updated         June 2025                                  ║
╚══════════════════════════════════════════════════════════════════╝
```

### Field-by-Field Explanation

| Field | Why it appears | What the user does with it |
|---|---|---|
| **Category** | Quick visual anchor — user sees MODERATE before reading details | Filter watersheds by category in KYL |
| **Total Species Recorded** | Core metric | Compare across watersheds |
| **Threatened Species** | Most actionable conservation signal | Prioritise field surveys or protection measures |
| **Rare Species** | Quality caveat — many singletons = less reliable richness | Adjust confidence in richness number |
| **Total Observations** | Survey effort proxy | Judge whether richness is well-sampled |
| **Taxonomic Breakdown** | What kind of biodiversity, not just how much | Identify watershed character (forest vs. grassland vs. wetland) |
| **Dominant Group** | Single-word characterization | Quick summary for non-scientists |
| **Shannon / Simpson / Evenness** | Ecological depth for analysts | Compare ecosystem health, not just species count |
| **Observation Density** | Normalised survey effort | Compare across watersheds of different sizes |
| **Data Source + DOI** | Mandatory citation for GBIF usage policy | Report and publication references |
| **Last Updated** | Data currency | Understand when the survey data was compiled |

---

## 6. GeoJSON Output

Complete properties schema for each MWS Feature in the exported GeoJSON.
This is what GeoServer stores, what WFS returns, and what the Excel sheet
is built from.

```json
{
  "type": "Feature",
  "geometry": {
    "type": "MultiPolygon",
    "coordinates": ["..."]
  },
  "properties": {

    "uid": "KA_RAMANAGARA_CHANNAPATNA_MWS_042",

    "species_richness":          47,
    "occurrence_count":          312,
    "threatened_species_count":  3,
    "rare_species_count":        14,

    "shannon_diversity_index":   3.21,
    "simpson_diversity_index":   0.87,
    "pielou_evenness":           0.84,

    "bird_species_count":        18,
    "mammal_species_count":      8,
    "plant_species_count":       12,
    "reptile_species_count":     5,
    "amphibian_species_count":   4,
    "insect_species_count":      6,
    "other_species_count":       7,

    "dominant_class":            "Aves",
    "biodiversity_category":     "Moderate",
    "observation_density_per_km2": 4.8,

    "data_poor":                 false
  }
}
```

### Property Definitions

| Property | Type | Default | Computed in | Notes |
|---|---|---|---|---|
| `uid` | String | — | GEE export | MWS unique identifier; join key for Excel + KYL |
| `species_richness` | Int | 0 | GEE | `aggregate_count_distinct("taxonKey")` |
| `occurrence_count` | Int | 0 | GEE | `.size()` of joined sub-collection |
| `threatened_species_count` | Int | 0 | GEE | filter iucnRedListCategory ∈ {VU, EN, CR} → `aggregate_count_distinct` |
| `rare_species_count` | Int | 0 | GEE | count histogram values == 1 |
| `shannon_diversity_index` | Float | 0.0 | GEE | −Σ pᵢ ln(pᵢ), rounded to 3 dp |
| `simpson_diversity_index` | Float | 0.0 | GEE | 1 − Σ pᵢ², rounded to 3 dp |
| `pielou_evenness` | Float | 0.0 | GEE | H / ln(S), rounded to 3 dp |
| `bird_species_count` | Int | 0 | GEE | filter class=Aves → `aggregate_count_distinct` |
| `mammal_species_count` | Int | 0 | GEE | filter class=Mammalia → `aggregate_count_distinct` |
| `plant_species_count` | Int | 0 | GEE | filter kingdom=Plantae → `aggregate_count_distinct` |
| `reptile_species_count` | Int | 0 | GEE | filter class=Reptilia → `aggregate_count_distinct` |
| `amphibian_species_count` | Int | 0 | GEE | filter class=Amphibia → `aggregate_count_distinct` |
| `insect_species_count` | Int | 0 | GEE | filter class=Insecta → `aggregate_count_distinct` |
| `other_species_count` | Int | 0 | GEE | species_richness minus sum of above groups |
| `dominant_class` | String | "Unknown" | Local (post-export) | argmax over per-class species counts |
| `biodiversity_category` | String | "Unknown" | Local (post-export) | threshold classification on species_richness |
| `observation_density_per_km2` | Float | null | Local (post-export) | occurrence_count / (area_ha / 100) |
| `data_poor` | Boolean | true | GEE | occurrence_count < 20 |

### NaN / Null Policy

- Integer fields: default to `0` if null
- Float fields: default to `0.0` if null
- String fields: default to `"Unknown"` if null
- Boolean fields: default to `true` (assume data_poor if uncertain)
- `observation_density_per_km2`: can be `null` if MWS area is unavailable

All NaN handling is applied in `prepare_geojson_for_geoserver()` before
GeoServer sync.

---

## 7. Excel Output

The `biodiversity` sheet in `{district}_{block}.xlsx`.

### Complete Column Schema

| Column | Type | Example | Purpose |
|---|---|---|---|
| `UID` | String | `KA_RAM_CHP_MWS_042` | Join key for KYL generation |
| `species_richness` | Int | `47` | Primary biodiversity metric |
| `occurrence_count` | Int | `312` | Survey effort proxy |
| `threatened_species_count` | Int | `3` | Conservation priority signal |
| `rare_species_count` | Int | `14` | Data reliability indicator |
| `shannon_diversity_index` | Float | `3.21` | Ecological diversity (evenness-adjusted) |
| `simpson_diversity_index` | Float | `0.87` | Intuitive diversity (probability) |
| `pielou_evenness` | Float | `0.84` | Dominance vs. evenness |
| `bird_species_count` | Int | `18` | Bird diversity (most reliable taxon) |
| `mammal_species_count` | Int | `8` | Mammal diversity |
| `plant_species_count` | Int | `12` | Plant diversity |
| `reptile_species_count` | Int | `5` | Reptile diversity |
| `amphibian_species_count` | Int | `4` | Amphibian diversity (water quality proxy) |
| `insect_species_count` | Int | `6` | Pollinator/decomposer diversity |
| `other_species_count` | Int | `7` | All other taxa |
| `dominant_class` | String | `Aves` | Characteristic taxon group |
| `biodiversity_category` | String | `Moderate` | Plain-language category |
| `observation_density_per_km2` | Float | `4.8` | Survey intensity normalised for area |
| `data_poor` | Boolean | `False` | Quality flag for downstream use |

### Why Excel Is Still Required

In the existing CoRE Stack architecture:
- `stats_generator/utils.py` fetches data from GeoServer WFS and writes Excel
- `mws_indicators.py` reads the Excel sheet (not GeoServer directly) to build KYL
- Report generators read from Excel

The Excel sheet is the connective tissue between GeoServer and the KYL/report
pipeline. Removing it would require refactoring multiple existing modules.
The V1 implementation should follow this pattern.

---

## 8. MWS Report Section

This section describes the complete biodiversity block that appears in the
per-MWS HTML report. It is designed to be read by an environmental planner,
not a biologist.

### Section Layout

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  BIODIVERSITY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ⚠ UNDER-SURVEYED WATERSHED
  [Only shown if data_poor = True]
  This watershed has fewer than 20 occurrence records in the GBIF
  database. Species richness and diversity indices below are likely
  underestimates. Field survey is recommended before drawing
  conclusions or planning conservation actions.

  ─────────────────────────────────────────────────────────────────
  Summary
  ─────────────────────────────────────────────────────────────────

  This watershed has MODERATE biodiversity. 47 distinct species have
  been recorded across 312 occurrence records. The dominant taxonomic
  group is Birds (Aves) with 18 bird species.

  3 IUCN-threatened species (Vulnerable, Endangered, or Critically
  Endangered) have been recorded in this watershed.

  ─────────────────────────────────────────────────────────────────
  Species Breakdown
  ─────────────────────────────────────────────────────────────────

  ┌────────────────────────┬──────────────┐
  │ Taxonomic Group        │ Species Count│
  ├────────────────────────┼──────────────┤
  │ Birds (Aves)           │ 18           │
  │ Plants (Plantae)       │ 12           │
  │ Mammals (Mammalia)     │  8           │
  │ Insects (Insecta)      │  6           │
  │ Reptiles (Reptilia)    │  5           │
  │ Amphibians (Amphibia)  │  4           │
  │ Other                  │  7           │
  ├────────────────────────┼──────────────┤
  │ TOTAL                  │ 47           │
  └────────────────────────┴──────────────┘

  ─────────────────────────────────────────────────────────────────
  Diversity Indices
  ─────────────────────────────────────────────────────────────────

  ┌──────────────────────────┬────────┬────────────────────────────┐
  │ Index                    │ Value  │ Interpretation             │
  ├──────────────────────────┼────────┼────────────────────────────┤
  │ Shannon Diversity (H)    │  3.21  │ High — varied species      │
  │ Simpson Diversity (D)    │  0.87  │ High — community is even   │
  │ Pielou Evenness (J)      │  0.84  │ No single species dominates│
  └──────────────────────────┴────────┴────────────────────────────┘

  ─────────────────────────────────────────────────────────────────
  Conservation Note
  ─────────────────────────────────────────────────────────────────

  [Only shown if threatened_species_count > 0]

  ⚠ 3 threatened species (IUCN VU/EN/CR) have been recorded in this
  watershed. This warrants attention during any land-use planning
  or intervention design. Consult the Wildlife Institute of India
  or State Forest Department before implementing activities that
  may disturb these habitats.

  ─────────────────────────────────────────────────────────────────
  Data Quality
  ─────────────────────────────────────────────────────────────────

  ┌──────────────────────────────┬─────────────────────────────────┐
  │ Survey Records               │ 312 occurrences                 │
  │ Rare Species (singletons)    │ 14 species (30% of total)       │
  │ Observation Density          │ 4.8 records / km²               │
  │ Survey Coverage              │ Moderate                        │
  └──────────────────────────────┴─────────────────────────────────┘

  Note: GBIF data is opportunistic — it reflects where surveyors have
  visited, not a complete ecological survey. Absence of a species in
  GBIF does not confirm its absence in the watershed.

  ─────────────────────────────────────────────────────────────────
  Data Source
  ─────────────────────────────────────────────────────────────────

  GBIF.org. DOI: 10.15468/dl.xxxxxxxx. Downloaded: June 2025.
  Processed via CoRE Stack Biodiversity Module v2.0.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### Conditional Display Rules

| Condition | What to show |
|---|---|
| `data_poor = True` | Warning banner at top |
| `threatened_species_count > 0` | Conservation Note section |
| `rare_species_count / species_richness > 0.4` | Add a note: "A high proportion of species are singletons — richness may be an overestimate" |
| `species_richness = 0` | Replace entire section with: "No biodiversity records found for this watershed in GBIF. This likely indicates a survey gap rather than true absence of species." |

---

## 9. Tehsil Report Section

The tehsil report covers the entire block. This section aggregates per-MWS
biodiversity data into a block-level summary.

### Section Layout

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  BIODIVERSITY — Channapatna Block
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ─────────────────────────────────────────────────────────────────
  Block Summary
  ─────────────────────────────────────────────────────────────────

  ┌──────────────────────────────────┬────────────┐
  │ Metric                           │ Value      │
  ├──────────────────────────────────┼────────────┤
  │ Total MWS in block               │ 87         │
  │ MWS with biodiversity data       │ 82 (94%)   │
  │ MWS flagged as data-poor (< 20)  │ 23 (26%)   │
  │ Total unique observations        │ 14,230     │
  │ Average species richness         │ 31.4       │
  │ Median species richness          │ 28         │
  │ Average Shannon diversity        │ 2.81       │
  │ MWS with threatened species      │ 12 (14%)   │
  └──────────────────────────────────┴────────────┘

  ─────────────────────────────────────────────────────────────────
  Species Richness Distribution
  ─────────────────────────────────────────────────────────────────

  ┌──────────────┬───────────┬──────────────────────────────────┐
  │ Category     │ MWS Count │ Share of block                   │
  ├──────────────┼───────────┼──────────────────────────────────┤
  │ Very High    │  4        │ ████  5%                         │
  │ High         │ 12        │ ████████████  14%                │
  │ Moderate     │ 34        │ ██████████████████████████  39%  │
  │ Low          │ 28        │ ████████████████████  32%        │
  │ Very Low     │  9        │ █████████  10%                   │
  └──────────────┴───────────┴──────────────────────────────────┘

  ─────────────────────────────────────────────────────────────────
  Most Biodiverse Watersheds (Top 5)
  ─────────────────────────────────────────────────────────────────

  ┌──────────────────────────┬────────────┬──────────┬───────────┐
  │ Watershed                │ Richness   │ Shannon  │ Category  │
  ├──────────────────────────┼────────────┼──────────┼───────────┤
  │ MWS_031                  │ 112        │ 4.12     │ Very High │
  │ MWS_008                  │  97        │ 3.94     │ High      │
  │ MWS_059                  │  91        │ 3.87     │ High      │
  │ MWS_042                  │  47        │ 3.21     │ Moderate  │
  │ MWS_017                  │  44        │ 3.09     │ Moderate  │
  └──────────────────────────┴────────────┴──────────┴───────────┘

  ─────────────────────────────────────────────────────────────────
  Least Surveyed Watersheds (Highest Data Gap)
  ─────────────────────────────────────────────────────────────────

  ┌──────────────────────────┬──────────────┬───────────────────┐
  │ Watershed                │ Observations │ Richness          │
  ├──────────────────────────┼──────────────┼───────────────────┤
  │ MWS_071                  │  3           │  2                │
  │ MWS_064                  │  7           │  5                │
  │ MWS_029                  │ 11           │  8                │
  └──────────────────────────┴──────────────┴───────────────────┘

  These watersheds may be biodiversity-rich but under-surveyed.
  They are candidates for priority field surveys.

  ─────────────────────────────────────────────────────────────────
  Conservation Hotspots
  ─────────────────────────────────────────────────────────────────

  12 watersheds in this block have at least one IUCN-threatened
  species record. These should be treated as sensitive areas during
  any watershed development planning.

  [Table: MWS_031, MWS_008, MWS_059, ... threatened_species_count ≥ 1]

  ─────────────────────────────────────────────────────────────────
  Block-Level Taxonomic Profile
  ─────────────────────────────────────────────────────────────────

  Averaged across all MWS in this block:

  ┌────────────────────────┬────────────────────────────────────┐
  │ Taxonomic Group        │ Avg. Species Count per MWS         │
  ├────────────────────────┼────────────────────────────────────┤
  │ Birds (Aves)           │ 12.4                               │
  │ Plants (Plantae)       │  8.1                               │
  │ Mammals (Mammalia)     │  4.2                               │
  │ Insects (Insecta)      │  3.7                               │
  │ Reptiles (Reptilia)    │  2.9                               │
  │ Amphibians (Amphibia)  │  1.8                               │
  └────────────────────────┴────────────────────────────────────┘

  ─────────────────────────────────────────────────────────────────
  Data Quality Summary
  ─────────────────────────────────────────────────────────────────

  26% of watersheds in this block are data-poor (< 20 observations).
  These represent survey gaps, not confirmed absence of biodiversity.
  Interpret block-level averages with this limitation in mind.

  Source: GBIF.org · DOI: 10.15468/dl.xxxxxxxx · June 2025
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### Computations Required

All of these are derived from the `biodiversity` Excel sheet in Stage 8,
which already has all 19 columns per MWS. No additional GEE computation is
needed for the tehsil report.

```python
# In get_biodiversity_pattern_data(state, district, block):

df = pd.read_excel(xl_path, sheet_name="biodiversity")

total_mws       = len(df)
with_data       = int((df["occurrence_count"] > 0).sum())
data_poor_count = int(df["data_poor"].sum())
total_obs       = int(df["occurrence_count"].sum())
avg_richness    = round(float(df["species_richness"].mean()), 1)
median_richness = round(float(df["species_richness"].median()), 0)
avg_shannon     = round(float(df["shannon_diversity_index"].mean()), 2)
threatened_mws  = int((df["threatened_species_count"] > 0).sum())

# Top 5 by richness
top5 = df.nlargest(5, "species_richness")[
    ["UID", "species_richness", "shannon_diversity_index", "biodiversity_category"]
].to_dict("records")

# Worst 3 by observation count (data gap)
data_gaps = df.nsmallest(3, "occurrence_count")[
    ["UID", "occurrence_count", "species_richness"]
].to_dict("records")

# Conservation hotspots
hotspots = df[df["threatened_species_count"] > 0][
    ["UID", "threatened_species_count"]
].to_dict("records")

# Category distribution
category_counts = df["biodiversity_category"].value_counts().to_dict()

# Per-taxon averages
taxon_averages = {
    "birds":      round(float(df["bird_species_count"].mean()), 1),
    "plants":     round(float(df["plant_species_count"].mean()), 1),
    "mammals":    round(float(df["mammal_species_count"].mean()), 1),
    "reptiles":   round(float(df["reptile_species_count"].mean()), 1),
    "amphibians": round(float(df["amphibian_species_count"].mean()), 1),
    "insects":    round(float(df["insect_species_count"].mean()), 1),
}
```

---

## 10. Future Enhancements

These are NOT part of V1 or V2. They are listed here so the pipeline design
accounts for them in data model and architecture decisions.

### 10.1 Temporal Diversity Trends

**What:** Species richness over time — is this watershed gaining or losing
species year-over-year based on GBIF records?

**Why important:** Detects biodiversity change driven by land degradation or
restoration interventions.

**Why not V1:** Requires `year` / `month` from GBIF's `eventDate` field, which
is not currently in `OUTPUT_COLS`. Adding it requires no architecture change —
just add `"year"` to `OUTPUT_COLS` in `clean.py` and `gee_upload.py`. Once
present in the GEE asset, temporal aggregation is straightforward.

**Pipeline readiness:** Medium. Requires 2–3 years of GBIF snapshots.

---

### 10.2 IUCN Species List (by Name)

**What:** The names of threatened species recorded in each watershed, not
just the count.

**Why important:** "3 threatened species" is useful. "Indian Rock Python (EN),
Indian Pangolin (EN), Great Indian Bustard (CR)" is actionable.

**Why not V1:** GEE exports aggregated statistics, not per-species lists.
Exporting species lists requires a different GEE export structure (a separate
Export.table with one row per species per MWS, then a local join). This is
a non-trivial schema change.

**How to implement in V2:** Add a second GEE export per block:
`species_list_{district}_{block}.geojson` — one row per MWS × species
combination for threatened species only. Join to main stats locally.

---

### 10.3 Habitat-Stratified Diversity

**What:** Species richness broken down by habitat type within the MWS
(forest, grassland, wetland, agriculture). Requires LULC overlay.

**Why important:** An MWS might have 50 species total but all concentrated
in the 5% that is forest. The rest is agricultural monoculture. This would
not be visible from overall richness.

**Why not V1:** Requires cross-joining GBIF FeatureCollection with LULC
raster in GEE. Complex computation. Needs LULC layer to be a GEE ImageCollection.
This is a V3 feature that leverages other CoRE Stack modules.

---

### 10.4 Endemic Species Count

**What:** Species that are endemic to India or the Western Ghats specifically.

**Why important:** Endemism is a major conservation priority. India has ~7,000
endemic plant species. Identifying endemic-rich watersheds is critical for
conservation zoning.

**Why not V1:** Requires an external endemic species list (e.g., IUCN, ENVIS).
Not available directly from GBIF fields.

---

### 10.5 Invasive Species Flag

**What:** Detection of invasive species (Lantana camara, Parthenium
hysterophorus, etc.) in the watershed.

**Why important:** Invasive plants are a major driver of native biodiversity
loss in Indian watersheds. Their presence is a priority management concern.

**Why not V1:** Requires a curated invasive species list by `taxonKey`. The
GBIF data does not flag records as invasive. Requires an external lookup table.

---

### 10.6 Biodiversity Change Detection

**What:** Comparing GBIF snapshots from two different download dates to
identify which species have disappeared or appeared in a watershed.

**Why important:** Directly measures the impact of CoRE Stack interventions
on biodiversity.

**Why not V1:** Requires multiple temporal downloads. Not feasible until the
pipeline has been running for at least 2–3 years.

---

### 10.7 Conservation Priority Score

**What:** A composite score combining species richness, threatened species,
IUCN categories, endemism, and data quality into a single ranking.

**Why important:** Enables direct "which MWS needs conservation investment
most?" ranking across a district.

**Why not V1:** Composite scoring requires careful calibration, scientific
validation, and stakeholder agreement on weighting. Rushing this produces
misleading rankings. Design the score properly in V3 after the pipeline
has produced data for at least one full district.

---

## 11. Priority Tables

---

### Table 1 — Must Have (Version 1)

These indicators must be in the first production release. They are the minimum
for the biodiversity module to be meaningfully different from a GBIF data viewer.

| Indicator | Computation | EE Complexity | Compute Cost | Usefulness | Priority |
|---|---|---|---|---|---|
| `species_richness` | `aggregate_count_distinct("taxonKey")` | Low | Very Low | Very High — primary metric | P0 |
| `occurrence_count` | `.size()` | Low | Very Low | Very High — survey effort proxy | P0 |
| `data_poor` | `n.lt(20)` | Low | Very Low | Very High — prevents misleading conclusions | P0 |
| `threatened_species_count` | filter iucnRedListCategory + `aggregate_count_distinct` | Low | Low | Very High — most actionable output | P0 |
| `shannon_diversity_index` | histogram → `−Σ pᵢ ln(pᵢ)` | Medium | Low | High — standard ecological metric | P1 |
| `simpson_diversity_index` | same histogram → `1 − Σ pᵢ²` | Medium | Very Low (reuses Shannon histogram) | High — intuitive diversity probability | P1 |
| `pielou_evenness` | `H / ln(S)` | Low (derived) | Very Low | High — detects single-species dominance | P1 |
| `rare_species_count` | `count histogram values == 1` | Medium | Low | High — quality indicator | P1 |
| `bird_species_count` | filter Aves + `aggregate_count_distinct` | Low | Low | High — best-documented group in India | P1 |
| `mammal_species_count` | filter Mammalia + `aggregate_count_distinct` | Low | Low | High — habitat health indicator | P1 |
| `plant_species_count` | filter Plantae + `aggregate_count_distinct` | Low | Low | High — vegetation diversity | P1 |
| `reptile_species_count` | filter Reptilia + `aggregate_count_distinct` | Low | Low | Medium | P2 |
| `amphibian_species_count` | filter Amphibia + `aggregate_count_distinct` | Low | Low | High — water quality proxy | P1 |
| `insect_species_count` | filter Insecta + `aggregate_count_distinct` | Low | Low | Medium — pollinator proxy | P2 |
| `dominant_class` | argmax over per-class counts (local) | N/A | Very Low | High — quick characterization | P1 |
| `biodiversity_category` | threshold on species_richness (local) | N/A | Very Low | High — plain-language summary | P1 |
| `observation_density_per_km2` | occurrence_count / area_km2 (local) | N/A | Very Low | High — normalised effort | P1 |

**Pipeline change required for V1 (one line):**

```python
# In clean.py, add "iucnRedListCategory" to REQUIRED_COLS and OUTPUT_COLS
OUTPUT_COLS = [
    "gbifID", "taxonKey", "species", "kingdom", "class",
    "decimalLatitude", "decimalLongitude", "stateProvince",
    "iucnRedListCategory",   # ADD THIS
]
```

---

### Table 2 — Good to Have (Version 2)

These add analytical depth but are not blocking for V1.

| Indicator | What requires | EE Complexity | Compute Cost | Usefulness | Notes |
|---|---|---|---|---|---|
| `dominant_species` | Per-species occurrence export + local join | Medium | Medium | High — identifies characteristic species | Needs separate GEE export schema |
| Threatened species names (list) | Per-species export for threatened only | Medium | Medium | Very High — actionable conservation | Needs separate GeoJSON schema |
| `other_species_count` | Sum of all per-class counts | Very Low | Very Low | Medium — completeness indicator | Easy, low cost |
| Temporal species trends | Add `year` to OUTPUT_COLS | Medium | High (multiple snapshots) | Very High | Need 2+ years of data |
| `local_richness_rank` | Post-block computation (rank within block) | N/A | Low | Medium | Adds cross-MWS context |
| Butterfly species count | Filter order=Lepidoptera (add `order` to OUTPUT_COLS) | Low | Low | High for pollinator context | Needs `order` field added |
| Conservation hotspot flag | `threatened_species_count > 0` (already have this) | None | None | High | Just a derived bool from V1 data |

---

### Table 3 — Future Research Features

These require significant additional work, external data, or multi-year
data collection. Do NOT plan architecture to accommodate them now except
where noted.

| Feature | External Data Required | EE Complexity | Timeline | Notes |
|---|---|---|---|---|
| IUCN threatened species names | None (can derive from V1 data) | Low | V2 | Just a schema change — list export per block |
| Endemic species count | IUCN / ENVIS endemic species list | Medium | V3 | Needs maintained endemic taxonKey list |
| Invasive species flag | Curated invasive taxonKey list | Low | V3 | Relatively easy once list is available |
| Habitat-stratified diversity | LULC raster from CoRE Stack | High | V3 | Cross-join GBIF FC with LULC Image in GEE |
| Biodiversity Change Detection | 2+ temporal GBIF downloads | Medium | V3+ | Needs multi-year pipeline maturity |
| Conservation Priority Score | Composite design + stakeholder validation | N/A | V4 | Design after real data is available |
| Species Distribution Models | WorldClim, elevation, habitat | Very High | Research | Requires ecological modelling expertise |
| Beta Diversity (turnover between MWS) | Cross-MWS computation | High | V4 | Requires full block data before computing |
| Habitat Suitability Index | MaxEnt or BRT models | Very High | Research | Out of scope for CoRE Stack |

**Architecture note:** The V1 pipeline cleanly supports V2/V3 enhancements
without structural changes. The block-first GEE asset approach (one FC per
block) means all future enhancements simply add more properties to
`compute_stats()` or add a second `Export.table` call in `mws_statistics.py`.
The Excel, KYL, and report pipeline is additive — new columns in the Excel
sheet automatically become available in KYL without schema changes.
