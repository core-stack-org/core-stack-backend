# 20th Livestock Census 2019: Village-Level Livestock Layer

## Summary

This dataset is a CoRE Stack village-level livestock layer derived from the 20th Livestock Census 2019. It reports five livestock species groups at rural village level:

- cattle
- buffalo
- sheep
- goat
- pig

Each species is represented by male, female, and total population counts. Cattle and buffalo are separate census categories: cattle refers to bovine cattle such as cows, bulls, bullocks, and calves; buffalo is counted separately and is not included under cattle.

The final village layer is keyed by LGD `village_code` corresponding to a matched rural village. 

**Geospatial Assets:**

- GeoPackage: [pan_india_livestock.gpkg](https://drive.google.com/file/d/1kLSMPVb0Iysg8Ms_ViomHK-vyxogv0HM/view?usp=sharing)
- Google Earth Engine asset: `projects/corestack-datasets/assets/datasets/pan_india_livestocks`

## Sources

### Primary Source

The primary source is the Department of Animal Husbandry and Dairying, Government of India village/ward-level workbook:

- Source page: https://dahd.gov.in/en/node/569
- File: https://dahd.gov.in/sites/default/files/2023-07/VillageAndWardLevelDataMale-Female.xlsx
- Page title: Village and Ward Level Data [Male & Female] - 20th LSC
- DAHD page section: Basic Animal Husbandry Statistics

The workbook has four sheets:

- Rural Male Population
- Rural Female Population
- Urban Male Population
- Urban Female Population

The rural sheets use:

```text
state_name, district_name, block_name, village_name, cattle, buffalo, sheep, goat, pig
```

The urban sheets use:

```text
state_name, district_name, town_name, ward_name, cattle, buffalo, sheep, goat, pig
```

### Secondary Enriched Source Used As Processing Base

For this pipeline, we use the ARTPARK/IISc public-data version as the processing base:

- Raw CSV: https://raw.githubusercontent.com/dsih-artpark/publicdata/refs/heads/main/data/0041/all-india-20th-livestock-census.csv
- Local file: `data/livestock/all-india-20th-livestock-census-artpark-iitm.csv`
- ARTPARK documentation page: https://publicdata.artpark.ai/en/latest/datasets/0041.html

ARTPARK harmonized the raw livestock census rows to a consistent CSV schema and added location IDs through district level. The local base file has the following columns:

```text
state.name
state.ID
district.name
district.ID
block.name
village.name
town.name
ward.name
location.type
population.cattle.male
population.buffalo.male
population.sheep.male
population.goat.male
population.pig.male
population.cattle.female
population.buffalo.female
population.sheep.female
population.goat.female
population.pig.female
```

### Administrative Reference Used For Village IDs

The village harmonization step uses Local Government Directory-compatible rural village and gram panchayat mapping data:

- Local reference: `data/livestock/gp_mapping.01Apr2026.csv`
- Main identifiers used: state code, district code, subdistrict code, village code, village name, census 2011 village code, local body code, and local body name.

## CoRE Stack Processing

The harmonized village-level layer is generated with:

```text
utilities/scripts/livestock/prepare_livestock.py
```

The script uses `data/livestock/all-india-20th-livestock-census-artpark-iitm.csv` as input, processes only `location.type = rural`, and emits:

- `data/livestock/processed/livestock_pan_india.csv`
- `data/livestock/processed/livestock_lgd_aligned.csv`
- `data/livestock/processed/livestock_lgd_alignment_all.csv`
- `data/livestock/processed/livestock_lgd_unmatched.csv`
- `data/livestock/processed/livestock_lgd_alignment_summary.json`
- `data/livestock/processed/livestock_lgd_unmatched_analysis.json`

### Simple Processing Flow

```text
DAHD village/ward livestock workbook
  -> ARTPARK/IISc harmonized CSV with IDs through district level
  -> CoRE Stack rural-only LGD matching
  -> unique village-level livestock CSV keyed by village_code
  -> admin boundary join for village geometries
  -> GeoPackage and Google Earth Engine asset
  -> GeoPackage QA and descriptive analysis files
```

### Matching Strategy

The source file already carries state and district IDs. The CoRE Stack script extends harmonization two levels deeper:

```text
district -> subdistrict/block/tehsil -> village
```

Important algorithmic choices:

- Rural-only filtering: urban ward rows are skipped for this village layer.
- Source row grouping: source rows that represent ward fragments or repeated base-village labels are grouped by district, block, and cleaned village label before matching. Counts are summed for the final village record.
- Local text normalization: names are cleaned for punctuation, suffixes such as CT/RV/OG, ward-number fragments, roman numerals, census numeric suffixes, and phonetic variants.
- Child-overlap subdistrict inference: a source block is matched to one or more LGD subdistricts using overlap between source child-village names and LGD child-village names.
- Boundary-split handling: same-state district split candidates are allowed only when bulk child-village overlap strongly supports the alternate LGD district.
- Multi-subdistrict blocks: where one source block appears to span multiple LGD subdistricts, the script keeps a short list of supported subdistrict candidates rather than forcing a single block winner.
- Staged one-to-one assignment: stronger village matches lock first, and weaker fallback stages can only fill still-unmatched source groups and unused LGD village IDs.
- Duplicate prevention: final output is constrained to one row per LGD `village_code`.
- Ambiguity handling: duplicate LGD village names inside the same subdistrict are left unmatched when the source row does not contain enough information to select one code safely.

## Processing Result

The current processed rural layer has:

```text
total_source_rows: 683626
rural_source_rows: 604301
urban_source_rows_skipped: 79325
source_village_groups: 588299
rural_output_rows: 513599
rural_matched_rows: 524612
rural_unmatched_rows: 79689
rural_match_rate: 0.86813
duplicate_village_codes_in_output: 0
```

The match rate is calculated against rural source rows. The final row count is lower than matched source rows due to some mismatches.

## Descriptive Analysis Files

Descriptive QA files for the GeoPackage are generated with:

```text
uv run --with pandas --with numpy python utilities/scripts/livestock/analyze_livestock_gpkg.py
```

This produces:

- `data/livestock/processed/livestock_gpkg_analysis.json`: machine-readable summary of input files, GeoPackage join coverage, CSV-vs-GPKG totals, and spatial-metric notes.
- `data/livestock/processed/livestock_metric_percentiles.csv`: clean village-level metric summary for livestock counts, derived totals, shares, and R-tree bounding-box dimensions.
- `data/livestock/processed/livestock_district_spatial_variation_metrics.csv`: district-level variation table with livestock distribution and approximate spatial spread metrics.

The percentile CSV reports `p02`, `p10`, `p25`, `median`, `mean`, `p75`, `p90`, and `p98`. These are easier to interpret than only showing minimum and maximum values. For example, `p90` means 90 percent of matched villages have a value at or below that number, while `p98` helps show high but non-extreme values without being dominated by rare outliers.

The district variation CSV includes:

- coefficient of variation: a scale-free measure of unevenness; higher values mean more village-to-village variation inside the district.
- Gini coefficient: a concentration score from 0 to 1, where values closer to 1 mean livestock counts are concentrated in fewer villages.
- `p90_p10_ratio` and `p98_p02_ratio`: simple upper-to-lower distribution ratios.
- `top_10pct_villages_livestock_share`: share of a district's livestock held by the highest-count 10 percent of matched villages.
- R-tree envelope width, height, area, and density estimates: approximate spatial spread metrics based on GeoPackage bounding boxes.

### What The R-tree Means

A GeoPackage stores an internal spatial index called an R-tree. It is like a fast lookup table of map rectangles: for each village geometry, it stores the smallest rectangle that contains the village polygon. This lets software quickly answer spatial questions such as "which features are near this area?" without opening every full polygon first.

The analysis uses this R-tree for helpful spatial summaries such as approximate district spread and matched-village density. These values are useful for QA and broad comparison, but they are not official area measurements. They use bounding rectangles, not exact village polygon area.

## Final Output Schema

The primary processed table is:

```text
data/livestock/processed/livestock_pan_india.csv
```

Columns:

```text
village_code
row_index
location_type
state_code
state_name
district_code
district_name
subdistrict_code
subdistrict_name
local_body_code
local_body_name
village_name
town_name
ward_code
ward_number
ward_name
cattle_male
cattle_female
cattle_total
buffalo_male
buffalo_female
buffalo_total
sheep_male
sheep_female
sheep_total
goat_male
goat_female
goat_total
pig_male
pig_female
pig_total
```

## Caveats

- Some source rows remain unmatched because village keys are absent in the admin layers.
- The processed CSV is the primary matched village-level table. The current local GeoPackage contains the geometry layer and joined livestock attributes as well.
- R-tree spatial metrics in the analysis files are approximate bounding-boxes around your geometry which let us scan the data by simply scanning index tree's indexed hashes itself.

When representing small, highly skewed rural data alongside massive aggregates, standard linear binning fails because a few highly concentrated dairy or pastoralist zones obliterate the variation among average villages.

---

## 1. Data-Driven Binning Recommendations

Based on the distribution metrics from the 2019 census data (where village livestock counts range from single digits to over 30,000, with a massive standard deviation of $\approx 495$ for cattle alone), we must use **nested geometric or quantile-based bins** rather than linear intervals.

### For Maps & Reports (Total Livestock / Species Sums)

To ensure the same color ramp works dynamically whether a user zooms into a single village or pans across an entire Tehsil, use **Head/Tail Breaks** or **Modified Logarithmic Quantiles**.

| Bin Level | Village-Level Range (Heuristic) | MWS / Tehsil Scale | Structural Meaning / Interpretation |
| --- | --- | --- | --- |
| **Bin 1: Critical Low** | $0 - 50$ animals | $0 - 500$ | Subsistence-deficit or highly urbanized/barren village. |
| **Bin 2: Low-Subsistence** | $51 - 200$ animals | $501 - 2,000$ | Smallholder family-level domestic asset holdings. |
| **Bin 3: Moderate Baseline** | $201 - 500$ animals | $2,001 - 5,000$ | Standard rural agrarian community baseline distribution. |
| **Bin 4: High-Density** | $501 - 1,500$ animals | $5,001 - 15,000$ | Intensified small-scale livestock rearing / pastoral community. |
| **Bin 5: Hyper-Concentration** | $> 1,500$ animals | $> 15,000$ | Commercial dairy belts, intensive pastoral tracts, or major clusters. |

> 📊 **Map Optimization Tip:** For choropleth maps across scales, normalize your data by **Livestock Density per $km^2$** or use **Gini-adjusted Quantiles** (using the Gini coefficient of $\approx 0.48 - 0.54$ present in your district data) to prevent large, sparsely populated villages from visually dominating the map layout.

---

## 2. UI Anatomy: The Unfolding Button & Emoji Set

To create a clean visual distinction within a compact UI or sidebar, choose emojis that maintain clear profile silhouettes even at small scales ($14px - 16px$).

### The Unfolded Hierarchy Layout

* **🟢 Level 1: Macro Button (Collapsed)**
* `[ 📦 Total Livestock: 352,410 ] ▼`


* **🔽 Level 2: Species Unfolded (Vertical Stack)**
* 🐄 **Cattle:** $162,441,896$ *(High Share)*
* 🦬 **Buffalo:** $93,464,317$ *(Moderate-High)*
* 🐑 **Sheep:** $64,843,913$ *(Localized)*
* 🐐 **Goat:** $127,707,393$ *(Ubiquitous)*
* 🐖 **Pig:** $6,447,823$ *(Low/Sparse)*



### Distinctive Animal Emoji Specifications

| Species | Selected Emoji | Hex Code | Visual Justification |
| --- | --- | --- | --- |
| **Cattle** | 🐄 | `U+1F404` | Full-profile cow allows instant recognition against buffalo. |
| **Buffalo** | 🦬 | `U+1F9A2` | Explicitly depicts distinct horn structure and dark silhouette. |
| **Sheep** | 🐑 | `U+1F411` | Woolly white silhouette contrasts sharply against goat/cattle. |
| **Goat** | 🐐 | `U+1F410` | Distinctive bearded profile, represents browse-heavy small ruminants. |
| **Pig** | 🐖 | `U+1F416` | Pink full-body profile stands out completely from ruminants. |

---

## 3. UI/UX Indicator Shifts for Sex Bifurcation

When the user dives deeper into a species to see the **Male vs. Female** split, dense text tables quickly cause cognitive overload. Instead, implement a **relative proportion bar** embedded right inside the row.

```
[▼] 🐄 Cattle: 162,441,896
    ├── ♂️ Male:   40,749,173   [██░░░░░░░░░░░░░░] 25.1% (Draft/Breeding)
    └── ♀️ Female: 121,692,723  [████████████░░░░] 74.9% (Dairy-Driven)

```

### UI Interaction Polish

1. **Asymmetric Opacity:** In Indian livestock distributions, female cattle and buffalo heavily dominate the population due to dairy imperatives. Make the dominant sex bar slightly more vibrant, while the minor sex uses a softer, higher-transparency shade.
2. **Contextual Tooltips:** Add micro-labels explaining the structural divergence (e.g., *“High female ratio indicates active milk-shed cluster; low male ratio indicates mechanized agriculture reducing draft animal reliance”*).

---

## 4. Hierarchical Decomposition Plot Selection

For an production-ready application dealing with village-level planning data, choose your plot layout based on whether your primary goal is **analytical utility** or **public engagement**:

### Option A: The Icicle Plot (Best for UI Sidebar Integration)

An icicle plot stacks layers horizontally or vertically. It maps perfectly to a slide-out dashboard menu because it preserves text readability.

```
+------------------------------------------------------------+
|                       TOTAL LIVESTOCK                      |
+------------+------------+------------+--------+------------+
|   Cattle   |   Buffalo  |    Goat    | Sheep  |    Pig     |
+---+--------+---+--------+---+--------+---+----+---+--------+
| ♂ |   ♀    | ♂ |   ♀    | ♂ |   ♀    | ♂ | ♀  | ♂ |   ♀    |
+---+--------+---+--------+---+--------+---+----+---+--------+

```

* **Why it works:** Unlike circular plots, text labels don't rotate or shear, making it easy to read actual numbers at the lowest level of hierarchy.

### Option B: The Radial Sunburst (Best for Map Popups)

If a user clicks a village on the map, a compact, floating Sunburst chart is highly intuitive.

* **Center Anchor:** The innermost circle displays the total livestock count.
* **First Ring (Species):** Divided into 5 colored wedges proportional to their total population share.
* **Outer Ring (Sex):** Each wedge splits into two sub-wedges (Male/Female).
* **Color Design Tip:** Keep one consistent hue family per species (e.g., shades of brown/orange for cattle, dark greys for buffalo, greens for goats). Divide the outer ring into lighter variants for males ($♂$) and deeper saturated tones for females ($♀$) to create an immediate visual pattern without needing a massive legend.


## Catalog Metadata JSON

```json
{
  "layername": "livestocks_census_2019",
  "layer_description": "Village-level rural livestock population counts from the 20th Livestock Census 2019 for cattle, buffalo, sheep, goat, and pig. The layer uses the ARTPARK/IISc district-harmonized CSV as a processing base, extends harmonization to village level using character and spatial matching, and contains rural level harmonisation with block, GP, Village ids added where match was successful.",
  "ee_layer_name": "projects/corestack-datasets/assets/datasets/pan_india_livestocks",
  "db_dataset_name": "Livestock Census 2019",
  "columns": [
    {
      "column_name": "village_code",
      "column_name_normalized": "village_code",
      "column_data_type": "integer",
      "column_name_description": "LGD village code used as the primary village identifier.",
      "comments": "Unique in the final processed layer."
    },
    {
      "column_name": "row_index",
      "column_name_normalized": "row_index",
      "column_data_type": "integer",
      "column_name_description": "Source CSV row index used for auditability.",
      "comments": "When multiple source fragments are grouped, this is the first source row in the grouped record."
    },
    {
      "column_name": "location_type",
      "column_name_normalized": "location_type",
      "column_data_type": "string",
      "column_name_description": "Location type retained from source.",
      "comments": "All records in this layer are rural."
    },
    {
      "column_name": "state_code",
      "column_name_normalized": "state_code",
      "column_data_type": "integer",
      "column_name_description": "LGD state code.",
      "comments": "Resolved from source district-level harmonization and LGD reference mapping."
    },
    {
      "column_name": "state_name",
      "column_name_normalized": "state_name",
      "column_data_type": "string",
      "column_name_description": "State or union territory name.",
      "comments": "LGD/reference-aligned name."
    },
    {
      "column_name": "district_code",
      "column_name_normalized": "district_code",
      "column_data_type": "integer",
      "column_name_description": "LGD district code.",
      "comments": "May reflect current LGD district after boundary-split matching."
    },
    {
      "column_name": "district_name",
      "column_name_normalized": "district_name",
      "column_data_type": "string",
      "column_name_description": "District name.",
      "comments": "LGD/reference-aligned name."
    },
    {
      "column_name": "subdistrict_code",
      "column_name_normalized": "subdistrict_code",
      "column_data_type": "integer",
      "column_name_description": "LGD subdistrict, tehsil, taluk, or equivalent rural administrative code.",
      "comments": "Resolved by district anchoring, name normalization, and child-village overlap."
    },
    {
      "column_name": "subdistrict_name",
      "column_name_normalized": "subdistrict_name",
      "column_data_type": "string",
      "column_name_description": "LGD subdistrict, tehsil, taluk, or equivalent rural administrative name.",
      "comments": "Reference-aligned name."
    },
    {
      "column_name": "local_body_code",
      "column_name_normalized": "local_body_code",
      "column_data_type": "integer",
      "column_name_description": "LGD local body or gram panchayat code linked to the village where available.",
      "comments": "Can be blank where the GP mapping has no local body link."
    },
    {
      "column_name": "local_body_name",
      "column_name_normalized": "local_body_name",
      "column_data_type": "string",
      "column_name_description": "Local body or gram panchayat name linked to the village where available.",
      "comments": "Can be blank where unavailable in the GP mapping."
    },
    {
      "column_name": "village_name",
      "column_name_normalized": "village_name",
      "column_data_type": "string",
      "column_name_description": "LGD village name.",
      "comments": "Reference-aligned village name."
    },
    {
      "column_name": "town_name",
      "column_name_normalized": "town_name",
      "column_data_type": "string",
      "column_name_description": "Town name field retained for schema compatibility.",
      "comments": "Blank for this rural village layer."
    },
    {
      "column_name": "ward_code",
      "column_name_normalized": "ward_code",
      "column_data_type": "integer",
      "column_name_description": "Ward code field retained for schema compatibility.",
      "comments": "Blank for this rural village layer."
    },
    {
      "column_name": "ward_number",
      "column_name_normalized": "ward_number",
      "column_data_type": "string",
      "column_name_description": "Ward number field retained for schema compatibility.",
      "comments": "Blank for this rural village layer."
    },
    {
      "column_name": "ward_name",
      "column_name_normalized": "ward_name",
      "column_data_type": "string",
      "column_name_description": "Ward name field retained for schema compatibility.",
      "comments": "Blank for this rural village layer."
    },
    {
      "column_name": "cattle_male",
      "column_name_normalized": "cattle_male",
      "column_data_type": "integer",
      "column_name_description": "Male cattle population.",
      "comments": "Cattle are bovine cattle; buffalo are counted separately."
    },
    {
      "column_name": "cattle_female",
      "column_name_normalized": "cattle_female",
      "column_data_type": "integer",
      "column_name_description": "Female cattle population.",
      "comments": "Cattle are bovine cattle; buffalo are counted separately."
    },
    {
      "column_name": "cattle_total",
      "column_name_normalized": "cattle_total",
      "column_data_type": "integer",
      "column_name_description": "Total cattle population.",
      "comments": "Computed as cattle_male + cattle_female."
    },
    {
      "column_name": "buffalo_male",
      "column_name_normalized": "buffalo_male",
      "column_data_type": "integer",
      "column_name_description": "Male buffalo population.",
      "comments": "Buffalo are counted separately from cattle."
    },
    {
      "column_name": "buffalo_female",
      "column_name_normalized": "buffalo_female",
      "column_data_type": "integer",
      "column_name_description": "Female buffalo population.",
      "comments": "Buffalo are counted separately from cattle."
    },
    {
      "column_name": "buffalo_total",
      "column_name_normalized": "buffalo_total",
      "column_data_type": "integer",
      "column_name_description": "Total buffalo population.",
      "comments": "Computed as buffalo_male + buffalo_female."
    },
    {
      "column_name": "sheep_male",
      "column_name_normalized": "sheep_male",
      "column_data_type": "integer",
      "column_name_description": "Male sheep population.",
      "comments": "Count from the 20th Livestock Census source."
    },
    {
      "column_name": "sheep_female",
      "column_name_normalized": "sheep_female",
      "column_data_type": "integer",
      "column_name_description": "Female sheep population.",
      "comments": "Count from the 20th Livestock Census source."
    },
    {
      "column_name": "sheep_total",
      "column_name_normalized": "sheep_total",
      "column_data_type": "integer",
      "column_name_description": "Total sheep population.",
      "comments": "Computed as sheep_male + sheep_female."
    },
    {
      "column_name": "goat_male",
      "column_name_normalized": "goat_male",
      "column_data_type": "integer",
      "column_name_description": "Male goat population.",
      "comments": "Count from the 20th Livestock Census source."
    },
    {
      "column_name": "goat_female",
      "column_name_normalized": "goat_female",
      "column_data_type": "integer",
      "column_name_description": "Female goat population.",
      "comments": "Count from the 20th Livestock Census source."
    },
    {
      "column_name": "goat_total",
      "column_name_normalized": "goat_total",
      "column_data_type": "integer",
      "column_name_description": "Total goat population.",
      "comments": "Computed as goat_male + goat_female."
    },
    {
      "column_name": "pig_male",
      "column_name_normalized": "pig_male",
      "column_data_type": "integer",
      "column_name_description": "Male pig population.",
      "comments": "Count from the 20th Livestock Census source."
    },
    {
      "column_name": "pig_female",
      "column_name_normalized": "pig_female",
      "column_data_type": "integer",
      "column_name_description": "Female pig population.",
      "comments": "Count from the 20th Livestock Census source."
    },
    {
      "column_name": "pig_total",
      "column_name_normalized": "pig_total",
      "column_data_type": "integer",
      "column_name_description": "Total pig population.",
      "comments": "Computed as pig_male + pig_female."
    }
  ]
}
```
