# Biodiversity (GBIF) Module — Flow Diagrams

Mermaid data-flow diagrams for the block-first, GEE-native biodiversity layer.
Four levels of zoom:

| Level                                 | Scope           | Question it answers                                         |
| ------------------------------------- | --------------- | ----------------------------------------------------------- |
| [HLD](#hld--system-context)            | System context  | Which systems talk to each other?                           |
| [Level 1](#level-1--pipeline-stages)   | Pipeline stages | What are the 6 stages and what flows between them?          |
| [Level 2](#level-2--stage-internals)   | Stage internals | What does each stage do, function by function?              |
| [Level 3](#level-3--call-level-detail) | Call level      | Exactly which API/EE calls fire, with payloads and timings? |

The diagrams are written for **any block**, not a specific one. Symbols used throughout:

| Symbol                         | Meaning                                                              |
| ------------------------------ | -------------------------------------------------------------------- |
| `<state>/<district>/<block>` | the block being processed                                            |
| **N_raw**                | occurrence records GBIF returns for the block bbox (before cleaning) |
| **N_clean**              | records surviving the 5 cleaning filters                             |
| **S**                    | distinct species among the clean records                             |
| **M**                    | MWS polygons in the block                                            |

Timings are **order-of-magnitude ranges**, not guarantees: they scale with block size, record
volume, and GBIF/GEE queue depth. See [Reference run](#reference-run) for one concrete measurement.

---

## HLD — System Context

Five external systems. The Django/Celery worker is the only thing that talks to all of them.

```mermaid
flowchart LR
    UI["Frontend / curl<br/>POST /api/v1/generate_biodiversity_layer/"]

    subgraph CORE["CoRE Stack backend"]
        API["Django REST API<br/>computing/api.py"]
        Q(["RabbitMQ<br/>queue = nrm"])
        W["Celery worker<br/>generate_biodiversity_block"]
        DB[("PostgreSQL<br/>Layer / Dataset / LayerInfo")]
        FS[("Local disk cache<br/>_data/ + _cache/")]
    end

    GBIF["GBIF.org<br/>Download API + Species API"]
    GEE["Google Earth Engine<br/>assets + compute"]
    GS["GeoServer<br/>workspace: biodiversity"]
    DOWN["Excel / KYL / DPR reports<br/>stats_generator + dpr"]

    UI -->|"HTTP 200 immediately"| API
    API -->|"apply_async"| Q --> W
    W <-->|"1. occurrences + IUCN"| GBIF
    W <-->|"2. MWS polygons, 3. spatial join, 4. asset"| GEE
    W -->|"5. publish GeoPackage + SLD"| GS
    W -->|"6. Layer row + misc provenance"| DB
    W <--> FS
    DB --> DOWN
    GS --> DOWN
    GS -->|"WMS / WFS tiles"| UI
```

**Contract at each boundary**

| Boundary            | Direction | Payload                                                       |
| ------------------- | --------- | ------------------------------------------------------------- |
| UI → API           | in        | `{state, district, block, gee_account_id}`                  |
| Worker → GBIF      | out       | predicate download request (all taxa, block bbox)             |
| GBIF → Worker      | in        | zipped`SIMPLE_CSV` (N_raw records) + citable DOI            |
| Worker → GEE       | out       | N_clean point features + M MWS polygons                       |
| GEE → Worker       | in        | per-MWS indicator FeatureCollection (19 properties × M)      |
| Worker → GeoServer | out       | GeoPackage +`biodiversity_mws` SLD                          |
| Worker → DB        | out       | `Layer` row, `misc` = GBIF provenance (DOI, keys, counts) |

---

## Level 1 — Pipeline Stages

The six stages of `generate_biodiversity_block`. Note the two caches and the one idempotency
gate — these are what make a re-run cheap.

```mermaid
flowchart TD
    START([POST → Celery task<br/>state, district, block, gee_account_id]) --> S0

    S0["<b>Stage 0 — Init</b><br/>assert GBIF_USER/PWD/EMAIL<br/>ee_initialize(gee_account_id)<br/>⏱ seconds"]

    S0 --> S1["<b>Stage 1 — Download</b><br/>gbif_download.py<br/>bbox from MWS asset → GBIF Download API<br/>⏱ minutes–tens of minutes cold · instant cached"]
    S1 -->|"occurrences_raw.csv (N_raw rows)<br/>+ meta{doi, key, date, raw_record_count}"| S2

    S2["<b>Stage 2 — Clean + Enrich</b><br/>gbif_clean.py + gbif_iucn.py<br/>5 filters, then IUCN per taxonKey<br/>⏱ ~0.4 s × uncached species · seconds when cached"]
    S2 -->|"DataFrame: N_clean rows · S species<br/>+ iucnRedListCategory column"| GATE

    GATE{"<b>is_gee_asset_exists</b><br/>biodiversity_&lt;district&gt;_&lt;block&gt;?"}
    GATE -->|"yes — skip compute"| S5
    GATE -->|"no"| S3

    S3["<b>Stage 3 — Upload to GEE</b><br/>gdf_to_ee_fc(gdf)<br/>4 props only, NaN-free<br/>⏱ scales with N_clean"]
    S3 -->|"ee.FeatureCollection<br/>N_clean points"| S4

    S4["<b>Stage 4 — Compute + Export</b><br/>gbif_mws_stats.py<br/>Join.saveAll → 19 indicators → toAsset<br/>⏱ minutes (M × N_clean)"]
    S4 -->|"GEE asset: M MWS features"| S5

    S5["<b>Stage 5 — Register</b><br/>save_layer_info_to_db + make_asset_public<br/>⏱ seconds"]
    S5 -->|"layer_id"| S6

    S6["<b>Stage 6 — Publish</b><br/>sync_fc_to_geoserver → GeoPackage + SLD<br/>update_layer_sync_status<br/>⏱ under a minute"]
    S6 --> END([return 'biodiversity done … layer_id=… synced=… doi=…'])

    C1[("_data/&lt;state&gt;/&lt;district&gt;/&lt;block&gt;/<br/>occurrences_raw.csv + meta.txt")] -.->|cache| S1
    C2[("_cache/iucn_by_taxonkey.json")] -.->|cache| S2

    style GATE fill:#fff3cd,stroke:#856404
    style C1 fill:#e7f3ff,stroke:#0366d6
    style C2 fill:#e7f3ff,stroke:#0366d6
```

**Cold vs warm run** — what each re-run actually costs:

| Run                              | Path                      | Dominated by                    |
| -------------------------------- | ------------------------- | ------------------------------- |
| Cold (first ever)                | all stages                | GBIF download-queue wait        |
| Warm (CSV cached, asset missing) | skip the GBIF download    | GEE spatial join + table export |
| Idempotent (asset exists)        | skip Stages 3–4 entirely | GeoServer publish only — cheap |

---

## Level 2 — Stage Internals

### Stage 1 — Download (`gbif_download.py`)

```mermaid
flowchart TD
    A["download_block_occurrences(state, district, block)"] --> B{"_data/…/occurrences_raw.csv<br/>exists?"}
    B -->|yes| C["_read_meta(meta.txt)<br/>→ return (csv, meta)"]
    B -->|no| D["get_block_bbox_wkt()"]

    D --> D1["ee.FeatureCollection(filtered_mws_&lt;d&gt;_&lt;b&gt;_uid)<br/>.geometry().bounds().getInfo()"]
    D1 --> D2["min/max lon-lat + 0.01° buffer<br/>→ POLYGON((minlon minlat, maxlon minlat, …))"]

    D2 --> E["request_block_download(bbox_wkt)"]
    E --> E1["pygbif occ.download(queries=[…5 predicates…],<br/>format='SIMPLE_CSV')<br/>→ download_key"]

    E1 --> F["wait_and_fetch(key)"]
    F --> F1{"occ.download_meta(key)['status']"}
    F1 -->|PREPARING / RUNNING| F2["sleep 60 s"] --> F1
    F1 -->|"KILLED / CANCELLED / FAILED"| FX["raise RuntimeError"]
    F1 -->|SUCCEEDED| G["occ.download_get() → &lt;key&gt;.zip<br/>zipfile.extractall → &lt;key&gt;.csv"]

    G --> H["os.replace → occurrences_raw.csv<br/>write meta.txt {download_key, doi,<br/>download_date, raw_record_count}"]
    H --> I(["(csv_path, meta) — N_raw records"])
    C --> I

    style FX fill:#f8d7da,stroke:#721c24
```

**The 5 GBIF predicates** — this is the *only* place taxon scope is decided. There is no
class/kingdom filter here, so **all taxa** are downloaded (birds are counted later, in GEE,
into `bird_species_count` — they are not filtered out at download).

```python
"hasCoordinate = TRUE"                                    # must have lat/lon
"hasGeospatialIssue = FALSE"                              # drop land-species-in-ocean etc.
"occurrenceStatus = PRESENT"                              # no absence records
"basisOfRecord in " + json.dumps(KEEP_BASIS_OF_RECORD)    # HUMAN_OBSERVATION, PRESERVED_SPECIMEN,
                                                          # MACHINE_OBSERVATION, OBSERVATION
f"geometry within {bbox_wkt}"                             # block bbox from the MWS asset
```

The bbox comes from the block's own MWS asset, so "where is this block" has a single source of
truth. A bbox (not the exact MWS union) is deliberate — GBIF's geometry predicate wants a simple
polygon, and precise per-MWS assignment happens later in the GEE spatial join.

### Stage 2 — Clean + Enrich (`gbif_clean.py` → `gbif_iucn.py`)

```mermaid
flowchart TD
    R["read_csv(sep='\t', usecols=13 cols)<br/>N_raw rows"] --> F1

    F1["<b>Filter 1</b> dropna(species, taxonKey, lat, lon)"]
    F1 --> F2["<b>Filter 2</b> inside INDIA_BBOX (68, 6.5, 97.5, 37.6)<br/>and not exactly (0,0)"]
    F2 --> F3["<b>Filter 3</b> coordinateUncertainty ≤ MAX_COORD_UNCERTAINTY_M (10 km)<br/>(NaN allowed through)"]
    F3 --> F4["<b>Filter 4</b> drop centroid piles<br/>&gt; CENTROID_DUP_THRESHOLD records on one exact coord"]
    F4 --> F5["<b>Filter 5</b> drop_duplicates(species, lat, lon)"]
    F5 --> OUT["N_clean rows · S distinct species"]

    OUT --> I1["enrich_with_iucn(df)"]
    I1 --> I2["S distinct taxonKeys<br/>minus _cache/iucn_by_taxonkey.json"]
    I2 --> I3["for each missing key:<br/>GET api.gbif.org/v1/species/{key}/iucnRedListCategory"]
    I3 --> I4["normalise via _SHORT map<br/>CRITICALLY_ENDANGERED → CR, VULNERABLE → VU, …"]
    I4 --> I5["persist cache · attach iucnRedListCategory column"]
    I5 --> RES(["records tagged LC / NT / VU / EN / CR / DD / ''<br/>VU+EN+CR ⇒ threatened_species_count"])

    style F1 fill:#fff3cd
    style F2 fill:#fff3cd
    style F3 fill:#fff3cd
    style F4 fill:#fff3cd
    style F5 fill:#fff3cd
```

Why the IUCN hop exists at all: `iucnRedListCategory` is **not** in GBIF's SIMPLE_CSV, so without
this step `threatened_species_count` would always be 0. The cache is keyed on `taxonKey` globally
(a species' Red List category doesn't vary by block), so the second block onward pays almost nothing.

### Stages 3–4 — GEE compute (`gbif_mws_stats.py`)

```mermaid
flowchart TD
    subgraph LOCAL["Client-side (worker)"]
        P["df[['taxonKey','kingdom','class','iucnRedListCategory']]<br/>taxonKey → str · fillna('')"]
        P --> G["gpd.GeoDataFrame(points_from_xy, EPSG:4326)"]
        G --> U["gdf_to_ee_fc(gdf) — one ee.Feature per record"]
    end

    subgraph EE["Server-side (Earth Engine)"]
        U --> J
        M["load_mws_featurecollection()<br/>filtered_mws_&lt;d&gt;_&lt;b&gt;_uid — M polygons"] --> J

        J["<b>ee.Join.saveAll</b>(matchesKey='gbif_occurrences')<br/>ee.Filter.intersects(.geo, maxError=10)"]
        J --> MAP["joined.map(compute_stats)"]

        MAP --> H["aggregate_histogram('taxonKey')<br/>→ counts per species per MWS"]
        H --> IDX["shannon · simpson · pielou<br/>richness = aggregate_count_distinct<br/>rare = species with count == 1"]
        MAP --> T["6 × filter(class/kingdom).count_distinct<br/>Aves · Mammalia · Reptilia<br/>Amphibia · Insecta · Plantae"]
        MAP --> TH["filter(inList(iucn, THREATENED_IUCN_CATEGORIES))<br/>→ threatened_species_count"]
        T --> D["dominant_class = argmax over the 6 counts"]
        IDX --> CAT["biodiversity_category<br/>richness thresholds: Very Low → Very High"]
        MAP --> DEN["observation_density_per_km2 = n / (area_in_ha / 100)"]
        MAP --> DP["data_poor = occurrence_count &lt; MIN_RECORDS"]

        IDX --> FR
        D --> FR
        CAT --> FR
        TH --> FR
        DEN --> FR
        DP --> FR
        FR["ee.Feature(geom, props) — FRESH feature<br/>detaches the join's List<Feature>"]

        Z["MWS with 0 points are DROPPED by the join<br/>→ re-added as explicit zeros, data_poor = 1"]
        FR --> MG["stats.merge(zeros)<br/>.select(_OUTPUT_PROPERTIES) — 19 static columns"]
        Z --> MG
    end

    MG --> EX["export_vector_asset_to_gee<br/>Export.table.toAsset"]
    EX --> CTS["check_task_status([task_id])<br/>polls ee.data.listOperations every 60 s"]
    CTS --> AS[("GEE asset<br/>…/biodiversity_&lt;district&gt;_&lt;block&gt;<br/>M features")]

    style J fill:#d4edda,stroke:#155724
    style FR fill:#d4edda,stroke:#155724
```

> **Why `Join.saveAll` and not `reduceRegions`?** Rasterizing the points would destroy `taxonKey`,
> and species identity is exactly what richness / Shannon / rare-species need. The join keeps every
> point's attributes attached to its MWS.

> **Why a fresh `ee.Feature` and a static `_OUTPUT_PROPERTIES`?** `Export.table.toAsset` needs one
> resolvable schema. Carrying the join's `gbif_occurrences` list, or letting the matched and
> zero-record branches emit different EE types, produced `Unsupported table schema: Type<Feature>`.
> Explicit `.toInt()` / `.toFloat()` / `ee.String()` casts in **both** branches fixed it.

### Stages 5–6 — Register + Publish

```mermaid
flowchart TD
    A["save_layer_info_to_db(...)"] --> A1["Dataset.objects.get('Biodiversity Occurrence')<br/>StateSOI / DistrictSOI / TehsilSOI lookup"]
    A1 --> A2["Layer upsert (version-aware)<br/>algorithm='GBIF_GEE_BLOCK_JOIN' v2.0"]
    A2 --> A3["<b>Layer.misc</b> = {gbif_doi, download_key,<br/>taxon_scope: 'all', raw_record_count,<br/>clean_record_count, download_date}"]
    A3 --> B["make_asset_public(asset_id)<br/>ACL all_users_can_read = True"]

    B --> C["sync_fc_to_geoserver(ee.FeatureCollection(asset_id), …)"]
    C --> C1{"fc.getInfo() succeeds?"}
    C1 -->|yes| C2["GeoJSON in memory"]
    C1 -->|"too large / timeout"| C3["fallback: sync_vector_to_gcs → GeoJSON<br/>→ get_geojson_from_gcs"]
    C2 --> D["GeoDataFrame → fix_invalid_geometry<br/>→ <b>.gpkg</b> (NOT shapefile:<br/>avoids 10-char field truncation,<br/>e.g. species_richness → species_ri)"]
    C3 --> D
    D --> E["Geoserver().create_featurestore + publish<br/>workspace = 'biodiversity'<br/>style = 'biodiversity_mws' SLD"]
    E --> F["update_layer_sync_status(layer_id, sync_to_geoserver=True)"]
    F --> G(["WMS/WFS live · Excel + KYL + DPR can read it"])

    style D fill:#d4edda,stroke:#155724
```

`Layer.misc` carries only the GBIF provenance that cannot be derived from `Layer` / `Dataset` /
`LayerInfo` — mirroring how `change_detection` stores just its qualifying years.

---

## Level 3 — Call-Level Detail

Every external call the pipeline makes, in order.

```mermaid
sequenceDiagram
    autonumber
    participant UI as Client
    participant API as Django API
    participant MQ as RabbitMQ (nrm)
    participant W as Celery worker
    participant FS as Disk cache
    participant GBIF as GBIF.org
    participant EE as Earth Engine
    participant PG as PostgreSQL
    participant GS as GeoServer

    UI->>API: POST /api/v1/generate_biodiversity_layer/<br/>{state, district, block, gee_account_id}
    API->>MQ: apply_async(queue="nrm")
    API-->>UI: 200 {"Success": "biodiversity task initiated"} (returns immediately)
    MQ->>W: generate_biodiversity_block

    Note over W: Stage 0 — init
    W->>W: assert GBIF_USER / GBIF_PWD / GBIF_EMAIL
    W->>EE: ee_initialize(gee_account_id) — service-account auth

    Note over W,GBIF: Stage 1 — download
    W->>FS: exists(_data/&lt;state&gt;/&lt;district&gt;/&lt;block&gt;/occurrences_raw.csv)?
    alt cache hit
        FS-->>W: csv + meta.txt (instant)
    else cache miss
        W->>EE: FC("…/filtered_mws_&lt;d&gt;_&lt;b&gt;_uid").geometry().bounds().getInfo()
        EE-->>W: block bbox → WKT POLYGON
        W->>GBIF: POST /occurrence/download (5 predicates, SIMPLE_CSV)
        GBIF-->>W: download_key
        loop poll every 60 s
            W->>GBIF: GET /occurrence/download/{key} → status
            GBIF-->>W: PREPARING → RUNNING → SUCCEEDED
        end
        Note right of GBIF: the long pole: GBIF queue depth
        W->>GBIF: GET download_get → &lt;key&gt;.zip
        W->>FS: unzip → occurrences_raw.csv (N_raw rows)
        W->>GBIF: GET download_meta → citable DOI + totalRecords
        W->>FS: write meta.txt
    end

    Note over W,GBIF: Stage 2 — clean + IUCN
    W->>W: clean_occurrences() — 5 filters → N_clean rows, S species
    W->>FS: load _cache/iucn_by_taxonkey.json
    loop each uncached taxonKey (≤ S cold, 0 fully warm)
        W->>GBIF: GET /v1/species/{taxonKey}/iucnRedListCategory
        GBIF-->>W: {"category": "LEAST_CONCERN"} → "LC"
    end
    W->>FS: persist cache

    Note over W,EE: Idempotency gate
    W->>EE: is_gee_asset_exists(".../biodiversity_&lt;district&gt;_&lt;block&gt;")
    alt asset already exists
        EE-->>W: true — SKIP Stages 3–4
    else asset missing
        Note over W,EE: Stage 3 — upload
        W->>EE: gdf_to_ee_fc — N_clean ee.Feature (taxonKey, kingdom, class, iucn)
        Note over EE: Stage 4 — compute
        W->>EE: FC("filtered_mws_&lt;d&gt;_&lt;b&gt;_uid") — M polygons
        W->>EE: Join.saveAll + Filter.intersects(maxError=10)
        W->>EE: map(compute_stats) — 19 properties per MWS
        W->>EE: Export.table.toAsset(description=biodiversity_&lt;district&gt;_&lt;block&gt;)
        EE-->>W: task_id
        loop check_task_status — sleep 60 s, ee.data.listOperations()
            W->>EE: poll state
            EE-->>W: RUNNING → SUCCEEDED
        end
    end

    Note over W,PG: Stage 5 — register
    W->>PG: Dataset.get("Biodiversity Occurrence") · StateSOI/DistrictSOI/TehsilSOI
    W->>PG: Layer upsert → layer_id, misc = {gbif_doi, download_key, counts, date}
    W->>EE: make_asset_public(asset_id) — ACL all_users_can_read

    Note over W,GS: Stage 6 — publish
    W->>EE: ee.FeatureCollection(asset_id).getInfo()
    EE-->>W: GeoJSON — M features × 19 props
    W->>W: GeoDataFrame → fix_invalid_geometry → .gpkg
    W->>GS: create_featurestore + publish (workspace=biodiversity)
    W->>GS: apply style biodiversity_mws (SLD)
    W->>PG: update_layer_sync_status(layer_id, sync_to_geoserver=True)
    W-->>MQ: "biodiversity done for &lt;district&gt;_&lt;block&gt;: layer_id=… synced=… doi=…"
```

### External API calls — the complete list

| #  | System    | Call                                                     | When                 | Cost driver              |
| -- | --------- | -------------------------------------------------------- | -------------------- | ------------------------ |
| 1  | GEE       | `FC(filtered_mws_*_uid).geometry().bounds().getInfo()` | cache miss only      | fixed, seconds           |
| 2  | GBIF      | `POST /occurrence/download` (pygbif `occ.download`)  | cache miss only      | instant — returns a key |
| 3  | GBIF      | `GET /occurrence/download/{key}` (`download_meta`)   | poll, 60 s interval  | GBIF queue depth         |
| 4  | GBIF      | `download_get` → zip                                  | once                 | N_raw                    |
| 5  | GBIF      | `GET /v1/species/{taxonKey}/iucnRedListCategory`       | per uncached species | S (once, ever)           |
| 6  | GEE       | `ee.Asset(path).exists()`                              | every run            | fixed, <1 s              |
| 7  | GEE       | `gdf_to_ee_fc` upload                                  | asset missing        | N_clean                  |
| 8  | GEE       | `Export.table.toAsset` + `listOperations` polling    | asset missing        | M × N_clean             |
| 9  | GEE       | `ee.data.setAssetAcl` (`make_asset_public`)          | every run            | fixed                    |
| 10 | GEE       | `FeatureCollection(asset).getInfo()`                   | every run            | M                        |
| 11 | GeoServer | `create_featurestore` + `publish` + style            | every run            | M                        |

Only calls 1–5, 7 and 8 are avoidable — by the download cache, the IUCN cache, and the asset
idempotency gate respectively.

### The 19 exported properties (`_OUTPUT_PROPERTIES`)

```mermaid
flowchart LR
    subgraph ID["Identity — 2"]
        A["uid"]
        B["area_in_ha"]
    end
    subgraph AB["Abundance — 2"]
        C["species_richness"]
        D["occurrence_count"]
    end
    subgraph DIV["Diversity — 3"]
        E["shannon_diversity_index"]
        F["simpson_diversity_index"]
        G["pielou_evenness"]
    end
    subgraph CONS["Conservation — 2"]
        H["rare_species_count"]
        I["threatened_species_count"]
    end
    subgraph TAX["Taxonomy — 6"]
        J["bird_species_count"]
        K["mammal_species_count"]
        L["reptile_species_count"]
        M["amphibian_species_count"]
        N["insect_species_count"]
        O["plant_species_count"]
    end
    subgraph DER["Derived — 4"]
        P["dominant_class"]
        Q["biodiversity_category"]
        R["observation_density_per_km2"]
        S["data_poor"]
    end
```

All 19 are computed **server-side in GEE** — nothing is post-processed in pandas after the export.

### Reuse map — what is ours vs what already existed

```mermaid
flowchart TD
    subgraph NEW["New code — computing/gbif/ (6 files)"]
        N1["config.py — thresholds, workspace, asset id"]
        N2["gbif_download.py"]
        N3["gbif_clean.py"]
        N4["gbif_iucn.py"]
        N5["gbif_mws_stats.py"]
        N6["biodiversity_task.py — orchestrator"]
    end

    subgraph REUSED["Reused CoRE Stack infrastructure — zero new abstractions"]
        R1["utilities/gee_utils: ee_initialize · gdf_to_ee_fc<br/>is_gee_asset_exists · export_vector_asset_to_gee<br/>check_task_status · make_asset_public<br/>get_gee_asset_path · valid_gee_text"]
        R2["computing/utils: save_layer_info_to_db<br/>sync_fc_to_geoserver · update_layer_sync_status"]
        R3["Models: Layer · Dataset · LayerInfo · Layer.misc<br/>(no GBIFBlockDownload model)"]
        R4["nrm_app.celery @app.task(bind=True), queue='nrm'"]
        R5["stats_generator: create_excel_for_biodiversity<br/>mws_indicators KYL keys"]
        R6["dpr: gen_mws_report · gen_tehsil_report"]
    end

    N6 --> R1
    N6 --> R2
    N6 --> R3
    N6 --> R4
    R3 --> R5 --> R6

    style REUSED fill:#e7f3ff,stroke:#0366d6
```

---

## Failure modes

```mermaid
flowchart TD
    X1["Missing GBIF_USER/PWD/EMAIL"] --> Y1["RuntimeError at Stage 0 — fail fast, no GEE cost"]
    X2["GBIF download KILLED / CANCELLED / FAILED"] --> Y2["RuntimeError in wait_and_fetch"]
    X3["IUCN lookup times out"] --> Y3["defaults to '' — counted as NOT threatened (silent undercount)"]
    X4["MWS with 0 GBIF points"] --> Y4["dropped by the join → re-added as zeros, data_poor = 1"]
    X5["MWS with fewer than MIN_RECORDS"] --> Y5["data_poor = 1 — 'cannot assess', never '0 species'"]
    X6["fc.getInfo() too large"] --> Y6["sync_fc_to_geoserver falls back via GCS"]
    X7["GeoServer down"] --> Y7["Layer row still written; is_sync_to_geoserver stays False — re-run publishes"]

    style Y3 fill:#f8d7da,stroke:#721c24
    style Y7 fill:#fff3cd,stroke:#856404
```

The two worth knowing: an **IUCN timeout silently undercounts threatened species** (no retry, no
warning), and a **GeoServer outage leaves the DB row present but unsynced** — recoverable, because
a re-run hits the idempotency gate and only redoes Stages 5–6.

---

## Reference run

One concrete measurement, kept for calibration only — the diagrams above are deliberately
block-agnostic because these numbers vary by an order of magnitude between blocks.

| Quantity                      | Value                                                       |
| ----------------------------- | ----------------------------------------------------------- |
| Block                         | `bihar / jamui / jamui`, all taxa                         |
| M (MWS polygons)              | 324                                                         |
| N_raw (downloaded records)    | 21,859                                                      |
| N_clean (after 5 filters)     | 9,086                                                       |
| S (distinct species)          | 364                                                         |
| IUCN breakdown                | LC 8,520 · NT 307 · unknown 127 · VU 118 · EN 9 · DD 5 |
| Threatened species (VU+EN+CR) | 9                                                           |
| `gdf_to_ee_fc` upload       | ~20 s for 9,086 points                                      |
| GBIF download (queue + fetch) | ~10 min                                                     |
| GEE join +`toAsset` export  | ~5 min                                                      |
