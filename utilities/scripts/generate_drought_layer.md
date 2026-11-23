# generate_drought_layer.py

Builds the pan-India kharif drought layer from the inputs that
[download_drought_inputs.py](download_drought_inputs.md) stores, into `data/base_layers/drought/`.
Each season gets the weeks of mild, moderate and severe drought on cropland and the weeks of dry spell,
on the 500 m grid and per micro-watershed (MWS).
The method is described in the method note (CoRE v3 writeup section 3.2.2.1 with the rules of the
Manual for Drought Management, updated to December 2020).

The script can be stopped at any time. The next run skips what is finished and continues, and later
runs add seasons whose inputs have arrived.

| Stage | What it makes | Grid |
|---|---|---|
| `rain` | Weekly rainfall since 1981; per season, against 1981 to the year before it: onset, dry spells, each window's rainfall trigger | CHIRPS 0.05° (all of India, always) |
| `cropland` | Kharif-sown share per season from the 10 m LULC v4 (plantation counted as tree), the 2017-2024 normal, the cropland mask, area sown | 500 m MODIS |
| `vegetation` | Highest NDVI and NDWI per window for every year from 2000, VCI per season | 500 m MODIS |
| `mai` | ET / PET per window, and the window ET and PET | 500 m MODIS |
| `classify` | Weekly labels, the season summary, and an EPSG:4326 copy of the summary | 500 m MODIS |
| `mws` | The indicators aggregated over each MWS, then the 2020 rules; one GeoPackage per season | the 260,072 MWS of the hydrology tehsil files |

## Before you start

1. Download the inputs with [download_drought_inputs.py](download_drought_inputs.md).
2. The cropland stage reads the temporally corrected LULC v4 in
   `data/base_layers/periodic/lulc_v4_temporal_corrected/`, which the `pan-india/lulc_temporal_correction_six_year/`
   and `.../incremental/` APIs make: one file per year,
   `<prefix>_<year>-07-01_<year+1>-06-30_temporal_corrected.tif`, and `temporal_correction_manifest.json`.
   The newest year is provisional until the next year's raw layer is added; a season made with it is made
   again, by the next run, once the incremental API has finalised it.
3. The `mws` stage needs the hydrology tehsil watersheds in
   `data/base_layers/hydrology/runoff/commons/tehsil_watersheds/`, with `tehsil_watershed_manifest.csv`.
4. Check free disk space: a pan-India run needs about 70 GB, of which about 25 GB is the MWS layers.

## Usage

Run these from the repo folder. The repo is mounted at `/app` and the data at `$DATA_DIR`, so no rebuild is needed.

1. See which seasons can run:

   ```bash
   docker compose exec backend python utilities/scripts/generate_drought_layer.py --status
   ```

   Each season shows `ready`, `waiting` (with the missing input, such as
   `LULC_v4_new_panindia_2026-07-01_2027-06-30_temporal_corrected.tif`), or `done`. A `done` season also says
   whether its MWS layer is made, and `LULC provisional` when its LULC year is not final yet.

2. Try one MODIS tile first (about 15 minutes; outputs go to `drought/tiles/h25v07/`):

   ```bash
   docker compose exec backend python utilities/scripts/generate_drought_layer.py --tile h25v07 --seasons 2023
   ```

   The first run of the `mws` stage, with or without `--tile`, also copies the hydrology MWS once into
   `drought/v1/mws/` (a few minutes). A tile run covers the MWS that lie wholly in the tile.

3. Run all of India (about 1.5 hours, plus the MWS layers), keeping a copy of the output in a log:

   ```bash
   docker compose exec backend sh -c 'python utilities/scripts/generate_drought_layer.py 2>&1 | tee -a $DATA_DIR/drought_layer.log'
   ```

   It ends with the status table, with every season whose inputs exist as `done`.

To stop it, press Ctrl+C. To continue, run the same command again.
When new inputs arrive (for example a new LULC year), run it again: only the new season is computed.
When a LULC file is replaced (a provisional year finalised, or the layers made again), the next run makes
the cropland files made from it again, and `classify` and `mws` follow. The first run after the move from
LULC v3 to v4 therefore remakes the cropland stage, the normal included, for every season.
When a stage's output is rewritten (for example after a change to the rainfall rules), the next run
classifies the affected seasons again and remakes their MWS layers.
To remake only the MWS layers, add `--stages mws`.

MAI files from before October 2026 have no window ET and PET, which the `mws` stage needs. The next full
run rewrites them, then classifies those seasons again (the classes do not change).

## Output

```
data/base_layers/drought/
├── status.json                         which seasons and stages are done, per region
├── v1/                                 pan-India, method version 1
│   ├── rain/                           weekly/<year>.tif, seasons/<year>.tif
│   ├── cropland/                       share_<year>.tif, normal_2017_2024.tif
│   ├── vegetation/                     window_max/<year>.tif, vci_<year>.tif
│   ├── moisture/                       mai_<year>.tif
│   ├── classified/                     labels_<year>.tif, summary_<year>.tif
│   ├── published/drought_<year>.tif    the summary in EPSG:4326, for GeoServer
│   └── mws/                            watersheds.gpkg, pixels.npz (the MWS and their pixels, made once),
│                                       drought_mws_<year>.gpkg (the MWS layer)
└── tiles/<tile>/v1/                    the same, for a --tile run
```

`published/drought_<year>.tif` has one byte per value (255 = no data) and these bands:

| Band | Meaning |
|---|---|
| `weeks_none`, `weeks_mild`, `weeks_moderate`, `weeks_severe` | Weeks in each class, cropland pixels only |
| `season_weeks` | Weeks from onset to week 43 (22-28 October) |
| `dry_spell_weeks`, `longest_dry_spell_weeks` | Weeks inside dry spells of 4 or more weeks, and the longest one (all pixels) |
| `onset_week` | IMD standard week of monsoon onset |
| `cropland` | 1 where kharif crop is the most common land category (2017-2024 average) |
| `windows_cloud_fallback` | Windows whose VCI had to use cloud-flagged images |

### MWS layer

`mws/drought_mws_<year>.gpkg` has one row per MWS, in EPSG:4326, with the MWS of the hydrology layers
(same `uid`), so a tehsil layer can be cut out by `uid`. Each MWS takes the 500 m pixels whose centre lies
in it (a very small MWS takes the pixels it touches). The indicators are aggregated over the MWS first,
and the 2020 rules then classify the MWS itself:

- rain, normal and SPI: the mean over the MWS's pixels, each pixel taking its CHIRPS cell's value
- onset: the median of the pixels' onset days
- VCI: the mean over the MWS's cropland pixels
- MAI: the sum of ET divided by the sum of PET over the cropland pixels
- area sown: the MWS's kharif-sown share this season as % of its 2017-2024 average

The column names are those of the GEE yearly drought asset (`drought_<district>_<block>_<year>_v2`), not
the short names of the GEE layer on GeoServer (`drlb_`, `drysp_`, `w_mod_`, `rd…`, which its merge made).

**Season columns**, one set per season:

| Column | Meaning | Short name in GEE layers |
|---|---|---|
| `uid`, `area_in_ha`, `bacode`, `sbcode`, `wsconc` | From the hydrology MWS | |
| `drought_labels_<year>` | Weekly classes from the onset week to week 43, as a JSON list: 0 none, 1 mild, 2 moderate, 3 severe | `drlb_` |
| `number_of_weeks_in_{no,mild,moderate,severe}_drought_<year>` | Weeks of no, mild, moderate and severe drought | `w_no_` … `w_sev_` |
| `freq_of_drought_<year>_at_threshold_0` to `_3` | Weeks of class 0, 1, 2, 3 or worse | `frth0_` … `frth3_` |
| `intensity_of_drought_<year>_at_threshold_0` to `_3` | Mean class of those weeks | `inth0_` … `inth3_` |
| `total_weeks_<year>` | Weeks from the onset week to week 43 | `t_wks_` |
| `monsoon_onset_<year>` | Onset date, such as `2023-6-21` | `m_ons_` |
| `total_dryspell_weeks_<year>` | Weeks inside dry spells (4 or more dry weeks in a row) from onset; not in the GEE layer | `dsw_` (local only) |
| `dryspell_length_<year>` | The longest dry spell, in weeks | `drysp_` |
| `kharif_cropped_sqkm_<year>` | Kharif-sown area, km² | `kh_cr_` |
| `percent_of_area_cropped_kharif_<year>` | Area sown, % of the MWS's 2017-2024 average (in GEE: % of the area ever sown in kharif) | `pcr_k_` |

**Window columns**, for each 4-week window. `<date>` is the window's start, `<year>-5-21` to `<year>-10-1`,
the same 20 dates every year, and every window is always there (in a few seasons the earliest are empty for
every MWS). The windows of 7 and 14 May have no columns: a window counts from the MWS's onset week, and
onset is never before 22 May (the onset rule's Kerala date, week 21), so they are never counted. The
classification itself still runs on all 22 windows from 7 May, where those two always come out as no drought.

| Column | Meaning |
|---|---|
| `weekly_label_<date>` | Drought class of the week starting that day: 0 none, 1 mild, 2 moderate, 3 severe |
| `dryspell_<date>` | 1 if the window is a dry spell, else 0 |
| `monthly_rainfall_deviation_<date>` | Rainfall deviation of the window, % of normal (GEE layer: `rd<yy>-<m>-<d>`) |
| `rfdev_class_<date>` | `Normal rf` (-19% or above), `Deficit rf` (below -19% to above -60%), `Scanty rf` (-60% or worse, the 2020 trigger) |
| `spi_<date>` | SPI of the window |
| `meteorological_drought_<date>` | 1 if dry spell, rainfall 60% or more below normal, or SPI below -1.5 |
| `vci_<date>` | VCI, mean over the MWS's cropland pixels |
| `et_<date>`, `pet_<date>` | ET and PET of the window, mm, mean over the cropland pixels with both (GEE's are sums over pixels) |
| `mai_<date>` | MAI, %: ET / PET over those pixels |

All of them are empty for a window that starts before the MWS's onset week, as GEE has no windows before
onset; `weekly_label`, VCI, ET, PET and MAI also where the MWS has no cropland.

The values the rules compare with a threshold (rainfall deviation, SPI, VCI, MAI and
`percent_of_area_cropped_kharif`) are stored as computed, not rounded, so they always agree with the
rainfall class, trigger, drought classes and causality beside them. Rounded to 2 decimals, an SPI of
-1.5004 would read -1.50 next to a trigger that fired. ET and PET are rounded to 2 decimals.

**Drought causality**, per season:

| Column | Meaning |
|---|---|
| `severe_moderate_drought_causality_<year>` | The three most frequent combinations among the windows classed moderate or severe, with how many windows had each |
| `mild_drought_causality_<year>` | The same among the windows classed mild |

A combination is the window's class, the triggers that fired in it and the class of each impact, for
example:

```json
{"moderate: scanty rain; VCI moderate, MAI severe, area sown moderate": 3,
 "severe: dry spell + scanty rain + SPI; VCI severe, MAI severe, area sown moderate": 1}
```

- The windows are classed by the same 2020 rule as the drought layer: the triggers are dry spell, scanty rain
  (-60% or worse) and SPI below -1.5; each impact (VCI, MAI, area sown) is severe, moderate or normal.
- Every trigger that fired is listed, in the order dry spell, scanty rain, SPI; all three impacts are listed.
- Counts are windows. Ties go to the worse class, then by text.
- Both columns are filled only for a drought year of the MWS, more than 5 moderate + severe weeks (the
  causality writeup's rule), and are empty otherwise. In a drought year without mild windows the mild
  column is `{}`.
- The GEE causality layer named them `se_mo_<year>` and `mild_<year>` and used numbered paths and scores.

The tehsil drought layer on GeoServer carries the season columns and `monthly_rainfall_deviation_<date>`,
39 per season, plus `avg_dryspell`; the tehsil drought causality layer carries `uid`, `area_in_ha` and the
two causality columns. The other window values stay in the pan-India layer.

An MWS without cropland pixels has the class columns (`drought_labels`, `number_of_weeks_in_…`, `freq_of_…`,
`intensity_of_…`) and causality empty (about 50,000 of 260,072). The 523 MWS of the Andaman and Nicobar
Islands lie outside the grid, which covers mainland India only, and have every drought column empty. The
hydrology layers leave out 77 of them (Campbell Bay), so they have 259,995 MWS.

Files written from the container are owned by root on the host.
`rain/normals.tif` and `rain/spi_fit.tif` from runs before October 2026 (fixed 1991-2020 normals) are no
longer used and can be deleted.

## Size and time

Measured on tile h25v07 with 16 workers and the uint8 LULC; a pan-India run is about 4 times larger.

| Stage | Tile h25v07 | Pan-India (estimate) | Disk, pan-India |
|---|---|---|---|
| `rain` | 3 min | 3 min (always pan-India) | 1.3 GB |
| `cropland` | 9 min | about 40 min | about 0.5 GB |
| `vegetation` | 3 min | about 25 min | about 30 GB |
| `mai` | 1 min | about 4 min | about 9 GB (1 GB per season) |
| `classify` | 1 min | about 8 min | about 0.6 GB |
| `mws` | under 1 min | about 1.5 min per season, and a few minutes once for the MWS copy | about 25 GB (about 2.8 GB per season, and a 2.6 GB copy of the MWS) |

The largest stages hold up to about 20 GB in memory.

## Options

| Option | Default | |
|---|---|---|
| `--status` | | show each season's state and exit |
| `--seasons` | 2017 to this year | e.g. `2017-2024`, `2023` or `2018,2023` |
| `--stages` | all | e.g. `rain,cropland` or `mws` |
| `--tile` | all of India | one MODIS tile, e.g. `h25v07` |
| `--workers` | 16 (or the number of cores) | parallel processes |
| `--lulc-dir` | `lulc_v4_temporal_corrected` in `computing/config.yaml` | folder with `<prefix>_<year>-07-01_<year+1>-06-30_temporal_corrected.tif` |
| `--data-dir` | `$DATA_DIR`, else `./data` | data folder |

## Problems

| Message | Fix |
|---|---|
| `Another run is using ... drought` | another run is still going; check with `docker compose exec backend pgrep -af generate_drought_layer` |
| `modis_india_mask.tif not found` | run the download script first |
| A season stays `waiting` | its detail names the missing input; download it, then run again |
| `LULC not found: ..._temporal_corrected.tif` | the LULC year is missing from the LULC folder (run the temporal-correction APIs), or `--lulc-dir` points to the wrong folder |
| `MWS <year>: mai_<year>.tif has no window ET and PET yet` | run with the `mai` stage too (a full run does it) |
| `MWS stage skipped: ... tehsil_watershed_manifest.csv not found` | the hydrology tehsil watersheds are missing (see Before you start) |
| `Could not find written pan-India watersheds`, or `MWS: <n> watersheds copied` with far fewer than 260,072 | the manifest's paths do not point to the tehsil files; check `tehsil_watershed_manifest.csv` |

To rebuild the MWS copy (for example after the hydrology watersheds change without a new manifest), delete
`drought/v1/mws/watersheds.gpkg`; the next run makes it again and remakes every MWS layer.

## Tests

```bash
docker compose exec backend python -m unittest computing.drought.local.test_rules computing.drought.local.test_rainfall \
    computing.drought.local.test_cropland computing.drought.local.test_vegetation \
    computing.drought.local.test_moisture computing.drought.local.test_classify computing.drought.local.test_mws
```
