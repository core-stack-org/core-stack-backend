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
| `cropland` | Kharif-sown share per season, the 2017-2024 normal, the cropland mask, area sown | 500 m MODIS |
| `vegetation` | Highest NDVI and NDWI per window for every year from 2000, VCI per season | 500 m MODIS |
| `mai` | ET / PET per window, and the window ET and PET | 500 m MODIS |
| `classify` | Weekly labels, the season summary, and an EPSG:4326 copy of the summary | 500 m MODIS |
| `mws` | The indicators aggregated over each MWS, then the 2020 rules; one GeoPackage per season | the 260,072 MWS of the hydrology tehsil files |

## Before you start

1. Download the inputs with [download_drought_inputs.py](download_drought_inputs.md).
2. Use the uint8 LULC. The float64 originals in `data/base_layers/lulc/` make the cropland stage several
   times slower. Either finish the swap in [convert_lulc_to_uint8.md](convert_lulc_to_uint8.md), or pass
   `--lulc-dir $DATA_DIR/base_layers/lulc_uint8` to every command below.
3. The `mws` stage needs the hydrology tehsil watersheds in
   `data/base_layers/hydrology/runoff/commons/tehsil_watersheds/`, with `tehsil_watershed_manifest.csv`.
4. Check free disk space: a pan-India run needs about 70 GB, of which about 25 GB is the MWS layers.

## Usage

Run these from the repo folder. The repo is mounted at `/app` and the data at `$DATA_DIR`, so no rebuild is needed.

1. See which seasons can run:

   ```bash
   docker compose exec backend python utilities/scripts/generate_drought_layer.py --status
   ```

   Each season shows `ready`, `waiting` (with the missing input, such as `lulc_v3_2025_2026.tif`),
   or `done`. A `done` season also says whether its MWS layer is made.

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
When new inputs arrive (for example `lulc_v3_2025_2026.tif`), run it again: only the new season is computed.
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

The column names follow the GEE drought layer:

| Column | Meaning |
|---|---|
| `uid`, `area_in_ha`, `bacode`, `sbcode`, `wsconc` | From the hydrology MWS |
| `drlb_<year>` | Weekly classes from the onset week to week 43, as a JSON list: 0 none, 1 mild, 2 moderate, 3 severe |
| `w_no_<year>`, `w_mld_<year>`, `w_mod_<year>`, `w_sev_<year>` | Weeks of no, mild, moderate and severe drought |
| `frth0_<year>` to `frth3_<year>` | Weeks of class 0, 1, 2, 3 or worse |
| `inth0_<year>` to `inth3_<year>` | Mean class of those weeks |
| `t_wks_<year>` | Weeks from the onset week to week 43 |
| `m_ons_<year>` | Onset date, such as `2023-6-21` |
| `dsw_<year>` | Weeks inside dry spells (4 or more dry weeks in a row) from onset; not in the GEE layer |
| `drysp_<year>` | The longest dry spell, in weeks |
| `kh_cr_<year>` | Kharif-sown area, km² |
| `pcr_k_<year>` | Area sown, % of the MWS's 2017-2024 average (in GEE: % of the area ever sown in kharif) |
| `rd<yy>-<m>-<d>` | Rainfall deviation, %, of the 4-week window starting that day (windows starting in weeks 19 to 40) |

An MWS without cropland pixels has the class columns (`drlb`, `w_`, `frth`, `inth`) empty (about 59,000 of
260,072). The 523 MWS of the Andaman and Nicobar Islands lie outside the grid, which covers mainland India
only, and have every drought column empty. The hydrology layers leave out 77 of them (Campbell Bay), so
they have 259,995 MWS.

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
| `--lulc-dir` | the LULC folder in `computing/config.yaml` | folder with `lulc_v3_<year>_<year+1>.tif` |
| `--data-dir` | `$DATA_DIR`, else `./data` | data folder |

## Problems

| Message | Fix |
|---|---|
| `Another run is using ... drought` | another run is still going; check with `docker compose exec backend pgrep -af generate_drought_layer` |
| `modis_india_mask.tif not found` | run the download script first |
| A season stays `waiting` | its detail names the missing input; download it, then run again |
| `LULC not found: lulc_v3_...` | the LULC year is missing from the LULC folder, or `--lulc-dir` points to the wrong folder |
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
