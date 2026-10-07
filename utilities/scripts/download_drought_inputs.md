# download_drought_inputs.py

Downloads the rainfall and MODIS inputs of the drought layer and keeps India only, under
`data/base_layers/periodic/`. It can be stopped at any time: the next run skips finished
files and continues, and later runs fetch only what has been published since.

| Folder | Data | Dates | Source |
|---|---|---|---|
| `rainfall_chirps/` | CHIRPS v2.0 daily rainfall (mm) | 1 Jan 1981 → latest day | Climate Hazards Center |
| `mod09a1/` | MODIS MOD09A1 v061 8-day reflectance: b01, b02, b04, b05, b06, StateQA | 2000 → latest, composites starting 1 May – 3 Dec | NASA LP DAAC |
| `mod16a2gf/` | MODIS MOD16A2GF v061 8-day ET and PET | 2017 → latest year, composites starting 1 May – 3 Dec | NASA LP DAAC |

## Usage

The repo is mounted at `/app` and the data at `$DATA_DIR` (the host's `./data`), so no rebuild is needed.

1. Add an Earthdata login to `nrm_app/.env`. MODIS needs it; CHIRPS does not.
   Create a free account at https://urs.earthdata.nasa.gov, then add (keep the single quotes
   if the password contains `$`):

   ```dotenv
   USERNAME_GESDISC=your-username
   PASSWORD_GESDISC='your-password'
   ```

   It is the same login the ET download uses. The script reads `nrm_app/.env` itself, so no restart is needed.

2. Check the login and see what will be fetched:

   ```bash
   docker compose exec backend python utilities/scripts/download_drought_inputs.py --dry-run
   ```

   It prints `Earthdata login works`, then one line per CHIRPS year and one line per MODIS product,
   such as `MOD09A1: would fetch 744 composites, about 505 GB to download`.

3. Start the download in the background:

   ```bash
   docker compose exec -d backend sh -c 'python utilities/scripts/download_drought_inputs.py >> $DATA_DIR/drought_inputs_download.log 2>&1'
   ```

4. Follow it on the host:

   ```bash
   tail -f data/drought_inputs_download.log
   ```

   It logs a line per CHIRPS year and per MODIS composite, and ends with `Up to date.`

To stop it, run `docker compose exec backend pkill -f download_drought_inputs.py`. To continue, run step 3 again.
To add new data later (for example once a month), also run step 3 again.

If the login is missing or wrong, the script stops at the start and says what to fix.
Run with `--products chirps` to fetch CHIRPS in the meantime.

## Output

```
data/base_layers/periodic/
├── modis_india_mask.tif                       grid and India mask shared by both MODIS folders
├── rainfall_chirps/
│   ├── daily/<year>/chirps_<YYYY_MM_DD>.tif
│   └── manifest.json
├── mod09a1/
│   ├── <year>/MOD09A1_<YYYY_MM_DD>_<b01|b02|b04|b05|b06|state>.tif
│   ├── _raw/                                  HDF tiles of the composite in progress
│   └── manifest.json
└── mod16a2gf/
    ├── <year>/MOD16A2GF_<YYYY_MM_DD>_<ET|PET>.tif
    ├── _raw/
    └── manifest.json
```

| | CHIRPS | MODIS |
|---|---|---|
| Area | 67.6–99.4°E, 6–38°N. Covers the five monsoon-zone rectangles used for onset | India (`PanIndia_Boundaries/india_state_outer_no_islands.geojson`) + 10 km; the rest is no-data |
| Grid | EPSG:4326, 0.05°, the same grid GEE uses | MODIS sinusoidal, 463 m, tiles merged without resampling |
| Values | mm/day, no-data `-9999` | Raw integers as NASA stores them; the scale factor is in each file's tags (0.0001 reflectance, 0.1 ET/PET) |
| No-data | `-9999` | reflectance `-28672`, StateQA `65535`, ET/PET `32767`. ET/PET values above 32700 are class codes (water, urban, barren …), not data |
| Date in the name | the day | the first day of the 8-day composite (the same date GEE uses) |

The values are the same as the source files. A day or composite counts as done when all its files exist.
`manifest.json` lists what each folder holds, the latest date, and anything that failed.

Keep `modis_india_mask.tif`. If it is deleted, the next run builds it again from the boundary file, and if that
file has changed, new MODIS files land on a different grid. Delete it only together with `mod09a1/` and `mod16a2gf/`.

Files written from the container are owned by root on the host.

## Size and time

| | Download | Kept on disk |
|---|---|---|
| CHIRPS | about 9 GB | about 4.5 GB |
| MOD09A1 | about 505 GB | about 88 GB |
| MOD16A2GF | about 36 GB | about 10 GB |

MODIS files are deleted from `_raw/` once a composite is written, so the disk only needs the right-hand
column plus a few GB.

CHIRPS took 100 s for 243 days in a test, so all of it takes about 2 hours. MODIS time depends on the
connection: about 540 GB at 1 MB/s is about a week. If another machine has a faster connection,
run it there and copy `data/base_layers/periodic/` across.

New data appears about a month after the day for CHIRPS and about three weeks after for MOD09A1.
MOD16A2GF comes out once a year, early in the next year. NASA expects the Terra satellite, which
carries this MODIS, to stop around February 2027, so the MODIS folders stop growing after that.

## Options

| Option | Default | |
|---|---|---|
| `--products` | `chirps,mod09a1,mod16a2gf` | which folders to fill |
| `--dry-run` | | check the login and show what would be fetched, without downloading |
| `--data-dir` | `$DATA_DIR`, else `./data` | files go to `<data-dir>/base_layers/periodic/` |
| `--boundary` | `<data-dir>/base_layers/PanIndia_Boundaries/india_state_outer_no_islands.geojson` | India outline for the MODIS mask (used only when the mask is first built) |
| `--chirps-start-year` | `1981` | |
| `--mod09a1-start-year` | `2000` | VCI needs every year from 2000 |
| `--mod16a2gf-start-year` | `2017` | |
| `--end-year` | this year | last year to fetch |
| `--workers` | `4` | MODIS tiles downloaded in parallel |
| `--chirps-workers` | `8` | CHIRPS days fetched in parallel |
| `--keep-raw` | | keep the MODIS HDF files in `_raw/` |

It exits with `0` when everything is up to date, `1` when some files failed (they are in `manifest.json`;
run again to retry), and `2` when it could not start (login, a run already going, missing boundary file).

## Problems

| Message | Fix |
|---|---|
| `Earthdata login not found` | step 1 |
| `Earthdata rejected the username or password` | correct `USERNAME_GESDISC` / `PASSWORD_GESDISC`; check them by logging in at https://urs.earthdata.nasa.gov |
| `Earthdata returned a web page instead of the file` | log in at https://urs.earthdata.nasa.gov and accept any pending agreement or application approval |
| `… is in use by another run of this script` | another run is still going; check with `docker compose exec backend pgrep -af download_drought_inputs` |
| `This GDAL cannot read HDF4` | run it in the backend container (or the corestack conda environment), not a plain Python |
| `was not written with the expected CRS` | the container's PROJ setup is broken; run it on the host in the corestack conda environment instead |
