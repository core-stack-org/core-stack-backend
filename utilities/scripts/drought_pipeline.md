# Drought pipeline: how to run

Four steps make the kharif drought layer of a tehsil: download the inputs, make the pan-India layers,
call the tehsil API, and check the result in GeoServer. Every command runs on the server, from the repo
folder, with the Docker stack up. The method and its rules are described in the drought method doc.

| Step | What it does | Guide |
|---|---|---|
| 1. Download the inputs | CHIRPS daily rain, MOD09A1 reflectance, MOD16A2GF ET and PET, India only | [download_drought_inputs.md](download_drought_inputs.md) |
| 2. Make the pan-India layers | 500 m layer and one MWS layer per season | [generate_drought_layer.md](generate_drought_layer.md) |
| 3. Make a tehsil layer | cuts the tehsil's MWS out of the pan-India layers and publishes them | below |
| 4. Check it in GeoServer | | below |

## Before you start

1. Check that the stack is up: `docker compose ps` lists `backend`, `celery-nrm` and `celery-geoserver` as
   running. The repo is mounted at `/app` and the data at `$DATA_DIR` (the repo's `data/`), so a code change
   needs only a restart of those services, not a rebuild.
2. Add an Earthdata login to `nrm_app/.env`; the MODIS download needs it. Create a free account at
   https://urs.earthdata.nasa.gov, then add `USERNAME_GESDISC=<user>` and `PASSWORD_GESDISC='<password>'`
   (keep the quotes if the password has a `$`).
3. Check that the temporally corrected LULC v4 is in `data/base_layers/periodic/lulc_v4_temporal_corrected/`,
   one `<prefix>_<year>-07-01_<year+1>-06-30_temporal_corrected.tif` per year with
   `temporal_correction_manifest.json`. The `pan-india/lulc_temporal_correction_six_year/` and
   `.../incremental/` APIs make them; the drought layer's cropland mask and area sown come from them.
4. Check that the hydrology tehsil watersheds are in `data/base_layers/hydrology/runoff/commons/tehsil_watersheds/`,
   with `tehsil_watershed_manifest.csv`. The MWS layers use the same micro-watersheds as the hydrology layers.
5. Check disk space: the inputs keep about 105 GB and the outputs need about 70 GB, of which about 25 GB is
   the MWS layers.

## Step 1: Download the inputs

1. Check the login and see what will be fetched:

   ```bash
   docker compose exec backend python utilities/scripts/download_drought_inputs.py --dry-run
   ```

2. Start the download in the background:

   ```bash
   docker compose exec -d backend sh -c 'python utilities/scripts/download_drought_inputs.py >> $DATA_DIR/drought_inputs_download.log 2>&1'
   ```

3. Follow it with `tail -f data/drought_inputs_download.log`. It ends with `Up to date.`

A first download is about 550 GB, about a week at 1 MB/s; CHIRPS alone takes about 2 hours. If another
machine has a faster connection, download there and copy `data/base_layers/periodic/` across. To stop it,
run `docker compose exec backend pkill -f download_drought_inputs.py`; to continue, run step 2 again.

## Step 2: Make the pan-India layers

One script runs six stages in order: `rain`, `cropland`, `vegetation`, `mai` and `classify` make the 500 m
layer, and `mws` makes one layer per season with every micro-watershed. It skips what is done and leaves out
seasons whose inputs are missing, so the same command continues a stopped run.

1. See which seasons can run. Each shows `ready`, `waiting` (with the missing input) or `done`, and whether
   its MWS layer is made:

   ```bash
   docker compose exec backend python utilities/scripts/generate_drought_layer.py --status
   ```

2. Try one MODIS tile first (about 15 minutes; outputs go to `data/base_layers/drought/tiles/h25v07/`). The
   first run of the `mws` stage also copies the hydrology micro-watersheds once into
   `data/base_layers/drought/v1/mws/`, which takes a few minutes:

   ```bash
   docker compose exec backend python utilities/scripts/generate_drought_layer.py --tile h25v07 --seasons 2023
   ```

3. Run all of India, keeping a log. The 500 m stages take about 1.5 hours and the MWS layers about
   1.5 minutes per season:

   ```bash
   docker compose exec backend sh -c 'python utilities/scripts/generate_drought_layer.py 2>&1 | tee -a $DATA_DIR/drought_layer.log'
   ```

4. Run `--status` again: every season with inputs should read `done; MWS layer done`.

| Output | Where, under `data/base_layers/drought/v1/` |
|---|---|
| 500 m summary in EPSG:4326 | `published/drought_<year>.tif` |
| MWS layer, one per season | `mws/drought_mws_<year>.gpkg` |
| Stage files (rain, cropland, VCI, MAI, weekly labels) | `rain/`, `cropland/`, `vegetation/`, `moisture/`, `classified/` |

Useful options: `--seasons 2017-2024`, `--stages mws` (remake only the MWS layers), `--workers 16`.

## Step 3: Make a tehsil layer

The API cuts a tehsil's micro-watersheds out of the pan-India MWS layers and publishes them; nothing is
recomputed. It is the GEE drought endpoint, `POST /api/v1/generate_drought_layer/`, with `"compute": "local"`.
Without `compute` it still runs GEE.

Before the first call:

- After any code change, restart the services that run it:
  `docker compose restart backend celery-nrm celery-geoserver celery-layer-bulk`.
- Check whether production is set: `docker compose exec backend printenv PROD_GEOSERVER_URL PROD_BACKEND_URL`.
  If either prints a URL, the layer also goes to the production GeoServer and database and replaces the GEE
  layer of that tehsil there.

1. Get a login token for the superuser in `nrm_app/.env`:

   ```bash
   export no_proxy=localhost,127.0.0.1 NO_PROXY=localhost,127.0.0.1
   TOKEN=$(set -a; . nrm_app/.env; set +a; python3 -c '
   import json, os, urllib.request
   req = urllib.request.Request("http://localhost:8000/api/v1/auth/login/",
       data=json.dumps({"username": os.environ["DJANGO_SUPERUSER_USERNAME"],
                        "password": os.environ["DJANGO_SUPERUSER_PASSWORD"]}).encode(),
       headers={"Content-Type": "application/json"})
   print(json.load(urllib.request.urlopen(req))["access"])')
   ```

2. Call the API. `block` is the tehsil, named as in the tehsil watershed folders. `start_year` and
   `end_year` default to 2017 and the latest season:

   ```bash
   curl -s -X POST "http://localhost:8000/api/v1/generate_drought_layer/" \
     -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
     -d '{"compute":"local","state":"karnataka","district":"raichur","block":"devadurga","start_year":2017,"end_year":2024}'; echo
   ```

   It answers `{"Success":"generate_drought_layer task initiated"}` at once. If a season's pan-India layer
   is missing, it answers 400 with the season and queues nothing.

3. Follow the task with `docker compose logs -f celery-nrm celery-geoserver`.

| What it makes | Where |
|---|---|
| File | `data/drought/output/<state>/<district>/<tehsil>/<district>_<tehsil>_drought.gpkg` |
| GeoServer layer | `drought:<district>_<tehsil>_drought`, the same name GEE used |
| Database record | dataset Drought, algorithm `local_drought`, with `start_year` and `end_year` in `misc` |

The layer has the season columns and each window's `monthly_rainfall_deviation_<date>`, 39 per season,
under the long names of the GEE yearly drought asset (see the MWS layer table in
[generate_drought_layer.md](generate_drought_layer.md)); layers made by the GEE task have the short names
(`drlb_`, `drysp_`, `w_mod_`, `rd…`). The other window values and drought causality stay in the pan-India
layers. `avg_dryspell` is the mean of `dryspell_length_<year>` over the seasons. A micro-watershed without
cropland has `drought_labels_<year>` = `[]` and empty class columns.

### The drought causality layer

The causality API cuts the drought causality columns the same way: `POST /api/v1/mws_drought_causality/`
with `"compute": "local"` (without it, it still runs GEE). Call it after Step 2, with the same token and
location:

```bash
curl -s -X POST "http://localhost:8000/api/v1/mws_drought_causality/" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"compute":"local","state":"karnataka","district":"raichur","block":"devadurga","start_year":2017,"end_year":2025}'; echo
```

It answers 400 and queues nothing if a season's pan-India layer is missing or was made before drought
causality was added (run Step 2 with `--stages mws` first).

| What it makes | Where |
|---|---|
| File | `data/drought/causality/<state>/<district>/<tehsil>/<district>_<tehsil>_drought_causality.gpkg` |
| GeoServer layer | `drought_causality:<district>_<tehsil>_drought_causality`, the same name GEE used |
| Database record | dataset Drought Causality, algorithm `local_drought_causality` |

Columns: `uid`, `area_in_ha`, and per season `severe_moderate_drought_causality_<year>` and
`mild_drought_causality_<year>` (GEE: `se_mo_<year>`, `mild_<year>`): the three most frequent combinations of
window class, triggers and impact classes, filled only in a drought year of the micro-watershed (more than 5
moderate + severe weeks).

## Step 4: Check it in GeoServer

The server's GeoServer listens only on its own port 8080, so open a tunnel from your laptop first.

1. On your laptop, keep this running (it prints nothing while the tunnel is open; use another local port if
   18080 is taken). In VS Code connected to the server, forwarding port 8080 in the Ports panel does the same:

   ```bash
   ssh -N -L 18080:localhost:8080 corestk@act4dws11
   ```

2. Open http://localhost:18080/geoserver/web and log in with the GeoServer admin account from `nrm_app/.env`.
   If the page does not load, add `localhost` to the browser's no-proxy list.
3. Go to Layer Preview, search for `<district>_<tehsil>_drought`, open it with OpenLayers and click a
   micro-watershed to see its values.
4. To summarise the layer from the server's terminal:

   ```bash
   curl -s --noproxy '*' "http://localhost:8080/geoserver/drought/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=drought:raichur_devadurga_drought&outputFormat=application/json" \
     | python3 -c '
   import json, sys
   f = [x["properties"] for x in json.load(sys.stdin)["features"]]
   print(len(f), "MWS")
   for y in range(2017, 2026):
       mod, sev = f"number_of_weeks_in_moderate_drought_{y}", f"number_of_weeks_in_severe_drought_{y}"
       ms = [p[mod] + p[sev] for p in f if p.get(mod) is not None]
       print(y, "mean moderate+severe weeks", round(sum(ms) / len(ms), 1))'
   ```

   If the layer is not there, the reason is in `docker compose logs celery-geoserver`.

## New seasons and reruns

| When | Run |
|---|---|
| New CHIRPS or MODIS data is out (CHIRPS about a month after the day; MOD16A2GF once a year, early in the next year) | Step 1 |
| A new season's inputs are all on disk, including its LULC (`..._<year>-07-01_<year+1>-06-30_temporal_corrected.tif`) | Step 2: only the new season runs. Then Step 3 for each tehsil with the new `end_year` |
| A provisional LULC year is finalised (the incremental temporal-correction API, after the next year's raw layer) | Step 2: that season's cropland, classes and MWS layer are made again. Then Step 3 for the tehsils |
| The rainfall rules change (onset or normals) | Step 2: the rain stage redoes the seasons made under the old rule, and `classify` and `mws` follow |
| Another stage's code changes | Delete that stage's files for the seasons concerned (for example `v1/moisture/mai_2023.tif`), then Step 2 |
| The hydrology tehsil watersheds change | Step 2: a newer manifest rebuilds the MWS copy and every MWS layer. Without a new manifest, delete `v1/mws/watersheds.gpkg` first |
| A tehsil layer is needed again | Step 3: it rewrites the file and replaces the GeoServer layer |
| The MWS layer's columns or the drought causality code change (`LAYOUT` in `computing/drought/local/mws.py` is bumped) | Step 2 with `--stages mws`: every MWS layer is made again. Then Step 3 (drought and causality) for the tehsils |

NASA expects the Terra satellite, which carries this MODIS, to stop around February 2027; seasons after that
will need VIIRS inputs.

## Tests

```bash
docker compose exec backend python -m unittest computing.drought.local.test_rules computing.drought.local.test_rainfall \
    computing.drought.local.test_cropland computing.drought.local.test_vegetation \
    computing.drought.local.test_moisture computing.drought.local.test_classify computing.drought.local.test_mws
docker compose exec backend python manage.py test computing.drought.tests
```
