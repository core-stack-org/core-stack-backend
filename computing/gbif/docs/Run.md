# Running the Biodiversity (GBIF) Pipeline — Locally

> Everything needed to generate the biodiversity layer for **any block on your own machine**, and nothing else.
> Architecture: [`PIPELINE.md`](PIPELINE.md) · [`FLOW_DIAGRAMS.md`](FLOW_DIAGRAMS.md) ·
> stage-by-stage tests: [`VALIDATION_CHECKLIST.md`](VALIDATION_CHECKLIST.md).
>
> The production path (HTTP `POST /api/v1/generate_biodiversity_layer/` → Celery → `nrm` queue) is **not**
> covered here: it needs a RabbitMQ broker and a JWT, neither of which exists on a dev box. It runs the same
> task function this runbook calls directly. See [`PIPELINE.md`](PIPELINE.md) for it.
>
> **Prerequisite:** biodiversity is built **on top of the micro-watershed (MWS) layer** — the block must already
> have its `filtered_mws_<district>_<block>_uid` asset in GEE.
> Blocks validated so far: `bihar/jamui/jamui`, `karnataka/hassan/hassan`.

---

## Two terminals

| Terminal             | Runs                                            | Restart when?   |
| -------------------- | ----------------------------------------------- | --------------- |
| **Terminal 1** | GeoServer (Docker), port**8080**          | Only if it dies |
| **Terminal 2** | Everything you type (`manage.py shell`, curl) | n/a             |

Terminal 1 you start once and leave alone. Only Terminal 2 changes per block.

> ⚠️ **Ports are different services.** GeoServer = **8080**. Django (if you ever run it) = **8000**.

---

## 0. One-time setup

### 0.1 Environment — **both terminals**

```bash
conda activate corestackenv
cd <path-to>/core-stack-backend
```

*Why:* the pipeline needs the project's Python env (geopandas, pygbif, earthengine, Django).

**Pick your block once — every command below reuses these variables**, so a new block means editing only this:

```bash
STATE=karnataka
DISTRICT=hassan
BLOCK=hassan
```

*(Shell variables, not a file. They live only in this terminal and vanish when you close it.)*

**Names with spaces must be quoted**, and must match the database exactly — the lookups are case-insensitive
(`__iexact`) but **whitespace-sensitive**:

```bash
STATE=karnataka
DISTRICT="Bengaluru Urban"
BLOCK="Bengaluru  South"      # yes, TWO spaces — that is the real value in the DB
```

Don't type these from memory; copy the exact string from §1's listing, or from:

```bash
python manage.py shell -c "
from geoadmin.models import TehsilSOI
[print(repr(t.district.district_name), repr(t.tehsil_name)) for t in TehsilSOI.objects.filter(tehsil_name__icontains='bengaluru')]
"
```

The pipeline sanitises names itself (`valid_gee_text`: lowercase, spaces → underscores), so
`"Bengaluru Urban" / "Bengaluru  South"` becomes the asset path `.../bengaluru_urban/bengaluru__south/` and the
layer `bengaluru_urban_bengaluru__south_biodiversity`. You pass the human name; the code handles the rest.

**Derive the layer name once** — the verify/preview commands below all use it:

```bash
LAYER="$(echo "${DISTRICT}_${BLOCK}_biodiversity" | tr 'A-Z' 'a-z' | tr ' ' '_')"
echo "$LAYER"      # e.g. hassan_hassan_biodiversity
```

### 0.2 GBIF credentials in `nrm_app/.env`

```bash
GBIF_USER="your_gbif_username"
GBIF_PWD="your_gbif_password"
GBIF_EMAIL="you@org.org"
```

*Why:* the GBIF Download API needs a (free) GBIF.org account. GEE/GCS credentials do **not** cover it — GBIF is a
separate service. Without these the task aborts immediately, before spending any GEE quota.

### 0.3 Register the DB rows — **Terminal 2**

```bash
python manage.py register_biodiversity_layer      # LayerInfo — idempotent, safe to re-run
```

*Why:* the Excel/KYL generator needs the `LayerInfo` row (`excel_to_be_generated=True`).

`save_layer_info_to_db` also needs a `Dataset` row named `"Biodiversity Occurrence"`, or you get
`Dataset.DoesNotExist`. Check before loading anything:

```bash
python manage.py shell -c "from computing.models import Dataset; print(Dataset.objects.filter(name='Biodiversity Occurrence').exists())"
```

If that prints `True` you're done. Only if it prints `False`:

```bash
python manage.py loaddata installation/seed/seed_data.json
```

*Why the check:* `loaddata` rewrites **every** row in the seed file (all states/districts/tehsils), not just the
biodiversity one. It's safe but slow and far broader than you need. Don't run it habitually.

### 0.4 GeoServer — **Terminal 1**

GeoServer is a **pre-existing standalone container** named `geoserver`. There is no compose file in this repo, so
`docker compose up` will not work.

```bash
sudo docker start geoserver
```

*The `sudo` is needed because your user isn't in the `docker` group.* To drop it permanently:
`sudo usermod -aG docker $USER`, then **log out and back in** (group membership is only read at login).

Then in **Terminal 2**, wait for it to actually finish booting, and provision the workspace + style:

```bash
# GeoServer answers /web/ with a 302 BEFORE its REST API is ready — so poll REST, not the web page:
until curl -s -o /dev/null -m 5 -u admin:geoserver http://localhost:8080/geoserver/rest/about/version.json; do sleep 5; done

# workspace (install.sh's `geoserver` step does this too; 201 = created, 409 = already there):
curl -s -u admin:geoserver -H 'Content-Type: application/json' -d '{"workspace":{"name":"biodiversity"}}' http://localhost:8080/geoserver/rest/workspaces
# style: biodiversity_mws is part of the bundled style set (installation/geoserver/styles/manifest.json)
python installation/geoserver_style_bundle.py sync --url http://localhost:8080/geoserver --username admin --password geoserver
```

*Why:* the layer is published to GeoServer and styled with `biodiversity_mws`. The SLD lives at
`installation/geoserver/styles/workspaces/biodiversity/biodiversity_mws.sld` and is listed in `manifest.json`, so the
installer's `geoserver` step (or the manual `sync` above) provisions it idempotently. Both must run **before** you
generate a layer, because the publish step applies that style.

*If it dies with a read timeout:* GeoServer is still warming up — that's exactly what the `until` loop above
prevents. Wait ~30 s and re-run; the sync is idempotent, so a failed attempt costs nothing.

*If you can't run GeoServer at all:* skip it. The pipeline still completes the GEE asset + DB row and just leaves
`is_sync_to_geoserver=False`. Re-run once GeoServer is up and it will publish — the compute is skipped by the
idempotency gate, so it's cheap.

---

## 1. Check the block has an MWS asset — **Terminal 2**

```bash
python manage.py shell -c "
from utilities.gee_utils import ee_initialize, get_gee_asset_path, valid_gee_text, is_gee_asset_exists
import ee; ee_initialize('1')
from utilities.gee_utils import GEE_ASSET_PATH
base=GEE_ASSET_PATH.rstrip('/')          # projects/<gee-project>/assets/apps/mws
ls=lambda p:[a['id'].split('/')[-1] for a in ee.data.listAssets({'parent':p}).get('assets',[])]
[print(st,d,b) for st in ls(base) for d in ls(f'{base}/{st}') for b in ls(f'{base}/{st}/{d}')
 if is_gee_asset_exists(get_gee_asset_path(st,d,b)+'filtered_mws_'+valid_gee_text(d.lower())+'_'+valid_gee_text(b.lower())+'_uid')]
"
```

*Why:* biodiversity can only run where the micro-watershed layer exists — both the block bounding box and the
per-MWS join come from that asset. If your block isn't listed, run the upstream MWS pipeline (`generate_mws_layer`)
for it first.

⚠️ **This prints GEE *folder* names, which are already sanitised** (`bengaluru_urban`, `bengaluru__south`). Do
**not** paste those into `DISTRICT`/`BLOCK` — the DB lookup in `save_layer_info_to_db` needs the **human** name
(`Bengaluru Urban`), and the underscored form won't match, so the `Layer` row silently fails to register. Use this
listing only to check *whether* a block is eligible; take the actual strings from the DB (§0.1). For single-word
names like `hassan` the two forms are identical, which is why it doesn't bite until you hit a multi-word block.

---

## 2. Generate the layer — **Terminal 2**

**First, confirm the variables are actually set in *this* terminal** — this is the single most common mistake:

```bash
echo "$STATE/$DISTRICT/$BLOCK"     # must print e.g. karnataka/hassan/hassan
```

If it prints `//` they are empty (new terminal? forgot §0.1?). Go set them — otherwise the run dies minutes later
with `Collection asset 'projects/.../apps/mws//filtered_mws___uid' not found`, which is just your empty strings
substituted into the asset path.

```bash
python manage.py shell -c "
from computing.gbif.biodiversity_task import generate_biodiversity_block as g
print(g('$STATE','$DISTRICT','$BLOCK',1))
"
```

*What:* runs the whole pipeline in the foreground and prints, e.g.
`biodiversity done for hassan_hassan: layer_id=26 synced=True doi=10.15468/dl.4ebch5`.

*How long:* **5–15 min** for a fresh block — most of it is GBIF preparing the download on their servers (the task
polls every 60 s). A block whose CSV is cached and whose GEE asset already exists finishes in about a minute.

**What that one command does internally** (all reused CoRE Stack helpers):

1. Derives the block bounding box from the MWS GEE asset.
2. Downloads all GBIF occurrences in that bbox (cached under `computing/gbif/_data/<state>/<district>/<block>/`,
   names sanitised like the GEE asset path, so spelling/case variants share one cache entry).
3. Cleans coordinates + enriches each species with its IUCN category (cached in `computing/gbif/_cache/`).
4. Uploads the points to GEE (inline for small blocks; chunked intermediate assets above
   `config.POINTS_INLINE_LIMIT`, because GEE rejects requests over 10 MB), joins them to the MWS polygons,
   computes 19 properties per MWS.
5. Exports to a GEE asset — **idempotent**: skips the recompute if the asset already exists.
6. Registers a `Layer` row (+ `Layer.misc` provenance) and publishes the GeoPackage to GeoServer with the style.

---

## 3. Verify the run — **Terminal 2**

### 3.1 DB row + provenance

```bash
python manage.py shell -c "
from computing.models import Layer
l=Layer.objects.filter(layer_name='$LAYER').latest('id')
print(l.id, l.is_sync_to_geoserver, l.misc)
"
```

*Why:* confirms DB registration and that the GBIF provenance persisted — DOI, download key, raw/clean record
counts, download date. `is_sync_to_geoserver=True` means the GeoServer publish succeeded.

### 3.2 The published layer (WFS — returns the data)

```bash
curl -s "http://localhost:8080/geoserver/biodiversity/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=biodiversity:$LAYER&outputFormat=application/json&maxFeatures=2" | python3 -m json.tool | head -40
```

*Why:* confirms the layer is published, has one feature per MWS, and carries **full field names** with values
(`species_richness`, `threatened_species_count`, `dominant_class`, `biodiversity_category`, `data_poor`, …).
The layer is always named `<district>_<block>_biodiversity`.

### 3.3 Excel sheet

```bash
python manage.py shell -c "
from stats_generator.utils import get_vector_layer_geoserver as x
x('$STATE','$DISTRICT','$BLOCK', specific_sheets=['biodiversity'])
"
```

*Why:* KYL filters and the DPR reports read the **Excel sheet**, not GeoServer directly. `specific_sheets` limits
generation to this one sheet, so you don't need the block's other layers.

### 3.4 Block summary report

```bash
python manage.py shell -c "
from dpr.gen_tehsil_report import get_biodiversity_summary_data as s
print(s('$STATE','$DISTRICT','$BLOCK'))
"
```

*Why:* confirms the tehsil report section is populated (total MWS, data-poor count, average richness,
MWS-with-threatened, top-5). The per-MWS equivalent is `get_biodiversity_data(state, district, block, uid)`.
*Note:* the report readers read from `EXCEL_DIR`; if that differs from where 3.3 wrote, copy the file there first
(see the EXCEL_PATH/EXCEL_DIR note in [`REVIEW.md`](REVIEW.md)).

---

## 4. Where to see the block

Layers are **always** named `<district>_<block>_biodiversity` and live in the `biodiversity` workspace.

### 4.1 The map — direct preview link

```
http://localhost:8080/geoserver/biodiversity/wms/reflect?layers=biodiversity:hassan_hassan_biodiversity&format=application/openlayers
```

Swap the layer name for any other block. (The WMS *reflector* works out the bounding box for you, so this is a
one-click link — no coordinates to look up.)

Or through the UI: `http://localhost:8080/geoserver/web/` → **Layer Preview** → filter for `biodiversity` →
click **OpenLayers**.

*What you're looking at:* **green ramp** = species richness (light = Low → dark = Very High); **grey** = data-poor
MWS (fewer than `MIN_RECORDS` records). Click any watershed to see its indicators.

### 4.2 What have I built so far?

```bash
# every biodiversity layer published in GeoServer:
curl -s -u admin:geoserver http://localhost:8080/geoserver/rest/workspaces/biodiversity/layers.json

# the same from the DB — plus record counts and the citable GBIF DOI per block:
python manage.py shell -c "
from computing.models import Layer
for l in Layer.objects.filter(layer_name__endswith='_biodiversity').order_by('id'):
    print(l.id, l.layer_name, l.is_sync_to_geoserver, l.misc.get('clean_record_count'), l.misc.get('gbif_doi'))
"
```

### 4.3 The GEE asset

`<GEE_ASSET_PATH>/<state>/<district>/<block>/biodiversity_<district>_<block>` (`GEE_ASSET_PATH` is the
`projects/<gee-project>/assets/apps/mws/` root from `nrm_app/.env`) — open it in the Earth Engine Code Editor to
inspect the raw per-MWS FeatureCollection. For blocks with more than `POINTS_INLINE_LIMIT` cleaned points you will
briefly also see `biodiversity_points_<district>_<block>_partN` assets: those are the chunked point uploads and are
deleted automatically once the stats asset is written.

### 4.4 Rendered image (WMS GetMap) — **Terminal 2**

```bash
export STATE DISTRICT BLOCK      # manage.py shell runs in a subprocess — it needs them exported

python manage.py shell -c "
import requests, os
GS='http://localhost:8080/geoserver'; L=f\"biodiversity:{os.environ['DISTRICT']}_{os.environ['BLOCK']}_biodiversity\"
ft=requests.get(requests.get(f'{GS}/rest/layers/{L}.json',auth=('admin','geoserver')).json()['layer']['resource']['href'],auth=('admin','geoserver')).json()['featureType']
bb=ft['latLonBoundingBox']; bbox=f\"{bb['minx']},{bb['miny']},{bb['maxx']},{bb['maxy']}\"
url=f'{GS}/biodiversity/wms?service=WMS&version=1.1.1&request=GetMap&layers={L}&styles=biodiversity_mws&bbox={bbox}&srs=EPSG:4326&width=900&height=900&format=image/png'
open('biodiversity_map.png','wb').write(requests.get(url,auth=('admin','geoserver')).content); print('wrote biodiversity_map.png')
"
```

*Why:* a quick static check that the layer + SLD render. **Grey** = data-poor MWS (fewer than `MIN_RECORDS`
records); **green ramp** = species richness (light = Low → dark = Very High).

---

## 5. Running more blocks — the repeat loop

**Everything in §0 was one-time.** You do *not* re-run the GeoServer workspace/style provisioning,
`register_biodiversity_layer`, or `loaddata` per block — those are environment-level. GeoServer just needs to be
up (Terminal 1).

Per block it is **three commands** in Terminal 2:

```bash
# 1. point at the new block (check it's listed in §1 first — it needs an MWS asset)
STATE=bihar DISTRICT=gaya BLOCK=gaya
echo "$STATE/$DISTRICT/$BLOCK"      # sanity: must NOT print //

# 2. build it  (5–15 min)
python manage.py shell -c "from computing.gbif.biodiversity_task import generate_biodiversity_block as g; print(g('$STATE','$DISTRICT','$BLOCK',1))"

# 3. Excel sheet  (needed for KYL filters + DPR reports)
python manage.py shell -c "from stats_generator.utils import get_vector_layer_geoserver as x; x('$STATE','$DISTRICT','$BLOCK', specific_sheets=['biodiversity'])"
```

Then view it via §4. Later blocks tend to be faster: the IUCN cache (`computing/gbif/_cache/`) is global per
species, so species already seen in an earlier block cost nothing.

### Re-running and forcing a rebuild

- **Re-running the same block is safe.** The GBIF download is cached on disk, and if the GEE asset already exists
  the compute/export is **skipped** (`is_gee_asset_exists`) — it just re-registers and re-syncs. That's exactly
  what you want after a GeoServer outage.
- **Force a full recompute** (e.g. after changing the indicator logic) — delete the GEE asset first:

  ```bash
  python manage.py shell -c "
  from utilities.gee_utils import ee_initialize; from computing.gbif import config; import ee
  ee_initialize('1'); ee.data.deleteAsset(config.get_gee_block_asset_id('$STATE','$DISTRICT','$BLOCK'))"
  ```

  *Why:* otherwise the idempotency guard reuses the existing (now stale) asset and your change has no effect.
- **Re-download fresh GBIF data:** delete the block's cache dir. It is keyed by the *sanitised* names
  (lowercase, spaces → underscores, same as the GEE asset path), e.g. `computing/gbif/_data/tamil_nadu/chennai/guindy/`.

---

## 6. Common failures

| Symptom                                                                                                     | Cause                                                                                                                                                                    | Fix                                                                                                                     |
| ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| `NameError` / `ImportError` from `computing/gbif/*.py` on **any** `manage.py` command         | `computing/api.py` imports the task at module load, so a broken GBIF file breaks **every** Django command — even unrelated ones like `loaddata`               | Fix the file. Check in isolation:`python -c "import ast; ast.parse(open('computing/gbif/gbif_mws_stats.py').read())"` |
| `permission denied ... /var/run/docker.sock`                                                              | User not in the`docker` group                                                                                                                                          | `sudo docker start geoserver`, or `sudo usermod -aG docker $USER` + re-login                                        |
| `docker: 'compose' is not a docker command`                                                               | No compose plugin — and no compose file in this repo anyway                                                                                                             | `sudo docker start geoserver`; the container already exists                                                           |
| `geoserver_style_bundle.py sync` → read timeout                                                           | GeoServer still booting:`/web/` returns 302 before REST is ready                                                                                                       | Wait ~30 s, re-run (idempotent). Use the`until` loop in §0.4                                                         |
| `Collection asset '.../apps/mws//filtered_mws___uid' not found` — note the **empty** path segments | `$STATE/$DISTRICT/$BLOCK` are unset in this terminal (they don't survive a new shell) | `echo "$STATE/$DISTRICT/$BLOCK"` — if it prints `//`, re-set them (§0.1) |                                                                                                                         |
| `filtered_mws_<district>_<block>_uid not found` with the names **filled in** / 0 MWS                | Block genuinely has no MWS asset                                                                                                                                         | Run`generate_mws_layer` for that block first (§1)                                                                    |
| Aborts asking for`GBIF_USER/PWD/EMAIL`                                                                    | GBIF creds not set                                                                                                                                                       | Add them to`nrm_app/.env` (§0.2)                                                                                     |
| `Dataset "Biodiversity Occurrence" not found`                                                             | Seed row missing                                                                                                                                                         | §0.3                                                                                                                   |
| GeoServer connection refused during sync                                                                    | GeoServer not running (Terminal 1)                                                                                                                                       | Start it; the`Layer` row survives — just re-run §2 to publish                                                       |
| `No such resource: biodiversity_mws.sld`                                                                  | Style not provisioned                                                                                                                                                    | `python installation/geoserver_style_bundle.py sync ...` (§0.4)                                                      |
| Report empty /`total_mws=0`                                                                               | Excel written to a different dir than the reader reads                                                                                                                   | Reconcile`EXCEL_PATH` vs `EXCEL_DIR` (see [`REVIEW.md`](REVIEW.md))                                                |
| `Error making asset public: ... permitted customer`                                                       | GEE project IAM rejects the public binding                                                                                                                               | Harmless — GeoServer serves the data; not required for the layer                                                       |
| `Request payload size exceeds the limit: 10485760 bytes`                                                  | Block has more cleaned points than `config.POINTS_INLINE_LIMIT` allows inline (should not happen: the task switches to chunked uploads automatically)                     | Lower `POINTS_INLINE_LIMIT` / `POINTS_CHUNK_SIZE` in `config.py`                                                       |

---

## Quick copy-paste — a new block, happy path

```bash
# Terminal 1 (once):
sudo docker start geoserver

# Terminal 2:
conda activate corestackenv && cd <path-to>/core-stack-backend
STATE=karnataka DISTRICT=hassan BLOCK=hassan

curl -s -u admin:geoserver -H 'Content-Type: application/json' -d '{"workspace":{"name":"biodiversity"}}' http://localhost:8080/geoserver/rest/workspaces
python installation/geoserver_style_bundle.py sync --url http://localhost:8080/geoserver --username admin --password geoserver
python manage.py shell -c "from computing.gbif.biodiversity_task import generate_biodiversity_block as g; print(g('$STATE','$DISTRICT','$BLOCK',1))"
python manage.py shell -c "from stats_generator.utils import get_vector_layer_geoserver as x; x('$STATE','$DISTRICT','$BLOCK', specific_sheets=['biodiversity'])"
# then: http://localhost:8080/geoserver/web/ -> Layer Preview -> biodiversity:hassan_hassan_biodiversity
```
