# CoRE Stack — LULC Pipeline Runbook

Complete steps to build, set up and run a LULC pipeline from scratch.

---

## Prerequisites

- Docker installed and running
- `service-account.json` GEE key file on your host machine
- GCS bucket created (e.g. `fpl-core-stack-dev`) in GCP project `arcane-mason-493503-a6`
- Local clone of `core-stack-backend` repo with:
  - `Dockerfile` placed at the repo root
  - `install-docker.sh` placed at `installation/install-docker.sh`

```
core-stack-backend/       ← your local clone / build context
├── Dockerfile            ← add this
├── installation/
│   └── install-docker.sh        ← replace with patched version
└── ...rest of repo
```

---

## Part 1 — Build and Start the Container

```bash
# 0. Enter your local repo clone
cd core-stack-backend

# 1. Build the image (only needed once or after Dockerfile changes)
# add `--no-cache` to create one without any cached layers
# add `--platform linux/arm64` to build for ARM64 architecture
docker build -t corestack .

# 2. Create a shared network for core-stack and geoserver
docker network create corestack-network 2>/dev/null || true

# 3. Start GeoServer
# add `--platform linux/arm64` to run on ARM64 architecture
docker run -dit \
  --name geoserver \
  --network corestack-network \
  -p 8080:8080 \
  docker.osgeo.org/geoserver:2.28.0

# 4. Start the CoRE Stack container
docker run -it \
  --name core-stack \
  --network corestack-network \
  -p 9001:8000 \
  -v /path/to/service-account.json:/opt/gee-keys/service-account.json \
  corestack
```

> After step 4 you will be inside the container at `/opt/corestack`.

---

## Part 2 — Run install-docker.sh (inside the container)

Run each step in order. If a step was already completed in a previous run,
`install-docker.sh` will skip it automatically.

```bash
# Step 1 — PostgreSQL setup
CONDA_ENV_NAME=corestack-backend bash installation/install-docker.sh --only postgres

# Step 2 — Generate .env file
CONDA_ENV_NAME=corestack-backend bash installation/install-docker.sh --only env_file

# Step 3 — Fix $BACKEND_DIR literal in .env
sed -i 's|\$BACKEND_DIR|/opt/corestack|g' /opt/corestack/nrm_app/.env

# Step 4 — Set credentials in .env
sed -i 's|GCS_BUCKET_NAME=""|GCS_BUCKET_NAME="fpl-core-stack-dev"|g' nrm_app/.env
sed -i 's|S3_BUCKET=""|S3_BUCKET="fpl-core-stack-dev"|g' nrm_app/.env
sed -i 's|DPR_S3_BUCKET=""|DPR_S3_BUCKET="fpl-core-stack-dev"|g' nrm_app/.env
sed -i 's|GEE_STORAGE_PROJECT=""|GEE_STORAGE_PROJECT="arcane-mason-493503-a6"|g' nrm_app/.env
sed -i 's|GEE_STORAGE_PROJECT_HELPER=""|GEE_STORAGE_PROJECT_HELPER="arcane-mason-493503-a6"|g' nrm_app/.env
sed -i 's|GEOSERVER_URL=""|GEOSERVER_URL="http://geoserver:8080/geoserver"|g' nrm_app/.env
sed -i 's|GEOSERVER_USERNAME=""|GEOSERVER_USERNAME="admin"|g' nrm_app/.env
sed -i 's|GEOSERVER_PASSWORD=""|GEOSERVER_PASSWORD="geoserver"|g' nrm_app/.env

# Step 5 — Django migrations + seed data + superuser
CONDA_ENV_NAME=corestack-backend bash installation/install-docker.sh \
  --only django_migrations,seed_data,superuser

# Step 6 — GEE configuration
CONDA_ENV_NAME=corestack-backend bash installation/install-docker.sh \
  --only gee_configuration \
  --gee-json /opt/gee-keys/service-account.json

# Step 7 — Admin boundary data (~8GB, takes a while)
CONDA_ENV_NAME=corestack-backend bash installation/install-docker.sh \
  --only admin_boundary_data
```

> After Step 5 note your superuser credentials — install-docker.sh prints:
> `Installer test superuser: username=test_user_XXXX password=test_change_me`

---

## Part 3 — Set Up GeoServer Workspaces

Run these from your **host machine** (not inside the container):

```bash
# Create all required workspaces
for workspace in mws panchayat_boundaries customkml ndvi_timeseries \
  nrega_assets plantation swb crop_grid_layers ne test_workspace; do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST http://localhost:8080/geoserver/rest/workspaces \
    -H "Content-Type: application/json" \
    -u admin:geoserver \
    -d "{\"workspace\": {\"name\": \"$workspace\"}}")
  if [ "$STATUS" = "201" ]; then
    echo "Created: $workspace"
  elif [ "$STATUS" = "409" ]; then
    echo "Already exists: $workspace"
  else
    echo "Warning ($STATUS): $workspace"
  fi
done
```

---

## Part 4 — Start Django and Celery

Open **two terminal windows**. In each, exec into the container:

**Terminal 1 — Django:**
```bash
docker exec -it core-stack bash -c "
  source /opt/conda/etc/profile.d/conda.sh && \
  conda activate corestack-backend && \
  cd /opt/corestack && \
  python manage.py runserver 0.0.0.0:8000
"
```

**Terminal 2 — Celery:**
```bash
docker exec -it core-stack bash -c "
  source /opt/conda/etc/profile.d/conda.sh && \
  conda activate corestack-backend && \
  cd /opt/corestack && \
  celery -A nrm_app worker -l info -Q nrm --pool solo
"
```

---

## Part 5 — Run the LULC Pipeline

Run these from your **host machine**.

**Get a token:**
```bash
TOKEN=$(curl -s -X POST http://localhost:9001/api/v1/auth/login/ \
  -H "Content-Type: application/json" \
  -d '{"username":"test_user_0944","password":"test_change_me"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access'])")
echo $TOKEN
```

** COPY echoed value into $TOKEN in the following commands**
eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzg4OTUyNzk1LCJpYXQiOjE3ODExNzY3OTUsImp0aSI6ImE1YTJlMWYxNGIyYzQ3YjJiNjBkMTQ5MDk0YWYwZjgxIiwidXNlcl9pZCI6Mn0.GICG9iyoDd2D5xXlfJLld_wT4ech7QPoMS13zCmvehg

**Step 1 — Admin boundary** (wait for `succeeded` in Celery before next step):
```bash
curl -s -X POST http://localhost:9001/api/v1/generate_block_layer/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "state": "karnataka",
    "district": "hassan",
    "block": "hassan",
    "gee_account_id": 1
  }'
```
Output should be: ```{"Success":"Successfully initiated"}```
Wait until the terminal window with Celery shows a status such as:

```
[2026-06-11 16:52:44,979: INFO/MainProcess] Task computing.misc.admin_boundary.generate_tehsil_shape_file_data[9fe9f019-a775-491f-8f8a-9ef728a6fa79] succeeded in 31.10820138899726s: True
```

**Step 2 — MWS layer** (wait for `succeeded` before next step):
```bash
curl -s -X POST http://localhost:9001/api/v1/generate_mws_layer/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "state": "karnataka",
    "district": "hassan",
    "block": "hassan",
    "gee_account_id": 1
  }'
```
Output should be: ```{"Success":"Successfully initiated"}%```
Wait until the terminal window with Celery show a status such as:
```
[2026-06-11 16:57:47,249: INFO/MainProcess] Task computing.mws.mws.mws_layer[796d4f16-74c6-4885-beba-3d84919f51bf] succeeded in 7.108237585998722s: True
```

**Step 3 — LULC** (takes 10–30 minutes):
```bash
curl -s -X POST http://localhost:9001/api/v1/lulc_v3/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "state": "karnataka",
    "district": "hassan",
    "block": "hassan",
    "start_year": 2017,
    "end_year": 2024,
    "gee_account_id": 1
  }'
```
Output should be: ```{"Success":"LULC v3 task initiated"}%```

**Watch Celery progress:**
```bash
docker exec core-stack tail -f /var/log/corestack/celery.log
```

---
## Part 6 — View the LULC Layer

**Apply colormap style to GeoServer:** (or see alternate way):
```bash
curl -s -X POST http://localhost:8080/geoserver/rest/styles \
  -H "Content-Type: application/vnd.ogc.sld+xml" \
  -u admin:geoserver \
  -d '<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
  xmlns="http://www.opengis.net/sld"
  xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd">
  <NamedLayer>
    <Name>lulc_style</Name>
    <UserStyle>
      <Title>LULC Style</Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap type="values">
              <ColorMapEntry color="#1f8c2a" quantity="1"  label="Tree Cover"/>
              <ColorMapEntry color="#a0dc5b" quantity="2"  label="Shrubland"/>
              <ColorMapEntry color="#c9d426" quantity="3"  label="Grassland"/>
              <ColorMapEntry color="#64c8fa" quantity="4"  label="Cropland"/>
              <ColorMapEntry color="#f5a500" quantity="5"  label="Built-up"/>
              <ColorMapEntry color="#e6e64b" quantity="6"  label="Bare/Sparse"/>
              <ColorMapEntry color="#f5f5dc" quantity="7"  label="Snow/Ice"/>
              <ColorMapEntry color="#0064c8" quantity="8"  label="Water"/>
              <ColorMapEntry color="#009678" quantity="9"  label="Wetland"/>
              <ColorMapEntry color="#a0643c" quantity="10" label="Mangroves"/>
              <ColorMapEntry color="#fad5a5" quantity="11" label="Moss/Lichen"/>
              <ColorMapEntry color="#c8a0d2" quantity="12" label="Agriculture"/>
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>'

# Apply style to the LULC layer
curl -s -X PUT \
  "http://localhost:8080/geoserver/rest/layers/ne:LULC_17_18_hassan_hassan_level_1" \
  -H "Content-Type: application/json" \
  -u admin:geoserver \
  -d '{"layer": {"defaultStyle": {"name": "lulc_style"}}}'
```
======
The styling can also be added at:http://localhost:8080/geoserver/web/wicket/bookmarkable/org.geoserver.wms.web.data.StyleNewPage?12 with any name (e.g. lulc_style):

```
  <?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
  xmlns="http://www.opengis.net/sld"
  xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd">
  <NamedLayer>
    <Name>lulc_style</Name>
    <UserStyle>
      <Title>LULC Style</Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap type="values">
              <ColorMapEntry color="#1f8c2a" quantity="1"  label="Tree Cover"/>
              <ColorMapEntry color="#a0dc5b" quantity="2"  label="Shrubland"/>
              <ColorMapEntry color="#c9d426" quantity="3"  label="Grassland"/>
              <ColorMapEntry color="#64c8fa" quantity="4"  label="Cropland"/>
              <ColorMapEntry color="#f5a500" quantity="5"  label="Built-up"/>
              <ColorMapEntry color="#e6e64b" quantity="6"  label="Bare/Sparse"/>
              <ColorMapEntry color="#f5f5dc" quantity="7"  label="Snow/Ice"/>
              <ColorMapEntry color="#0064c8" quantity="8"  label="Water"/>
              <ColorMapEntry color="#009678" quantity="9"  label="Wetland"/>
              <ColorMapEntry color="#a0643c" quantity="10" label="Mangroves"/>
              <ColorMapEntry color="#fad5a5" quantity="11" label="Moss/Lichen"/>
              <ColorMapEntry color="#c8a0d2" quantity="12" label="Agriculture"/>
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>
```



**View in browser:**
```
http://localhost:8080/geoserver/wms?service=WMS&version=1.1.1&request=GetMap&layers=ne:LULC_17_18_hassan_hassan_level_1&bbox=75.8,12.8,76.5,13.5&width=800&height=600&srs=EPSG:4326&format=image/png
```

Or via GeoServer Layer Preview:
```
http://localhost:8080/geoserver/web/
```
→ Layer Preview → `ne:LULC_17_18_hassan_hassan_level_1` → OpenLayers

---

## Quick Reference

| Service      | URL                                      | Credentials         |
|--------------|------------------------------------------|---------------------|
| Django admin | http://localhost:9001/admin/             | test_user_0944 / test_change_me |
| Swagger API  | http://localhost:9001/swagger/           | (log in via admin first) |
| GeoServer    | http://localhost:8080/geoserver/web/     | admin / geoserver   |

---

## Troubleshooting

**Container already exists:**
```bash
docker stop core-stack && docker rm core-stack
docker stop geoserver && docker rm geoserver
```

**RabbitMQ not running:**
```bash
docker exec core-stack service rabbitmq-server start
```

**PostgreSQL not running:**
```bash
docker exec core-stack service postgresql start
```

**GeoServer namespace null error:**
```bash
# Re-create all workspaces
for workspace in mws panchayat_boundaries customkml ndvi_timeseries \
  nrega_assets plantation swb crop_grid_layers ne test_workspace; do
  curl -s -X POST http://localhost:8080/geoserver/rest/workspaces \
    -H "Content-Type: application/json" \
    -u admin:geoserver \
    -d "{\"workspace\": {\"name\": \"$workspace\"}}"
done
```

**Token expired:**
```bash
TOKEN=$(curl -s -X POST http://localhost:9001/api/v1/auth/login/ \
  -H "Content-Type: application/json" \
  -d '{"username":"test_user_XXXX","password":"test_change_me"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access'])")
```

**Check Celery logs:**
```bash
docker exec core-stack tail -50 /var/log/corestack/celery.log
```

** If old GEE assets remain **
```bash
docker exec -it core-stack bash -c "
  source /opt/conda/etc/profile.d/conda.sh && \
  conda activate corestack-backend && \
  cd /opt/corestack && \
  python manage.py shell -c \"
import ee
from utilities.gee_utils import ee_initialize
ee_initialize(account_id=1)

base = 'projects/arcane-mason-493503-a6/assets/apps/mws/<STATE>/<DISTRICT>/<BLOCK>
assets = ee.data.listAssets({'parent': base})
for a in assets.get('assets', []):
    asset_name = a['name']
    if '_old' in asset_name:
        print('Deleting:', asset_name)
        ee.data.deleteAsset(asset_name)
        print('Deleted.')
\"
"