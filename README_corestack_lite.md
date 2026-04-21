# CoRE Stack Lite — Branch Setup Guide

**Branch:** `corestack-lite`
**Author:** Saharsh Laud, IIT Delhi
**Last Updated:** March 2026

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Prerequisites](#3-prerequisites)
4. [Step 1 — Clone and Environment Setup](#4-step-1--clone-and-environment-setup)
5. [Step 2 — PostgreSQL Setup](#5-step-2--postgresql-setup)
6. [Step 3 — Environment Variables](#6-step-3--environment-variables-nrmappenv)
7. [Step 4 — GEE Account Configuration](#7-step-4--gee-account-configuration)
8. [Step 5 — Google Cloud Storage Setup](#8-step-5--google-cloud-storage-gcs-setup)
9. [Step 6 — GeoServer Setup (Tomcat 9)](#9-step-6--geoserver-setup-tomcat-9)
10. [Step 7 — Django Migrations and Seed Data](#10-step-7--django-migrations-and-seed-data)
11. [Step 8 — Running the Server](#11-step-8--running-the-server)
12. [Step 9 — Testing the APIs](#12-step-9--testing-the-apis)
13. [What Changed from Production](#13-what-changed-from-corestack-production)
14. [Starting Everything After a Reboot](#14-starting-everything-after-a-reboot)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. Overview

`corestack-lite` is a modified version of the CoRE Stack Django backend adapted for the **STACD (SpatioTemporal Asset Catalog for Data)** pipeline — an Airflow-based geospatial pipeline orchestration system developed at IIT Delhi.

**Key difference from production CoRE Stack:**
- All 8 algorithm APIs are **synchronous** (no Celery) — they block until GEE tasks complete and return `asset_ids` + STAC specs directly
- Uses your own GEE account for writing outputs instead of the production account
- Uses your own GCS bucket for raster storage
- Uses a **local GeoServer** (Tomcat 9) instead of the production GeoServer

---

## 2. Architecture

```
STACD Airflow DAG
      │
      │ HTTP POST (Bearer token auth)
      ▼
CoRE Stack Lite Django Backend (port 8000)
      │
      ├──► Google Earth Engine (GEE) — computation, exports raster/vector to GEE asset (your account)
      │
      ├──► Google Cloud Storage (GCS) — raster-only: gs://your-bucket/nrm_raster.tif
      │
      ├──► GeoServer (port 8080) — reads from GCS (rasters) or shapefile (vectors), serves WMS/WFS
      │
      └──► PostgreSQL — layer tracking DB (layer_id, asset_id, sync status, STAC specs)
```

**GCS sync only applies to raster layers.** Vector layers go GEE → GeoServer directly (no GCS step).

---

## 3. Prerequisites

| Component | Version | Notes |
|---|---|---|
| Ubuntu/WSL | 24.04 | WSL2 on Windows |
| Python | 3.10 | Via conda |
| Conda | latest | miniconda recommended |
| PostgreSQL | 14+ | Local install |
| Java JDK | 21 | OpenJDK via apt |
| Apache Tomcat | 9.0.98 | **Must be Tomcat 9, NOT Tomcat 10** |
| GeoServer | 2.23.6 | **Last version compatible with Tomcat 9** |
| gcloud CLI | latest | For GCS authentication |

> ⚠️ **GeoServer 2.24+ requires Tomcat 10 and is NOT compatible with this setup. Always use GeoServer 2.23.x with Tomcat 9.**

---

## 4. Step 1 — Clone and Environment Setup

```bash
# Clone the corestack-lite branch
git clone -b corestack-lite https://github.com/core-stack-org/core-stack-backend.git
cd core-stack-backend

# Create conda environment
conda create -n corestack-backend python=3.10 -y
conda activate corestack-backend

# Install dependencies
pip install -r requirements.txt
```

> If `requirements.txt` install fails on individual packages, install them one by one and skip conflicting ones.

---

## 5. Step 2 — PostgreSQL Setup

```bash
sudo apt install postgresql postgresql-contrib -y
sudo service postgresql start

# Create database and user
sudo -u postgres psql -c "CREATE USER admin WITH PASSWORD 'admin';"
sudo -u postgres psql -c "CREATE DATABASE corestacklite OWNER admin;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE corestacklite TO admin;"
```

---

## 6. Step 3 — Environment Variables (`nrm_app/.env`)

The `.env` file is loaded by `django-environ` from `nrm_app/.env` (not the project root).

```bash
cp nrm_app/.env.example nrm_app/.env   # if example exists
```

Minimum required values:

```env
# PostgreSQL
DB_NAME=corestacklite
DB_USER=admin
DB_PASSWORD=admin

# Django
DEBUG=True
SECRET_KEY=django-insecure-your-secret-key-here

# GeoServer (local instance)
GEOSERVER_URL=http://localhost:8080/geoserver
GEOSERVER_USERNAME=admin
GEOSERVER_PASSWORD=geoserver

# GEE Account
GEE_DEFAULT_ACCOUNT_ID=2

# GCS Authentication (Application Default Credentials)
GOOGLE_APPLICATION_CREDENTIALS=/home/yourusername/.config/gcloud/application_default_credentials.json

# Fernet Key (required for credential encryption)
FERNET_KEY= your-fernet-key
```

> Replace `yourusername` with your actual Linux username.

---

## 7. Step 4 — GEE Account Configuration

### 4a. Add GEE service account key

Create a Django superuser, start the server, go to `http://127.0.0.1:8000/admin` → **GEE Accounts** → add your service account JSON key for **account ID 2**.

```bash
python manage.py createsuperuser  # username: admin, password: admin
```

### 4b. Change GEE asset output paths in `utilities/constants.py`

```python
# Change these to your GEE project (all occurrences of ee-saharshlaud → your project)
GCS_BUCKET_NAME = "your-bucket-name"

GEE_ASSET_PATH = "projects/your-gee-project/assets/apps/mws/"
GEE_HELPER_PATH = "projects/your-gee-project/assets/apps/mws/"

GEE_PATH_PLANTATION = "projects/your-gee-project/assets/apps/plantation/"
GEE_PATH_PLANTATION_HELPER = "projects/your-gee-project/assets/apps/plantation/"

GEE_BASE_PATH = "projects/your-gee-project/assets/apps"
GEE_HELPER_BASE_PATH = "projects/your-gee-project/assets/apps"
```

> **Note:** `GEE_DATASET_PATH = "projects/corestack-datasets/..."` is left unchanged — it points to a shared read-only dataset project. MWS boundaries in async functions also read from `projects/ee-corestackdev` (production, read-only) — this is intentional and does NOT need to change.

---

## 8. Step 5 — Google Cloud Storage (GCS) Setup

### 5a. Create a GCS bucket

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Cloud Storage → Create bucket
   - Name: globally unique, lowercase (e.g. `your-bucket-name`)
   - Region: `asia-south1` (Mumbai) or nearest
   - Storage class: Standard
   - Access control: Uniform

### 5b. Grant bucket permissions

```bash
# Your personal Google account
gcloud storage buckets add-iam-policy-binding gs://your-bucket-name \
  --member=user:your-email@gmail.com --role=roles/storage.admin

# Your GEE service account (so GEE can export directly to bucket)
gcloud storage buckets add-iam-policy-binding gs://your-bucket-name \
  --member=serviceAccount:your-gee-sa@your-gee-project.iam.gserviceaccount.com \
  --role=roles/storage.admin
```

### 5c. Install gcloud CLI and authenticate (ADC)

```bash
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Login
gcloud auth login

# Set up Application Default Credentials (used by the Django backend to write to GCS)
gcloud auth application-default login --no-launch-browser

# Set your project
gcloud config set project your-gcp-project-id
```

> **Why ADC instead of a service account JSON key?** The `iam.disableServiceAccountKeyCreation` org policy blocks JSON key creation on many GCP accounts. ADC is the recommended alternative for local development — more secure, no key file management.

---

## 9. Step 6 — GeoServer Setup (Tomcat 9)

### 6a. Install Java

```bash
sudo apt update
sudo apt install default-jdk -y
java --version  # Expected: openjdk version 21.x.x
```

### 6b. Create Tomcat user

```bash
sudo groupadd tomcat
sudo useradd -s /bin/false -g tomcat -d /opt/tomcat tomcat
```

### 6c. Download and install Tomcat 9

```bash
cd /tmp
wget https://archive.apache.org/dist/tomcat/tomcat-9/v9.0.98/bin/apache-tomcat-9.0.98.tar.gz
sudo mkdir /opt/tomcat
sudo tar xzvf apache-tomcat-9.0.98.tar.gz -C /opt/tomcat --strip-components=1
```

### 6d. Set permissions

```bash
cd /opt/tomcat
sudo chgrp -R tomcat /opt/tomcat
sudo chmod -R g+r conf
sudo chmod g+x conf
sudo chown -R tomcat webapps work temp logs
```

### 6e. Start Tomcat

```bash
sudo /opt/tomcat/bin/startup.sh

# Verify
curl http://localhost:8080   # Should return Tomcat HTML page
```

### 6f. Download and deploy GeoServer 2.23.6

```bash
cd /tmp
wget -L "https://sourceforge.net/projects/geoserver/files/GeoServer/2.23.6/geoserver-2.23.6-war.zip/download" \
  -O geoserver-2.23.6-war.zip

# Verify file size — should be ~108MB
ls -lh geoserver-2.23.6-war.zip

sudo apt install unzip -y
unzip geoserver-2.23.6-war.zip -d /tmp/geoserver-war
sudo cp /tmp/geoserver-war/geoserver.war /opt/tomcat/webapps/

# Wait for auto-deployment (~45 seconds)
sleep 45
sudo tail -5 /opt/tomcat/logs/catalina.out
# Look for: "Deployment of web application archive geoserver.war has finished"
```

### 6g. Login to GeoServer

Open: `http://localhost:8080/geoserver/web`
- **Username:** `admin`
- **Password:** `geoserver`

### 6h. Create required workspaces

The CoRE Stack algorithms publish layers to specific GeoServer workspaces. **All 8 workspaces must be created manually before running any algo API for the first time.** GeoServer does not create these automatically.

```bash
# Create all STACD project workspaces (skips if already exists)
for ws in \
  panchayat_boundaries mws mws_layers mws_centroid mws_connectivity \
  LULC LULC_level_1 LULC_level_2 LULC_level_3 lulc_vector terrain_lulc \
  terrain change_detection crop_intensity drought drought_causality \
  soge stream_order restoration aquifer natural_depression \
  catchment_area_singleflow distance_nearest_upstream_DL slope_percentage \
  lcw agroecological factory_csr green_credit mining \
  mws_centroid swb water_bodies nrega_assets plantation \
  canopy_height ccd tree_overall_ch; do
  result=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST http://localhost:8087/geoserver/rest/workspaces \
    -H "Content-Type: application/json" -u admin:geoserver \
    -d "{\"workspace\": {\"name\": \"$ws\"}}")
  if [ "$result" = "201" ]; then
    echo "✅ Created: $ws"
  elif [ "$result" = "409" ]; then
    echo "⚠️  Already exists: $ws"
  else
    echo "❌ Failed ($result): $ws"
  fi
done

# Verify
echo ""
echo "Total workspaces:"
curl -s -u admin:geoserver \
  http://localhost:8087/geoserver/rest/workspaces.json \
  | python3 -m json.tool | grep '"name"' | wc -l
```

**Expected workspaces after setup:** `panchayat_boundaries`, `mws`, `terrain`, `LULC_level_1`, `LULC_level_2`, `LULC_level_3`, `lulc_vector`, `terrain_lulc`

| Workspace | Used by | Layer type |
|---|---|---|
| `panchayat_boundaries` | AdminBoundary | Vector |
| `mws` | MWSLayer | Vector |
| `terrain` | TerrainAlgorithm, TerrainVectorization | Raster + Vector |
| `LULC_level_1` | LULCAlgorithm | Raster |
| `LULC_level_2` | LULCAlgorithm | Raster |
| `LULC_level_3` | LULCAlgorithm | Raster |
| `lulc_vector` | LULCVectorization | Vector |
| `terrain_lulc` | TerrainLULCSlope, TerrainLULCPlain | Vector |

> If a workspace already exists, GeoServer returns a `500` error for that entry but continues — safe to re-run this loop on an existing setup.


---

## 10. Step 7 — Django Migrations and Seed Data

```bash
conda activate corestack-backend
cd path/to/core-stack-backend

# Run migrations
python manage.py migrate

# Create required directories for seed data
mkdir -p data/activated_locations
touch data/activated_locations/active_locations.json

# Load seed data (master dataset types — required for save_layer_info_to_db)
python manage.py loaddata installation/seed/seeddata.json
# ⚠️ This takes 15-20 minutes. You will see many "Active locations json regenerated" lines — this is normal.

# Collect static files
python manage.py collectstatic --noinput

# Create superuser
python manage.py createsuperuser  # username: admin, password: admin
```

> **The seed data load is mandatory.** Without it, all algo APIs will fail with `Dataset matching query does not exist` because `save_layer_info_to_db` cannot find the dataset type records.

---

## 11. Step 8 — Running the Server

```bash
conda activate corestack-backend
cd path/to/core-stack-backend
python manage.py runserver --insecure
# Server runs at http://127.0.0.1:8000
```

Get an auth token:
```bash
curl -s -X POST http://127.0.0.1:8000/api/v1/auth/login/ \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}' | python3 -m json.tool
# Copy the "access" token — use it as Bearer token in all API requests
```

---

## 12. Step 9 — Testing the APIs

Test in dependency order. All bodies use `"state": "jharkhand"`, `"district": "dumka"`, `"block": "masalia"` as example.

### AdminBoundary
```json
POST http://127.0.0.1:8000/api/v1/generate_block_layer/
{
  "execution_id": "test-001",
  "state": "jharkhand", "district": "dumka", "block": "masalia"
}
```

### MWSLayer
```json
POST http://127.0.0.1:8000/api/v1/generate_mws_layer/
{
  "execution_id": "test-002",
  "state": "jharkhand", "district": "dumka", "block": "masalia",
  "PanIndiaMWS": null
}
```

### LULCAlgorithm
```json
POST http://127.0.0.1:8000/api/v1/lulc_v3/
{
  "execution_id": "test-003",
  "state": "jharkhand", "district": "dumka", "block": "masalia",
  "start_year": 2017, "end_year": 2018, "MWSBoundaries": null
}
```

### LULCVectorization
```json
POST http://127.0.0.1:8000/api/v1/lulc_vector/
{
  "execution_id": "test-004",
  "state": "jharkhand", "district": "dumka", "block": "masalia",
  "start_year": 2017, "end_year": 2018, "LULC_Raster": null
}
```

### TerrainAlgorithm
```json
POST http://127.0.0.1:8000/api/v1/generate_terrain_raster/
{
  "execution_id": "test-005",
  "state": "jharkhand", "district": "dumka", "block": "masalia",
  "MWSBoundaries": null
}
```

### TerrainVectorization
```json
POST http://127.0.0.1:8000/api/v1/generate_terrain_descriptor/
{
  "execution_id": "test-006",
  "state": "jharkhand", "district": "dumka", "block": "masalia",
  "Terrain_Raster": null
}
```

### TerrainLULCSlope
```json
POST http://127.0.0.1:8000/api/v1/terrain_lulc_slope_cluster/
{
  "execution_id": "test-007",
  "state": "jharkhand", "district": "dumka", "block": "masalia",
  "start_year": 2017, "end_year": 2018,
  "LULC_Raster": null, "Terrain_Raster": null
}
```

### TerrainLULCPlain
```json
POST http://127.0.0.1:8000/api/v1/terrain_lulc_plain_cluster/
{
  "execution_id": "test-008",
  "state": "jharkhand", "district": "dumka", "block": "masalia",
  "start_year": 2017, "end_year": 2018,
  "LULC_Raster": null, "Terrain_Raster": null
}
```

**Expected response shape (all APIs):**
```json
{
  "status": "success",
  "message": "...",
  "execution_id": "test-001",
  "node_type": "LULCAlgorithm",
  "asset_ids": ["projects/your-gee-project/assets/apps/mws/..."],
  "hosting_platform": "GEE",
  "stac_spec": { "stac_version": "1.0.0", ... },
  "execution_time": 194.5
}
```

---

## 13. What Changed from CoRE Stack Production

### `utilities/constants.py`
```python
# Changed from production accounts to personal dev account
GCS_BUCKET_NAME = "your-bucket-name"          # was "core_stack"
GEE_ASSET_PATH = "projects/your-gee-project/assets/apps/mws/"   # was ee-corestackdev
GEE_HELPER_PATH = "projects/your-gee-project/assets/apps/mws/"  # was ee-corestack-helper
GEE_BASE_PATH = "projects/your-gee-project/assets/apps"
GEE_HELPER_BASE_PATH = "projects/your-gee-project/assets/apps"
```

### `utilities/gee_utils.py`
```python
# gcs_config() switched to ADC (Application Default Credentials)
# Original service account version commented out
def gcs_config():
    from google.auth import default as google_auth_default
    credentials, project = google_auth_default(
        scopes=["https://www.googleapis.com/auth/devstorage.full_control"]
    )
    storage_client = storage.Client(credentials=credentials, project=project)
    return storage_client.bucket(GCS_BUCKET_NAME)
```

### `computing/api.py`
All 8 STACD algorithm API functions converted from async Celery to synchronous:

| API endpoint | Old (async) | New (sync) |
|---|---|---|
| `generate_block_layer/` | `generate_tehsil_shapefile_data.apply_async` | `generate_tehsil_shapefile_data_sync(...)` |
| `generate_mws_layer/` | `mws_layer.apply_async` | `mws_layer_sync(...)` |
| `lulc_v3/` | `clip_lulc_v3.apply_async` | `clip_lulc_v3_sync(...)` |
| `lulc_vector/` | `vectorise_lulc.apply_async` | `vectorise_lulc_sync(...)` |
| `generate_terrain_raster/` | `generate_terrain_raster_clip.apply_async` | `generate_terrain_raster_clip_sync(...)` |
| `generate_terrain_descriptor/` | `generate_terrain_clusters.apply_async` | `generate_terrain_clusters_sync(...)` |
| `terrain_lulc_slope_cluster/` | `lulc_on_slope_cluster.apply_async` | `lulc_on_slope_cluster_sync(...)` |
| `terrain_lulc_plain_cluster/` | `lulc_on_plain_cluster.apply_async` | `lulc_on_plain_cluster_sync(...)` |

### New sync functions added (GCS + GeoServer end-to-end)

| File | Sync function | GCS | GeoServer workspace |
|---|---|---|---|
| `terraindescriptor/terrain_raster_fabdem.py` | `generate_terrain_raster_clip_sync` | ✅ | `terrain` (raster) |
| `terraindescriptor/terrain_clusters.py` | `generate_terrain_clusters_sync` | — | `terrain` (vector) |
| `lulc/lulc_v3.py` | `clip_lulc_v3_sync` | ✅ | `LULC_level_1/2/3` (raster) |
| `lulc/lulc_vector.py` | `vectorise_lulc_sync` | — | `lulc_vector` (vector) |
| `lulcXterrain/lulc_on_slope_cluster.py` | `lulc_on_slope_cluster_sync` | — | `terrain_lulc` (vector) |
| `lulcXterrain/lulc_on_plain_cluster.py` | `lulc_on_plain_cluster_sync` | — | `terrain_lulc` (vector) |

### `computing/utils.py` — `sync_layer_to_geoserver` bugfix
```python
# Fixed: store_name was never passed to push_shape_to_geoserver
return push_shape_to_geoserver(
    path,
    store_name=layer_name,  # ← this line was missing
    workspace=workspace,
    layer_name=layer_name
)
```

---

## 14. Starting Everything After a Reboot

Run these in order every time WSL restarts:

```bash
# 1. Start PostgreSQL
sudo service postgresql start

# 2. Start GeoServer (Tomcat)
sudo /opt/tomcat/bin/startup.sh

# Wait 60 seconds for GeoServer to fully load
sleep 60
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/geoserver/web
# Should return 200

# 3. Start Django backend
conda activate corestack-backend
cd path/to/core-stack-backend
python manage.py runserver --insecure
```

### Tomcat/GeoServer management commands

| Action | Command |
|---|---|
| Start | `sudo /opt/tomcat/bin/startup.sh` |
| Stop | `sudo /opt/tomcat/bin/shutdown.sh` |
| View logs | `sudo tail -f /opt/tomcat/logs/catalina.out` |
| Check running | `curl http://localhost:8080` |
| Check GeoServer | `curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/geoserver/web` |

> ⚠️ **Tomcat does NOT auto-start when WSL is restarted. Always run `startup.sh` before working.**

---

## 15. Troubleshooting

**GeoServer not responding (curl returns empty or connection refused)**
```bash
sudo /opt/tomcat/bin/startup.sh
sleep 60 && curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/geoserver/web
```

**GeoServer sync fails with `namespace is null` or `workspace not found`**
```bash
# Create the missing workspace (replace workspace-name)
curl -X POST http://localhost:8080/geoserver/rest/workspaces \
  -H "Content-Type: application/json" -u admin:geoserver \
  -d '{"workspace": {"name": "workspace-name"}}'
```

**`Dataset matching query does not exist`**
Seed data not loaded. Run:
```bash
mkdir -p data/activated_locations
touch data/activated_locations/active_locations.json
python manage.py loaddata installation/seed/seeddata.json
```

**Permission denied on Tomcat directories**
Always use `sudo` for all Tomcat operations:
```bash
sudo /opt/tomcat/bin/startup.sh
sudo tail -f /opt/tomcat/logs/catalina.out
sudo ls /opt/tomcat/webapps
```

**GeoServer WAR download is only a few KB**
The SourceForge URL redirects — always use `-L` flag:
```bash
wget -L "https://sourceforge.net/projects/geoserver/files/GeoServer/2.23.6/geoserver-2.23.6-war.zip/download" \
  -O geoserver-2.23.6-war.zip
ls -lh geoserver-2.23.6-war.zip  # Must be ~108MB
```

**`401 Unauthorized` on API calls**
Token expired. Get a fresh token:
```bash
curl -s -X POST http://127.0.0.1:8000/api/v1/auth/login/ \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}'
```

**GCS sync fails with authentication error**
ADC credentials expired. Re-authenticate:
```bash
gcloud auth application-default login --no-launch-browser
```

**GEE task fails or times out**
Check GEE task status at [code.earthengine.google.com](https://code.earthengine.google.com) → Tasks tab. GEE exports can take 3–30 minutes depending on region size.

**`store_name: None` in GeoServer vector publish logs**
This was a bug in `computing/utils.py` — `sync_layer_to_geoserver` was not passing `store_name` to `push_shape_to_geoserver`. Already fixed in this branch (see [What Changed](#13-what-changed-from-corestack-production)).
