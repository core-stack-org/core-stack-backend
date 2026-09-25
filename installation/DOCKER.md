# Docker installation

Runs the CoRE Stack backend with PostgreSQL, Redis, GeoServer and Celery in
Docker. Nothing else is installed on the host.

When it is done you have:

- the API and Django admin at http://localhost:8000
- GeoServer at http://localhost:8080/geoserver

## 1. Before you start

| You need | Check |
| --- | --- |
| Docker Engine with Compose v2 | `docker compose version` |
| Your user can run Docker (member of the `docker` group) | `docker ps` works without `sudo` |
| git | `git --version` |
| About 20 GB free disk, plus space for the [data](#data-for-local-compute) you add | `df -h .` |
| Ports 8000, 8080 and 5432 free | `ss -ltn \| grep -E ':(8000\|8080\|5432) '` prints nothing |

For the GPU jobs you also need an NVIDIA driver and the
[NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html).
This must print your GPU:

```bash
docker run --rm --gpus all nvidia/cuda:12.9.0-base-ubuntu22.04 nvidia-smi
```

On a network that only reaches the internet through a proxy (for example a
campus network), read [Behind a proxy](#behind-a-proxy) first.

## 2. Install

**1. Get the code**

```bash
git clone https://github.com/core-stack-org/core-stack-backend.git
cd core-stack-backend
```

**2. Create the settings file**

```bash
cp installation/docker/env.template nrm_app/.env
chmod 600 nrm_app/.env
```

`nrm_app/.env` holds every setting and password. Docker Compose and Django
both read it.

**3. Edit `nrm_app/.env`**

Set the admin account you will log in with:

```dotenv
DJANGO_SUPERUSER_USERNAME=admin
DJANGO_SUPERUSER_EMAIL=you@example.com
DJANGO_SUPERUSER_PASSWORD='choose-a-password'
```

On a machine with an NVIDIA GPU, uncomment this line (see
[GPU and long jobs](#gpu-and-long-jobs)):

```dotenv
COMPOSE_PROFILES=heavy
```

Leave everything else as it is for now.

**4. Optional: download the admin boundaries yourself**

The first start downloads the admin-boundary archive (about 600 MB) from
Google Drive. To use a browser download instead, which is often faster,
download it from
[here](https://drive.google.com/file/d/1VqIhB6HrKFDkDnlk1vedcEHhh5fk4f1d/view)
and save it as `data/dataset.7z` in the repository.

**5. Build and start**

```bash
docker compose --env-file nrm_app/.env up -d --build
```

The first run takes 5 to 60 minutes, depending on your connection. The
command waits while the database is set up and the data is downloaded; let
it finish. If it is interrupted, run the same command again.

Every Compose command needs `--env-file nrm_app/.env`, and must be run
from the repository root.

**6. Check that it works**

```bash
docker compose --env-file nrm_app/.env ps -a
```

- `app-init`, `database-init`, `geoserver-init`, `data-download`,
  `gee-config` and `tehsil-watershed-setup` show `Exited (0)`.
- `backend`, `postgres`, `redis` and `geoserver` show `Up (healthy)`.
- The `celery-*` workers show `Up`. `celery-heavy` is there only with
  `COMPOSE_PROFILES=heavy`.

If a job shows a non-zero exit code, read its log:
`docker compose --env-file nrm_app/.env logs <service>`.

Log in to the API. This reads the username and password from `nrm_app/.env`
and keeps the token in `$TOKEN` for the requests in
[Test the APIs](#test-the-apis):

```bash
export no_proxy=localhost,127.0.0.1 NO_PROXY=localhost,127.0.0.1
TOKEN=$(set -a; . nrm_app/.env; set +a; python3 -c '
import json, os, urllib.request
req = urllib.request.Request("http://localhost:8000/api/v1/auth/login/",
    data=json.dumps({"username": os.environ["DJANGO_SUPERUSER_USERNAME"],
                     "password": os.environ["DJANGO_SUPERUSER_PASSWORD"]}).encode(),
    headers={"Content-Type": "application/json"})
print(json.load(urllib.request.urlopen(req))["access"])')
echo "${TOKEN:0:20}"
```

It prints the start of a token (`eyJhbGci...`). You can also log in to
Django admin at http://localhost:8000/admin/.

## 3. What to set up next

The stack now runs. Set up only what you need:

| To | Set up |
| --- | --- |
| Compute layers locally (LULC, hydrology, runoff) | [Data for local compute](#data-for-local-compute) |
| Run Google Earth Engine jobs | [Google Earth Engine](#google-earth-engine) |
| Download ET (evapotranspiration) data | [NASA Earthdata](#nasa-earthdata) |
| Run the GPU and multi-hour jobs | [GPU and long jobs](#gpu-and-long-jobs) |
| Work behind a proxy | [Behind a proxy](#behind-a-proxy) |

Other settings are listed in [Settings](#settings).

## Data for local compute

Local computation (`"compute": "local"` in a request) reads its inputs from
`data/base_layers/`. Download what the APIs you use need and place it as
shown.

| Data | Download | Place at `data/base_layers/` | Needed by |
| --- | --- | --- | --- |
| Terrain (569 MB) | `<link>` | `terrain_raster_fabdam_pan_india.tif` | runoff |
| Soil (6 MB) | `<link>` | `soil/hysogs_india_250m_4326.tif` | runoff |
| LULC, one file per year (63 GB) | `<link>` | `lulc/lulc_v3_<year>_<year+1>.tif` | runoff, LULC |
| India boundary (8 MB) | `<link>` | `PanIndia_Boundaries/india_state_outer_no_islands.geojson` | pan-India runoff |
| Aquifer (102 MB) | `<link>` | `aquifer/aquifer.geojson` | pan-India hydrology |
| Microwatersheds (5.4 GB) | `<link>` | `static_layers/mws/Microwatershed_v2_with_details.geojson` | MWS layers |
| SOI tehsils (316 MB) | `<link>` | `admin_boundary/soi_tehsil.geojson` | tehsil watersheds |
| Tehsil watersheds | `<link>` | `tehsil_watersheds/<state>/<district>/<tehsil>.gpkg` | every tehsil-level request |
| Runoff (164 GB) | `<link>` | `hydrology/runoff/` | pan-India hydrology |
| ET (114 GB) | `<link>` | `hydrology/et/` | pan-India hydrology |
| Pan-India annual hydrology (20 GB) | `<link>` | `hydrology/annual/` | tehsil hydrology |

Files can be added while the stack runs; no restart is needed.

Runoff, ET and pan-India annual hydrology can also be generated with the
APIs in [Test the APIs](#test-the-apis), but that takes many hours.
Tehsil hydrology needs the pan-India annual layer for every year it covers;
the API tells you which years are missing.

With S3 credentials for the CoRE Stack datasets bucket, terrain, LULC,
aquifer and microwatersheds can be downloaded automatically: set
`S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION`, `S3_BUCKET` and
`SKIP_BASE_LAYER_DOWNLOAD=0`, then run
`docker compose --env-file nrm_app/.env run --rm data-download`.

## Google Earth Engine

You need a Google Cloud service account with Earth Engine access and its JSON
key.

1. Open http://localhost:8000/admin/gee_computing/geeaccount/add/ and log in.
2. Fill in a name, the `client_email` from the JSON as the service account
   email, and upload the JSON as the credentials file. Save.
3. Open the account again, set **Helper account** to the same account, and
   save.
4. The account id is the number in the page address
   (`.../geeaccount/1/change/`). Put it in `nrm_app/.env`:

   ```dotenv
   GEE_DEFAULT_ACCOUNT_ID=1
   GEE_HELPER_ACCOUNT_ID=1
   ```

5. Apply it:

   ```bash
   docker compose --env-file nrm_app/.env up -d --force-recreate
   ```

The key is stored encrypted in the database and the uploaded file is deleted,
so keep your own copy. The encryption key is `FERNET_KEY` in `nrm_app/.env`;
if it changes, upload the JSON again.

## NASA Earthdata

The ET download (`/api/v1/et_download/`) fetches FLDAS data from NASA GES
DISC.

1. Create an account at https://urs.earthdata.nasa.gov.
2. In your profile, under **Applications → Authorized Apps**, approve
   **NASA GESDISC DATA ARCHIVE**.
3. Put the login in `nrm_app/.env`. Keep the single quotes if the password
   contains `$`:

   ```dotenv
   USERNAME_GESDISC=your-username
   PASSWORD_GESDISC='your-password'
   ```

4. Apply it:

   ```bash
   docker compose --env-file nrm_app/.env up -d --force-recreate
   ```

A wrong password or an unapproved application makes the task fail with an
HTML page from GES DISC in the `celery-heavy` log.

## GPU and long jobs

Four endpoints start jobs that run for hours:

| Endpoint | Uses the GPU |
| --- | --- |
| `/api/v1/runoff_gpu/` | yes |
| `/api/v1/et_download/` | no |
| `/api/v1/pan-india/hydrology_annual/` | no |
| `/api/v1/pan-india/hydrology_fortnightly/` | no |

They run on the `celery-heavy` worker, one at a time, so they never block
the other layers. `COMPOSE_PROFILES=heavy` in `nrm_app/.env` creates that
worker and gives it the GPU. Without it, these four endpoints answer `503`.

To run the three CPU jobs on a machine without a GPU, set both:

```dotenv
COMPOSE_PROFILES=heavy
GPU_AVAILABLE=False
```

After changing the profile, run
`docker compose --env-file nrm_app/.env up -d --remove-orphans`.

Check that the worker sees the GPU:

```bash
docker compose --env-file nrm_app/.env exec celery-heavy nvidia-smi
```

## Behind a proxy

Image pulls are done by the Docker daemon, which needs its own proxy
setting: see [Docker daemon proxy](https://docs.docker.com/engine/daemon/proxy/).
`docker info | grep -i proxy` shows the current one.

Builds and containers use the proxy from your shell. If `http_proxy` and
`https_proxy` are exported, nothing else is needed. Otherwise uncomment and
set these in `nrm_app/.env`:

```dotenv
HTTP_PROXY=http://proxy.example.org:3128
HTTPS_PROXY=http://proxy.example.org:3128
```

For your own `curl` calls to the stack, keep local addresses off the proxy:

```bash
export no_proxy=localhost,127.0.0.1 NO_PROXY=localhost,127.0.0.1
```

## Settings

All in `nrm_app/.env`. After a change, run
`docker compose --env-file nrm_app/.env up -d --force-recreate`.

| Setting | Default | Meaning |
| --- | --- | --- |
| `CORESTACK_HOST_DATA_DIR` | `.` | Where `data/`, `gee_confs/` and `backups/` live on the host. Set before the first start. |
| `BACKEND_PORT`, `GEOSERVER_PORT`, `POSTGRES_PORT` | `8000`, `8080`, `5432` | Host ports, bound to `127.0.0.1` only. |
| `DB_PASSWORD`, `GEOSERVER_PASSWORD` | placeholders | Change before the first start on any shared machine. |
| `CELERY_NRM_CONCURRENCY` | `3` | Layer jobs that run in parallel. |
| `SKIP_ADMIN_BOUNDARY_DOWNLOAD` | `0` | `1` skips the admin-boundary download. |
| `SKIP_BASE_LAYER_DOWNLOAD` | `1` | `0` downloads base layers from S3 (needs S3 credentials). |
| `SKIP_TEHSIL_WATERSHEDS` | `0` | `1` skips fetching tehsil watersheds from GeoServer. |
| `CELERY_TASK_ALWAYS_EAGER` | `False` | Keep `False`. `True` runs every task inside the web server and bypasses the workers. |

## Test the APIs

Log in first ([step 6](#2-install)). Each request answers at once and queues
a task; follow it in the worker log, for example
`docker compose --env-file nrm_app/.env logs -f celery-nrm`.

```bash
api() { curl -s -X POST "http://localhost:8000/api/v1/$1/" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d "$2"; echo; }
```

| Test | Request | Worker | Output |
| --- | --- | --- | --- |
| LULC | `api lulc_vector '{"compute":"local","state":"karnataka","district":"raichur","block":"devadurga","start_year":2023,"end_year":2023}'` | `celery-nrm` | `data/lulc/lulc_vector_local/...`, GeoServer workspace `lulc_vector` |
| Tehsil hydrology | `api hydrology_annual '{"compute":"local","state":"karnataka","district":"raichur","block":"devadurga","start_year":2017,"end_year":2024}'` | `celery-nrm` | `data/hydrology/hydrology_local/...`, GeoServer workspace `mws_layers` |
| ET download | `api et_download '{"compute":"local","pan_india":true,"start_date":"2023-07-01","end_date":"2023-07-03"}'` | `celery-heavy` | `data/base_layers/hydrology/et/` |
| Pan-India annual | `api pan-india/hydrology_annual '{"compute":"local","start_year":2017,"end_year":2018}'` | `celery-heavy` | `data/base_layers/hydrology/annual/` |
| Pan-India fortnightly | `api pan-india/hydrology_fortnightly '{"compute":"local","start_year":2017,"end_year":2018}'` | `celery-heavy` | `data/base_layers/hydrology/fortnightly/` |
| Runoff (hours) | `api runoff_gpu '{"compute":"local","pan_india":true,"start_year":2023,"end_year":2024}'` | `celery-heavy` | `data/base_layers/hydrology/runoff/` |

Tehsil hydrology needs `start_year` 2017. Pan-India requests take one year
per call: `end_year` is `start_year + 1`.

## Everyday use

```bash
docker compose --env-file nrm_app/.env ps            # status
docker compose --env-file nrm_app/.env logs -f backend
docker compose --env-file nrm_app/.env stop          # stop, keep everything
docker compose --env-file nrm_app/.env up -d         # start again
```

After `git pull`:

```bash
docker compose --env-file nrm_app/.env up -d --build --force-recreate
```

Commands that write files, such as `manage.py` commands, should run as your
user so the files stay yours:

```bash
docker compose --env-file nrm_app/.env exec --user "$(id -u):$(id -g)" backend python manage.py <command>
```

`docker compose --env-file nrm_app/.env down -v` deletes the database,
GeoServer and Redis data. `data/` on the host is kept.

## Troubleshooting

| Problem | Cause and fix |
| --- | --- |
| `curl` to `localhost` returns `503` | Your proxy is answering. `export no_proxy=localhost,127.0.0.1 NO_PROXY=localhost,127.0.0.1`. |
| `backend` never starts | An init job failed. `ps -a` shows which; read its log. |
| Build fails at `apt-get` or `pip` | No internet from the build. See [Behind a proxy](#behind-a-proxy). |
| `Missing Pan-India hydrology annual base layer(s)` | Tehsil hydrology needs the pan-India annual layer for those years. Add it from [Data](#data-for-local-compute) or generate it. |
| `JSONDecodeError` on a tehsil request | `data/base_layers/tehsil_watersheds/<state>/<district>/<tehsil>.gpkg` is missing. |
| The four long-job endpoints return `503` | `COMPOSE_PROFILES=heavy` is not set. See [GPU and long jobs](#gpu-and-long-jobs). |
| `Earth Engine client library not initialized` in logs | Earth Engine is not set up. Harmless for local compute. |
| `401` from `geoserver.core-stack.org` in logs | The STAC catalog step uses the public CoRE Stack GeoServer. The layer itself is saved and published locally. |
| Admin page has no styling | Static files are not served by the web server. The admin still works. |
| `Permission denied` on files in the repository | Left by an older setup that ran as root. `docker compose --env-file nrm_app/.env up -d` gives them back to you. |
| A second copy of the repository uses the first one's database | All copies share the Compose project name `core-stack`. Run one installation per machine. |

To stop a long job that is running on `celery-heavy`:

```bash
docker compose --env-file nrm_app/.env kill celery-heavy
docker compose --env-file nrm_app/.env exec celery-nrm celery -A nrm_app purge -Q heavy -f
docker compose --env-file nrm_app/.env exec redis redis-cli del unacked unacked_index
docker compose --env-file nrm_app/.env up -d celery-heavy
```

Without the `purge` and `redis-cli` steps the job starts again when the worker
restarts.
The `redis-cli` step also drops tasks started but not finished on other
workers.

## Running on a server

- Change `DB_PASSWORD`, `GEOSERVER_PASSWORD` and the admin password before
  the first start.
- Set `DEBUG=False`, and `ALLOWED_HOSTS` to the server's host name.
- Keep ports bound to `127.0.0.1`; put an HTTPS reverse proxy in front.
- Set `CORESTACK_HOST_DATA_DIR` to a path on a disk with room for the data,
  for example `/srv/core-stack-data`.
- Back up the database regularly:
  `docker compose --env-file nrm_app/.env --profile maintenance run --rm database-backup`
  writes a dump to `backups/postgres/`.
