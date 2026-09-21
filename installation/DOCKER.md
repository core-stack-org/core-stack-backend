# Reliable CoRE Stack Docker setup

The repository-root `docker-compose.yml` is the only supported CoRE Stack
Compose definition. It builds the backend environment, mounts source from the
host, starts PostgreSQL, Redis, GeoServer, Gunicorn and Celery, and runs
initialization jobs in dependency order.

## Architecture

| State | Location | Persistence |
| --- | --- | --- |
| Backend source | Host checkout mounted at `/app` | Git/host filesystem |
| Downloaded and generated layers | `${CORESTACK_DATA_DIR:-./data}` mounted at `/var/tmp/core-stack-data` | Host filesystem |
| PostgreSQL | Separate `postgres` container | Docker volume `postgres_data` |
| GeoServer catalog | Separate `geoserver` container | Docker volume `geoserver_data` |
| Celery broker | Separate `redis` container with AOF | Docker volume `redis_data` |
| GEE JSON | `${GEE_CONFS_DIR:-./gee_confs}` | Read-only host mount |

PostgreSQL is never stored in the backend container. `./installation/docker/compose.sh down`
keeps all volumes. `./installation/docker/compose.sh down -v` deliberately destroys the
database, GeoServer catalog and Redis data.

## Requirements

- Docker Engine or Docker Desktop with Compose v2
- Linux/amd64 support; Apple Silicon uses Docker emulation
- Enough free disk for images and requested layers
- Ports 8000, 8080 and 5432 free on loopback, or overridden in
  `.env.core-stack-docker`

## One-click first start

From the backend repository:

```bash
cp installation/docker/env.core-stack-docker.example .env.core-stack-docker
chmod 600 .env.core-stack-docker
mkdir -p data gee_confs backups/postgres backups/geoserver
./installation/docker/compose.sh up -d --build
```

For a local evaluation, the placeholder passwords work. Before any shared or
production deployment, replace both passwords in `.env.core-stack-docker`.

Always use `./installation/docker/compose.sh` for this stack. The wrapper
supplies `--env-file .env.core-stack-docker`, fixes the project directory, and
fails clearly when the environment file is missing. To use a different file
deliberately, set `CORESTACK_COMPOSE_ENV_FILE` to its absolute path.

The retired parent-repository `.env.core-stack` is not read. If it exists,
manually transfer only the values still needed into this repository's
`.env.core-stack-docker`, verify the stack, and securely delete the legacy
credentials.

Compose runs these one-shot services before starting Gunicorn:

1. `app-init` creates the ignored `nrm_app/.env`, generates secret keys, and
   copies the resolved database, GeoServer, Celery and runtime values from the
   Compose container environment into that Django environment file.
2. `database-init` creates/updates installation-local migration files, prints
   the plan, applies it with `--fake-initial`, collects static files and loads
   seed data once.
3. `geoserver-init` reconciles workspaces and bundled styles.
4. `data-download` downloads only the requested/missing source layers.
5. `gee-config` discovers optional mounted GEE JSON credentials.
6. `tehsil-watershed-setup` downloads active tehsil watershed layers directly
   from the GeoServer `mws` WFS workspace.
7. Gunicorn and the queue-specific Celery workers start.

Follow first-start progress:

```bash
./installation/docker/compose.sh ps
./installation/docker/compose.sh logs -f app-init database-init data-download geoserver-init \
  gee-config tehsil-watershed-setup backend
```

Local URLs:

- Django: http://localhost:8000
- GeoServer: http://localhost:8080/geoserver
- PostgreSQL: `127.0.0.1:5432`

## Large-download controls

Each expensive data family has its own switch:

| Variable | Effect when set to `1` |
| --- | --- |
| `SKIP_ADMIN_BOUNDARY_DOWNLOAD` | Do not download the approximately 8 GB admin-boundary archive |
| `SKIP_BASE_LAYER_DOWNLOAD` | Do not download terrain, MWS, LULC and static/tehsil-level base layers |
| `SKIP_TEHSIL_WATERSHEDS` | Do not fetch active tehsil watershed GPKGs from GeoServer |

Set the flags in `.env.core-stack-docker` before the first start, or for one
invocation:

```bash
SKIP_ADMIN_BOUNDARY_DOWNLOAD=1 \
SKIP_BASE_LAYER_DOWNLOAD=1 \
SKIP_TEHSIL_WATERSHEDS=1 \
./installation/docker/compose.sh up -d --build
```

Existing files are skipped individually. To intentionally refresh the
admin-boundary archive:

```bash
FORCE_DATA_DOWNLOAD=1 ./installation/docker/compose.sh run --rm data-download
```

To fetch base layers later:

```bash
SKIP_ADMIN_BOUNDARY_DOWNLOAD=1 \
SKIP_BASE_LAYER_DOWNLOAD=0 \
./installation/docker/compose.sh run --rm data-download
```

### Tehsil watersheds

The Compose bootstrap always calls:

```bash
python manage.py local_compute_layer_setup \
  --ensure-tehsil-watersheds --geoserver
```

It queries active tehsils from PostgreSQL and downloads each
`mws:mws_<district>_<tehsil>` layer from GeoServer WFS into:

```text
data/base_layers/tehsil_watersheds/<state>/<district>/<tehsil>.gpkg
```

This path does not run the alternative local process that intersects or copies
the pan-India microwatershed file. Missing GeoServer layers are reported, and
the backend can still start so they can be published and retried later:

```bash
./installation/docker/compose.sh run --rm tehsil-watershed-setup
```

## GEE setup

The stack starts without GEE. For Earth Engine jobs:

```bash
cp /secure/path/service-account.json gee_confs/gee-service-account.json
chmod 600 gee_confs/gee-service-account.json
./installation/docker/compose.sh run --rm gee-config
./installation/docker/compose.sh up -d --force-recreate backend \
  celery-nrm celery-layer-bulk celery-geoserver celery-general
```

The mount is read-only. The setup reads `project_id` and writes only derived
runtime values under `CORESTACK_DATA_DIR`. Add the corresponding
`GEEAccount` through Django admin if it is not already in the database.
Raster export/publishing also requires `GCS_BUCKET_NAME`.

Use `SKIP_GEE_CONFIG=1` when GEE must be completely disabled.

## Database reliability

PostgreSQL 16 runs separately with:

- an explicitly named persistent volume;
- data checksums on new database volumes;
- a readiness check before migrations;
- a one-minute graceful shutdown window;
- loopback-only host exposure by default;
- persistent Django connections through `DB_CONN_MAX_AGE`;
- migrations in a single one-shot service, never in every web/worker restart.

Do not copy a random host PostgreSQL data directory into the volume. PostgreSQL
major version, filesystem ownership and initialization settings must match.

### Backup

```bash
mkdir -p backups/postgres
./installation/docker/compose.sh --profile maintenance run --rm database-backup
ls -lh backups/postgres
```

Because migrations are intentionally ignored by Git and are installation-local,
back up the local `*/migrations/` directories with the database:

```bash
tar -czf backups/postgres/local-migrations.tgz \
  */migrations
```

Copy both artifacts off the Docker host and periodically test restoration.

### Restore

Restoration replaces database contents and must be performed during a
maintenance window. Stop backend/workers, take another backup, then restore a
validated custom-format dump:

```bash
./installation/docker/compose.sh stop backend celery-nrm celery-layer-bulk \
  celery-geoserver celery-general celery-beat
./installation/docker/compose.sh exec -T postgres dropdb --if-exists -U corestack_admin corestack_db
./installation/docker/compose.sh exec -T postgres createdb -U corestack_admin corestack_db
./installation/docker/compose.sh exec -T postgres pg_restore \
  --exit-on-error --no-owner -U corestack_admin -d corestack_db \
  < backups/postgres/<validated-backup>.dump
./installation/docker/compose.sh run --rm database-init
./installation/docker/compose.sh up -d
```

Substitute the configured database/user. Restore the matching
`local-migrations.tgz` before `database-init` when it is available. If it is
not available, the job generates a current initial migration set and
`--fake-initial` recognizes matching tables. Test this on a cloned database
first; a dump whose schema does not match the checked-out code must not be
started. Never use `./installation/docker/compose.sh down -v` as a restore procedure.

## Installation-local migrations

Migration files remain in Git ignore, matching `installation/install.sh`.
Every installation keeps its migration history in the host checkout alongside
its PostgreSQL volume. `database-init` performs:

```bash
python manage.py makemigrations --skip-checks
python manage.py migrate --plan --skip-checks
python manage.py migrate --fake-initial --noinput --skip-checks
```

For a new empty database, Django creates all tables. For a restored database,
`--fake-initial` marks matching initial tables without recreating them and
then applies later local migrations.

Do not delete local migration files during a normal upgrade. They are the
state Django uses to generate the next incremental migration for that machine.
Set `RESET_LOCAL_MIGRATIONS=1` only for a fresh database or a restored
database already verified to match the checked-out models.

The Gunicorn/Celery runtime entrypoint never changes schema. Only the
`database-init` one-shot job does. Back up PostgreSQL and local migration
files before every code update, inspect its printed plan, and test schema
changes on a restored clone before production.

## GeoServer and layer-data recovery

GeoServer runs in its own container and keeps its catalog, workspaces and
configuration in the `geoserver_data` named volume. Back it up while GeoServer
is stopped so the archive is internally consistent:

```bash
mkdir -p backups/geoserver
./installation/docker/compose.sh stop geoserver
./installation/docker/compose.sh --profile maintenance run --rm geoserver-backup
./installation/docker/compose.sh start geoserver
```

Treat that catalog archive, the PostgreSQL dump and the matching Git revision
as one release backup. Test a GeoServer restore on a non-production volume
before replacing production state.

Downloaded inputs and generated layers are not in the GeoServer volume. They
remain in `CORESTACK_DATA_DIR` on the host. Snapshot or synchronize that
directory with the host's normal backup system; do not add it to a Docker
image. GEE JSON remains in `GEE_CONFS_DIR` and must be backed up as a secret.

## Superuser

Either set the three `DJANGO_SUPERUSER_*` values before first start, or create
the account interactively:

```bash
./installation/docker/compose.sh exec backend python manage.py createsuperuser
```

When the automated username already exists, setup leaves its password
unchanged.
## Behind a campus or corporate proxy

Docker does not pass the host's proxy settings into image builds or containers. On a network where the only route out is an HTTP proxy, this shows up in two places:

- the image build fails at `apt-get install` with `Unable to locate package ...` (the preceding `apt-get update` could not reach the mirrors), or later in `micromamba`/`pip`;
- the first start fails while downloading the admin-boundary dataset with `Failed to establish a new connection: [Errno 101] Network is unreachable`.

Compose reads the proxy from your environment and passes it both as build arguments (for `apt`, `micromamba` and `pip` in the `Dockerfile`) and as environment variables to every backend, init and Celery container. Usually you only need the variables your shell already exports:

```bash
export HTTP_PROXY=http://proxy.example.org:3128/
export HTTPS_PROXY=http://proxy.example.org:3128/
./installation/docker/compose.sh up -d --build
```

To make it stick across shells, set them in `.env.core-stack-docker` instead (see the commented block in `installation/docker/env.core-stack-docker.example`):

```bash
HTTP_PROXY=http://proxy.example.org:3128/
HTTPS_PROXY=http://proxy.example.org:3128/
```

Lowercase `http_proxy` / `https_proxy` are picked up too, and `NO_PROXY` is honoured if you set it. The Compose service names (`postgres`, `redis`, `geoserver`, `backend`, `core-stack`) are always added to `NO_PROXY`, so traffic between containers stays off the proxy. If no proxy variables are set, nothing changes. The proxy is passed as Docker's predefined proxy build arguments, so it is not stored in the built image.

Pulling the base images (`micromamba`, `postgres`, `redis`, `geoserver`) is separate: that is done by the Docker daemon, not by a container, so it needs the daemon's own proxy configuration. Check with `docker info | grep -i proxy` and see [Docker's daemon proxy docs](https://docs.docker.com/engine/daemon/proxy/) if pulling is what fails.

## Troubleshooting

## Day-to-day operations

```bash
./installation/docker/compose.sh ps
./installation/docker/compose.sh logs -f backend
./installation/docker/compose.sh logs -f celery-nrm celery-layer-bulk celery-geoserver celery-general
./installation/docker/compose.sh stop
./installation/docker/compose.sh start
./installation/docker/compose.sh down
```

Celery Beat is intentionally opt-in:

```bash
./installation/docker/compose.sh --profile periodic up -d celery-beat
```

## Code and dependency updates

Source is host-mounted, so a code-only update needs a pinned Git checkout and
process recreation:

```bash
git pull --ff-only
./installation/docker/compose.sh run --rm database-init
./installation/docker/compose.sh up -d --force-recreate backend \
  celery-nrm celery-layer-bulk celery-geoserver celery-general
```

When `Dockerfile` or `installation/environment.yml` changes, rebuild the
environment and recreate every backend service:

```bash
./installation/docker/compose.sh build --pull
./installation/docker/compose.sh up -d --force-recreate
```

Do not install packages in running containers. Production releases should pin
`CORESTACK_IMAGE_TAG` or `CORESTACK_IMAGE` and the Git commit so code and
dependencies can be rolled back together.

## Testing

Fast host-side checks:

```bash
./installation/docker/compose.sh config --quiet
bash -n installation/docker/*.sh
python3 -m unittest discover -s installation/tests
```

Container checks:

```bash
./installation/docker/compose.sh run --rm backend python manage.py check
./installation/docker/compose.sh run --rm backend python manage.py check --deploy
./installation/docker/compose.sh run --rm backend python manage.py test
```

The deployment check intentionally warns in development mode. Production must
run with `DEBUG=False` and explicit public host/origin settings.

## Production checklist

1. Pin the Git commit and image tag; do not deploy moving `latest`.
2. Replace PostgreSQL and GeoServer passwords in the protected
   `.env.core-stack-docker` before creating the database volume.
3. Set `DEBUG=False`, exact `ALLOWED_HOSTS`, and trusted HTTPS origins.
4. Keep 8000, 8080 and 5432 on loopback; expose only the HTTPS reverse proxy.
5. Back up PostgreSQL, local migration files, the GeoServer catalog,
   `CORESTACK_DATA_DIR` and GEE secrets.
6. Run tests and `check --deploy`; review the plan printed by
   `database-init` on a restored clone.
7. Run `database-init` once before recreating Gunicorn/Celery.
8. Review Beat schedules before enabling the `periodic` profile.
9. Monitor container health, queue depth, disk space and backup completion.
10. Test rollback and database restore before launch.

## Complete reset

This is destructive:

```bash
./installation/docker/compose.sh down -v
```

It deletes PostgreSQL, Redis and GeoServer volumes. Host-mounted `./data`,
`./gee_confs` and backups are not deleted.
