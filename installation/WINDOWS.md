# Windows Setup

This repo now includes a native Windows setup path alongside the existing Linux installer.

## What Changed

- Runtime bootstrap is now OS-aware in [`nrm_app/runtime.py`](../nrm_app/runtime.py).
- Django settings now resolve temp, media, asset, Excel, and WhatsApp paths without hardcoded Unix locations in [`nrm_app/settings.py`](../nrm_app/settings.py).
- Django entrypoints now initialize GDAL/PROJ and Conda library paths consistently on Windows in [`manage.py`](../manage.py), [`nrm_app/wsgi.py`](../nrm_app/wsgi.py), [`nrm_app/asgi.py`](../nrm_app/asgi.py), and [`nrm_app/celery.py`](../nrm_app/celery.py).
- Temp/media/shapefile path handling was normalized in key runtime flows, including [`plans/api.py`](../plans/api.py), [`bot_interface/api.py`](../bot_interface/api.py), [`computing/utils.py`](../computing/utils.py), and [`utilities/gee_utils.py`](../utilities/gee_utils.py).
- Native Windows bootstrap scripts were added in [`installation/bootstrap_env.py`](./bootstrap_env.py) and [`installation/install_windows.ps1`](./install_windows.ps1).

## Prerequisites

- Windows 10 or Windows 11
- PowerShell 5.1+ or PowerShell 7+
- Git
- Miniconda or Anaconda
- PostgreSQL with PostGIS
- RabbitMQ

Optional but recommended:

- Firefox for DPR PDF rendering
- FFmpeg for WhatsApp audio conversion

## Install

From PowerShell:

```powershell
cd .\installation
.\install_windows.ps1
```

If you want the script to create the app database and role for you, pass PostgreSQL admin credentials:

```powershell
.\install_windows.ps1 `
  -PostgresAdminUser postgres `
  -PostgresAdminPassword "<postgres-admin-password>"
```

What the script does:

- creates or updates the Conda environment from [`installation/environment.yml`](./environment.yml)
- generates `nrm_app/.env` with Windows-safe path defaults
- creates required local directories
- runs `collectstatic`
- runs migrations
- loads seed data
- runs [`computing/misc/internal_api_initialisation_test.py`](../computing/misc/internal_api_initialisation_test.py)

## Run

API server:

```powershell
conda run -n corestackenv python manage.py runserver
```

Celery worker:

```powershell
conda run -n corestackenv celery -A nrm_app worker -l info -Q nrm --pool=solo
```

Create a superuser:

```powershell
conda run -n corestackenv python manage.py createsuperuser --skip-checks
```

## Environment Notes

- `CELERY_WORKER_POOL` defaults to `solo` on Windows.
- `TMP_LOCATION`, `WHATSAPP_MEDIA_PATH`, `EXCEL_PATH`, and related paths are now resolved relative to the repo when left blank.
- If Firefox is not on `PATH`, set `FIREFOX_BIN`.
- If FFmpeg is not on `PATH`, set `FFMPEG_BIN`.

## Remaining Caveats

The core app bootstrap is now Windows-aware, but a few standalone utility scripts still carry developer-specific absolute paths and should be normalized separately before relying on them on Windows:

- [`utilities/zip_to_geoserver.py`](../utilities/zip_to_geoserver.py)
- [`utilities/tiff_to_geoserver.py`](../utilities/tiff_to_geoserver.py)
- [`utilities/generate_json_data.py`](../utilities/generate_json_data.py)
- [`utilities/nrega_asset_categ.py`](../utilities/nrega_asset_categ.py)
