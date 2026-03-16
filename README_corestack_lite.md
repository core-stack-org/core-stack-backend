# CoRE Stack Lite — Branch Setup Guide

This branch (`corestack-lite`) is a modified version of the CoRE Stack backend dev branch, intended for local development and integration with the STACD (SpatioTemporal Asset Catalog for Data) pipeline.

## What's Different in This Branch

The primary change from `dev` is that all GEE (Google Earth Engine) asset paths in `utilities/constants.py` have been pointed to a local developer's GEE account instead of the production CoRE Stack accounts. This allows anyone to run the backend locally and have pipeline outputs go to their own GEE project.

## Before You Start — Required GEE Account Changes

After cloning/pulling this branch, you **must** update `utilities/constants.py` to point to your own GEE project. Otherwise pipeline outputs will attempt to write to someone else's GEE account and will fail with a permission error.

Open `utilities/constants.py` and find the GEE Paths section (around line 156):

```python
# MARK: GEE Paths
GCS_BUCKET_NAME = "core_stack"

GEE_ASSET_PATH = "projects/ee-saharshlaud/assets/apps/mws/"
GEE_HELPER_PATH = "projects/ee-saharshlaud/assets/apps/mws/"

GEE_PATH_PLANTATION = "projects/ee-saharshlaud/assets/apps/plantation/"
GEE_PATH_PLANTATION_HELPER = "projects/ee-saharshlaud/assets/apps/plantation/"

GEE_BASE_PATH = "projects/ee-saharshlaud/assets/apps"
GEE_HELPER_BASE_PATH = "projects/ee-saharshlaud/assets/apps"
```

Replace `ee-saharshlaud` with your own GEE project ID. For example if your GEE project is `ee-yourname`, it should look like:

```python
GEE_ASSET_PATH = "projects/ee-yourname/assets/apps/mws/"
GEE_HELPER_PATH = "projects/ee-yourname/assets/apps/mws/"

GEE_PATH_PLANTATION = "projects/ee-yourname/assets/apps/plantation/"
GEE_PATH_PLANTATION_HELPER = "projects/ee-yourname/assets/apps/plantation/"

GEE_BASE_PATH = "projects/ee-yourname/assets/apps"
GEE_HELPER_BASE_PATH = "projects/ee-yourname/assets/apps"
```

Also update your `nrm_app/.env` file with the paths to your own GEE service account key JSON files:

```
GEE_SERVICE_ACCOUNT_KEY_PATH=data/gee_confs/your-key.json
GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH=data/gee_confs/your-helper-key.json
GEE_DATASETS_SERVICE_ACCOUNT_KEY_PATH=data/gee_confs/your-datasets-key.json
```

## Full Installation

Follow the main installation guide in `installation/INSTALLATION.md`. The `.env.example` file in the project root lists all required environment variables.

## Purpose of This Branch

This branch is being developed as part of the STACD integration project at IIT Delhi. The goal is to modify the CoRE Stack backend to work as a synchronous REST API that can be invoked by an Airflow-based STACD pipeline, as opposed to the production setup which uses Celery for asynchronous task queuing.