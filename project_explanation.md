# Project Explanation: CoRE Stack Backend

## Overview
The **CoRE Stack Backend** is a comprehensive **Django-based** application designed for **Natural Resource Management (NRM)**. It serves as the computational and data management engine for applications related to watershed planning, agriculture, and rural development.

At its heart, it orchestrates complex geospatial computations, manages vast datasets (including satellite imagery via Google Earth Engine), and exposes APIs for frontend clients (web and mobile).

## Key Architecture Components

### 1. Web Framework (Django)
-   **Core App**: The main configuration lives in `nrm_app/`.
-   **API**: Uses **Django Rest Framework (DRF)** to expose RESTful endpoints, versioned as `api/v1/`.
-   **Documentation**: API docs are auto-generated using `drf_yasg` (Swagger/Redoc).

### 2. Geospatial Computation Engine
The project relies heavily on **Google Earth Engine (GEE)** and **Geoserver** for processing and serving map data.
-   **`computing/`**: This directory contains the scientific logic. Modules like [mws](file:///Users/stevesumit/core-stack-backend/computing/mws/mws.py#33-92) (Micro-watershed), `drought`, `lulc` (Land Use Land Cover), and `terrain_descriptor` perform heavy calculations.
-   **`gee_computing/`**: Likely contains specific GEE-related logic.
-   **Data Flow**: Tasks often involve fetching data from GEE, processing it (sometimes using Celery for async execution), and syncing the results to a Geoserver implementation.

### 3. Asynchronous Task Queue
-   **Celery**: Used for handling long-running background tasks, such as generating reports or running complex geospatial models (e.g., [mws_layer](file:///Users/stevesumit/core-stack-backend/computing/mws/mws.py#33-92) in [computing/mws/mws.py](file:///Users/stevesumit/core-stack-backend/computing/mws/mws.py)).
-   **Redis/RabbitMQ**: (Implied) Backend for Celery message brokering.

### 4. Modules & Domain Areas
The application is modularized into specific domains:
-   **`apiadmin`, `geoadmin`**: Administration of the platform and geospatial layers.
-   **`users`, `organization`**: User management, authentication (JWT), and organizational hierarchy.
-   **`projects`, `plans`, `dpr`**: Managing NRM projects, planning phases, and Detailed Project Reports.
-   **`community_engagement`, `bot_interface`**: Interaction with users via platforms like WhatsApp and ODK (Open Data Kit) for ground-truth data collection.
-   **`waterrejuvenation`, `plantations`**: Specific vertical applications.

### 5. Infrastructure & Integration
-   **Database**: **PostgreSQL** (referenced in [settings.py](file:///Users/stevesumit/core-stack-backend/nrm_app/settings.py)).
-   **External Services**:
    -   **Google Earth Engine (GEE)**: For satellite data processing.
    -   **AWS S3**: For file storage (media coverage, reports).
    -   **WhatsApp**: For user engagement bots.
    -   **ODK**: For field data collection.

## Directory Structure Highlights
-   `nrm_app/`: Project settings and main URL routing.
-   `computing/`: Core scientific algorithms (hydrology, climate, etc.).
-   `public_api/`: Public-facing API endpoints.
-   `installation/`: Scripts for setting up the environment (using Conda).
-   [manage.py](file:///Users/stevesumit/core-stack-backend/manage.py): Django's command-line utility.

## Development Stack
-   **Language**: Python
-   **Framework**: Django 4.2+
-   **Environment**: Conda (managed via [installation/environment.yml](file:///Users/stevesumit/core-stack-backend/installation/environment.yml))
-   **Deployment**: Apache with mod_wsgi (Linux recommended).

## Summary via [README.md](file:///Users/stevesumit/core-stack-backend/README.md)
The existing [README.md](file:///Users/stevesumit/core-stack-backend/README.md) also highlights a few specific "Themes" mapped to script paths, such as:
-   **Hydrology**: `computing/mws/`
-   **Climate**: `computing/drought/`
-   **Terrain**: `computing/terrain_descriptor/`
-   **Land Use**: `computing/lulc/`

This suggests the backend is the central brain for a larger ecosystem of tools used for environmental planning and monitoring.
