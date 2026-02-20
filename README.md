## CoRE Stack Backend

Django-based backend for the CoRE Stack platform, providing geospatial computing, data management, and API services for natural resource management and landscape analysis.

### Installation

We provide a single installation script that handles everything (**on a linux environment, if you are using Windows, you may need to install ```wsl``` first**).
- Installs **Miniconda** and sets up the Python environment
- Installs & configures **PostgreSQL**
- Installs & configures **Apache with mod_wsgi**
- Clones the backend repo and applies Django **migrations**
- Collects **static files**
- Sets up **logs** and **Apache config**

#### Requirements

Before starting, make sure you have the following installed on your system:

- A **Linux-based operating system** (Ubuntu 20.04+ recommended)
- **Git** (to clone the repository)
- **Bash** (usually preinstalled on Linux)

The installation script will handle the rest (Conda, PostgreSQL, Apache, etc.).

#### 1. Clone the repository

```bash
git clone https://github.com/core-stack-org/core-stack-backend.git
cd core-stack-backend/installation
```

#### 2. Run the installation script

```bash
chmod +x install.sh
./install.sh
```

> The script will automatically install Conda, PostgreSQL, Apache, set up the `corestack-backend` environment, run
> migrations, and configure Apache.

#### 3. Running the server
After the successfull installation of all the packages, run the following commands to start the Django server:
```bash
conda activate corestack-backend (or whatever is the name of your virtual environment)
python manage.py runserver
```
- **Running celery:**
If you are running some tasks, you need to run 
```bash
celery -A nrm_app worker -l info -Q nrm
```
where 'nrm_app' is the app_name and 'nrm' is the rabbitmq queue.


#### 4. Open in Browser

- API Docs: [http://localhost](http://localhost)
- Django Admin: [http://localhost/admin/](http://localhost/admin/)

---

### Project Structure

#### Django Applications

- **geoadmin** - Geographic administrative boundaries and spatial data management
- **computing** - Geospatial analysis and computation pipelines
- **dpr** - Detailed Project Reports management
- **stats_generator** - Statistical data generation and aggregation
- **organization** - Organization and user group management
- **users** - User authentication and profile management
- **projects** - Project lifecycle management
- **plantations** - Plantation site planning and monitoring
- **plans** - Planning and intervention strategies
- **public_api** - Public-facing API endpoints
- **gee_computing** - Google Earth Engine integration
- **community_engagement** - Community interaction and engagement tools
- **bot_interface** - WhatsApp bot integration
- **waterrejuvenation** - Water body restoration tracking
- **moderation** - Data moderation and ODK sync
- **apiadmin** - API administration and monitoring

#### Technology Stack

- Django 5.2 with Django REST Framework
- PostgreSQL with PostGIS
- Celery with RabbitMQ for async tasks
- Google Earth Engine for satellite imagery analysis
- Apache with mod_wsgi for production deployment
- JWT authentication
- Swagger/ReDoc API documentation

---

### Script path

|    | Theme                    | Variable                        | Script path                                                           |
|----|--------------------------|---------------------------------|-----------------------------------------------------------------------|
| 1  | Hydrology                | Microwatersheds                 | /computing/mws/mws.py                                                 |
| 2  | Hydrology                | Precipitation                   | /computing/mws/precipitation.py                                       |
| 3  | Hydrology                | Runoff                          | /computing/mws/run_off.py                                             |
| 4  | Hydrology                | Evapotranspiration              | /computing/mws/evapotranspiration.py                                  |
| 5  | Hydrology                | Change in groundwater           | /computing/mws/delta_g.py                                             |
| 6  | Hydrology                | Change in well depth            | /computing/mws/well_depth.py                                          |
| 7  | Hydrology                | Aquifers                        | /computing/misc/aquifer_vector.py                                     |
| 8  | Hydrology                | Stage of Groundwater Extraction | /computing/misc/soge_vector.py                                        |
| 9  | Climate                  | Drought frequency and intensity | /computing/drought/drought.py                                         |
| 10 | Climate                  | Drought causality               | /computing/drought/drought_causality.py                               |
| 11 | Terrain                  | Terrain classification          | /computing/terrain_descriptor/terrain_raster.py                       |
| 12 | Terrain                  | Terrain cluster                 | /computing/terrain_descriptor/terrain_clusters.py                     |
| 13 | Land use                 | Land use land cover             | /computing/lulc/lulc_v3.py                                            |
| 14 | Land use                 | Land use on terrain             | Land use on Plain: /computing/lulc_X_terrain/lulc_on_plain_cluster.py |
|    | Land use                 | Land use on terrain             | Land use on Slope: /computing/lulc_X_terrain/lulc_on_slope_cluster.py |
| 15 | Land use                 | Land use changes                | /computing/change_detection/change_detection.py                       |
| 16 | Land use                 | Cropping intensity              | /computing/cropping_intensity/cropping_intensity.py                   |
| 17 | Land use                 | Water bodies                    | /computing/surface_water_bodies/swb.py                                |
| 18 | Land use                 | First census of water bodies    | /computing/surface_water_bodies/swb3.py                               |
| 19 | Tree health              | Tree canopy cover density       | /computing/tree_health/ccd.py                                         |
| 20 | Tree health              | Tree canopy height              | /computing/tree_health/canopy_height.py                               |
| 21 | Tree health              | Tree cover change               | /computing/tree_health/overall_change.py                              |
| 22 | Welfare                  | NREGA assets categorization     | /computing/misc/nrega.py                                              |
| 23 | Administrative           | State                           | /computing/misc/admin_boundary.py                                     |
| 24 | Administrative           | District                        | /computing/misc/admin_boundary.py                                     |
| 25 | Administrative           | Block/Tehsil                    | /computing/misc/admin_boundary.py                                     |
| 26 | Administrative           | Village                         | /computing/misc/admin_boundary.py                                     |
| 27 | Water structure planning | Lithology                       | /computing/clart/lithology.py                                         |
| 28 | Water structure planning | Drainage lines                  | /computing/misc/drainage_lines.py                                     |
| 29 | Water structure planning | Stream order raster             | /computing/misc/stream_order.py                                       |
| 30 | Water structure planning | CLART                           | /computing/clart/clart.py                                             |

---

### Configuration

Required environment variables (see `.env.example`):

**Database:**
- `DB_NAME` - PostgreSQL database name
- `DB_USER` - PostgreSQL username
- `DB_PASSWORD` - PostgreSQL password

**Django:**
- `SECRET_KEY` - Django secret key
- `DEBUG` - Debug mode (True/False)

**ODK (Open Data Kit):**
- `ODK_USERNAME` - ODK username
- `ODK_PASSWORD` - ODK password
- `ODK_USER_EMAIL_SYNC` - ODK sync email
- `ODK_USER_PASSWORD_SYNC` - ODK sync password

**GeoServer:**
- `GEOSERVER_URL` - GeoServer URL
- `GEOSERVER_USERNAME` - GeoServer username
- `GEOSERVER_PASSWORD` - GeoServer password

**Google Earth Engine:**
- `GEE_SERVICE_ACCOUNT_KEY_PATH` - GEE service account key path
- `GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH` - GEE helper account key path
- `GEE_DATASETS_SERVICE_ACCOUNT_KEY_PATH` - GEE datasets account key path

**Email:**
- `EMAIL_HOST_USER` - Email host user
- `EMAIL_HOST_PASSWORD` - Email host password

**AWS S3:**
- `S3_BUCKET` - S3 bucket name
- `S3_REGION` - S3 region
- `S3_ACCESS_KEY` - S3 access key
- `S3_SECRET_KEY` - S3 secret key
- `NREGA_BUCKET` - NREGA data bucket

**EarthData:**
- `EARTH_DATA_USER` - NASA EarthData username
- `EARTH_DATA_PASSWORD` - NASA EarthData password
- `USERNAME_GESDISC` - GES DISC username
- `PASSWORD_GESDISC` - GES DISC password

**Other:**
- `OVERPASS_URL` - Overpass API URL
- `LOCAL_COMPUTE_API_URL` - Local compute API URL
- `FERNET_KEY` - Encryption key
- `TMP_LOCATION` - Temporary file location
- `DEPLOYMENT_DIR` - Deployment directory

---

### Development Commands

**Run migrations:**
```bash
python manage.py migrate
```

**Create superuser:**
```bash
python manage.py createsuperuser
```

**Collect static files:**
```bash
python manage.py collectstatic
```

**Run tests:**
```bash
python manage.py test
```

---

### API Documentation

Interactive API documentation available at:
- **Swagger UI:** http://localhost/swagger/
- **ReDoc:** http://localhost/redoc/

API endpoints are organized under `/api/v1/`:
- `/api/v1/geoadmin/` - Geographic administrative data
- `/api/v1/computing/` - Geospatial analysis pipelines
- `/api/v1/plans/` - Planning and intervention tools
- `/api/v1/dpr/` - Detailed Project Reports
- `/api/v1/stats_generator/` - Statistical data generation
- `/api/v1/organization/` - Organization management
- `/api/v1/users/` - User authentication and management
- `/api/v1/projects/` - Project management
- `/api/v1/plantations/` - Plantation management
- `/api/v1/public_api/` - Public API endpoints
- `/api/v1/gee_computing/` - Google Earth Engine operations
- `/api/v1/community_engagement/` - Community engagement
- `/api/v1/bot_interface/` - WhatsApp bot interface
- `/api/v1/waterrejuvenation/` - Water rejuvenation tracking
- `/api/v1/moderation/` - Data moderation

---

### Integrating custom pipelines on CoREStack

We have prepared
a [detailed guide](https://docs.google.com/document/d/1lfx2hJKndmzVp55ZHIIFYqRTz-8fZCWc9QikUDQpTN0/edit?usp=sharing) on
how to integrate custom pipelines on the CoREStack backend.

### Further references
- [DB Design](https://github.com/core-stack-org/core-stack-backend/wiki/DB-Design) 
- [API Doc](https://api-doc.core-stack.org/)

---

### License

GNU Affero General Public License v3.0
