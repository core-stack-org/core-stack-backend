## CoRE Stack Backend 

### Installation

We provide a single installation script that handles everything:  
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

> The script will automatically install Conda, PostgreSQL, Apache, set up the `corestack-backend` environment, run migrations, and configure Apache.


#### 3. Open in Browser
- API Docs: [http://localhost](http://localhost)
- Django Admin: [http://localhost/admin/](http://localhost/admin/)

---

### Script path 
|    | Theme                    | Variable                        | Script path                                                           |
| -- | ------------------------ | ------------------------------- | --------------------------------------------------------------------- |
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
| 18 | Land use                 | First census of water bodies    | /computing/surface_water_bodies/swb3.py'                              |
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
| 31 | Disaster Risk            | Landslide Susceptibility        | /computing/landslide/landslide_vector.py                              |

### Integrating custom pipelines on CoREStack 
We have prepared a [detailed guide](https://docs.google.com/document/d/1lfx2hJKndmzVp55ZHIIFYqRTz-8fZCWc9QikUDQpTN0/edit?usp=sharing) on how to integrate custom pipelines on the CoREStack backend. 

### Further references
- [DB Design](https://github.com/core-stack-org/core-stack-backend/wiki/DB-Design) 
- [API Doc](https://github.com/core-stack-org/core-stack-backend/wiki/Project-API-Doc)
