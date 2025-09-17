# CoRE Stack Backend Setup

# DB Setup
Install PostgreSQL
1. sudo apt-get update
2. sudo apt-get install postgresql postgresql-contrib

*Start PostgreSQL*
1. sudo service postgresql start

*Check PostgreSQL status*
1. sudo systemctl status postgresql

*Create a new database*
1. sudo -u postgres psql
2. CREATE DATABASE corestack;

*Create a new user*
1. sudo -u postgres psql
2. create user corestack_user with password 'your_password';
3. GRANT USAGE, CREATE ON SCHEMA public TO corestack_user;
4. ALTER DATABASE corestack OWNER TO corestack_user;
5. ALTER USER corestack_user WITH SUPERUSER; (Optional, only if you want superuser rights)

# Installation of the libraries
If you want to install directly from a conda snap of environment.yml
> conda env create -f environment.yml --verbose

This will create a new conda environment called *black* with all the dependencies specified in the environment.yml file.

# Running the server
After the successfull installation of all the packages, run the following commands to start the Django server:
1. conda activate black (or whatever is the name of your virtual environment)
2. python manage.py runserver 

*Make Migrations*
1. Make sure the environment is active. Otherwise, run `conda activate black`
2. Run `python manage.py migrate`
For any migration related queries, please send an email to contact@core-stack.org

*Run the server*
1. Make sure the environment is active. Otherwise, run `conda activate black`
2. Run `python manage.py runserver`
> Tip: To make local hits to your server, use `python manage.py runserver 0.0.0.0:8080`

*Running celery*
If you are running some tasks, you need to run `celery -A tasks worker -l info`
> example: `celery -A nrm_app worker -l info -Q nrm &` 
nrm queue is running in this example.

# Env and config setup
1. Use .env.example as a template to create .env file inside nrm_app/ directory
2. Update the environment variables
3. Add *JSON* files inside the data/ directory

# Layers script path mapping
|    | Theme                    | Variable                            | Script path                                                                                                                                    |
| -- | ------------------------ | ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| 1  | Hydrology                | Microwatersheds                     | /computing/mws/mws.py                                                                                                                          |
| 2  | Hydrology                | Upstream Downstream Microwatersheds |                                                                                                                                                |
| 3  | Hydrology                | Precipitation                       | /computing/mws/precipitation.py                                                                                                                |
| 4  | Hydrology                | Runoff                              | /computing/mws/run_off.py                                                                                                                      |
| 5  | Hydrology                | Evapotranspiration                  | /computing/mws/evapotranspiration.py                                                                                                           |
| 6  | Hydrology                | Change in groundwater               | /computing/mws/delta_g.py                                                                                                                      |
| 7  | Hydrology                | Change in well depth                | /computing/mws/well_depth.py                                                                                                                   |
| 8  | Hydrology                | Aquifers                            | /computing/misc/aquifer_vector.py                                                                                                              |
| 9  | Hydrology                | Stage of Groundwater Extraction     | /computing/misc/soge_vector.py                                                                                                                 |
| 10 | Climate                  | Drought frequency and intensity     | /computing/drought/drought.py                                                                                                                  |
| 11 | Climate                  | Drought causality                   | /computing/drought/drought_causality.py                                                                                                        |
| 12 | Terrain                  | Terrain classification              | /computing/terrain_descriptor/terrain_raster.py                                                                                                |
| 13 | Terrain                  | Terrain cluster                     | /computing/terrain_descriptor/terrain_clusters.py                                                                                              |
| 14 | Land use                 | Land use land cover                 | /computing/lulc/lulc_v3.py                                                                                                                     |
| 15 | Land use                 | Land use on terrain                 | Land use on Plain: /computing/lulc_X_terrain/lulc_on_plain_cluster.py<br>Land use on Slope: /computing/lulc_X_terrain/lulc_on_slope_cluster.py |
| 16 | Land use                 | Land use changes                    | /computing/change_detection/change_detection.py                                                                                                |
| 17 | Land use                 | Cropping intensity                  | /computing/cropping_intensity/cropping_intensity.py                                                                                            |
| 18 | Land use                 | Water bodies                        | /computing/surface_water_bodies/swb.py                                                                                                         |
| 19 | Land use                 | First census of water bodies        | /computing/surface_water_bodies/swb3.py'                                                                                                       |
| 20 | Tree health              | Tree canopy cover density           | /computing/tree_health/ccd.py                                                                                                                  |
| 21 | Tree health              | Tree canopy height                  | /computing/tree_health/canopy_height.py                                                                                                        |
| 22 | Tree health              | Tree cover change                   | /computing/tree_health/overall_change.py                                                                                                       |
| 23 | Welfare                  | NREGA assets categorization         | /computing/misc/nrega.py                                                                                                                       |
| 24 | Administrative           | Village                             | /computing/misc/admin_boundary.py                                                                                                              |
| 25 | Water structure planning | Lithology                           | /computing/clart/lithology.py                                                                                                                  |
| 26 | Water structure planning | Drainage lines                      | /computing/misc/drainage_lines.py                                                                                                              |
| 27 | Water structure planning | Stream order raster                 | /computing/misc/stream_order.py                                                                                                                |
| 28 | Water structure planning | CLART                               | /computing/clart/clart.py                                                                                                                      |
# More
- [DB Design](https://github.com/core-stack-org/core-stack-backend/wiki/DB-Design) 
- [API Doc](https://github.com/core-stack-org/core-stack-backend/wiki/Project-API-Doc)