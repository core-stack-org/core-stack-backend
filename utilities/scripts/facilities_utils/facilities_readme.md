### Data Processing for Facilities' layer:

For this layer, we downloaded data from multiple resources -- primarily from https://grammanchitra.gov.in/, which used publicly available GIS layers of facilities prepared for PMGSY implementation plan.

We then cleaned and compiled it into 25 csv files, one for each facility type. Each file necessarily contain an uid column {facility}_uid for the facility, along with its coordinates ({facility}_lat, {facility}_long). Wherever the names and any other properties, classifications of the facility were available, they were also retained. You can find the script [here](https://github.com/core-stack-org/core-stack-backend/blob/main/utilities/scripts/facilities_data_cleaning.py).

Using the location in these pan-india point-files and shapes of villages in corestack data/admin-boundary/input/, we calculated the distance of closest facility to each village across India, using k-d tree algorithm. We ran [this process](https://github.com/core-stack-org/core-stack-backend/blob/main/utilities/scripts/closest_facility_finder.py) across all 25 facilities, in order to get the distances of the closest-facility-access-points for each of the village. The output geojson file contains distances of 25 facilities from each Indian village, along with 9 columns of basic identifiers of village, such as id, name, state, district, tehsil names etc. 

The file is also publicly available on GoogleEarthEngine (1. Pan India [Facilities GIS Location Dataset](https://code.earthengine.google.com/?asset=projects/corestack-datasets/assets/facilities), 2. Pan India [Village Facility Proximity Dataset](https://code.earthengine.google.com/?asset=projects/corestack-datasets/assets/facilities)) and on [GoogleDrive](https://docs.google.com/spreadsheets/d/1xS5d7vgyjyoqqnmmajKDZBx9qS6GqyAdSbNDR62ot2Y/edit?gid=0#gid=0&range=A138).

### GEE v2 asset exports

`facilities_gee_assets.py` builds two Earth Engine table-ingestion TSVs from the
current facilities GeoPackages. The full reproducible flow is documented in
[`GEE_INGESTION_README.md`](GEE_INGESTION_README.md).

- `pan_india_facilities_v2`: point features expanded by facility-class
  membership, so L1/L2/L3/L4 and `class_k*` filters work directly in GEE.
- `village_facility_proximity_v2`: one village polygon per row with wide L3/L2
  distance and nearest-facility UID columns for fast map styling.

Smoke test without creating full outputs:

```bash
uv run --with pyyaml --with shapely \
  python utilities/scripts/facilities_utils/facilities_gee_assets.py build \
  --asset village_facility_proximity_v2 --limit-villages 10 \
  --output-suffix .sample --overwrite
```

Full build:

```bash
uv run --with pyyaml --with shapely \
  python utilities/scripts/facilities_utils/facilities_gee_assets.py build --asset all --overwrite
```

Upload built TSVs to GEE:

```bash
uv run --with geemap --with google-cloud-storage --with pyyaml --with shapely \
  python utilities/scripts/facilities_utils/facilities_gee_assets.py upload \
  --asset all --replace-existing --wait --make-public --cleanup-gcs
```

The local outputs are tab-delimited, but the upload stages them to GCS with a
`.csv` suffix because Earth Engine identifies CSV table sources by file
extension. The manifest still sets `csvDelimiter` to a tab.
