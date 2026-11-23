"""Local drought causality layer of a tehsil, cut from the pan-India MWS drought layers.

The mws stage of the drought pipeline (computing/drought/local/mws.py, causality_columns)
computes drought causality for every MWS and season with the local drought rules: for a
drought year (more than 5 moderate + severe weeks), the three most frequent combinations of
window class, the triggers that fired and each impact's class, over the moderate and severe
windows and over the mild ones. This task takes the tehsil's MWS (the precomputed tehsil
watersheds), joins each season's two causality columns by uid and publishes
<district>_<block>_drought_causality in the drought_causality workspace, the layer the GEE task
(drought_causality.py) made. Nothing is computed here.

Columns: uid, area_in_ha and, per season, severe_moderate_drought_causality_<year> and
mild_drought_causality_<year>: JSON objects such as
    {"moderate: scanty rain + SPI; VCI moderate, MAI severe, area sown normal": 3}
(combination -> windows), empty when the season is not a drought year for the MWS. The GEE
task's layer named them se_mo_<year> and mild_<year>, with path numbers and scores.
"""

import logging

import pandas as pd

from computing.config_loader import LOCAL_DROUGHT_CAUSALITY_OUTPUT, PRECOMPUTED_TEHSIL_WATERSHED_DIR
from computing.drought import drought_local_compute as drought_local
from computing.drought.local import mws as drought_mws
from nrm_app.celery import app

logger = logging.getLogger(__name__)

DATASET_NAME = "Drought Causality"
GEOSERVER_WORKSPACE = "drought_causality"
LOCAL_ALGORITHM = "local_drought_causality"
LOCAL_ALGORITHM_VERSION = "local-1.0"
UID = drought_local.UID
KEEP_ATTRIBUTES = (UID, "area_in_ha")


def seasons_without_causality(start_year=None, end_year=None, paths=None):
    """Seasons in the range whose pan-India layer exists but was made before drought causality was
    added (no causality columns), so the API can refuse before queueing."""
    start_year, end_year = drought_local.season_range(start_year, end_year, paths)
    without = []
    for year in range(start_year, end_year + 1):
        path = drought_local.season_layer_path(year, paths)
        if path.is_file() and not any(drought_mws.causality_column(c) for c in drought_local.layer_column_names(path)):
            without.append(year)
    return without


def tehsil_causality(watersheds_gdf, start_year, end_year, paths=None):
    """The tehsil's MWS (uid, area_in_ha, geometry) with every season's causality columns."""
    keep = [c for c in KEEP_ATTRIBUTES if c in watersheds_gdf.columns] + [watersheds_gdf.geometry.name]
    result = watersheds_gdf[keep].copy()
    result[UID] = result[UID].astype(str)
    if result[UID].duplicated().any():
        raise ValueError("Duplicate uid values found in the tehsil watersheds")
    uids = result[UID].tolist()

    seasons = []
    for year in range(start_year, end_year + 1):
        path = drought_local.season_layer_path(year, paths)
        frame = drought_local.read_season_columns(path, uids, keep=drought_mws.causality_column)
        if frame.columns.empty:
            raise ValueError(
                f"{path.name} has no drought causality columns: it was made before drought causality was "
                "added. Make it again with utilities/scripts/generate_drought_layer.py --stages mws."
            )
        missing = sorted(set(uids) - set(frame.index))
        if missing:
            raise ValueError(
                f"{path.name} has no row for {len(missing)} MWS of this tehsil. First missing uids: {missing[:5]}"
            )
        seasons.append(frame.reindex(uids))

    columns = pd.concat(seasons, axis=1)
    columns.index = result.index
    return result.join(columns)


def run_drought_causality_local(
    state=None,
    district=None,
    block=None,
    start_year=None,
    end_year=None,
    paths=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    output_base_dir=LOCAL_DROUGHT_CAUSALITY_OUTPUT,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    return drought_local.publish_tehsil_layer(
        build=tehsil_causality,
        layer_suffix="drought_causality",
        dataset_name=DATASET_NAME,
        algorithm=LOCAL_ALGORITHM,
        algorithm_version=LOCAL_ALGORITHM_VERSION,
        workspace=GEOSERVER_WORKSPACE,
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        paths=paths,
        precomputed_roi_dir=precomputed_roi_dir,
        output_base_dir=output_base_dir,
        push_to_geoserver=push_to_geoserver,
        sync_layer_metadata=sync_layer_metadata,
    )


@app.task(bind=True)
def drought_causality_local(
    self,
    state=None,
    district=None,
    block=None,
    start_year=None,
    end_year=None,
    gee_account_id=None,
    app_type="MWS",
):
    """Tehsil drought causality layer from the pan-India MWS layers; same arguments as the GEE task."""
    _ = self, gee_account_id, app_type
    return run_drought_causality_local(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
    )
