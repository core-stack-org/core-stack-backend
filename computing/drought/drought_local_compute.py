"""Local drought layer of a tehsil, cut from the pan-India MWS drought layers.

utilities/scripts/generate_drought_layer.py (stage mws) writes one pan-India layer per
kharif season, data/base_layers/drought/v1/mws/drought_mws_<year>.gpkg. This task takes
the tehsil's MWS (the precomputed tehsil watersheds, as the other local layers do),
joins each season's columns by uid, adds avg_dryspell as the GEE merge does (the mean
of drysp_<year>), and publishes <district>_<block>_drought in the drought workspace,
the layer the GEE task made. Nothing is computed here.
"""

import logging
import os
import sqlite3

import pandas as pd

from computing.config_loader import LOCAL_DROUGHT_OUTPUT, PRECOMPUTED_TEHSIL_WATERSHED_DIR
from computing.drought.local import mws as drought_mws
from computing.drought.local.config import FIRST_SEASON, METHOD_VERSION, Paths
from computing.local_compute_helper import (
    build_output_vector_path,
    load_precomputed_watersheds,
    queue_local_vector_for_geoserver,
    write_vector_output,
)
from computing.utils import save_layer_info_to_db
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text

logger = logging.getLogger(__name__)

DATASET_NAME = "Drought"
GEOSERVER_WORKSPACE = "drought"
LOCAL_ALGORITHM = "local_drought"
LOCAL_ALGORITHM_VERSION = "local-1.0"
UID = "uid"
# drlb of an MWS without cropland, which has no weekly classes. The pan-India layer leaves
# it empty; GEE layers never had empty labels and readers call drlb.count(...).
NO_CLASSES = "[]"


def season_layer_path(season, paths=None):
    return drought_mws.layer_file(paths or Paths.build(), season)


def available_seasons(paths=None):
    folder = season_layer_path(FIRST_SEASON, paths).parent
    seasons = []
    for path in folder.glob("drought_mws_*.gpkg"):
        suffix = path.stem.rsplit("_", 1)[-1]
        if suffix.isdigit():
            seasons.append(int(suffix))
    return sorted(seasons)


def season_range(start_year=None, end_year=None, paths=None):
    """The seasons to cut; by default from 2017 to the latest pan-India layer."""
    start_year = int(start_year) if start_year not in (None, "") else FIRST_SEASON
    if end_year in (None, ""):
        seasons = available_seasons(paths)
        if not seasons:
            raise FileNotFoundError(
                f"No pan-India drought MWS layer in {season_layer_path(FIRST_SEASON, paths).parent}. "
                "Make them with utilities/scripts/generate_drought_layer.py (stage mws)."
            )
        end_year = seasons[-1]
    end_year = int(end_year)
    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")
    return start_year, end_year


def missing_drought_mws_layers(start_year=None, end_year=None, paths=None):
    """Seasons in the range whose pan-India layer is not there yet."""
    start_year, end_year = season_range(start_year, end_year, paths)
    return [y for y in range(start_year, end_year + 1) if not season_layer_path(y, paths).is_file()]


def read_season_columns(path, uids):
    """A season's drought columns for the given uids, indexed by uid; integer columns as Int64."""
    table = path.stem
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as con:
        info = con.execute(f'PRAGMA table_info("{table}")').fetchall()
        columns = [(name, kind.upper()) for _, name, kind, *_ in info
                   if name not in drought_mws.ATTRIBUTES and name not in ("fid", "geom")]
        con.execute("CREATE TEMP TABLE wanted (uid TEXT PRIMARY KEY)")
        con.executemany("INSERT OR IGNORE INTO wanted VALUES (?)", [(u,) for u in uids])
        select = ", ".join(f'd."{name}"' for name, _ in columns)
        frame = pd.read_sql(f'SELECT d.uid, {select} FROM "{table}" d JOIN wanted w ON d.uid = w.uid', con)
    for name, kind in columns:
        if "INT" in kind:
            frame[name] = frame[name].astype("Int64")
    return frame.set_index(UID)


def tehsil_drought(watersheds_gdf, start_year, end_year, paths=None):
    """The tehsil's MWS with every season's columns and avg_dryspell."""
    result = watersheds_gdf.copy()
    result[UID] = result[UID].astype(str)
    if result[UID].duplicated().any():
        raise ValueError("Duplicate uid values found in the tehsil watersheds")
    uids = result[UID].tolist()

    seasons = {}
    for year in range(start_year, end_year + 1):
        path = season_layer_path(year, paths)
        frame = read_season_columns(path, uids)
        missing = sorted(set(uids) - set(frame.index))
        if missing:
            raise ValueError(
                f"{path.name} has no row for {len(missing)} MWS of this tehsil. First missing uids: {missing[:5]}"
            )
        frame = frame.reindex(uids)
        frame[f"drlb_{year}"] = frame[f"drlb_{year}"].fillna(NO_CLASSES)
        seasons[year] = frame

    columns = pd.concat(seasons.values(), axis=1)
    dryspell = columns[[f"drysp_{y}" for y in seasons]].astype("float64")
    columns.insert(0, "avg_dryspell", dryspell.mean(axis=1, skipna=False))
    columns.index = result.index
    return result.join(columns)


def run_drought_local(
    state=None,
    district=None,
    block=None,
    start_year=None,
    end_year=None,
    paths=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    output_base_dir=LOCAL_DROUGHT_OUTPUT,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    if not (state and district and block):
        raise ValueError("state, district and block are required")
    state, district, block = (str(v).strip().lower() for v in (state, district, block))
    start_year, end_year = season_range(start_year, end_year, paths)
    missing = missing_drought_mws_layers(start_year, end_year, paths)
    if missing:
        raise FileNotFoundError(
            f"Pan-India drought MWS layer missing for {', '.join(map(str, missing))}. "
            "Make it with utilities/scripts/generate_drought_layer.py --stages mws."
        )

    watersheds_gdf, watershed_source = load_precomputed_watersheds(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    result_gdf = tehsil_drought(watersheds_gdf, start_year, end_year, paths)

    layer_name = f"{valid_gee_text(district)}_{valid_gee_text(block)}_drought"
    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=output_base_dir,
    )
    if os.path.exists(output_path):
        os.remove(output_path)
    asset_id = write_vector_output(result_gdf, output_path, layer_name)
    logger.info("Saved local drought vector: %s (%d MWS, %s-%s)", asset_id, len(result_gdf), start_year, end_year)

    layer_id = None
    if sync_layer_metadata:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name=DATASET_NAME,
            algorithm=LOCAL_ALGORITHM,
            algorithm_version=LOCAL_ALGORITHM_VERSION,
            misc={
                "start_year": start_year,
                "end_year": end_year,
                "is_generated_locally": True,
                "source": "base_layer_clip",
                "method_version": METHOD_VERSION,
                "watershed_source": watershed_source,
                "inputs": [str(season_layer_path(y, paths)) for y in range(start_year, end_year + 1)],
            },
        )

    if not push_to_geoserver:
        return True
    response = queue_local_vector_for_geoserver(
        path=os.path.splitext(asset_id)[0],
        layer_name=layer_name,
        workspace=GEOSERVER_WORKSPACE,
        file_type="gpkg",
        layer_id=layer_id,
    )
    logger.info("GeoServer publication for %s: %s", layer_name, response)
    return response.get("status_code") == 202


@app.task(bind=True)
def calculate_drought_local(
    self,
    state=None,
    district=None,
    block=None,
    start_year=None,
    end_year=None,
    gee_account_id=None,
):
    """Tehsil drought layer from the pan-India MWS layers; same arguments as the GEE task."""
    _ = self, gee_account_id
    return run_drought_local(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
    )
