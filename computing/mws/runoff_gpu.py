import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from nrm_app.celery import app

from computing.config_loader import (
    LULC_BASE_DIR,
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    PROJECT_ROOT,
    SOIL_RASTER_PATH,
    TERRAIN_RASTER_PATH,
)
from utilities.gee_utils import valid_gee_text


DATA_ROOT = PROJECT_ROOT / "data"
HYDROLOGY_OUTPUT_ROOT = DATA_ROOT / "hydrology_gpu"
DEFAULT_LOCAL_DEM_PATH = TERRAIN_RASTER_PATH
DEFAULT_LOCAL_SOIL_PATH = SOIL_RASTER_PATH
PAN_INDIA_DEFAULT_TILE_SIZE = 11264
STATE_DEFAULT_TILE_SIZE = 4096


def _is_blank(value):
    return value is None or str(value).strip().lower() in {"", "none", "null"}


def _parse_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _parse_date(value, field_name):
    if _is_blank(value):
        raise ValueError(f"{field_name} is required")
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be in YYYY-MM-DD format") from exc


def _slug(value, fallback):
    return valid_gee_text(str(value).strip().lower()) or fallback


def _resolve_dates(start_date, end_date, start_year=None, end_year=None):
    if not _is_blank(start_date) and not _is_blank(end_date):
        start = _parse_date(start_date, "start_date")
        end = _parse_date(end_date, "end_date")
        if end <= start:
            raise ValueError("end_date must be after start_date")
        return start.isoformat(), end.isoformat(), start.year, end.year if end.year > start.year else start.year + 1

    if _is_blank(start_year) or _is_blank(end_year):
        raise ValueError("Provide start_date and end_date, or start_year and end_year")

    start_year = int(start_year)
    end_year = int(end_year)
    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    annual_end_year = end_year + 1
    return f"{start_year}-07-01", f"{annual_end_year}-07-01", start_year, annual_end_year


def _resolve_lulc_path(lulc_start_year, lulc_end_year):
    expected_name = f"lulc_v3_{lulc_start_year}_{lulc_end_year}.tif"
    lulc_path = LULC_BASE_DIR / expected_name
    if not lulc_path.exists():
        raise FileNotFoundError(f"LULC raster not found for requested annual period: {lulc_path}")
    return lulc_path


def _validate_scope(pan_india, state, district, tehsil):
    if pan_india:
        if any(not _is_blank(value) for value in (state, district, tehsil)):
            raise ValueError("pan_india=true cannot be combined with state, district, or tehsil")
        return "pan_india", None, None, None

    if _is_blank(state):
        raise ValueError("state is required unless pan_india=true")
    if not _is_blank(tehsil) and _is_blank(district):
        raise ValueError("district is required when tehsil is provided")

    state = str(state).strip()
    district = None if _is_blank(district) else str(district).strip()
    tehsil = None if _is_blank(tehsil) else str(tehsil).strip()

    if tehsil:
        return "tehsil", state, district, tehsil
    if district:
        return "district", state, district, None
    return "state", state, None, None


def _scope_slug(scope, state, district, tehsil):
    parts = [scope]
    for value, fallback in ((state, "state"), (district, "district"), (tehsil, "tehsil")):
        if not _is_blank(value):
            parts.append(_slug(value, fallback))
    return "/".join(parts)


def _ensure_default_inputs_exist():
    for label, path in (
        ("Default local DEM/terrain raster", DEFAULT_LOCAL_DEM_PATH),
        ("Default local soil raster", DEFAULT_LOCAL_SOIL_PATH),
    ):
        if not path.exists():
            raise FileNotFoundError(f"{label} not found: {path}")


def _build_runner_args(
    *,
    state,
    district,
    tehsil,
    pan_india,
    start_date,
    end_date,
    local_lulc_path,
    annual_key,
):
    scope, state, district, tehsil = _validate_scope(pan_india, state, district, tehsil)
    slug_path = _scope_slug(scope, state, district, tehsil)
    output_root = HYDROLOGY_OUTPUT_ROOT / slug_path / annual_key
    boundary_output = output_root / "boundaries" / f"{slug_path.replace('/', '_')}.geojson"

    return SimpleNamespace(
        pre_req=True,
        boundary=None,
        t=True,
        start=start_date,
        end=end_date,
        rainfall_folder=str(output_root / "rainfall"),
        runoffs_folder=str(output_root / "runoffs"),
        demfile_path=str(output_root / "dem.tif"),
        pan_india=pan_india,
        state=state,
        district=district,
        tehsil=tehsil,
        watershed_root=str(PRECOMPUTED_TEHSIL_WATERSHED_DIR),
        watershed_boundary_output=str(boundary_output),
        reuse_watershed_boundary=True,
        local_dem=str(DEFAULT_LOCAL_DEM_PATH),
        local_lulc=str(local_lulc_path),
        lulc_source="indiasatv3",
        local_soil=str(DEFAULT_LOCAL_SOIL_PATH),
        tile_size=None,
    )


@contextmanager
def _working_directory(path):
    previous = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def run_runoff_gpu_local(
    *,
    state=None,
    district=None,
    tehsil=None,
    pan_india=False,
    start_date=None,
    end_date=None,
    start_year=None,
    end_year=None,
):
    _ensure_default_inputs_exist()
    pan_india = _parse_bool(pan_india)
    start_date, end_date, lulc_start_year, lulc_end_year = _resolve_dates(
        start_date=start_date,
        end_date=end_date,
        start_year=start_year,
        end_year=end_year,
    )
    local_lulc_path = _resolve_lulc_path(lulc_start_year, lulc_end_year)
    annual_key = f"{lulc_start_year}_{lulc_end_year}"
    args = _build_runner_args(
        state=state,
        district=district,
        tehsil=tehsil,
        pan_india=pan_india,
        start_date=start_date,
        end_date=end_date,
        local_lulc_path=local_lulc_path,
        annual_key=annual_key,
    )

    with _working_directory(PROJECT_ROOT):
        from computing.hydrology_gpu import runoff as hydro_runoff
        from computing.hydrology_gpu.downloads import dem

        hydro_runoff.validate_local_raster(args.local_lulc, "--local-lulc")
        hydro_runoff.validate_local_raster(args.local_soil, "--local-soil")
        hydro_runoff.resolve_boundary(args)
        if args.tile_size is None:
            if args.pan_india:
                args.tile_size = PAN_INDIA_DEFAULT_TILE_SIZE
            elif args.state and not args.district:
                args.tile_size = STATE_DEFAULT_TILE_SIZE
            else:
                args.tile_size = 0
        hydro_runoff.modify_cfg(args)
        hydro_runoff.cfg.DEMFILE_PATH = args.local_dem if args.pan_india else args.demfile_path

        hydro_runoff.shutil.rmtree(hydro_runoff.cfg.RAINFALL_FOLDER, ignore_errors=True)
        if args.pan_india:
            hydro_runoff.logger.info(
                "Using existing pan-India DEM/slope raster directly: %s",
                hydro_runoff.cfg.DEMFILE_PATH,
            )
        else:
            with hydro_runoff.timed_stage(f"local DEM/slope clip from {args.local_dem}"):
                dem.clip_local_raster(
                    args.local_dem,
                    hydro_runoff.cfg.BOUNDARY_GEOJSON_PATH,
                    hydro_runoff.cfg.DEMFILE_PATH,
                    hydro_runoff.logger,
                )

        with hydro_runoff.timed_stage(f"loading DEM reference grid from {hydro_runoff.cfg.DEMFILE_PATH}"):
            hydro_runoff.utils.tif_handler = hydro_runoff.GeoTIFFHandler(
                hydro_runoff.cfg.DEMFILE_PATH,
                hydro_runoff.logger,
            )

        with hydro_runoff.timed_stage("remaining prerequisite downloads"):
            hydro_runoff.prereq(args)

        hydro_runoff.shutil.rmtree(hydro_runoff.cfg.RUNOFFS_FOLDER, ignore_errors=True)
        with hydro_runoff.timed_stage("runoff/timeseries processing"):
            if args.tile_size:
                hydro_runoff.tiled_timeseries.TiledTimeSeries(args.tile_size).run()
            else:
                hydro_runoff.timeseries.TimeSeries().run()

    return {
        "scope": "pan_india" if args.pan_india else ("tehsil" if args.tehsil else ("district" if args.district else "state")),
        "state": args.state,
        "district": args.district,
        "tehsil": args.tehsil,
        "start_date": start_date,
        "end_date": end_date,
        "annual_key": annual_key,
        "lulc_path": str(local_lulc_path),
        "rainfall_folder": args.rainfall_folder,
        "runoffs_folder": args.runoffs_folder,
        "boundary": args.boundary,
        "microwatersheds": args.microwatersheds,
        "timeseries_vector": str(hydro_runoff.cfg.TIMESERIES_VECTOR),
        "tile_size": args.tile_size,
    }


@app.task(bind=True)
def generate_runoff_gpu(
    self,
    state=None,
    district=None,
    tehsil=None,
    pan_india=False,
    start_date=None,
    end_date=None,
    start_year=None,
    end_year=None,
):
    _ = self
    return run_runoff_gpu_local(
        state=state,
        district=district,
        tehsil=tehsil,
        pan_india=pan_india,
        start_date=start_date,
        end_date=end_date,
        start_year=start_year,
        end_year=end_year,
    )
