import shutil
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter
from .downloads import lulc, soil
from .algorithms import tiled_timeseries, timeseries
from .downloads import rainfall
from . import config as cfg
from .utils import GeoTIFFHandler, make_logger
from . import utils
from .watershed_boundary import (
    DEFAULT_WATERSHED_ROOT,
    download_boundary_path,
    materialize_district_boundary,
    materialize_pan_india_boundary,
    materialize_state_boundary,
    materialize_tehsil_boundary,
)

logger = make_logger("runoff_only_with_rainfall.log")
PAN_INDIA_DEFAULT_TILE_SIZE = 11264
STATE_DEFAULT_TILE_SIZE = 4096


def format_elapsed(seconds):
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {seconds:.2f}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {seconds:.2f}s"


@contextmanager
def timed_stage(name):
    start_time = perf_counter()
    logger.info("Starting %s", name)
    try:
        yield
    except Exception:
        logger.exception("Failed %s after %s", name, format_elapsed(perf_counter() - start_time))
        raise
    else:
        logger.info("Finished %s in %s", name, format_elapsed(perf_counter() - start_time))


def validate_local_raster(path_value, option_name):
    if not path_value:
        return

    path = Path(path_value)
    if not path.exists():
        raise ValueError(f"{option_name} path does not exist: {path}")

    if path.is_dir():
        has_tif = any(
            child.is_file() and child.suffix.lower() in {".tif", ".tiff"}
            for child in path.rglob("*")
        )
        if not has_tif:
            raise ValueError(f"{option_name} directory has no GeoTIFF files: {path}")


def resolve_boundary(args):
    selectors = [args.pan_india, args.state, args.district, args.tehsil]
    if not any(selectors):
        return
    if args.pan_india and any([args.state, args.district, args.tehsil]):
        raise ValueError("--pan-india cannot be combined with --state, --district, or --tehsil")
    if args.pan_india:
        microwatersheds_path, source_paths, feature_count = materialize_pan_india_boundary(
            watershed_root=args.watershed_root,
            output_path=args.watershed_boundary_output,
            overwrite=not args.reuse_watershed_boundary,
        )
        boundary_path = download_boundary_path(microwatersheds_path)
        args.boundary = str(boundary_path)
        args.microwatersheds = str(microwatersheds_path)
        logger.info(
            "Resolved pan-India watershed boundary: sources=%s download_boundary=%s microwatersheds=%s features=%s",
            len(source_paths),
            boundary_path,
            microwatersheds_path,
            feature_count,
        )
        return

    if not args.state:
        raise ValueError("--state is required for watershed boundary lookup")
    if args.tehsil and not args.district:
        raise ValueError("--district is required when --tehsil is provided")

    if not args.district:
        microwatersheds_path, source_paths, feature_count = materialize_state_boundary(
            state=args.state,
            watershed_root=args.watershed_root,
            output_path=args.watershed_boundary_output,
            overwrite=not args.reuse_watershed_boundary,
        )
        boundary_path = download_boundary_path(microwatersheds_path)
        args.boundary = str(boundary_path)
        args.microwatersheds = str(microwatersheds_path)
        logger.info(
            "Resolved state watershed boundary: state=%s sources=%s download_boundary=%s microwatersheds=%s features=%s",
            args.state,
            len(source_paths),
            boundary_path,
            microwatersheds_path,
            feature_count,
        )
        return

    if not args.tehsil:
        microwatersheds_path, source_paths, feature_count = materialize_district_boundary(
            state=args.state,
            district=args.district,
            watershed_root=args.watershed_root,
            output_path=args.watershed_boundary_output,
            overwrite=not args.reuse_watershed_boundary,
        )
        boundary_path = download_boundary_path(microwatersheds_path)
        args.boundary = str(boundary_path)
        args.microwatersheds = str(microwatersheds_path)
        logger.info(
            "Resolved district watershed boundary: state=%s district=%s sources=%s download_boundary=%s microwatersheds=%s features=%s",
            args.state,
            args.district,
            len(source_paths),
            boundary_path,
            microwatersheds_path,
            feature_count,
        )
        return

    boundary_path, source_path, feature_count = materialize_tehsil_boundary(
        state=args.state,
        district=args.district,
        tehsil=args.tehsil,
        watershed_root=args.watershed_root,
        output_path=args.watershed_boundary_output,
        overwrite=not args.reuse_watershed_boundary,
    )
    args.boundary = str(boundary_path)
    args.microwatersheds = str(boundary_path)
    logger.info(
        "Resolved watershed boundary: state=%s district=%s tehsil=%s source=%s output=%s features=%s",
        args.state,
        args.district,
        args.tehsil,
        source_path,
        boundary_path,
        feature_count,
    )

def _required_arg(args, name):
    value = getattr(args, name, None)
    if value is None:
        raise ValueError(f"{name} is required")
    return value


def modify_cfg(args):
    cfg.BOUNDARY_GEOJSON_PATH = _required_arg(args, "boundary")
    cfg.MICROWATERSHEDS_PATH = _required_arg(args, "microwatersheds")
    cfg.LULC_SOURCE = args.lulc_source
    if args.local_lulc is not None:
        cfg.LULC_PATH = args.local_lulc
    if args.local_soil is not None:
        cfg.SOIL_PATH = args.local_soil
    cfg.RAINFALL_FOLDER = _required_arg(args, "rainfall_folder")
    cfg.RUNOFFS_FOLDER = _required_arg(args, "runoffs_folder")
    if args.t:
        configured_timeseries = getattr(args, "timeseries_vector", None)
        if configured_timeseries:
            cfg.TIMESERIES_VECTOR = Path(configured_timeseries)
        else:
            path_obj = Path(cfg.MICROWATERSHEDS_PATH)
            new_path = path_obj.with_name(f"{path_obj.stem}_timeseries{path_obj.suffix}")
            cfg.TIMESERIES_VECTOR = new_path
    cfg.ARG_START_DATE = args.start
    cfg.ARG_END_DATE = args.end
    cfg.TILE_SIZE = args.tile_size

def prereq(args):
    downloaders = []
    if args.local_lulc:
        logger.info("Using local LULC from %s; skipping downloads.lulc.Downloader", args.local_lulc)
    else:
        downloaders.append(lulc.Downloader)

    if args.local_soil:
        logger.info("Using local soil from %s; skipping downloads.soil.Downloader", args.local_soil)
    else:
        downloaders.append(soil.Downloader)

    downloaders.append(rainfall.Download_to_database)

    for downloader in downloaders:
        stage_name = f"prerequisite downloader: {downloader.__module__}.{downloader.__name__}"
        with timed_stage(stage_name):
            downloader().main()
