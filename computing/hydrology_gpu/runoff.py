import shutil
from argparse import ArgumentParser
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter
import ee
from .downloads import dem, lulc, soil
from .algorithms import tiled_timeseries, timeseries
from .downloads import rainfall
from . import config as cfg
from .lulc_mapping import LULC_SOURCE_INDIASATV3, LULC_SOURCES
from .utils import GeoTIFFHandler, make_logger
from . import utils
from .watershed_boundary import (
    DEFAULT_WATERSHED_ROOT,
    PAN_INDIA_SLUG,
    download_boundary_path,
    materialize_district_boundary,
    materialize_pan_india_boundary,
    materialize_state_boundary,
    materialize_tehsil_boundary,
    slugify,
)

parser = ArgumentParser()
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


def selected_output_folder(root, args):
    if getattr(args, "pan_india", False):
        return str(Path(root) / PAN_INDIA_SLUG)

    path = Path(root) / slugify(args.state)
    if args.district:
        path = path / slugify(args.district)
    if args.tehsil:
        path = path / slugify(args.tehsil)
    return str(path)


def validate_local_raster(path_value, option_name):
    if not path_value:
        return

    path = Path(path_value)
    if not path.exists():
        parser.error(f"{option_name} path does not exist: {path}")

    if path.is_dir():
        has_tif = any(
            child.is_file() and child.suffix.lower() in {".tif", ".tiff"}
            for child in path.rglob("*")
        )
        if not has_tif:
            parser.error(f"{option_name} directory has no GeoTIFF files: {path}")


def resolve_boundary(args):
    selectors = [args.pan_india, args.state, args.district, args.tehsil]
    if not any(selectors):
        return
    if args.pan_india and any([args.state, args.district, args.tehsil]):
        parser.error("--pan-india cannot be combined with --state, --district, or --tehsil")
    if args.pan_india:
        microwatersheds_path, source_paths, feature_count = materialize_pan_india_boundary(
            watershed_root=args.watershed_root,
            output_path=args.watershed_boundary_output,
            overwrite=not args.reuse_watershed_boundary,
        )
        boundary_path = download_boundary_path(microwatersheds_path)
        args.boundary = str(boundary_path)
        args.microwatersheds = str(microwatersheds_path)
        if args.rainfall_folder is None:
            args.rainfall_folder = selected_output_folder("./tifs/rainfall_pan_india", args)
        if args.runoffs_folder is None:
            args.runoffs_folder = selected_output_folder("./tifs/runoffs_pan_india", args)
        logger.info(
            "Resolved pan-India watershed boundary: sources=%s download_boundary=%s microwatersheds=%s features=%s",
            len(source_paths),
            boundary_path,
            microwatersheds_path,
            feature_count,
        )
        return

    if not args.state:
        parser.error("--state is required for watershed boundary lookup")
    if args.tehsil and not args.district:
        parser.error("--district is required when --tehsil is provided")

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
        if args.rainfall_folder is None:
            args.rainfall_folder = selected_output_folder("./tifs/rainfall_state", args)
        if args.runoffs_folder is None:
            args.runoffs_folder = selected_output_folder("./tifs/runoffs_state", args)
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
        if args.rainfall_folder is None:
            args.rainfall_folder = selected_output_folder("./tifs/rainfall_district", args)
        if args.runoffs_folder is None:
            args.runoffs_folder = selected_output_folder("./tifs/runoffs_district", args)
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
    if args.rainfall_folder is None:
        args.rainfall_folder = selected_output_folder("./tifs/rainfall_tehsil", args)
    if args.runoffs_folder is None:
        args.runoffs_folder = selected_output_folder("./tifs/runoffs_tehsil", args)
    logger.info(
        "Resolved watershed boundary: state=%s district=%s tehsil=%s source=%s output=%s features=%s",
        args.state,
        args.district,
        args.tehsil,
        source_path,
        boundary_path,
        feature_count,
    )

def modify_cfg(args):
    cfg.BOUNDARY_GEOJSON_PATH = args.boundary
    cfg.MICROWATERSHEDS_PATH = getattr(args, "microwatersheds", args.boundary)
    cfg.LULC_SOURCE = args.lulc_source
    if args.local_lulc is not None:
        cfg.LULC_PATH = args.local_lulc
    if args.local_soil is not None:
        cfg.SOIL_PATH = args.local_soil
    if args.rainfall_folder is not None:
        cfg.RAINFALL_FOLDER = args.rainfall_folder
    if args.runoffs_folder is not None:
        cfg.RUNOFFS_FOLDER = args.runoffs_folder
    if args.t:
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

if __name__=="__main__":
    overall_start = perf_counter()
    logger.info("Starting up")

    try:
        parser.add_argument('-p', "--pre-req", action='store_true', help="also do pre-req stuff")
        parser.add_argument('-b', '--boundary', help="use another boundary file", default=cfg.BOUNDARY_GEOJSON_PATH)
        parser.add_argument('-t', help="Dump timeseries next to boundary file", action='store_true')
        parser.add_argument('--start', help="in YYYY-MM-DD format (inclusive)", default=cfg.ARG_START_DATE)
        parser.add_argument('--end', help="in YYYY-MM-DD format (exclusive)", default=cfg.ARG_END_DATE)
        parser.add_argument('--rainfall-folder', help=f"folder containing rainfall_archive.zarr (default: {cfg.RAINFALL_FOLDER})")
        parser.add_argument('--runoffs-folder', help=f"folder where runoff GeoZarr output will be written (default: {cfg.RUNOFFS_FOLDER})")
        parser.add_argument('--pan-india', action='store_true', help="run all written watershed boundaries in the manifest as one pan-India job")
        parser.add_argument('--state', help="state name for state/district/tehsil watershed lookup; omit --district to run the whole state")
        parser.add_argument('--district', help="district name for district/tehsil watershed lookup")
        parser.add_argument('--tehsil', help="optional tehsil name; omit to run the whole district")
        parser.add_argument('--watershed-root', default=str(DEFAULT_WATERSHED_ROOT), help="root folder containing tehsil watershed GeoPackages")
        parser.add_argument('--watershed-boundary-output', help="optional GeoJSON path for the resolved watershed boundary")
        parser.add_argument('--reuse-watershed-boundary', action='store_true', help="reuse an existing resolved watershed GeoJSON instead of rebuilding it")
        parser.add_argument('--local-dem', help="clip this local terrain/slope raster to the selected boundary instead of downloading DEM/slope from GEE")
        parser.add_argument('--local-lulc', help="read LULC from this local GeoTIFF file or folder of GeoTIFF tiles instead of downloading LULC from GEE")
        parser.add_argument('--lulc-source', choices=LULC_SOURCES, default=getattr(cfg, "LULC_SOURCE", "dynamicworld"), help="LULC class scheme for --local-lulc or downloaded LULC")
        parser.add_argument('--local-soil', help="read soil/HSG from this local GeoTIFF file or folder of GeoTIFF tiles instead of downloading soil from GEE")
        parser.add_argument('--tile-size', type=int, default=None, help=f"process runoff/timeseries in square pixel tiles; default is {PAN_INDIA_DEFAULT_TILE_SIZE} for pan-India, {STATE_DEFAULT_TILE_SIZE} for whole-state runs, and disabled otherwise; pass 0 to disable")

        args = parser.parse_args()
        if args.lulc_source == LULC_SOURCE_INDIASATV3 and args.local_lulc is None:
            args.local_lulc = getattr(cfg, "INDIASATV3_LULC_PATH", "./tifs/lulc_v3_2024_2025.tif")
        validate_local_raster(args.local_lulc, "--local-lulc")
        validate_local_raster(args.local_soil, "--local-soil")
        resolve_boundary(args)
        if args.tile_size is None:
            if args.pan_india:
                args.tile_size = PAN_INDIA_DEFAULT_TILE_SIZE
            elif args.state and not args.district:
                args.tile_size = STATE_DEFAULT_TILE_SIZE
            else:
                args.tile_size = 0
        if args.tile_size < 0:
            parser.error("--tile-size cannot be negative")
        modify_cfg(args)

        if args.pre_req:
            shutil.rmtree(cfg.RAINFALL_FOLDER, ignore_errors=True)
            if args.local_dem:
                with timed_stage(f"local DEM/slope clip from {args.local_dem}"):
                    dem.clip_local_raster(args.local_dem, cfg.BOUNDARY_GEOJSON_PATH, cfg.DEMFILE_PATH, logger)
            else:
                with timed_stage("prerequisite downloader: downloads.dem.Downloader"):
                    dem.Downloader().main()
            with timed_stage(f"loading DEM reference grid from {cfg.DEMFILE_PATH}"):
                utils.tif_handler = GeoTIFFHandler(cfg.DEMFILE_PATH, logger)
            with timed_stage("remaining prerequisite downloads"):
                prereq(args)
        else:
            with timed_stage(f"loading DEM reference grid from {cfg.DEMFILE_PATH}"):
                utils.tif_handler = GeoTIFFHandler(cfg.DEMFILE_PATH, logger)

        shutil.rmtree(cfg.RUNOFFS_FOLDER, ignore_errors=True)
        with timed_stage("runoff/timeseries processing"):
            if args.tile_size:
                logger.info(
                    "Using tiled runoff/timeseries processing with tile_size=%s; runoff GeoZarr rasters are skipped in tiled mode",
                    args.tile_size,
                )
                tiled_timeseries.TiledTimeSeries(args.tile_size).run()
            else:
                timeseries.TimeSeries().run()
        logger.info("Done")
    finally:
        logger.info("Overall runtime: %s", format_elapsed(perf_counter() - overall_start))
