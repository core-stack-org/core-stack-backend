import logging

from nrm_app.celery import app

from computing.config_loader import PROJECT_ROOT
from computing.hydrology_gpu.et_download import download_pan_india_et_assets

from .runoff_gpu import HYDROLOGY_OUTPUT_ROOT, _parse_bool, _resolve_dates


def _make_logger():
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("hydrology_gpu.et_download")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_dir / "et_download.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def run_et_download_local(
    *,
    pan_india=False,
    start_date=None,
    end_date=None,
    start_year=None,
    end_year=None,
    overwrite=False,
):
    pan_india = _parse_bool(pan_india)
    if not pan_india:
        raise ValueError("et_download currently supports pan_india=true only")

    start_date, end_date, annual_start_year, annual_end_year = _resolve_dates(
        start_date=start_date,
        end_date=end_date,
        start_year=start_year,
        end_year=end_year,
    )
    annual_key = f"{annual_start_year}_{annual_end_year}"
    output_root = HYDROLOGY_OUTPUT_ROOT / "pan_india" / annual_key
    output_root.mkdir(parents=True, exist_ok=True)

    logger = _make_logger()
    manifest = download_pan_india_et_assets(
        output_root=output_root,
        start_date=start_date,
        end_date=end_date,
        overwrite=_parse_bool(overwrite),
        logger=logger,
    )

    return {
        "scope": "pan_india",
        "start_date": start_date,
        "end_date": end_date,
        "annual_key": annual_key,
        "output_root": str(output_root),
        "et_root": manifest["et_root"],
        "sources": manifest["sources"],
        "manifest": str(output_root / "et" / "manifest.json"),
    }


@app.task(bind=True)
def et_download(
    self,
    pan_india=False,
    start_date=None,
    end_date=None,
    start_year=None,
    end_year=None,
    overwrite=False,
):
    _ = self
    return run_et_download_local(
        pan_india=pan_india,
        start_date=start_date,
        end_date=end_date,
        start_year=start_year,
        end_year=end_year,
        overwrite=overwrite,
    )
