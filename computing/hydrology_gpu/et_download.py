import datetime as dt
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode

import requests
from django.conf import settings
import numpy as np
import rasterio
from rasterio.fill import fillnodata


GESDISC_OTF_URL = "https://hydro1.gesdisc.eosdis.nasa.gov/daac-bin/OTF/HTTP_services.cgi"
EVAP_VARIABLE = "Evap_tavg"
FLDAS_FORMAT = "Y29nLw"

FLDAS_CA_DAILY_SOURCE = "fldas_ca_daily"
FLDAS_CA_DAILY_PATCH_FILLED_SOURCE = "fldas_ca_daily_patch_filled"
FLDAS_CA_DAILY_SHORTNAME = "FLDAS_NOAHMP001_G_CA_D"
FLDAS_CA_DAILY_PRODUCT = "FLDAS_NOAHMP001_G_CA_D.001"
FLDAS_CA_DAILY_BBOX = "21,65.566,37.932,99.844"

FLDAS_GLOBAL_MONTHLY_SOURCE = "fldas_global_monthly"
FLDAS_GLOBAL_MONTHLY_PATCH_FILLED_SOURCE = "fldas_global_monthly_patch_filled"
FLDAS_GLOBAL_MONTHLY_SHORTNAME = "FLDAS_NOAH01_C_GL_M"
FLDAS_GLOBAL_MONTHLY_PRODUCT = "FLDAS_NOAH01_C_GL_M.001"
PAN_INDIA_BBOX = "6,68,38,98"
DEFAULT_MAX_WORKERS = 3


@dataclass(frozen=True)
class SourceManifest:
    source: str
    shortname: str
    product: str
    variable: str
    bbox: str
    temporal_resolution: str
    output_folder: str
    file_count: int
    downloaded_count: int
    skipped_count: int
    failed_count: int
    patch_filled_source: str | None = None
    patch_filled_folder: str | None = None
    patch_filled_count: int = 0
    patch_fill_skipped_count: int = 0
    patch_fill_failed_count: int = 0
    patch_fill_invalid_pixels: int = 0


def _coerce_date(value, field_name):
    if isinstance(value, dt.date):
        return value
    if value is None or str(value).strip().lower() in {"", "none", "null"}:
        raise ValueError(f"{field_name} is required")
    try:
        return dt.datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be in YYYY-MM-DD format") from exc


def _iter_days(start_date: dt.date, end_date: dt.date) -> Iterable[dt.date]:
    current = start_date
    while current < end_date:
        yield current
        current += dt.timedelta(days=1)


def _iter_month_starts(start_date: dt.date, end_date: dt.date) -> Iterable[dt.date]:
    current = start_date.replace(day=1)
    while current < end_date:
        yield current
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def _download_url(params):
    return f"{GESDISC_OTF_URL}?{urlencode(params)}"


def _daily_ca_params(day: dt.date, bbox: str):
    stamp = day.strftime("%Y%m%d")
    return {
        "FILENAME": (
            f"/data/FLDAS/{FLDAS_CA_DAILY_PRODUCT}/"
            f"{day.year}/{day.month:02d}/"
            f"{FLDAS_CA_DAILY_SHORTNAME}.A{stamp}.001.nc"
        ),
        "SERVICE": "L34RS_LDAS",
        "BBOX": bbox,
        "FORMAT": FLDAS_FORMAT,
        "VERSION": "1.02",
        "SHORTNAME": FLDAS_CA_DAILY_SHORTNAME,
        "LABEL": f"{FLDAS_CA_DAILY_SHORTNAME}.A{stamp}.001.nc.SUB.tif",
        "DATASET_VERSION": "001",
        "VARIABLES": EVAP_VARIABLE,
    }


def _monthly_global_params(month_start: dt.date, bbox: str):
    stamp = month_start.strftime("%Y%m")
    return {
        "FILENAME": (
            f"/data/FLDAS/{FLDAS_GLOBAL_MONTHLY_PRODUCT}/"
            f"{month_start.year}/"
            f"{FLDAS_GLOBAL_MONTHLY_SHORTNAME}.A{stamp}.001.nc"
        ),
        "VARIABLES": EVAP_VARIABLE,
        "FORMAT": FLDAS_FORMAT,
        "LABEL": f"{FLDAS_GLOBAL_MONTHLY_SHORTNAME}.A{stamp}.001.nc.SUB.tif",
        "SERVICE": "L34RS_LDAS",
        "DATASET_VERSION": "001",
        "VERSION": "1.02",
        "SHORTNAME": FLDAS_GLOBAL_MONTHLY_SHORTNAME,
        "BBOX": bbox,
    }


def _gesdisc_auth():
    username = getattr(settings, "USERNAME_GESDISC", None)
    password = getattr(settings, "PASSWORD_GESDISC", None)
    if not username or not password:
        raise ValueError("USERNAME_GESDISC and PASSWORD_GESDISC are required")
    return username, password


def _raise_if_html_response(response, first_chunk: bytes, url: str):
    content_type = response.headers.get("Content-Type", "").lower()
    stripped = first_chunk.lstrip().lower()
    if "text/html" not in content_type and not stripped.startswith((b"<html", b"<!doctype")):
        return

    snippet = first_chunk[:500].decode("utf-8", errors="replace")
    raise RuntimeError(
        "GES DISC returned an HTML response instead of a raster. "
        f"Check Earthdata credentials and data access approval. URL={url}. "
        f"Response starts with: {snippet}"
    )


def _download_file(session, url: str, output_path: Path, overwrite: bool, logger):
    if output_path.exists() and output_path.stat().st_size > 0 and not overwrite:
        logger.info("Skipping existing ET raster: %s", output_path)
        return "skipped"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    logger.info("Downloading ET raster: %s", output_path)

    try:
        with session.get(url, stream=True, timeout=(30, 300)) as response:
            if response.status_code != 200:
                body = response.content[:500].decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"GES DISC download failed with HTTP {response.status_code}. "
                    f"URL={url}. Response starts with: {body}"
                )

            wrote_any = False
            with tmp_path.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    if not wrote_any:
                        _raise_if_html_response(response, chunk, url)
                        wrote_any = True
                    handle.write(chunk)

            if not wrote_any or tmp_path.stat().st_size == 0:
                raise RuntimeError(f"GES DISC returned an empty raster for URL={url}")

        tmp_path.replace(output_path)
        return "downloaded"
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _download_record(record, auth, overwrite, logger, max_attempts, retry_delay_seconds):
    session = requests.Session()
    session.auth = auth
    try:
        for attempt in range(1, max_attempts + 1):
            record["attempts"] = attempt
            try:
                status = _download_file(
                    session=session,
                    url=record["url"],
                    output_path=Path(record["path"]),
                    overwrite=overwrite,
                    logger=logger,
                )
                record["status"] = status
                record.pop("error", None)
                return status
            except Exception as exc:
                record["error"] = str(exc)
                logger.warning(
                    "ET raster download failed for %s on attempt %s/%s: %s",
                    record["path"],
                    attempt,
                    max_attempts,
                    exc,
                )
                if attempt < max_attempts:
                    time.sleep(retry_delay_seconds * attempt)

        record["status"] = "failed"
        logger.error(
            "Skipping ET raster after %s failed attempts: %s",
            max_attempts,
            record["path"],
        )
        return "failed"
    finally:
        session.close()


def _download_records(auth, records, overwrite, logger, max_attempts, retry_delay_seconds, max_workers):
    downloaded_count = 0
    skipped_count = 0
    failed_count = 0
    if not records:
        return downloaded_count, skipped_count, failed_count

    worker_count = min(max_workers, len(records))
    logger.info("Downloading %s ET rasters with %s workers", len(records), worker_count)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(
                _download_record,
                record,
                auth,
                overwrite,
                logger,
                max_attempts,
                retry_delay_seconds,
            )
            for record in records
        ]
        for future in as_completed(futures):
            status = future.result()
            if status == "downloaded":
                downloaded_count += 1
            elif status == "skipped":
                skipped_count += 1
            elif status == "failed":
                failed_count += 1

    return downloaded_count, skipped_count, failed_count


def _invalid_pixel_mask(values: np.ndarray, nodata) -> np.ndarray:
    invalid = ~np.isfinite(values)
    if nodata is not None:
        invalid |= values == nodata
    invalid |= values < 0
    return invalid


def _fill_raster_band(
    values: np.ndarray,
    nodata,
    *,
    max_search_distance: float,
    smoothing_iterations: int,
):
    invalid = _invalid_pixel_mask(values, nodata)
    invalid_count = int(invalid.sum())
    if invalid_count == 0:
        return values, invalid_count

    valid = ~invalid
    if not valid.any():
        raise ValueError("raster band has no valid pixels to fill from")

    working = values.astype("float32", copy=True)
    working[invalid] = 0.0
    mask = valid.astype("uint8")
    filled = fillnodata(
        working,
        mask=mask,
        max_search_distance=max_search_distance,
        smoothing_iterations=smoothing_iterations,
    )
    return filled, invalid_count


def _patch_fill_raster(
    input_path: Path,
    output_path: Path,
    *,
    overwrite: bool,
    max_search_distance: float | None,
    smoothing_iterations: int,
):
    if output_path.exists() and output_path.stat().st_size > 0 and not overwrite:
        return "skipped", 0, None

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(
        f"{output_path.stem}.{int(time.time() * 1000)}.tmp{output_path.suffix}"
    )
    try:
        with rasterio.open(input_path) as src:
            profile = src.profile.copy()
            nodata = src.nodata
            data = src.read()
            search_distance = (
                float(max(src.width, src.height))
                if max_search_distance is None
                else max_search_distance
            )

        filled = np.empty((data.shape[0], data.shape[1], data.shape[2]), dtype="float32")
        invalid_pixels = 0
        for band_index in range(data.shape[0]):
            filled_band, band_invalid = _fill_raster_band(
                data[band_index].astype("float32", copy=False),
                nodata,
                max_search_distance=search_distance,
                smoothing_iterations=smoothing_iterations,
            )
            filled[band_index] = filled_band
            invalid_pixels += band_invalid

        profile.update(
            dtype="float32",
            count=data.shape[0],
            compress=profile.get("compress") or "deflate",
            tiled=profile.get("tiled", True),
        )
        with rasterio.open(temporary_path, "w", **profile) as dst:
            dst.write(filled)
        temporary_path.replace(output_path)
        return "patch_filled", invalid_pixels, search_distance
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def _patch_fill_record(
    record,
    output_root,
    overwrite,
    max_search_distance,
    smoothing_iterations,
):
    input_path = Path(record["path"])
    output_path = Path(output_root) / input_path.name
    patch_record = {
        key: value for key, value in record.items() if key in {"date", "month", "url"}
    }
    patch_record.update(
        {
            "source_path": str(input_path),
            "path": str(output_path),
        }
    )

    if not input_path.exists():
        patch_record["status"] = "failed"
        patch_record["error"] = f"source raster not found: {input_path}"
        return "failed", patch_record

    try:
        status, invalid_pixels, search_distance = _patch_fill_raster(
            input_path,
            output_path,
            overwrite=overwrite,
            max_search_distance=max_search_distance,
            smoothing_iterations=smoothing_iterations,
        )
        patch_record["status"] = status
        patch_record["invalid_pixels_filled"] = invalid_pixels
        patch_record["max_search_distance"] = search_distance
        patch_record.pop("error", None)
        return status, patch_record
    except Exception as exc:
        patch_record["status"] = "failed"
        patch_record["error"] = str(exc)
        return "failed", patch_record


def _patch_fill_records(
    source_name,
    records,
    output_root,
    *,
    overwrite,
    logger,
    max_workers,
    max_search_distance=None,
    smoothing_iterations=0,
):
    patched_count = 0
    skipped_count = 0
    failed_count = 0
    invalid_pixels = 0
    patch_records = []
    if not records:
        return patch_records, patched_count, skipped_count, failed_count, invalid_pixels

    output_root = Path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    worker_count = min(max_workers, len(records))
    logger.info(
        "Patch-filling %s ET rasters into %s with %s workers",
        source_name,
        output_root,
        worker_count,
    )
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(
                _patch_fill_record,
                record,
                output_root,
                overwrite,
                max_search_distance,
                smoothing_iterations,
            )
            for record in records
        ]
        for future in as_completed(futures):
            status, patch_record = future.result()
            patch_records.append(patch_record)
            if status == "patch_filled":
                patched_count += 1
                invalid_pixels += int(patch_record.get("invalid_pixels_filled") or 0)
            elif status == "skipped":
                skipped_count += 1
            elif status == "failed":
                failed_count += 1
                logger.error(
                    "Patch-fill failed for %s: %s",
                    patch_record.get("source_path"),
                    patch_record.get("error"),
                )

    patch_records.sort(key=lambda item: item.get("date") or item.get("month") or "")
    return patch_records, patched_count, skipped_count, failed_count, invalid_pixels


def download_pan_india_et_assets(
    output_root,
    start_date,
    end_date,
    *,
    et_root=None,
    overwrite=False,
    fldas_ca_daily_bbox=FLDAS_CA_DAILY_BBOX,
    fldas_global_monthly_bbox=PAN_INDIA_BBOX,
    max_attempts=5,
    retry_delay_seconds=5,
    max_workers=DEFAULT_MAX_WORKERS,
    patch_fill=True,
    patch_fill_max_search_distance=None,
    patch_fill_smoothing_iterations=0,
    logger=None,
):
    """
    Download source ET rasters used by the GEE hydrology flow.

    The Central Asia FLDAS daily product is stored for the northern/high-resolution
    branch. The global monthly FLDAS product is stored for the pan-India fallback
    branch currently named GLDAS in utilities/constants.py.
    """
    logger = logger or logging.getLogger(__name__)
    start_date = _coerce_date(start_date, "start_date")
    end_date = _coerce_date(end_date, "end_date")
    if end_date <= start_date:
        raise ValueError("end_date must be after start_date")
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if patch_fill_smoothing_iterations < 0:
        raise ValueError("patch_fill_smoothing_iterations must be >= 0")
    if (
        patch_fill_max_search_distance is not None
        and patch_fill_max_search_distance < 0
    ):
        raise ValueError("patch_fill_max_search_distance must be >= 0")

    output_root = Path(output_root)
    et_root = Path(et_root) if et_root is not None else output_root / "et"
    daily_root = et_root / FLDAS_CA_DAILY_SOURCE / "daily"
    monthly_root = et_root / FLDAS_GLOBAL_MONTHLY_SOURCE / "monthly"
    daily_patch_filled_root = et_root / FLDAS_CA_DAILY_PATCH_FILLED_SOURCE
    monthly_patch_filled_root = et_root / FLDAS_GLOBAL_MONTHLY_PATCH_FILLED_SOURCE

    daily_records = []
    for day in _iter_days(start_date, end_date):
        params = _daily_ca_params(day, fldas_ca_daily_bbox)
        daily_records.append(
            {
                "date": day.isoformat(),
                "path": str(daily_root / f"{day:%Y%m%d}.tif"),
                "url": _download_url(params),
            }
        )

    monthly_records = []
    for month_start in _iter_month_starts(start_date, end_date):
        params = _monthly_global_params(month_start, fldas_global_monthly_bbox)
        monthly_records.append(
            {
                "month": month_start.strftime("%Y-%m"),
                "path": str(monthly_root / f"{month_start:%Y%m}.tif"),
                "url": _download_url(params),
            }
        )

    auth = _gesdisc_auth()

    logger.info(
        "Downloading local ET assets for [%s, %s): %s daily CA rasters, %s global monthly rasters",
        start_date,
        end_date,
        len(daily_records),
        len(monthly_records),
    )
    daily_downloaded, daily_skipped, daily_failed = _download_records(
        auth,
        daily_records,
        overwrite,
        logger,
        max_attempts,
        retry_delay_seconds,
        max_workers,
    )
    monthly_downloaded, monthly_skipped, monthly_failed = _download_records(
        auth,
        monthly_records,
        overwrite,
        logger,
        max_attempts,
        retry_delay_seconds,
        max_workers,
    )

    daily_patch_records = []
    monthly_patch_records = []
    daily_patch_count = daily_patch_skipped = daily_patch_failed = 0
    monthly_patch_count = monthly_patch_skipped = monthly_patch_failed = 0
    daily_patch_invalid_pixels = monthly_patch_invalid_pixels = 0
    if patch_fill:
        (
            daily_patch_records,
            daily_patch_count,
            daily_patch_skipped,
            daily_patch_failed,
            daily_patch_invalid_pixels,
        ) = _patch_fill_records(
            FLDAS_CA_DAILY_SOURCE,
            daily_records,
            daily_patch_filled_root,
            overwrite=overwrite,
            logger=logger,
            max_workers=max_workers,
            max_search_distance=patch_fill_max_search_distance,
            smoothing_iterations=patch_fill_smoothing_iterations,
        )
        (
            monthly_patch_records,
            monthly_patch_count,
            monthly_patch_skipped,
            monthly_patch_failed,
            monthly_patch_invalid_pixels,
        ) = _patch_fill_records(
            FLDAS_GLOBAL_MONTHLY_SOURCE,
            monthly_records,
            monthly_patch_filled_root,
            overwrite=overwrite,
            logger=logger,
            max_workers=max_workers,
            max_search_distance=patch_fill_max_search_distance,
            smoothing_iterations=patch_fill_smoothing_iterations,
        )

    sources = [
        SourceManifest(
            source=FLDAS_CA_DAILY_SOURCE,
            shortname=FLDAS_CA_DAILY_SHORTNAME,
            product=FLDAS_CA_DAILY_PRODUCT,
            variable=EVAP_VARIABLE,
            bbox=fldas_ca_daily_bbox,
            temporal_resolution="daily",
            output_folder=str(daily_root),
            file_count=len(daily_records),
            downloaded_count=daily_downloaded,
            skipped_count=daily_skipped,
            failed_count=daily_failed,
            patch_filled_source=(
                FLDAS_CA_DAILY_PATCH_FILLED_SOURCE if patch_fill else None
            ),
            patch_filled_folder=(
                str(daily_patch_filled_root) if patch_fill else None
            ),
            patch_filled_count=daily_patch_count,
            patch_fill_skipped_count=daily_patch_skipped,
            patch_fill_failed_count=daily_patch_failed,
            patch_fill_invalid_pixels=daily_patch_invalid_pixels,
        ),
        SourceManifest(
            source=FLDAS_GLOBAL_MONTHLY_SOURCE,
            shortname=FLDAS_GLOBAL_MONTHLY_SHORTNAME,
            product=FLDAS_GLOBAL_MONTHLY_PRODUCT,
            variable=EVAP_VARIABLE,
            bbox=fldas_global_monthly_bbox,
            temporal_resolution="monthly",
            output_folder=str(monthly_root),
            file_count=len(monthly_records),
            downloaded_count=monthly_downloaded,
            skipped_count=monthly_skipped,
            failed_count=monthly_failed,
            patch_filled_source=(
                FLDAS_GLOBAL_MONTHLY_PATCH_FILLED_SOURCE if patch_fill else None
            ),
            patch_filled_folder=(
                str(monthly_patch_filled_root) if patch_fill else None
            ),
            patch_filled_count=monthly_patch_count,
            patch_fill_skipped_count=monthly_patch_skipped,
            patch_fill_failed_count=monthly_patch_failed,
            patch_fill_invalid_pixels=monthly_patch_invalid_pixels,
        ),
    ]

    manifest = {
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "output_root": str(output_root),
        "et_root": str(et_root),
        "date_range": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "end_date_is_exclusive": True,
        },
        "purpose": (
            "Local source rasters for replacing GEE evapotranspiration inputs in "
            "computing/mws/generate_hydrology.py."
        ),
        "retry_policy": {
            "max_attempts": max_attempts,
            "retry_delay_seconds": retry_delay_seconds,
            "failed_records_are_skipped": True,
        },
        "download_policy": {
            "max_workers": max_workers,
        },
        "patch_fill_policy": {
            "enabled": bool(patch_fill),
            "method": "rasterio.fill.fillnodata",
            "invalid_pixels": ["nodata", "nan", "inf", "negative"],
            "max_search_distance": (
                "max(width, height) per raster"
                if patch_fill_max_search_distance is None
                else patch_fill_max_search_distance
            ),
            "smoothing_iterations": patch_fill_smoothing_iterations,
        },
        "sources": [asdict(source) for source in sources],
        "records": {
            FLDAS_CA_DAILY_SOURCE: daily_records,
            FLDAS_GLOBAL_MONTHLY_SOURCE: monthly_records,
            FLDAS_CA_DAILY_PATCH_FILLED_SOURCE: daily_patch_records,
            FLDAS_GLOBAL_MONTHLY_PATCH_FILLED_SOURCE: monthly_patch_records,
        },
    }
    _write_json(et_root / "manifest.json", manifest)
    _write_json(
        et_root / FLDAS_CA_DAILY_SOURCE / "metadata.json",
        {
            "source": asdict(sources[0]),
            "records": daily_records,
        },
    )
    _write_json(
        et_root / FLDAS_GLOBAL_MONTHLY_SOURCE / "metadata.json",
        {
            "source": asdict(sources[1]),
            "records": monthly_records,
        },
    )
    return manifest
