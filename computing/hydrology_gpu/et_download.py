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


GESDISC_OTF_URL = "https://hydro1.gesdisc.eosdis.nasa.gov/daac-bin/OTF/HTTP_services.cgi"
EVAP_VARIABLE = "Evap_tavg"
FLDAS_FORMAT = "Y29nLw"

FLDAS_CA_DAILY_SHORTNAME = "FLDAS_NOAHMP001_G_CA_D"
FLDAS_CA_DAILY_PRODUCT = "FLDAS_NOAHMP001_G_CA_D.001"
FLDAS_CA_DAILY_BBOX = "21,65.566,37.932,99.844"

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


def download_pan_india_et_assets(
    output_root,
    start_date,
    end_date,
    *,
    overwrite=False,
    fldas_ca_daily_bbox=FLDAS_CA_DAILY_BBOX,
    fldas_global_monthly_bbox=PAN_INDIA_BBOX,
    max_attempts=5,
    retry_delay_seconds=5,
    max_workers=DEFAULT_MAX_WORKERS,
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

    output_root = Path(output_root)
    et_root = output_root / "et"
    daily_root = et_root / "fldas_ca_daily" / "daily"
    monthly_root = et_root / "fldas_global_monthly" / "monthly"

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

    sources = [
        SourceManifest(
            source="fldas_ca_daily",
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
        ),
        SourceManifest(
            source="fldas_global_monthly",
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
        "sources": [asdict(source) for source in sources],
        "records": {
            "fldas_ca_daily": daily_records,
            "fldas_global_monthly": monthly_records,
        },
    }
    _write_json(et_root / "manifest.json", manifest)
    _write_json(
        et_root / "fldas_ca_daily" / "metadata.json",
        {
            "source": asdict(sources[0]),
            "records": daily_records,
        },
    )
    _write_json(
        et_root / "fldas_global_monthly" / "metadata.json",
        {
            "source": asdict(sources[1]),
            "records": monthly_records,
        },
    )

    return manifest
