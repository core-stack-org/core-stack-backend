"""
Download drought inputs locally from Google Earth Engine.

This script downloads pan-India or single-state GeoTIFFs locally for:
    - UCSB-CHG/CHIRPS/DAILY precipitation
    - MODIS/061/MOD16A2GF PET

It downloads one small GeoTIFF per time step for each dataset.

Examples:
    main(aoi="india", datasets=["both"])
    main(aoi="india", datasets=["chirps", "modis_pet"], start_date="2004-01-01", end_date="2023-12-31")
    main(aoi="state", state="Madhya Pradesh", datasets=["chirps"])
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import ee
import requests

from utilities.gee_utils import ee_initialize

CHIRPS_COLLECTION = "UCSB-CHG/CHIRPS/DAILY"
MODIS_PET_COLLECTION = "MODIS/061/MOD16A2GF"
DEFAULT_PROJECT = "ee-corestackdev"
DATASET_CHOICES = ("chirps", "modis_pet", "both")


def initialize_earth_engine(project: str) -> None:
    # Uses the repo's shared Earth Engine initialization helper.
    # If auth/project errors happen, debug utilities.gee_utils.ee_initialize first.
    ee_initialize()
    print("Earth Engine initialized.")


def get_aoi(aoi_type: str, state_name: str) -> ee.FeatureCollection:
    # GAUL level1 contains Indian state boundaries. For pan-India, keep all
    # Indian state features and use their combined geometry downstream.
    # admin = ee.FeatureCollection("FAO/GAUL/2015/level1").filter(
    #     ee.Filter.eq("ADM0_NAME", "India")
    # )
    admin = ee.FeatureCollection(
        "projects/ext-datasets/assets/datasets/State_pan_india"
    )

    if aoi_type == "india":
        admin = admin.filter(ee.Filter.neq("Name", "Andaman & Nicobar")).filter(
            ee.Filter.neq("Name", "Lakshadweep")
        )
        return admin.union()
    return admin.filter(ee.Filter.eq("Name", state_name))


def date_range(
    start_date: ee.Date, end_date_exclusive: ee.Date, unit: str
) -> list[ee.Date]:
    # Builds monthly/daily date anchors. end_date_exclusive is intentionally
    # advanced by one day in build_dataset_image so user input is inclusive.
    count = end_date_exclusive.difference(start_date, unit).round().getInfo()
    return [start_date.advance(offset, unit) for offset in range(count)]


def collection_dates(
    collection: ee.ImageCollection, start_date: ee.Date, end_date_exclusive: ee.Date
) -> list[ee.Date]:
    # Native cadence should use the source image timestamps. This is important
    # for MODIS PET, which is 8-day rather than daily.
    millis = (
        collection.filterDate(start_date, end_date_exclusive)
        .aggregate_array("system:time_start")
        .getInfo()
    )
    return [ee.Date(value) for value in millis]


def expand_datasets(selected: list[str]) -> list[str]:
    print("Inside expand_datasets")
    if isinstance(selected, str):
        selected = [selected]
    if "both" in selected:
        return ["chirps", "modis_pet"]
    return selected


def make_chirps_image(
    chirps: ee.ImageCollection,
    aoi: ee.FeatureCollection,
    start_date: ee.Date,
    frequency: str,
) -> tuple[ee.Image, str]:
    # CHIRPS is daily precipitation. Monthly mode sums daily precipitation
    # into one monthly image before local download.
    if frequency in ("daily", "native"):
        end_date = start_date.advance(1, "day")
        label = start_date.format("YYYYMMdd").getInfo()
        image = chirps.filterDate(start_date, end_date).first()
    else:
        end_date = start_date.advance(1, "month")
        label = start_date.format("YYYYMM").getInfo()
        image = chirps.filterDate(start_date, end_date).sum()

    image = (
        ee.Image(image)
        .select("precipitation")
        .rename("precipitation")
        .clip(aoi)
        .toFloat()
    )
    return image, label


def make_modis_pet_image(
    modis_pet: ee.ImageCollection,
    aoi: ee.FeatureCollection,
    start_date: ee.Date,
    frequency: str,
    target_projection: ee.Projection | None = None,
) -> tuple[ee.Image, str]:
    # MOD16A2GF PET is not daily. The PET band has a 0.1 scale factor, applied
    # here so downloaded rasters contain real PET values.
    if frequency == "daily":
        raise ValueError("MODIS PET is not daily. Use --frequency monthly or native.")

    if frequency == "native":
        end_date = start_date.advance(8, "day")
        label = start_date.format("YYYYMMdd").getInfo()
        image = modis_pet.filterDate(start_date, end_date).first()
    else:
        end_date = start_date.advance(1, "month")
        label = start_date.format("YYYYMM").getInfo()
        image = modis_pet.filterDate(start_date, end_date).sum()
        image = image.setDefaultProjection(modis_pet.first().projection())

    image = ee.Image(image).multiply(0.1).rename("PET")

    if target_projection is not None:
        # Match the original GEE P-PET script:
        # PET.reduceResolution(mean).reproject(crs=P.projection())
        image = image.reduceResolution(
            reducer=ee.Reducer.mean(), maxPixels=65536
        ).reproject(crs=target_projection)

    image = image.clip(aoi).toFloat()
    return image, label


def download_image(
    image: ee.Image,
    aoi: ee.FeatureCollection,
    output_path: Path,
    scale: int,
    crs: str | None = "EPSG:4326",
    retries: int = 3,
) -> None:
    # Earth Engine's direct download endpoint has a request-size limit, so this
    # function downloads only one time step at a time.
    params = {
        "scale": scale,
        "region": aoi.geometry(),
        "format": "GEO_TIFF",
    }
    if crs:
        params["crs"] = crs

    for attempt in range(1, retries + 1):
        try:
            # Generate a signed EE URL, then stream the GeoTIFF bytes to disk.
            url = image.getDownloadURL(params)
            with requests.get(url, stream=True, timeout=300) as response:
                response.raise_for_status()
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with output_path.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            handle.write(chunk)
            return
        except Exception:
            # Remove partial files so retries/reruns do not treat corrupt files
            # as valid completed downloads.
            if output_path.exists():
                output_path.unlink()
            if attempt == retries:
                raise
            time.sleep(2 * attempt)


def validate_inputs(aoi: str, datasets: list[str] | str, frequency: str) -> None:
    # Keep validation near main() so notebook calls fail early and clearly.
    print("Inside validate_inputs")
    if aoi not in ("india", "state"):
        raise ValueError("aoi must be 'india' or 'state'.")
    if frequency not in ("daily", "monthly", "native"):
        raise ValueError("frequency must be 'daily', 'monthly', or 'native'.")

    selected = [datasets] if isinstance(datasets, str) else datasets
    invalid_datasets = set(selected) - set(DATASET_CHOICES)
    if invalid_datasets:
        raise ValueError(f"Invalid dataset(s): {sorted(invalid_datasets)}")


def dataset_label(dataset: str) -> str:
    # Output labels are used in file names and final raster band descriptions.
    if dataset == "chirps":
        return "CHIRPS"
    if dataset == "modis_pet":
        return "MODIS_PET"
    raise ValueError(f"Unknown dataset: {dataset}")


def build_dataset_image(
    aoi: str,
    state: str,
    dataset: str,
    start_date: str,
    end_date: str,
    frequency: str,
) -> tuple[ee.ImageCollection, ee.FeatureCollection, str, list[tuple[ee.Date, str]]]:
    # Prepares the collection, AOI, and date labels for exactly one dataset.
    # CHIRPS and MODIS PET stay separate from this point through final output.
    start_ee_date = ee.Date(start_date)
    end_date_exclusive = ee.Date(end_date).advance(1, "day")

    region = get_aoi(aoi, state)
    chirps = ee.ImageCollection(CHIRPS_COLLECTION).select("precipitation")
    modis_pet = ee.ImageCollection(MODIS_PET_COLLECTION).select("PET")

    aoi_label = "India" if aoi == "india" else state

    if dataset == "chirps":
        unit = "day" if frequency in ("daily", "native") else "month"
        dates = date_range(start_ee_date, end_date_exclusive, unit)
        collection = chirps
        name = dataset_label(dataset)
    else:
        if frequency == "daily":
            raise ValueError(
                "MODIS PET is not daily. Use --frequency monthly or native."
            )
        if frequency == "native":
            dates = collection_dates(modis_pet, start_ee_date, end_date_exclusive)
        else:
            dates = date_range(start_ee_date, end_date_exclusive, "month")
        collection = modis_pet
        name = dataset_label(dataset)

    labeled_dates = []
    for date in dates:
        # These labels become the downloaded GeoTIFF file names.
        date_format = "YYYYMMdd" if frequency in ("daily", "native") else "YYYYMM"
        label = date.format(date_format).getInfo()
        labeled_dates.append((date, label))

    print(
        f"Preparing {len(labeled_dates)} {frequency} {name} image(s) "
        f"for {aoi_label}"
    )
    return collection, region, name, labeled_dates


def download_dataset_images(
    aoi: str,
    state: str,
    dataset: str,
    start_date: str,
    end_date: str,
    frequency: str,
    output_dir: str,
    scale: int,
    crs: str | None,
    sleep: float,
    overwrite: bool,
    max_workers: int,
) -> None:
    # End-to-end local workflow for one dataset:
    # download one small GeoTIFF per time step and keep those files on disk.
    aoi_label = "India" if aoi == "india" else state
    safe_aoi = aoi_label.replace(" ", "_")
    name = dataset_label(dataset)
    dataset_output_dir = Path(output_dir) / safe_aoi / frequency / dataset

    collection, region, _, labeled_dates = build_dataset_image(
        aoi=aoi,
        state=state,
        dataset=dataset,
        start_date=start_date,
        end_date=end_date,
        frequency=frequency,
    )

    download_jobs = []
    target_projection = None
    if dataset == "modis_pet" and frequency == "monthly":
        # Use CHIRPS monthly precipitation projection as the output grid for PET.
        # This keeps local PET files aligned with CHIRPS and reproduces the
        # original GEE reduceResolution(mean)->reproject(P.projection()) step.
        first_date = labeled_dates[0][0]
        first_chirps, _ = make_chirps_image(
            ee.ImageCollection(CHIRPS_COLLECTION).select("precipitation"),
            region,
            first_date,
            frequency,
        )
        target_projection = first_chirps.projection()

    for index, (current_date, label) in enumerate(labeled_dates, start=1):
        # Keeping one time step per request avoids the EE direct-download size
        # error seen when trying to download a large multiband image directly.
        band_name = f"{name}_{label}"
        single_path = dataset_output_dir / f"{band_name}.tif"

        if single_path.exists() and not overwrite:
            print(f"[{index}/{len(labeled_dates)}] exists: {single_path.name}")
        else:
            download_jobs.append((index, current_date, band_name, single_path))

    if max_workers <= 1:
        for index, current_date, band_name, single_path in download_jobs:
            if dataset == "chirps":
                image, _ = make_chirps_image(collection, region, current_date, frequency)
            else:
                image, _ = make_modis_pet_image(
                    collection, region, current_date, frequency, target_projection
                )
            print(f"[{index}/{len(labeled_dates)}] downloading: {single_path.name}")
            download_image(image.rename(band_name), region, single_path, scale, crs)
            time.sleep(sleep)
    else:
        # Parallel downloads are the main speed-up. Keep max_workers modest
        # because Earth Engine may throttle too many simultaneous URL requests.
        def download_one(job: tuple[int, ee.Date, str, Path]) -> tuple[int, str]:
            index, current_date, band_name, single_path = job
            if dataset == "chirps":
                image, _ = make_chirps_image(collection, region, current_date, frequency)
            else:
                image, _ = make_modis_pet_image(
                    collection, region, current_date, frequency, target_projection
                )
            download_image(image.rename(band_name), region, single_path, scale, crs)
            if sleep:
                time.sleep(sleep)
            return index, single_path.name

        print(f"Downloading {len(download_jobs)} file(s) with {max_workers} workers")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(download_one, job) for job in download_jobs]
            for future in as_completed(futures):
                index, file_name = future.result()
                print(f"[{index}/{len(labeled_dates)}] downloaded: {file_name}")

    print(f"Downloaded {len(labeled_dates)} {name} image(s) to {dataset_output_dir}")


def main(
    aoi: str = "india",
    datasets: list[str] | str | None = None,
    start_date: str = "2004-01-01",
    end_date: str = "2023-12-31",
    frequency: str = "monthly",
    state: str = "Madhya Pradesh",
    project: str = DEFAULT_PROJECT,
    output_dir: str = "data/drought_inputs",
    sleep: float = 0.2,
    max_workers: int = 4,
    overwrite: bool = False,
) -> None:
    # Public entry point for notebooks/scripts. datasets=["both"] downloads
    # separate CHIRPS and MODIS PET time-step GeoTIFFs.
    selected_datasets = datasets or ["both"]
    validate_inputs(aoi, selected_datasets, frequency)
    initialize_earth_engine(project)

    expanded_datasets = expand_datasets(selected_datasets)
    for dataset in expanded_datasets:
        scale = 5500
        crs = "EPSG:4326"
        download_dataset_images(
            aoi=aoi,
            state=state,
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            output_dir=output_dir,
            scale=scale,
            crs=crs,
            sleep=sleep,
            overwrite=overwrite,
            max_workers=max_workers,
        )

    print("Done.")
