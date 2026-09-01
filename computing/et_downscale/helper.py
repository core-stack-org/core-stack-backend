import ee
import calendar
import time


MONTH_ABBR = [
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
]

DEFAULT_CROP_YEAR_START_MONTH = 7
MONTHLY_INTERPOLATION_WINDOW_DAYS = 45
MODIS_COL = "MODIS/061/MOD16A2GF"
MCD12Q1_COL = "MODIS/061/MCD12Q1"

# NoData sentinel used in GEE images and written to masked pixels in assets.
NODATA = -9999.0
EXPORT_BAND_NAMES = [f"b{i}" for i in range(1, 14)]


def crop_year_start_month(cfg: dict | None = None) -> int:
    value = DEFAULT_CROP_YEAR_START_MONTH
    if cfg is not None:
        value = int(cfg.get("crop_year_start_month", DEFAULT_CROP_YEAR_START_MONTH))
    if value != DEFAULT_CROP_YEAR_START_MONTH:
        raise ValueError("Core ET products use a fixed July-June crop year")
    return value


def crop_month_index(
    calendar_month: int,
    start_month: int = DEFAULT_CROP_YEAR_START_MONTH,
) -> int:
    """Return the 1-based crop-year band index for a calendar month."""
    calendar_month = int(calendar_month)
    start_month = int(start_month)
    return ((calendar_month - start_month) % 12) + 1


def crop_month_abbrs(start_month: int = DEFAULT_CROP_YEAR_START_MONTH) -> list[str]:
    return [
        calendar.month_abbr[((int(start_month) - 1 + idx) % 12) + 1]
        for idx in range(12)
    ]


def crop_year_start_date(
    year: int, start_month: int = DEFAULT_CROP_YEAR_START_MONTH
) -> ee.Date:
    return ee.Date.fromYMD(int(year), int(start_month), 1)


def _asset_token(value: str) -> str:
    cleaned = []
    for char in str(value).strip().lower():
        cleaned.append(char if char.isalnum() else "_")
    token = "".join(cleaned).strip("_")
    while "__" in token:
        token = token.replace("__", "_")
    return token or "unknown"


def _build_asset_id(cfg: dict, label: str) -> str:
    root = str(cfg.get("asset_root", "")).rstrip("/")
    if not root:
        raise ValueError("asset_root is required")
    asset_suffix = _asset_token(cfg.get("asset_suffix"))
    year = int(cfg["year"])
    return f"{root}/{label}_{asset_suffix}_{year}"


def product_asset_id(cfg: dict, label: str, year=None) -> str:
    asset_cfg = dict(cfg)
    if year is not None:
        asset_cfg["year"] = year
    return _build_asset_id(asset_cfg, label)


def _asset_exists(asset_id: str) -> bool:
    try:
        ee.data.getAsset(asset_id)
        return True
    except Exception:
        return False


def asset_exists(asset_id: str) -> bool:
    return _asset_exists(asset_id)


def ee_annual_total_band(
    monthly_stack: ee.Image,
    prefix: str,
    year: int,
    band_name: str = "annual",
    start_month=7,
) -> ee.Image:
    annual = ee.Image.constant(0).float()
    valid_count = ee.Image.constant(0).float()

    for agri_month_idx in range(12):
        month_band = monthly_stack.select(f"{prefix}_{agri_month_idx + 1:02d}")
        actual_month = ((start_month - 1 + agri_month_idx) % 12) + 1
        actual_year = year if actual_month >= start_month else year + 1
        days = calendar.monthrange(actual_year, actual_month)[1]
        annual = annual.add(month_band.unmask(0).multiply(days))
        valid_count = valid_count.add(month_band.mask().gt(0).unmask(0))

    return annual.updateMask(valid_count.eq(12)).rename(band_name).float()


def ee_annual_mean_band(
    monthly_stack: ee.Image, prefix: str, band_name: str = "annual"
) -> ee.Image:
    images = [
        monthly_stack.select(f"{prefix}_{month:02d}").rename("annual_src").float()
        for month in range(1, 13)
    ]
    collection = ee.ImageCollection.fromImages(images)
    valid_count = collection.map(
        lambda img: ee.Image(img).mask().gt(0).unmask(0).rename("annual_src")
    ).sum()
    return collection.mean().updateMask(valid_count.eq(12)).rename(band_name).float()


def _apply_image_properties(img: ee.Image, props: dict) -> ee.Image:
    out = img
    for key, value in props.items():
        out = out.set(key, value)
    return out


def finalize_export_image(
    monthly_stack: ee.Image,
    annual_band: ee.Image,
    region: ee.Geometry,
    metadata: dict,
    band_descriptions: list,
    default_proj: ee.Projection = None,
) -> ee.Image:
    image = monthly_stack.addBands(annual_band).rename(EXPORT_BAND_NAMES)
    if default_proj is not None:
        image = image.setDefaultProjection(default_proj)
    image = image.clip(region)
    image = image.unmask(NODATA).float()
    props = {"nodata": NODATA}
    props.update(metadata)
    for idx, desc in enumerate(band_descriptions, start=1):
        props[f"band_{idx}_description"] = desc
    return _apply_image_properties(image, props)


def mask_nodata(img: ee.Image, nodata: float = NODATA) -> ee.Image:
    """Mask the numeric NoData sentinel written into exported assets."""
    img = ee.Image(img).float()
    return img.updateMask(img.neq(nodata))


def load_exported_monthly_stack(
    asset_id: str,
    output_prefix: str,
    nodata: float = NODATA,
) -> ee.Image:
    """Load b1..b12 from an exported asset as PREFIX_01..PREFIX_12."""
    image = ee.Image(asset_id)
    bands = []
    for month in range(1, 13):
        band = mask_nodata(image.select(f"b{month}"), nodata)
        bands.append(band.rename(f"{output_prefix}_{month:02d}").float())
    return ee.Image.cat(bands).setDefaultProjection(image.select("b1").projection())


def load_product_monthly_stack(
    cfg: dict,
    label: str,
    output_prefix: str,
    year=None,
    nodata: float = NODATA,
) -> ee.Image:
    """Load b1..b12 from one of this pipeline's exported product assets."""
    return load_exported_monthly_stack(
        product_asset_id(cfg, label, year=year),
        output_prefix,
        nodata=nodata,
    )


def divide_where_valid(numerator: ee.Image, denominator: ee.Image) -> ee.Image:
    """Divide only where both inputs exist and the denominator is greater than 0."""
    numerator = mask_nodata(numerator)
    denominator = mask_nodata(denominator)
    valid = (
        numerator.mask()
        .gt(0)
        .multiply(denominator.mask().gt(0))
        .multiply(denominator.gt(0))
        .gt(0)
    )
    return numerator.divide(denominator).updateMask(valid).float()


def _start_asset_export(image: ee.Image, asset_id: str, description: str):
    export_kwargs = {
        "image": image,
        "description": description,
        "assetId": asset_id,
        "scale": 30,
        "maxPixels": 1e13,
    }
    task = ee.batch.Export.image.toAsset(**export_kwargs)
    task.start()
    print(f"  Export task started -> {asset_id}")
    return task


def _start_asset_drive_export(
    image: ee.Image, asset_id: str, description: str, aez: str
):
    export_kwargs = {
        "image": image,
        "description": description,
        "folder": f"ET_downscale_{aez}",
        "fileFormat": "GeoTIFF",
        "scale": 30,
        "crs": "EPSG:4326",
        "maxPixels": 1e13,
    }
    task = ee.batch.Export.image.toDrive(**export_kwargs)
    task.start()
    print(f"  Export task started -> {asset_id}")
    return task


def wait_for_tasks(
    task_specs: list, poll_seconds: int = 30, fail_on_error: bool = False
) -> dict:
    if not task_specs:
        return {}
    poll_seconds = max(5, int(poll_seconds))
    pending = {spec["asset_id"]: spec for spec in task_specs}
    final_statuses = {}
    print(f"\n[exports] Waiting for {len(task_specs)} Earth Engine task(s) ...")
    while pending:
        finished_now = []
        for asset_id, spec in pending.items():
            if not spec["task"]:
                finished_now.append(asset_id)
                final_statuses[asset_id] = "No Task"
                continue
            status = spec["task"].status()
            state = status.get("state", "UNKNOWN")
            if state in {"COMPLETED", "FAILED", "CANCELLED", "CANCEL_REQUESTED"}:
                finished_now.append(asset_id)
                final_statuses[asset_id] = status
                print(f"  [{state}] {spec['label']} -> {asset_id}")
                if status.get("error_message"):
                    print(f"    Error: {status['error_message']}")
        for asset_id in finished_now:
            pending.pop(asset_id, None)
        if pending:
            print(
                f"  Still running: {len(pending)} task(s). Checking again in {poll_seconds}s ..."
            )
            time.sleep(poll_seconds)
    if fail_on_error:
        failed = []
        for spec in task_specs:
            if not spec["task"]:
                continue
            asset_id = spec["asset_id"]
            status = final_statuses.get(asset_id, {})
            state = status.get("state", "UNKNOWN")
            if state != "COMPLETED":
                message = status.get(
                    "error_message", "No error message from Earth Engine."
                )
                failed.append(f"{spec['label']} ({asset_id}) -> {state}: {message}")
        if failed:
            raise RuntimeError(
                "One or more Earth Engine export tasks did not complete successfully:\n"
                + "\n".join(failed)
            )
    return final_statuses


def _prepare_asset_target(asset_id: str, overwrite: bool) -> bool:
    if not _asset_exists(asset_id):
        return False
    if not overwrite:
        print(f"GEE asset already exists: {asset_id}\n")
        return True
    print(f"  Overwriting existing asset -> {asset_id}")
    ee.data.deleteAsset(asset_id)
    return False


def export_product_asset(
    label: str, display_name: str, image: ee.Image, cfg: dict
) -> dict:
    asset_id = _build_asset_id(cfg, label)
    asset_exists = _prepare_asset_target(
        asset_id, bool(cfg.get("overwrite_assets", False))
    )
    print(f"  {display_name} asset -> {asset_id}")
    task = None
    if not asset_exists:
        if cfg.get("aez", False):
            _start_asset_drive_export(
                image,
                asset_id,
                description=f"export_{label}_{_asset_token(cfg['asset_suffix'])}_{cfg['year']}",
                aez=cfg["aez"],
            )
        else:
            task = _start_asset_export(
                image,
                asset_id,
                description=f"export_{label}_{_asset_token(cfg['asset_suffix'])}_{cfg['year']}",
            )
    return {"asset_id": asset_id, "task": task, "label": label}


def build_classifier(model_path: str) -> ee.Classifier:
    trees = (
        ee.FeatureCollection(model_path)
        .aggregate_array("tree")
        .map(lambda s: ee.String(s).replace("#.*", "", "g").trim())
    )
    return ee.Classifier.decisionTreeEnsemble(trees)


def cast_monthly_band(img: ee.Image, value_band: str) -> ee.Image:
    """Force a one-band monthly image to a generic float schema."""
    return (
        ee.Image(img)
        .select([value_band])
        .cast({value_band: ee.PixelType.float()}, [value_band])
    )


def empty_monthly_band(value_band: str, proj: ee.Projection = None) -> ee.Image:
    """Return a fully masked one-band image that preserves monthly schema."""
    img = ee.Image.constant(0).rename(value_band).updateMask(ee.Image.constant(0))
    img = cast_monthly_band(img, value_band)
    if proj is not None:
        img = img.setDefaultProjection(proj)
    return img


def ensure_monthly_band(
    img: ee.Image, value_band: str, proj: ee.Projection = None
) -> ee.Image:
    """Keep value_band if present; otherwise replace with a masked placeholder."""
    img = ee.Image(img)
    empty = empty_monthly_band(value_band, proj)
    out = ee.Image(
        ee.Algorithms.If(
            img.bandNames().contains(value_band),
            cast_monthly_band(img.select(value_band).rename(value_band), value_band),
            empty,
        )
    )
    if proj is not None:
        out = out.setDefaultProjection(proj)
    return out


def fill_monthly_collection(
    raw_monthly: ee.ImageCollection,
    value_band: str,
    proj: ee.Projection = None,
) -> ee.ImageCollection:
    """Fill masked monthly pixels from +/-45-day neighbours."""

    def normalize(img):
        img = ee.Image(img)
        out = ensure_monthly_band(img, value_band, proj)
        return (
            out.set("month", img.get("month"))
            .set("calendar_month", img.get("calendar_month"))
            .set("system:time_start", img.get("system:time_start"))
            .set("source_count", img.get("source_count"))
            .set("is_placeholder", img.get("is_placeholder"))
        )

    safe_monthly = raw_monthly.map(normalize)

    def interpolate(img):
        img = ee.Image(img)
        time_start = img.get("system:time_start")
        start_window = (
            ee.Date(time_start)
            .advance(-MONTHLY_INTERPOLATION_WINDOW_DAYS, "day")
            .millis()
        )
        end_window = (
            ee.Date(time_start)
            .advance(MONTHLY_INTERPOLATION_WINDOW_DAYS, "day")
            .millis()
        )
        window_neighbours = (
            safe_monthly.select(value_band)
            .filter(ee.Filter.gte("system:time_start", start_window))
            .filter(ee.Filter.lt("system:time_start", end_window))
        )
        neighbours = window_neighbours
        filled = neighbours.mean()
        out = img.select(value_band).unmask(filled)
        return (
            cast_monthly_band(out.rename(value_band), value_band)
            .set("month", img.get("month"))
            .set("calendar_month", img.get("calendar_month"))
            .set("system:time_start", time_start)
            .set("source_count", img.get("source_count"))
            .set("is_placeholder", img.get("is_placeholder"))
        )

    return safe_monthly.map(interpolate)


def monthly_collection_to_stack(
    monthly_col: ee.ImageCollection,
    value_band: str,
    output_prefix: str,
    region: ee.Geometry,
) -> ee.Image:
    """Convert a monthly image collection into a named 12-band stack."""

    def rename_month(img):
        month_str = ee.String(ee.Number(img.get("month")).format("%02d"))
        return (
            img.select(value_band)
            .rename(ee.String(output_prefix).cat(month_str))
            .float()
        )

    named = monthly_col.filter(ee.Filter.gte("month", 1)).filter(ee.Filter.lte("month", 12)).sort("month").map(rename_month)
    stack = named.toBands().clip(region)
    current_names = stack.bandNames()
    new_names = current_names.map(lambda n: ee.String(n).split("_").slice(1).join("_"))
    return stack.rename(new_names)


def interpolate_monthly_stack(
    monthly_stack: ee.Image,
    prefix: str,
    region: ee.Geometry,
    year: int,
    proj: ee.Projection = None,
    start_month: int = DEFAULT_CROP_YEAR_START_MONTH,
) -> ee.Image:
    """Fill masked pixels in a PREFIX_01...PREFIX_12 stack month by month."""
    if proj is None:
        proj = monthly_stack.select(f"{prefix}_01").projection()

    value_band = f"{prefix}_value"
    start_date = crop_year_start_date(year, start_month)
    monthly_images = []
    for agri_month_idx in range(12):
        agri_month = agri_month_idx + 1
        date = start_date.advance(agri_month_idx, "month")
        monthly_images.append(
            cast_monthly_band(
                monthly_stack.select(f"{prefix}_{agri_month:02d}").rename(value_band),
                value_band,
            )
            .set("month", agri_month)
            .set("calendar_month", date.get("month"))
            .set("system:time_start", date.advance(15, "day").millis())
            .set("source_count", 1)
            .set("is_placeholder", False)
        )

    raw_monthly = ee.ImageCollection.fromImages(monthly_images)
    filled_monthly = fill_monthly_collection(raw_monthly, value_band, proj=proj)
    return monthly_collection_to_stack(filled_monthly, value_band, f"{prefix}_", region)


def get_proj_30m(
    region: ee.Geometry,
    year: int,
    start_month: int = DEFAULT_CROP_YEAR_START_MONTH,
) -> ee.Projection:
    """Return a stable 30 m grid, even when the crop-year has no Landsat scene."""
    start = crop_year_start_date(year, start_month)
    ls_col = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
        .filterBounds(region)
        .filterDate(start, start.advance(12, "month"))
    )
    fallback = (
        ee.Image.constant(0)
        .rename("SR_B5")
        .setDefaultProjection(ee.Projection("EPSG:4326").atScale(30))
    )
    ls_ref = ee.Image(
        ee.Algorithms.If(ls_col.size().gt(0), ls_col.first().select("SR_B5"), fallback)
    )
    return ls_ref.select("SR_B5").projection()
