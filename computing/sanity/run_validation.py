from pathlib import Path
import logging
from typing import Any, Dict, Iterable, Optional, Tuple, Union

import pandas as pd
import yaml

try:
    from .validator import VectorValidator
    from .report import summarize
except ImportError:
    from validator import VectorValidator
    from report import summarize


logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parent / "configs"
ID_COLUMNS = ("UID", "uid", "MWS UID", "mws_id", "vill_id", "village_id")


def _config_path(rule_name: str) -> Path:
    """Return the YAML config path for a rule, layer, or configured sheet name."""
    direct_path = CONFIG_DIR / f"{rule_name}.yaml"
    if direct_path.exists():
        return direct_path

    normalized_rule_name = rule_name.lower()
    for config_path in CONFIG_DIR.glob("*.yaml"):
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}

        aliases = [config_path.stem, config.get("layer_name")]
        aliases.extend(config.get("sheet_names", []))
        if normalized_rule_name in {
            str(alias).lower() for alias in aliases if alias
        }:
            logger.debug(
                "Resolved rule '%s' to config '%s'", rule_name, config_path.name
            )
            return config_path

    raise FileNotFoundError(f"No validation config found for '{rule_name}'")


def _load_config(rule_name: str) -> Dict[str, Any]:
    """Load a validation rule config from disk."""
    logger.debug("Loading validation config for rule '%s'", rule_name)
    with open(_config_path(rule_name)) as f:
        return yaml.safe_load(f)


def _summarize_frame(df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
    """Run configured rules on a DataFrame and return the compact summary."""
    validator = VectorValidator(config=config)
    logger.debug(
        "Running validation for layer '%s' with %d rows and %d columns",
        config.get("layer_name"),
        len(df),
        len(df.columns),
    )
    report = summarize(validator.validate(df))
    logger.info(
        "Validation finished for layer '%s' with status '%s'",
        config.get("layer_name"),
        report["status"],
    )
    return report


def _resolve_sheet_name(
    xls: pd.ExcelFile,
    rule_name: str,
    config: Dict[str, Any],
    sheet_name: Optional[str] = None,
) -> str:
    """Resolve the worksheet to validate using explicit input and config aliases."""
    if sheet_name:
        candidates = [sheet_name]
    else:
        candidates = []
        candidates.extend(config.get("sheet_names", []))
        if config.get("sheet_name"):
            candidates.append(config["sheet_name"])
        candidates.append(rule_name)

    sheet_lookup = {sheet.lower(): sheet for sheet in xls.sheet_names}
    for candidate in candidates:
        match = sheet_lookup.get(candidate.lower())
        if match:
            logger.debug(
                "Resolved rule '%s' to workbook sheet '%s'", rule_name, match
            )
            return match

    logger.error(
        "Unable to resolve sheet for rule '%s'. Tried %s", rule_name, candidates
    )
    raise ValueError(
        f"Sheet for '{rule_name}' not found. "
        f"Tried: {candidates}. Available sheets: {xls.sheet_names}"
    )


def _generic_excel_config(sheet_name: str, df: pd.DataFrame) -> Dict[str, Any]:
    """Build basic ID and numeric sanity rules for sheets without domain configs."""
    rules = []
    id_column = next((column for column in ID_COLUMNS if column in df.columns), None)
    if id_column:
        rules.append(
            {
                "name": f"{sheet_name}_id_not_null",
                "type": "not_null",
                "field": id_column,
            }
        )

    numeric_fields = [
        column
        for column in df.columns
        if pd.api.types.is_numeric_dtype(df[column])
    ]
    if numeric_fields:
        rules.extend(
            [
                {
                    "name": f"{sheet_name}_numeric_type",
                    "type": "data_type",
                    "fields": numeric_fields,
                    "expected_type": "numeric",
                },
                {
                    "name": f"{sheet_name}_numeric_not_null",
                    "type": "not_null",
                    "fields": numeric_fields,
                },
            ]
        )

    logger.debug(
        "Built generic Excel config for sheet '%s' with %d rules",
        sheet_name,
        len(rules),
    )
    return {"layer_name": sheet_name, "rules": rules}


def run_layer_validation(
    layer_path: Union[str, Path],
    rule_name: str,
) -> Dict[str, Any]:
    """
    Validate a vector layer (GeoJSON/Shapefile/etc.)
    """
    import geopandas as gpd

    logger.info("Reading vector layer '%s' for rule '%s'", layer_path, rule_name)
    gdf = gpd.read_file(layer_path)

    report = _summarize_frame(gdf, _load_config(rule_name))
    print(report)
    return report


# run_layer_validation("data/cropping_intensity_bhavnagar.geojson", "cropping_intensity")


def run_excel_validation(
    excel_path: Union[str, Path],
    rule_name: str,
    sheet_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Validate data from an Excel workbook.

    Looks for a configured sheet name first, then a sheet whose name matches
    rule_name (case-insensitive).
    """

    logger.info("Opening Excel workbook '%s' for rule '%s'", excel_path, rule_name)
    xls = pd.ExcelFile(excel_path)
    config = _load_config(rule_name)
    resolved_sheet_name = _resolve_sheet_name(xls, rule_name, config, sheet_name)

    logger.info("Reading sheet '%s' from workbook '%s'", resolved_sheet_name, excel_path)
    df = pd.read_excel(excel_path, sheet_name=resolved_sheet_name)

    return _summarize_frame(df, config)


def run_excel_workbook_validation(
    excel_path: Union[str, Path],
    rule_names: Optional[Iterable[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Validate all sheets in a stats-generator workbook.

    Sheets with explicit configs use those domain rules. Other sheets get a
    conservative generic sanity pass for ID/null/numeric-type checks.
    """
    logger.info("Opening Excel workbook '%s' for workbook validation", excel_path)
    xls = pd.ExcelFile(excel_path)
    rule_names = rule_names or [
        path.stem for path in CONFIG_DIR.glob("*.yaml")
    ]

    configured_sheets: Dict[str, Tuple[str, Dict[str, Any]]] = {}
    for rule_name in rule_names:
        config = _load_config(rule_name)
        try:
            sheet_name = _resolve_sheet_name(xls, rule_name, config)
        except ValueError:
            logger.warning(
                "Skipping rule '%s' because no matching workbook sheet was found",
                rule_name,
            )
            continue
        configured_sheets[sheet_name] = (rule_name, config)

    workbook_report: Dict[str, Dict[str, Any]] = {}
    for sheet_name in xls.sheet_names:
        logger.info("Validating workbook sheet '%s'", sheet_name)
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        rule_name, config = configured_sheets.get(
            sheet_name,
            (sheet_name, _generic_excel_config(sheet_name, df)),
        )
        workbook_report[sheet_name] = {
            "rule_name": rule_name,
            "report": _summarize_frame(df, config),
        }

    logger.info("Workbook validation finished for '%s'", excel_path)
    return workbook_report
