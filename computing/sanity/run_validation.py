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
    """
    Resolve a validation config path from a rule name, layer name, or sheet name.

    Args:
        rule_name: Config stem, config layer_name, or one of the configured
            Excel sheet_names.

    Returns:
        Path to the matching YAML config file.
    """
    direct_path = CONFIG_DIR / f"{rule_name}.yaml"
    if direct_path.exists():
        return direct_path

    # Configs can be invoked by Excel sheet name, so inspect aliases if the
    # direct filename lookup does not match.
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
    """
    Load a validation config from disk.

    Args:
        rule_name: Config stem, layer name, or sheet name.

    Returns:
        Parsed YAML config dictionary.
    """
    logger.debug("Loading validation config for rule '%s'", rule_name)
    with open(_config_path(rule_name)) as f:
        return yaml.safe_load(f)


def _summarize_frame(
    df: pd.DataFrame,
    config: Dict[str, Any],
    validation_context: str,
) -> Dict[str, Any]:
    """
    Run context-applicable configured rules on a DataFrame.

    Args:
        df: The pandas/geopandas dataframe to validate.
        config: Parsed YAML config dictionary.
        validation_context: Either "excel" or "vector"; controls rule filtering.

    Returns:
        Compact validation summary with status and error list.
    """
    validator = VectorValidator(
        config=config,
        validation_context=validation_context,
    )
    logger.debug(
        "Running %s validation for layer '%s' with %d rows and %d columns",
        validation_context,
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
    """
    Resolve which Excel worksheet should be read for a config.

    Args:
        xls: Open pandas ExcelFile object.
        rule_name: User-provided rule/config/sheet name.
        config: Parsed YAML config.
        sheet_name: Optional explicit sheet name override.

    Returns:
        The workbook sheet name with original workbook casing.
    """
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
    """
    Build fallback Excel-only sanity rules for sheets without a domain config.

    Args:
        sheet_name: Name of the workbook sheet being validated.
        df: The sheet data as a pandas DataFrame.

    Returns:
        An in-memory config that checks ID nulls and numeric-column sanity.
    """
    rules = []
    id_column = next((column for column in ID_COLUMNS if column in df.columns), None)
    if id_column:
        rules.append(
            {
                "name": f"{sheet_name}_id_not_null",
                "type": "not_null",
                "field": id_column,
                "applies_to": ["excel"],
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
                    "applies_to": ["excel"],
                },
                {
                    "name": f"{sheet_name}_numeric_not_null",
                    "type": "not_null",
                    "fields": numeric_fields,
                    "applies_to": ["excel"],
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
    Validate a vector layer (GeoJSON/Shapefile/etc.) with vector-only rules.

    Args:
        layer_path: Path to the local vector layer file.
        rule_name: Config stem, layer_name, or alias for the layer.

    Returns:
        Compact validation summary for the vector layer.
    """
    import geopandas as gpd

    logger.info("Reading vector layer '%s' for rule '%s'", layer_path, rule_name)
    gdf = gpd.read_file(layer_path)

    report = _summarize_frame(gdf, _load_config(rule_name), "vector")
    print(report)
    return report


# run_layer_validation("data/cropping_intensity_bhavnagar.geojson", "cropping_intensity")


def run_excel_validation(
    excel_path: Union[str, Path],
    rule_name: str,
    sheet_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Validate one sheet from an Excel workbook with Excel-only rules.

    Args:
        excel_path: Path to the workbook generated by stats_generator.
        rule_name: Config stem, layer_name, or sheet alias.
        sheet_name: Optional explicit sheet name override.

    Returns:
        Compact validation summary for the resolved workbook sheet.
    """

    logger.info("Opening Excel workbook '%s' for rule '%s'", excel_path, rule_name)
    xls = pd.ExcelFile(excel_path)
    config = _load_config(rule_name)
    resolved_sheet_name = _resolve_sheet_name(xls, rule_name, config, sheet_name)

    logger.info("Reading sheet '%s' from workbook '%s'", resolved_sheet_name, excel_path)
    df = pd.read_excel(excel_path, sheet_name=resolved_sheet_name)

    return _summarize_frame(df, config, "excel")


def run_excel_workbook_validation(
    excel_path: Union[str, Path],
    rule_names: Optional[Iterable[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Validate every sheet in a stats-generator workbook with Excel-only rules.

    Args:
        excel_path: Path to the workbook generated by stats_generator.
        rule_names: Optional iterable of config names to consider. When omitted,
            all YAML configs in CONFIG_DIR are considered.

    Returns:
        Dictionary keyed by sheet name. Each value contains the config/rule name
        used and the compact validation report for that sheet.
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
            "report": _summarize_frame(df, config, "excel"),
        }

    logger.info("Workbook validation finished for '%s'", excel_path)
    return workbook_report
