import logging
from typing import Any, Dict, List, Optional

import yaml

try:
    from .rules import RULE_MAP
except ImportError:
    from rules import RULE_MAP


logger = logging.getLogger(__name__)


class VectorValidator:
    def __init__(
        self,
        config_file: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        validation_context: Optional[str] = None,
    ) -> None:
        """
        Initialize a validator from a YAML file or an in-memory config.

        Args:
            config_file: Path to a YAML config file. Used when config is not passed.
            config: Already-loaded YAML config as a dictionary.
            validation_context: Optional execution context. Use "excel" when
                validating stats-generator workbook sheets and "vector" when
                validating geospatial vector layers.

        Returns:
            None. The parsed config and validation context are stored on self.
        """
        self.validation_context = (
            validation_context.lower() if validation_context is not None else None
        )

        if config is not None:
            self.config = config
            logger.debug(
                "Initialized validator from provided config for layer '%s'",
                self.config.get("layer_name"),
            )
            return

        if config_file is None:
            logger.error("VectorValidator initialized without config_file or config")
            raise ValueError("Either config_file or config must be provided")

        logger.debug("Loading validator config from '%s'", config_file)
        with open(config_file) as f:
            self.config = yaml.safe_load(f)

    def _rule_applies_to_context(self, rule_cfg: Dict[str, Any]) -> bool:
        """
        Decide whether a configured rule should run for the current context.

        Args:
            rule_cfg: One rule entry from the YAML config.

        Returns:
            True when the rule should be built and executed. False when it is
            scoped to another context and should be skipped.
        """
        if self.validation_context is None:
            return True

        applies_to = rule_cfg.get("applies_to")
        if applies_to is not None:
            if isinstance(applies_to, str):
                applies_to = [applies_to]
            return self.validation_context in {str(item).lower() for item in applies_to}

        # Backward-compatible fallback for existing mixed configs.
        rule_name = rule_cfg.get("name", "").lower()
        if "_excel_" in rule_name:
            return self.validation_context == "excel"
        if "_vector_" in rule_name:
            return self.validation_context == "vector"

        return True

    def build_rules(self) -> List[Any]:
        """
        Instantiate concrete rule objects from config entries.

        Args:
            None. Uses self.config and self.validation_context.

        Returns:
            A list of rule instances that apply to the active validation context.
        """
        rules = []

        for rule_cfg in self.config["rules"]:
            if not self._rule_applies_to_context(rule_cfg):
                logger.debug(
                    "Skipping rule '%s' for context '%s'",
                    rule_cfg["name"],
                    self.validation_context,
                )
                continue

            rule_class = RULE_MAP[rule_cfg["type"]]
            logger.debug(
                "Building rule '%s' of type '%s'",
                rule_cfg["name"],
                rule_cfg["type"],
            )

            rules.append(
                rule_class(
                    name=rule_cfg["name"],
                    field=rule_cfg.get("field"),
                    field_pattern=rule_cfg.get("field_pattern"),
                    value=rule_cfg.get("value"),
                    expected_type=rule_cfg.get("expected_type"),
                    severity=rule_cfg.get("severity", "ERROR"),
                    fields=rule_cfg.get("fields"),
                    exclude_field_patterns=rule_cfg.get("exclude_field_patterns"),
                    allow_missing_fields=rule_cfg.get("allow_missing_fields", False),
                )
            )

        logger.info(
            "Built %d rules for layer '%s'",
            len(rules),
            self.config.get("layer_name"),
        )
        return rules

    def validate(self, gdf: Any) -> List[Dict[str, Any]]:
        """
        Run every context-applicable rule against the provided dataframe.

        Args:
            gdf: A pandas/geopandas dataframe-like object containing layer rows.

        Returns:
            A list of rule result dictionaries. Each dictionary contains the rule
            name and field-level pass/fail details.
        """

        rules = self.build_rules()
        if not rules:
            logger.warning(
                "No rules apply to layer '%s' for context '%s'",
                self.config.get("layer_name"),
                self.validation_context,
            )
            return [
                {
                    "rule": "context_rules_present",
                    "results": [
                        {
                            "field": self.validation_context or "all",
                            "passed": False,
                            "invalid_count": 1,
                            "uids": [],
                            "message": "No validation rules apply to this context",
                        }
                    ],
                }
            ]

        results = []
        for rule in rules:
            logger.debug("Validating rule '%s'", rule.name)
            results.append(rule.validate(gdf))

        logger.info(
            "Completed %d validation rules for layer '%s'",
            len(results),
            self.config.get("layer_name"),
        )
        return results
