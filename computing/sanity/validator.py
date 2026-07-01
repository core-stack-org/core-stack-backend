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
    ) -> None:
        """Initialize a validator from a YAML file or an in-memory config."""
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

    def build_rules(self) -> List[Any]:
        """Instantiate concrete rule objects from config entries."""
        rules = []

        for rule_cfg in self.config["rules"]:
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
        """Run every configured rule against the provided dataframe-like object."""

        results = []

        for rule in self.build_rules():
            logger.debug("Validating rule '%s'", rule.name)
            results.append(rule.validate(gdf))

        logger.info(
            "Completed %d validation rules for layer '%s'",
            len(results),
            self.config.get("layer_name"),
        )
        return results
