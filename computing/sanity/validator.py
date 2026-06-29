import yaml
from .rules import RULE_MAP
import re


class VectorValidator:
    def __init__(self, config_file):
        with open(config_file) as f:
            self.config = yaml.safe_load(f)

    def build_rules(self):
        rules = []

        for rule_cfg in self.config["rules"]:
            rule_class = RULE_MAP[rule_cfg["type"]]

            rules.append(
                rule_class(
                    name=rule_cfg["name"],
                    field=rule_cfg.get("field"),
                    field_pattern=rule_cfg.get("field_pattern"),
                    value=rule_cfg.get("value"),
                    expected_type=rule_cfg.get("expected_type"),
                    severity=rule_cfg.get("severity", "ERROR"),
                )
            )

        return rules

    def validate(self, gdf):

        results = []

        for rule in self.build_rules():
            results.append(rule.validate(gdf))

        return results
