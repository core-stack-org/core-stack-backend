from abc import ABC, abstractmethod
import fnmatch
import pandas as pd
from pandas.api.types import (
    is_integer_dtype,
    is_float_dtype,
    is_numeric_dtype,
    is_string_dtype,
    is_bool_dtype,
)


class Rule(ABC):
    def __init__(
            self,
            name,
            field=None,
            field_pattern=None,
            value=None,
            expected_type=None,
            severity="ERROR",
    ):
        self.name = name
        self.field = field
        self.field_pattern = field_pattern
        self.value = value
        self.expected_type = expected_type
        self.severity = severity

    def get_fields(self, gdf):

        if self.field:
            return [self.field]

        if self.field_pattern:
            return [
                col for col in gdf.columns if fnmatch.fnmatch(col, self.field_pattern)
            ]

        return []

    @abstractmethod
    def validate(self, gdf):
        pass


class DataTypeRule(Rule):
    TYPE_CHECKS = {
        "integer": is_integer_dtype,
        "float": is_float_dtype,
        "numeric": is_numeric_dtype,
        "string": is_string_dtype,
        "boolean": is_bool_dtype,
    }

    def validate(self, gdf):

        fields = self.get_fields(gdf)

        if not fields:
            raise ValueError(f"No fields matched pattern '{self.field_pattern}'")

        expected = self.expected_type.lower()

        if expected not in self.TYPE_CHECKS:
            raise ValueError(f"Unsupported type '{expected}'")

        checker = self.TYPE_CHECKS[expected]

        results = []

        for field in fields:
            passed = checker(gdf[field])

            results.append(
                {
                    "field": field,
                    "passed": passed,
                    "expected_type": expected,
                    "actual_type": str(gdf[field].dtype),
                }
            )

        return {
            "rule": self.name,
            "results": results,
        }


class MaxValueRule(Rule):
    def validate(self, gdf):
        results = []

        fields = self.get_fields(gdf)

        if not fields:
            raise ValueError(f"No fields matched pattern '{self.field_pattern}'")

        for field in fields:
            invalid = gdf[gdf[field] > self.value]
            uid_column = "UID"
            results.append(
                {
                    "field": field,
                    "passed": len(invalid) == 0,
                    "invalid_count": len(invalid),
                    "uids": invalid[uid_column].tolist(),
                }
            )

        return {
            "rule": self.name,
            "results": results,
        }


class MinValueRule(Rule):
    def validate(self, gdf):
        results = []

        fields = self.get_fields(gdf)

        if not fields:
            raise ValueError(f"No fields matched pattern '{self.field_pattern}'")

        for field in fields:
            invalid = gdf[gdf[field] < self.value]
            uid_column = "UID"

            results.append(
                {
                    "field": field,
                    "passed": len(invalid) == 0,
                    "invalid_count": len(invalid),
                    "uids": invalid[uid_column].tolist(),
                }
            )

        return {
            "rule": self.name,
            "results": results,
        }


class NotNullRule(Rule):
    def validate(self, gdf):
        results = []

        fields = self.get_fields(gdf)

        if not fields:
            raise ValueError(f"No fields matched pattern '{self.field_pattern}'")

        for field in fields:
            invalid = gdf[gdf[field].isna()]
            uid_column = "UID"

            results.append(
                {
                    "field": field,
                    "passed": len(invalid) == 0,
                    "invalid_count": len(invalid),
                    "uids": invalid[uid_column].tolist(),
                }
            )

        return {
            "rule": self.name,
            "results": results,
        }


class AllowedValuesRule(Rule):
    def validate(self, gdf):
        results = []

        fields = self.get_fields(gdf)

        if not fields:
            raise ValueError(f"No fields matched pattern '{self.field_pattern}'")

        for field in fields:
            invalid = gdf[gdf[field].isin(self.value)]
            uid_column = "UID"

            results.append(
                {
                    "field": field,
                    "passed": len(invalid) == 0,
                    "invalid_count": len(invalid),
                    "uids": invalid[uid_column].tolist(),
                }
            )

        return {
            "rule": self.name,
            "results": results,
        }


RULE_MAP = {
    "max_value": MaxValueRule,
    "min_value": MinValueRule,
    "not_null": NotNullRule,
    "allowed_values": AllowedValuesRule,
    "data_type": DataTypeRule,
}
