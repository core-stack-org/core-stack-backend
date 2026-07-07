from abc import ABC, abstractmethod
import fnmatch
import logging
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
from pandas.api.types import (
    is_integer_dtype,
    is_float_dtype,
    is_numeric_dtype,
    is_string_dtype,
    is_bool_dtype,
)


logger = logging.getLogger(__name__)


class Rule(ABC):
    def __init__(
        self,
        name: str,
        field: Optional[str] = None,
        field_pattern: Optional[str] = None,
        value: Any = None,
        expected_type: Optional[str] = None,
        severity: str = "ERROR",
        fields: Optional[List[str]] = None,
        exclude_field_patterns: Optional[List[str]] = None,
        allow_missing_fields: bool = False,
    ) -> None:
        """
        Store common rule configuration used by all concrete rules.

        Args:
            name: Human-readable rule name used in reports.
            field: Exact field name to validate.
            field_pattern: Wildcard field pattern to validate.
            value: Rule-specific comparison value or allowed-values list.
            expected_type: Expected data type for data_type rules.
            severity: Severity label reserved for future report filtering.
            fields: Explicit list of fields to validate.
            exclude_field_patterns: Wildcard patterns to remove from matches.
            allow_missing_fields: Whether no matching fields should pass.

        Returns:
            None. The normalized rule configuration is stored on self.
        """
        self.name = name
        self.field = field
        self.field_pattern = field_pattern
        self.value = value
        self.expected_type = expected_type
        self.severity = severity
        self.fields = fields or []
        self.exclude_field_patterns = exclude_field_patterns or []
        self.allow_missing_fields = allow_missing_fields

    def get_fields(self, gdf: pd.DataFrame) -> List[str]:
        """
        Resolve configured field names or wildcard patterns against columns.

        Args:
            gdf: DataFrame or GeoDataFrame containing the layer data.

        Returns:
            List of field names this rule should validate.
        """
        if self.field:
            return [self.field]

        if self.fields:
            if self.allow_missing_fields:
                return [field for field in self.fields if field in gdf.columns]
            return self.fields

        if self.field_pattern:
            fields = [
                col
                for col in gdf.columns
                if fnmatch.fnmatchcase(col.lower(), self.field_pattern.lower())
            ]
            return [
                col
                for col in fields
                if not any(
                    fnmatch.fnmatchcase(col.lower(), pattern.lower())
                    for pattern in self.exclude_field_patterns
                )
            ]

        return []

    def get_uid_column(self, gdf: pd.DataFrame) -> Optional[str]:
        """
        Find the identifier column used to report invalid rows.

        Args:
            gdf: DataFrame or GeoDataFrame containing the layer data.

        Returns:
            Name of the first known ID column, or None when no ID column exists.
        """
        for uid_column in ("UID", "uid", "MWS UID", "mws_id", "vill_id", "village_id"):
            if uid_column in gdf.columns:
                return uid_column
        return None

    def get_uids(self, gdf: pd.DataFrame) -> List[Any]:
        """
        Return row identifiers for failed records when an ID column exists.

        Args:
            gdf: Filtered DataFrame containing invalid rows.

        Returns:
            List of invalid row identifiers. Returns an empty list when no known
            ID column is present.
        """
        uid_column = self.get_uid_column(gdf)
        if uid_column is None:
            return []
        return gdf[uid_column].tolist()

    def no_fields_result(self) -> Dict[str, Any]:
        """
        Build a consistent result when a rule matches no columns.

        Args:
            None. Uses the rule's configured field, fields, or field_pattern.

        Returns:
            Rule result dictionary. The result passes only when
            allow_missing_fields is true.
        """
        label = self.field or self.field_pattern or ", ".join(self.fields)
        if self.allow_missing_fields:
            logger.debug(
                "Rule '%s' matched no fields, but missing fields are allowed",
                self.name,
            )
            return {
                "rule": self.name,
                "results": [
                    {
                        "field": label,
                        "passed": True,
                        "invalid_count": 0,
                        "uids": [],
                    }
                ],
            }

        logger.warning("Rule '%s' matched no fields for '%s'", self.name, label)
        return {
            "rule": self.name,
            "results": [
                {
                    "field": label,
                    "passed": False,
                    "invalid_count": 1,
                    "uids": [],
                    "message": f"No fields matched '{label}'",
                }
            ],
        }

    @abstractmethod
    def validate(self, gdf: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate the input frame and return rule-level results.

        Args:
            gdf: DataFrame or GeoDataFrame containing layer rows.

        Returns:
            Rule result dictionary with field-level pass/fail details.
        """
        pass


class DataTypeRule(Rule):
    TYPE_CHECKS: Dict[str, Callable[[pd.Series], bool]] = {
        "integer": is_integer_dtype,
        "float": is_float_dtype,
        "numeric": is_numeric_dtype,
        "string": is_string_dtype,
        "boolean": is_bool_dtype,
    }

    def validate(self, gdf: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that each matched field has the configured pandas dtype.

        Args:
            gdf: DataFrame or GeoDataFrame containing layer rows.

        Returns:
            Rule result dictionary with actual and expected type details.
        """
        fields = self.get_fields(gdf)

        if not fields:
            return self.no_fields_result()

        if self.expected_type is None:
            logger.error("Missing expected_type in data type rule '%s'", self.name)
            raise ValueError(f"Rule '{self.name}' requires expected_type")

        expected = self.expected_type.lower()

        if expected not in self.TYPE_CHECKS:
            logger.error(
                "Unsupported expected type '%s' in rule '%s'", expected, self.name
            )
            raise ValueError(f"Unsupported type '{expected}'")

        checker = self.TYPE_CHECKS[expected]
        results = []

        for field in fields:
            if field not in gdf.columns:
                passed = False
                actual_type = "missing"
            else:
                passed = checker(gdf[field])
                actual_type = str(gdf[field].dtype)

            results.append(
                {
                    "field": field,
                    "passed": passed,
                    "invalid_count": 0 if passed else len(gdf),
                    "uids": [] if passed else self.get_uids(gdf),
                    "expected_type": expected,
                    "actual_type": actual_type,
                }
            )

        failed_count = sum(1 for result in results if not result["passed"])
        logger.info(
            "Data type rule '%s' checked %d fields; %d failed",
            self.name,
            len(results),
            failed_count,
        )
        return {
            "rule": self.name,
            "results": results,
        }


class MaxValueRule(Rule):
    def validate(self, gdf: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that matched numeric fields do not exceed the configured maximum.

        Args:
            gdf: DataFrame or GeoDataFrame containing layer rows.

        Returns:
            Rule result dictionary with invalid counts and row identifiers.
        """
        results = []
        fields = self.get_fields(gdf)

        if not fields:
            return self.no_fields_result()

        for field in fields:
            if field not in gdf.columns:
                results.append(
                    {
                        "field": field,
                        "passed": False,
                        "invalid_count": 1,
                        "uids": [],
                        "message": "Field is missing",
                    }
                )
                continue

            # Excel imports may read numeric-looking cells as objects.
            values = pd.to_numeric(gdf[field], errors="coerce")
            invalid = gdf[values.notna() & (values > self.value)]
            results.append(
                {
                    "field": field,
                    "passed": len(invalid) == 0,
                    "invalid_count": len(invalid),
                    "uids": self.get_uids(invalid),
                }
            )

        invalid_count = sum(result["invalid_count"] for result in results)
        logger.info(
            "Max value rule '%s' checked %d fields; %d invalid rows",
            self.name,
            len(results),
            invalid_count,
        )
        return {
            "rule": self.name,
            "results": results,
        }


class MinValueRule(Rule):
    def validate(self, gdf: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that matched numeric fields are not below the configured minimum.

        Args:
            gdf: DataFrame or GeoDataFrame containing layer rows.

        Returns:
            Rule result dictionary with invalid counts and row identifiers.
        """
        results = []
        fields = self.get_fields(gdf)

        if not fields:
            return self.no_fields_result()

        for field in fields:
            if field not in gdf.columns:
                results.append(
                    {
                        "field": field,
                        "passed": False,
                        "invalid_count": 1,
                        "uids": [],
                        "message": "Field is missing",
                    }
                )
                continue

            # Excel imports may read numeric-looking cells as objects.
            values = pd.to_numeric(gdf[field], errors="coerce")
            invalid = gdf[values.notna() & (values < self.value)]
            results.append(
                {
                    "field": field,
                    "passed": len(invalid) == 0,
                    "invalid_count": len(invalid),
                    "uids": self.get_uids(invalid),
                }
            )

        invalid_count = sum(result["invalid_count"] for result in results)
        logger.info(
            "Min value rule '%s' checked %d fields; %d invalid rows",
            self.name,
            len(results),
            invalid_count,
        )
        return {
            "rule": self.name,
            "results": results,
        }


class NotNullRule(Rule):
    def validate(self, gdf: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that matched fields do not contain null values.

        Args:
            gdf: DataFrame or GeoDataFrame containing layer rows.

        Returns:
            Rule result dictionary with null counts and row identifiers.
        """
        results = []
        fields = self.get_fields(gdf)

        if not fields:
            return self.no_fields_result()

        for field in fields:
            if field not in gdf.columns:
                results.append(
                    {
                        "field": field,
                        "passed": False,
                        "invalid_count": 1,
                        "uids": [],
                        "message": "Field is missing",
                    }
                )
                continue

            invalid = gdf[gdf[field].isna()]
            results.append(
                {
                    "field": field,
                    "passed": len(invalid) == 0,
                    "invalid_count": len(invalid),
                    "uids": self.get_uids(invalid),
                }
            )

        invalid_count = sum(result["invalid_count"] for result in results)
        logger.info(
            "Not-null rule '%s' checked %d fields; %d null rows",
            self.name,
            len(results),
            invalid_count,
        )
        return {
            "rule": self.name,
            "results": results,
        }


class AllowedValuesRule(Rule):
    def validate(self, gdf: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that matched fields only contain configured allowed values.

        Args:
            gdf: DataFrame or GeoDataFrame containing layer rows.

        Returns:
            Rule result dictionary with disallowed-value counts and row IDs.
        """
        results = []
        fields = self.get_fields(gdf)

        if not fields:
            return self.no_fields_result()

        for field in fields:
            if field not in gdf.columns:
                results.append(
                    {
                        "field": field,
                        "passed": False,
                        "invalid_count": 1,
                        "uids": [],
                        "message": "Field is missing",
                    }
                )
                continue

            invalid = gdf[gdf[field].notna() & ~gdf[field].isin(self.value)]
            results.append(
                {
                    "field": field,
                    "passed": len(invalid) == 0,
                    "invalid_count": len(invalid),
                    "uids": self.get_uids(invalid),
                }
            )

        invalid_count = sum(result["invalid_count"] for result in results)
        logger.info(
            "Allowed-values rule '%s' checked %d fields; %d invalid rows",
            self.name,
            len(results),
            invalid_count,
        )
        return {
            "rule": self.name,
            "results": results,
        }


class RequiredFieldsRule(Rule):
    def validate(self, gdf: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that all configured field names are present.

        Args:
            gdf: DataFrame or GeoDataFrame containing layer rows.

        Returns:
            Rule result dictionary containing one failure per missing field.
        """
        required_fields = self.value or self.fields
        missing = [field for field in required_fields if field not in gdf.columns]
        if missing:
            logger.warning(
                "Required-fields rule '%s' missing fields: %s", self.name, missing
            )
        else:
            logger.info("Required-fields rule '%s' passed", self.name)

        return {
            "rule": self.name,
            "results": [
                {
                    "field": field,
                    "passed": False,
                    "invalid_count": 1,
                    "uids": [],
                    "message": "Required field is missing",
                }
                for field in missing
            ],
        }


class RequiredAnyFieldPatternRule(Rule):
    def validate(self, gdf: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that at least one configured field pattern is present.

        Args:
            gdf: DataFrame or GeoDataFrame containing layer rows.

        Returns:
            Rule result dictionary. It fails when none of the configured wildcard
            patterns match any input column.
        """
        patterns = self.value or []
        matched_fields = [
            col
            for col in gdf.columns
            if any(
                fnmatch.fnmatchcase(col.lower(), pattern.lower())
                for pattern in patterns
            )
        ]
        logger.info(
            "Required-any-pattern rule '%s' matched %d fields",
            self.name,
            len(matched_fields),
        )

        return {
            "rule": self.name,
            "results": [
                {
                    "field": ", ".join(patterns),
                    "passed": len(matched_fields) > 0,
                    "invalid_count": 0 if matched_fields else 1,
                    "uids": [],
                    "message": None
                    if matched_fields
                    else "No required field pattern matched",
                }
            ],
        }


RULE_MAP = {
    "max_value": MaxValueRule,
    "min_value": MinValueRule,
    "not_null": NotNullRule,
    "allowed_values": AllowedValuesRule,
    "data_type": DataTypeRule,
    "required_fields": RequiredFieldsRule,
    "required_any_field_pattern": RequiredAnyFieldPatternRule,
}
