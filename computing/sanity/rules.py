from abc import ABC, abstractmethod
import fnmatch
import logging
import re
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

# Columns used to report which rows failed, matched case-insensitively.
ID_COLUMNS = (
    "uid",
    "mws uid",
    "mws_id",
    "vill_id",
    "village_id",
    "asset_id",
    "pc11_village_id",
    "objectid",
)


def numeric_values(series: pd.Series, ignore_values: List[Any]) -> pd.Series:
    """
    Convert a column to numbers, treating ignored sentinel values as missing.

    Args:
        series: Column to convert. Excel imports may hold numbers as objects.
        ignore_values: Values to drop before comparing, e.g. a -9999 no-data
            sentinel.

    Returns:
        Numeric series with non-numeric and ignored values set to NaN.
    """
    values = pd.to_numeric(series, errors="coerce")
    if ignore_values:
        values = values.mask(values.isin(ignore_values))
    return values


def blank_mask(series: pd.Series) -> pd.Series:
    """
    Flag null values and strings that are empty or whitespace only.

    Args:
        series: Column to inspect.

    Returns:
        Boolean series, True where the value is missing or blank.
    """
    blank = series.isna()
    if series.dtype == object or pd.api.types.is_string_dtype(series):
        blank = blank | series.astype(str).str.strip().eq("")
    return blank


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
        **options: Any,
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
            **options: Rule-type specific settings from the YAML entry, e.g.
                ignore_values (values a rule skips, such as a -9999 no-data
                sentinel), allow_empty_strings, suffix_min.

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
        self.options = options
        self.ignore_values = list(options.get("ignore_values") or [])

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
        lookup = {str(col).lower(): col for col in gdf.columns}
        for uid_column in ID_COLUMNS:
            if uid_column in lookup:
                return lookup[uid_column]
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
            values = numeric_values(gdf[field], self.ignore_values)
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
            values = numeric_values(gdf[field], self.ignore_values)
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

            # Blank strings are missing values too, unless the config says
            # they are meaningful for this field.
            if self.options.get("allow_empty_strings"):
                invalid = gdf[gdf[field].isna()]
            else:
                invalid = gdf[blank_mask(gdf[field])]
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


class UniqueRule(Rule):
    def validate(self, gdf: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that the matched fields, taken together, identify one row.

        Every matched field forms part of one composite key. Rows whose key
        contains a null or an ignored value (e.g. a placeholder id 0) are
        skipped.

        Args:
            gdf: DataFrame or GeoDataFrame containing layer rows.

        Returns:
            Rule result dictionary listing the repeated keys.
        """
        fields = self.get_fields(gdf)
        if not fields:
            return self.no_fields_result()

        missing = [field for field in fields if field not in gdf.columns]
        if missing:
            return {
                "rule": self.name,
                "results": [
                    {
                        "field": field,
                        "passed": False,
                        "invalid_count": 1,
                        "uids": [],
                        "message": "Field is missing",
                    }
                    for field in missing
                ],
            }

        keys = gdf[fields]
        usable = ~keys.isna().any(axis=1)
        for field in fields:
            usable &= ~keys[field].isin(self.ignore_values)
        duplicated = keys[usable].duplicated(keep=False)
        invalid = gdf[usable][duplicated]
        repeated = (
            invalid[fields].astype(str).agg("|".join, axis=1).value_counts()
            if len(invalid)
            else pd.Series(dtype=int)
        )
        logger.info(
            "Unique rule '%s' found %d repeated keys in %d rows",
            self.name,
            len(repeated),
            len(invalid),
        )
        return {
            "rule": self.name,
            "results": [
                {
                    "field": ", ".join(fields),
                    "passed": len(invalid) == 0,
                    "invalid_count": len(invalid),
                    "uids": self.get_uids(invalid),
                    "message": None
                    if len(invalid) == 0
                    else "Repeated keys (rows): "
                    + ", ".join(f"{k} ({n})" for k, n in repeated.head(5).items()),
                }
            ],
        }


class MaxNullFractionRule(Rule):
    def validate(self, gdf: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that no matched field is mostly missing.

        Null values, blank strings and ignored sentinel values (e.g. -9999)
        count as missing. A field fails when its missing share exceeds value,
        which catches columns a pipeline filled entirely with no-data.

        Args:
            gdf: DataFrame or GeoDataFrame containing layer rows.

        Returns:
            Rule result dictionary with the missing share per field.
        """
        fields = self.get_fields(gdf)
        if not fields:
            return self.no_fields_result()

        limit = 0.0 if self.value is None else float(self.value)
        results = []
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
            missing = blank_mask(gdf[field]) | gdf[field].isin(self.ignore_values)
            share = float(missing.mean()) if len(gdf) else 0.0
            passed = share <= limit
            results.append(
                {
                    "field": field,
                    "passed": passed,
                    "invalid_count": 0 if passed else int(missing.sum()),
                    "uids": [],
                    "message": None
                    if passed
                    else f"{share:.0%} of rows are missing or no-data (limit {limit:.0%})",
                }
            )
        return {"rule": self.name, "results": results}


class DistinctColumnsRule(Rule):
    def validate(self, gdf: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that two or more fields are not copies of each other.

        Fails when a pair of matched fields holds identical values in every
        row, which usually means an export wrote one column into another.
        Pairs that are entirely zero or null are not flagged.

        Args:
            gdf: DataFrame or GeoDataFrame containing layer rows.

        Returns:
            Rule result dictionary with one entry per identical pair.
        """
        fields = [field for field in self.get_fields(gdf) if field in gdf.columns]
        if len(fields) < 2:
            return self.no_fields_result()

        min_rows = int(self.options.get("min_rows", 3))
        results = []
        for i, left in enumerate(fields):
            for right in fields[i + 1:]:
                a, b = gdf[left], gdf[right]
                both = a.notna() & b.notna()
                informative = both & ~((a == 0) & (b == 0))
                identical = (
                    informative.sum() >= min_rows and (a[both] == b[both]).all()
                )
                results.append(
                    {
                        "field": f"{left}, {right}",
                        "passed": not identical,
                        "invalid_count": int(both.sum()) if identical else 0,
                        "uids": [],
                        "message": "Fields are identical in every row"
                        if identical
                        else None,
                    }
                )
        return {"rule": self.name, "results": results}


class ExpressionRule(Rule):
    """
    Row-level check written as a pandas expression over several fields.

    value is the expression, e.g. "total = a + b" written as
    "abs(total - (a + b)) <= 0.02 * total". Column names that are not valid
    Python names go in backticks. Year- or season-suffixed column families
    use a placeholder: "{year}" in the expression is filled in from every
    column matching suffix_from (e.g. "soge_dev_percent_{year}"), and the
    expression is checked once per suffix. suffix_min skips smaller numeric
    suffixes (e.g. years before a method change).

    Rows where any referenced value is null or an ignored sentinel are
    skipped, so no-data never counts as a violation here.
    """

    PLACEHOLDER = re.compile(r"\{(\w+)\}")

    def _suffixes(self, gdf: pd.DataFrame) -> List[Dict[str, str]]:
        source = self.options.get("suffix_from")
        if not source:
            return [{}]
        names = self.PLACEHOLDER.findall(source)
        pattern = re.escape(source)
        for name in names:
            pattern = pattern.replace(
                re.escape("{" + name + "}"), f"(?P<{name}>[^_]+?)"
            )
        regex = re.compile(f"^{pattern}$", re.IGNORECASE)
        found = []
        for col in gdf.columns:
            match = regex.match(str(col))
            if match and match.groupdict() not in found:
                found.append(match.groupdict())
        suffix_min = self.options.get("suffix_min")
        if suffix_min is not None:
            found = [
                groups
                for groups in found
                if all(
                    not str(v).isdigit() or int(v) >= int(suffix_min)
                    for v in groups.values()
                )
            ]
        return found

    @staticmethod
    def _referenced(expression: str, columns: List[str]) -> List[str]:
        quoted = re.findall(r"`([^`]+)`", expression)
        bare = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", re.sub(r"`[^`]+`", " ", expression))
        return [col for col in dict.fromkeys(quoted + bare) if col in columns]

    def validate(self, gdf: pd.DataFrame) -> Dict[str, Any]:
        """
        Evaluate the expression per row and report rows where it is false.

        Args:
            gdf: DataFrame or GeoDataFrame containing layer rows.

        Returns:
            Rule result dictionary with one entry per suffix checked.
        """
        if not self.value:
            raise ValueError(f"Rule '{self.name}' requires an expression in value")

        results = []
        columns = [str(col) for col in gdf.columns]
        for groups in self._suffixes(gdf):
            expression = self.value.format(**groups) if groups else self.value
            label = expression if len(expression) <= 80 else expression[:77] + "..."
            referenced = self._referenced(expression, columns)
            frame = gdf.drop(columns="geometry", errors="ignore")
            try:
                outcome = frame.eval(expression, engine="python")
            except Exception as exc:  # unknown column or bad syntax
                results.append(
                    {
                        "field": label,
                        "passed": False,
                        "invalid_count": 1,
                        "uids": [],
                        "message": f"Expression could not be evaluated: {exc}",
                    }
                )
                continue

            usable = pd.Series(True, index=gdf.index)
            for col in referenced:
                values = gdf[col]
                usable &= values.notna()
                if self.ignore_values:
                    usable &= ~values.isin(self.ignore_values)
            invalid = gdf[usable & ~outcome.astype(bool)]
            results.append(
                {
                    "field": label,
                    "passed": len(invalid) == 0,
                    "invalid_count": len(invalid),
                    "uids": self.get_uids(invalid),
                }
            )

        if not results:
            return self.no_fields_result()
        return {"rule": self.name, "results": results}


RULE_MAP = {
    "max_value": MaxValueRule,
    "min_value": MinValueRule,
    "not_null": NotNullRule,
    "allowed_values": AllowedValuesRule,
    "data_type": DataTypeRule,
    "required_fields": RequiredFieldsRule,
    "required_any_field_pattern": RequiredAnyFieldPatternRule,
    "unique": UniqueRule,
    "max_null_fraction": MaxNullFractionRule,
    "distinct_columns": DistinctColumnsRule,
    "expression": ExpressionRule,
}


def _find_column(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    """Return the first column matching one of names, ignoring case."""
    lookup = {str(col).lower(): col for col in df.columns}
    for name in names:
        if str(name).lower() in lookup:
            return lookup[str(name).lower()]
    return None


class PairRule(ABC):
    """
    Rule comparing a vector layer with its stats-generator Excel sheet.

    Pair rules run only through VectorValidator.validate_pair, with
    applies_to: [pair]. They catch sheets built from a different generation
    of the data than the vector they summarise.
    """

    DEFAULT_KEYS = ["uid", "mws uid", "mws_id"]

    def __init__(self, name: str, value: Any = None, **options: Any) -> None:
        self.name = name
        self.value = value
        self.options = options

    def keys(self, vector_df: pd.DataFrame, excel_df: pd.DataFrame):
        vector_key = _find_column(
            vector_df, [self.options["vector_key"]] if "vector_key" in self.options else self.DEFAULT_KEYS
        )
        excel_key = _find_column(
            excel_df, [self.options["excel_key"]] if "excel_key" in self.options else self.DEFAULT_KEYS
        )
        return vector_key, excel_key

    def missing_key_result(self, vector_key, excel_key) -> Dict[str, Any]:
        return {
            "rule": self.name,
            "results": [
                {
                    "field": f"{vector_key or '?'} / {excel_key or '?'}",
                    "passed": False,
                    "invalid_count": 1,
                    "uids": [],
                    "message": "Id column not found in "
                    + " and ".join(
                        form
                        for form, key in (("vector", vector_key), ("excel", excel_key))
                        if key is None
                    ),
                }
            ],
        }

    @abstractmethod
    def validate_pair(self, vector_df: pd.DataFrame, excel_df: pd.DataFrame) -> Dict[str, Any]:
        pass


def _mws_level(ids: set) -> set:
    """Reduce ids like 12_305795_1377 (feature 1377 in MWS 12_305795) to the MWS id."""
    return {"_".join(str(i).split("_")[:2]) for i in ids}


class IdCoveragePairRule(PairRule):
    def validate_pair(self, vector_df: pd.DataFrame, excel_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that the vector and the sheet cover the same ids.

        When one form is keyed more finely than the other (per water body vs
        per MWS), both are compared at MWS level.

        Args:
            vector_df: The vector layer.
            excel_df: The Excel sheet.

        Returns:
            Rule result dictionary listing ids found in only one form.
        """
        vector_key, excel_key = self.keys(vector_df, excel_df)
        if vector_key is None or excel_key is None:
            return self.missing_key_result(vector_key, excel_key)

        v_ids = {str(i) for i in vector_df[vector_key].dropna()}
        x_ids = {str(i) for i in excel_df[excel_key].dropna()}
        depth = lambda ids: max((i.count("_") for i in ids), default=0)
        if depth(v_ids) != depth(x_ids):
            v_ids, x_ids = _mws_level(v_ids), _mws_level(x_ids)

        only_vector, only_excel = sorted(v_ids - x_ids), sorted(x_ids - v_ids)
        results = []
        for form, ids in (("vector", only_vector), ("excel", only_excel)):
            results.append(
                {
                    "field": f"{vector_key} / {excel_key}",
                    "passed": not ids,
                    "invalid_count": len(ids),
                    "uids": ids,
                    "message": f"{len(ids)} ids only in the {form}" if ids else None,
                }
            )
        return {"rule": self.name, "results": results}


class FieldMatchPairRule(PairRule):
    def validate_pair(self, vector_df: pd.DataFrame, excel_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that fields shared by the vector and the sheet agree.

        value lists field pairs: [{vector: area_in_ha, excel: area_in_ha,
        tolerance: 0.01}]. Rows are joined on the id columns; numbers must
        agree within the relative tolerance, other values exactly. A value
        present in one form and null in the other is a mismatch.

        Args:
            vector_df: The vector layer.
            excel_df: The Excel sheet.

        Returns:
            Rule result dictionary with mismatching ids per field pair.
        """
        vector_key, excel_key = self.keys(vector_df, excel_df)
        if vector_key is None or excel_key is None:
            return self.missing_key_result(vector_key, excel_key)

        results = []
        for pair in self.value or []:
            v_col = _find_column(vector_df, [pair["vector"]])
            x_col = _find_column(excel_df, [pair["excel"]])
            label = f"{pair['vector']} / {pair['excel']}"
            if v_col is None or x_col is None:
                results.append(
                    {
                        "field": label,
                        "passed": False,
                        "invalid_count": 1,
                        "uids": [],
                        "message": "Field is missing",
                    }
                )
                continue
            left = pd.DataFrame(
                {"key": vector_df[vector_key].astype(str), "v": vector_df[v_col].values}
            )
            right = pd.DataFrame(
                {"key": excel_df[excel_key].astype(str), "x": excel_df[x_col].values}
            )
            joined = left.merge(right, on="key", how="inner")
            v_blank, x_blank = blank_mask(joined["v"]), blank_mask(joined["x"])
            v_num = pd.to_numeric(joined["v"], errors="coerce")
            x_num = pd.to_numeric(joined["x"], errors="coerce")
            numeric = v_num.notna() & x_num.notna()
            tolerance = float(pair.get("tolerance", 0))
            differs = pd.Series(False, index=joined.index)
            differs[numeric] = (v_num - x_num).abs()[numeric] > tolerance * x_num.abs()[numeric]
            text = ~numeric & ~v_blank & ~x_blank
            differs[text] = joined["v"][text].astype(str) != joined["x"][text].astype(str)
            differs |= v_blank != x_blank
            bad = joined[differs]
            results.append(
                {
                    "field": label,
                    "passed": len(bad) == 0,
                    "invalid_count": len(bad),
                    "uids": bad["key"].tolist(),
                    "message": f"{len(bad)} of {len(joined)} shared ids differ"
                    if len(bad)
                    else None,
                }
            )
        return {"rule": self.name, "results": results}


PAIR_RULE_MAP = {
    "id_coverage": IdCoveragePairRule,
    "field_match": FieldMatchPairRule,
}
