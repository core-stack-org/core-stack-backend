from abc import ABC, abstractmethod
import fnmatch


class Rule(ABC):
    def __init__(
            self,
            name,
            field=None,
            field_pattern=None,
            value=None,
            severity="ERROR",
    ):
        self.name = name
        self.field = field
        self.field_pattern = field_pattern
        self.value = value
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
}
