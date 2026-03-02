import re

import pyspark.sql.types as T
from pyspark.sql import functions as F

from datachecker.data_checkers.general_validator import Validator


class PySparkValidator(Validator):
    def __init__(
        self,
        schema: dict,
        data,
        file: str,
        format: str,
        hard_check: bool = True,
        custom_checks: dict = None,
    ):
        # raise NotImplementedError("PySpark support is not implemented yet")
        super().__init__(schema, data, file, format, hard_check, custom_checks)
        self._convert_schema_dtypes()

    def validate(self):
        super().validate()
        self._convert_pyspark_error_messages()

    def _convert_pyspark_error_messages(self):
        message = "Pyspark does not return cases or index"
        for i in range(1, len(self.log) - 1):
            entry = self.log[i]
            if (
                entry["failing_ids"] is None
                or len(entry["failing_ids"]) == 0
                or not isinstance(entry["failing_ids"][0], str)
            ):
                continue
            # <Schema Column ...> is the message when a check fails for pyspark
            # replace it with blanket statement. Should still pass other important errors
            # back to user if they are not related to pyspark checks
            elif re.search(r"<Schema Column", entry["failing_ids"][0]) is not None:
                entry["failing_ids"][0] = message
            else:
                continue

    def _convert_schema_dtypes(self):
        mapping_dtypes = {
            "int": T.IntegerType(),
            "float": T.FloatType(),
            "string": T.StringType(),
            "str": T.StringType(),
            "bool": T.BooleanType(),
            "date": T.DateType(),
            "datetime": T.DateType(),
            "timestamp": T.TimestampType(),
        }
        for col in self.schema.get("columns", {}):
            input_type = self.schema["columns"][col].get("type")
            if input_type not in mapping_dtypes and input_type not in mapping_dtypes.values():
                raise ValueError(
                    f"Unsupported data type '{input_type}' for column '{col}' in schema. "
                    f"Supported types are: {list(mapping_dtypes.keys())}"
                )
            self.schema["columns"][col]["type"] = mapping_dtypes.get(
                self.schema["columns"][col]["type"], self.schema["columns"][col]["type"]
            )

    def _check_duplicates(self):
        # Check for duplicate rows in the dataframe
        if self.schema.get("check_duplicates", False):
            # Find duplicate rows (based on all columns)
            dup_counts = self.data.groupBy(*self.data.columns).count().filter(F.col("count") > 1)

            # Collect duplicate row identifiers (entire row as dicts)
            duplicate_rows = [row.asDict() for row in dup_counts.drop("count").collect()]

            self._add_qa_entry(
                description="Checking for duplicate rows in the dataframe",
                failing_ids=duplicate_rows,
                outcome=not duplicate_rows,
                entry_type="error",
            )

    def _check_completeness(self):
        if self.schema.get("check_completeness", False):
            cols_to_check = self.schema.get("completeness_columns", self.data.columns)

            # Build the expected cartesian product of distinct (non-null) values per column
            distinct_per_col = [
                self.data.select(F.col(c)).where(F.col(c).isNotNull()).distinct()
                for c in cols_to_check
            ]

            expected = distinct_per_col[0]
            for df in distinct_per_col[1:]:
                expected = expected.crossJoin(df)

            # Build the set of existing (non-null) combinations
            existing = (
                self.data.select(*[F.col(c) for c in cols_to_check])
                .where(F.concat_ws("||", *[F.col(c) for c in cols_to_check]).isNotNull())
                .dropna()
                .distinct()
            )

            # Missing combinations are expected minus existing
            missing_combinations = expected.subtract(existing)

            # True if no missing combinations exist
            result = missing_combinations.limit(1).count() == 0
            if len(cols_to_check) > 4:
                cols_to_check = cols_to_check[:4] + ["..."]
            formatted_cols_to_check = ", ".join(cols_to_check)
            self._add_qa_entry(
                description="Checking for missing rows in the dataframe "
                + f"columns: {formatted_cols_to_check}",
                failing_ids=None,
                outcome=result,
                entry_type="error",
            )
