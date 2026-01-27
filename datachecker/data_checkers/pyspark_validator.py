import pandas as pd
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
        raise NotImplementedError("PySpark support is not implemented yet")
        super().__init__(schema, data, file, format, hard_check, custom_checks)

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


if __name__ == "__main__":
    # Example usage (pandas)

    data = pd.DataFrame(
        [
            (1, "A"),
            (2, "B"),
            (1, "A"),  # Duplicate row
            (3, "C"),
        ],
        columns=["id", "value"],
    )

    schema = {
        "check_duplicates": True,
        "check_completeness": True,
        "completeness_columns": ["id", "value"],
        "columns": {
            "id": {"type": "integer", "check_duplicates": True},
            "value": {"type": "string", "check_duplicates": True},
        },
    }

    validator = PySparkValidator(schema, data, "datafile.csv", "csv")
    validator.run_checks()
    for entry in validator.qa_report:
        print(entry)
