from itertools import product

from pyspark.sql import SparkSession

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
        super().__init__(schema, data, file, format, hard_check, custom_checks)

    def _check_duplicates(self):
        # Check for duplicate rows in the dataframe
        if self.schema.get("check_duplicates", False):
            duplicate_rows = self.data[self.data.duplicated(keep="first")]
            duplicate_indices = duplicate_rows.index.tolist()
            self._add_qa_entry(
                description="Checking for duplicate rows in the dataframe",
                failing_ids=duplicate_indices,
                outcome=not duplicate_indices,
                entry_type="error",
            )

    def _check_completeness(self):
        if self.schema.get("check_completeness", False):
            cols_to_check = self.schema.get("completeness_columns", self.data.columns.tolist())
            # Generate all possible combinations of the column values
            unique_values = [self.data[col].dropna().unique() for col in cols_to_check]
            combinations = set(product(*unique_values))
            existing_combinations = set(map(tuple, self.data[cols_to_check].dropna().values))
            missing_combinations = combinations - existing_combinations
            result = len(missing_combinations) == 0
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
    spark = SparkSession.builder.appName("DataValidator").getOrCreate()
    # Example usage
    data = spark.createDataFrame(
        [
            (1, "A"),
            (2, "B"),
            (1, "A"),  # Duplicate row
            (3, "C"),
        ],
        ["id", "value"],
    )

    schema = {
        "check_duplicates": True,
        "check_completeness": True,
        "completeness_columns": ["id", "value"],
    }

    validator = PySparkValidator(schema, data, "datafile.csv", "csv")
    validator.run_checks()
    for entry in validator.qa_report:
        print(entry)
    spark.stop()
