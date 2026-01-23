from itertools import product

import polars as pl

from datachecker.data_checkers.general_validator import Validator


class PolarsValidator(Validator):
    def __init__(
        self,
        schema: dict,
        data: pl.DataFrame,
        file: str,
        format: str,
        hard_check: bool = True,
        custom_checks: dict = None,
    ):
        super().__init__(schema, data, file, format, hard_check, custom_checks)

    def validate(self):
        for check in (
            super()._check_colnames,
            super()._check_column_contents,
            self._check_duplicates,
            self._check_completeness,
        ):
            check()
        # Formatting to convert pandera descriptions to more readable format
        super()._format_log_descriptions()
        super()._convert_frame_wide_check_to_single_entry()
        return self

    def _check_duplicates(self):
        # Check for duplicate rows in the dataframe
        if self.schema.get("check_duplicates", False):
            df_with_row_nr = self.data.with_row_index("_row_nr")
            duplicate_indices = (
                df_with_row_nr.filter(self.data.is_duplicated()).get_column("_row_nr").to_list()
            )
            # Polars doesn't have a pandas-style index; return row numbers instead
            self._add_qa_entry(
                description="Checking for duplicate rows in the dataframe",
                failing_ids=duplicate_indices,
                outcome=not duplicate_indices,
                entry_type="error",
            )

    def _check_completeness(self):
        if self.schema.get("check_completeness", False):
            cols_to_check = self.schema.get("completeness_columns", self.data.columns)
            # Generate all possible combinations of the column values
            unique_values = [self.data[col].drop_nulls().unique() for col in cols_to_check]
            combinations = set(product(*unique_values))
            existing_combinations = set(map(tuple, self.data[cols_to_check].drop_nulls().to_numpy()))
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
