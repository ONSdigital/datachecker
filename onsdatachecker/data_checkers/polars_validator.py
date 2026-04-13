import polars as pl

from onsdatachecker.data_checkers.general_validator import Validator


class PolarsValidator(Validator):
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

            missing_df = self.data.with_columns(pl.col(cols_to_check).is_null()).with_row_index(
                "_row_nr"
            )

            missing_eval = missing_df.select(pl.any_horizontal(pl.all())).to_pandas().any()

            if missing_eval:
                result = False
                missing_dict = {}
                for col in cols_to_check:
                    missing_dict.update(
                        {
                            col: pl.Series(
                                missing_df.filter(col).select("_row_nr").get_columns()
                            ).to_list()[0]
                        }
                    )
            else:
                result = True
                missing_dict = None

            if len(cols_to_check) > 4:
                cols_to_check = cols_to_check[:4] + ["..."]
            formatted_cols_to_check = ", ".join(cols_to_check)
            self._add_qa_entry(
                description="Checking for missing rows in the dataframe "
                + f"columns: {formatted_cols_to_check}",
                failing_ids=missing_dict,
                outcome=result,
                entry_type="error",
            )
