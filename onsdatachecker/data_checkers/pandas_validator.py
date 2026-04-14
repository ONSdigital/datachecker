import pandas as pd

from onsdatachecker.data_checkers.general_validator import Validator


class DataValidator(Validator):
    """
    DataValidator is a subclass of Validator specifically for validating data.
    It inherits all methods and attributes from the Validator class.
    """

    def __init__(
        self,
        schema: dict,
        data: pd.DataFrame,
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

            missing_df = self.data[cols_to_check].isna()

            # True/False evaluation for the presence of missing values
            if missing_df.any().any():
                result = False
                missing_dict = {}
                for col in cols_to_check:
                    missing_dict.update({col: missing_df[missing_df[col]].index.tolist()})
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
