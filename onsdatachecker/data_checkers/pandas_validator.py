from itertools import product

import pandas as pd

from onsdatachecker.data_checkers.general_validator import Validator


class DataValidator(Validator):
    """
    `DataValidator` is a subclass of Validator specifically for validating data loaded
    using pandas.
    It inherits all methods and attributes from the Validator class.

    Methods
    -------
    __init__(schema, data, file, format, hard_check, custom_checks)
        Initializes the `DataValidator` class with the provided parameters.
    _check_duplicates()
        Checks for duplicate rows in the dataframe based on the specified columns in the schema.
    _check_completeness()
        Checks for missing combinations of values in specified columns to ensure data completeness.
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
        """
        initialise class

        Parameters
        ----------
        schema : dict or str
            The schema or path to schema file to validate against.
        data : pd.DataFrame
            The data to be validated.
        file : str
            The file path for exporting validation logs.
        format : str
            The format to use when exporting logs.
        hard_check : bool, optional
            Determines if strict validation is enforced will stop pipeline if error or warning
            detected if set to true, by default True
        custom_checks : dict, optional
            any custom checks to run, by default None
        """
        super().__init__(schema, data, file, format, hard_check, custom_checks)

    def _check_duplicates(self):
        """
        checks for duplicate rows within the dataframe. Updates log entry to highlight
        status of check
        """
        # Check for duplicate rows in the dataframe
        col_subset = self.schema.get("duplicates_columns", None)
        if col_subset is None:
            col_subset = self.data.columns

        if self.schema.get("check_duplicates", False):
            duplicate_rows = self.data[self.data.duplicated(subset=col_subset, keep="first")]
            duplicate_indices = duplicate_rows.index.tolist()
            self._add_qa_entry(
                description="Checking for duplicate rows in the dataframe",
                failing_ids=duplicate_indices,
                outcome=not duplicate_indices,
                entry_type="error",
            )

    def _check_completeness(self):
        """
        checks dataframe for completeness i.e. checks combinations of values in each column
        to see if each possible combination is present in the data, does not return information
        on missing combinations.
        """
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
