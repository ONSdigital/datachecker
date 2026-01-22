from itertools import product

import pandas as pd

from datachecker.data_checkers.general_validator import Validator


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

    def validate(self):
        for check in (
            super()._check_colnames,
            super()._check_column_contents,
            self._check_duplicates,
            self._check_completeness,
        ):
            check()
        # Formatting to convert pandera descriptions to more readable format
        self._format_log_descriptions()
        self._convert_frame_wide_check_to_single_entry()
        return self

    def _validate_schema(self, schema):
        schema = super()._validate_schema(schema)
        # Additional checks specific to DataValidator
        df_columns = set(self.data.columns)
        schema_keys = set(schema["columns"].keys())
        # if not df_columns.issubset(schema_keys):
        missing = df_columns - schema_keys
        self._add_qa_entry(
            description="Dataframe columns missing from schema",
            failing_ids=list(missing),
            outcome=not missing,
            entry_type="error",
        )
        # if not schema_keys.issubset(df_columns):
        extra = schema_keys - df_columns
        self._add_qa_entry(
            description="Schema keys not in dataframe",
            failing_ids=list(extra),
            outcome=not extra,
            entry_type="warning",
        )

        # Only mandatory entry inside columns is "allow_na"
        for col, item in schema["columns"].items():
            item_keys = list(item.keys())
            required_keys = ["type", "allow_na", "optional"]
            if not all(key in item_keys for key in required_keys):
                # missing_key_values = True
                self._add_qa_entry(
                    description=(
                        f"Missing required properties in schema for column '{col}': "
                        f"{[key for key in required_keys if key not in item_keys]}"
                    ),
                    failing_ids=[col],
                    outcome=False,
                    entry_type="error",
                )

        self._check_unused_schema_arguments(schema)

        return schema

    def _check_unused_schema_arguments(self, schema):
        # Unused arguments in schema.
        valid_schema_keys = {
            "type",
            "min_val",
            "max_val",
            "min_length",
            "max_length",
            "allowed_strings",
            "forbidden_strings",
            "allow_na",
            "optional",
            "min_decimal",
            "max_decimal",
            "max_date",
            "min_date",
            "max_datetime",
            "min_datetime",
        }
        unpacked_keys = []
        for _col, item in schema["columns"].items():
            unpacked_keys.extend(item.keys())
        unpacked_keys = set(unpacked_keys)
        unused_keys = unpacked_keys.difference(valid_schema_keys)
        self._add_qa_entry(
            description="Checking for unused arguments in schema",
            failing_ids=list(unused_keys),
            outcome=not unused_keys,
            entry_type="warning",
        )

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
