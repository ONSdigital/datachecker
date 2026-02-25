import getpass
import platform
import re
import warnings
from importlib.metadata import version

import pandas as pd
import pandera

from datachecker.checks_loaders_and_exporters.checks import (
    convert_schema,
    validate_using_pandera,
)
from datachecker.checks_loaders_and_exporters.schema_loader import SchemaLoader
from datachecker.checks_loaders_and_exporters.validator_exporter import Exporter


class SetupStructure:
    """
    Base class for setting up the structure of validation logs, including methods for
    exporting, printing logs, and adding QA entries.

    """

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        # Create a table header
        sys_info = "\n".join([f"{key}: {value}" for key, value in self.log[0].items()])
        headers = [
            "Timestamp",
            "Status",
            "Description",
            "Outcome",
            "Failing IDs",
            "Number Failing",
        ]
        header_row = " | ".join(headers)
        separator = "-|-".join(["-" * len(h) for h in headers])
        # Create table rows
        rows = []
        for entry in self.log[1:]:
            row = [
                entry.get("timestamp", ""),
                entry.get("status", "").upper(),
                entry.get("description", ""),
                entry.get("outcome", ""),
                ", ".join(map(str, entry.get("failing_ids", []))),
                str(entry.get("number_failing", "")),
            ]
            rows.append(" | ".join(row))
        log_entries = "\n".join([sys_info, "\n", header_row, separator] + rows)
        return log_entries if log_entries else "No log entries."

    def export(self):
        Exporter.export(self.log, self.format, self.file)
        self._hard_check_status()

    def _create_log(self):
        sys_info = {
            "date": pd.Timestamp.now().strftime("%Y-%m-%d"),
            "user": getpass.getuser(),
            "device": platform.node(),
            "device_platform": platform.platform(),
            "architecture": platform.architecture()[0],
            "python_version": platform.python_version(),
            "pandas_version": pd.__version__,
            "pandera_version": pandera.__version__,
            "datachecker_version": version("datachecker"),
        }
        return [sys_info]

    def _add_qa_entry(self, description, failing_ids, outcome, entry_type="info"):
        outcome = "pass" if outcome else "fail"
        if entry_type not in ["info", "error", "warning"]:
            raise ValueError("entry_type must be 'info', 'error', or 'warning'.")
        if failing_ids is not None:
            n_failing = len(failing_ids)
            if len(failing_ids) > 10:
                failing_ids = failing_ids[:10] + ["..."]
        else:
            failing_ids = []
            n_failing = 0
        timestamp = pd.Timestamp.now().strftime("%H:%M:%S")

        log_entry = {
            "timestamp": timestamp,
            "description": description,
            "outcome": outcome,
            "failing_ids": failing_ids,
            "number_failing": n_failing,
            "status": entry_type,
        }

        self.log.append(log_entry)


class Validator(SetupStructure):
    """
    Validator class for validating data against a specified schema.

    Attributes
    ----------
    log : list
        Stores log entries for validation steps and outcomes.
    schema : dict or object
        The loaded schema used for validation.
    data : any
        The data to be validated.
    file : str
        The file path for exporting validation logs.
    format : str
        The format to use when exporting logs.
    hard_check : bool
        Determines if strict validation is enforced.

    Methods
    -------
    __init__(schema, data, file, format, hard_check=True)
        Initializes the Validator with schema, data, file, format, and validation strictness.
    validate()
        Runs a series of validation checks on the data, including column names, types, and contents.
    add_qa_entry(description, outcome, entry_type="info")
        Adds a QA log entry with a description, outcome, and entry type (default is "info").
    export()
        Exports the validation log using the specified format and file path.
    __repr__()
        Returns a string representation of the Validator instance.
    __str__()
        Returns a formatted string with schema information and the validation log.
    validate_schema(schema)
        Loads and validates the schema from a file path or returns it if already loaded.
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
        self.log = self._create_log()
        self.data = data
        self.file = file
        self.format = format
        self.hard_check = hard_check
        self._validate_and_assign_custom_checks(custom_checks)
        self.schema = self._validate_schema(schema)

    def validate(self):
        for check in (
            self._check_colnames,
            self._check_column_contents,
            self._check_duplicates,
            self._check_completeness,
        ):
            check()
        # Formatting to convert pandera descriptions to more readable format
        self._format_log_descriptions()
        self._convert_frame_wide_check_to_single_entry()
        return self

    def _validate_and_assign_custom_checks(self, custom_checks):
        if custom_checks is not None:
            if not isinstance(custom_checks, dict):
                raise TypeError("custom_checks must be a dictionary of check_name: function pairs.")
            for name, func in custom_checks.items():
                if not callable(func):
                    raise TypeError(f"Custom check '{name}' is not callable.")
        self.custom_checks = custom_checks

    def _validate_schema(self, schema):
        if not isinstance(schema, (str, dict)):
            raise ValueError("Schema must be a file path (str) or a loaded schema (dict).")

        if isinstance(schema, str):
            format = schema.split(".")[-1]
            schema = SchemaLoader.load(schema, format)

        # Handles case where type is given as string, "str" will pass check
        # "object" will also pass checks but "string" fails for some reason, this fixes
        if isinstance(schema, dict) and "columns" in schema:
            for _col, props in schema["columns"].items():
                if "type" in props and props["type"] == "string":
                    props["type"] = "str"

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

    def _hard_check_status(self):
        error_count = 0
        warning_count = 0
        for entry in self.log[1:]:
            if entry["status"] == "error" and entry["outcome"] == "fail":
                error_count += 1
            elif entry["status"] == "warning" and entry["outcome"] == "fail":
                warning_count += 1

        # Always raise a warning for the number of warnings, if any
        if warning_count > 0:
            warnings.warn(
                f"Soft checks failed: {warning_count} warning(s) found, "
                "see log output for more details",
                UserWarning,
                stacklevel=2,
            )
        if self.hard_check and error_count > 0:
            raise ValueError(
                f"Hard checks failed: {error_count} error(s) found, see log output for more details"
            )
        elif error_count > 0:
            warnings.warn(
                f"Soft checks failed: {error_count} warning(s) found, "
                "see log output for more details",
                UserWarning,
                stacklevel=2,
            )

    def _format_log_descriptions(self):
        # Optional method to format log descriptions for better readability

        regex_replacements = [
            (
                r"str_length\(\s*(\d+(?:\.\d+)?)\s*,\s*None\s*\)",
                r"string length greater than or equal to \1",
            ),
            (
                r"str_length\(\s*(\d+(?:\.\d+)?)\s*,\s*(\d+(?:\.\d+)?)\s*\)",
                r"string length between \1 and \2",
            ),
            (
                r"str_length\(\s*None\s*,\s*(\d+(?:\.\d+)?)\s*\)",
                r"string length less than or equal to \1",
            ),
            (r"dtype\('(\S+)'\)", r"is data type \1"),
            (r"isin\(\s*\[([^\]]+)\]\s*\)", r"contains only [\1]"),
            (r"str_matches\(\s*r?['\"](.*?)['\"]\s*\)", r"string matches pattern '\1'"),
            (r"greater_than_or_equal_to\(\s*(\d+(?:\.\d+)?)\s*\)", r"greater than or equal to \1"),
            (r"less_than_or_equal_to\(\s*(\d+(?:\.\d+)?)\s*\)", r"less than or equal to \1"),
            (r"less_than_or_equal_to\(\s*(\S{10}\s+\S{8})\s*\)", r"before or equal to \1"),
            (r"greater_than_or_equal_to\(\s*(\S{10}\s+\S{8})\s*\)", r"after or equal to \1"),
            # Add more regex patterns as needed
        ]
        for entry in self.log[1:]:
            # Bulk replace items in description using a dictionary
            desc = entry["description"]
            for pattern, repl in regex_replacements:
                desc = re.sub(pattern, repl, desc)

            entry["description"] = desc

    def _convert_frame_wide_check_to_single_entry(self):
        # Want to take any repeated log entries i.e. custom checks and convert to a single
        # Log entry with description "Custom data check {check_name}"

        # escape if no custom checks
        if self.custom_checks is None:
            return

        log_entries = self.log[1:]  # Exclude system info
        log_df = pd.DataFrame(log_entries)
        grouped = pd.DataFrame()
        for custom_check in self.custom_checks:
            pattern = rf"\b{re.escape(custom_check)}\b"
            custom_check_entries = log_df[
                log_df["description"].str.contains(pattern, case=False)
            ].copy()
            custom_check_entries["custom_check_name"] = custom_check
            # Drop the row from log_df before further processing
            # Only match exact custom_check as a whole word in description
            log_df = log_df[~log_df["description"].str.contains(pattern, case=False)]
            if not custom_check_entries.empty:
                grouped = pd.concat([grouped, custom_check_entries])

        if not grouped.empty:
            single_entry = grouped.groupby(["custom_check_name"], as_index=False).first()
            single_entry["description"] = single_entry["custom_check_name"].apply(
                lambda x: f"Custom data check {x}"
            )
            # Convert grouped DataFrame back to list of dicts
            wide_checks_entries = single_entry.drop(columns=["custom_check_name"]).to_dict(
                orient="records"
            )
            # Remove old custom check entries from log
            self.log = (
                [self.log[0]]
                + [
                    entry
                    for entry in self.log[1:]
                    if not any(
                        entry.get("description", "").lower().find(custom_check.lower()) != -1
                        for custom_check in self.custom_checks
                    )
                ]
                + wide_checks_entries
            )

    def _check_colnames(self):
        # Check column names do not contain symbols other than underscore or spaces
        invalid_cols = [
            col for col in self.data.columns if not all(c.isalnum() or c in ["_"] for c in col)
        ]
        self._add_qa_entry(
            description="Checking column names",
            failing_ids=invalid_cols,
            outcome=not invalid_cols,
            entry_type="error",
        )

        # Check column names are all lowercase
        uppercase_cols = [col for col in self.data.columns if any(c.isupper() for c in col)]
        self._add_qa_entry(
            description="Checking column names are lowercase",
            failing_ids=uppercase_cols,
            outcome=not uppercase_cols,
            entry_type="warning",
        )

        # Check mandatory columns are present
        missing_mandatory = [
            col
            for col, props in self.schema.get("columns", {}).items()
            if not props.get("optional", False) and col not in self.data.columns
        ]
        self._add_qa_entry(
            description="Checking mandatory columns are present",
            failing_ids=missing_mandatory,
            outcome=not missing_mandatory,
            entry_type="error",
        )

        # Check no unexpected columns are present
        unexpected_cols = [
            col for col in self.data.columns if col not in self.schema.get("columns", {})
        ]
        self._add_qa_entry(
            description="Checking for unexpected columns",
            failing_ids=unexpected_cols,
            outcome=not unexpected_cols,
            entry_type="warning",
        )

    def _check_column_contents(self, converted_schema=None):
        # code to pass through converted schema. helps unit testing
        if converted_schema is None:
            converted_schema = convert_schema(self.schema, self.data, self.custom_checks)
        grouped_validation_return = validate_using_pandera(converted_schema, data=self.data)
        # Issue, only failed data type checks are returned from pandera validation
        if grouped_validation_return is not None:
            for i in grouped_validation_return.index:
                entry_description = (
                    f"Checking {grouped_validation_return.at[i, 'column']} "
                    + f"{grouped_validation_return.at[i, 'check']}"
                )
                invalid_ids = grouped_validation_return.at[i, "invalid_ids"]
                if invalid_ids == [None]:
                    invalid_ids = grouped_validation_return.at[i, "failure_case"]

                self._add_qa_entry(
                    description=entry_description,
                    failing_ids=invalid_ids,
                    outcome=not bool(invalid_ids),
                    entry_type="error",
                )

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
