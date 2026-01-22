import pandas as pd
import pandera.pandas as pa
import pandera.polars as pap
import polars as pl


def get_dtype_lib(df) -> object:
    if type(df) is pd.DataFrame:
        return pa

    elif type(df) is pl.DataFrame:
        return pap

    else:
        raise TypeError("Unsupported DataFrame type")


# Functions to create pandera checks based on schema column keys
def min_val(value: int | float, library=pa):
    """
    Create a pandera check for minimum value.

    Parameters
    ----------
    value : int | float
        The minimum value to check against.

    Returns
    -------
    get_dtype_lib().Check
        A pandera check for the minimum value.
    """
    return library.Check.ge(value)


def max_val(value: int | float, library=pa):
    """
    Create a pandera check for maximum value.

    Parameters
    ----------
    value : int | float
        The maximum value to check against.

    Returns
    -------
    get_dtype_lib().Check
        A pandera check for the maximum value.
    """
    return library.Check.le(value)


def string_length(max_length: int = None, min_length: int = None, library=pa):
    """
    Create a pandera check for string length.

    Parameters
    ----------
    max_length : int | None
        The maximum length of the string.
    min_length : int | None
        The minimum length of the string.

    Returns
    -------
    get_dtype_lib().Check
        A pandera check for the string length.
    """
    return library.Check.str_length(max_value=max_length, min_value=min_length)


def allowed_strings(value: list | str, library=pa):
    """
    Create a pandera check for allowed strings.

    Parameters
    ----------
    value : list | str
        The list of allowed strings or regex pattern.

    Returns
    -------
    get_dtype_lib().Check
        A pandera check for the allowed strings.
    """
    if isinstance(value, str):
        return library.Check.str_matches(value)
    elif isinstance(value, list):
        return library.Check.isin(value)
    else:
        raise TypeError("allowed_strings value must be a list or string")


def forbidden_strings(value: list, library=pa):
    """
    Create a pandera check for forbidden strings.

    Parameters
    ----------
    value : list | str
        The list of forbidden strings.

    Returns
    -------
    get_dtype_lib().Check
        A pandera check for the forbidden strings.

    Raises
    ------
    TypeError
        If the value is a string. Regex is not supported for forbidden_strings or general
        TypeError if value is not a list or string.
    """
    if isinstance(value, list):
        return library.Check.notin(value)
    if isinstance(value, str):
        raise TypeError(
            "String patterns are not supported for forbidden_strings, "
            "please use either a list or a regex pattern in allowed_strings."
        )
    else:
        raise TypeError("forbidden_strings value must be a list or string")


def min_decimal(value: int, library=pa):
    """
    Create a pandera check for minimum decimal places for floats (possible with pandera
    decimal data type)

    Parameters
    ----------
    value : int
        The minimum number of decimal places.

    Returns
    -------
    get_dtype_lib().Check
        A pandera check for the minimum decimal places.
    """
    return library.Check(
        lambda s: s.apply(
            lambda x: (
                isinstance(x, float) and not pd.isnull(x) and len(str(x).split(".")[1]) >= value
            )
            if isinstance(x, float) and not pd.isnull(x)
            else True
        ),
        element_wise=False,
        error=f"has at least {value} decimal places",
    )


def max_decimal(value: int, library=pa):
    """
    Create a pandera check for maximum decimal places.

    Parameters
    ----------
    value : int
        The maximum number of decimal places.

    Returns
    -------
    get_dtype_lib().Check
        A pandera check for the maximum decimal places.
    """
    return library.Check(
        lambda s: s.apply(
            lambda x: (
                isinstance(x, float) and not pd.isnull(x) and len(str(x).split(".")[1]) <= value
            )
            if isinstance(x, float) and not pd.isnull(x)
            else True
        ),
        element_wise=False,
        error=f"has at most {value} decimal places",
    )


def max_date(value: str, library=pa):
    """
    Create a pandera check for maximum date.

    Parameters
    ----------
    value : str
        The maximum date or datetime in 'YYYY-MM-DD HH:MM' or equivalent format.
        YYYYMMDDHHMM is also accepted but recommend separating with - or / for clarity.
        Date format with no timestamp is also accepted.

    Returns
    -------
    get_dtype_lib().Check
        A pandera check for the maximum date.
    """
    max_date_value = pd.to_datetime(value)
    return library.Check.le(max_date_value)


def min_date(value: str, library=pa):
    """
    Create a pandera check for minimum date.

    Parameters
    ----------
    value : str
        The minimum date in 'YYYY-MM-DD HH:MM' or equivalent format.
        YYYYMMDDHHMM is also accepted but recommend separating with - or / for clarity.
        Date format with no timestamp is also accepted.

    Returns
    -------
    get_dtype_lib().Check
        A pandera check for the minimum date.
    """
    min_date_value = pd.to_datetime(value)
    return library.Check.ge(min_date_value)


def format_custom_checks(custom_checks: dict, library=pa) -> list:
    formatted_checks = []
    for check_name, check in custom_checks.items():
        formatted_checks.append(library.Check(check, name=check_name, error=check_name))
    return formatted_checks


def convert_schema(
    schema: dict, df: pd.DataFrame | pl.DataFrame, custom_checks: dict = None
) -> pa.DataFrameSchema:
    """
    Convert the loaded schema to a pandera DataFrameSchema. Uses simple defined
    functions to map schema keys to pandera checks. To add further checks, define
    schema key function above and add to the loop within this function.

    Parameters
    ----------
    schema : dict
        The schema to convert.

    Returns
    -------
    get_dtype_lib().DataFrameSchema
        The converted pandera DataFrameSchema.
    """
    # Convert JSON schema to pandera schema
    # Loop over each column in the JSON schema and create corresponding pandera Column objects
    library = get_dtype_lib(df)
    pa_schema_format = {}
    for column_name, constraints in schema["columns"].items():
        column_type = constraints["type"]
        nullable = constraints.get("allow_na", False)
        checks = []
        if "min_val" in constraints:
            checks.append(min_val(constraints["min_val"], library=library))
        if "max_val" in constraints:
            checks.append(max_val(constraints["max_val"], library=library))
        if "min_length" in constraints:
            checks.append(string_length(min_length=constraints["min_length"], library=library))
        if "max_length" in constraints:
            checks.append(string_length(max_length=constraints["max_length"], library=library))
        if "allowed_strings" in constraints:
            checks.append(allowed_strings(constraints["allowed_strings"], library=library))
        if "forbidden_strings" in constraints:
            checks.append(forbidden_strings(constraints["forbidden_strings"], library=library))
        if "min_decimal" in constraints:
            checks.append(min_decimal(constraints["min_decimal"], library=library))
        if "max_decimal" in constraints:
            checks.append(max_decimal(constraints["max_decimal"], library=library))
        if (
            "max_date" in constraints or "max_datetime" in constraints
        ) and column_type is pd.Timestamp:
            checks.append(
                max_date(
                    constraints.get("max_date", constraints.get("max_datetime")), library=library
                )
            )
        if (
            "min_date" in constraints or "min_datetime" in constraints
        ) and column_type is pd.Timestamp:
            checks.append(
                min_date(
                    constraints.get("min_date", constraints.get("min_datetime")), library=library
                )
            )

        pa_type = column_type
        if type(pa_type) is str:
            pa_type = {
                "int": int,
                "float": float,
                "str": str,
                "bool": bool,
                "datetime": pd.Timestamp,
            }.get(pa_type)

        pa_schema_format[column_name] = library.Column(
            dtype=pa_type, checks=checks, nullable=nullable
        )

    if custom_checks is not None:
        formatted_custom_checks = format_custom_checks(custom_checks)
    else:
        formatted_custom_checks = []

    return library.DataFrameSchema(pa_schema_format, checks=formatted_custom_checks)


def validate_using_pandera(
    converted_schema: pa.DataFrameSchema, data: pd.DataFrame
) -> pd.DataFrame | None:
    """
    validate data using a pandera DataFrameSchema. Returns a dataframe of failed checks
    with columns: 'column', 'check', 'failure_case', 'invalid_ids'.
    If all checks pass, returns None.

    Parameters
    ----------
    converted_schema : get_dtype_lib().DataFrameSchema
        The pandera DataFrameSchema to use for validation.
    data : pd.DataFrame
        The data to validate.

    Returns
    -------
    pd.DataFrame | None
        A dataframe of failed checks or None if all checks pass.
    """
    # ISSUE - NA pass not present in output log

    try:
        converted_schema.validate(data, lazy=True)

        # The following code is to add all checks when validation passes
        grouped_validation_return = None
    except get_dtype_lib(data).errors.SchemaErrors as e:
        # validation_return is now a pandas dataframe
        validation_return = e.failure_cases[["column", "check", "failure_case", "index"]]
        # Group by 'column' and 'check', collect failure cases and indices for each group
        grouped_validation_return = (
            validation_return.groupby(["column", "check"])
            .agg({"failure_case": list, "index": list})
            .reset_index()
            .rename(columns={"index": "invalid_ids"})
        )
    passing_tests = convert_schema_into_log_entries(converted_schema=converted_schema)
    combined = pd.concat([grouped_validation_return, passing_tests], ignore_index=True)
    # Drop duplicates
    combined = combined.drop_duplicates(["column", "check"], keep="first")
    # Sort by order in passing_tests["column"]
    if passing_tests is not None and not passing_tests.empty:
        col_order_passing = passing_tests["column"].unique().tolist()
        # make sure we grab failed column names if no passing test for that column
        if grouped_validation_return is None:
            col_order_failed = []
        else:
            col_order_failed = grouped_validation_return["column"].unique().tolist()
        col_order = col_order_passing + [
            col for col in col_order_failed if col not in col_order_passing
        ]
        combined["column"] = pd.Categorical(combined["column"], categories=col_order, ordered=True)
        combined = combined.sort_values("column").reset_index(drop=True)
    return combined


def convert_schema_into_log_entries(converted_schema: pa.DataFrameSchema) -> pd.DataFrame | None:
    """
    converts pandera schema into log entries dataframe for all checks defined in schema
    Used to create a complete log of all checks, including those that pass.

    Parameters
    ----------
    converted_schema : get_dtype_lib().DataFrameSchema
        The pandera DataFrameSchema to convert.

    Returns
    -------
    pd.DataFrame | None
        A dataframe of log entries or None if no checks are defined.
    """
    # allow_na (nullable) checks are not included in .checks
    # need to find a way to include these in the log output
    dict_column_checks = {}
    for i in converted_schema.columns:
        dict_column_checks[i] = converted_schema.columns[i].checks + converted_schema.checks

    if not dict_column_checks:
        # message = f"Checking {i} {temp[0].error}"
        return None
    else:
        list_of_checks = []
        list_of_columns = []
        for col, checks in dict_column_checks.items():
            for check in checks:
                list_of_checks.append(check.error)
                list_of_columns.append(col)
        for col in converted_schema.columns:
            data_type = converted_schema.columns[col].dtype
            list_of_checks.append(f"dtype('{data_type}')")
            list_of_columns.append(col)
        return pd.DataFrame(
            {
                "check": list_of_checks,
                "column": list_of_columns,
                "invalid_ids": [[]] * len(list_of_checks),
            }
        )
