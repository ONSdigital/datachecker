import pandas as pd
import polars as pl

from datachecker.data_checkers.general_validator import Validator
from datachecker.data_checkers.pandas_validator import DataValidator
from datachecker.data_checkers.polars_validator import PolarsValidator


def check_and_export(schema, data, file, format, hard_check=True, custom_checks=None) -> Validator:
    """
    function to create validation object, run validation and export log.

    Parameters
    ----------
    schema : dict
        The schema to validate against.
    data : pd.DataFrame | pl.DataFrame
        The data to validate.
    file : str
        The file path to export the validation log.
    format : str
        The format to use when exporting the log.
    hard_check : bool, optional
        Whether to perform strict validation checks, by default True
    custom_checks : list, optional
        A list of custom validation checks to apply, by default None

    Returns
    -------
    DataValidator
        Returns data validator object after validation and export.
    """
    if type(data) is pl.DataFrame:
        validator = PolarsValidator(
            schema=schema,
            data=data,
            file=file,
            format=format,
            hard_check=hard_check,
            custom_checks=custom_checks,
        )
    elif type(data) is pd.DataFrame:
        validator = DataValidator(
            schema=schema,
            data=data,
            file=file,
            format=format,
            hard_check=hard_check,
            custom_checks=custom_checks,
        )

    validator.validate()
    validator.export()
    return validator
