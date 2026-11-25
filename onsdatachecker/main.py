from onsdatachecker.checks_loaders_and_exporters.checks import _type_id
from onsdatachecker.data_checkers.general_validator import Validator
from onsdatachecker.data_checkers.pandas_validator import DataValidator
from onsdatachecker.data_checkers.polars_validator import PolarsValidator
from onsdatachecker.data_checkers.pyspark_validator import PySparkValidator


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
    mod, name = _type_id(data)

    if (mod, name) == ("polars.dataframe.frame", "DataFrame"):
        validator = PolarsValidator(
            schema=schema,
            data=data,
            file=file,
            format=format,
            hard_check=hard_check,
            custom_checks=custom_checks,
        )

    elif (mod, name) == ("pandas", "DataFrame") or (mod, name) == ("pandas.core.frame", "DataFrame"):
        validator = DataValidator(
            schema=schema,
            data=data,
            file=file,
            format=format,
            hard_check=hard_check,
            custom_checks=custom_checks,
        )

    elif (mod, name) == ("pyspark.sql.dataframe", "DataFrame") or (mod, name) == (
        "pyspark.sql.classic.dataframe",
        "DataFrame",
    ):
        validator = PySparkValidator(
            schema=schema,
            data=data,
            file=file,
            format=format,
            hard_check=hard_check,
            custom_checks=custom_checks,
        )

    else:
        raise ValueError(f"Unsupported data type: ({mod}, {name})")

    validator.validate()
    validator.export()
    return validator
