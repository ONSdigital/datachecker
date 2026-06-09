import pandas as pd
import polars as pl
from pandas.testing import assert_frame_equal

from onsdatachecker import DataValidator, PolarsValidator


def test_failed_cases_index():
    schema = {
        "check_duplicates": False,
        "check_completeness": False,
        "columns": {
            "age": {"type": "int", "min_val": 0, "max_val": 120, "optional": False},
            "name": {"type": "string", "allowed_values": r"^[A-Za-z\s]+$", "optional": False},
        },
    }
    df = pd.DataFrame({"age": [25, 142, 160, 40], "name": ["Alice", "Bob", "Charlie@", "David"]})
    validator = DataValidator(
        schema, df, file="test_report.html", format="html", hard_check=False
    ).validate()

    failed_cases = validator._id_failed_cases()
    assert failed_cases == [1, 2]


def test_failed_cases_no_failing_ids():
    schema = {
        "columns": {
            "age": {"type": "int", "min_val": 0, "max_val": 120},
            "name": {"type": "string", "allowed_values": r"^[A-Za-z\s]+$"},
        }
    }
    df = pd.DataFrame({"age": [25, 30, 35, 40], "name": ["Alice", "Bob", "Charlie", "David"]})
    validator = DataValidator(schema, df, file="test_report.html", format="html", hard_check=False)
    failed_cases = validator._id_failed_cases()
    assert failed_cases == []


def test_failed_cases_pandas():
    schema = {
        "columns": {
            "age": {"type": "int", "min_val": 0, "max_val": 120},
            "name": {"type": "string", "allowed_values": r"^[A-Za-z\s]+$"},
        }
    }
    df = pd.DataFrame({"age": [25, -30, 35, 40], "name": ["Alice", "Bob", "@Charlie", "David"]})
    validator = DataValidator(schema, df, file="test_report.html", format="html", hard_check=False)
    validator.validate()
    failed_cases = validator.failed_cases()
    assert isinstance(failed_cases, pd.DataFrame)
    assert_frame_equal(failed_cases, df.loc[[1, 2]])


def test_failed_cases_polars():
    schema = {
        "columns": {
            "age": {"type": "int", "min_val": 0, "max_val": 120},
            "name": {"type": "string", "allowed_values": r"^[A-Za-z\s]+$"},
        }
    }
    df = pl.DataFrame({"age": [25, -30, 35, 40], "name": ["Alice", "Bob", "@Charlie", "David"]})
    validator = PolarsValidator(schema, df, file="test_report.html", format="html", hard_check=False)
    validator.validate()
    failed_cases = validator.failed_cases()
    assert isinstance(failed_cases, pl.DataFrame)
    assert failed_cases.shape[0] == 2
    assert failed_cases["age"].to_list() == [-30, 35]
    assert failed_cases["name"].to_list() == ["Bob", "@Charlie"]


def test_failed_high_number():
    # test to check row ids returned when more than 10 failed cases found.
    df = pd.DataFrame(
        {
            "id": range(1, 21),
            "age": [25, 30, 35, 40, 28, 32, 29, 31, 26, 33, 27, 34, 36, 38, 39, 41, 42, 43, 44, 45],
            "score": [
                85,
                90,
                78,
                92,
                88,
                91,
                79,
                87,
                84,
                89,
                86,
                93,
                95,
                97,
                98,
                100,
                102,
                104,
                105,
                107,
            ],
        }
    )

    schema = {
        "check_duplicates": False,
        "check_completeness": False,
        "columns": {
            "id": {"type": "int", "min_val": 0, "optional": False},
            "age": {"type": "int", "max_val": 30, "optional": False},
            "score": {"type": "int", "optional": False},
        },
    }
    validator = DataValidator(schema=schema, data=df, file=None, format=None).validate()
    failed_cases_return = validator.failed_cases()
    failed_cases_expected = df.loc[df["age"] > 30]
    assert_frame_equal(failed_cases_expected, failed_cases_return)
