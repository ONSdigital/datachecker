import pandas as pd
import polars as pl
from pandas.testing import assert_frame_equal

from onsdatachecker import DataValidator, PolarsValidator, Validator


def test_failed_cases_index():
    schema = {
        "columns": {
            "age": {"type": "int", "min": 0, "max": 120},
            "name": {"type": "string", "regex": r"^[A-Za-z\s]+$"},
        }
    }
    df = pd.DataFrame({"age": [25, 30, 35, 40], "name": ["Alice", "Bob", "Charlie", "David"]})
    validator = Validator(schema, df, file="test_report.html", format="html", hard_check=False)
    validator.log[1] = {
        "description": "Checking if age is between 0 and 120",
        "failing_ids": [1, 2],
        "outcome": "fail",
        "entry_type": "error",
    }
    validator.log[2] = {
        "description": "Checking if name contains only letters and spaces",
        "failing_ids": [2, "..."],
        "outcome": "fail",
        "entry_type": "error",
    }
    validator.log[3] = {
        "description": "Checking dtype",
        "failing_ids": ["int64"],
        "outcome": "fail",
        "entry_type": "error",
    }
    failed_cases = validator._id_failed_cases()
    assert failed_cases == [1, 2]


def test_failed_cases_no_failing_ids():
    schema = {
        "columns": {
            "age": {"type": "int", "min": 0, "max": 120},
            "name": {"type": "string", "regex": r"^[A-Za-z\s]+$"},
        }
    }
    df = pd.DataFrame({"age": [25, 30, 35, 40], "name": ["Alice", "Bob", "Charlie", "David"]})
    validator = Validator(schema, df, file="test_report.html", format="html", hard_check=False)
    validator.log[1] = {
        "description": "Checking if age is between 0 and 120",
        "failing_ids": None,
        "outcome": "fail",
        "entry_type": "error",
    }
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
