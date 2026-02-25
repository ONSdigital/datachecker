import pandas as pd
import pandera.pandas as pa
import pytest

from datachecker.data_checkers.pandas_validator import DataValidator


@pytest.fixture(scope="class")
def df():
    return pd.DataFrame(
        {
            "valid_column": [1, 2, 3],
            "second_valid_column": [4.0, 5.0, 6],
            "third_valid_column": ["a", "b", "cghdgfdf"],
        }
    )


class TestCheckColumnContents:
    def test_check_column_contents_invalid(self, df):
        schema = {
            "columns": {
                "valid_column": {"type": "integer"},
                "Invalid Column": {"type": "integer", "optional": True},
                "invalid-column": {"type": "integer", "optional": True},
            }
        }
        validator = DataValidator(data=df, schema=schema, file="", format="")
        validator._check_column_contents(
            converted_schema=pa.DataFrameSchema(
                {
                    "valid_column": pa.Column(int, [pa.Check.gt(1)]),
                    "second_valid_column": pa.Column(float, [pa.Check.gt(0)]),
                    "third_valid_column": pa.Column(str, [pa.Check.str_length(min_value=1)]),
                }
            )
        )
        assert "Checking valid_column greater_than(1)" in validator.log[-6].get("description")
        assert validator.log[-6].get("outcome") == "fail"

    def test_check_column_contents_valid(self, df):
        schema = {
            "columns": {
                "valid_column": {"type": "integer"},
                "Invalid Column": {"type": "integer", "optional": True},
                "invalid-column": {"type": "integer", "optional": True},
            }
        }
        validator = DataValidator(data=df, schema=schema, file="", format="")
        validator._check_column_contents(
            converted_schema=pa.DataFrameSchema(
                {
                    "valid_column": pa.Column(int, [pa.Check.gt(0), pa.Check.lt(10)]),
                    "second_valid_column": pa.Column(float, [pa.Check.gt(0)]),
                    "third_valid_column": pa.Column(str, [pa.Check.str_length(min_value=1)]),
                }
            )
        )
        assert "Checking valid_column greater_than(0)" in validator.log[-7].get("description")
        assert validator.log[-7].get("outcome") == "pass"

    def test_check_column_contents_some_valid_some_invalid(self, df):
        schema = {
            "columns": {
                "valid_column": {"type": int},
                "Invalid Column": {"type": int, "optional": True},
                "invalid-column": {"type": int, "optional": True},
            }
        }
        validator = DataValidator(data=df, schema=schema, file="", format="")
        validator._check_column_contents(
            converted_schema=pa.DataFrameSchema(
                {
                    "valid_column": pa.Column(int, [pa.Check.gt(0), pa.Check.lt(10)]),
                    "second_valid_column": pa.Column(float, [pa.Check.gt(0)]),
                    "third_valid_column": pa.Column(
                        str, [pa.Check.str_length(min_value=1), pa.Check.str_length(max_value=5)]
                    ),
                }
            )
        )
        # Our log should include details of the passing tests and the failed tests
        # should include 8 column checks. 2 column name checks and 1 info entry
        assert len(validator.log) == 15
        assert "Checking valid_column greater_than(0)" in validator.log[-8].get("description")
        assert validator.log[-8].get("outcome") == "pass"
        assert "Checking third_valid_column str_length(None, 5)" in validator.log[-3].get(
            "description"
        )
        assert validator.log[-3].get("outcome") == "fail"

    def test_check_regex_strings(self, df):
        schema = {
            "columns": {
                "valid_column": {"type": int},
                "Invalid Column": {"type": int, "optional": True},
                "invalid-column": {"type": int, "optional": True},
            }
        }
        validator = DataValidator(data=df, schema=schema, file="", format="")
        validator._check_column_contents(
            converted_schema=pa.DataFrameSchema(
                {
                    "valid_column": pa.Column(int, [pa.Check.gt(0), pa.Check.lt(10)]),
                    "second_valid_column": pa.Column(float, [pa.Check.gt(0)]),
                    "third_valid_column": pa.Column(str, [pa.Check.str_matches(r"^[a-z]{1,5}$")]),
                }
            )
        )
        assert "Checking third_valid_column str_matches('^[a-z]{1,5}$')" in validator.log[-2].get(
            "description"
        )
        assert validator.log[-2].get("outcome") == "fail"
