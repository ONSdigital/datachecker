import pandas as pd
import pandera.pandas as pa
import pytest

from datachecker.checks_loaders_and_exporters.checks import convert_schema, validate_using_pandera
from datachecker.data_checkers.pandas_validator import DataValidator


def test_convert_schema():
    schema_dict = {
        "columns": {
            "id": {"type": int, "min_val": 1, "max_val": 100},
            "age": {"type": float, "optional": True, "min_val": 0.0, "max_val": 120.0},
            "sex": {
                "type": str,
                "optional": True,
                "min_length": 1,
                "max_length": 10,
                "allowed_strings": ["M", "F"],
            },
        }
    }
    df = pd.DataFrame({"id": [-10, 50, 101], "age": [25.0, 120.0, -1.0], "sex": ["M", "F", "X"]})

    schema_obj = convert_schema(schema_dict, df)

    try:
        schema_obj.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        failed_validations = e.failure_cases[["column", "check", "failure_case", "index"]]
        assert failed_validations.shape == (4, 4)


def test_validate_allow_na():
    schema_dict = {
        "columns": {
            "id": {"type": "int", "min_val": 1, "max_val": 100, "allow_na": False},
            "age": {
                "type": float,
                "optional": True,
                "min_val": 0.0,
                "max_val": 120.0,
                "allow_na": True,
            },
        }
    }
    df = pd.DataFrame({"id": [-10, None, 101], "age": [25.0, None, -1.0]})
    schema_obj = convert_schema(schema_dict, df)
    result = validate_using_pandera(schema_obj, df)
    # 1) Check that a row is included with "id" and "not_nullable"
    assert any(
        (row["column"] == "id") and ("not_nullable" in str(row["check"]))
        for _, row in result.iterrows()
    )

    # 2) Check that a row is NOT included with "age" and "not_nullable"
    assert not any(
        (row["column"] == "age") and ("not_nullable" in str(row["check"]))
        for _, row in result.iterrows()
    )


def test_adding_passing_data_checks():
    schema_dict = {
        "columns": {
            "id": {"type": int, "min_val": 1, "max_val": 100},
            "age": {"type": float, "min_val": 0.0, "max_val": 120.0},
            "sex": {
                "type": str,
                "min_length": 1,
                "max_length": 10,
                "allowed_strings": ["M", "F"],
            },
        }
    }
    df = pd.DataFrame({"id": [10.0, 50.0, 90.0], "age": [25.0, 30.0, 35.0], "sex": ["M", "F", "M"]})

    schema_obj = convert_schema(schema_dict, df)
    # ID data check should fail
    # id: 3 checks, type min and max
    # age: 3 checks, type min and max
    # sex: 4 checks, type, min_length, max_length, allowed_strings
    # age and sex data checks should pass
    result = validate_using_pandera(schema_obj, df)

    expected_output_groupby = pd.Series({"id": 3, "age": 3, "sex": 4})
    actual = result.groupby("column", observed=False).size()
    actual.index = actual.index.astype(str)  # ensure index dtype matches
    actual.index.name = None  # remove index name for comparison
    pd.testing.assert_series_equal(actual, expected_output_groupby)


class TestStringChecks:
    def test_converting_string_regex(self):
        schema_dict = {
            "columns": {
                "code": {
                    "type": str,
                    "optional": True,
                    "allowed_strings": r"^[A-Z][0-9]$",
                },
            }
        }
        df = pd.DataFrame({"code": ["A1", "B2", "D4", "E5", "f6"]})
        schema_obj = convert_schema(schema_dict, df)
        try:
            schema_obj.validate(df, lazy=True)
        except pa.errors.SchemaErrors as e:
            failed_validations = e.failure_cases[["column", "check", "failure_case", "index"]]
            assert failed_validations.shape == (1, 4)

    def test_converting_allowed_string_raise_error(self):
        schema_dict = {
            "columns": {
                "code": {
                    "type": str,
                    "optional": True,
                    "allowed_strings": 2,
                },
            }
        }
        df = pd.DataFrame({"code": ["A1", "B2", "D4", "E5", "f6"]})
        try:
            convert_schema(schema_dict, df)
        except TypeError as e:
            assert str(e) == "allowed_strings value must be a list or string"

    def test_converting_forbidden_string_general_type_error(self):
        schema_dict = {
            "columns": {
                "code": {
                    "type": str,
                    "optional": True,
                    "forbidden_strings": 2,
                },
            }
        }
        df = pd.DataFrame({"code": ["A1", "B2", "D4", "E5", "f6"]})

        try:
            convert_schema(schema_dict, df)
        except TypeError as e:
            assert str(e) == "forbidden_strings value must be a list or string"

    def test_forbidden_string_raise_regex_error(self):
        schema_dict = {
            "columns": {
                "code": {
                    "type": str,
                    "optional": True,
                    "forbidden_strings": r"^[a-z][0-9]$",
                },
            }
        }
        df = pd.DataFrame({"code": ["A1", "B2", "D4", "E5", "f6"]})
        try:
            convert_schema(schema_dict, df)
        except TypeError as e:
            assert str(e) == (
                "String patterns are not supported for forbidden_strings, "
                "please use either a list or a regex pattern in allowed_strings."
            )

    def test_forbidden_string_list(self):
        schema_dict = {
            "columns": {
                "code": {
                    "type": str,
                    "optional": True,
                    "forbidden_strings": ["A1", "B2", "C3"],
                },
            }
        }
        df = pd.DataFrame({"code": ["A1", "B2", "D4", "E5", "f6"]})
        schema_obj = convert_schema(schema_dict, df)
        try:
            schema_obj.validate(df, lazy=True)
        except pa.errors.SchemaErrors as e:
            failed_validations = e.failure_cases[["column", "check", "failure_case", "index"]]
            assert failed_validations.shape == (2, 4)


class TestDecimalChecks:
    def setup_method(self):
        self.df = pd.DataFrame({"price": [10.12, 20.1, 30.123, 40.1234, 50.12345]})

    def test_check_max_decimal(self):
        schema_dict = {
            "columns": {
                "price": {
                    "type": float,
                    "optional": True,
                    "max_decimal": 3,
                },
            }
        }
        schema_obj = convert_schema(schema_dict, self.df)
        try:
            schema_obj.validate(self.df, lazy=True)
        except pa.errors.SchemaErrors as e:
            failed_validations = e.failure_cases[["column", "check", "failure_case", "index"]]
            assert failed_validations.shape == (2, 4)
            assert failed_validations["index"].tolist() == [3, 4]

    def test_check_min_decimal(self):
        schema_dict = {
            "columns": {
                "price": {
                    "type": float,
                    "optional": True,
                    "min_decimal": 2,
                },
            }
        }
        schema_obj = convert_schema(schema_dict, self.df)
        try:
            schema_obj.validate(self.df, lazy=True)
        except pa.errors.SchemaErrors as e:
            failed_validations = e.failure_cases[["column", "check", "failure_case", "index"]]
            assert failed_validations.shape == (1, 4)
            assert failed_validations["index"].tolist() == [1]


class TestDateTimeChecks:
    def setup_method(self):
        self.df = pd.DataFrame({"date": pd.to_datetime(["2000-01-01", "2005-06-15", "2010-12-31"])})

    def test_check_max_date(self):
        schema_dict = {
            "columns": {
                "date": {
                    "type": pd.Timestamp,
                    "optional": True,
                    "max_date": "2008-12-31",
                },
            }
        }
        schema_obj = convert_schema(schema_dict, self.df)
        try:
            schema_obj.validate(self.df, lazy=True)
        except pa.errors.SchemaErrors as e:
            failed_validations = e.failure_cases[["column", "check", "failure_case", "index"]]
            assert failed_validations.shape == (1, 4)
            assert failed_validations["index"].tolist() == [2]

    def test_check_min_date(self):
        schema_dict = {
            "columns": {
                "date": {
                    "type": pd.Timestamp,
                    "optional": True,
                    "min_date": "2002-01-01",
                },
            }
        }
        schema_obj = convert_schema(schema_dict, self.df)
        try:
            schema_obj.validate(self.df, lazy=True)
        except pa.errors.SchemaErrors as e:
            failed_validations = e.failure_cases[["column", "check", "failure_case", "index"]]
            assert failed_validations.shape == (1, 4)
            assert failed_validations["index"].tolist() == [0]

    def test_check_max_datetime(self):
        schema_dict = {
            "columns": {
                "date": {
                    "type": pd.Timestamp,
                    "optional": True,
                    "max_datetime": "2008-12-31 23:59",
                },
            }
        }
        schema_obj = convert_schema(schema_dict, self.df)
        try:
            schema_obj.validate(self.df, lazy=True)
        except pa.errors.SchemaErrors as e:
            failed_validations = e.failure_cases[["column", "check", "failure_case", "index"]]
            assert failed_validations.shape == (1, 4)
            assert failed_validations["index"].tolist() == [2]

    def test_check_min_datetime(self):
        schema_dict = {
            "columns": {
                "date": {
                    "type": pd.Timestamp,
                    "optional": True,
                    "min_datetime": "2002-01-01 00:00",
                },
            }
        }
        schema_obj = convert_schema(schema_dict, self.df)
        try:
            schema_obj.validate(self.df, lazy=True)
        except pa.errors.SchemaErrors as e:
            failed_validations = e.failure_cases[["column", "check", "failure_case", "index"]]
            assert failed_validations.shape == (1, 4)
            assert failed_validations["index"].tolist() == [0]


class TestingCustomChecks:
    def setup_method(self):
        self.schema = {
            "columns": {
                "age": {
                    "type": int,
                },
                "income": {
                    "type": int,
                },
                "sex": {
                    "type": str,
                },
            },
        }
        self.df = pd.DataFrame(
            {"age": [16, 25, 30], "income": [500, 1500, 2000], "sex": ["M", "F", "M"]}
        )

    def test_custom_checks(self):
        custom_checks_dict = {
            "adult_income_check": lambda df: (df["income"] > 0) & (df["age"] >= 18),
        }

        schema_obj = convert_schema(self.schema, self.df, custom_checks=custom_checks_dict)

        try:
            schema_obj.validate(self.df, lazy=True)
        except pa.errors.SchemaErrors as e:
            failed_validations = e.failure_cases[["column", "check", "failure_case", "index"]]
            # df wide checks produce a check per column in df, not the number involved in
            # the check. We would expect 2 failures here but have 3 for columns.
            assert failed_validations.shape == (self.df.shape[0], 4)
            assert failed_validations["index"].unique().tolist() == [0]

    def test_custom_checks_type_error_key(self):
        custom_checks_dict = {
            "adult_income_check": "this is not a function",
        }
        with pytest.raises(TypeError):
            DataValidator(
                schema=self.schema,
                data=self.df,
                file=None,
                format=None,
                custom_checks=custom_checks_dict,
            )

    def test_custom_checks_type_error_overall(self):
        custom_checks_dict = [
            lambda df: (df["income"] > 0) & (df["age"] >= 18),
        ]
        with pytest.raises(TypeError):
            DataValidator(
                schema=self.schema,
                data=self.df,
                file=None,
                format=None,
                custom_checks=custom_checks_dict,
            )
