import os
import re
import tempfile

import pandas as pd
import polars as pl
import pytest

from datachecker.data_checkers.pandas_validator import DataValidator
from datachecker.data_checkers.polars_validator import PolarsValidator
from datachecker.main import check_and_export

data = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
mock_df = pd.DataFrame(data)


@pytest.fixture(scope="class")
def new_validator():
    return DataValidator(
        schema="tests/data/test.json", data=mock_df, file="exported_log.yaml", format="yaml"
    )


class TestValidatorInstantiation:
    def test_validator(self, new_validator):
        assert isinstance(new_validator, DataValidator)
        assert new_validator.schema is not None

    def test_validator_validate(self, new_validator: DataValidator):
        new_validator.validate()
        assert len(new_validator.log) > 0

    def test_validator_export(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmpfile:
            DataValidator(
                schema="tests/data/test.json",
                data=mock_df,
                file=tmpfile.name,
                format="yaml",
                hard_check=False,
            ).export()
            assert os.path.exists(tmpfile.name)

    def test_validator_str(self, new_validator):
        new_validator.validate()
        str_repr = str(new_validator)
        assert "INFO" in str_repr or "ERROR" in str_repr or "WARNING" in str_repr

    def test_validator_repr(self, new_validator):
        new_validator.validate()
        repr_str = repr(new_validator)
        assert "INFO" in repr_str or "ERROR" in repr_str or "WARNING" in repr_str


class TestDataValidatorCustomChecks:
    def test_custom_checks_pass(self, new_validator: DataValidator):
        custom_checks = {
            "age_id_check": lambda df: (df["age"] > 18) & (df["id"].isin([1, 2, 3])),
        }
        schema = {
            "columns": {
                "age": {
                    "type": int,
                    "max_val": 120,
                    "allow_na": False,
                },
                "id": {
                    "type": int,
                    "allow_na": False,
                },
                "name": {
                    "type": str,
                    "allow_na": False,
                },
            }
        }
        new_validator.custom_checks = custom_checks
        new_validator.schema = schema

        new_validator.validate()
        # Access the protected method for testing purposes
        assert any("age_id_check" in str(log_entry) for log_entry in new_validator.log)

    def test_custom_checks_fail(self, new_validator: DataValidator):
        custom_checks = {
            "age_id_check": lambda df: (df["age"] < 18) & (df["id"].isin([1, 2, 3])),
            "age_id_check2": lambda df: (df["age"] < 18) & (df["id"].isin([1, 2, 3])),
        }
        schema = {
            "columns": {
                "age": {
                    "type": int,
                    "max_val": 120,
                    "allow_na": False,
                },
                "id": {
                    "type": int,
                    "allow_na": False,
                },
                "name": {
                    "type": str,
                    "allow_na": False,
                },
            }
        }
        new_validator.custom_checks = custom_checks
        new_validator.schema = schema

        new_validator.validate()
        assert (
            sum(
                bool(re.search(r"\bage_id_check\b", str(log_entry)))
                for log_entry in new_validator.log
            )
            == 1
        )
        assert (
            sum(
                bool(re.search(r"\bage_id_check2\b", str(log_entry)))
                for log_entry in new_validator.log
            )
            == 1
        )


def test_limiting_output_counts():
    data = {"id": list(range(10)), "value": list(range(10))}
    df = pd.DataFrame(data)
    validator = DataValidator(schema="tests/data/test.json", data=df, file=None, format=None)
    validator._add_qa_entry(
        description="Test entry", failing_ids=list(range(1, 16)), outcome=False, entry_type="error"
    )
    compressed_entry = validator.log[-1]
    # Check that only a limited number of rows are shown in the output
    assert "..." in str(compressed_entry)


def test_qa_type_error():
    data = {"id": list(range(10)), "value": list(range(10))}
    df = pd.DataFrame(data)
    validator = DataValidator(
        schema="tests/data/test.json", data=df, file=None, format=None, hard_check=False
    )
    with pytest.raises(ValueError, match="entry_type must be 'info', 'error', or 'warning'."):
        validator._add_qa_entry(
            description="Test entry",
            failing_ids=list(range(1, 16)),
            outcome=False,
            entry_type="my_custom_error",
        )


class TestCheckAndExport:
    def setup_method(self):
        self.data = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
        self.df = pd.DataFrame(self.data)

    def test_check_and_export(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmpfile:
            check_and_export(
                schema="tests/data/test.json",
                data=self.df,
                file=tmpfile.name,
                format="yaml",
                hard_check=False,
            )
        assert os.path.exists(tmpfile.name)

    def test_check_and_export_hard_check(self):
        with (
            tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmpfile,
            pytest.raises(ValueError),
        ):
            check_and_export(
                schema="tests/data/test.json",
                data=self.df,
                file=tmpfile.name,
                format="yaml",
                hard_check=True,
            )
        assert os.path.exists(tmpfile.name)


class TestPolarsValidaor:
    def test_polars_validator(self):
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 2],
                "name": ["Alice", "Bob", "Charlie", "Bob"],
                "score": [90.5, 82.0, 95.25, 82.0],
                "passed": [True, True, True, True],
            }
        )

        schema = {
            "check_duplicates": True,
            "check_completeness": True,
            "columns": {
                "id": {"type": "int", "nullable": False},
                "name": {"type": "str", "nullable": False},
                "score": {"type": "float", "nullable": False, "min": 0, "max": 100},
                "passed": {"type": "bool", "nullable": False},
            },
        }

        new_validator = PolarsValidator(
            schema=schema, data=df, file="temp.html", format="html", hard_check=False
        )
        new_validator.validate()
        new_validator.export()

        assert isinstance(new_validator, PolarsValidator)
        assert len(new_validator.log) > 0
        assert os.path.exists("temp.html")

        # Clean up
        os.remove("temp.html")

    def test_polars_all_dtypes(self):
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 2],
                "name": ["Alice", "Bob", "Charlie", "Bob"],
                "score": [90.5, 82.0, 95.25, 82.0],
                "passed": [True, True, True, True],
            }
        )

        schema = {
            "check_duplicates": True,
            "check_completeness": True,
            "columns": {
                "id": {
                    "type": "int",
                    "allow_na": False,
                    "max_val": 2,
                    "min_val": 0,
                    "optional": False,
                },
                "name": {
                    "type": "str",
                    "allow_na": False,
                    "optional": False,
                    "min_length": 4,
                    "max_length": 10,
                },
                "score": {
                    "type": "float",
                    "allow_na": False,
                    "min_val": 0,
                    "max_val": 100,
                    "max_decimal": 5,
                    "min_decimal": 2,
                    "optional": False,
                },
                "passed": {"type": "bool", "allow_na": False, "optional": False},
            },
        }
        new_validator = PolarsValidator(
            schema=schema, data=df, file="temp.html", format="html", hard_check=False
        )
        new_validator.validate()
        new_validator.export()

        assert isinstance(new_validator, PolarsValidator)
        assert len(new_validator.log) > 0
        assert os.path.exists("temp.html")

        # Clean up
        os.remove("temp.html")
