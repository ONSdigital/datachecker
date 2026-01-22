import pandas as pd
import pytest

from datachecker.data_checkers.pandas_validator import DataValidator


@pytest.fixture(scope="class")
def df():
    return pd.DataFrame(
        {
            "valid_column": [1, 2, 3],
            "second_valid_column": [4, 5, 6],
        }
    )


class TestCheckColnames:
    def test__check_colnames_fail(self):
        # Create a sample dataframe
        data = pd.DataFrame(
            {"valid_column": [1, 2, 3], "Invalid Column": [4, 5, 6], "invalid-column": [7, 8, 9]}
        )

        # Define a simple schema
        schema = {
            "columns": {
                "valid_column": {"type": "integer"},
                "Invalid Column": {"type": "integer", "optional": True},
                "invalid-column": {"type": "integer", "optional": True},
            }
        }

        # Create a DataValidator instance
        test_validator = DataValidator(schema=schema, data=data, file=None, format=None)

        # Run the validation
        test_validator._check_colnames()

        # Check the log for expected entries
        # skip first entry as this is system info
        log = test_validator.log[1:]
        qa_entry_column_check = [
            entry for entry in log if entry["description"] == "Checking column names"
        ]
        assert qa_entry_column_check[0]["outcome"] == "fail"
        assert set(["Invalid Column", "invalid-column"]).issubset(
            set(qa_entry_column_check[0]["failing_ids"])
        )

    def test__check_colnames_pass(self, df):
        # Define a simple schema
        schema = {
            "columns": {
                "valid_column": {"type": "integer"},
                "second_valid_column": {"type": "integer", "optional": True},
            }
        }

        # Create a DataValidator instance
        test_validator = DataValidator(schema=schema, data=df, file=None, format=None)

        # Run the validation
        test_validator._check_colnames()

        # Check the log for expected entries
        # skip first entry as this is system info
        log = test_validator.log[1:]
        qa_entry_column_check = [
            entry for entry in log if entry["description"] == "Checking column names"
        ]
        assert qa_entry_column_check[0]["outcome"] == "pass"

    def test__check_colnames_mandatory_fail(self, df):
        # Define a simple schema with a mandatory column missing in data
        schema = {
            "columns": {
                "valid_column": {"type": "integer"},
                "second_valid_column": {"type": "integer", "optional": True},
                "mandatory_column": {"type": "string", "optional": False},
            }
        }

        # Create a DataValidator instance
        test_validator = DataValidator(schema=schema, data=df, file=None, format=None)

        # Run the validation
        test_validator._check_colnames()

        # Check the log for expected entries
        # skip first entry as this is system info
        log = test_validator.log[1:]
        qa_entry_mandatory_check = [
            entry
            for entry in log
            if entry["description"] == "Checking mandatory columns are present"
        ]
        assert qa_entry_mandatory_check[0]["outcome"] == "fail"
        assert "mandatory_column" in qa_entry_mandatory_check[0]["failing_ids"]

    def test__check_colnames_unexpected_col_fail(self, df):
        # Define a simple schema with a mandatory column missing in data
        # second_valid_column is unexpected
        schema = {
            "columns": {
                "valid_column": {"type": "integer"},
            }
        }

        # Create a DataValidator instance
        test_validator = DataValidator(schema=schema, data=df, file=None, format=None)

        # Run the validation
        test_validator._check_colnames()

        # Check the log for expected entries
        # skip first entry as this is system info
        log = test_validator.log[1:]
        qa_entry_unexpected_check = [
            entry for entry in log if entry["description"] == "Checking for unexpected columns"
        ]
        assert qa_entry_unexpected_check[0]["outcome"] == "fail"
        assert "second_valid_column" in qa_entry_unexpected_check[0]["failing_ids"]
