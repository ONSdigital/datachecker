import pandas as pd

from datachecker import DataValidator


class TestCheckDuplicates:
    def setup_method(self):
        self.df = pd.DataFrame(
            {"id": [1, 2, 3, 2], "age": [25, 30, 22, 30], "sex": ["M", "F", "M", "F"]}
        )

        self.schema = {
            "check_duplicates": True,
            "columns": {
                "id": {"type": "integer", "allow_na": False},
                "age": {"type": "float", "allow_na": False},
                "sex": {"type": "string", "allow_na": False},
            },
        }

    def test__check_duplicates_not_found(self):
        df_no_dupe = self.df[0:3]  # Remove the duplicate row
        validator = DataValidator(schema=self.schema, data=df_no_dupe, file=None, format=None)
        validator._check_duplicates()
        dupe_log_entry = [
            entry
            for entry in validator.log[1:]
            if "Checking for duplicate rows in the dataframe" in entry["description"]
        ][0]
        assert dupe_log_entry["outcome"] == "pass"

    def test__check_duplicates_found(self):
        validator = DataValidator(schema=self.schema, data=self.df, file=None, format=None)
        validator._check_duplicates()
        dupe_log_entry = [
            entry
            for entry in validator.log[1:]
            if "Checking for duplicate rows in the dataframe" in entry["description"]
        ][0]
        assert dupe_log_entry["outcome"] == "fail"
