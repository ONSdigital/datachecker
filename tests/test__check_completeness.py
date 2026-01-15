import pandas as pd

from datachecker import DataValidator


class TestCheckCompleteness:
    def setup_method(self):
        self.df = pd.DataFrame(
            {"id": [1, 2, 3, 4], "age": [10, 20, 10, 20], "sex": ["M", "M", "F", "F"]}
        )

        self.schema = {
            "check_completeness": True,
            "completeness_columns": ["age", "sex"],
            "columns": {
                "id": {"type": "integer", "allow_na": False},
                "age": {"type": "float", "allow_na": False},
                "sex": {"type": "string", "allow_na": False},
            },
        }

    def test__check_completeness_pass(self):
        validator = DataValidator(schema=self.schema, data=self.df, file=None, format=None)
        validator._check_completeness()
        completeness_entry = [
            entry
            for entry in validator.log[1:]
            if "Checking for missing rows in the dataframe columns" in entry["description"]
        ][0]
        assert completeness_entry["outcome"] == "pass"

    def test__check_completeness_fail(self):
        df_dropped_row = self.df.iloc[0:3]  # Remove one row to create incompleteness
        validator = DataValidator(schema=self.schema, data=df_dropped_row, file=None, format=None)
        validator._check_completeness()
        completeness_entry = [
            entry
            for entry in validator.log[1:]
            if "Checking for missing rows in the dataframe columns" in entry["description"]
        ][0]
        assert completeness_entry["outcome"] == "fail"

    def test__check_completeness_default(self):
        removed_entry = self.schema.copy()
        removed_entry.pop("check_completeness")
        validator = DataValidator(schema=removed_entry, data=self.df, file=None, format=None)
        validator._check_duplicates()
        dupe_log_entry = [
            entry
            for entry in validator.log[1:]
            if "Checking for duplicate rows in the dataframe" in entry["description"]
        ]
        assert dupe_log_entry == []
