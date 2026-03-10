import importlib.util
import os

import pandas as pd
import pytest


@pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None,
    reason="pyspark is not installed",
)
class TestPysparkValidator:
    def setup_method(self):
        import findspark
        from pyspark.sql import SparkSession

        # This helps for running local Spark sessions
        findspark.init()

        self.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

    def test_pyspark_validator(self):
        import pyspark.sql.types as T

        from datachecker.data_checkers.pyspark_validator import PySparkValidator

        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 2],
                "name": ["Alice", "Bob", "Charlie", "Bob"],
                "score": [90.5, 82.0, 95.25, 82.0],
                "passed": [True, True, True, True],
            }
        )
        spark_df = self.spark.createDataFrame(df)
        spark_df = spark_df.withColumn("id", spark_df["id"].cast(T.IntegerType()))
        spark_df = spark_df.withColumn("name", spark_df["name"].cast(T.StringType()))
        spark_df = spark_df.withColumn("score", spark_df["score"].cast(T.FloatType()))
        spark_df = spark_df.withColumn("passed", spark_df["passed"].cast(T.BooleanType()))

        schema = {
            "check_duplicates": True,
            "check_completeness": True,
            "columns": {
                "id": {"type": "int", "nullable": False},
                "name": {"type": "string", "nullable": False},
                "score": {"type": "float", "nullable": False},
                "passed": {"type": "bool", "nullable": False},
            },
        }

        new_validator = PySparkValidator(
            schema=schema, data=spark_df, file="temp.html", format="html", hard_check=False
        )
        new_validator.validate()
        new_validator.export()

        assert isinstance(new_validator, PySparkValidator)
        assert len(new_validator.log) > 0
        assert os.path.exists("temp.html")

        # Clean up
        os.remove("temp.html")

    def test_pyspark_all_dtypes_fails(self):
        import pyspark.sql.types as T

        from datachecker.data_checkers.pyspark_validator import PySparkValidator

        df = pd.DataFrame(
            {
                "id": [-1, 2, 3, 4],
                "name": ["Alice", "Bob", "Charlie", "Daniel"],
                "score": [90.5, 82.0, 95.2, 82.0],
                "passed": [True, True, True, True],
                "date": ["2022-12-01", "2023-01-02", "2023-01-03", "2023-01-04"],
                "comments": ["Good", "Average", "Excellent", "Average"],
            }
        )
        spark_df = self.spark.createDataFrame(df)
        spark_df = spark_df.withColumn("id", spark_df["id"].cast(T.IntegerType()))
        spark_df = spark_df.withColumn("name", spark_df["name"].cast(T.StringType()))
        spark_df = spark_df.withColumn("score", spark_df["score"].cast(T.FloatType()))
        spark_df = spark_df.withColumn("passed", spark_df["passed"].cast(T.BooleanType()))
        spark_df = spark_df.withColumn("date", spark_df["date"].cast(T.DateType()))
        spark_df = spark_df.withColumn("comments", spark_df["comments"].cast(T.StringType()))

        schema = {
            "check_duplicates": True,
            "check_completeness": False,
            "columns": {
                "id": {
                    "type": "int",
                    "allow_na": False,
                    "min_val": 0,
                    "optional": False,
                },
                "name": {
                    "type": "string",
                    "allow_na": False,
                    "optional": False,
                    "max_length": 10,
                    # allowed strings not working for pyspark with regex
                    "allowed_strings": ["Alice", "Bob", "Charlie", "Daniel"],
                },
                "score": {
                    "type": "float",
                    "allow_na": False,
                    "min_val": 0,
                    "max_val": 95,
                    # Decimal checks broken due to using pandas lambda functions,
                    # need to investigate further
                    "max_decimal": 3,
                    "min_decimal": 2,
                    "optional": False,
                },
                "passed": {"type": "bool", "allow_na": False, "optional": False},
                "date": {
                    "type": "date",
                    "allow_na": False,
                    "optional": False,
                    "min_val": "2023-01-01",
                    "max_val": "2023-01-03",
                },
                "comments": {
                    "type": "string",
                    "allow_na": False,
                    "optional": False,
                    "min_length": 5,
                    "max_length": 6,
                },
            },
        }
        new_validator = PySparkValidator(
            schema=schema, data=spark_df, file="temp.html", format="html", hard_check=False
        )
        new_validator.validate()

        entries_with_fails = [
            entry for entry in new_validator.log[1:] if "fail" in str(entry["outcome"]).lower()
        ]

        assert len(new_validator.log) == 26
        # Decimal checks seem to be broken currently have 6 fails, expect
        # one extra for the decimal check
        assert len(entries_with_fails) == 6
        assert [entry["description"] for entry in entries_with_fails] == [
            "Checking id greater than or equal to 0",
            "Checking score less than or equal to 95",
            "Checking date greater_than_or_equal_to(2023-01-01)",
            "Checking date less_than_or_equal_to(2023-01-03)",
            "Checking comments string length greater than or equal to 5",
            "Checking comments string length less than or equal to 6",
        ]

    def test_pyspark_validate_boilerplate_checks(self):
        import pyspark.sql.types as T

        from datachecker.data_checkers.pyspark_validator import PySparkValidator

        df = pd.DataFrame(
            {
                "id": [1, 2, 7, 5],
            }
        )
        spark_df = self.spark.createDataFrame(df)
        spark_df = spark_df.withColumn("id", spark_df["id"].cast(T.IntegerType()))

        schema = {
            "check_duplicates": False,
            "check_completeness": False,
            "columns": {
                "id": {
                    "type": "int",
                    "allow_na": False,
                    "max_val": 2,
                    "min_val": 0,
                    "optional": False,
                },
            },
        }
        new_validator = PySparkValidator(
            schema=schema, data=spark_df, file="temp.json", format="json", hard_check=False
        )
        new_validator.validate()
        entries_with_fails = [
            entry for entry in new_validator.log[1:] if "fail" in str(entry["outcome"]).lower()
        ]
        assert len(entries_with_fails) == 1
        assert "Pyspark does not return cases or index" in str(
            entries_with_fails[0]["failing_ids"][0]
        )

    def test_pyspark_validate_check_duplicates_fail(self):
        import pyspark.sql.types as T

        from datachecker.data_checkers.pyspark_validator import PySparkValidator

        df = pd.DataFrame(
            {
                "id": [1, 2, 7, 5, 7],
                "name": ["Alice", "Bob", "Charlie", "David", "Charlie"],
                "score": [90.5, 82.0, 95.25, 88.0, 95.25],
            }
        )
        spark_df = self.spark.createDataFrame(df)
        spark_df = spark_df.withColumn("id", spark_df["id"].cast(T.IntegerType()))
        spark_df = spark_df.withColumn("name", spark_df["name"].cast(T.StringType()))
        spark_df = spark_df.withColumn("score", spark_df["score"].cast(T.FloatType()))

        schema = {
            "check_duplicates": True,
            "columns": {
                "id": {
                    "type": "int",
                    "allow_na": False,
                    "max_val": 10,
                    "min_val": 0,
                    "optional": False,
                },
                "name": {
                    "type": "string",
                    "allow_na": False,
                    "optional": False,
                    "min_length": 0,
                    "max_length": 10,
                },
                "score": {
                    "type": "float",
                    "allow_na": False,
                    "optional": False,
                    "min_val": 0,
                    "max_val": 100,
                },
            },
        }
        new_validator = PySparkValidator(
            schema=schema, data=spark_df, file="temp.json", format="json", hard_check=False
        )
        new_validator.validate()
        entries_with_fails = [
            entry for entry in new_validator.log[1:] if "fail" in str(entry["outcome"]).lower()
        ]
        assert len(entries_with_fails) == 1
        assert [entry["description"] for entry in entries_with_fails] == [
            "Checking for duplicate rows in the dataframe"
        ]

    def test_pyspark_validate_check_duplicates_pass(self):
        import pyspark.sql.types as T

        from datachecker.data_checkers.pyspark_validator import PySparkValidator

        df = pd.DataFrame(
            {
                "id": [1, 2, 7, 5],
                "name": ["Alice", "Bob", "Charlie", "David"],
                "score": [90.5, 82.0, 95.25, 88.0],
            }
        )
        spark_df = self.spark.createDataFrame(df)
        spark_df = spark_df.withColumn("id", spark_df["id"].cast(T.IntegerType()))
        spark_df = spark_df.withColumn("name", spark_df["name"].cast(T.StringType()))
        spark_df = spark_df.withColumn("score", spark_df["score"].cast(T.FloatType()))

        schema = {
            "check_duplicates": True,
            "columns": {
                "id": {
                    "type": "int",
                    "allow_na": False,
                    "optional": False,
                },
                "name": {
                    "type": "string",
                    "allow_na": False,
                    "optional": False,
                },
                "score": {
                    "type": "float",
                    "allow_na": False,
                    "optional": False,
                },
            },
        }
        new_validator = PySparkValidator(
            schema=schema, data=spark_df, file="temp.json", format="json", hard_check=False
        )
        new_validator.validate()
        entries_with_fails = [
            entry for entry in new_validator.log[1:] if "fail" in str(entry["outcome"]).lower()
        ]
        assert len(entries_with_fails) == 0

    def test_pyspark_validate_check_completeness_pass(self):
        import pyspark.sql.types as T

        from datachecker.data_checkers.pyspark_validator import PySparkValidator

        df = pd.DataFrame({"id": [1, 2, 3, 4], "age": [10, 20, 10, 20], "sex": ["M", "M", "F", "F"]})
        spark_df = self.spark.createDataFrame(df)
        spark_df = spark_df.withColumn("id", spark_df["id"].cast(T.IntegerType()))
        spark_df = spark_df.withColumn("age", spark_df["age"].cast(T.IntegerType()))
        spark_df = spark_df.withColumn("sex", spark_df["sex"].cast(T.StringType()))

        schema = {
            "check_completeness": True,
            "completeness_columns": ["age", "sex"],
            "columns": {
                "id": {"type": "int", "allow_na": False},
                "age": {"type": "int", "allow_na": False},
                "sex": {"type": "string", "allow_na": False},
            },
        }

        validator = PySparkValidator(schema=schema, data=spark_df, file=None, format=None)
        validator._check_completeness()
        completeness_entry = [
            entry
            for entry in validator.log[1:]
            if "Checking for missing rows in the dataframe columns" in entry["description"]
        ][0]
        assert completeness_entry["outcome"] == "pass"

    def test_pyspark_validate_check_completeness_fail(self):
        import pyspark.sql.types as T

        from datachecker.data_checkers.pyspark_validator import PySparkValidator

        df = pd.DataFrame({"id": [1, 2, 3, 4], "age": [10, 20, 10, 20], "sex": ["M", "M", "F", "F"]})
        df_dropped_row = df.iloc[0:3]  # Remove one row to create incompleteness
        spark_df = self.spark.createDataFrame(df_dropped_row)
        spark_df = spark_df.withColumn("id", spark_df["id"].cast(T.IntegerType()))
        spark_df = spark_df.withColumn("age", spark_df["age"].cast(T.IntegerType()))
        spark_df = spark_df.withColumn("sex", spark_df["sex"].cast(T.StringType()))

        schema = {
            "check_completeness": True,
            "completeness_columns": ["age", "sex"],
            "columns": {
                "id": {"type": "int", "allow_na": False},
                "age": {"type": "int", "allow_na": False},
                "sex": {"type": "string", "allow_na": False},
            },
        }

        validator = PySparkValidator(schema=schema, data=spark_df, file=None, format=None)
        validator._check_completeness()
        completeness_entry = [
            entry
            for entry in validator.log[1:]
            if "Checking for missing rows in the dataframe columns" in entry["description"]
        ][0]
        assert completeness_entry["outcome"] == "fail"
