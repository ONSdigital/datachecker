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

    @pytest.mark.xfail(
        reason="Decimal checks seem to be broken in pyspark, need to investigate further"
    )
    def test_pyspark_all_dtypes(self):
        import pyspark.sql.types as T

        from datachecker.data_checkers.pyspark_validator import PySparkValidator

        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 2],
                "name": ["Alice", "Bob", "@", "Bob"],
                "score": [90.5, 82.0, 95.2, 82.0],
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
                "id": {
                    "type": T.IntegerType(),
                    "allow_na": False,
                    "max_val": 2,
                    "min_val": 0,
                    "optional": False,
                },
                "name": {
                    "type": T.StringType(),
                    "allow_na": False,
                    "optional": False,
                    "min_length": 4,
                    "max_length": 10,
                    # allowed strings not working for pyspark with regex
                    "allowed_strings": ["Alice", "Bob", "Charlie"],
                },
                "score": {
                    "type": T.FloatType(),
                    "allow_na": False,
                    "min_val": 0,
                    "max_val": 95,
                    "max_decimal": 3,
                    "min_decimal": 2,
                    "optional": False,
                },
                "passed": {"type": T.BooleanType(), "allow_na": False, "optional": False},
            },
        }
        new_validator = PySparkValidator(
            schema=schema, data=spark_df, file="temp.html", format="html", hard_check=False
        )
        new_validator.validate()

        entries_with_fails = [
            entry for entry in new_validator.log[1:] if "fail" in str(entry["outcome"]).lower()
        ]

        assert len(new_validator.log) == 23
        # Decimal checks seem to be broken currently have 6 fails, expect
        # one extra for the decimal check
        assert len(entries_with_fails) == 7
        assert [entry["description"] for entry in entries_with_fails] == [
            "Checking id less than or equal to 2",
            "Checking name contains only ['Alice', 'Bob', 'Charlie']",
            "Checking name string length greater than or equal to 4",
            "Checking score less than or equal to 95",
            "Checking score has at least 2 decimal places",
            "Checking for duplicate rows in the dataframe",
            "Checking for missing rows in the dataframe columns: id, name, score, passed",
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
            entry for entry in new_validator.log[1:-1] if "fail" in str(entry["outcome"]).lower()
        ]
        assert len(entries_with_fails) == 1
        assert "Pyspark does not return cases or index" in str(
            entries_with_fails[0]["failing_ids"][0]
        )
