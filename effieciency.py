from time import perf_counter

import matplotlib.pyplot as plt
import pandas as pd
import polars as pl
import seaborn as sns

from onsdatachecker import DataValidator, PolarsValidator, PySparkValidator


def create_long_schema(region_count, measure_count):
    regions, sexes, measures = create_regions_sexes_measures(region_count, measure_count)

    long_schema = {
        "check_duplicates": False,
        "check_completeness": False,
        "columns": {
            "id": {
                "type": int,
                "min_val": 1,
                "allow_na": False,
                "optional": False,
            },
            "region": {
                "type": str,
                "allowed_values": regions,
                "allow_na": False,
                "optional": False,
            },
            "sex": {
                "type": str,
                "allowed_values": sexes,
                "allow_na": False,
                "optional": False,
            },
            "measure": {
                "type": str,
                "allowed_values": measures,
                "allow_na": False,
                "optional": False,
            },
            "count": {
                "type": int,
                "min_val": 0,
                "allow_na": False,
                "optional": False,
            },
        },
    }
    return long_schema


def create_long_dataframe(rows_per_group, measure_count, region_count):
    regions, sexes, measures = create_regions_sexes_measures(region_count, measure_count)
    long_dataframe = pd.DataFrame(
        [
            (region, sex, measure, count)
            for region in regions
            for sex in sexes
            for measure in measures
            for count in range(rows_per_group)
        ],
        columns=["region", "sex", "measure", "count"],
    )

    long_dataframe.insert(0, "id", range(1, len(long_dataframe) + 1))
    return long_dataframe


def create_regions_sexes_measures(region_count, measure_count):
    regions = [f"region_{index:03d}" for index in range(1, region_count + 1)]
    sexes = ["Female", "Male"]
    measures = [f"measure_{index:03d}" for index in range(1, measure_count + 1)]
    return regions, sexes, measures


def create_wide_schema(region_count, measure_count):
    regions, sexes, measures = create_regions_sexes_measures(region_count, measure_count)

    wide_schema = {
        "check_duplicates": False,
        "check_completeness": False,
        "columns": {
            "id": {
                "type": int,
                "min_val": 1,
                "allow_na": False,
                "optional": False,
            },
            "region": {
                "type": str,
                "allowed_values": regions,
                "allow_na": False,
                "optional": False,
            },
            "sex": {
                "type": str,
                "allowed_values": sexes,
                "allow_na": False,
                "optional": False,
            },
            **{
                measure: {
                    "type": int,
                    "min_val": 0,
                    "allow_na": False,
                    "optional": False,
                }
                for measure in measures
            },
        },
    }
    return wide_schema


def create_wide_dataframe(rows_per_group, measure_count, region_count):
    regions, sexes, measures = create_regions_sexes_measures(region_count, measure_count)
    wide_dataframe = pd.DataFrame(
        [
            {
                "region": region,
                "sex": sex,
                **{measure: observation for measure in measures},
            }
            for region in regions
            for sex in sexes
            for observation in range(rows_per_group)
        ],
        columns=["region", "sex"] + measures,
    )

    wide_dataframe.insert(0, "id", range(1, len(wide_dataframe) + 1))
    return wide_dataframe


def validate_and_time_dataframe(
    rows_per_group, measure_count, region_count, n_iterations, dataframe_library, dataframe_shape
):
    if dataframe_shape == "wide":
        df = create_wide_dataframe(rows_per_group, measure_count, region_count)
        schema = create_wide_schema(region_count, measure_count)
    elif dataframe_shape == "long":
        df = create_long_dataframe(rows_per_group, measure_count, region_count)
        schema = create_long_schema(region_count, measure_count)
    else:
        raise ValueError("dataframe_shape must be either 'wide' or 'long'")

    validator_class = DataValidator
    if dataframe_library == "polars":
        df = pl.from_pandas(df)
        validator_class = PolarsValidator
    elif dataframe_library == "pyspark":
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(df)
        validator_class = PySparkValidator
    elif dataframe_library != "pandas" and dataframe_library != "pyspark":
        raise ValueError("dataframe_library must be either 'pandas', 'polars', or 'pyspark'")

    start_time = perf_counter()
    for _ in range(n_iterations):
        validator_class(
            schema=schema, data=df, file=None, format="yaml", hard_check=False
        ).validate()
    elapsed = (perf_counter() - start_time) / n_iterations
    print(f"Dataframe shape ({dataframe_library}, {dataframe_shape}): {df.shape}")
    print(f"Average validation time: {elapsed:.6f} seconds")

    return elapsed


def run_test():
    REGION_COUNT = 100
    MEASURE_COUNT = 20
    ROWS_PER_GROUP_SIZES = [10, 50, 100, 250, 500, 750, 1000]
    n_iterations = int(1000)

    cases = [
        ("pandas", "wide"),
        ("pandas", "long"),
        ("polars", "wide"),
        ("polars", "long"),
    ]  # ("pyspark", "wide"), ("pyspark", "long")]
    runtime = {f"{library} {shape}": [] for library, shape in cases}

    for rows_per_group in ROWS_PER_GROUP_SIZES:
        for dataframe_library, dataframe_shape in cases:
            elapsed = validate_and_time_dataframe(
                rows_per_group,
                MEASURE_COUNT,
                REGION_COUNT,
                n_iterations,
                dataframe_library,
                dataframe_shape,
            )
            runtime[f"{dataframe_library} {dataframe_shape}"].append(elapsed)

    plt.figure(figsize=(10, 6))
    runtime_df = pd.DataFrame(runtime)
    runtime_df.to_csv("validation_runtime.csv")
    runtime_df["rows_per_group"] = ROWS_PER_GROUP_SIZES
    runtime_long = runtime_df.melt(id_vars="rows_per_group", var_name="case", value_name="elapsed")
    sns.barplot(data=runtime_long, x="rows_per_group", y="elapsed", hue="case")
    plt.xlabel("Rows per group")
    plt.ylabel(f"Average execution time over {n_iterations} iterations (seconds)")
    plt.title("Validation runtime by data shape and input size")
    plt.gca().text(
        0.02,
        0.98,
        f"Region count: {REGION_COUNT}\nMeasure count: {MEASURE_COUNT}\n Sex count: 2 \n Long format shape: ({REGION_COUNT * MEASURE_COUNT * 2}* rows_per_group, 5)\n Wide format shape: ({REGION_COUNT * 2} * rows_per_group, {MEASURE_COUNT + 3})",  # noqa: E501
        transform=plt.gca().transAxes,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
    )

    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig("validation_runtime.png")


if __name__ == "__main__":
    t0 = perf_counter()
    run_test()
    t1 = perf_counter()
    print(f"Total execution time: {t1 - t0:.6f} seconds")
