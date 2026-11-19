import statistics
from time import perf_counter

import matplotlib.pyplot as plt
import pandas as pd
import polars as pl

from onsdatachecker import DataValidator, PolarsValidator, PySparkValidator


def count_measure_data_points(region_count, measure_count, rows_per_group):
    sex_count = 2
    long_points = region_count * sex_count * measure_count * rows_per_group
    wide_points = region_count * sex_count * rows_per_group * measure_count
    return long_points, wide_points


def print_data_point_parity(region_count, measure_count, rows_per_group):
    long_points, wide_points = count_measure_data_points(region_count, measure_count, rows_per_group)
    print(
        "Data-point parity check "
        f"(rows_per_group={rows_per_group}, region_count={region_count}, measure_count={measure_count}): "  # noqa: E501
        f"long={long_points}, wide={wide_points}, equal={long_points == wide_points}"
    )


def create_extreme_wide_config(max_rows=10000, min_measure_columns=401):
    region_count = 10
    rows_per_group = max_rows // (region_count * 2)

    if rows_per_group < 1:
        raise ValueError("max_rows is too small for the selected region_count")

    return {
        "region_count": region_count,
        "measure_count": max(min_measure_columns, 420),
        "rows_per_group_sizes": [rows_per_group],
        "n_iterations": int(5),
        "test_cases": [
            ("pandas", "wide"),
            ("polars", "wide"),
        ],
    }


def create_long_schema(region_count, measure_count):
    regions, sexes, measures = create_regions_sexes_measures(region_count, measure_count)

    long_schema = {
        "check_duplicates": True,
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
        "check_duplicates": True,
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

    timings = []
    for _ in range(n_iterations):
        iteration_start = perf_counter()
        validator_class(
            schema=schema, data=df, file=None, format="yaml", hard_check=False
        ).validate()
        timings.append(perf_counter() - iteration_start)

    elapsed_mean = statistics.fmean(timings)
    elapsed_std = statistics.stdev(timings) if len(timings) > 1 else 0.0
    print(f"Dataframe shape ({dataframe_library}, {dataframe_shape}): {df.shape}")
    print(f"Validation time: mean={elapsed_mean:.6f}s, std={elapsed_std:.6f}s (n={n_iterations})")

    return {
        "mean": elapsed_mean,
        "std": elapsed_std,
        "timings": timings,
    }


def run_test(config, output_stem="validation_runtime"):
    region_count = config.get("region_count")
    measure_count = config.get("measure_count")
    rows_per_group_sizes = config.get("rows_per_group_sizes")
    n_iterations = int(config.get("n_iterations"))

    summary_rows = []
    iteration_rows = []

    for rows_per_group in rows_per_group_sizes:
        print_data_point_parity(region_count, measure_count, rows_per_group)
        for dataframe_library, dataframe_shape in config.get("test_cases"):
            result = validate_and_time_dataframe(
                rows_per_group,
                measure_count,
                region_count,
                n_iterations,
                dataframe_library,
                dataframe_shape,
            )
            case_name = f"{dataframe_library} {dataframe_shape}"

            summary_rows.append(
                {
                    "rows_per_group": rows_per_group,
                    "case": case_name,
                    "elapsed_mean": result["mean"],
                    "elapsed_std": result["std"],
                }
            )

            for index, elapsed in enumerate(result["timings"], start=1):
                iteration_rows.append(
                    {
                        "rows_per_group": rows_per_group,
                        "case": case_name,
                        "iteration": index,
                        "elapsed": elapsed,
                    }
                )

    runtime_summary = pd.DataFrame(summary_rows)
    runtime_iterations = pd.DataFrame(iteration_rows)

    runtime_summary.to_csv(f"efficiency-testing-results/{output_stem}.csv", index=False)
    runtime_iterations.to_csv(
        f"efficiency-testing-results/{output_stem}_iterations.csv", index=False
    )

    plt.figure(figsize=(10, 6))
    for case, case_df in runtime_summary.groupby("case"):
        case_df = case_df.sort_values("rows_per_group")
        lower_bound = (case_df["elapsed_mean"] - case_df["elapsed_std"]).clip(lower=0)
        upper_bound = case_df["elapsed_mean"] + case_df["elapsed_std"]

        plt.plot(case_df["rows_per_group"], case_df["elapsed_mean"], marker="o", label=case)
        plt.fill_between(
            case_df["rows_per_group"],
            lower_bound,
            upper_bound,
            alpha=0.2,
        )

    plt.xlabel("Rows per group")
    plt.ylabel("Validation time (seconds)")
    plt.title(
        f"Validation runtime by data shape and input size\n"
        f"Line = mean, shaded band = +/- 1 SD over {n_iterations} iterations"
    )
    plt.gca().text(
        0.02,
        0.98,
        f"Region count: {region_count}\nMeasure count: {measure_count}\n Sex count: 2 \n Long format shape: ({region_count * measure_count * 2}* rows_per_group, 5)\n Wide format shape: ({region_count * 2} * rows_per_group, {measure_count + 3})",  # noqa: E501
        transform=plt.gca().transAxes,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
    )

    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"efficiency-testing-results/{output_stem}.png")


if __name__ == "__main__":
    t0 = perf_counter()
    test_config = {
        "region_count": 100,
        "measure_count": 20,
        "rows_per_group_sizes": [10, 50, 100, 250],  # 500, 750, 1000],
        "n_iterations": int(10),
        "test_cases": [
            ("pandas", "wide"),
            ("pandas", "long"),
            ("polars", "wide"),
            ("polars", "long"),
        ],
    }

    extreme_wide_config = create_extreme_wide_config(max_rows=10000, min_measure_columns=401)

    print("\nRunning baseline test config...")
    run_test(test_config, output_stem="validation_runtime")

    print("\nRunning extreme wide test config (400+ columns, max 10000 rows)...")
    run_test(extreme_wide_config, output_stem="validation_runtime_extreme_wide")

    t1 = perf_counter()
    print(f"Total execution time: {t1 - t0:.6f} seconds")


# todo:
# Standard deviation of execution times for each test case
# Compare performance between long and wide formats for each library
