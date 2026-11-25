import statistics
from pathlib import Path
from time import perf_counter

import matplotlib.pyplot as plt
import pandas as pd
import polars as pl

from onsdatachecker import DataValidator, PolarsValidator, PySparkValidator

RESULTS_DIR = Path("efficiency-testing-results")


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


def create_extreme_wide_cases(min_measure_columns=401, region_count=10, max_rows_factor=20):
    measure_count = max(min_measure_columns, 420)
    libraries = ("pandas", "polars")

    return [
        {
            "library": library,
            "shape": "wide",
            "label": f"{library} wide extreme",
            "region_count": region_count,
            "measure_count": measure_count,
            "max_rows_factor": max_rows_factor,
        }
        for library in libraries
    ]


def normalize_test_case(test_case, default_region_count, default_measure_count):
    if isinstance(test_case, tuple):
        dataframe_library, dataframe_shape = test_case
        return {
            "library": dataframe_library,
            "shape": dataframe_shape,
            "label": f"{dataframe_library} {dataframe_shape}",
            "region_count": default_region_count,
            "measure_count": default_measure_count,
            "max_rows_factor": None,
        }

    dataframe_library = test_case["library"]
    dataframe_shape = test_case["shape"]
    return {
        "library": dataframe_library,
        "shape": dataframe_shape,
        "label": test_case.get("label", f"{dataframe_library} {dataframe_shape}"),
        "region_count": test_case.get("region_count", default_region_count),
        "measure_count": test_case.get("measure_count", default_measure_count),
        "max_rows_factor": test_case.get("max_rows_factor"),
    }


def get_row_count_factor(dataframe_shape, region_count, measure_count):
    if dataframe_shape == "wide":
        return region_count * 2
    return region_count * 2 * measure_count


def get_columns_checked(dataframe_shape, measure_count):
    if dataframe_shape == "wide":
        return measure_count + 3
    return 5


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


def resolve_plot_axis(x_axis_mode):
    x_axis_column = "rows_per_group"
    x_axis_label = "Rows per group"
    if x_axis_mode == "rows":
        x_axis_column = "row_count_used"
        x_axis_label = "Number of rows"
    elif x_axis_mode == "data_points":
        x_axis_column = "data_point_count_used"
        x_axis_label = "Number of data points"

    return x_axis_column, x_axis_label


def save_runtime_results(runtime_summary, runtime_iterations, output_stem, results_dir=RESULTS_DIR):
    results_dir = Path(results_dir)
    runtime_summary.to_csv(results_dir / f"{output_stem}.csv", index=False)
    runtime_iterations.to_csv(results_dir / f"{output_stem}_iterations.csv", index=False)


def plot_runtime_summary(
    runtime_summary,
    output_stem,
    plot_title,
    x_axis_mode="rows_per_group",
    plot_note="",
    results_dir=RESULTS_DIR,
):
    x_axis_column, x_axis_label = resolve_plot_axis(x_axis_mode)

    plt.figure(figsize=(10, 6))
    for case, case_df in runtime_summary.groupby("case"):
        case_df = case_df.sort_values(x_axis_column)
        lower_bound = (case_df["elapsed_mean"] - case_df["elapsed_std"]).clip(lower=0)
        upper_bound = case_df["elapsed_mean"] + case_df["elapsed_std"]

        plt.plot(case_df[x_axis_column], case_df["elapsed_mean"], marker="o", label=case)
        plt.fill_between(
            case_df[x_axis_column],
            lower_bound,
            upper_bound,
            alpha=0.2,
        )

    plt.xlabel(x_axis_label)
    plt.ylabel("Validation time (seconds)")
    plt.title(plot_title)

    if plot_note is None:
        plot_note = ""

    plt.gca().text(
        0.02,
        0.98,
        plot_note,
        transform=plt.gca().transAxes,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
    )

    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(Path(results_dir) / f"{output_stem}.png")


def load_runtime_results(output_stem, results_dir=RESULTS_DIR):
    results_dir = Path(results_dir)
    runtime_summary = pd.read_csv(results_dir / f"{output_stem}.csv")
    runtime_iterations = pd.read_csv(results_dir / f"{output_stem}_iterations.csv")
    return runtime_summary, runtime_iterations


def plot_runtime_from_saved_data(
    output_stem,
    plot_title,
    x_axis_mode="rows_per_group",
    plot_note="",
    results_dir=RESULTS_DIR,
):
    runtime_summary, _ = load_runtime_results(output_stem, results_dir=results_dir)
    plot_runtime_summary(
        runtime_summary=runtime_summary,
        output_stem=output_stem,
        plot_title=plot_title,
        x_axis_mode=x_axis_mode,
        plot_note=plot_note,
        results_dir=results_dir,
    )


def rerender_saved_plots(plot_jobs, results_dir=RESULTS_DIR):
    for plot_job in plot_jobs:
        output_stem = plot_job["output_stem"]
        print(f"Rebuilding plot from saved CSVs: {output_stem}")
        plot_runtime_from_saved_data(
            output_stem=output_stem,
            plot_title=plot_job["plot_title"],
            x_axis_mode=plot_job.get("x_axis_mode", "rows_per_group"),
            plot_note=plot_job.get("plot_note", ""),
            results_dir=results_dir,
        )


def run_test(config, output_stem="validation_runtime"):
    region_count = config.get("region_count")
    measure_count = config.get("measure_count")
    rows_per_group_sizes = config.get("rows_per_group_sizes")
    n_iterations = int(config.get("n_iterations"))
    normalized_test_cases = [
        normalize_test_case(test_case, region_count, measure_count)
        for test_case in config.get("test_cases")
    ]
    plot_title = config.get(
        "plot_title",
        f"Validation runtime by data shape and input size\n"
        f"Line = mean, shaded band = +/- 1 SD over {n_iterations} iterations",
    )
    plot_note = config.get("plot_note")
    x_axis_mode = config.get("x_axis_mode", "rows_per_group")
    include_columns_in_legend = config.get("include_columns_in_legend", False)

    summary_rows = []
    iteration_rows = []

    for requested_rows_per_group in rows_per_group_sizes:
        for test_case in normalized_test_cases:
            dataframe_library = test_case["library"]
            dataframe_shape = test_case["shape"]
            case_region_count = test_case["region_count"]
            case_measure_count = test_case["measure_count"]
            rows_per_group = requested_rows_per_group
            row_count_factor = get_row_count_factor(
                dataframe_shape, case_region_count, case_measure_count
            )
            row_count_requested = requested_rows_per_group * row_count_factor
            max_rows = None

            if test_case["max_rows_factor"] is not None:
                max_rows = requested_rows_per_group * test_case["max_rows_factor"]
                max_rows_per_group = max_rows // row_count_factor
                if max_rows_per_group < 1:
                    raise ValueError("Derived max_rows is too small for this case")
                rows_per_group = min(requested_rows_per_group, max_rows_per_group)

            row_count_used = rows_per_group * row_count_factor
            data_point_count_requested, _ = count_measure_data_points(
                case_region_count, case_measure_count, requested_rows_per_group
            )
            data_point_count_used, _ = count_measure_data_points(
                case_region_count, case_measure_count, rows_per_group
            )

            print_data_point_parity(case_region_count, case_measure_count, rows_per_group)
            result = validate_and_time_dataframe(
                rows_per_group,
                case_measure_count,
                case_region_count,
                n_iterations,
                dataframe_library,
                dataframe_shape,
            )
            case_name = test_case["label"]
            if include_columns_in_legend:
                columns_checked = get_columns_checked(dataframe_shape, case_measure_count)
                case_name = f"{case_name} ({columns_checked} columns)"

            summary_rows.append(
                {
                    "rows_per_group": requested_rows_per_group,
                    "rows_per_group_used": rows_per_group,
                    "row_count_requested": row_count_requested,
                    "row_count_used": row_count_used,
                    "data_point_count_requested": data_point_count_requested,
                    "data_point_count_used": data_point_count_used,
                    "max_rows": max_rows,
                    "case": case_name,
                    "elapsed_mean": result["mean"],
                    "elapsed_std": result["std"],
                    "region_count": case_region_count,
                    "measure_count": case_measure_count,
                }
            )

            for index, elapsed in enumerate(result["timings"], start=1):
                iteration_rows.append(
                    {
                        "rows_per_group": requested_rows_per_group,
                        "rows_per_group_used": rows_per_group,
                        "row_count_requested": row_count_requested,
                        "row_count_used": row_count_used,
                        "data_point_count_requested": data_point_count_requested,
                        "data_point_count_used": data_point_count_used,
                        "max_rows": max_rows,
                        "case": case_name,
                        "iteration": index,
                        "elapsed": elapsed,
                    }
                )

    runtime_summary = pd.DataFrame(summary_rows)
    runtime_iterations = pd.DataFrame(iteration_rows)

    save_runtime_results(runtime_summary, runtime_iterations, output_stem)
    plot_runtime_summary(
        runtime_summary=runtime_summary,
        output_stem=output_stem,
        plot_title=plot_title,
        x_axis_mode=x_axis_mode,
        plot_note=plot_note,
    )

    return runtime_summary, runtime_iterations


if __name__ == "__main__":
    t0 = perf_counter()

    # Set RUN_MODE to "plot_from_csv" to regenerate PNGs from existing CSV outputs.
    RUN_MODE = "plot_from_csv"

    saved_plot_jobs = [
        {
            "output_stem": "validation_runtime_baseline",
            "plot_title": "Baseline validation runtime\nLine = mean, shaded band = +/- 1 SD",  # noqa: E501
            "x_axis_mode": "data_points",
            "plot_note": "",
        },
        {
            "output_stem": "validation_runtime_extreme_wide",
            "plot_title": "Extreme-wide validation runtime (pandas vs polars)\nLine = mean, shaded band = +/- 1 SD",  # noqa: E501
            "x_axis_mode": "data_points",
            "plot_note": "",
        },
    ]

    if RUN_MODE == "plot_from_csv":
        rerender_saved_plots(saved_plot_jobs)
        t1 = perf_counter()
        print(f"Plot rebuild execution time: {t1 - t0:.6f} seconds")
        raise SystemExit(0)

    rows_per_group_sizes = [10, 50, 100, 250, 500, 750, 1000]
    extreme_wide_cases = create_extreme_wide_cases(min_measure_columns=501)

    baseline_test_config = {
        "region_count": 100,
        "measure_count": 20,
        "rows_per_group_sizes": rows_per_group_sizes,
        "n_iterations": int(100),
        "test_cases": [
            ("pandas", "wide"),
            ("pandas", "long"),
            ("polars", "wide"),
            ("polars", "long"),
        ],
        "x_axis_mode": "data_points",
        "plot_title": "Baseline validation runtime\nLine = mean, shaded band = +/- 1 SD",
        "plot_note": "",
    }

    extreme_wide_test_config = {
        "region_count": 100,
        "measure_count": 20,
        "rows_per_group_sizes": rows_per_group_sizes,
        "n_iterations": int(100),
        "test_cases": extreme_wide_cases,
        "plot_title": "Extreme-wide validation runtime (pandas vs polars)\nLine = mean, shaded band = +/- 1 SD",  # noqa: E501
        "x_axis_mode": "data_points",
        "include_columns_in_legend": True,
        "plot_note": (""),
    }

    print("\nRunning baseline test config...")
    run_test(baseline_test_config, output_stem="validation_runtime_baseline")

    print("\nRunning extreme-wide pandas vs polars test config...")
    run_test(extreme_wide_test_config, output_stem="validation_runtime_extreme_wide")

    t1 = perf_counter()
    print(f"Total execution time: {t1 - t0:.6f} seconds")


# todo:
# Compare performance between long and wide formats for each library
