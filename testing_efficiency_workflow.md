# Testing Efficiency in Spark vs Pandas

The examples below generate equivalent large datasets and validator schemas for
an efficiency comparison. With the default settings, the long dataframe has
1,000,000 rows and four columns; the wide dataframe has 10,000 rows and 102
columns. Both contain 1,000,000 `count` values.

```python
import numpy as np
import pandas as pd

REGION_COUNT = 500
MEASURE_COUNT = 100
ROWS_PER_GROUP = 10

regions = [f"region_{index:03d}" for index in range(1, REGION_COUNT + 1)]
sexes = ["Female", "Male"]
measures = [f"measure_{index:03d}" for index in range(1, MEASURE_COUNT + 1)]
```

## Long Dataframe Schema

The long dataframe has one row for each region, sex, measure, and observation.
It has many rows but a fixed set of columns.

```python
long_schema = {
	"check_duplicates": False,
	"check_completeness": False,
	"columns": {
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

long_dataframe = pd.DataFrame(
	[
		(region, sex, measure, count)
		for region in regions
		for sex in sexes
		for measure in measures
		for count in range(ROWS_PER_GROUP)
	],
	columns=["region", "sex", "measure", "count"],
)
```

## Wide Dataframe Schema

The wide dataframe represents the same values with one column for every
measure. Its generated schema has 100 `measure_*` columns by default.

```python
wide_schema = {
	"check_duplicates": False,
	"check_completeness": False,
	"columns": {
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

wide_dataframe = pd.DataFrame(
	[
		{
			"region": region,
			"sex": sex,
			**{
				measure: observation
				for measure in measures
			},
		}
		for region in regions
		for sex in sexes
		for observation in range(ROWS_PER_GROUP)
	]
)
```

Increase `REGION_COUNT`, `MEASURE_COUNT`, or `ROWS_PER_GROUP` to benchmark a
larger validation workload. The schemas can be passed directly to
`DataValidator`; use `yaml.safe_dump()` if you need to save either generated
schema as YAML.