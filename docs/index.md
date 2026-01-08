# Datachecker

## Quickstart 

```Bash
pip install datachecker
```
create a schema for your dataset (either in python or read in the supported formats).
Load in your dataset.
Create a new validator object using 

```python
from datachecker import check_and_export
validator = check_and_export(
    df=dataframe,
    schema="path/to_schema.json",
    output="html",
    output_name="my_log"
)
```
This will then directly validate your dataset and produce a log. If your schema is not the correct format or missing some key values, Python errors will be given.


## Pre-Defined Checks

These checks can be included in the lists for individual columns in your schema, depending on the data type.

| Data Type        | Check Name             | Parameter         | Check Definition                                                                                                                           |
|---------------|---------------|---------------|-----------------------------|
| integer / double | Minimum value          | min_val           | Checks that all values are above or equal to the minimum value                                                                             |
| integer / double | Maximum value          | max_val           | Checks that all values are below or equal to the maximum value                                                                             |
| character        | Minimum length         | min_length        | Checks that all strings have length are above or equal to the minimum length                                                               |
| character        | Maximum length         | max_length        | Checks that all strings have length below or equal to the maximum length                                                                   |
| character        | allowed strings        | allowed_strings   | Validates that entries match a set of permitted values, list or regex can be used. (Optional and can use forbidden strings instead)        |
| any              | Missing values check   | allow_na          | Checks for missing or NA values in the column.                                                                                             |
| double           | Minimum decimal places | min_decimal       | Checks that all values have more or equal amounts of decimal places                                                                        |
| double           | Maximum decimal places | max_decimal       | Checks that all values have less or equal amounts of decimal places                                                                        |
| character        | forbidden strings      | forbidden_strings | Validates that entries do not contain a set of forbidden values, list can be used. (Optional and can use allowed strings instead. Does not support regex to use regex we recommend using allowed_characters. A TypeError message will be provided with further details) |
| date / datetime  | Minimum Date           | min_date          | Checks that all dates are after the minimum date using the format “YYYY-MM-DD”                                                             |
| date / datetime  | Maximum Date           | max_date          | Checks that all dates are before the maximum date using the format “YYYY-MM-DD”                                                            |
| date/ datetime   | Minimum Datetime       | min_datetime      | Checks that all dates are after the minimum datetime. Accepted formats: Y, YM, YMD, YMDH, YMDHM and YMDHMS                                 |
| date/ datetime   | Maximum Datetime       | max_datetime      | Checks that all dates are before the maximum datetime. Accepted formats: Y, YM, YMD, YMDH, YMDHM and YMDHMS                                |

## Custom Checks 

The ability to add custom checks is supported through pandera using [lambda functions][lambda-functions]. 
Custom checks cannot be defined in the main schema and must instead be defined as its own dictionary in your python script.
Then when creating your `DataValidator` object, simply pass this as an additional argument and your custom check will be applied across the entire dataframe. 

!!! note 
    You will get a log entry per column for this check, even for columns that are not contained in your custom check.

```Python

my_custom_checks = {
    "my_custom_check_name" : lambda df: (df["column_1"] < 100) & (df["column_2"].isna())
}

new_validator = DataValidator(
    schema = schema, 
    data=df,
    file = "output_report.yaml",
    format="yaml",
    custom_checks = my_custom_checks)

new_validator.validate()
```


## Checks that are not supported currently.

| Data Type        | Check Name             | Parameter         | Check Definition                                                                                                                           |
|---------------|---------------|---------------|-----------------------------|
| any              | Class                  | class             | Checks that column data Class matches the specified type                                                                          |

[lambda-functions]: https://realpython.com/python-lambda/