# `datachecker`

A Data checker which uses pandera to perform the bulk checks.
This wrapper can be used to check contents of a dataframe against a pre defined schema
either defined directly within your python script, or saved as a supported configuration 
file.

## Getting started

To start using this project, first make sure your system meets its
requirements.

It's suggested that you install this package and its requirements within
a virtual environment.

## Requirements

- Python 3.9+ installed

Contributors have some additional requirements - please see our [contributing guidance][contributing].

## Installing the package

Whilst in the root folder, in a terminal, you can install the package and its
Python dependencies using:

```shell
pip install pip install git+https://gitlab-app-l-01/ASAP/py-onsdatavalidator.git
```

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
| date/ datetime   | Maximum Datetime       | max_datetime      | Checks that all dates are before the maximum datetime. Accepted formats: Y, YM, YMD, YMDH, YMDHM and YMDHMS     

## Custom Checks 

The ability to add custom checks is supported through pandera using [lambda functions][lambda-functions]. 
Custom checks cannot be defined in the main schema and must instead be defined as its own dictionary in your python script.
Then when creating your `DataValidator` object, simply pass this as an additional argument and your custom check will be applied across the entire dataframe. 

Note: You will get a log entry per column for this check, even for columns that are not contained in your custom check.

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

## Install for contributors/developers

To install the contributing requirements, use:
```shell
python -m pip install -U pip setuptools
pip install -e .[dev]
pre-commit install
```

This installs an editable version of the package. This means that when you update the
package code you do not have to reinstall it for the changes to take effect.
This saves a lot of time when you test your code.

Remember to update the setup and requirement files inline with any changes to your
package.


## Creating the documentation locally

first pip instal the developer requirements, then run mkdocs serve to host the documentation on your local environment

```shell
pip install -e .[dev]
mkdocs serve
```


## Project structure layout

The cookiecutter template generated for each project will follow this folder structure:

```shell
.
├── datachecker/
│   └── datachecker/
│       ├── checks_loaders_and_exporters/
│       │   ├── __init__.py
│       │   └── checks.py
│       │   └── schema_loader.py
│       │   └── validator_exporter.py
│       │   └── validator_template.html
│       ├── __init__.py
│       ├── main.yml
└── ...
```

## Bumping project version 

`bump-my-version` is used to streamline the process for creating new versions and releases. 
To view the possible version bumps use `bump-my-version show-bump` and the three possible options will be presented.
Then to bump the version use `bump-my-version bump <increment>` where `<increment>` is replaced with either `major`, `minor` or `patch`.

A git tag is also created and can be pushed using the folowing code (pushing the v1.0.0 release tag!)
`git push origin v1.0.0` 


## Licence

Unless stated otherwise, the codebase is released under the MIT License. This covers
both the codebase and any sample code in the documentation. The documentation is ©
Crown copyright and available under the terms of the Open Government 3.0 licence.

## Contributing

If you want to help us build and improve `datachecker`, please take a look at our
[contributing guidelines][contributing].

## Acknowledgements

This project structure is based on the [`govcookiecutter` template project][govcookiecutter].

[contributing]: https://github.com/best-practice-and-impact/govcookiecutter/blob/main/%7B%7B%20cookiecutter.repo_name%20%7D%7D/docs/contributor_guide/CONTRIBUTING.md
[govcookiecutter]: https://github.com/best-practice-and-impact/govcookiecutter
[docs-loading-environment-variables]: https://github.com/best-practice-and-impact/govcookiecutter/blob/main/%7B%7B%20cookiecutter.repo_name%20%7D%7D/docs/user_guide/loading_environment_variables.md
[docs-loading-environment-variables-secrets]: https://github.com/best-practice-and-impact/govcookiecutter/blob/main/%7B%7B%20cookiecutter.repo_name%20%7D%7D/docs/user_guide/loading_environment_variables.md#storing-secrets-and-credentials
[lambda-functions]: https://realpython.com/python-lambda/