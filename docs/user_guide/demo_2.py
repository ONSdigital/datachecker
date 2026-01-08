import pandas as pd

from datachecker import DataValidator, check_and_export

if __name__ == "__main__":
    data = {
        "id": [-1, 2, None],
        "name": ["Alice", "Bob", "Charlie"],
        "age_of_person": [25.01, None, 35.999],
        "dob": [pd.Timestamp("1990/01/01"), pd.Timestamp("1985/05/15"), pd.Timestamp("2000/12/31")],
    }
    mock_df = pd.DataFrame(data)

    schema = {
        "columns": {
            "id": {
                "type": "float16",
                "min_val": 0,
                "max_val": 100,
                "allow_na": False,
                "optional": False,
            },
            "name": {"type": str, "optional": False, "max_length": 3, "allow_na": False},
            "age_of_person": {
                "type": float,
                "optional": True,
                "min_val": 30,
                "allow_na": True,
                "min_decimal": 2,
                "max_decimal": 3,
                "max_value": 100,
            },
            "dob": {
                "type": pd.Timestamp,
                "optional": True,
                "allow_na": True,
                "max_date": "1999/12/31 18:00",
                "min_date": "1900-01-01",
                "max_datetime": "2005-12-31 23:59",
                "min_datetime": "1980-01-01 00:00",
            },
        }
    }

    # Create new validator instance

    new_validator = DataValidator(
        schema=schema,
        data=mock_df,
        file="produced_logs/exported_log.yaml",
        format="yaml",
        hard_check=False,
    )

    # Run validation and export log
    new_validator.validate()
    new_validator.export()
    # test printing the validator object
    print(new_validator)

    for file_type in ["html", "json", "yaml", "csv", "txt"]:
        print(f"Exporting log as {file_type}")
        new_validator.format = file_type
        new_validator.file = f"produced_logs/exported_log.{file_type}"
        new_validator.export()

        # Export the validation log to HTML format using Jinja2 template

    check_and_export(schema, mock_df, "produced_logs/exported_log_direct.html", "html")
