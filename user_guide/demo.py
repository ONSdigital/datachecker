import time

import pandas as pd

from datachecker import DataValidator

tic = time.time()
schema = {
    "columns": {
        "age": {"type": float, "min_val": 0, "max_val": 120, "allow_na": False, "optional": False},
        "name": {
            "type": str,
            "min_length": 2,
            "max_length": 10,
            "allow_na": False,
            "optional": False,
            "allowed_strings": r"^[A-Za-z\s]+$",
        },
        "email": {
            "type": str,
            "min_length": 5,
            "max_length": 50,
            "allow_na": False,
            "optional": False,
            "allowed_strings": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        },
        "is_active": {"type": bool, "allow_na": False, "optional": False},
        "sex": {
            "type": str,
            "allowed_strings": ["M", "F", "X"],
            "allow_na": False,
            "optional": False,
        },
    }
}

data = [
    {"age": 30, "name": "John Doe", "email": "john.doe@example.com", "is_active": True, "sex": "M"},
    {
        "age": 25,
        "name": "Jane Smith",
        "email": "jane.smith@example.com",
        "is_active": False,
        "sex": "F",
    },
    {"age": 40, "name": "Alice Brown", "email": "alice.brown.com", "is_active": True, "sex": "X"},
    {
        "age": -22,
        "name": "Bob White",
        "email": "bob.white@example.com",
        "is_active": False,
        "sex": "B",
    },
    {
        "age": 35,
        "name": "Carol Green1",
        "email": "carol.green@example.com",
        "is_active": True,
        "sex": "F",
    },
    {
        "age": 28,
        "name": "Eve Black",
        "email": "eve.black@example.com",
        "is_active": False,
        "sex": "M",
    },
]

df = pd.DataFrame(data)

new_validator = DataValidator(schema=schema, data=df, file="output_report.html", format="html")

new_validator.validate()
new_validator.export()
print(new_validator)
toc = time.time()
print(f"Validation completed in {toc - tic:.2f} seconds.")
