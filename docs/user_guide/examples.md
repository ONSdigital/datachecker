# Examples

## Validating a simple dataset

First we need to import the needed modules and load / create the schema. The contents of this schema could be saved in any form of configuration file (JSON, yaml, etc.) and be passed into our validator (example shown below)

Our schema we are expecting age to be a `float` with minimum value of 0 and max of 120. Name is a `string` with at least 2 characters. Email is also a string but we are using a regular expression (Regex) to test the format is correct for a valid email. Finally we are expecting `is_active` to be an integer.

```Python
from datachecker import DataValidator
import pandas as pd

schema = {
    "columns": {
        "age": {
            "type": float,
            "min_val":0,
            "max_val":120,
            "allow_na": False,
            "optional": False
        },
        "name": {
            "type": str,
            "min_len": 2,
            "max_len": 10,
            "allow_na": False,
            "optional": False,
            "allowed_strings": r"^[A-Za-z\s]+$"
        },
        "email": {
            "type": str,
            "min_len": 5,
            "max_len": 50,
            "allow_na": False,
            "optional": False,
            "allowed_strings": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        },
        "is_active": {
            "type": int,
            "allow_na": False,
            "optional": False
        }
    }
}

```

Next we need to load our dataset, for this example we will instead create our dataframe within our script. Note for this example we have actually created the `is_active` column to be a boolean and not a integer as outlined in our schema. This should be picked up in our validation checks! Also one email is slightly incorrect and a name contains a number.

```Python
data = [
    {"age": 30, "name": "John Doe", "email": "john.doe@example.com", "is_active": True},
    {"age": 25, "name": "Jane Smith", "email": "jane.smith@example.com", "is_active": False},
    {"age": 40, "name": "Alice Brown", "email": "alice.brown.com", "is_active": True},
    {"age": -22, "name": "Bob White", "email": "bob.white@example.com", "is_active": False},
    {"age": 35, "name": "Carol Green1", "email": "carol.green@example.com", "is_active": True},
    {"age": 28, "name": "Eve Black", "email": "eve.black@example.com", "is_active": False}
]

df = pd.DataFrame(data)
```

We can now run our validator and export our log. (IN THE FINAL VERSION VALIDATE AND EXPORT WILL BE DIRECTLY CALLED DURING CLASS INSTANTIATION)
Printing the new_validtor object will print the contents of the log file to the terminal or python session.

```Python
new_validator = DataValidator(
    schema = schema, 
    data=df,
    file = "output_report.yaml",
    format="yaml")

new_validator.validate()
new_validator.export()
print(new_validator)
```

Now looking at the contents of the yaml or command line we can see our dataframe has passed most validation checks. 

```yaml
- date: '2025-12-23'
  user: Omitted
  device: Omitted
  device_platform: Omitted
  architecture: 64bit
  python_version: 3.12.5
  pandas_version: 2.3.3
  pandera_version: 0.26.1
  datachecker_version: 0.0.1
- timestamp: '14:04:06'
  description: Dataframe columns missing from schema
  outcome: Pass
  failing_ids: []
  number_failing: 0
  status: error
- timestamp: '14:04:06'
  description: Schema keys not in dataframe
  outcome: Pass
  failing_ids: []
  number_failing: 0
  status: warning
- timestamp: '14:04:06'
  description: checking column names
  outcome: Pass
  failing_ids: []
  number_failing: 0
  status: error
- timestamp: '14:04:06'
  description: checking column names are lowercase
  outcome: Pass
  failing_ids: []
  number_failing: 0
  status: warning
- timestamp: '14:04:06'
  description: checking mandatory columns are present
  outcome: Pass
  failing_ids: []
  number_failing: 0
  status: error
- timestamp: '14:04:06'
  description: checking for unexpected columns
  outcome: Pass
  failing_ids: []
  number_failing: 0
  status: warning
- timestamp: '14:04:06'
  description: Checking age dtype('float64')
  outcome: Fail
  failing_ids:
  - int64
  number_failing: 1
  status: error
- timestamp: '14:04:06'
  description: Checking age greater_than_or_equal_to(0)
  outcome: Fail
  failing_ids:
  - 3
  number_failing: 1
  status: error
- timestamp: '14:04:06'
  description: Checking age less_than_or_equal_to(120)
  outcome: Pass
  failing_ids: []
  number_failing: 0
  status: error
- timestamp: '14:04:06'
  description: Checking name str_matches('^[A-Za-z\s]+$')
  outcome: Fail
  failing_ids:
  - 4
  number_failing: 1
  status: error
- timestamp: '14:04:06'
  description: Checking email str_matches('^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
  outcome: Fail
  failing_ids:
  - 2
  number_failing: 1
  status: error
- timestamp: '14:04:06'
  description: Checking is_active dtype('int64')
  outcome: Fail
  failing_ids:
  - bool
  number_failing: 1
  status: error
```

From out yaml output we can see it failed 5 checks. These were: 

    1. age was not a float, this was checked and found to be an integer
    2. not all ages were larger than 0, the entry in row 3 failed this check 
    3. The name columns did not contain only upper or lowercase letters with spaces, entry in row 4 failed this.
    4. an invalid email address was found in row 2
    5. data type of is_active was a boolean when it was expecting an integer. 

