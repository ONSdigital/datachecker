import json

import tomli
import yaml


class SchemaLoader:
    """
    SchemaLoader is a class for registering and loading schema parsing functions based on format.
    Class Attributes:
        format_dictionary (dict): A mapping from format names (str) to schema loader functions.
    Methods
    -------
    __init__(self, format, schema_loader_function):
        Registers a schema loader function for a given format.
    load(cls, schema, format):
        Loads and parses a schema using the registered loader function for the specified format.
        Parameters
        ----------
        schema : str
            The file path to the schema.
        format : str
            The format identifier (e.g., "json", "yaml").
        Returns
        -------
        dict
            The parsed schema as a Python dictionary.
        Raises
        ------
        ValueError
            If the requested format is not registered in format_dictionary.
    """

    format_dictionary = {}

    def __init__(self, format, schema_loader_function):
        type(self).format_dictionary[format] = schema_loader_function

    @classmethod
    def load(cls, schema, format):
        if format not in cls.format_dictionary:
            raise ValueError(f"Format '{format}' is not supported.")
        output_function = cls.format_dictionary[format]
        return output_function(schema)


class JSONSchemaLoader(SchemaLoader):
    """
    A schema loader for JSON files.
    This class extends the `SchemaLoader` to provide functionality for loading
    schemas defined in JSON format. It registers itself with the identifier "json"
    and uses the `json.load` method to parse JSON files.

    Methods
    -------
    __init__():
        Initializes the loader and registers the JSON loading function.
    _load(schema: str) -> dict:
        Static method that loads and parses a JSON schema file.
        Parameters
        ----------
        schema : str
            The file path to the JSON schema.
        Returns
        -------
        dict
            The parsed schema as a Python dictionary.
    """

    def __init__(self):
        SchemaLoader.__init__(self, "json", self._load)

    @staticmethod
    def _load(schema):
        with open(schema, "r") as f:
            return json.load(f)


class YAMLSchemaLoader(SchemaLoader):
    """
    A schema loader for YAML files.
    This class extends the `SchemaLoader` to provide functionality for loading
    schemas defined in YAML format. It registers itself with the identifier "yaml"
    and uses the `yaml.safe_load` method to parse YAML files.
    Methods
    -------
    __init__():
        Initializes the loader and registers the YAML loading function.
    _load(schema: str) -> dict:
        Static method that loads and parses a YAML schema file.
        Parameters
        ----------
        schema : str
            The file path to the YAML schema.
        Returns
        -------
        dict
            The parsed schema as a Python dictionary.
    """

    def __init__(self):
        SchemaLoader.__init__(self, "yaml", self._load)

    @staticmethod
    def _load(schema):
        with open(schema, "r") as f:
            return yaml.safe_load(f)


class TOMLSchemaLoader(SchemaLoader):
    """
    A schema loader for TOML files.
    This class extends the `SchemaLoader` to provide functionality for loading
    schemas defined in TOML format. It registers itself with the identifier "toml"
    and uses the `tomli.load` method to parse TOML files.
    Methods
    -------
    __init__():
        Initializes the loader and registers the TOML loading function.
    _load(schema: str) -> dict:
        Static method that loads and parses a TOML schema file.
        Parameters
        ----------
        schema : str
            The file path to the TOML schema.
        Returns
        -------
        dict
            The parsed schema as a Python dictionary.
    """

    def __init__(self):
        SchemaLoader.__init__(self, "toml", self._load)

    @staticmethod
    def _load(schema):
        with open(schema, "rb") as f:
            return tomli.load(f)


YAMLSchemaLoader()
JSONSchemaLoader()
TOMLSchemaLoader()
