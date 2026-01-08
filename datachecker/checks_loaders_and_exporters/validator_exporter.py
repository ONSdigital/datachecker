import importlib.resources
import json
import os

import pandas as pd
import yaml
from jinja2 import Template


class Exporter:
    """
    Exporter class for registering and exporting data in various formats.
    This class allows you to register exporter functions for different formats and use them
    to export data.
    Exporter functions should accept two arguments: `data` (the data to export) and `file`
    (the file or file-like object to write to).
    Attributes:
        format_dictionary (dict): A class-level dictionary mapping format names to
        exporter functions.
    Methods:
        __init__(self, format, exporter_function):
            Registers a new exporter function for the specified format.
        export(cls, data, format, file):
            Exports the given data using the exporter function registered for the specified format.
            Raises:
                ValueError: If the specified format is not supported.
    """

    format_dictionary = {}

    def __init__(self, format, exporter_function):
        type(self).format_dictionary[format] = exporter_function

    @classmethod
    def export(cls, data, format, file):
        if format not in cls.format_dictionary:
            raise ValueError(f"Format '{format}' is not supported.")
        output_function = cls.format_dictionary[format]
        return output_function(data, file)


class JSONExporter(Exporter):
    """
    JSONExporter is a subclass of Exporter that handles exporting validation logs to a JSON file.
    Methods
    -------
    __init__():
        Initializes the JSONExporter by registering the "json" export format and the associated
        export method.
    _export(data, file):
        Static method that writes the provided data to a file in JSON format.
        Parameters:
            data (Any): The validation log data to be exported.
            file (str): The path to the file where the data will be written.
        Returns:
            str: A message indicating the file has been exported.
    """

    def __init__(self):
        Exporter.__init__(self, "json", self._export)

    @staticmethod
    def _export(data, file):
        data = {"validation_log": data}
        with open(file, "w") as f:
            json.dump(data, f, indent=4)
        return f"{file} exported"


class CSVExporter(Exporter):
    """
    CSVExporter is a subclass of Exporter that provides functionality to export data to a CSV file.
    Methods
    -------
    __init__():
        Initializes the CSVExporter by registering the "csv" export format and its handler.
    _staticmethod_ _export(data, file):
        Exports the provided data to a CSV file.
        Parameters
        ----------
        data : list or dict
            The data to be exported, typically a list of dictionaries or a dictionary.
        file : str or file-like object
            The file path or file-like object where the CSV will be written.
        Returns
        -------
        str
            A message indicating the file has been exported.
    """

    def __init__(self):
        Exporter.__init__(self, "csv", self._export)

    @staticmethod
    def _export(data, file):
        data[0] = {"timestamp": "", "description": data[0]}
        df = pd.DataFrame(data)
        df.to_csv(file, index=False)
        return f"{file} exported"


class TXTExporter(Exporter):
    """
    TXTExporter is a subclass of Exporter that handles exporting data to a plain text
    (.txt) file.
    Methods
    -------
    __init__():
        Initializes the TXTExporter by registering the "txt" format and its export method.
    _staticmethod_ _export(data, file):
        Exports the provided data to a text file, writing each item on a new line.
    Parameters
    ----------
    data : iterable
        The data to be exported, where each item will be written as a separate line in the file.
    file : str or PathLike
        The path to the file where the data will be exported.
    Returns
    -------
    str
        A message indicating the file has been exported.
    """

    def __init__(self):
        Exporter.__init__(self, "txt", self._export)

    @staticmethod
    def _export(data, file):
        with open(file, "w") as f:
            for item in data:
                if isinstance(item, (dict, list)):
                    formatted = json.dumps(item, indent=4)
                    f.write(f"{formatted}\n")
                else:
                    f.write(f"{item}\n")
        return f"{file} exported"


class YAMLExporter(Exporter):
    """
    YAMLExporter is a subclass of Exporter that handles exporting data to a YAML file.
    Methods
    -------
    __init__():
        Initializes the YAMLExporter by setting the export format to 'yaml' and assigning the
        export method.
    _export(data, file):
        Static method that writes the provided data to the specified file in YAML format.
    Parameters
    ----------
    data : Any
        The data to be exported to YAML.
    file : str
        The path to the file where the YAML data will be written.
    Returns
    -------
    str
        A message indicating the file has been exported.
    """

    def __init__(self):
        Exporter.__init__(self, "yaml", self._export)

    @staticmethod
    def _export(data, file):
        with open(file, "w") as f:
            yaml.dump(data, f, sort_keys=False)
        return f"{file} exported"


class HTMLExporter(Exporter):
    def __init__(self):
        Exporter.__init__(self, "html", self._export)

    @staticmethod
    def _export(data, file):
        html_template = (
            importlib.resources.files("datachecker.checks_loaders_and_exporters")
            .joinpath("validator_template.html")
            .read_text(encoding="utf-8")
        )
        filename = os.path.splitext(os.path.basename(file))[0]
        system_info = data[0]
        log_df = pd.DataFrame(data[1:])
        template = Template(html_template)
        columns = log_df.columns.tolist()
        rows = log_df.values.tolist()
        rows = [["\u2705 pass" if v == "pass" else v for v in row] for row in rows]
        rows = [["\u274c fail" if v == "fail" else v for v in row] for row in rows]
        rendered_html = template.render(
            columns=columns, rows=rows, sys_info=system_info, name=filename
        )
        with open(f"{file}", "w", encoding="utf-8") as f:
            f.write(rendered_html)

        print(f"QA log exported to {file}")
        return f"{file} exported"


# register the exporters
JSONExporter()
CSVExporter()
TXTExporter()
YAMLExporter()
HTMLExporter()
