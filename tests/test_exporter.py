import os
import tempfile

import pytest

from datachecker.checks_loaders_and_exporters.validator_exporter import Exporter


def dummy_exporter(data, file):
    with open(file, "w") as f:
        f.write(str(data))
    return f"{file} exported"


def test_register_and_export_success():
    # Register a new format
    Exporter("dummy", dummy_exporter)
    data = {"a": 1}
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
    try:
        result = Exporter.export(data, "dummy", tmp_path)
        assert os.path.exists(tmp_path)
        with open(tmp_path) as f:
            content = f.read()
        assert content == str(data)
        assert result == f"{tmp_path} exported"
    finally:
        os.remove(tmp_path)


def test_export_unsupported_format():
    with pytest.raises(ValueError) as excinfo:
        Exporter.export({}, "unsupported_format", "somefile.txt")
    assert "Format 'unsupported_format' is not supported." in str(excinfo.value)


def test_export_json(tmp_path):
    data = {"foo": "bar", "num": 42}
    file_path = tmp_path / "data.json"
    Exporter.export(data, "json", file_path)
    assert os.path.exists(file_path)


def test_export_yaml(tmp_path):
    data = {"foo": "bar", "num": 42, "list": [1, 2, 3], "dict": {"key": "value"}}
    file_path = tmp_path / "data.yaml"
    Exporter.export(data, "yaml", file_path)
    assert os.path.exists(file_path)


def test_export_txt(tmp_path):
    data = {
        "foo": "bar",
        "num": 42,
        "list": [1, 2, 3],
        "dict": {"key": "value"},
        "tuple": ({"a": 1}, [2, 3]),
    }
    file_path = tmp_path / "data.txt"
    Exporter.export(data, "txt", file_path)
    assert os.path.exists(file_path)


def test_export_csv(tmp_path):
    data = [{"foo": "bar", "num": 42}]
    file_path = tmp_path / "data.csv"
    Exporter.export(data, "csv", file_path)
    assert os.path.exists(file_path)


def test_export_html(tmp_path):
    data = [{"foo": "bar", "num": 42}]
    file_path = tmp_path / "data.html"
    Exporter.export(data, "html", file_path)
    assert os.path.exists(file_path)
