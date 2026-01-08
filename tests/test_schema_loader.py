from datachecker.checks_loaders_and_exporters.schema_loader import SchemaLoader


class TestSchemaLoader:
    def test_loading_json(self):
        filepath = "tests/data/test.json"
        loaded_schema = SchemaLoader.load(filepath, "json")
        assert isinstance(loaded_schema, dict)
        assert "columns" in loaded_schema

    def test_loading_yaml(self):
        filepath = "tests/data/test.yaml"
        loaded_schema = SchemaLoader.load(filepath, "yaml")
        assert isinstance(loaded_schema, dict)
        assert "columns" in loaded_schema

    def test_loading_toml(self):
        filepath = "tests/data/test.toml"
        loaded_schema = SchemaLoader.load(filepath, "toml")
        assert isinstance(loaded_schema, dict)
        assert "columns" in loaded_schema

    def test_invalid_format(self):
        # csv not supported as schema format
        # test.csv does not exist
        filepath = "test.csv"
        try:
            SchemaLoader.load(filepath, "csv")
        except ValueError as e:
            assert str(e) == "Format 'csv' is not supported."
