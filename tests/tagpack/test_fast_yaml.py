"""Tests for fast YAML loading functionality."""

import pytest
import yaml

from graphsenselib.tagpack import ValidationError, load_yaml_fast


class TestLoadYamlFast:
    """Tests for load_yaml_fast using rapidyaml."""

    def test_basic_loading(self, tmp_path):
        content = {"title": "Test", "tags": [{"label": "a", "address": "b"}]}
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(yaml.dump(content))
        result = load_yaml_fast(str(yaml_file))
        assert result == content

    def test_preserves_data_types(self, tmp_path):
        content = {
            "string": "hello",
            "integer": 42,
            "float": 3.14,
            "boolean_true": True,
            "boolean_false": False,
            "null_value": None,
            "list": [1, 2, 3],
            "nested": {"a": "b", "c": 1},
        }
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(yaml.dump(content))
        result = load_yaml_fast(str(yaml_file))

        assert result["string"] == "hello"
        assert result["integer"] == 42
        assert isinstance(result["float"], float)
        assert result["boolean_true"] is True
        assert result["boolean_false"] is False
        assert result["null_value"] is None
        assert result["list"] == [1, 2, 3]
        assert result["nested"] == {"a": "b", "c": 1}

    def test_duplicate_key_raises(self, tmp_path):
        yaml_file = tmp_path / "dup.yaml"
        yaml_file.write_text("title: First\ntitle: Duplicate\n")

        with pytest.raises(ValidationError) as exc_info:
            load_yaml_fast(str(yaml_file))
        assert "Duplicate" in str(exc_info.value)
        assert "title" in str(exc_info.value)

    def test_nested_duplicate_key_raises(self, tmp_path):
        yaml_file = tmp_path / "nested_dup.yaml"
        yaml_file.write_text("outer:\n  inner: 1\n  inner: 2\n")

        with pytest.raises(ValidationError) as exc_info:
            load_yaml_fast(str(yaml_file))
        assert "Duplicate" in str(exc_info.value)
        assert "inner" in str(exc_info.value)

    def test_duplicate_key_in_list_item_raises(self, tmp_path):
        yaml_file = tmp_path / "list_dup.yaml"
        yaml_file.write_text("tags:\n  - label: a\n    label: b\n")

        with pytest.raises(ValidationError) as exc_info:
            load_yaml_fast(str(yaml_file))
        assert "Duplicate" in str(exc_info.value)
        assert "label" in str(exc_info.value)
