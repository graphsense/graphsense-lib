"""Tests for fast YAML loading functionality."""

from datetime import date

import pytest
import yaml

from graphsenselib.tagpack import (
    RYML_AVAILABLE,
    UniqueKeyLoader,
    ValidationError,
    load_yaml_fast,
)


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


@pytest.mark.skipif(not RYML_AVAILABLE, reason="rapidyaml not installed")
class TestYamlParserDifferences:
    """Tests documenting the differences between PyYAML and rapidyaml.

    These tests document the intentional differences in behavior between the
    two YAML parsers. The concept_mapping module handles both gracefully.
    """

    def test_yes_no_parsing_differs(self, tmp_path):
        """Document that yes/no are parsed differently by the two loaders.

        - PyYAML SafeLoader: yes/no -> True/False (YAML 1.1 behavior)
        - rapidyaml: yes/no -> "yes"/"no" (keeps as strings)
        """
        yaml_content = "value_yes: yes\nvalue_no: no\n"
        yaml_file = tmp_path / "yesno.yaml"
        yaml_file.write_text(yaml_content)

        # rapidyaml keeps as strings
        rapid_result = load_yaml_fast(str(yaml_file))
        assert rapid_result["value_yes"] == "yes"
        assert rapid_result["value_no"] == "no"
        assert isinstance(rapid_result["value_yes"], str)

        # PyYAML converts to booleans
        with open(yaml_file, "r") as f:
            pyyaml_result = yaml.load(f, UniqueKeyLoader)
        assert pyyaml_result["value_yes"] is True
        assert pyyaml_result["value_no"] is False

    def test_date_parsing_matches_pyyaml(self, tmp_path):
        """Verify that dates are parsed the same by both loaders.

        Both convert YYYY-MM-DD -> datetime.date object
        """
        yaml_content = "lastmod: 2021-04-21\nother: 2023-12-25\n"
        yaml_file = tmp_path / "date.yaml"
        yaml_file.write_text(yaml_content)

        rapid_result = load_yaml_fast(str(yaml_file))
        with open(yaml_file, "r") as f:
            pyyaml_result = yaml.load(f, UniqueKeyLoader)

        # Both should produce date objects
        assert rapid_result["lastmod"] == date(2021, 4, 21)
        assert rapid_result["other"] == date(2023, 12, 25)
        assert isinstance(rapid_result["lastmod"], date)

        # And match PyYAML exactly
        assert rapid_result == pyyaml_result

    def test_true_false_parsed_same(self, tmp_path):
        """Verify that true/false are parsed the same by both loaders."""
        yaml_content = "bool_true: true\nbool_false: false\n"
        yaml_file = tmp_path / "bool.yaml"
        yaml_file.write_text(yaml_content)

        rapid_result = load_yaml_fast(str(yaml_file))
        with open(yaml_file, "r") as f:
            pyyaml_result = yaml.load(f, UniqueKeyLoader)

        # Both should produce booleans for true/false
        assert rapid_result["bool_true"] is True
        assert rapid_result["bool_false"] is False
        assert pyyaml_result["bool_true"] is True
        assert pyyaml_result["bool_false"] is False


class TestPyYamlFallback:
    """Tests for PyYAML fallback when rapidyaml is not available."""

    def test_fallback_loads_basic_yaml(self, tmp_path, monkeypatch):
        """Test that fallback to PyYAML works correctly."""
        import graphsenselib.tagpack as tagpack_module

        # Force fallback by setting RYML_AVAILABLE to False
        monkeypatch.setattr(tagpack_module, "RYML_AVAILABLE", False)

        content = {"title": "Test", "tags": [{"label": "a", "address": "b"}]}
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(yaml.dump(content))

        result = load_yaml_fast(str(yaml_file))
        assert result == content

    def test_fallback_detects_duplicates(self, tmp_path, monkeypatch):
        """Test that duplicate detection works in fallback mode."""
        import graphsenselib.tagpack as tagpack_module

        monkeypatch.setattr(tagpack_module, "RYML_AVAILABLE", False)

        yaml_file = tmp_path / "dup.yaml"
        yaml_file.write_text("title: First\ntitle: Duplicate\n")

        with pytest.raises(ValidationError) as exc_info:
            load_yaml_fast(str(yaml_file))
        assert "Duplicate" in str(exc_info.value)
        assert "title" in str(exc_info.value)
