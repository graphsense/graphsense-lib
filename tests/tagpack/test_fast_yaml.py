"""Tests for fast YAML loading functionality."""

import os

import pytest
import yaml

from graphsenselib.tagpack import ValidationError, load_yaml_fast


class TestFastYamlLoading:
    """Tests for the fast YAML loading functionality."""

    def test_load_yaml_fast_small_file(self, tmp_path):
        content = {"title": "Test", "tags": [{"label": "a", "address": "b"}]}
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(yaml.dump(content))
        result = load_yaml_fast(str(yaml_file))
        assert result == content

    def test_load_yaml_fast_preserves_data_types(self, tmp_path):
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


class TestFastPathPerformance:
    """Tests that exercise the fast path for large files."""

    def test_large_file_uses_fast_path(self, tmp_path):
        tags = [{"label": f"label_{i}", "address": f"addr_{i}"} for i in range(5000)]
        content = {"title": "Large TagPack", "creator": "Test", "tags": tags}
        yaml_file = tmp_path / "large.yaml"
        yaml_file.write_text(yaml.dump(content))

        file_size = os.path.getsize(yaml_file)
        assert file_size > 100 * 1024, f"File too small: {file_size} bytes"

        result = load_yaml_fast(str(yaml_file))
        assert result["title"] == "Large TagPack"
        assert len(result["tags"]) == 5000

    def test_fast_path_matches_standard_loader(self, tmp_path):
        tags = [{"label": f"label_{i}", "address": f"addr_{i}"} for i in range(5000)]
        content = {"title": "Test", "is_cluster_definer": True, "tags": tags}
        yaml_file = tmp_path / "large.yaml"
        yaml_file.write_text(yaml.dump(content))

        standard_result = yaml.safe_load(yaml_file.read_text())
        fast_result = load_yaml_fast(str(yaml_file))

        assert fast_result["title"] == standard_result["title"]
        assert (
            fast_result["is_cluster_definer"] == standard_result["is_cluster_definer"]
        )
        assert len(fast_result["tags"]) == len(standard_result["tags"])


class TestDuplicateKeyDetection:
    """Tests for duplicate YAML key detection."""

    def test_duplicate_key_small_file_raises(self, tmp_path):
        """Duplicate keys in small files should raise ValidationError."""
        yaml_file = tmp_path / "dup.yaml"
        yaml_file.write_text("title: First\ntitle: Duplicate\n")

        with pytest.raises(ValidationError) as exc_info:
            load_yaml_fast(str(yaml_file))
        assert "Duplicate" in str(exc_info.value)
        assert "title" in str(exc_info.value)

    def test_duplicate_key_large_file_raises(self, tmp_path):
        """Duplicate keys in large files (fast path) should raise ValidationError."""
        padding = "".join([f"key_{i}: value_{i}\n" for i in range(5000)])
        yaml_file = tmp_path / "large_dup.yaml"
        yaml_file.write_text(f"title: First\n{padding}title: Duplicate\n")

        file_size = os.path.getsize(yaml_file)
        assert file_size > 100 * 1024, f"File too small: {file_size} bytes"

        with pytest.raises(ValidationError) as exc_info:
            load_yaml_fast(str(yaml_file))
        assert "Duplicate" in str(exc_info.value)
        assert "title" in str(exc_info.value)

    def test_duplicate_key_in_nested_map_raises(self, tmp_path):
        """Duplicate keys in nested maps should raise ValidationError."""
        yaml_file = tmp_path / "nested_dup.yaml"
        yaml_file.write_text("outer:\n  inner: 1\n  inner: 2\n")

        with pytest.raises(ValidationError) as exc_info:
            load_yaml_fast(str(yaml_file))
        assert "Duplicate" in str(exc_info.value)
        assert "inner" in str(exc_info.value)
