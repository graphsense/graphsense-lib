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


# Every value the old JSON-based fast loader read differently from PyYAML.
SAME_AS_PYYAML = {
    "digits with an exponent": "a: 12E34\nb: 38e1618118121767477645871397668758\n",
    "inf and nan": "a: inf\nb: .inf\nc: nan\nd: .NaN\ne: -.inf\n",
    "aliases": "x: &id001\n- one\n- two\ny: *id001\nz: {c: *id001}\n",
    "booleans": "a: yes\nb: 'yes'\nc: true\nd: On\ne: NO\nf: off\n",
    "numbers": "a: 0x1A\nb: 012\nc: 1_000\nd: '0x1A'\ne: 0b101\nf: 1:30\n",
    "nulls": "a: ~\nb: null\nc:\nd: ''\n",
    "dates": "a: '2025-03-24'\nb: 2025-03-24\nc: 2025-03-24 10:00:00\n"
    "d: 2025-03-24T10:00:00Z\n",
    "floats": "a: 1.5\nb: -3\nc: 1e3\nd: 1.5e3\n",
    "block scalars": 'a: |\n  12E34\n  yes\nb: >\n  folded\n  text\nc: "x\\ty"\n',
    "json text": 'a: \'{"name": "caf\\u00e9", "v": 1.0}\'\n',
    "non-string keys": "1: one\nfalse: f\n'2': two\n",
    "sequences": "- a\n- 1\n- [x, 2]\n- {k: v}\n",
    "scalar document": "plain text\n",
    "empty document": "",
    "explicit tag": "a: !!str 12\n",
}


@pytest.mark.parametrize("text", SAME_AS_PYYAML.values(), ids=SAME_AS_PYYAML.keys())
def test_fast_loader_reads_as_pyyaml(tmp_path, text):
    yaml_file = tmp_path / "pack.yaml"
    yaml_file.write_text(text, encoding="utf-8")
    fast = load_yaml_fast(str(yaml_file))
    slow = yaml.load(text, UniqueKeyLoader)
    assert repr(fast) == repr(slow)  # repr: also the types (1 vs True vs '1')


@pytest.mark.parametrize(
    "text",
    [
        "base: &b {x: 1}\nd:\n  <<: *b\n  y: 2\n",  # merge key
        "a: =\n",  # value tag
    ],
)
def test_fast_loader_leaves_errors_to_pyyaml(tmp_path, text):
    yaml_file = tmp_path / "pack.yaml"
    yaml_file.write_text(text)
    with pytest.raises(yaml.YAMLError) as fast:
        load_yaml_fast(str(yaml_file))
    with pytest.raises(yaml.YAMLError) as slow:
        yaml.load(text, UniqueKeyLoader)
    assert type(fast.value) is type(slow.value)


@pytest.mark.skipif(not RYML_AVAILABLE, reason="rapidyaml not installed")
def test_fast_loader_does_not_parse_with_pyyaml(tmp_path, monkeypatch):
    yaml_file = tmp_path / "pack.yaml"
    yaml_file.write_text("a: yes\nb: 2025-03-24\nc: [1, '2']\n")
    import graphsenselib.tagpack as tagpack_module

    def no_pyyaml(*a, **k):
        raise AssertionError("PyYAML parsed the file")

    monkeypatch.setattr(
        tagpack_module.yaml if hasattr(tagpack_module, "yaml") else yaml,
        "load",
        no_pyyaml,
    )
    assert load_yaml_fast(str(yaml_file)) == {
        "a": True,
        "b": date(2025, 3, 24),
        "c": [1, "2"],
    }


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


@pytest.mark.skipif(not RYML_AVAILABLE, reason="rapidyaml not installed")
class TestRapidyamlFallback:
    """A rapidyaml parse failure falls back to PyYAML instead of aborting."""

    def _fail_fast_parser(self, monkeypatch):
        import ryml

        real = ryml.parse_in_arena

        def boom(*a, **kw):
            return real(b"a: b: c\n")  # a genuine rapidyaml parse error

        monkeypatch.setattr(ryml, "parse_in_arena", boom)

    def test_falls_back_to_pyyaml(self, tmp_path, monkeypatch, caplog):
        self._fail_fast_parser(monkeypatch)
        yaml_file = tmp_path / "pack.yaml"
        yaml_file.write_text("title: Ünïcode\ntags:\n  - label: a\n", encoding="utf-8")
        assert load_yaml_fast(str(yaml_file)) == {
            "title": "Ünïcode",
            "tags": [{"label": "a"}],
        }
        assert "retrying with PyYAML" in caplog.text

    def test_broken_file_reports_line_and_column(self, tmp_path):
        yaml_file = tmp_path / "broken.yaml"
        yaml_file.write_text("title: x\ntags:\n  - label: Exchange: deposit\n")
        with pytest.raises(yaml.YAMLError) as exc_info:
            load_yaml_fast(str(yaml_file))
        assert "line 3" in str(exc_info.value)

    def test_fallback_still_rejects_duplicate_keys(self, tmp_path, monkeypatch):
        self._fail_fast_parser(monkeypatch)
        yaml_file = tmp_path / "dup.yaml"
        yaml_file.write_text("title: a\ntitle: b\n")
        with pytest.raises(Exception, match="title"):
            load_yaml_fast(str(yaml_file))


HEADER = "title: T\ncreator: C\nsource: http://example.com\ncurrency: BTC\n"
TAGS = "tags:\n- address: 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa\n  label: x\n"


@pytest.fixture
def pack_args():
    pytest.importorskip("yaml_include")
    from graphsenselib.tagpack.cli import DEFAULT_CONFIG
    from graphsenselib.tagpack.tagpack_schema import TagPackSchema
    from graphsenselib.tagpack.taxonomy import _load_taxonomies

    return TagPackSchema(), _load_taxonomies(DEFAULT_CONFIG)


@pytest.mark.skipif(not RYML_AVAILABLE, reason="rapidyaml not installed")
def test_pack_below_a_header_dir_uses_the_fast_loader(tmp_path, pack_args, monkeypatch):
    from graphsenselib.tagpack.tagpack import TagPack

    (tmp_path / "header.yaml").write_text(HEADER)
    pack = tmp_path / "plain.yaml"
    pack.write_text(HEADER + TAGS)

    def no_pyyaml(*a, **k):
        raise AssertionError("PyYAML parsed the pack")

    monkeypatch.setattr(yaml, "load", no_pyyaml)
    tagpack = TagPack.load_from_file(None, str(pack), *pack_args, str(tmp_path))
    assert tagpack.contents["title"] == "T"


def test_include_after_the_first_4kb_is_resolved(tmp_path, pack_args):
    from graphsenselib.tagpack.tagpack import TagPack

    (tmp_path / "header.yaml").write_text(HEADER)
    pack = tmp_path / "late.yaml"
    comments = "".join(
        f"# comment {i:04d} padding padding padding\n" for i in range(200)
    )
    pack.write_text(comments + "header: !include header.yaml\n" + TAGS)
    tagpack = TagPack.load_from_file(None, str(pack), *pack_args, str(tmp_path))
    assert tagpack.contents["title"] == "T"


def test_unquoted_hex_stays_text_in_both_loaders(tmp_path, monkeypatch):
    text = "address: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48\nn: 012\n"
    yaml_file = tmp_path / "pack.yaml"
    yaml_file.write_text(text)
    expected = {"address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "n": 10}
    assert yaml.load(text, UniqueKeyLoader) == expected
    assert load_yaml_fast(str(yaml_file)) == expected
    # plain PyYAML is not changed
    assert yaml.load("a: 0x1A\n", yaml.SafeLoader) == {"a": 26}
