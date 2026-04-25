import os
import tempfile
import pytest
from click.testing import CliRunner

pytest.importorskip("yamlinclude")
pytest.importorskip("rapidfuzz")

from graphsenselib.tagpack.cli import tagpacktool_cli
from graphsenselib.tagpack.cli import (
    DEFAULT_ACTORPACK_URL,
    _load_config,
    load_actorpack_for_validation,
)


def test_load_actorpack_for_validation_default_url_is_ephemeral(monkeypatch, tmp_path):
    """Default actorpack loading should fetch in-memory without writing a local file."""
    actorpack = """title: Test
creator: Test
description: Test
lastmod: 2024-01-01
actors:
- id: binance
  uri: https://binance.com
  label: Binance
  categories: [exchange]
"""

    class MockResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    urls = []

    def mock_get(url, timeout=30):
        urls.append(url)
        return MockResponse(actorpack)

    monkeypatch.setattr("requests.get", mock_get)
    monkeypatch.chdir(tmp_path)

    ap = load_actorpack_for_validation(None, _load_config(None))

    assert ap is not None
    assert urls == [DEFAULT_ACTORPACK_URL]
    assert not (tmp_path / "graphsense.actorpack.yaml").exists()


def test_validate_supports_actorpack_http_url(monkeypatch):
    """Actor validation should support actorpack-path as HTTP URL."""
    actorpack = """title: Test
creator: Test
description: Test
lastmod: 2024-01-01
actors:
- id: binance
  uri: https://binance.com
  label: Binance
  categories: [exchange]
"""

    tagpack_valid = """title: Test
creator: Test
source: http://example.com
currency: BTC
lastmod: 2024-01-01
actor: binance
tags:
- address: 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa
  label: Test
"""

    class MockResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    def mock_get(url, timeout=30):
        return MockResponse(actorpack)

    monkeypatch.setattr("requests.get", mock_get)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(tagpack_valid)
        tagpack_path = f.name

    try:
        result = CliRunner().invoke(
            tagpacktool_cli,
            [
                "tagpack-tool",
                "tagpack",
                "validate",
                tagpack_path,
                "--check-actor-references",
                "--actorpack-path",
                "https://example.com/actorpack.yaml",
            ],
        )

        assert result.exit_code == 0
    finally:
        os.unlink(tagpack_path)


def test_validate_with_actor_checking():
    """Test actor validation with valid and invalid actors"""
    actorpack = """title: Test
creator: Test
description: Test
lastmod: 2024-01-01
actors:
- id: binance
  aliases: ["binanceexchange"]
  uri: https://binance.com
  label: Binance
  categories: [exchange]
"""

    tagpack_valid = """title: Test
creator: Test
source: http://example.com
currency: BTC
lastmod: 2024-01-01
actor: binance
tags:
- address: 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa
  label: Test
"""

    tagpack_typo = tagpack_valid.replace("actor: binance", "actor: binanse")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(actorpack)
        actorpack_path = f.name

    try:
        # Test valid actor
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(tagpack_valid)
            tagpack_path = f.name

        result = CliRunner().invoke(
            tagpacktool_cli,
            [
                "tagpack-tool",
                "tagpack",
                "validate",
                tagpack_path,
                "--check-actor-references",
                "--no-strict-actor-references",
                "--actorpack-path",
                actorpack_path,
            ],
        )

        assert result.exit_code == 0
        assert "Unique actors found in actorpack: 1" in result.output
        os.unlink(tagpack_path)

        # Test typo with suggestions
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(tagpack_typo)
            tagpack_path = f.name

        result = CliRunner().invoke(
            tagpacktool_cli,
            [
                "tagpack-tool",
                "tagpack",
                "validate",
                tagpack_path,
                "--check-actor-references",
                "--no-strict-actor-references",
                "--actorpack-path",
                actorpack_path,
            ],
        )

        assert result.exit_code == 0
        assert "binanse" in result.output
        assert "binance" in result.output
        assert "suggestions" in result.output
        os.unlink(tagpack_path)
    finally:
        os.unlink(actorpack_path)


def test_validate_with_actor_on_tags_level():
    """Test actor validation when actor is defined at the tags level"""
    actorpack = """title: Test
creator: Test
description: Test
lastmod: 2024-01-01
actors:
- id: binance
  aliases: ["binanceexchange"]
  uri: https://binance.com
  label: Binance
  categories: [exchange]
"""

    tagpack_valid = """title: Test
creator: Test
source: http://example.com
currency: BTC
lastmod: 2024-01-01
tags:
- address: 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa
  label: Test
  actor: binance
"""

    tagpack_typo = """title: Test
creator: Test
source: http://example.com
currency: BTC
lastmod: 2024-01-01
tags:
- address: 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa
  label: Test
  actor: binanse
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(actorpack)
        actorpack_path = f.name

    try:
        # Test valid actor at tags level
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(tagpack_valid)
            tagpack_path = f.name

        result = CliRunner().invoke(
            tagpacktool_cli,
            [
                "tagpack-tool",
                "tagpack",
                "validate",
                tagpack_path,
                "--check-actor-references",
                "--no-strict-actor-references",
                "--actorpack-path",
                actorpack_path,
            ],
        )

        assert result.exit_code == 0
        assert "Unique actors found in actorpack: 1" in result.output
        os.unlink(tagpack_path)

        # Test typo at tags level with suggestions
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(tagpack_typo)
            tagpack_path = f.name

        result = CliRunner().invoke(
            tagpacktool_cli,
            [
                "tagpack-tool",
                "tagpack",
                "validate",
                tagpack_path,
                "--check-actor-references",
                "--no-strict-actor-references",
                "--actorpack-path",
                actorpack_path,
            ],
        )

        assert result.exit_code == 0
        assert "binanse" in result.output
        assert "binance" in result.output
        assert "suggestions" in result.output
        os.unlink(tagpack_path)
    finally:
        os.unlink(actorpack_path)


def test_validate_with_strict_actor_checking_fails_without_summary():
    """Strict actor checking should fail on unknown actors and suppress summary."""
    actorpack = """title: Test
creator: Test
description: Test
lastmod: 2024-01-01
actors:
- id: binance
  aliases: ["binanceexchange"]
  uri: https://binance.com
  label: Binance
  categories: [exchange]
"""

    tagpack_typo = """title: Test
creator: Test
source: http://example.com
currency: BTC
lastmod: 2024-01-01
actor: binanse
tags:
- address: 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa
  label: Test
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(actorpack)
        actorpack_path = f.name

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(tagpack_typo)
            tagpack_path = f.name

        result = CliRunner().invoke(
            tagpacktool_cli,
            [
                "tagpack-tool",
                "tagpack",
                "validate",
                tagpack_path,
                "--strict-actor-references",
                "--actorpack-path",
                actorpack_path,
            ],
        )

        assert result.exit_code == 1
        assert "ACTOR VALIDATION SUMMARY" not in result.output
        os.unlink(tagpack_path)
    finally:
        os.unlink(actorpack_path)
