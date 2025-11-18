import os
import tempfile
import pytest
from click.testing import CliRunner

pytest.importorskip("yamlinclude")
pytest.importorskip("rapidfuzz")

from graphsenselib.tagpack.cli import tagpacktool_cli


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
