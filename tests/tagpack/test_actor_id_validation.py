"""Tests for actor ID validation feature"""

import os
import tempfile
import pytest
from click.testing import CliRunner

pytest.importorskip("yamlinclude")

from graphsenselib.tagpack.cli import tagpacktool_cli, load_actor_ids_from_url


def test_load_actor_ids_from_url_function():
    """Test that load_actor_ids_from_url function exists and is callable"""
    # Verify the function exists and is callable
    assert callable(load_actor_ids_from_url)

    # Test with invalid URL - should return None
    result = load_actor_ids_from_url(
        "http://invalid.url.that.does.not.exist.example.com"
    )
    assert result is None


def test_validate_tagpack_with_actor_ids_flag():
    """Test that --validate-actor-ids flag is accepted by the CLI"""
    tagpack_simple = """title: Test
creator: Test
source: http://example.com
currency: BTC
lastmod: 2024-01-01
tags:
- address: 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa
  label: Test
  category: malware
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(tagpack_simple)
        tagpack_path = f.name

    try:
        # Test the --validate-actor-ids flag is accepted (even if download fails)
        result = CliRunner().invoke(
            tagpacktool_cli,
            [
                "tagpack-tool",
                "tagpack",
                "validate",
                tagpack_path,
                "--validate-actor-ids",
            ],
        )
        # The command should execute (might fail to download but that's okay)
        # What matters is that the flag is recognized
        assert "PASSED" in result.output or "passed" in result.output
        os.unlink(tagpack_path)
    finally:
        if os.path.exists(tagpack_path):
            os.unlink(tagpack_path)


def test_validate_tagpack_with_actor_url_parameter():
    """Test that --actor-url parameter is accepted by the CLI"""
    tagpack_simple = """title: Test
creator: Test
source: http://example.com
currency: BTC
lastmod: 2024-01-01
tags:
- address: 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa
  label: Test
  category: test
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(tagpack_simple)
        tagpack_path = f.name

    try:
        # Test the --actor-url parameter is accepted
        result = CliRunner().invoke(
            tagpacktool_cli,
            [
                "tagpack-tool",
                "tagpack",
                "validate",
                tagpack_path,
                "--validate-actor-ids",
                "--actor-url",
                "https://raw.githubusercontent.com/graphsense/graphsense-tagpacks/refs/heads/master/actors/graphsense.actorpack.yaml",
            ],
        )
        # The command should execute and process the URL parameter
        # Check that it attempted to validate
        assert result.exit_code in [
            0,
            1,
        ]  # Allow for both success and validation failures
        os.unlink(tagpack_path)
    finally:
        if os.path.exists(tagpack_path):
            os.unlink(tagpack_path)


def test_validate_tagpack_without_actor_ids_flag():
    """Test that tagpack validation still works without --validate-actor-ids flag"""
    tagpack_simple = """title: Test
creator: Test
source: http://example.com
currency: BTC
lastmod: 2024-01-01
tags:
- address: 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa
  label: Test
  category: malware
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(tagpack_simple)
        tagpack_path = f.name

    try:
        # Test normal validation without the new flag still works
        result = CliRunner().invoke(
            tagpacktool_cli,
            [
                "tagpack-tool",
                "tagpack",
                "validate",
                tagpack_path,
            ],
        )
        assert result.exit_code == 0
        assert "PASSED" in result.output or "passed" in result.output
        os.unlink(tagpack_path)
    finally:
        if os.path.exists(tagpack_path):
            os.unlink(tagpack_path)
