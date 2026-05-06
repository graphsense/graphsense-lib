import logging
from pathlib import Path

import pytest
import yaml
from fastmcp.exceptions import FastMCPError

from graphsenselib.mcp.config import GSMCPConfig


class _SuppressExpectedToolErrors(logging.Filter):
    """Drop fastmcp's ERROR-level traceback for ToolError / FastMCPError.

    fastmcp's call_tool wrapper logs a full traceback before re-raising,
    which floods the test output for negative-path tests that *expect*
    validation to reject input. Real errors (anything not a FastMCPError)
    still get through.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        exc = record.exc_info
        if exc and isinstance(exc[1], FastMCPError):
            return False
        return True


@pytest.fixture(autouse=True)
def _silence_expected_fastmcp_tool_errors():
    logger = logging.getLogger("fastmcp.server.server")
    flt = _SuppressExpectedToolErrors()
    logger.addFilter(flt)
    try:
        yield
    finally:
        logger.removeFilter(flt)


@pytest.fixture
def sample_curation_dict() -> dict:
    return {
        "version": 1,
        "defaults": {"tag_prefix": "gs_"},
        "include": {
            "get_statistics": {
                "description": "snapshot",
                "tags": ["overview"],
            },
            "get_block": {
                "description": "fetch block",
                "tags": ["block-level"],
            },
        },
        "consolidated_tools": [
            {
                "name": "lookup_address",
                "replaces": ["get_address", "get_address_entity"],
                "module": "graphsenselib.mcp.tools.consolidated:register_lookup_address",
            }
        ],
        "external_tools": {},
    }


@pytest.fixture
def sample_curation_file(tmp_path: Path, sample_curation_dict: dict) -> Path:
    path = tmp_path / "tools.yaml"
    path.write_text(yaml.safe_dump(sample_curation_dict))
    return path


@pytest.fixture
def gsmcp_config(sample_curation_file: Path, monkeypatch) -> GSMCPConfig:
    monkeypatch.delenv("GS_MCP_SEARCH_NEIGHBORS__BASE_URL", raising=False)
    monkeypatch.delenv("GS_MCP_SEARCH_NEIGHBORS__API_KEY_ENV", raising=False)
    return GSMCPConfig(curation_file=sample_curation_file)
