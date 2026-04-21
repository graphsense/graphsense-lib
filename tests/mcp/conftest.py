from pathlib import Path

import pytest
import yaml

from graphsenselib.mcp.config import GSMCPConfig


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
