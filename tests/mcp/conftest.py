import logging
from pathlib import Path

import pytest
import yaml

try:
    from fastmcp.exceptions import FastMCPError

    from graphsenselib.mcp.config import GSMCPConfig
except ImportError:
    # Skip the entire tests/mcp directory when the [mcp] extra isn't installed
    # (e.g. the base-deps CI matrix). collect_ignore_glob is honored during
    # test collection, unlike pytest.importorskip in a conftest body which
    # raises during conftest import and surfaces as a hard error.
    collect_ignore_glob = ["test_*.py"]
else:

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

    from graphsenselib.web.file_store import FileTooLargeError, StoredFile

    class InMemoryFileStore:
        """In-memory FileStore double for MCP tool tests.

        Satisfies the graphsenselib.web.file_store.FileStore protocol without
        Redis, and records every put so tests can assert on stored bytes.
        """

        def __init__(
            self,
            *,
            max_bytes: int = 5 * 1024 * 1024,
            base_url: str = "https://files.example.test",
            download_path: str = "/download",
        ) -> None:
            self._max_bytes = max_bytes
            self._base_url = base_url.rstrip("/")
            self._download_path = "/" + download_path.strip("/")
            self.files: dict[str, StoredFile] = {}
            self._counter = 0

        async def put(self, data, *, filename, content_type) -> str:
            if len(data) > self._max_bytes:
                raise FileTooLargeError(
                    f"file is {len(data)} bytes, exceeds {self._max_bytes}"
                )
            self._counter += 1
            token = f"testtoken{self._counter:020d}"
            self.files[token] = StoredFile(
                filename=filename, content_type=content_type, data=data
            )
            return token

        async def get(self, token):
            return self.files.get(token)

        def url_for(self, request, token) -> str:  # noqa: ARG002 — fake ignores request
            return f"{self._base_url}{self._download_path}/{token}"

    @pytest.fixture
    def make_file_store():
        """Factory for InMemoryFileStore instances (optionally size-capped)."""
        return InMemoryFileStore
