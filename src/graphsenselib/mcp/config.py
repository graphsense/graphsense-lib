from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from graphsenselib.web.config import LoggingConfig


class SearchNeighborsConfig(BaseSettings):
    model_config = SettingsConfigDict(populate_by_name=True)

    base_url: str = Field(..., description="Base URL of the external search service")
    api_key_env: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable holding the API key. "
            "Leave unset to talk to an unauthenticated backend. The variable "
            "itself is read at server boot; its value is never persisted in "
            "config."
        ),
    )
    auth_header: str = Field(
        default="Authorization",
        description="HTTP header used to send the API key (ignored when "
        "api_key_env is unset or the variable is empty)",
    )
    timeout_s: float = Field(
        default=660.0,
        description="Outer HTTP timeout for the underlying httpx client",
    )
    poll_interval_s: float = Field(
        default=1.0,
        description="Seconds between polls of /get_task_state/{task_id}",
    )
    max_poll_time_s: float = Field(
        default=600.0,
        description="Maximum cumulative time spent polling before raising TimeoutError",
    )


class GSMCPConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="GS_MCP_",
        case_sensitive=False,
        env_nested_delimiter="__",
        extra="allow",
    )

    enabled: bool = Field(
        default=True,
        description=(
            "Mount the MCP endpoint inside the FastAPI app. Set to false to "
            "run a pure REST deployment without MCP."
        ),
    )
    path: str = Field(
        default="/mcp",
        description="Path where the MCP endpoint is mounted on the FastAPI app",
    )
    stateless_http: bool = Field(
        default=True,
        description=(
            "Use fastmcp's stateless transport: every request gets a fresh "
            "session. Trade-off: no server-initiated push notifications, but "
            "uvicorn shuts down instantly instead of waiting for all SSE "
            "long-polls to drain. None of our tools need push notifications "
            "(search_neighbors polls inside a single tool call, not via a "
            "server-initiated stream), so the default is True."
        ),
    )

    curation_file: Optional[Path] = Field(
        default=None,
        description="Override path to the curation YAML (defaults to bundled curation/tools.yaml)",
    )
    strict_validation: bool = Field(
        default=True,
        description="Fail boot if curation YAML references unknown operation_ids",
    )

    instructions: Optional[str] = Field(
        default=None,
        description=(
            "Server-provided 'instructions' text sent to MCP clients in the "
            "initialize handshake (the MCP analogue of a system prompt). "
            "When unset, the bundled instructions.md is loaded. Set to an "
            "empty string to suppress instructions entirely."
        ),
    )
    instructions_file: Optional[Path] = Field(
        default=None,
        description=(
            "Override path to the instructions markdown (defaults to bundled "
            "curation/instructions.md)"
        ),
    )
    pathfinder_base_url: str = Field(
        default="https://app.iknaio.com",
        description=(
            "Base URL of the Pathfinder web app (works with the hosted and OSS "
            "deployments). Substituted into the instructions placeholder "
            "`{pathfinder_base_url}` so consumers can build deep links "
            "(e.g. `{pathfinder_base_url}/pathfinder/btc/address/<addr>`). "
            "Trailing slashes are stripped."
        ),
    )

    search_neighbors: Optional[SearchNeighborsConfig] = Field(
        default=None,
        description=(
            "External proprietary search_neighbors forward. When unset, the tool "
            "is not registered."
        ),
    )

    internal_base_url: Optional[str] = Field(
        default=None,
        description=(
            "Base URL the MCP wrappers should use for the REST calls they "
            "fan out to. When unset (default), wrappers dispatch directly "
            "to the FastAPI app via httpx ASGITransport — fast, no network "
            "hop, but the calls don't traverse any external middleware "
            "that may sit in front of the app. When set, wrappers use a "
            "real HTTP client pointed at this URL, so each fan-out call "
            "is a normal HTTP request observable by anything in the path. "
            "Identity is preserved either way — the originating MCP "
            "request's headers are forwarded on every internal call."
        ),
    )

    logging: LoggingConfig = Field(
        default_factory=LoggingConfig,
        description="Logging configuration (shared with the REST app)",
    )

    def bundled_curation_path(self) -> Path:
        return Path(__file__).parent / "curation" / "tools.yaml"

    def resolved_curation_path(self) -> Path:
        return self.curation_file or self.bundled_curation_path()

    def bundled_instructions_path(self) -> Path:
        return Path(__file__).parent / "curation" / "instructions.md"

    def resolved_instructions(self) -> Optional[str]:
        """Resolve the MCP 'instructions' text. Precedence: explicit
        `instructions` (including empty string -> suppress), then
        `instructions_file`, then the bundled instructions.md.
        Returns None when the source file is missing, which tells
        FastMCP to send no instructions at all.
        """
        if self.instructions is not None:
            text = self.instructions or None
        else:
            path = self.instructions_file or self.bundled_instructions_path()
            if not path.exists():
                return None
            text = path.read_text(encoding="utf-8").strip() or None
        if text is None:
            return None
        return text.replace(
            "{pathfinder_base_url}", self.pathfinder_base_url.rstrip("/")
        )
