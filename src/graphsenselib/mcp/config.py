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

    search_neighbors: Optional[SearchNeighborsConfig] = Field(
        default=None,
        description=(
            "External proprietary search_neighbors forward. When unset, the tool "
            "is not registered."
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
