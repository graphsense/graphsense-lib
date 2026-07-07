import os
import re
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from graphsenselib.web.config import LoggingConfig

# Mirrors the docs static mount in web/app.py (DOCS_STATIC_URL / DOCS_STATIC_DIR).
# Kept as local constants to avoid importing the heavy web.app module just for
# two paths; the REST app serves `docs/static/` at `/docs_assets`.
_DOCS_STATIC_URL = "/docs_assets"
_DOCS_STATIC_DIR = "./docs/static"
_DEFAULT_ICON_FILENAME = "favicon.png"

# Feature-gated instructions block for the open-in-Pathfinder deep link
# (see `pathfinder_open_url_enabled`). The block regex removes the whole
# marked region including the markers; the marker regex removes only the
# marker lines, keeping the content.
_OPEN_URL_MARKER_RE = re.compile(r"[ \t]*<!-- /?feature:pathfinder-open-url -->\n?")
_OPEN_URL_BLOCK_RE = re.compile(
    r"[ \t]*<!-- feature:pathfinder-open-url -->\n?"
    r".*?"
    r"<!-- /feature:pathfinder-open-url -->\n?",
    re.DOTALL,
)


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

    pathfinder_open_url_enabled: bool = Field(
        default=False,
        description=(
            "Feature flag for the open-in-Pathfinder deep link. When true, "
            "build_pathfinder_file mints an `open_url` "
            "(`{pathfinder_base_url}/pathfinder?import=<token>`) alongside "
            "the download link and the feature is advertised in the tool "
            "description and server instructions. Off by default; only "
            "enable where the Pathfinder deployment fetches its imports "
            "from this REST host (the `?import=` loader resolves the token "
            "against the dashboard's configured REST URL)."
        ),
    )

    website_url: Optional[str] = Field(
        default="https://www.iknaio.com/",
        description=(
            "`websiteUrl` advertised in the MCP initialize handshake "
            "(serverInfo). Some hosts surface it as a link next to the "
            "connector. Set to an empty string to suppress it."
        ),
    )
    icon_url: Optional[str] = Field(
        default=None,
        description=(
            "Override URL of the connector icon advertised in the MCP "
            "initialize handshake (serverInfo `icons`). When unset, the "
            "bundled favicon served by the REST app at "
            "`/docs_assets/favicon.png` is used (same asset as the API docs), "
            "a root-relative URL the host resolves against the server origin. "
            "Set an absolute, publicly reachable, unauthenticated URL for "
            "hosts that don't resolve relative refs; use a URL, not a data "
            "URI, to keep the handshake small. Note: not all hosts read this "
            "field (e.g. Mistral derives the icon from the origin favicon "
            "instead). See `resolved_icon_url`."
        ),
    )
    icon_mime_type: str = Field(
        default="image/png",
        description="MIME type reported for `icon_url` (ignored when icon_url is unset)",
    )
    icon_sizes: Optional[str] = Field(
        default="any",
        description=(
            "Optional `sizes` hint reported for `icon_url` (e.g. '48x48' or "
            "'any'); ignored when icon_url is unset. Set to empty to omit."
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

    def resolved_icon_url(self) -> Optional[str]:
        """Resolve the connector icon URL for the MCP initialize handshake.

        Precedence: explicit `icon_url` (including empty string -> suppress),
        then the bundled favicon served by the REST app at
        `/docs_assets/favicon.png` when that asset is present in the container
        (the same file the API docs use), else None. Mirrors the docs favicon
        fallback in web/app.py so the connector icon matches the API docs.
        """
        if self.icon_url is not None:
            return self.icon_url or None
        if os.path.isfile(f"{_DOCS_STATIC_DIR}/{_DEFAULT_ICON_FILENAME}"):
            return f"{_DOCS_STATIC_URL}/{_DEFAULT_ICON_FILENAME}"
        return None

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

        Feature-gated blocks: text wrapped in
        ``<!-- feature:pathfinder-open-url -->`` /
        ``<!-- /feature:pathfinder-open-url -->`` marker lines is kept
        (markers stripped) when `pathfinder_open_url_enabled` is true and
        removed entirely otherwise.
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
        if self.pathfinder_open_url_enabled:
            text = _OPEN_URL_MARKER_RE.sub("", text)
        else:
            text = _OPEN_URL_BLOCK_RE.sub("", text)
        text = text.strip()
        return text.replace(
            "{pathfinder_base_url}", self.pathfinder_base_url.rstrip("/")
        )
