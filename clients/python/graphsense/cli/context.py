"""Shared Click context for all `gs` commands."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from typing import Optional

import rich_click as click

from graphsense.ext.client import GraphSense


@dataclass
class CliContext:
    api_key: Optional[str] = None
    host: Optional[str] = None
    format: Optional[str] = None
    output: Optional[str] = None
    directory: Optional[str] = None
    input: Optional[str] = None
    input_format: str = "auto"
    # Primary id selectors. "address" in the flag name is conventional —
    # for lookup-tx / lookup-cluster these extract tx hashes / cluster ids.
    address_jq: Optional[str] = None
    address_col: Optional[str] = None
    # Per-row network selectors — see docs/cli/inputs.md.
    # "network" is the preferred term in new code; the generated API still
    # uses "currency" for backward compatibility (see CLAUDE.md).
    network_jq: Optional[str] = None
    network_col: Optional[str] = None
    bulk: Optional[bool] = None
    bulk_threshold: int = 10
    color: str = "auto"
    quiet: bool = False
    verbose: int = 0
    _gs: Optional[GraphSense] = field(default=None, repr=False)

    def gs(self) -> GraphSense:
        if self._gs is None:
            self._gs = GraphSense(
                api_key=self.api_key,
                host=self.host,
                quiet_deprecation=self.quiet,
                show_deprecated=os.environ.get(
                    "GRAPHSENSE_CLIENT_SHOW_DEPRECATED_ENDPOINTS"
                )
                == "1",
                # Resolve click's current stderr stream on each write so
                # `CliRunner(mix_stderr=False)` on click 8.1 captures it.
                deprecation_stream=lambda: click.get_text_stream("stderr"),
            )
        return self._gs

    def read_input_text(self) -> Optional[str]:
        """Read the input blob from --input or, if stdin is piped, from stdin."""
        if self.input:
            with open(self.input, "r", encoding="utf-8") as fh:
                return fh.read()
        if not sys.stdin.isatty():
            data = sys.stdin.read()
            return data if data else None
        return None
