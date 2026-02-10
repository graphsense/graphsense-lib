"""Root conftest: Slack notification on test failures."""

import json
import logging
import os
from pathlib import Path

import pytest
import requests
import yaml

logger = logging.getLogger(__name__)

GRAPHSENSE_CONFIG_PATHS = [
    Path(".graphsense.yaml"),
    Path.home() / ".graphsense.yaml",
]


def _load_slack_webhooks():
    """Load Slack webhook URLs from graphsense config (same as graphsense-lib)."""
    config_file = os.environ.get("GRAPHSENSE_CONFIG_YAML")
    if config_file:
        paths = [Path(config_file)]
    else:
        paths = GRAPHSENSE_CONFIG_PATHS

    for p in paths:
        if p.exists():
            with open(p) as f:
                config = yaml.safe_load(f)
            hooks = (
                config.get("slack_topics", {}).get("exceptions", {}).get("hooks", [])
            )
            if hooks:
                logger.debug(f"Loaded {len(hooks)} Slack webhook(s) from {p}")
                return hooks
    return []


def _send_to_slack(message, webhook):
    """Send a plain text message to Slack via webhook (same format as graphsense-lib)."""
    requests.post(
        webhook,
        json.dumps({"text": message}),
        headers={"Content-type": "application/json"},
        timeout=10,
    )


def pytest_sessionfinish(session, exitstatus):
    """Send Slack notification if any tests failed."""
    if exitstatus == 0:
        return

    webhooks = _load_slack_webhooks()
    if not webhooks:
        return

    # Collect failure info from the terminal reporter
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    failed = reporter.stats.get("failed", []) if reporter else []
    errors = reporter.stats.get("error", []) if reporter else []

    lines = []
    lines.append("*iknaio-tests-nightly: test failures*")
    lines.append(f"Host: `{os.uname().nodename}`")

    passed_count = len(reporter.stats.get("passed", [])) if reporter else 0
    total = passed_count + len(failed) + len(errors)
    lines.append(f"Result: {len(failed)} failed, {len(errors)} errors / {total} total")

    if failed:
        lines.append("")
        lines.append("Failed tests:")
        for report in failed[:15]:
            short_msg = str(report.longrepr).split("\n")[-1][:120] if report.longrepr else ""
            lines.append(f"  - `{report.nodeid}`: {short_msg}")
        if len(failed) > 15:
            lines.append(f"  ... and {len(failed) - 15} more")

    if errors:
        lines.append("")
        lines.append("Errors:")
        for report in errors[:5]:
            short_msg = str(report.longrepr).split("\n")[-1][:120] if report.longrepr else ""
            lines.append(f"  - `{report.nodeid}`: {short_msg}")
        if len(errors) > 5:
            lines.append(f"  ... and {len(errors) - 5} more")

    message = "\n".join(lines)

    for webhook in webhooks:
        logger.info("Sending test failure summary to Slack")
        _send_to_slack(message, webhook)
