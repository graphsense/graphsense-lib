"""Test fixtures for the new ``Settings`` plumbing.

Risk R9 from the consolidation plan: ``Settings.__init__`` mutates
``os.environ`` to promote legacy-prefixed env vars to the new
``GRAPHSENSE_<sub>__<field>`` form. ``monkeypatch.setenv`` only
undoes its own setenv calls, so without an autouse snapshot a
test that sets ``GS_MCP_PATH`` would leave ``GRAPHSENSE_MCP__PATH``
in the process env for every subsequent test.
"""

from __future__ import annotations

import os

import pytest


# Any var matching one of these prefixes is part of the new or legacy
# config surface and must be reset between tests.
_TRACKED_PREFIXES = (
    "GRAPHSENSE_",
    "GS_CASSANDRA_ASYNC_",
    "GRAPHSENSE_TAGSTORE_READ_",
    "GS_TAGSTORE_",
    "GSREST_",
    "GS_MCP_",
)

# Exact-name vars (no prefix) that also need the snapshot-restore treatment.
_TRACKED_EXACT_VARS = ("CONFIG_FILE",)


@pytest.fixture(autouse=True)
def _reset_env_around_tests(tmp_path_factory):
    """Snapshot env vars touching the config surface; restore after each
    test. Also isolate ``HOME`` so ``~/.graphsense.yaml`` on the developer's
    machine doesn't bleed into tests that construct ``Settings()``.
    """

    def _matches(k: str) -> bool:
        return k.startswith(_TRACKED_PREFIXES) or k in _TRACKED_EXACT_VARS

    snapshot = {k: v for k, v in os.environ.items() if _matches(k)}
    prev_home = os.environ.get("HOME")
    # Point HOME at a fresh empty directory so the default-YAML lookup
    # in _sources.py doesn't find a real config file.
    isolated_home = tmp_path_factory.mktemp("home_isolate")
    os.environ["HOME"] = str(isolated_home)
    yield
    for k in [k for k in os.environ if _matches(k)]:
        os.environ.pop(k, None)
    os.environ.update(snapshot)
    if prev_home is None:
        os.environ.pop("HOME", None)
    else:
        os.environ["HOME"] = prev_home


@pytest.fixture(autouse=True)
def _reset_settings_singleton():
    """Force a fresh Settings build for each test."""
    from graphsenselib.config.settings import reset_settings
    from graphsenselib.config._legacy import _reset_warning_dedup

    reset_settings()
    # Reset the per-process warning dedup so each test sees its own
    # warnings reliably (otherwise the first test consumes them).
    _reset_warning_dedup()
    yield
    reset_settings()
