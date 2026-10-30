"""Uncaught-exception tracebacks must not print local variables by default:
they hold DB URLs with passwords and end up in cron logs (2026: the tagstore
sync wrote its Postgres password into the log on every failure)."""

import importlib
import os
import subprocess
import sys

import pytest

# by full name: graphsenselib.utils itself imports the stdlib `logging`
gs_logging = importlib.import_module("graphsenselib.utils.logging")

SENTINEL = "SENTINEL-PASSWORD-must-not-be-printed"


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, False),
        ("", False),
        ("0", False),
        ("false", False),
        ("1", True),
        ("true", True),
        (" YES ", True),
        ("on", True),
    ],
)
def test_traceback_show_locals(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv(gs_logging.TRACEBACK_LOCALS_ENV, raising=False)
    else:
        monkeypatch.setenv(gs_logging.TRACEBACK_LOCALS_ENV, value)
    assert gs_logging.traceback_show_locals() is expected


@pytest.mark.parametrize("value, expected", [(None, False), ("1", True)])
def test_install_tracebacks_passes_show_locals(monkeypatch, value, expected):
    calls = []
    monkeypatch.setattr(
        gs_logging, "install_rich_traceback", lambda **kw: calls.append(kw)
    )
    if value is None:
        monkeypatch.delenv(gs_logging.TRACEBACK_LOCALS_ENV, raising=False)
    else:
        monkeypatch.setenv(gs_logging.TRACEBACK_LOCALS_ENV, value)
    gs_logging.install_tracebacks()
    assert len(calls) == 1
    assert calls[0]["show_locals"] is expected


def _failing_login(show_locals: bool) -> subprocess.CompletedProcess:
    """graphsense-cli with a DB URL built from the environment and a refused
    port (fast everywhere, see CLAUDE.md), so the command dies with an
    uncaught exception while the URL with the password is a local variable."""
    env = {k: v for k, v in os.environ.items() if k != gs_logging.TRACEBACK_LOCALS_ENV}
    env.update(
        POSTGRES_HOST="127.0.0.1",
        POSTGRES_PORT="1",
        POSTGRES_DB="tagstore",
        POSTGRES_USER="someone",
        POSTGRES_PASSWORD=SENTINEL,
    )
    if show_locals:
        env[gs_logging.TRACEBACK_LOCALS_ENV] = "1"
    code = (
        "import sys; sys.argv = ['graphsense-cli', 'tagpack-tool', 'tagstore', "
        "'show-composition', '--csv']; "
        "from graphsenselib.cli.main import main; main()"
    )
    return subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


def _tagpack_tool_available() -> bool:
    try:
        import graphsenselib.tagpack.cli  # noqa: F401
    except ImportError:
        return False
    return True


@pytest.mark.skipif(not _tagpack_tool_available(), reason="tagpack extra missing")
def test_failing_cli_does_not_print_the_password():
    r = _failing_login(show_locals=False)
    assert r.returncode != 0
    assert "Traceback" in r.stderr or "Error" in r.stderr
    assert SENTINEL not in r.stdout + r.stderr


@pytest.mark.skipif(not _tagpack_tool_available(), reason="tagpack extra missing")
def test_failing_cli_prints_locals_only_on_request():
    # control: proves the test above would see a leak
    r = _failing_login(show_locals=True)
    assert r.returncode != 0
    assert SENTINEL in r.stdout + r.stderr
