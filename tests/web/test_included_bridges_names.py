"""included_bridges is a list of free-form names, and the readers look their
bridge up in it by exact string (defi/conversions.py, defi/bridging/
delegated.py): a misspelled or wrongly cased entry silently disables that
bridge. Startup warns about every entry no reader knows, without failing,
since a config may be shared with an older or newer image."""

import logging

import pytest

from graphsenselib.defi.bridging.delegated import DELEGATED_BRIDGES
from graphsenselib.web.app import create_app_from_dict
from graphsenselib.web.config import KNOWN_BRIDGES, LOCAL_BRIDGES

UNKNOWN_PREFIX = "included_bridges names bridges no reader knows"


def make_app(included_bridges, caplog):
    with caplog.at_level(logging.WARNING, logger="graphsenselib.web.app"):
        app = create_app_from_dict(
            {
                "database": {"nodes": ["localhost"]},
                "included_bridges": list(included_bridges),
            }
        )
    return app, [r for r in caplog.records if r.getMessage().startswith(UNKNOWN_PREFIX)]


def test_known_names_are_the_local_readers_plus_the_delegated_ones():
    """One list, derived from the registries, not a second copy."""
    assert set(KNOWN_BRIDGES) == set(LOCAL_BRIDGES) | set(DELEGATED_BRIDGES)
    assert {"thorchain", "symbiosis", "cctp"} <= set(KNOWN_BRIDGES)
    assert list(KNOWN_BRIDGES) == sorted(set(KNOWN_BRIDGES))


@pytest.mark.parametrize(
    "included_bridges", [(), KNOWN_BRIDGES, ("cctp",), ("thorchain", "symbiosis")]
)
def test_known_names_do_not_warn(included_bridges, caplog):
    app, warnings = make_app(included_bridges, caplog)
    assert app is not None
    assert warnings == []


def test_each_unknown_name_is_warned_about_with_the_known_names(caplog):
    """`ccpt` is the finding; `CCTP` is wrong too, since the readers compare
    case-sensitively. Startup still succeeds."""
    app, warnings = make_app(("ccpt", "thorchain", "CCTP"), caplog)
    assert app is not None
    assert len(warnings) == 1
    record = warnings[0]
    assert record.levelno == logging.WARNING
    message = record.getMessage()
    assert "ccpt, CCTP" in message
    assert "thorchain" not in message.split("known")[0]
    assert ", ".join(KNOWN_BRIDGES) in message
    assert "case-sensitive" in message


def test_the_warning_is_logged_once_per_startup(caplog):
    app, warnings = make_app(("ccpt", "ccpt"), caplog)
    assert app is not None
    assert len(warnings) == 1
    assert warnings[0].getMessage().count("ccpt") == 1
