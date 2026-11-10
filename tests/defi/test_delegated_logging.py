"""A DepositForBurn that is no half bridge (unfunded, or without the
TokenMinter's burn: V1's replaceDepositForBurn) is said so at INFO, with
``event=cctp_half_skipped`` and the reason, so the gs-rest side of "did a
burn get skipped" can be answered from the log."""

import logging
import re

import pytest

from graphsenselib.defi.bridging.delegated import CCTP, half_bridges

from tests.defi.test_delegated_bridges import FIXTURE, bare, db_rows

LOGGER = "graphsenselib.defi.bridging.delegated"
_FIELD = re.compile(r"(\w+)=(\S+)")


def skipped_lines(caplog):
    return [
        (r, dict(_FIELD.findall(r.getMessage().rpartition(" | ")[2])))
        for r in caplog.records
        if "event=cctp_half_skipped" in r.getMessage()
    ]


def test_an_unfunded_burn_is_logged_at_info_with_its_reason(caplog):
    logs = [
        log
        for log in FIXTURE["v2_burn"]["logs"]
        if log["log_index"] != 694  # the funding Transfer
    ]
    with caplog.at_level(logging.INFO, logger=LOGGER):
        assert half_bridges("eth", db_rows("v2_burn", logs), CCTP) == ()
    ((record, fields),) = skipped_lines(caplog)
    assert record.levelno == logging.INFO
    assert "not a burn" in record.getMessage()
    assert fields["reason"] == "unfunded_burn"
    assert fields["tx"] == "0x" + bare("v2_burn")
    assert fields["log_index"] == "698"
    assert fields["version"] == "2"


@pytest.mark.parametrize(
    "part, burn, log_index, version",
    [("v1_burn", 48, "50", "1"), ("v2_burn", 696, "698", "2")],
)
def test_a_burn_without_the_minters_burn_is_logged_with_its_reason(
    caplog, part, burn, log_index, version
):
    logs = [log for log in FIXTURE[part]["logs"] if log["log_index"] != burn]
    with caplog.at_level(logging.INFO, logger=LOGGER):
        assert half_bridges("eth", db_rows(part, logs), CCTP) == ()
    ((_, fields),) = skipped_lines(caplog)
    assert fields["reason"] == "no_minter_burn"
    assert fields["tx"] == "0x" + bare(part)
    assert fields["log_index"] == log_index
    assert fields["version"] == version


def test_a_real_burn_logs_nothing(caplog):
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        assert len(half_bridges("eth", db_rows("v2_burn"), CCTP)) == 1
    assert skipped_lines(caplog) == []


def test_a_handler_failing_on_the_skipped_line_leaves_the_answer():
    """A logging failure never changes an answer: the skipped burn is still
    no half bridge, and the tx's real bridges are still found."""
    logger = logging.getLogger(LOGGER)

    class Faulty(logging.Handler):
        def emit(self, record):
            if "event=cctp_half_skipped" in record.getMessage():
                raise RuntimeError("handler failed")

    handler = Faulty(level=logging.INFO)
    logger.addHandler(handler)
    previous = logger.level
    logger.setLevel(logging.INFO)
    try:
        logs = [
            log
            for log in FIXTURE["v2_burn"]["logs"]
            if log["log_index"] != 694  # the funding Transfer
        ]
        assert half_bridges("eth", db_rows("v2_burn", logs), CCTP) == ()
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)
