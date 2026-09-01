"""The writer's conformance check is the payoff for schema-as-a-model: a column
mismatch fails at job start rather than partway through a multi-hour run."""

import pytest

from graphsense_v3.schema.definitions import transformed
from graphsense_v3.schema.model import Family
from graphsense_v3.spark.writer import conformance_errors

STATS = transformed(Family.UTXO).table("address_stats")
KEY_COLUMNS = ["address_bucket", "address", "epoch"]


def test_accepts_a_conforming_subset() -> None:
    """Cassandra ignores a column absent from an INSERT, so a partial write is
    legitimate -- an epoch delta row carries sums but no degrees."""
    assert conformance_errors([*KEY_COLUMNS, "no_incoming_txs"], STATS) == []


def test_rejects_unknown_column() -> None:
    errors = conformance_errors([*KEY_COLUMNS, "no_such_column"], STATS)
    assert errors and "no_such_column" in errors[0]


def test_rejects_missing_key_column() -> None:
    errors = conformance_errors(["address_bucket", "address"], STATS)
    assert errors and "epoch" in errors[0]


def test_rejects_duplicate_column() -> None:
    errors = conformance_errors([*KEY_COLUMNS, "address"], STATS)
    assert any("duplicated" in e for e in errors)


def test_reports_every_problem_at_once() -> None:
    """One run should surface all of them, not the first."""
    errors = conformance_errors(["address_bucket", "bogus"], STATS)
    assert len(errors) == 2


@pytest.mark.parametrize("family", list(Family))
def test_link_table_key_columns_are_required(family: Family) -> None:
    table = transformed(family).table("address_link_transactions")
    errors = conformance_errors(["src_address"], table)
    assert errors, "a write missing the counterparty must be rejected"
