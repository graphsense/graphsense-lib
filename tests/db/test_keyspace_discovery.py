import logging

import pytest
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.query import dict_factory

from graphsenselib.db.asynchronous.cassandra import Cassandra
from tests.conftest import create_discovery_keyspaces


@pytest.fixture(scope="module")
def discovery_db(gs_db_setup):
    cas_host, cas_port = gs_db_setup
    cluster = Cluster(
        [cas_host],
        port=cas_port,
        execution_profiles={
            EXEC_PROFILE_DEFAULT: ExecutionProfile(row_factory=dict_factory)
        },
    )
    session = cluster.connect()

    # Idempotent: the baked image already has these, CREATE IF NOT EXISTS is a no-op.
    # In the vanilla path, tests/db/ fixtures don't call create_web_schemas, so the
    # keyspaces need to be created here.
    create_discovery_keyspaces(session)

    db = Cassandra.__new__(Cassandra)
    db.session = session
    db.logger = logging.getLogger("test_keyspace_discovery")

    yield db

    cluster.shutdown()


def test_find_latest_transformed_picks_highest_no_blocks(discovery_db):
    # Candidates with no_blocks: 20260101=100, 20260201=500, 20260401=200.
    # 20260301 has an empty summary_statistics -> skipped as "not online".
    # Date is only a prefilter; selection is by no_blocks, so 20260201 wins
    # even though 20260401 is newer.
    assert (
        discovery_db.find_latest_transformed_keyspace("disctest_btc")
        == "disctest_btc_transformed_20260201"
    )


def test_find_latest_raw_picks_newest_dated(discovery_db):
    # 20260301 is the newest with a populated configuration row.
    # _20260201_prod has a non-parseable trailing segment -> filtered out.
    # Static disctest_btc_raw must not be picked when dated candidates exist.
    assert (
        discovery_db.find_latest_raw_keyspace("disctest_btc")
        == "disctest_btc_raw_20260301"
    )


def test_find_latest_raw_falls_back_to_static(discovery_db):
    # No keyspace matches this prefix -> function returns the static fallback
    # without probing anything.
    assert discovery_db.find_latest_raw_keyspace("fallbacktest") == "fallbacktest_raw"


def test_find_latest_raw_skips_incomplete_newer(discovery_db):
    # disctest_btc_raw_20260401 is newer than _20260301 but its state table
    # has no ingest_complete row -> must be skipped, _20260301 still wins.
    assert (
        discovery_db.find_latest_raw_keyspace("disctest_btc")
        == "disctest_btc_raw_20260301"
    )


def test_find_latest_transformed_skips_incomplete_with_higher_no_blocks(
    discovery_db,
):
    # disctest_btc_transformed_20260501 has no_blocks=999 (highest) but its
    # state table is missing the ingest_complete row -> must be skipped.
    # _20260201 (no_blocks=500, ingest_complete) remains the winner.
    assert (
        discovery_db.find_latest_transformed_keyspace("disctest_btc")
        == "disctest_btc_transformed_20260201"
    )


def test_find_latest_raw_back_compat_no_state_table(discovery_db):
    # discbc_btc_raw_20260301 has no state table; back-compat probe falls
    # through to checking the configuration table, which is populated.
    assert (
        discovery_db.find_latest_raw_keyspace("discbc_btc") == "discbc_btc_raw_20260301"
    )


def test_find_latest_transformed_back_compat_no_state_table(discovery_db):
    # discbc_btc_transformed_20260301 has no state table; back-compat falls
    # through to checking summary_statistics, which has a row.
    assert (
        discovery_db.find_latest_transformed_keyspace("discbc_btc")
        == "discbc_btc_transformed_20260301"
    )


def test_find_latest_raw_skips_state_table_with_only_other_keys(discovery_db):
    # discother_btc_raw_20260301 has a populated state table but no
    # `ingest_complete` row (only an `in_progress` row). Discovery must skip
    # it and fall back to the static name — the state table being non-empty
    # must NOT count as ingest_complete.
    assert discovery_db.find_latest_raw_keyspace("discother_btc") == "discother_btc_raw"
