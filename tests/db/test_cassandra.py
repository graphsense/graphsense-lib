import pytest

from graphsenselib.db import DbFactory
from graphsenselib.db.cassandra import CassandraDb, build_delete_stmt, build_select_stmt
from graphsenselib.schema.schema import GraphsenseSchemas


def test_cassandra_db_default_port():
    db = CassandraDb(["host-a", "host-b"])
    assert db.db_nodes == ["host-a", "host-b"]
    assert db.db_port == 9042


def test_cassandra_db_explicit_port():
    db = CassandraDb(["host-a:9043", "host-b:9043"])
    assert db.db_nodes == ["host-a", "host-b"]
    assert db.db_port == 9043


def test_cassandra_db_partial_port_inherits():
    db = CassandraDb(["host-a:9043", "host-b"])
    assert db.db_nodes == ["host-a", "host-b"]
    assert db.db_port == 9043


def test_cassandra_db_mixed_ports_warn_and_use_first(caplog):
    with caplog.at_level("WARNING"):
        db = CassandraDb(["host-a:9042", "host-b:9043"])
    assert db.db_port == 9042
    assert db.db_nodes == ["host-a", "host-b"]
    assert any("conflicting ports" in r.message for r in caplog.records)


def test_build_select_stmt_limit():
    assert (
        build_select_stmt(
            "test", columns=["a", "b"], keyspace="foo", where={"a": 1}, limit=2
        )
        == "SELECT a,b FROM foo.test WHERE a=1 LIMIT 2;"
    )
    assert (
        build_select_stmt(
            "test",
            columns=["a", "b"],
            keyspace="foo",
            where={"a": 1},
            limit=2,
            per_partition_limit=3,
        )
        == "SELECT a,b FROM foo.test WHERE a=1 PER PARTITION LIMIT 3;"
    )


def test_build_select_stmt_per_partition_limit():
    assert (
        build_select_stmt(
            "test",
            columns=["a", "b"],
            keyspace="foo",
            where={"a": 1},
            limit=2,
            per_partition_limit=3,
        )
        == "SELECT a,b FROM foo.test WHERE a=1 PER PARTITION LIMIT 3;"
    )


def test_build_delete_stmt():
    assert (
        build_delete_stmt(
            key_columns=["no_blocks"],
            table="summary_statistics",
            keyspace="test_transformed",
        )
        == "DELETE FROM test_transformed.summary_statistics WHERE no_blocks=?;"
    )


def test_patched_config():
    from graphsenselib.config import get_config

    assert list(get_config().environments.keys()) == ["pytest"]

    from graphsenselib.config.config import get_config

    assert list(get_config().environments.keys()) == ["pytest"]

    config = get_config()
    config.model_validate(config)


@pytest.mark.slow
def test_cassandra_create_schema(gs_db_setup):
    # create BTC schema
    GraphsenseSchemas().create_keyspaces_if_not_exist("pytest", "btc")

    # query data in config table
    with DbFactory().from_config("pytest", "btc") as db:
        c = db.raw.get_configuration()
        assert c.id == "pytest_btc_raw"
