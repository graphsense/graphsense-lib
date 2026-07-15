"""Wiring tests for `transformation delta-to-raw` --writer / --exchange-rates-from.

Cassandra, the Delta lake, locks, and the Spark factory are stubbed; the
tests assert what the CLI resolves and hands over.
"""

from collections import namedtuple

from click.testing import CliRunner

import graphsenselib.ingest.delta.sink as delta_sink_mod
import graphsenselib.transformation.cli as tcli
import graphsenselib.transformation.factory as factory_mod
import graphsenselib.utils.locking as locking_mod
from graphsenselib.config import get_config
from graphsenselib.config.config import (
    FullTransformArgs,
    IngestConfig,
    KeyspaceConfig,
    SidecarConfig,
)
from graphsenselib.transformation.cli import transformation_cli

_Rate = namedtuple("_Rate", ["date", "fiat_values"])


class _FakeSession:
    def __init__(self):
        self.inserts = []

    def execute(self, statement, params=None):
        if isinstance(statement, str):
            if "system_schema" in statement:
                return []  # target keyspace does not exist yet
            if "exchange_rates" in statement:
                return [_Rate("2024-01-01", {"EUR": 60.0})]
            return []
        self.inserts.append(params)
        return []

    def prepare(self, cql):
        self.prepared_cql = cql
        return object()


class _FakeCluster:
    def __init__(self, session):
        self._session = session

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def connect(self):
        return self._session


class _NullLock:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _stub_environment(monkeypatch, session):
    monkeypatch.setattr(
        tcli, "_cassandra_cluster", lambda *a, **k: _FakeCluster(session)
    )
    monkeypatch.setattr(delta_sink_mod, "delta_lake_highest_block", lambda *a, **k: 500)
    monkeypatch.setattr(locking_mod, "create_lock", lambda name: _NullLock())


def test_delta_to_raw_threads_sidecar_and_exchange_rates(monkeypatch, tmp_path):
    cfg = get_config()
    cfg.full_transform_args = FullTransformArgs(
        sidecar=SidecarConfig(
            contact_points=["node1:9043", "node2:9043"], local_dc="DC1"
        ),
    )
    session = _FakeSession()
    _stub_environment(monkeypatch, session)

    recorded = {}
    monkeypatch.setattr(factory_mod, "run", lambda **kwargs: recorded.update(kwargs))

    result = CliRunner().invoke(
        transformation_cli,
        [
            "transformation",
            "delta-to-raw",
            "-e",
            "pytest",
            "-c",
            "btc",
            "--delta-lake-path",
            str(tmp_path),
            "--writer",
            "sidecar",
            "--exchange-rates-from",
            "pytest_btc_raw_old",
        ],
    )

    assert result.exit_code == 0, result.output
    assert recorded["writer"] == "sidecar"
    assert recorded["sidecar_contact_points"] == ["node1:9043", "node2:9043"]
    assert recorded["sidecar_local_dc"] == "DC1"
    assert recorded["sidecar_consistency_level"] == "LOCAL_QUORUM"
    assert recorded["raw_keyspace"] == "pytest_btc_raw"
    assert recorded["end_block"] == 500

    # exchange_rates seeded from the old keyspace into the target
    assert "pytest_btc_raw.exchange_rates" in session.prepared_cql
    assert session.inserts == [("2024-01-01", {"EUR": 60.0})]


def test_delta_to_raw_defaults_to_cql_writer(monkeypatch, tmp_path):
    session = _FakeSession()
    _stub_environment(monkeypatch, session)

    recorded = {}
    monkeypatch.setattr(factory_mod, "run", lambda **kwargs: recorded.update(kwargs))

    result = CliRunner().invoke(
        transformation_cli,
        [
            "transformation",
            "delta-to-raw",
            "-e",
            "pytest",
            "-c",
            "btc",
            "--delta-lake-path",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert recorded["writer"] == "cassandra"


def test_delta_to_raw_rejects_sidecar_for_account_chains(monkeypatch, tmp_path):
    cfg = get_config()
    cfg.full_transform_args = FullTransformArgs(
        sidecar=SidecarConfig(contact_points=["node1:9043"], local_dc="DC1"),
    )
    cfg.environments["pytest"].keyspaces["eth"] = KeyspaceConfig(
        raw_keyspace_name="pytest_eth_raw",
        transformed_keyspace_name="pytest_eth_transformed",
        schema_type="account",
        ingest_config=IngestConfig(
            node_reference="http://test-data-eth",
            secondary_node_references=[],
            raw_keyspace_file_sinks={},
        ),
        keyspace_setup_config={},
    )

    result = CliRunner().invoke(
        transformation_cli,
        [
            "transformation",
            "delta-to-raw",
            "-e",
            "pytest",
            "-c",
            "eth",
            "--delta-lake-path",
            str(tmp_path),
            "--writer",
            "sidecar",
        ],
    )

    assert result.exit_code != 0
    assert "UTXO" in result.output
