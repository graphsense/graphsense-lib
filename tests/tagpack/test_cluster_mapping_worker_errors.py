"""Cluster-mapping pool workers: retry, clean shutdown, picklable errors.

Nightly sync 2026-11-24: one worker's batch failed against Cassandra. Its
exception held driver objects (locks) that do not pickle, so the pool could
not send it back and the sync died with ``MaybeEncodingError: ... cannot
pickle '_thread.RLock' object``, the cause nowhere in the log. The worker
also ended with requests in flight ("cannot schedule new futures after
shutdown" per driver retry).

DB-free: the worker runs against monkeypatched TagStore/GraphSense.
"""

import multiprocessing
import pickle
import threading
from types import SimpleNamespace

import pandas as pd
import pytest

pytest.importorskip("yaml_include", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagpack import cli as tp_cli


class UnpicklableDriverError(Exception):
    """Like a driver exception that keeps a connection (with a lock)."""

    def __init__(self, msg):
        super().__init__(msg)
        self.lock = threading.RLock()


class _Conn:
    def __init__(self, log):
        self.log = log

    def close(self):
        self.log.append("pg closed")


class _TagStore:
    log: list = []

    def __init__(self, *a, **kw):
        self.conn = _Conn(self.log)

    def insert_cluster_mappings(self, clusters, fresh=False):
        self.log.append(("insert", fresh, len(clusters)))


class _GraphSense:
    log: list = []
    failures_left = 0

    def __init__(self, *a, **kw):
        pass

    def keyspace_for_network_exists(self, network):
        return True

    def get_address_clusters(self, batch, network, fresh=False):
        if _GraphSense.failures_left > 0:
            _GraphSense.failures_left -= 1
            raise UnpicklableDriverError("ReadTimeout: coordinator timed out")
        return pd.DataFrame({"address": list(batch["address"]), "cluster_id": 7})

    def close(self):
        self.log.append("cassandra closed")


ARGS = SimpleNamespace(
    url="postgresql://x",
    schema="tagstore",
    db_nodes=["x"],
    cassandra_username=None,
    cassandra_password=None,
)
BATCH = pd.DataFrame({"address": ["A", "B"]})


@pytest.fixture
def fakes(monkeypatch):
    log = []
    _TagStore.log = _GraphSense.log = log
    _GraphSense.failures_left = 0
    monkeypatch.setattr(tp_cli, "TagStore", _TagStore)
    monkeypatch.setattr(tp_cli, "GraphSense", _GraphSense)
    monkeypatch.setattr(tp_cli, "CLUSTER_MAPPING_RETRY_DELAY_S", 0)
    return log


def test_a_clean_batch_closes_both_connections(fakes):
    assert tp_cli.insert_cluster_mapping_wp("BTC", {}, ARGS, BATCH, False) == (
        "BTC",
        2,
    )
    assert fakes == [("insert", False, 2), "cassandra closed", "pg closed"]


def test_a_transient_failure_is_retried(fakes):
    _GraphSense.failures_left = 1
    assert tp_cli.insert_cluster_mapping_wp("BTC", {}, ARGS, BATCH, False) == (
        "BTC",
        2,
    )
    # the failed attempt closed its connections too
    assert fakes.count("cassandra closed") == 2 and fakes.count("pg closed") == 2


def test_a_persistent_failure_raises_a_picklable_error_with_the_cause(fakes):
    _GraphSense.failures_left = 5
    with pytest.raises(tp_cli.ClusterMappingBatchError) as info:
        tp_cli.insert_cluster_mapping_wp("LTC", {}, ARGS, BATCH, True)
    err = info.value
    assert "UnpicklableDriverError" in str(err)
    assert "coordinator timed out" in str(err)
    assert "a batch of 2 LTC addresses failed 2 times" in str(err)
    assert "Traceback" in str(err)
    assert isinstance(pickle.loads(pickle.dumps(err)), tp_cli.ClusterMappingBatchError)
    assert fakes.count("cassandra closed") == 2


def test_through_a_real_pool_the_parent_sees_the_cause(fakes):
    """The nightly sync's case: starmap in a pool, the original exception
    unpicklable. Before: MaybeEncodingError without the cause."""
    _GraphSense.failures_left = 5
    ctx = multiprocessing.get_context("fork")  # children inherit the fakes
    with ctx.Pool(processes=1, maxtasksperchild=1) as pool:
        with pytest.raises(tp_cli.ClusterMappingBatchError) as info:
            pool.starmap(
                tp_cli.insert_cluster_mapping_wp,
                [("LTC", {}, ARGS, BATCH, False)],
            )
    assert "coordinator timed out" in str(info.value)
