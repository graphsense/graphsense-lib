"""UtxoTransformation writer selection: CQL connector vs Sidecar bulk write.

No Spark session needed — the DataFrame is faked and the write sinks are
recorded, mirroring tests/transformation/test_sidecar.py.
"""

import pytest

import graphsenselib.transformation.factory as factory_mod
import graphsenselib.transformation.utxo as utxo_mod
from graphsenselib.transformation.spark import _apply_sidecar_settings
from graphsenselib.transformation.sidecar import SIDECAR_PACKAGE
from graphsenselib.transformation.utxo import UtxoTransformation


class _WriterRecorder:
    def __init__(self):
        self.calls = []
        self._format = None
        self._options = None
        self._mode = None

    def format(self, fmt):
        self._format = fmt
        return self

    def options(self, **opts):
        self._options = opts
        return self

    def mode(self, mode):
        self._mode = mode
        return self

    def save(self):
        self.calls.append(
            {"format": self._format, "options": self._options, "mode": self._mode}
        )


class _FakeDataFrame:
    def __init__(self):
        self.write = _WriterRecorder()
        self.columns = ["tx_id_group", "tx_id"]

    def repartition(self, *cols):
        return self


def _transformer(**kwargs):
    return UtxoTransformation(
        spark=None,
        delta_lake_path="s3a://unused",
        raw_keyspace="ltc_raw_healed",
        network="ltc",
        **kwargs,
    )


def test_default_writer_uses_cql_connector():
    t = _transformer()
    df = _FakeDataFrame()

    t._write_cassandra(df, "transaction", partition_key="tx_id_group")

    assert df.write.calls == [
        {
            "format": "org.apache.spark.sql.cassandra",
            "options": {"table": "transaction", "keyspace": "ltc_raw_healed"},
            "mode": "append",
        }
    ]


def test_sidecar_writer_routes_through_bulk_write(monkeypatch):
    recorded = {}

    def fake_bulk_write(df, **kwargs):
        recorded["df"] = df
        recorded.update(kwargs)

    monkeypatch.setattr(utxo_mod, "bulk_write_dataframe", fake_bulk_write)

    t = _transformer(
        writer="sidecar",
        sidecar_contact_points=["node1:9043", "node2:9043"],
        sidecar_local_dc="DC1",
    )
    df = _FakeDataFrame()

    t._write_cassandra(df, "transaction", partition_key="tx_id_group")

    assert recorded["df"] is df
    assert recorded["keyspace"] == "ltc_raw_healed"
    assert recorded["table"] == "transaction"
    assert recorded["contact_points"] == ["node1:9043", "node2:9043"]
    assert recorded["local_dc"] == "DC1"
    assert recorded["consistency_level"] == "LOCAL_QUORUM"
    assert df.write.calls == []


def test_sidecar_writer_without_contact_points_fails_fast():
    with pytest.raises(ValueError, match="contact_points"):
        _transformer(writer="sidecar")


def test_sidecar_writer_local_quorum_without_dc_fails_fast():
    with pytest.raises(ValueError, match="local_dc"):
        _transformer(writer="sidecar", sidecar_contact_points=["node1:9043"])


def test_factory_rejects_sidecar_for_account_chains():
    with pytest.raises(ValueError, match="account"):
        factory_mod.run(
            env="prod",
            currency="eth",
            delta_lake_path="s3a://unused",
            cassandra_nodes=["localhost"],
            end_block=1,
            writer="sidecar",
            sidecar_contact_points=["node1:9043"],
            sidecar_local_dc="DC1",
        )


def test_apply_sidecar_settings_adds_package_and_jvm_flags():
    packages, spark_config = _apply_sidecar_settings(
        ["some:pkg:1.0"], {"spark.local.dir": "/data/spark-tmp"}
    )
    assert packages == ["some:pkg:1.0", SIDECAR_PACKAGE]
    assert (
        "-Djava.io.tmpdir=/data/spark-tmp"
        in spark_config["spark.driver.extraJavaOptions"]
    )
    assert "jdk.internal.misc" in spark_config["spark.executor.extraJavaOptions"]


def test_apply_sidecar_settings_requires_spark_local_dir():
    with pytest.raises(ValueError, match="spark.local.dir"):
        _apply_sidecar_settings(["some:pkg:1.0"], {})
