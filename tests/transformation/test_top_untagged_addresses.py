"""Tests for the top-untagged-addresses Spark job.

Runs in pure local Spark. The Cassandra read is stubbed with in-memory
DataFrames; the tagstore is stubbed at the psycopg2 connection, so the real
probe path (chunking, parameter order, network scoping) is exercised.
"""

import csv
import glob
import os

import pytest

from graphsenselib.untagged.job import TopUntaggedAddresses, psycopg2_dsn

pyspark = pytest.importorskip("pyspark")


def _currency_type():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("value", T.LongType()),
            T.StructField("fiat_values", T.ArrayType(T.FloatType())),
        ]
    )


def _utxo_addresses(spark):
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("address_id", T.IntegerType()),
            T.StructField("address", T.StringType()),
            T.StructField("cluster_id", T.IntegerType()),
            T.StructField("no_incoming_txs", T.IntegerType()),
            T.StructField("no_outgoing_txs", T.IntegerType()),
            T.StructField("in_degree", T.IntegerType()),
            T.StructField("out_degree", T.IntegerType()),
            T.StructField("total_received", _currency_type()),
        ]
    )
    rows = [
        (1, "1TaggedBusy", 10, 900, 100, 50, 50, (1000, [9000.0, 8000.0])),
        (2, "1UntaggedTop", 11, 500, 100, 10, 10, (500, [500.0, 450.0])),
        (3, "1ClusterTagged", 10, 400, 50, 300, 300, (99, [99.0, 90.0])),
        (4, "1Small", 12, 1, 0, 1, 1, (1, [1.0, 0.9])),
        (5, "1NullMetrics", 13, None, 7, None, None, (7, None)),
    ]
    return spark.createDataFrame(rows, schema)


def _account_addresses(spark):
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("address_id", T.IntegerType()),
            T.StructField("address", T.BinaryType()),
            T.StructField("no_incoming_txs", T.IntegerType()),
            T.StructField("no_outgoing_txs", T.IntegerType()),
            T.StructField("in_degree", T.IntegerType()),
            T.StructField("out_degree", T.IntegerType()),
            T.StructField("total_received", _currency_type()),
        ]
    )
    rows = [
        (1, bytes.fromhex("aa" * 20), 900, 100, 5, 5, (1, [10.0, 9.0])),
        (2, bytes.fromhex("bb" * 20), 500, 100, 5, 5, (1, [20.0, 18.0])),
    ]
    return spark.createDataFrame(rows, schema)


class FakeCursor:
    """Answers the job's two probe queries out of in-memory sets."""

    def __init__(self, tagged, tagged_clusters, executed):
        self._tagged = set(tagged)
        self._tagged_clusters = set(tagged_clusters)
        self._executed = executed
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params):
        self._executed.append((query, params))
        if "best_cluster_tag" in query:
            network, chunk = params
            assert network.isupper(), "network must be upper-cased for the tagstore"
            self._rows = [(c,) for c in chunk if c in self._tagged_clusters]
        else:
            (chunk,) = params
            self._rows = [(a,) for a in chunk if a in self._tagged]

    def __iter__(self):
        return iter(self._rows)


class FakeConnection:
    def __init__(self, tagged, tagged_clusters, executed):
        self._args = (tagged, tagged_clusters, executed)
        self.closed = False

    def cursor(self):
        return FakeCursor(*self._args)

    def close(self):
        self.closed = True


def _make_job(spark, currency, schema_type, addresses, tagged, tagged_clusters):
    """Build the job with its Cassandra read and tagstore connection stubbed."""
    job = TopUntaggedAddresses.__new__(TopUntaggedAddresses)
    job.spark = spark
    job.currency = currency
    job.schema_type = schema_type
    job.transformed_keyspace = "test_transformed"
    job.tagstore_schema = "public"
    job.dsn = "unused"
    job.executed = []
    job.connections = []

    def _connect():
        conn = FakeConnection(tagged, tagged_clusters, job.executed)
        job.connections.append(conn)
        return conn

    job._read_addresses = lambda: addresses
    job._connect = _connect
    return job


def _read_csv(path):
    with open(glob.glob(os.path.join(path, "*.csv"))[0]) as fh:
        return list(csv.DictReader(fh))


@pytest.fixture
def utxo_job(spark):
    def _make(tagged=("1TaggedBusy",), tagged_clusters=(10,)):
        return _make_job(
            spark, "btc", "utxo", _utxo_addresses(spark), tagged, tagged_clusters
        )

    return _make


def test_dsn_strips_sqlalchemy_driver_suffix():
    assert (
        psycopg2_dsn("postgresql+asyncpg://gs:secret@db:5433/tagstore")
        == "postgresql://gs:secret@db:5433/tagstore"
    )


def test_dsn_passes_through_plain_url():
    assert (
        psycopg2_dsn("postgresql://gs@localhost/tagstore")
        == "postgresql://gs@localhost/tagstore"
    )


def test_dsn_without_database_is_rejected():
    with pytest.raises(ValueError, match="no database component"):
        psycopg2_dsn("postgresql://localhost:5432/")


def test_probe_chunks_large_inputs_and_unions_results(utxo_job, monkeypatch):
    import graphsenselib.untagged.job as job_module

    monkeypatch.setattr(job_module, "PROBE_CHUNK_SIZE", 3)
    addresses = [f"addr{i}" for i in range(10)]
    job = utxo_job(tagged=["addr0", "addr4", "addr9"])

    assert job.probe_tagged_identifiers(addresses) == {"addr0", "addr4", "addr9"}
    # 10 addresses / chunk size 3 -> 4 round trips, one connection, closed after.
    assert len(job.executed) == 4
    assert [len(params[-1]) for _, params in job.executed] == [3, 3, 3, 1]
    assert len(job.connections) == 1
    assert job.connections[0].closed


def test_probe_skips_the_database_entirely_when_there_is_nothing_to_probe(utxo_job):
    job = utxo_job()
    assert job.probe_tagged_identifiers([]) == set()
    assert job.probe_tagged_clusters([]) == set()
    assert job.connections == []


def test_cluster_probe_is_network_scoped(utxo_job):
    job = utxo_job(tagged_clusters=[10, 11])
    assert job.probe_tagged_clusters([10, 12]) == {10}

    query, params = job.executed[0]
    assert "network = %s" in query
    assert params[0] == "BTC"


def test_utxo_excludes_tagged_and_ranks_by_tx_count(utxo_job, tmp_path):
    out = str(tmp_path / "out")
    utxo_job().run(out_path=out, limit=3, sort_by="txs", candidate_multiplier=10)

    rows = _read_csv(out)
    assert [r["address"] for r in rows][:2] == ["1UntaggedTop", "1ClusterTagged"]
    assert all(r["address"] != "1TaggedBusy" for r in rows)
    assert rows[0]["no_txs"] == "600"
    assert rows[0]["degree"] == "20"
    assert rows[0]["total_received_fiat"] == "500.0"


def test_utxo_reports_cluster_tag_without_excluding_the_address(utxo_job, tmp_path):
    out = str(tmp_path / "out")
    utxo_job().run(out_path=out, limit=3, sort_by="txs", candidate_multiplier=10)

    by_address = {r["address"]: r for r in _read_csv(out)}
    assert by_address["1ClusterTagged"]["cluster_tagged"] == "true"
    assert by_address["1UntaggedTop"]["cluster_tagged"] == "false"


def test_no_tagged_clusters_still_yields_a_false_flag(utxo_job, tmp_path):
    out = str(tmp_path / "out")
    utxo_job(tagged_clusters=[]).run(out_path=out, limit=3, sort_by="txs")
    assert all(r["cluster_tagged"] == "false" for r in _read_csv(out))


def test_no_tagged_addresses_excludes_nothing(utxo_job, tmp_path):
    out = str(tmp_path / "out")
    utxo_job(tagged=[]).run(out_path=out, limit=5, sort_by="txs")
    assert "1TaggedBusy" in {r["address"] for r in _read_csv(out)}


@pytest.mark.parametrize(
    "sort_by,expected",
    [("txs", "1UntaggedTop"), ("degree", "1ClusterTagged"), ("value", "1UntaggedTop")],
)
def test_sort_by_selects_the_metric(utxo_job, tmp_path, sort_by, expected):
    out = str(tmp_path / "out")
    utxo_job().run(out_path=out, limit=1, sort_by=sort_by, candidate_multiplier=10)
    assert _read_csv(out)[0]["address"] == expected


def test_fiat_index_selects_the_currency(utxo_job, tmp_path):
    out = str(tmp_path / "out")
    utxo_job().run(out_path=out, limit=1, sort_by="txs", fiat_index=1)
    assert _read_csv(out)[0]["total_received_fiat"] == "450.0"


def test_min_txs_filters_quiet_addresses(utxo_job, tmp_path):
    out = str(tmp_path / "out")
    utxo_job(tagged=[], tagged_clusters=[]).run(
        out_path=out, limit=10, sort_by="txs", min_txs=100
    )
    assert {r["address"] for r in _read_csv(out)} == {
        "1TaggedBusy",
        "1UntaggedTop",
        "1ClusterTagged",
    }


def test_null_tx_counts_and_degrees_are_coalesced(utxo_job, tmp_path):
    out = str(tmp_path / "out")
    utxo_job(tagged=[], tagged_clusters=[]).run(out_path=out, limit=10, sort_by="txs")

    row = {r["address"]: r for r in _read_csv(out)}["1NullMetrics"]
    assert row["no_txs"] == "7"
    assert row["degree"] == "0"
    assert row["total_received_fiat"] == ""


def test_underfetched_pool_warns(utxo_job, tmp_path, caplog):
    out = str(tmp_path / "out")
    with caplog.at_level("WARNING"):
        utxo_job().run(out_path=out, limit=100, sort_by="txs", candidate_multiplier=1)
    assert "raise --candidate-multiplier" in caplog.text


@pytest.mark.parametrize("currency,expected_prefix", [("eth", "0x"), ("trx", "T")])
def test_account_addresses_render_to_tagstore_identifier_form(
    spark, tmp_path, currency, expected_prefix
):
    from graphsenselib.utils.address import address_to_user_format

    tagged = address_to_user_format(currency, bytes.fromhex("aa" * 20))
    job = _make_job(spark, currency, "account", _account_addresses(spark), [tagged], [])
    out = str(tmp_path / "out")
    job.run(out_path=out, limit=5, sort_by="txs", candidate_multiplier=10)

    rows = _read_csv(out)
    assert len(rows) == 1
    assert rows[0]["address"].startswith(expected_prefix)
    assert rows[0]["address"] != tagged
    # Account keyspaces have no clustering, so the flag is null, not false.
    assert rows[0]["cluster_tagged"] == ""
    assert "cluster_id" not in rows[0]


def test_account_model_never_probes_the_cluster_view(spark, tmp_path):
    job = _make_job(spark, "eth", "account", _account_addresses(spark), [], [])
    job.run(out_path=str(tmp_path / "out"), limit=5, sort_by="txs")
    assert all("best_cluster_tag" not in q for q, _ in job.executed)


def test_parquet_output(utxo_job, spark, tmp_path):
    out = str(tmp_path / "out")
    utxo_job().run(out_path=out, out_format="parquet", limit=2, sort_by="txs")
    assert spark.read.parquet(out).count() == 2


def test_invalid_arguments_are_rejected(utxo_job, tmp_path):
    with pytest.raises(ValueError, match="out_format"):
        utxo_job().run(out_path=str(tmp_path / "o"), out_format="json")
    with pytest.raises(ValueError, match="sort_by"):
        utxo_job().run(out_path=str(tmp_path / "o"), sort_by="nonsense")


def test_oversized_candidate_pool_is_rejected(utxo_job, tmp_path):
    with pytest.raises(ValueError, match="collected to the driver"):
        utxo_job().run(
            out_path=str(tmp_path / "o"), limit=100_000, candidate_multiplier=100
        )
