"""Run settings, and the guarantee that a v3 run cannot touch a v2 keyspace.

That guarantee is structural rather than procedural, so these are the tests that
matter most in the package: a backfill pointed at a live keyspace would be
unrecoverable.
"""

import pytest

from graphsense_v3.schema import Kind
from graphsense_v3.settings import (
    RunSettings,
    UnsafeKeyspace,
    assert_no_conflict,
    assert_v3_keyspace,
    configured_keyspaces,
    v3_keyspace,
)

#: Real names, as they appear in graphsense.yaml today.
LIVE = [
    "trx_raw_20260428",
    "trx_transformed_20260828",
    "btc_raw_prod",
    "eth_transformed_dev",
    "btc_transformed_20260101",
]


@pytest.mark.parametrize("name", LIVE)
def test_no_live_keyspace_can_be_written(name: str) -> None:
    """The whole point: v2 names carry no `_v3` segment, so none of them can
    pass the gate a write has to go through."""
    with pytest.raises(UnsafeKeyspace):
        assert_v3_keyspace(name)


@pytest.mark.parametrize(
    "name",
    ["btc_raw_v3", "btc_derived_v3", "trx_raw_v3_aug", "zec_derived_v3_bench2"],
)
def test_v3_names_pass(name: str) -> None:
    assert_v3_keyspace(name) is None


@pytest.mark.parametrize("name", ["", None, "raw_v3", "btc_raw", "btc_raw_v3_"])
def test_malformed_names_are_refused(name) -> None:
    with pytest.raises(UnsafeKeyspace):
        assert_v3_keyspace(name)


def test_names_are_constructed_not_supplied() -> None:
    assert v3_keyspace("BTC", Kind.RAW) == "btc_raw_v3"
    assert v3_keyspace("btc", Kind.DERIVED, "aug") == "btc_derived_v3_aug"


def test_a_label_cannot_smuggle_in_another_name() -> None:
    for bad in ("has space", "UPPER", "with_underscore", "dash-ed", ""):
        with pytest.raises(ValueError):
            v3_keyspace("btc", Kind.RAW, bad)


class _Keyspace:
    def __init__(self, raw, derived):
        self.raw_keyspace_name = raw
        self.transformed_keyspace_name = derived


class _Environment:
    def __init__(self, keyspaces):
        self.keyspaces = keyspaces


class _Config:
    def __init__(self, environments):
        self.environments = environments


def test_configured_keyspaces_spans_every_environment() -> None:
    config = _Config(
        {
            "prod": _Environment({"btc": _Keyspace("btc_raw_prod", "btc_tf_prod")}),
            "dev": _Environment({"eth": _Keyspace("eth_raw_dev", "eth_tf_dev")}),
        }
    )
    assert configured_keyspaces(config) == {
        "btc_raw_prod",
        "btc_tf_prod",
        "eth_raw_dev",
        "eth_tf_dev",
    }


def test_an_environment_named_v3_is_caught_by_the_second_check() -> None:
    """The pattern alone would not catch this: an environment literally named
    `v3` makes `btc_raw_v3` a real, live keyspace. Hence the cross-check
    against every name the config mentions."""
    config = _Config(
        {"v3": _Environment({"btc": _Keyspace("btc_raw_v3", "btc_derived_v3")})}
    )
    settings = RunSettings(
        network="btc",
        env="v3",
        lake_root="s3a://lake/btc",
        raw_keyspace="btc_raw_v3",
        derived_keyspace="btc_derived_v3",
        rates_keyspace="btc_raw_v3",
        cassandra_nodes=["node"],
    )
    # the name passes the pattern...
    assert_v3_keyspace(settings.raw_keyspace)
    # ...and is still refused, because the config already claims it.
    with pytest.raises(UnsafeKeyspace, match="already a keyspace"):
        assert_no_conflict(settings, config)


def test_the_rates_keyspace_is_never_a_write_target() -> None:
    settings = RunSettings(
        network="btc",
        env="prod",
        lake_root="s3a://lake/btc",
        raw_keyspace="btc_raw_v3",
        derived_keyspace="btc_derived_v3",
        rates_keyspace="btc_raw_v3",
        cassandra_nodes=["node"],
    )
    with pytest.raises(UnsafeKeyspace, match="read-only"):
        assert_no_conflict(settings, _Config({}))


def test_the_writer_refuses_a_non_v3_keyspace() -> None:
    """Defence in depth: the gate is in `write`, not only in the driver, so a
    caller that skips the CLI cannot reach a live keyspace either."""
    from graphsense_v3.schema.definitions import derived
    from graphsense_v3.schema.model import Family
    from graphsense_v3.spark.writer import write

    table = derived(Family.UTXO).table("address_stats")
    with pytest.raises(UnsafeKeyspace):
        write(None, table, "btc_transformed_20260828")  # ty: ignore[invalid-argument-type]


def test_describe_names_what_is_read_and_what_is_written() -> None:
    text = RunSettings(
        network="btc",
        env="prod",
        lake_root="s3a://lake/btc",
        raw_keyspace="btc_raw_v3",
        derived_keyspace="btc_derived_v3",
        rates_keyspace="btc_raw_20260101",
        cassandra_nodes=["a", "b"],
    ).describe()
    assert "btc_raw_20260101      (READ ONLY)" in text
    assert "btc_raw_v3      (created, written)" in text
    assert "derived keyspace   btc_derived_v3" in text


# --------------------------------------------------------------------------- #
# spark profile                                                                #
# --------------------------------------------------------------------------- #


def test_the_delta_extension_is_not_clobbered() -> None:
    """`create_spark_session` sets the Delta extension and then applies
    spark_config LAST, so transform-utxo's Cassandra-only value would replace
    it and the lake read would fail. The Scala job never hit this because it
    does not read Delta through the same session."""
    from graphsense_v3.spark.profile import resolve

    resolved = resolve(
        {
            "spark.sql.extensions": "com.datastax.spark.connector.CassandraSparkExtensions"
        }
    )
    extensions = resolved["spark.sql.extensions"].split(",")
    assert "io.delta.sql.DeltaSparkSessionExtension" in extensions
    assert "com.datastax.spark.connector.CassandraSparkExtensions" in extensions


def test_null_writes_are_never_tombstones() -> None:
    """v3 frames are legitimately sparse. Without ignoreNulls the connector
    writes an explicit NULL, which is a tombstone, on a brand-new keyspace."""
    from graphsense_v3.spark.profile import resolve

    assert resolve({})["spark.cassandra.output.ignoreNulls"] == "true"
    # even if a profile says otherwise
    assert (
        resolve({"spark.cassandra.output.ignoreNulls": "false"})[
            "spark.cassandra.output.ignoreNulls"
        ]
        == "true"
    )


def test_the_configured_profile_wins_on_tuning() -> None:
    """Required settings are correctness; everything else is the cluster
    owner's call."""
    from graphsense_v3.spark.profile import resolve

    resolved = resolve({"spark.sql.shuffle.partitions": "800"})
    assert resolved["spark.sql.shuffle.partitions"] == "800"
    # and the defaults fill in what the profile is silent about
    assert resolved["spark.sql.adaptive.skewJoin.enabled"] == "true"
    assert resolved["spark.executor.pyspark.memory"] == "8g"


def test_missing_cluster_facts_are_reported_not_invented() -> None:
    """The executors have to be able to import graphsense_v3 for the pandas
    UDFs, and shuffle must not land on the root disk. Neither value is ours to
    guess."""
    from graphsense_v3.spark.profile import warnings

    missing = warnings({})
    assert any("spark.local.dir" in w for w in missing)
    assert any("spark.archives" in w for w in missing)
    assert (
        warnings(
            {
                "spark.local.dir": "/var/data/nvme4/spark/local_storage",
                "spark.archives": "file:///opt/graphsense/spark-env.tar.gz#environment",
                "spark.executorEnv.PYTHONPATH": "./environment",
            }
        )
        == []
    )


# --------------------------------------------------------------------------- #
# sidecar and DDL                                                              #
# --------------------------------------------------------------------------- #


def test_sidecar_config_is_applied_before_the_session() -> None:
    """Kryo registration and the SSTable writer's JDK module flags cannot be
    set after a SparkSession exists, so they are folded into spark_config."""
    from graphsense_v3.spark.sidecar import KRYO_REGISTRATOR, session_config

    props = session_config(
        {"spark.local.dir": "/var/data/nvme4/spark/local_storage"},
        ["10.0.0.1:9043"],
        "DC1",
    )
    assert props["spark.serializer"].endswith("KryoSerializer")
    assert KRYO_REGISTRATOR in props["spark.kryo.registrator"]
    assert (
        "--add-opens java.base/sun.nio.ch=ALL-UNNAMED"
        in (props["spark.executor.extraJavaOptions"])
    )
    # the temp dir must follow spark.local.dir off the ~23G root disk
    assert "-Djava.io.tmpdir=/var/data/nvme4" in props["spark.driver.extraJavaOptions"]
    assert "cassandra-analytics-core" in props["spark.jars.packages"]


def test_sidecar_refuses_without_a_local_dir() -> None:
    """SSTables and Vert.x would otherwise stage on the root disk."""
    from graphsense_v3.spark.sidecar import session_config

    with pytest.raises(ValueError, match="spark.local.dir"):
        session_config({}, ["10.0.0.1:9043"], "DC1")
    with pytest.raises(ValueError, match="contact point"):
        session_config({"spark.local.dir": "/tmp"}, [], "DC1")


def test_existing_java_options_are_kept() -> None:
    """The profiles already set G1GC; the module flags are appended, not
    substituted."""
    from graphsense_v3.spark.sidecar import session_config

    props = session_config(
        {
            "spark.local.dir": "/tmp",
            "spark.executor.extraJavaOptions": "-XX:+UseG1GC",
        },
        ["10.0.0.1:9043"],
        "DC1",
    )
    assert props["spark.executor.extraJavaOptions"].startswith("-XX:+UseG1GC")
    assert "--add-exports" in props["spark.executor.extraJavaOptions"]


def test_ddl_is_split_into_statements() -> None:
    from graphsense_v3.cassandra import keyspace_of, statements

    cql = (
        "-- a comment; with a semicolon\n"
        "CREATE KEYSPACE IF NOT EXISTS btc_raw_v3 WITH replication = "
        "{'class':'NetworkTopologyStrategy','DC1':'1'};\n\n"
        "USE btc_raw_v3;\n\n"
        "CREATE TABLE IF NOT EXISTS block (block_id int PRIMARY KEY);\n"
    )
    assert keyspace_of(cql) == "btc_raw_v3"
    parts = statements(cql)
    assert len(parts) == 3
    assert parts[0].startswith("CREATE KEYSPACE")
    assert not any("comment" in p for p in parts)


def test_ddl_can_only_target_a_v3_keyspace() -> None:
    """The same gate as the write path: `create` cannot alter a live keyspace."""
    from graphsense_v3.cassandra import apply_cql

    settings = RunSettings(
        network="btc",
        env="prod",
        lake_root="s3a://lake/btc",
        raw_keyspace="btc_raw_v3",
        derived_keyspace="btc_derived_v3",
        rates_keyspace="btc_raw_20260101",
        cassandra_nodes=["node"],
    )
    with pytest.raises(UnsafeKeyspace):
        apply_cql(settings, "CREATE KEYSPACE IF NOT EXISTS btc_raw_20260101 WITH x;")
    with pytest.raises(ValueError, match="creates no keyspace"):
        apply_cql(settings, "DROP KEYSPACE btc_raw_20260101;")
