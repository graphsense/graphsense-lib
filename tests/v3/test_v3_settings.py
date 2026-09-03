"""Run settings, and the guarantee that a v3 run cannot touch a v2 keyspace.

That guarantee is structural rather than procedural, so these are the tests that
matter most in the package: a backfill pointed at a live keyspace would be
unrecoverable.
"""

import os

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
    assert resolved["spark.executor.pyspark.memory"] == "16g"


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
        ["https://repos.spark-packages.org/"],
    )
    assert props["spark.serializer"].endswith("KryoSerializer")
    assert KRYO_REGISTRATOR in props["spark.kryo.registrator"]
    assert (
        "--add-opens java.base/sun.nio.ch=ALL-UNNAMED"
        in (props["spark.executor.extraJavaOptions"])
    )
    # The EXECUTORS' temp dir follows spark.local.dir, off the ~23G root disk.
    assert (
        "-Djava.io.tmpdir=/var/data/nvme4/spark/local_storage"
        in props["spark.executor.extraJavaOptions"]
    )
    # The DRIVER's does not: that path is the worker hosts' disk, so unless it
    # is mounted into the container the driver cannot write there at all.
    driver = props["spark.driver.extraJavaOptions"]
    assert "--add-opens" in driver
    assert os.access(driver.split("-Djava.io.tmpdir=")[1].split()[0], os.W_OK)
    # NOT spark.jars.packages: create_spark_session builds that itself and
    # applies spark_config afterwards, so setting it here would replace the
    # connector, delta and hadoop-aws instead of adding to them.
    assert "spark.jars.packages" not in props


def test_sidecar_package_is_added_without_displacing_the_others() -> None:
    """The failure this prevents: `spark.jars.packages` in spark_config is
    applied AFTER create_spark_session sets its own, so it replaces the list
    rather than extending it. The sidecar jar then resolves alone and the only
    symptom is a ClassNotFoundException for the Delta extension."""
    from graphsenselib.transformation.spark import DEFAULT_SPARK_PACKAGES

    from graphsense_v3.spark.sidecar import SIDECAR_PACKAGE, package_override

    overrides = package_override({})
    coords = {**DEFAULT_SPARK_PACKAGES, **overrides}
    joined = ",".join(coords.values())
    for name, coordinate in DEFAULT_SPARK_PACKAGES.items():
        assert coordinate in joined, f"{name} was displaced"
    assert SIDECAR_PACKAGE in joined


def test_sidecar_package_override_is_idempotent() -> None:
    """`create` then `run` reuse the same settings; adding it twice would ask
    Ivy to resolve a duplicate coordinate."""
    from graphsense_v3.spark.sidecar import SIDECAR_PACKAGE, package_override

    once = package_override({})
    twice = package_override(once)
    assert once == twice
    assert twice["delta_spark"].count(SIDECAR_PACKAGE) == 1


def test_sidecar_package_respects_a_configured_coordinate() -> None:
    """A profile pinning its own delta version must keep it."""
    from graphsense_v3.spark.sidecar import SIDECAR_PACKAGE, package_override

    pinned = package_override({"delta_spark": "io.delta:delta-spark_2.12:3.9.9"})
    assert pinned["delta_spark"] == f"io.delta:delta-spark_2.12:3.9.9,{SIDECAR_PACKAGE}"


def test_sidecar_refuses_without_a_local_dir() -> None:
    """SSTables and Vert.x would otherwise stage on the root disk."""
    from graphsense_v3.spark.sidecar import session_config

    with pytest.raises(ValueError, match="spark.local.dir"):
        session_config(
            {}, ["10.0.0.1:9043"], "DC1", ["https://repos.spark-packages.org/"]
        )
    with pytest.raises(ValueError, match="contact point"):
        session_config(
            {"spark.local.dir": "/tmp"},
            [],
            "DC1",
            ["https://repos.spark-packages.org/"],
        )


def test_sidecar_refuses_without_a_repository() -> None:
    """cassandra-analytics is not on Maven Central. Ivy resolution is all or
    nothing, so an unresolvable coordinate does not fail on its own -- it takes
    Delta and the Cassandra connector with it, and the only symptom is a
    ClassNotFoundException naming neither. Fail here instead."""
    from graphsense_v3.spark.sidecar import session_config

    with pytest.raises(ValueError, match="not on Maven Central"):
        session_config({"spark.local.dir": "/tmp"}, ["10.0.0.1:9043"], "DC1", [])


def test_sidecar_keeps_a_configured_repository() -> None:
    from graphsense_v3.spark.sidecar import session_config

    props = session_config(
        {
            "spark.local.dir": "/tmp",
            "spark.jars.repositories": "https://internal.example/repo",
        },
        ["10.0.0.1:9043"],
        "DC1",
        ["https://repos.spark-packages.org/"],
    )
    assert props["spark.jars.repositories"] == (
        "https://internal.example/repo,https://repos.spark-packages.org/"
    )


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
        ["https://repos.spark-packages.org/"],
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


def test_plan_shows_the_path_that_is_really_read() -> None:
    """`plan` exists to say what a run will do, so it must not print a lake
    path that differs from the one Spark opens. The config stores `s3://`;
    Hadoop 3 dropped that scheme, so the reader uses `s3a://`."""
    from graphsense_v3.settings import effective_lake_root

    assert effective_lake_root("s3://raw-data/ltc/") == "s3a://raw-data/ltc"
    # idempotent, and non-s3 paths are untouched
    assert effective_lake_root("s3a://raw-data/ltc") == "s3a://raw-data/ltc"
    assert effective_lake_root("hdfs://nn/lake/ltc") == "hdfs://nn/lake/ltc"
    assert effective_lake_root("/local/lake/ltc") == "/local/lake/ltc"

    text = RunSettings(
        network="ltc",
        env="prod",
        lake_root=effective_lake_root("s3://raw-data/ltc"),
        raw_keyspace="ltc_raw_v3",
        derived_keyspace="ltc_derived_v3",
        rates_keyspace="ltc_raw_20260727",
        cassandra_nodes=["172.22.240.72"],
    ).describe()
    assert "s3a://raw-data/ltc" in text


@pytest.mark.parametrize(
    "filename",
    [
        "raw_utxo.sql",
        "raw_account_eth.sql",
        "raw_account_trx.sql",
        "derived_utxo.sql",
        "derived_account.sql",
    ],
)
def test_every_generated_schema_splits_into_whole_statements(filename: str) -> None:
    """The splitter has to survive the CQL the renderer actually emits.

    It did not: `address_stats.epoch` carries the comment "0 = compacted base;
    else block_id // epoch_size + 1", and stripping only whole-line comments
    left that semicolon to cut its own statement in half. Cassandra reported it
    as `mismatched character '<EOF>'` four statements later.
    """
    from graphsense_v3.cassandra import statements
    from graphsense_v3.schema.emit import GENERATED_DIR

    cql = (GENERATED_DIR / filename).read_text(encoding="utf-8")
    parts = statements(cql)

    # one per CREATE, plus the USE
    assert len(parts) == cql.count("CREATE ") + 1
    for part in parts:
        assert part.count("(") == part.count(")"), part[:120]
        assert part.count("{") == part.count("}"), part[:120]
        assert "--" not in part
        assert part.startswith(("CREATE ", "USE "))


def test_a_semicolon_in_a_comment_does_not_split_a_statement() -> None:
    """The exact shape that broke `create` against the live cluster."""
    from graphsense_v3.cassandra import statements

    cql = (
        "CREATE TABLE IF NOT EXISTS t (\n"
        "    a int,   -- 0 = base; else something\n"
        "    b text,\n"
        "    PRIMARY KEY (a)\n"
        ");\n"
    )
    parts = statements(cql)
    assert len(parts) == 1
    assert parts[0].count("(") == parts[0].count(")")


def test_the_driver_falls_back_when_the_cluster_scratch_is_not_mounted(
    tmp_path, monkeypatch
) -> None:
    """`spark.local.dir` applies to the driver too, and the cluster profiles
    point it at the worker hosts' nvme. Mounting it into the container is the
    right answer; dying with AccessDeniedException before a stage runs is not.
    """
    from graphsense_v3.spark.session import ensure_driver_scratch

    monkeypatch.delenv("SPARK_LOCAL_DIRS", raising=False)
    fallback = ensure_driver_scratch({"spark.local.dir": "/var/data/nvme4/nope"})
    assert fallback is not None
    assert os.environ["SPARK_LOCAL_DIRS"] == fallback
    assert os.access(fallback, os.W_OK)


def test_a_mounted_scratch_is_left_alone(tmp_path, monkeypatch) -> None:
    """When the real disk IS mounted, the driver uses it -- overriding would
    defeat the mount and put broadcasts on the container's writable layer."""
    from graphsense_v3.spark.session import ensure_driver_scratch

    monkeypatch.delenv("SPARK_LOCAL_DIRS", raising=False)
    assert ensure_driver_scratch({"spark.local.dir": str(tmp_path)}) is None
    assert "SPARK_LOCAL_DIRS" not in os.environ
