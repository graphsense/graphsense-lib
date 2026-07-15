import pytest

from graphsenselib.transformation.sidecar import (
    SIDECAR_PACKAGE,
    SIDECAR_WRITE_FORMAT,
    bulk_write_dataframe,
    sidecar_packages,
    sidecar_spark_properties,
    sidecar_writer_options,
)


def test_writer_options_full():
    opts = sidecar_writer_options(
        keyspace="ltc_raw_new",
        table="transaction",
        contact_points=["db0:9043", "db1:9043"],
        local_dc="DC1",
        consistency_level="LOCAL_ONE",
    )
    assert opts == {
        "keyspace": "ltc_raw_new",
        "table": "transaction",
        "sidecar_contact_points": "db0:9043,db1:9043",
        "bulk_writer_cl": "LOCAL_ONE",
        "local_dc": "DC1",
    }


def test_writer_options_local_cl_requires_local_dc():
    # the bulk writer's keyspace-replication validation fails at save() time
    # for LOCAL_* levels without a datacenter — reject it up front
    with pytest.raises(ValueError, match="local_dc"):
        sidecar_writer_options(keyspace="ks", table="t", contact_points=["db0"])


def test_writer_options_non_local_cl_omits_local_dc():
    opts = sidecar_writer_options(
        keyspace="ks", table="t", contact_points=["db0"], consistency_level="QUORUM"
    )
    assert opts["bulk_writer_cl"] == "QUORUM"
    assert "local_dc" not in opts


def test_writer_options_extra_options_merge_and_override():
    opts = sidecar_writer_options(
        keyspace="ks",
        table="t",
        contact_points=["db0"],
        local_dc="DC1",
        options={"number_splits": "64", "bulk_writer_cl": "QUORUM"},
    )
    assert opts["number_splits"] == "64"
    assert opts["bulk_writer_cl"] == "QUORUM"


def test_writer_options_reject_identity_override():
    with pytest.raises(ValueError, match="keyspace"):
        sidecar_writer_options(
            keyspace="ks",
            table="t",
            contact_points=["db0"],
            local_dc="DC1",
            options={"keyspace": "other_ks"},
        )


def test_writer_options_require_contact_points():
    with pytest.raises(ValueError, match="contact_points"):
        sidecar_writer_options(
            keyspace="ks", table="t", contact_points=[], local_dc="DC1"
        )


def test_spark_properties_require_local_dir():
    with pytest.raises(ValueError, match="spark.local.dir"):
        sidecar_spark_properties({})


def test_spark_properties_augment_java_options():
    props = sidecar_spark_properties(
        {
            "spark.local.dir": "/data/tmp",
            "spark.executor.extraJavaOptions": "-Xss4m",
        }
    )
    for key in ("spark.driver.extraJavaOptions", "spark.executor.extraJavaOptions"):
        assert "--add-opens java.base/sun.nio.ch=ALL-UNNAMED" in props[key]
        assert "-Djava.io.tmpdir=/data/tmp" in props[key]
        # Vert.x creates the cache dir as a SIBLING of cacheDirBase
        # (`<base>-<uuid>`); the base must carry a subpath so the sibling
        # lands inside spark.local.dir, not next to it in a possibly
        # read-only parent.
        assert "-Dvertx.cacheDirBase=/data/tmp/vertx-cache" in props[key]
    assert props["spark.executor.extraJavaOptions"].startswith("-Xss4m ")


def test_spark_properties_use_first_of_local_dir_list():
    # spark.local.dir may be a comma-separated list; java.io.tmpdir must be
    # a single path
    props = sidecar_spark_properties({"spark.local.dir": "/data1/tmp,/data2/tmp"})
    assert "-Djava.io.tmpdir=/data1/tmp " in props["spark.driver.extraJavaOptions"]
    assert (
        "-Dvertx.cacheDirBase=/data1/tmp/vertx-cache"
        in props["spark.driver.extraJavaOptions"]
    )


def test_spark_properties_do_not_mutate_input():
    original = {"spark.local.dir": "/data/tmp"}
    sidecar_spark_properties(original)
    assert original == {"spark.local.dir": "/data/tmp"}


def test_sidecar_packages_appends_once():
    assert sidecar_packages([]) == [SIDECAR_PACKAGE]
    assert sidecar_packages([SIDECAR_PACKAGE]) == [SIDECAR_PACKAGE]
    existing = ["a:b:1"]
    assert sidecar_packages(existing) == ["a:b:1", SIDECAR_PACKAGE]
    assert existing == ["a:b:1"]


class _WriterRecorder:
    def __init__(self):
        self.calls = {}

    def format(self, fmt):
        self.calls["format"] = fmt
        return self

    def options(self, **opts):
        self.calls["options"] = opts
        return self

    def mode(self, mode):
        self.calls["mode"] = mode
        return self

    def save(self):
        self.calls["saved"] = True


class _FakeDataFrame:
    def __init__(self):
        self.write = _WriterRecorder()


def test_bulk_write_dataframe_wiring():
    df = _FakeDataFrame()
    bulk_write_dataframe(
        df,
        keyspace="ltc_raw_new",
        table="transaction",
        contact_points=["db0:9043"],
        local_dc="DC1",
    )
    assert df.write.calls["format"] == SIDECAR_WRITE_FORMAT
    assert df.write.calls["mode"] == "append"
    assert df.write.calls["saved"] is True
    assert df.write.calls["options"]["keyspace"] == "ltc_raw_new"
    assert df.write.calls["options"]["sidecar_contact_points"] == "db0:9043"
