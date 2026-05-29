import pytest

from graphsenselib.transformation.spark_jar import (
    SIDECAR_PACKAGE,
    apply_sidecar,
    asset_name,
    build_spark_submit,
    release_jar_url,
)


def test_asset_name_fat_strips_leading_v():
    assert asset_name("fat", "v26.06.0") == "graphsense-spark-assembly-26.06.0.jar"
    assert asset_name("fat", "26.06.0") == "graphsense-spark-assembly-26.06.0.jar"


def test_asset_name_slim():
    assert asset_name("slim", "v26.06.0") == "graphsense-spark_2.12-26.06.0.jar"


def test_asset_name_invalid_artifact():
    with pytest.raises(ValueError):
        asset_name("medium", "v1.0.0")


def test_release_jar_url():
    assert release_jar_url("graphsense/graphsense-spark", "v26.06.0", "fat") == (
        "https://github.com/graphsense/graphsense-spark/releases/download/"
        "v26.06.0/graphsense-spark-assembly-26.06.0.jar"
    )


def test_build_spark_submit_fat_no_packages():
    cmd = build_spark_submit(
        spark_home=None,
        jar_path="/jars/x.jar",
        main_class="org.graphsense.TransformationJob",
        spark_props={
            "spark.master": "local[*]",
            "spark.cassandra.connection.host": "h",
        },
        packages=[],
        repositories=["https://repos.spark-packages.org/"],
        jar_args=["--network", "btc"],
        extra_submit_args=[],
    )
    assert cmd[0] == "spark-submit"
    assert cmd[1:3] == ["--class", "org.graphsense.TransformationJob"]
    assert "--packages" not in cmd  # fat artifact bundles deps
    assert "spark.master=local[*]" in cmd
    # jar followed by its args, at the very end
    assert cmd[-3:] == ["/jars/x.jar", "--network", "btc"]


def test_build_spark_submit_slim_packages_repos_and_spark_home():
    cmd = build_spark_submit(
        spark_home="/opt/spark",
        jar_path="/x.jar",
        main_class="C",
        spark_props={},
        packages=["a:b:1", "c:d:2"],
        repositories=["https://repos.spark-packages.org/"],
        jar_args=[],
        extra_submit_args=["--properties-file", "/p.conf"],
    )
    assert cmd[0] == "/opt/spark/bin/spark-submit"
    i = cmd.index("--packages")
    assert cmd[i + 1] == "a:b:1,c:d:2"
    assert "--repositories" in cmd
    assert cmd[-3:-1] == ["--properties-file", "/p.conf"]  # before the jar
    assert cmd[-1] == "/x.jar"


def test_apply_sidecar_augments_props_packages_and_args():
    in_props = {"spark.local.dir": "/nvme", "spark.driver.extraJavaOptions": "-Dx=1"}
    in_pkgs = ["a:b:1"]
    in_args = ["--network", "eth"]
    props, pkgs, args = apply_sidecar(
        in_props,
        in_pkgs,
        in_args,
        contact_points=["h1:9043", "h2:9043"],
        local_dc="DC1",
        consistency_level="LOCAL_QUORUM",
    )
    # analytics package added, existing kept
    assert SIDECAR_PACKAGE in pkgs and "a:b:1" in pkgs
    # tmpdir redirect on both driver and executor; existing driver opts preserved
    assert "-Dx=1" in props["spark.driver.extraJavaOptions"]
    assert "-Djava.io.tmpdir=/nvme" in props["spark.driver.extraJavaOptions"]
    assert "-Dvertx.cacheDirBase=/nvme" in props["spark.executor.extraJavaOptions"]
    # job args appended after the originals
    assert args[:2] == ["--network", "eth"]
    assert "--writer" in args and "sidecar" in args
    assert "h1:9043,h2:9043" in args
    assert "--sidecar-local-dc" in args and "DC1" in args
    assert "--sidecar-consistency-level" in args
    # inputs not mutated
    assert in_pkgs == ["a:b:1"]
    assert in_args == ["--network", "eth"]
    assert "spark.executor.extraJavaOptions" not in in_props


def test_apply_sidecar_requires_local_dir():
    with pytest.raises(ValueError, match="spark.local.dir"):
        apply_sidecar(
            {},
            [],
            [],
            contact_points=["h:9043"],
            local_dc=None,
            consistency_level="LOCAL_QUORUM",
        )


def test_apply_sidecar_requires_contact_points():
    with pytest.raises(ValueError, match="contact_points"):
        apply_sidecar(
            {"spark.local.dir": "/x"},
            [],
            [],
            contact_points=[],
            local_dc=None,
            consistency_level="LOCAL_QUORUM",
        )
