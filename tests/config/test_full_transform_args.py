from graphsenselib.config import get_config
from graphsenselib.config.config import (
    DEFAULT_SCALA_JOB_PACKAGES,
    FullTransformArgs,
    SidecarConfig,
)


def test_defaults():
    fta = FullTransformArgs()
    assert fta.backend == "scala"
    assert fta.artifact == "fat"
    assert fta.repo == "graphsense/graphsense-spark"
    assert fta.main_class == "org.graphsense.TransformationJob"
    assert fta.packages == DEFAULT_SCALA_JOB_PACKAGES
    assert fta.sidecar.enabled is False


def test_version_and_profile_resolution():
    fta = FullTransformArgs(
        version="v26.06.0",
        version_overrides={"trx": "v26.07.0"},
        spark_profile={"btc": "utxo", "eth": "account"},
    )
    assert fta.version_for("btc") == "v26.06.0"
    assert fta.version_for("trx") == "v26.07.0"
    assert fta.profile_for("btc") == "utxo"
    assert fta.profile_for("ltc") is None


def test_construct_from_dicts():
    # Mirrors the YAML -> config path (nested dicts coerced into models).
    fta = FullTransformArgs.model_validate(
        {
            "version": "v26.06.0",
            "artifact": "slim",
            "jar_args": {"btc": ["--bech32-prefix", "bc", "--bucket-size", "5000"]},
            "sidecar": {
                "enabled": True,
                "contact_points": ["h1:9043"],
                "local_dc": "DC1",
            },
        }
    )
    assert fta.artifact == "slim"
    assert fta.jar_args["btc"] == ["--bech32-prefix", "bc", "--bucket-size", "5000"]
    assert isinstance(fta.sidecar, SidecarConfig)
    assert fta.sidecar.enabled is True
    assert fta.sidecar.contact_points == ["h1:9043"]
    assert fta.sidecar.consistency_level == "LOCAL_QUORUM"


def test_get_full_transform_args_default_when_unset():
    # The repo-wide patch_config fixture builds a config without the section.
    fta = get_config().get_full_transform_args()
    assert isinstance(fta, FullTransformArgs)
    assert fta.backend == "scala"
