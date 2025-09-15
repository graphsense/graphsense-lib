import json


def test_custom_json_encoder_decoder():
    from graphsenselib.utils.generic import custom_json_encoder, custom_json_decoder

    data = {
        "normal_int": 42,
        "large_int": 2**70,
        "normal_bytes": b"hello",
        "nested": {
            "another_large_int": -(2**65),
            "another_bytes": b"world",
        },
    }

    json_str = json.dumps(data, default=custom_json_encoder)
    decoded_data = json.loads(json_str, object_hook=custom_json_decoder)

    assert decoded_data["normal_int"] == 42
    assert decoded_data["large_int"] == 2**70
    assert decoded_data["normal_bytes"] == b"hello"
    assert decoded_data["nested"]["another_large_int"] == -(2**65)
    assert decoded_data["nested"]["another_bytes"] == b"world"


def test_sys_argv_filtering():
    from graphsenselib.utils.generic import filter_sensitive_sys_argv

    original_argv = [
        "tagpack-tool",
        "sync",
        "-u",
        "postgresql://test:pass@localhost:7432/tagstore",
        "-r",
        "/home/iknaio/docker/tagstore2/tagpack-repos.config",
        "--rerun-cluster-mapping-with-env",
        "prod",
        "--dont-update-quality-metrics",
    ]

    filtered_argv = filter_sensitive_sys_argv(original_argv)

    assert filtered_argv == [
        "tagpack-tool",
        "sync",
        "-u",
        "***",
        "-r",
        "/home/iknaio/docker/tagstore2/tagpack-repos.config",
        "--rerun-cluster-mapping-with-env",
        "prod",
        "--dont-update-quality-metrics",
    ]
