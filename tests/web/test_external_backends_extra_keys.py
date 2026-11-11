"""external_backends and its backends accept keys they do not know, as the
top level does: one config file is shared by the images that run side by side
during a rollout, and an older image must not refuse to start over a key a
newer one added (as v2.20 did over delegated_conversions). Startup warns about
each unknown key, so a misspelling is not silent."""

import logging

from graphsenselib.web.app import create_app_from_dict

WARNING_PREFIX = "Ignoring config keys this version does not know"


def make_app(external_backends, caplog):
    with caplog.at_level(logging.WARNING, logger="graphsenselib.web.app"):
        app = create_app_from_dict(
            {
                "database": {"nodes": ["localhost"]},
                "external_backends": external_backends,
            }
        )
    warnings = [
        r.getMessage()
        for r in caplog.records
        if r.getMessage().startswith(WARNING_PREFIX)
    ]
    return app, warnings


def test_unknown_keys_load_and_are_warned_about(caplog):
    app, warnings = make_app(
        {
            "enabled": True,
            "networks": {"arb": {"url": "http://adapter.test", "weight": 2}},
            "future_feature": {"eth": {"url": "http://adapter.test"}},
            "delegated_conversions": {
                "eth": {"url": "http://adapter.test", "region": "eu"}
            },
        },
        caplog,
    )
    assert app.state.config.external_backends.networks["arb"].url == (
        "http://adapter.test"
    )
    assert warnings == [
        f"{WARNING_PREFIX}: external_backends.future_feature, "
        "external_backends.networks.arb.weight, "
        "external_backends.delegated_conversions.eth.region"
    ]


def test_known_keys_do_not_warn(caplog):
    _, warnings = make_app(
        {
            "enabled": True,
            "networks": {"arb": {"url": "http://adapter.test", "api_key": "k"}},
            "delegated_conversions": {"eth": {"url": "http://adapter.test"}},
            "merge_timeout_s": 5,
        },
        caplog,
    )
    assert warnings == []
