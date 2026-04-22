from graphsenselib.web.config import GSRestConfig


def test_gsrest_config_loads_tagstore_from_graphsense_env(monkeypatch):
    monkeypatch.setenv("GS_CASSANDRA_ASYNC_NODES", '["localhost"]')
    monkeypatch.setenv(
        "GRAPHSENSE_TAGSTORE_READ_URL",
        "postgresql://env_user:env_pass@localhost:5432/env_tagstore",
    )

    cfg = GSRestConfig()

    assert cfg.tagstore is not None
    assert (
        cfg.tagstore.url == "postgresql://env_user:env_pass@localhost:5432/env_tagstore"
    )


def test_explicit_tagstore_overrides_graphsense_env(monkeypatch):
    monkeypatch.setenv("GS_CASSANDRA_ASYNC_NODES", '["localhost"]')
    monkeypatch.setenv(
        "GRAPHSENSE_TAGSTORE_READ_URL",
        "postgresql://env_user:env_pass@localhost:5432/env_tagstore",
    )

    cfg = GSRestConfig.model_validate(
        {
            "database": {"nodes": ["localhost"]},
            "gs-tagstore": {
                "url": "postgresql://explicit_user:explicit_pass@localhost:5432/explicit_tagstore"
            },
        }
    )

    assert cfg.tagstore is not None
    assert (
        cfg.tagstore.url
        == "postgresql://explicit_user:explicit_pass@localhost:5432/explicit_tagstore"
    )
