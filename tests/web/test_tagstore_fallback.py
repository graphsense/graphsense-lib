from types import SimpleNamespace

from fastapi import FastAPI

import graphsenselib.web.app as web_app
from graphsenselib.tagstore.db import Taxonomies
from graphsenselib.web.config import GSRestConfig
from graphsenselib.web.dependencies import MockTagstoreDb


class _FakeDbClient:
    def __init__(self, _config, _logger):
        self.closed = False

    def close(self):
        self.closed = True


class _FakeEngine:
    def __init__(self):
        self.disposed = False
        self.pool = SimpleNamespace(status=lambda: "fake-pool")

    async def dispose(self):
        self.disposed = True


def _build_config(with_tagstore: bool) -> GSRestConfig:
    payload: dict[str, object] = {"database": {"nodes": ["localhost"]}}
    if with_tagstore:
        payload["gs-tagstore"] = {
            "url": "postgresql+asyncpg://user:password@localhost:5432/tagstore"
        }
    return GSRestConfig.model_validate(payload)


def _mock_db_module(_module_name: str):
    return SimpleNamespace(Cassandra=_FakeDbClient)


async def test_setup_database_uses_mock_tagstore_when_not_configured(monkeypatch):
    monkeypatch.setattr(web_app.importlib, "import_module", _mock_db_module)

    def _fail_if_called(*_args, **_kwargs):
        raise AssertionError("TagStore engine creation must not be called")

    monkeypatch.setattr(web_app, "get_db_engine_async", _fail_if_called)

    app = FastAPI()
    app.state.config = _build_config(with_tagstore=False)

    await web_app.setup_database(app)

    assert isinstance(app.state.tagstore_db, MockTagstoreDb)
    assert app.state.tagstore_engine is None
    assert app.state.taxonomy_cache["labels"][Taxonomies.CONCEPT] == {}
    assert app.state.taxonomy_cache["labels"][Taxonomies.COUNTRY] == {}
    assert app.state.taxonomy_cache["abuse"] == set()

    await web_app.setup_services(app)
    assert isinstance(app.state.services.tagstore_db, MockTagstoreDb)

    await web_app.teardown_database(app)
    assert app.state.db.closed is True


async def test_setup_database_falls_back_to_mock_when_tagstore_unreachable(monkeypatch):
    monkeypatch.setattr(web_app.importlib, "import_module", _mock_db_module)

    engine = _FakeEngine()
    monkeypatch.setattr(web_app, "get_db_engine_async", lambda *_args, **_kwargs: engine)

    async def _raise_setup_cache(cls, _tagstore_db, _app):
        raise RuntimeError("tagstore unavailable")

    monkeypatch.setattr(
        web_app.ConceptsCacheServiceFastAPI,
        "setup_cache",
        classmethod(_raise_setup_cache),
    )

    app = FastAPI()
    app.state.config = _build_config(with_tagstore=True)

    await web_app.setup_database(app)

    assert engine.disposed is True
    assert isinstance(app.state.tagstore_db, MockTagstoreDb)
    assert app.state.tagstore_engine is None

    search_result = await app.state.tagstore_db.search_labels(
        "internet", 10, ["public"], query_actors=True, query_labels=True
    )
    assert search_result.actor_labels == []
    assert search_result.tag_labels == []

    await web_app.teardown_database(app)
    assert app.state.db.closed is True
