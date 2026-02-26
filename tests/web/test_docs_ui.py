from fastapi import FastAPI
from starlette.testclient import TestClient
from types import SimpleNamespace

from graphsenselib.web.app import _setup_custom_docs_ui
from graphsenselib.web.config import GSRestConfig


def _build_test_config(**overrides: object) -> GSRestConfig:
    base = GSRestConfig.model_validate(
        {
            "database": {"nodes": ["localhost"]},
            "gs-tagstore": {
                "url": "postgresql+asyncpg://user:password@localhost:5432/tagstore"
            },
        }
    )
    return base.model_copy(update=overrides)


def _build_test_app(config: GSRestConfig | None = None) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url="/openapi.json")
    app.state.config = config or _build_test_config()
    _setup_custom_docs_ui(app)
    return app


def test_docs_ui_default_crosslinks_present():
    app = _build_test_app()
    with TestClient(app) as client:
        swagger_html = client.get("/ui").text
        redoc_html = client.get("/redoc").text

    assert 'href="/redoc"' in swagger_html
    assert ">ReDoc<" in swagger_html
    assert 'href="/ui"' in redoc_html
    assert ">Try the API<" in redoc_html


def test_docs_ui_default_python_client_link_present():
    app = _build_test_app()
    with TestClient(app) as client:
        swagger_html = client.get("/ui").text
        redoc_html = client.get("/redoc").text

    assert 'href="https://github.com/graphsense/graphsense-lib/tree/master/clients/python"' in swagger_html
    assert ">Python Client Docs<" in swagger_html
    assert 'href="https://github.com/graphsense/graphsense-lib/tree/master/clients/python"' in redoc_html
    assert ">Python Client Docs<" in redoc_html


def test_docs_ui_external_link_present_when_configured():
    app = _build_test_app(
        _build_test_config(
            docs_external_url="https://docs.example.com",
            docs_external_label="Platform Docs",
        )
    )
    with TestClient(app) as client:
        swagger_html = client.get("/ui").text
        redoc_html = client.get("/redoc").text

    assert 'href="https://docs.example.com"' in swagger_html
    assert ">Platform Docs<" in swagger_html
    assert 'href="https://docs.example.com"' in redoc_html
    assert ">Platform Docs<" in redoc_html


def test_docs_ui_crosslinks_can_be_disabled():
    app = _build_test_app(
        _build_test_config(
            docs_swagger_crosslink_url=None,
            docs_redoc_crosslink_url=None,
        )
    )
    with TestClient(app) as client:
        swagger_html = client.get("/ui").text
        redoc_html = client.get("/redoc").text

    assert 'href="/redoc"' not in swagger_html
    assert 'href="/ui"' not in redoc_html


def test_docs_ui_accepts_mock_config_object():
    app = _build_test_app(
        SimpleNamespace(
            docs_swagger_crosslink_url="/redoc",
            docs_swagger_crosslink_label="ReDoc",
            docs_redoc_crosslink_url="/ui",
            docs_redoc_crosslink_label="Try the API",
            docs_python_client_url="https://example.com/client-docs",
            docs_python_client_label="Client Docs",
            docs_external_url=None,
            docs_logo_url=None,
            docs_favicon_url=None,
        )
    )

    with TestClient(app) as client:
        swagger_html = client.get("/ui").text
        redoc_html = client.get("/redoc").text

    assert 'href="https://example.com/client-docs"' in swagger_html
    assert ">Client Docs<" in swagger_html
    assert 'href="https://example.com/client-docs"' in redoc_html
    assert ">Client Docs<" in redoc_html
