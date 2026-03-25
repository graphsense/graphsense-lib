from fastapi import FastAPI
from pydantic import BaseModel
from starlette.testclient import TestClient
from types import SimpleNamespace

from graphsenselib.config.config import SlackTopic
from graphsenselib.web.app import create_app
from graphsenselib.web.app import create_spec_app
from graphsenselib.web.app import _register_exception_handlers
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
        redoc_html = client.get("/docs").text

    assert 'href="/docs"' in swagger_html
    assert ">ReDoc<" in swagger_html
    assert 'href="/ui"' in redoc_html
    assert ">Try the API<" in redoc_html


def test_docs_ui_default_python_client_link_present():
    app = _build_test_app()
    with TestClient(app) as client:
        swagger_html = client.get("/ui").text
        redoc_html = client.get("/docs").text

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
        redoc_html = client.get("/docs").text

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
        redoc_html = client.get("/docs").text

    assert 'href="/docs"' not in swagger_html
    assert 'href="/ui"' not in redoc_html


def test_docs_ui_accepts_mock_config_object():
    app = _build_test_app(
        SimpleNamespace(
            docs_swagger_crosslink_url="/docs",
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
        redoc_html = client.get("/docs").text

    assert 'href="https://example.com/client-docs"' in swagger_html
    assert ">Client Docs<" in swagger_html
    assert 'href="https://example.com/client-docs"' in redoc_html
    assert ">Client Docs<" in redoc_html


def test_gsrest_config_parses_slack_topics_from_dict():
    cfg = GSRestConfig.from_dict(
        {
            "database": {"nodes": ["localhost"]},
            "slack_topics": {
                "exceptions": {
                    "hooks": ["https://hooks.slack.com/services/T000/B000/TESTHOOK"]
                }
            },
        }
    )

    topic = cfg.get_slack_hooks_by_topic("exceptions")
    assert topic is not None
    assert topic.hooks == ["https://hooks.slack.com/services/T000/B000/TESTHOOK"]


def test_create_app_prefers_gsrest_slack_topics(monkeypatch):
    class DummyAppConfig:
        default_environment = "test"

        def load_partial(self):
            return True, []

        def get_slack_hooks_by_topic(self, topic: str):
            if topic == "exceptions":
                return SlackTopic(hooks=["https://hooks.slack.com/services/T000/B000/FALLBACK"])
            return None

    captured = {}

    def fake_setup_logging(app_logger, slack_exception_hook, default_environment, logging_config):
        captured["slack_exception_hook"] = slack_exception_hook
        captured["default_environment"] = default_environment

    monkeypatch.setattr("graphsenselib.web.app.AppConfig", lambda: DummyAppConfig())
    monkeypatch.setattr("graphsenselib.web.app.setup_logging", fake_setup_logging)

    cfg = GSRestConfig.from_dict(
        {
            "database": {"nodes": ["localhost"]},
            "slack_topics": {
                "exceptions": {
                    "hooks": ["https://hooks.slack.com/services/T000/B000/PRIMARY"]
                }
            },
        }
    )

    create_app(config=cfg)

    assert captured["slack_exception_hook"] is not None
    assert captured["slack_exception_hook"].hooks == [
        "https://hooks.slack.com/services/T000/B000/PRIMARY"
    ]


def test_report_tag_does_not_expose_internal_username_header_in_openapi():
    app = create_spec_app()
    with TestClient(app) as client:
        spec = client.get("/openapi.json").json()

    operation = spec["paths"]["/tags/report-tag"]["post"]
    parameter_names = {param["name"] for param in operation.get("parameters", [])}
    assert "x-consumer-username" not in parameter_names


def test_pydantic_validation_error_is_handled_as_bad_user_input():
    class SampleInput(BaseModel):
        value: int

    app = FastAPI()
    _register_exception_handlers(app)

    @app.get("/validation-error")
    def raise_validation_error():
        SampleInput.model_validate({"value": "not-an-int"})
        return {"ok": True}

    with TestClient(app) as client:
        response = client.get("/validation-error")

    assert response.status_code == 400
    assert "Input should be a valid integer" in response.json()["detail"]
