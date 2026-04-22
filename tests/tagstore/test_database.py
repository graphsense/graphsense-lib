import graphsenselib.tagstore.db.database as database


class _DummyEngine:
    def __init__(self):
        self.disposed = False

    def dispose(self):
        self.disposed = True


def test_to_sync_db_url_converts_asyncpg_scheme():
    src = "postgresql+asyncpg://user:pass@localhost:5432/tagstore"

    out = database.to_sync_db_url(src)

    assert out == "postgresql+psycopg2://user:pass@localhost:5432/tagstore"


def test_to_sync_db_url_keeps_sync_scheme_unchanged():
    src = "postgresql+psycopg2://user:pass@localhost:5432/tagstore"

    out = database.to_sync_db_url(src)

    assert out == src


def test_get_views_ddl_sql_can_exclude_quality_measures_section():
    full_sql = database.get_views_ddl_sql(include_quality_measures=True)
    runtime_sql = database.get_views_ddl_sql(include_quality_measures=False)

    assert "CREATE MATERIALIZED VIEW IF NOT EXISTS statistics" in runtime_sql
    assert "DROP TABLE IF EXISTS address_quality" not in runtime_sql
    assert len(runtime_sql) < len(full_sql)


def test_ensure_database_initialized_skips_when_schema_exists(monkeypatch):
    engine = _DummyEngine()
    init_calls = []

    monkeypatch.setattr(database, "get_db_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(database, "is_database_initialized", lambda _engine: True)
    monkeypatch.setattr(
        database,
        "init_database",
        lambda *_args, **_kwargs: init_calls.append("called"),
    )

    out = database.ensure_database_initialized("postgresql+asyncpg://dummy")

    assert out is False
    assert init_calls == []
    assert engine.disposed is True


def test_ensure_database_initialized_runs_init_when_missing(monkeypatch):
    engine = _DummyEngine()
    init_calls = []

    monkeypatch.setattr(database, "get_db_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(database, "is_database_initialized", lambda _engine: False)

    def _record_init(_engine, include_quality_measures=True):
        init_calls.append(include_quality_measures)

    monkeypatch.setattr(database, "init_database", _record_init)

    out = database.ensure_database_initialized(
        "postgresql+asyncpg://dummy",
        include_quality_measures=False,
    )

    assert out is True
    assert init_calls == [False]
    assert engine.disposed is True
