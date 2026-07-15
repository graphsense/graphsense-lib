from click.testing import CliRunner

from graphsenselib.schema.cli import schema as schema_cli
from graphsenselib.schema.schema import GraphsenseSchemas


def test_create_cli_passes_overrides(monkeypatch):
    recorded = {}

    def fake_create(self, env, currency, keyspace_type, **kwargs):
        recorded.update(env=env, currency=currency, keyspace_type=keyspace_type)
        recorded.update(kwargs)

    monkeypatch.setattr(GraphsenseSchemas, "create_keyspace_if_not_exist", fake_create)
    result = CliRunner().invoke(
        schema_cli,
        [
            "create",
            "-e",
            "prod",
            "-c",
            "ltc",
            "--keyspace-type",
            "raw",
            "--keyspace-name",
            "ltc_raw_healed",
            "--replication-config",
            "{'class': 'NetworkTopologyStrategy', 'DC1': '2'}",
        ],
    )
    assert result.exit_code == 0, result.output
    assert recorded["keyspace_type"] == "raw"
    assert recorded["keyspace_name_override"] == "ltc_raw_healed"
    assert (
        recorded["replication_config_override"]
        == "{'class': 'NetworkTopologyStrategy', 'DC1': '2'}"
    )
    # validation report runs against config keyspaces; with an override it
    # would validate the wrong keyspace, so it must be skipped
    assert "matches the expectation" not in result.output


def test_create_cli_keyspace_name_requires_type(monkeypatch):
    monkeypatch.setattr(
        GraphsenseSchemas,
        "create_keyspace_if_not_exist",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not be called")),
    )
    result = CliRunner().invoke(
        schema_cli,
        ["create", "-e", "prod", "-c", "ltc", "--keyspace-name", "ltc_raw_healed"],
    )
    assert result.exit_code != 0
    assert "--keyspace-type" in result.output


class _FakeKeyspaceDb:
    def __init__(self, name):
        self._name = name

    def keyspace_name(self):
        return self._name

    def exists(self):
        return False

    def is_configuration_populated(self):
        return True


class _FakeCassandraDb:
    def __init__(self):
        self.created_schema = None

    def setup_keyspace_using_schema(self, schema_string):
        self.created_schema = schema_string


class _FakeAnalyticsDb:
    def __init__(self, keyspace_name):
        self._ksdb = _FakeKeyspaceDb(keyspace_name)
        self._db = _FakeCassandraDb()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def by_ks_type(self, keyspace_type):
        return self._ksdb

    def db(self):
        return self._db


def test_replication_override_lands_in_create_statement(monkeypatch):
    fake_db = _FakeAnalyticsDb("ltc_raw_healed")
    monkeypatch.setattr(
        GraphsenseSchemas,
        "_db_with_override",
        staticmethod(lambda env, currency, keyspace_type, override: fake_db),
    )
    GraphsenseSchemas().create_keyspace_if_not_exist(
        "prod",
        "ltc",
        "raw",
        keyspace_name_override="ltc_raw_healed",
        replication_config_override=(
            "{'class': 'NetworkTopologyStrategy', 'DC1': '2', 'DC2': '0'}"
        ),
    )
    assert fake_db._db.created_schema is not None
    assert "ltc_raw_healed" in fake_db._db.created_schema
    assert (
        "{'class': 'NetworkTopologyStrategy', 'DC1': '2', 'DC2': '0'}"
        in fake_db._db.created_schema
    )
