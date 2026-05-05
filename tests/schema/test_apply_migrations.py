from collections import namedtuple
from contextlib import contextmanager

from graphsenselib.schema.schema import GraphsenseSchemas


class _FakeDbKs:
    def __init__(self, configuration_row):
        self._row = configuration_row
        self.executed = []

    def get_configuration(self):
        return self._row

    def execute_raw_cql(self, cql: str):
        self.executed.append(cql)


class _FakeDb:
    def __init__(self, db_ks):
        self._db_ks = db_ks

    def by_ks_type(self, _keyspace_type):
        return self._db_ks


@contextmanager
def _fake_db_with_override(_env, _currency, _keyspace_type, _override, db_ks):
    yield _FakeDb(db_ks)


def _patch_db_with_override(monkeypatch, db_ks):
    @staticmethod
    def _override(env, currency, keyspace_type, keyspace_name_override):
        return _fake_db_with_override(
            env, currency, keyspace_type, keyspace_name_override, db_ks
        )

    monkeypatch.setattr(GraphsenseSchemas, "_db_with_override", _override)


def test_apply_migrations_updates_transformed_configuration_by_keyspace_name(
    monkeypatch,
):
    # Reproduces the prod failure where apply_migrations against a
    # transformed_utxo configuration row blew up with
    # "AttributeError: 'Row' object has no attribute 'id'".
    # The transformed configuration table uses ``keyspace_name`` as PK,
    # not ``id`` (which only exists on raw configurations).
    TransformedRow = namedtuple(
        "Row",
        [
            "keyspace_name",
            "bucket_size",
            "address_prefix_length",
            "bech_32_prefix",
            "coinjoin_filtering",
            "fiat_currencies",
        ],
    )
    row = TransformedRow(
        keyspace_name="pytest_btc_transformed",
        bucket_size=5000,
        address_prefix_length=3,
        bech_32_prefix="bc1",
        coinjoin_filtering=True,
        fiat_currencies=["EUR", "USD"],
    )
    db_ks = _FakeDbKs(row)
    _patch_db_with_override(monkeypatch, db_ks)

    GraphsenseSchemas().apply_migrations("pytest", "btc", "transformed")

    update_stmts = [
        s for s in db_ks.executed if s.lstrip().upper().startswith("UPDATE")
    ]
    assert update_stmts, f"no UPDATE configuration statement: {db_ks.executed}"
    for update in update_stmts:
        assert "WHERE keyspace_name = 'pytest_btc_transformed'" in update
        assert " id =" not in update
    assert "schema_version = 1" in update_stmts[0]


def test_apply_migrations_updates_raw_configuration_by_id(monkeypatch):
    # Raw configurations keep their existing ``id`` PK; the fix must not
    # regress that path.
    RawRow = namedtuple(
        "Row", ["id", "block_bucket_size", "tx_bucket_size", "tx_prefix_length"]
    )
    row = RawRow(
        id="pytest_btc_raw",
        block_bucket_size=100,
        tx_bucket_size=25000,
        tx_prefix_length=5,
    )
    db_ks = _FakeDbKs(row)
    _patch_db_with_override(monkeypatch, db_ks)

    GraphsenseSchemas().apply_migrations("pytest", "btc", "raw")

    update_stmts = [
        s for s in db_ks.executed if s.lstrip().upper().startswith("UPDATE")
    ]
    assert update_stmts, f"no UPDATE configuration statement: {db_ks.executed}"
    for update in update_stmts:
        assert "WHERE id = 'pytest_btc_raw'" in update
        assert "keyspace_name" not in update
