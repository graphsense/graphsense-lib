from collections import namedtuple
from typing import Optional

from ..config import get_config, schema_types
from ..datatypes import (
    AddressAccount,
    AddressUtxo,
    TransactionHashAccount,
    TransactionHashUtxo,
)
from .account import RawDbAccount, RawDbAccountTrx, TransformedDbAccount
from .analytics import AnalyticsDb
from .analytics import KeyspaceConfig as KeyspaceConfigDB
from .cassandra import CassandraDb
from .utxo import RawDbUtxo, TransformedDbUtxo

DbTypeStrategy = namedtuple(
    "DatabaseStrategy",
    ["raw_db_type", "transformed_db_type", "address_type", "transaction_type"],
)


def get_db_types_by_schema_type(schema_type) -> DbTypeStrategy:
    if schema_type not in schema_types:
        raise ValueError(f"{schema_type} not yet defined.")

    if schema_type == "utxo":
        return DbTypeStrategy(
            RawDbUtxo, TransformedDbUtxo, AddressUtxo, TransactionHashUtxo
        )
    elif schema_type == "account":
        return DbTypeStrategy(
            RawDbAccount, TransformedDbAccount, AddressAccount, TransactionHashAccount
        )
    elif schema_type == "account_trx":
        return DbTypeStrategy(
            RawDbAccountTrx,
            TransformedDbAccount,
            AddressAccount,
            TransactionHashAccount,
        )
    else:
        raise ValueError(f"{schema_type} not yet supported.")


class DbFactory:
    def from_config(self, env, currency, readonly: bool = False) -> AnalyticsDb:
        config = get_config()
        e = config.get_environment(env)
        ks = e.get_keyspace(currency)

        user = e.username
        pw = e.password

        if readonly:
            user = e.readonly_username
            pw = e.readonly_password

        return self.from_name(
            ks.raw_keyspace_name,
            ks.transformed_keyspace_name,
            ks.schema_type,
            e.cassandra_nodes,
            currency,
            username=user,
            password=pw,
        )

    def from_name(
        self,
        raw_keyspace_name,
        transformed_keyspace_name,
        schema_type,
        cassandra_nodes,
        currency,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> AnalyticsDb:
        db_types = get_db_types_by_schema_type(schema_type)
        return AnalyticsDb(
            raw=KeyspaceConfigDB(
                raw_keyspace_name,
                db_types.raw_db_type,
                db_types.address_type,
                db_types.transaction_type,
                currency,
            ),
            transformed=KeyspaceConfigDB(
                transformed_keyspace_name,
                db_types.transformed_db_type,
                db_types.address_type,
                db_types.transaction_type,
                currency,
            ),
            db=CassandraDb(cassandra_nodes, username=username, password=password),
        )
