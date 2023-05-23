from collections import namedtuple

from ..config import config, schema_types
from ..datatypes import AddressAccount, AddressUtxo
from .account import RawDbAccount, TransformedDbAccount
from .analytics import AnalyticsDb
from .analytics import KeyspaceConfig as KeyspaceConfigDB
from .cassandra import CassandraDb
from .utxo import RawDbUtxo, TransformedDbUtxo

DbTypeStrategy = namedtuple(
    "DatabaseStrategy", ["raw_db_type", "transformed_db_type", "address_type"]
)


def get_db_types_by_schema_type(schema_type) -> DbTypeStrategy:
    if schema_type not in schema_types:
        raise ValueError(f"{schema_type} not yet defined.")

    if schema_type == "utxo":
        return DbTypeStrategy(RawDbUtxo, TransformedDbUtxo, AddressUtxo)
    elif schema_type == "account":
        return DbTypeStrategy(RawDbAccount, TransformedDbAccount, AddressAccount)
    else:
        raise ValueError(f"{schema_type} not yet supported.")


class DbFactory:
    def from_config(self, env, currency) -> AnalyticsDb:
        e = config.get_environment(env)
        ks = e.get_keyspace(currency)
        return self.from_name(
            ks.raw_keyspace_name,
            ks.transformed_keyspace_name,
            ks.schema_type,
            e.cassandra_nodes,
        )

    def from_name(
        self, raw_keyspace_name, transformed_keyspace_name, schema_type, cassandra_nodes
    ) -> AnalyticsDb:
        db_types = get_db_types_by_schema_type(schema_type)
        return AnalyticsDb(
            raw=KeyspaceConfigDB(
                raw_keyspace_name, db_types.raw_db_type, db_types.address_type
            ),
            transformed=KeyspaceConfigDB(
                transformed_keyspace_name,
                db_types.transformed_db_type,
                db_types.address_type,
            ),
            db=CassandraDb(cassandra_nodes),
        )
