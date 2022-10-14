from ...config import currency_to_schema_type
from .abstractupdater import UpdateStrategy
from .account import UpdateStrategyAccount
from .utxo import UpdateStrategyUtxo


class UpdaterFactory:
    def get_updater(self, currency):
        return self.get_updater_from_schema_type(currency_to_schema_type[currency])

    def get_updater_from_schema_type(self, schema_type) -> UpdateStrategy:
        if schema_type == "utxo":
            return UpdateStrategyUtxo
        elif schema_type == "account":
            return UpdateStrategyAccount
        else:
            raise Exception(f"Unsupported schema type for updates {schema_type}")
