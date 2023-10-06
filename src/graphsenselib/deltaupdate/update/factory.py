from ...config import currency_to_schema_type
from ...db import AnalyticsDb
from .abstractupdater import AbstractUpdateStrategy
from .account import UpdateStrategyAccount
from .generic import ApplicationStrategy
from .utxo import UpdateStrategyUtxo
from .utxolegacy import UpdateStrategyUtxoLegacy


class UpdaterFactory:
    def get_updater(
        self,
        currency: str,
        db: AnalyticsDb,
        version: int,
        write_new: bool,
        write_dirty: bool,
        pedantic: bool,
        write_batch: int,
        patch_mode: bool,
        forward_fill_rates: bool = False,
    ) -> AbstractUpdateStrategy:
        schema_type = currency_to_schema_type[currency]
        if schema_type == "utxo" and version == 1:
            return UpdateStrategyUtxoLegacy(db, currency, write_new, write_dirty)
        if schema_type == "utxo" and version == 2:
            app_strat = (
                ApplicationStrategy.BATCH if write_batch > 1 else ApplicationStrategy.TX
            )
            return UpdateStrategyUtxo(
                db,
                currency,
                pedantic,
                app_strat,
                patch_mode,
                forward_fill_rates=forward_fill_rates,
            )
        if schema_type == "account" and version == 1:
            return UpdateStrategyAccount(
                db,
                currency,
                write_new,
                write_dirty,
                forward_fill_rates=forward_fill_rates,
            )
        else:
            raise Exception(f"Unsupported schema type {schema_type} or {version}")
