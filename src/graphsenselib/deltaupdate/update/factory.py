from graphsenselib.deltaupdate.update.account.accountlegacy import (
    UpdateStrategyAccountLegacy,
)
from graphsenselib.deltaupdate.update.generic import ApplicationStrategy
from graphsenselib.deltaupdate.update.utxo.utxolegacy import UpdateStrategyUtxoLegacy

from ...config import currency_to_schema_type
from ...config.config import DeltaUpdaterConfig
from ...db import AnalyticsDb
from .abstractupdater import AbstractUpdateStrategy
from .account import UpdateStrategyAccount
from .utxo import UpdateStrategyUtxo


class UpdaterFactory:
    def get_updater(
        self,
        du_config: DeltaUpdaterConfig,
        db: AnalyticsDb,
        version: int,
        write_new: bool,
        write_dirty: bool,
        pedantic: bool,
        write_batch: int,
        patch_mode: bool,
        forward_fill_rates: bool = False,
    ) -> AbstractUpdateStrategy:
        currency = du_config.currency
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
        if (schema_type == "account" or schema_type == "account_trx") and version == 1:
            return UpdateStrategyAccountLegacy(
                db,
                currency,
                write_new,
                write_dirty,
                forward_fill_rates=forward_fill_rates,
            )
        if (schema_type == "account" or schema_type == "account_trx") and version == 2:
            app_strat = ApplicationStrategy.BATCH
            return UpdateStrategyAccount(
                db,
                du_config,
                pedantic,
                app_strat,
                patch_mode,
                forward_fill_rates=forward_fill_rates,
            )
        else:
            raise Exception(f"Unsupported schema type {schema_type} or {version}")
