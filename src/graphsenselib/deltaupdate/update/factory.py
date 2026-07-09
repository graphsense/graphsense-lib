import logging

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

logger = logging.getLogger(__name__)


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
        parallel_pool=None,
        wal_enabled: bool = False,
    ) -> AbstractUpdateStrategy:
        currency = du_config.currency
        schema_type = currency_to_schema_type[currency]
        is_account_v2 = (
            schema_type == "account" or schema_type == "account_trx"
        ) and version == 2
        is_utxo_v2 = schema_type == "utxo" and version == 2
        if parallel_pool is not None and not (is_account_v2 or is_utxo_v2):
            logger.warning(
                "--parallel-workers is only supported for the account "
                f"and utxo updaters v2; ignoring it for {schema_type} v{version}."
            )
            parallel_pool = None
        if schema_type == "utxo" and version == 1:
            return UpdateStrategyUtxoLegacy(db, currency, write_new, write_dirty)
        if is_utxo_v2:
            app_strat = (
                ApplicationStrategy.BATCH if write_batch > 1 else ApplicationStrategy.TX
            )
            if parallel_pool is not None and app_strat == ApplicationStrategy.TX:
                logger.warning(
                    "--parallel-workers has no effect in TX mode "
                    "(write-batch-size 1); running single-process."
                )
                parallel_pool = None
            return UpdateStrategyUtxo(
                db,
                currency,
                pedantic,
                app_strat,
                patch_mode,
                forward_fill_rates=forward_fill_rates,
                parallel_pool=parallel_pool,
                wal_enabled=wal_enabled,
            )
        if (schema_type == "account" or schema_type == "account_trx") and version == 1:
            return UpdateStrategyAccountLegacy(
                db,
                currency,
                write_new,
                write_dirty,
                forward_fill_rates=forward_fill_rates,
            )
        if is_account_v2:
            app_strat = ApplicationStrategy.BATCH
            return UpdateStrategyAccount(
                db,
                du_config,
                pedantic,
                app_strat,
                patch_mode,
                forward_fill_rates=forward_fill_rates,
                parallel_pool=parallel_pool,
                wal_enabled=wal_enabled,
            )
        else:
            raise Exception(f"Unsupported schema type {schema_type} or {version}")
