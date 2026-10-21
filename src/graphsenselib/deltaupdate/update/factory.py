import logging

from graphsenselib.deltaupdate.update.generic import ApplicationStrategy

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
        pedantic: bool,
        write_batch: int,
        patch_mode: bool,
        forward_fill_rates: bool = False,
        parallel_pool=None,
        wal_enabled: bool = False,
    ) -> AbstractUpdateStrategy:
        currency = du_config.currency
        schema_type = currency_to_schema_type[currency]
        if schema_type == "utxo":
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
        if schema_type == "account" or schema_type == "account_trx":
            return UpdateStrategyAccount(
                db,
                du_config,
                pedantic,
                ApplicationStrategy.BATCH,
                patch_mode,
                forward_fill_rates=forward_fill_rates,
                parallel_pool=parallel_pool,
                wal_enabled=wal_enabled,
            )
        raise Exception(f"Unsupported schema type {schema_type}")
