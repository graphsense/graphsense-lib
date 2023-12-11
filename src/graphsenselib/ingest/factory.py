from abc import ABC, abstractmethod
from typing import List, Optional
from warnings import warn

from ..config import currency_to_schema_type
from ..db import AnalyticsDb
from .account import ingest as ingest_account
from .account import ingest_async as ingest_account_async
from .utxo import ingest as ingest_utxo


class IngestModule(ABC):
    @abstractmethod
    def ingest(
        self,
        db: AnalyticsDb,
        currency: str,
        sources: List[str],
        sink_config: dict,
        user_start_block: Optional[int],
        user_end_block: Optional[int],
        batch_size: int,
        info: bool,
        previous_day: bool,
        provider_timeout: int,
        mode: str,
    ):
        pass


class IngestModuleAccount(IngestModule):
    def ingest(
        self,
        db: AnalyticsDb,
        currency: str,
        sources: List[str],
        sink_config: dict,
        user_start_block: Optional[int],
        user_end_block: Optional[int],
        batch_size: int,
        info: bool,
        previous_day: bool,
        provider_timeout: int,
        mode: str,
    ):
        ingest_account(
            db=db,
            currency=currency,
            sources=sources,
            sink_config=sink_config,
            user_start_block=user_start_block,
            user_end_block=user_end_block,
            batch_size=batch_size,
            info=info,
            previous_day=previous_day,
            provider_timeout=provider_timeout,
            mode=mode,
        )


class IngestModuleAccountAsync(IngestModule):
    def ingest(
        self,
        db: AnalyticsDb,
        currency: str,
        sources: List[str],
        sink_config: dict,
        user_start_block: Optional[int],
        user_end_block: Optional[int],
        batch_size: int,
        info: bool,
        previous_day: bool,
        provider_timeout: int,
        mode: str,
    ):
        ingest_account_async(
            db=db,
            currency=currency,
            sources=sources,
            sink_config=sink_config,
            user_start_block=user_start_block,
            user_end_block=user_end_block,
            batch_size_user=batch_size,
            info=info,
            previous_day=previous_day,
            provider_timeout=provider_timeout,
            mode=mode,
        )


class IngestModuleUtxo(IngestModule):
    def ingest(
        self,
        db: AnalyticsDb,
        currency: str,
        sources: List[str],
        sink_config: dict,
        user_start_block: Optional[int],
        user_end_block: Optional[int],
        batch_size: int,
        info: bool,
        previous_day: bool,
        provider_timeout: int,
        mode: str,
    ):
        ingest_utxo(
            db=db,
            currency=currency,
            provider_uri=sources[0],
            sink_config=sink_config,
            user_start_block=user_start_block,
            user_end_block=user_end_block,
            batch_size=batch_size,
            info=info,
            previous_day=previous_day,
            provider_timeout=provider_timeout,
            mode=mode,
        )


class IngestFactory:
    def from_config(self, env, currency, version) -> IngestModule:
        schema_type = currency_to_schema_type.get(currency, "")
        if schema_type in ["account", "account_trx"] and version == 1:
            warn("Serial ingestion is deprecated, use asynchronous ingestion instead.")
            return IngestModuleAccount()
        if schema_type in ["account", "account_trx"] and version == 2:
            return IngestModuleAccountAsync()
        if schema_type == "utxo" and version == 1:
            return IngestModuleUtxo()
        else:
            raise Exception(
                f"No ingest module defined for {env}:{currency}>{schema_type}"
                f" and version {version}"
            )
