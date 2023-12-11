from abc import ABC, abstractmethod
from typing import Optional

from ..config import currency_to_schema_type
from ..db import AnalyticsDb
from .account import ingest as ingest_eth
from .utxo import ingest as ingest_utxo


class IngestModule(ABC):
    @abstractmethod
    def ingest(
        self,
        db: AnalyticsDb,
        currency: str,
        source: str,
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
        source: str,
        sink_config: dict,
        user_start_block: Optional[int],
        user_end_block: Optional[int],
        batch_size: int,
        info: bool,
        previous_day: bool,
        provider_timeout: int,
        mode: str,
    ):
        ingest_eth(
            db=db,
            currency=currency,
            provider_uri=source,
            sink_config=sink_config,
            user_start_block=user_start_block,
            user_end_block=user_end_block,
            batch_size=batch_size,
            info=info,
            previous_day=previous_day,
            provider_timeout=provider_timeout,
        )


class IngestModuleUtxo(IngestModule):
    def ingest(
        self,
        db: AnalyticsDb,
        currency: str,
        source: str,
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
            provider_uri=source,
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
    def from_config(self, env, currency) -> IngestModule:
        schema_type = currency_to_schema_type.get(currency, "")
        if schema_type == "account":
            return IngestModuleAccount()
        if schema_type == "utxo":
            return IngestModuleUtxo()
        else:
            raise Exception(
                f"No ingest module defined for {env}:{currency}>{schema_type}"
            )
