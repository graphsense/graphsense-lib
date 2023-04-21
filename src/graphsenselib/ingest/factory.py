from abc import ABC
from typing import Optional

from ..db import AnalyticsDb
from .account import ingest as ingest_eth


class IngestModule(ABC):
    def ingest(
        self,
        db: AnalyticsDb,
        source: str,
        sink_config: dict,
        user_start_block: Optional[int],
        user_end_block: Optional[int],
        batch_size: int,
        info: bool,
        previous_day: bool,
        provider_timeout: int,
    ):
        pass


class IngestModuleAccount(IngestModule):
    def ingest(
        self,
        db: AnalyticsDb,
        source: str,
        sink_config: dict,
        user_start_block: Optional[int],
        user_end_block: Optional[int],
        batch_size: int,
        info: bool,
        previous_day: bool,
        provider_timeout: int,
    ):
        ingest_eth(
            db=db,
            provider_uri=source,
            sink_config=sink_config,
            user_start_block=user_start_block,
            user_end_block=user_end_block,
            batch_size=batch_size,
            info=info,
            previous_day=previous_day,
            provider_timeout=provider_timeout,
        )


class IngestFactory:
    def from_config(self, env, currency) -> IngestModule:
        if currency == "eth":
            return IngestModuleAccount()
        else:
            raise Exception(f"No ingest module defined for {env}:{currency}")
