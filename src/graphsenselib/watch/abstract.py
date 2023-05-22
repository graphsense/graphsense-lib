from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

from ..datatypes.common import FlowDirection


@dataclass
class FlowEvent:
    direction: FlowDirection
    address: str
    value: int
    block: int
    timestamp: Optional[datetime]
    tx_ref: Optional[str]


@dataclass
class WatchConfig:
    email: str
    on_incoming: bool
    on_outgoing: bool
    value_gt: Optional[int]


class WatchpointProvider(ABC):
    @abstractmethod
    def is_watched(self, address: str):
        pass

    @abstractmethod
    def get_configuration(self, address: str) -> WatchConfig:
        pass


class EventNotifier(ABC):
    @abstractmethod
    def add_notification(self, flow: FlowEvent, receiver_config: WatchConfig, raw_tx):
        pass

    @abstractmethod
    def send_notifications(self):
        pass


class FlowProvider(ABC):
    @abstractmethod
    def get_flows_for_block(
        self, block: int
    ) -> Optional[List[Tuple[FlowEvent, object]]]:
        pass


class WatcherState(ABC):
    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def get_next_watch_block(self) -> int:
        pass

    @abstractmethod
    def done_with_block(self):
        pass

    @abstractmethod
    def persist(self):
        pass
