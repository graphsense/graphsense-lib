import logging

from .abstract import EventNotifier, FlowEvent, WatchConfig

logger = logging.getLogger(__name__)


class LoggingEventNotifier(EventNotifier):
    def __init__(self):
        self.msgs = []

    def add_notification(self, flow: FlowEvent, receiver_config: WatchConfig, raw_tx):
        self.msgs.append(f"{receiver_config.email}, {flow}, {raw_tx}")

    def send_notifications(self):
        for msg in self.msgs:
            logger.info(self.msgs)
        self.msgs = []
