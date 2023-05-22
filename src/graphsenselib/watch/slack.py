from typing import List

from ..utils.slack import send_message_to_slack
from .abstract import EventNotifier, FlowEvent, WatchConfig


class SlackEventNotifier(EventNotifier):
    def __init__(self, slack_webhook_urls: List[str]):
        self.slack_webhook_urls = slack_webhook_urls
        self.msgs = []

    def add_notification(self, flow: FlowEvent, receiver_config: WatchConfig, raw_tx):
        self.msgs.append(f"{receiver_config.email}{raw_tx}")

    def send_notifications(self):
        for msg in self.msgs:
            for hook in self.slack_webhook_urls:
                send_message_to_slack(f"{msg}", hook)
        self.msgs = []
