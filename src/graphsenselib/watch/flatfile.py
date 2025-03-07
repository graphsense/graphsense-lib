import json
import logging
import os

from .abstract import WatchConfig, WatcherState, WatchpointProvider

logger = logging.getLogger(__name__)


class JsonWatchpointProvider(WatchpointProvider):
    def __init__(self, filename: str):
        with open(filename) as f:
            self.data = json.load(f)

    def is_watched(self, address: str):
        return address in self.data

    def get_configuration(self, address: str) -> WatchConfig:
        d = self.data.get(address, None)
        return (
            WatchConfig(
                email=d["email"],
                on_outgoing=d["on_outgoing"],
                on_incoming=d["on_incoming"],
                value_gt=d.get("value_lgt", None),
            )
            if d
            else None
        )


class JsonWatcherState(WatcherState):
    def __init__(self, filename: str):
        if not os.path.isfile(filename):
            logging.info(f"Watcher state file not found. Creating new file {filename}")
            with open(filename, "w") as f:
                json.dump({"block": 0}, f)

        self.filename = filename
        self.data = None

    def load(self):
        with open(self.filename) as f:
            self.data = json.load(f)

    def get_next_watch_block(self) -> int:
        return self.data.get("block", 0)

    def done_with_block(self):
        self.data["block"] = self.get_next_watch_block() + 1

    def persist(self):
        with open(self.filename, "w") as f:
            json.dump(self.data, f)
