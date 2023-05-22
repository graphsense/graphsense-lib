import logging
import time
from typing import List

from ..datatypes import FlowDirection
from ..utils.signals import gracefull_ctlc_shutdown
from .abstract import EventNotifier, FlowProvider, WatcherState, WatchpointProvider

logger = logging.getLogger(__name__)


class FlowWatcher:
    def __init__(
        self,
        state: WatcherState,
        watchpoints: WatchpointProvider,
        flow_provider: FlowProvider,
        notifiers: List[EventNotifier],
        new_block_backoff_sec: int = 600,
    ):
        self.provider = flow_provider
        self.notifiers = notifiers
        self.state = state
        self.watchpoints = watchpoints
        self.new_block_backoff_sec = new_block_backoff_sec

        self.state.load()

    def watch(self):
        try:
            with gracefull_ctlc_shutdown() as shutdown_initialized:
                while True:
                    next_block = self.state.get_next_watch_block()

                    flows = self.provider.get_flows_for_block(next_block)

                    if next_block % 1000 == 0:
                        logger.info(f"Generate notifications for block {next_block}")

                    if shutdown_initialized():
                        self.state.persist()
                        return

                    if flows is None:
                        wait_sec = int(self.new_block_backoff_sec * 1.3)
                        logger.info(f"No data found for block, waiting {wait_sec}s.")
                        time.sleep(wait_sec)
                        continue

                    for flow, raw_flow in flows:
                        if self.watchpoints.is_watched(flow.address):
                            wconfig = self.watchpoints.get_configuration(flow.address)
                            if (
                                (
                                    wconfig.on_incoming
                                    and flow.direction == FlowDirection.IN
                                )
                                or (
                                    wconfig.on_outgoing
                                    and flow.direction == FlowDirection.OUT
                                )
                            ) and (
                                wconfig.value_gt is None
                                or flow.value > wconfig.value_gt
                            ):
                                for notifier in self.notifiers:
                                    notifier.add_notification(flow, wconfig, raw_flow)

                    for notifier in self.notifiers:
                        notifier.send_notifications()
                    self.state.done_with_block()
        finally:
            self.state.persist()
