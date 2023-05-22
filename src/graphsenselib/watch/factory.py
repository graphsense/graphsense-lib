import os

from ..config import avg_blocktimes_by_currencies, config, currency_to_schema_type
from .account import EthereumEtlFlowProvider
from .flatfile import JsonWatcherState, JsonWatchpointProvider
from .logging import LoggingEventNotifier
from .slack import SlackEventNotifier
from .utxo import UtxoNodeFlowProvider
from .watcher import FlowWatcher


class FlowWatcherFactory:
    def file_based_from_config(
        self, env, currency, state_file, watchpoints_file
    ) -> FlowWatcher:
        if not os.path.isfile(watchpoints_file):
            raise Exception(f"Watchpoints file ({watchpoints_file}) not found.")
        schema = currency_to_schema_type[currency]
        notifiers = [LoggingEventNotifier()]
        slack_topic = config.get_slack_hooks_by_topic("payment_flow_notifications")
        ks_config = config.get_keyspace_config(env, currency)
        if (
            ks_config.ingest_config is None
            or ks_config.ingest_config.node_reference is None
        ):
            raise Exception(
                "There is no node_reference specified in the config "
                f"({env}.{currency}.ingest_config.node_reference is missing)"
            )
        node_ref = ks_config.ingest_config.node_reference
        if slack_topic:
            notifiers.append(SlackEventNotifier(slack_topic.hooks))
        return FlowWatcher(
            state=JsonWatcherState(state_file),
            watchpoints=JsonWatchpointProvider(watchpoints_file),
            flow_provider=EthereumEtlFlowProvider(node_ref)
            if schema == "account"
            else UtxoNodeFlowProvider(node_ref),
            notifiers=notifiers,
            new_block_backoff_sec=avg_blocktimes_by_currencies.get(currency, 600),
        )
