import os

from ..config import (
    ConfigError,
    avg_blocktimes_by_currencies,
    currency_to_schema_type,
    get_config,
)
from ..db import DbFactory
from ..ingest.utxo import CassandraOutputResolver
from .account import EthereumEtlFlowProvider
from .flatfile import JsonWatcherState, JsonWatchpointProvider
from .logging import LoggingEventNotifier
from .slack import SlackEventNotifier
from .utxo import BitcoinEtlFlowProvider
from .watcher import FlowWatcher


class FlowWatcherFactory:
    def file_based_from_config(
        self, env, currency, state_file, watchpoints_file
    ) -> FlowWatcher:
        config = get_config()
        if not os.path.isfile(watchpoints_file):
            raise ValueError(f"Watchpoints file ({watchpoints_file}) not found.")
        schema = currency_to_schema_type[currency]

        notifiers = [LoggingEventNotifier()]
        slack_topic = config.get_slack_hooks_by_topic("payment_flow_notifications")
        if slack_topic:
            notifiers.append(SlackEventNotifier(slack_topic.hooks))

        ks_config = config.get_keyspace_config(env, currency)
        if (
            ks_config.ingest_config is None
            or ks_config.ingest_config.get_first_node_reference() is None
        ):
            raise ConfigError(
                "There is no node_reference specified in the config "
                f"({env}.{currency}.ingest_config.node_reference is missing)"
            )
        node_ref = ks_config.ingest_config.get_first_node_reference()

        return FlowWatcher(
            state=JsonWatcherState(state_file),
            watchpoints=JsonWatchpointProvider(watchpoints_file),
            flow_provider=(
                EthereumEtlFlowProvider(node_ref)
                if schema == "account"
                else BitcoinEtlFlowProvider(
                    currency,
                    node_ref,
                    CassandraOutputResolver(DbFactory().from_config(env, currency)),
                )
            ),
            notifiers=notifiers,
            new_block_backoff_sec=avg_blocktimes_by_currencies.get(currency, 600),
        )
