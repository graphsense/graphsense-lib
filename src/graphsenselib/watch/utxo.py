import logging
from typing import List, Optional, Tuple

from ..datatypes.common import FlowDirection
from ..ingest.fast_btc import FastBtcBlockExporter
from ..ingest.utxo import (
    OutputResolverBase,
    enrich_txs,
)
from ..utils import parse_timestamp
from ..utils.logging import suppress_log_level
from .abstract import FlowEvent, FlowProvider

logger = logging.getLogger(__name__)


def parse_btcetl_txs(tx) -> List[FlowEvent]:
    output = []
    tx_h = tx["hash"]
    time = parse_timestamp(tx["block_timestamp"])
    b = tx["block_number"]

    for o in tx["outputs"]:
        v = (
            (o["value"] / len(o["addresses"]))
            if len(o["addresses"]) > 0
            else o["value"]
        )
        for a in o.get("addresses", []):
            output.append(
                FlowEvent(
                    direction=FlowDirection.OUT,
                    address=a,
                    value=v,
                    block=b,
                    tx_ref=tx_h,
                    timestamp=time,
                )
            )

    for o in tx["inputs"]:
        if "addresses" in o and len(o["addresses"]) != 0:
            v = (
                (o["value"] / len(o["addresses"]))
                if len(o["addresses"]) > 0
                else o["value"]
            )
        for a in o.get("addresses", []):
            output.append(
                FlowEvent(
                    direction=FlowDirection.OUT,
                    address=a,
                    value=v,
                    block=b,
                    tx_ref=tx_h,
                    timestamp=time,
                )
            )
    return output


class BitcoinEtlFlowProvider(FlowProvider):
    node_url: str
    exporter: FastBtcBlockExporter
    output_resolver: OutputResolverBase

    def __init__(
        self, currency: str, node_url: str, output_resolver: OutputResolverBase
    ):
        self.node_url = node_url
        self.exporter = FastBtcBlockExporter(
            provider_uri=node_url, max_workers=1, timeout=60
        )
        self.output_resolver = output_resolver

    def __enter__(self):
        self.output_resolver.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.output_resolver.__exit__(exc_type, exc_val, exc_tb)

    def get_flows_for_block(
        self, block: int
    ) -> Optional[List[Tuple[FlowEvent, object]]]:
        with suppress_log_level(logging.INFO):
            _, txs = self.exporter.export_blocks_and_transactions(block, block)
            enrich_txs(txs, self.output_resolver, ignore_missing_outputs=True)

        events = []
        for tx in txs:
            for e in parse_btcetl_txs(tx):
                events.append((e, tx))

        return events if events else None
