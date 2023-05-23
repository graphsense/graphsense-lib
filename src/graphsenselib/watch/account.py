import logging
from typing import List, Optional, Tuple

import requests

from ..datatypes.common import FlowDirection
from ..ingest.account import EthStreamerAdapter, get_connection_from_url
from ..utils import to_int
from ..utils.logging import suppress_log_level
from .abstract import FlowEvent, FlowProvider

logger = logging.getLogger(__name__)


def parse_node_trace(trace) -> List[FlowEvent]:
    a = trace["action"]
    atype = trace["type"]
    tx = None
    v = to_int(a.get("value", "0x0"))
    if atype == "reward":
        f = None
        t = a["author"]
    elif atype == "call":
        f = a.get("from", None)
        t = a.get("to", None)
    elif atype == "create":
        f = a.get("from", None)
        t = trace["result"]["address"]
    elif atype == "suicide":
        f = a.get("address", None)
        t = a["refundAddress"]
        v = to_int(a.get("balance", "0x0"))
    else:
        raise Exception(f"Unknown trace type: {trace}")
    tx = a.get("transactionHash", None)
    b = trace["blockNumber"]

    return [
        FlowEvent(
            direction=FlowDirection.OUT,
            address=f,
            value=v,
            block=b,
            tx_ref=tx,
            timestamp=None,
        ),
        FlowEvent(
            direction=FlowDirection.IN,
            address=t,
            value=v,
            block=b,
            tx_ref=tx,
            timestamp=None,
        ),
    ]


class AccountNodeFlowProvider(FlowProvider):
    def __init__(self, node_url):
        self.node_url = node_url

    def get_flows_for_block(
        self, block: int
    ) -> Optional[List[Tuple[FlowEvent, object]]]:
        resp = requests.post(
            self.node_url,
            json={
                "jsonrpc": "2.0",
                "method": "trace_block",
                "params": [hex(block)],
                "id": 1,
            },
        )

        if resp.status_code == 200:
            data = resp.json()
            events = []
            if "error" in data:
                logger.info(f"Error reading block_traces {data}")
                return None
            for trace in data["result"]:
                for e in parse_node_trace(trace):
                    events.append((e, trace))
            return events
        else:
            raise Exception(f"Failed to query the node {self.node_url}: {resp}")

        return []


def parse_ethereumetl_trace(trace) -> List[FlowEvent]:
    v = trace["value"]
    b = trace["block_number"]
    tx = trace["transaction_hash"]
    return [
        FlowEvent(
            direction=FlowDirection.OUT,
            address=trace["from_address"],
            value=v,
            block=b,
            tx_ref=tx,
            timestamp=None,
        ),
        FlowEvent(
            direction=FlowDirection.IN,
            address=trace["to_address"],
            value=v,
            block=b,
            tx_ref=tx,
            timestamp=None,
        ),
    ]


class EthereumEtlFlowProvider(FlowProvider):
    def __init__(self, node_url):
        self.node_url = node_url
        self.stream = EthStreamerAdapter(
            get_connection_from_url(self.node_url), batch_size=1
        )

    def get_flows_for_block(
        self, block: int
    ) -> Optional[List[Tuple[FlowEvent, object]]]:
        with suppress_log_level(logging.INFO):
            try:
                traces = self.stream.export_traces(block, block, True, True)
            except ValueError as e:
                if "could not find block" in str(e):
                    return None
                raise e

        events = []
        for trace in traces:
            for e in parse_ethereumetl_trace(trace):
                events.append((e, trace))

        return events if events else None
