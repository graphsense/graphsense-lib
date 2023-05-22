import itertools
import json
import logging
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import requests

# from ..datatypes.common import FlowDirection
# from ..utils import fallback
# from ..utils.accountmodel import to_int
from .abstract import FlowEvent, FlowProvider

# from requests.auth import HTTPBasicAuth


logger = logging.getLogger(__name__)

id_counter = itertools.count()


class BtcLikeRPC(object):
    def __init__(self, url, user, passwd, log, method=None, timeout=30):
        self.url = url
        self._user = user
        self._passwd = passwd
        self._method_name = method
        self._timeout = timeout
        self._log = log

    def __getattr__(self, method_name):
        return BtcLikeRPC(
            self.url,
            self._user,
            self._passwd,
            self._log,
            method_name,
            timeout=self._timeout,
        )

    def __call__(self, *args):
        playload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": next(id_counter),
                "method": self._method_name,
                "params": args,
            }
        )
        headers = {"Content-type": "application/json", "cache-control": "no-cache"}
        resp = requests.post(
            self.url,
            headers=headers,
            data=playload,
            timeout=self._timeout,
            auth=(self._user, self._passwd),
        )
        return resp


class UtxoNodeFlowProvider(FlowProvider):
    def __init__(self, node_url):
        purl = urlparse(node_url)
        self.node_url = node_url.replace(f"{purl.username}:{purl.password}@", "")
        self.rpc = BtcLikeRPC(self.node_url, purl.username, purl.password, logger)

    def get_block_hash(self, block: int) -> Optional[str]:
        resp = self.rpc.getblockhash(block)

        # resp2 = self.rpc["getblock"](block)

        if resp.status_code == 200:
            data = resp.json()
            tx_hash = data["result"]
            resp = self.rpc.getblock(tx_hash, 3)
        else:
            return None

    def get_flows_for_block(
        self, block: int
    ) -> Optional[List[Tuple[FlowEvent, object]]]:
        self.get_block_hash(block)

        return []
