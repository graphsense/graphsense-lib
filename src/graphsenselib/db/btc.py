from collections import namedtuple
from typing import Iterable, Union

from ..utils import flatten
from .analytics import RawDb, TransformedDb

AddressIntoduced = namedtuple(
    "AddressIntroduced", ["address", "block_id", "timestamp", "tx_hash"]
)


class TransformedDbBTC(TransformedDb):
    pass


class RawDbBTC(RawDb):
    def get_transaction_ids_in_block(self, block: int) -> Iterable:
        block_bucket_size = self.get_block_bucket_size()
        group = block // block_bucket_size
        result = self.select(
            "block_transactions",
            columns=["txs"],
            where={"block_id_group": group, "block_id": block},
        )
        txs = flatten([block.txs for block in result])
        ids = [tx.tx_id for tx in txs]
        return ids

    def get_addresses_in_block(self, block: int) -> Iterable:
        # flake8: noqa: W503
        tx_ids = self.get_transaction_ids_in_block(block)
        tx_bucket_size = self.get_tx_bucket_size()
        addresses = []
        for tx_id in tx_ids:
            group = tx_id // tx_bucket_size
            result = self.select(
                "transaction",
                columns=["inputs", "outputs", "block_id", "tx_hash", "timestamp"],
                where={"tx_id_group": group, "tx_id": tx_id},
                limit=2,
            )
            result = self._at_most_one_result(result)

            addresses += [
                AddressIntoduced(
                    addr, result.block_id, result.timestamp, result.tx_hash
                )
                for addr in flatten(
                    (
                        [x.address for x in result.outputs if x.address is not None]
                        if result.outputs
                        else []
                    )
                    + (
                        [x.address for x in result.inputs if x.address is not None]
                        if result.inputs
                        else []
                    )
                )
            ]
        return addresses
