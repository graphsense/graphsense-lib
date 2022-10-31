from typing import Iterable

from ..utils import flatten
from ..utils.utxo import SlimTx, get_slim_tx_from_transaction
from .analytics import RawDb, TransformedDb


class TransformedDbUtxo(TransformedDb):
    pass


class RawDbUtxo(RawDb):
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

    def get_transactions_in_block(self, block: int) -> Iterable:
        tx_ids = self.get_transaction_ids_in_block(block)
        tx_bucket_size = self.get_tx_bucket_size()
        stmt = self.select_stmt(
            "transaction",
            where={"tx_id_group": "?", "tx_id": "?"},
            limit=1,
        )

        parameters = [(tx_id, [tx_id // tx_bucket_size, tx_id]) for tx_id in tx_ids]
        results = self._db.execute_batch_async(stmt, parameters)
        return [tx.one() for tx_id, tx in self._db.await_batch(results)]

    def get_addresses_in_block(self, block: int) -> Iterable[SlimTx]:
        tx_ids = self.get_transaction_ids_in_block(block)
        tx_bucket_size = self.get_tx_bucket_size()
        addresses = []
        for tx_id in tx_ids:
            group = tx_id // tx_bucket_size
            result = self.select_one(
                "transaction",
                columns=["inputs", "outputs", "block_id", "tx_hash", "timestamp"],
                where={"tx_id_group": group, "tx_id": tx_id},
            )

            addresses += get_slim_tx_from_transaction(result)
        return addresses
