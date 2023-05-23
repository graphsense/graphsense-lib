from typing import Iterable, Optional

from ..utils import flatten, hex_to_bytearray
from ..utils.utxo import SlimTx, get_slim_tx_from_transaction
from .analytics import RawDb, TransformedDb


class TransformedDbUtxo(TransformedDb):
    def get_highest_cluster_id(self, sanity_check=True) -> Optional[int]:
        """Return last ingested cluster ID from cluster table."""
        ha = self._get_hightest_id(table="cluster", sanity_check=sanity_check)
        return ha


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

    def get_tx_outputs(
        self,
        tx_hash: str,
        tx_prefix_length: Optional[int],
        tx_bucket_size: Optional[int],
    ) -> Optional[Iterable]:
        tx_prefix_length = tx_prefix_length or self.get_tx_prefix_length()
        tx_bucket_size = tx_bucket_size or self.get_tx_bucket_size()

        tx_id_record = self.select_one_safe(
            "transaction_by_tx_prefix",
            columns=["tx_id"],
            where={
                "tx_prefix": f"{tx_hash[:tx_prefix_length]}",
                "tx_hash": hex_to_bytearray(tx_hash),
            },
        )
        if tx_id_record:
            tx_id = tx_id_record.tx_id
            result = self.select_one_safe(
                "transaction",
                columns=["outputs"],
                where={"tx_id_group": tx_id // tx_bucket_size, "tx_id": tx_id},
            )

            res = {}
            for i, item in enumerate(result.outputs):
                res[i] = {
                    "addresses": item.address,
                    "value": item.value,
                    "type": item.address_type,
                }
            return res
        else:
            return None

    def get_latest_tx_id_before_block(self, block_id: int) -> Optional[int]:
        last_block = block_id - 1
        bucket_size = self.get_block_bucket_size()

        block = self.select_one_safe(
            "block_transactions",
            where={"block_id_group": last_block // bucket_size, "block_id": last_block},
        )
        latest_tx_id = -1

        if not block:
            return latest_tx_id

        return max(tx.tx_id for tx in block.txs)
