from typing import Iterable, Optional

from ..errors.errors import DBInconsistencyException
from ..utils import flatten, hex_to_bytes
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
        group = self.get_id_group(block, block_bucket_size)
        result = self.select(
            "block_transactions",
            columns=["txs"],
            where={"block_id_group": group, "block_id": block},
        )
        txs = flatten([block.txs for block in result])
        ids = [tx.tx_id for tx in txs]
        return ids

    def get_transactions_in_block(self, block: int) -> Iterable:
        tx_bucket_size = self.get_tx_bucket_size()
        minb = self.get_latest_tx_id_before_block(block)
        maxb = self.get_latest_tx_id_before_block(block + 1)

        mbg = self.get_id_group(minb, tx_bucket_size)
        mxbg = self.get_id_group(maxb, tx_bucket_size)

        rg = list(range(mbg, mxbg + 1))

        stmt = (
            f"select * from {self.get_keyspace()}.transaction where "
            "tx_id_group in :txidgroups and tx_id > :txid_lower "
            "and tx_id <= :txid_upper"
        )

        prepared_statement = self._db.get_prepared_statement(stmt)

        bstmt = prepared_statement.bind(
            {"txidgroups": rg, "txid_lower": minb, "txid_upper": maxb}
        )

        results = self._db.execute_statement(bstmt)

        return list(results)

    def get_addresses_in_block(self, block: int) -> Iterable[SlimTx]:
        tx_ids = self.get_transaction_ids_in_block(block)
        tx_bucket_size = self.get_tx_bucket_size()
        addresses = []
        for tx_id in tx_ids:
            group = self.get_id_group(tx_id, tx_bucket_size)
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
                "tx_hash": hex_to_bytes(tx_hash),
            },
        )
        if tx_id_record:
            tx_id = tx_id_record.tx_id
            result = self.select_one_safe(
                "transaction",
                columns=["outputs"],
                where={
                    "tx_id_group": self.get_id_group(tx_id, tx_bucket_size),
                    "tx_id": tx_id,
                },
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

    def get_latest_tx_id_before_block(self, block_id: int) -> int:
        if block_id <= 0:
            return -1

        last_block = block_id - 1
        bucket_size = self.get_block_bucket_size()

        block = self.select_one_safe(
            "block_transactions",
            where={
                "block_id_group": self.get_id_group(last_block, bucket_size),
                "block_id": last_block,
            },
        )
        if block is not None:
            return max(tx.tx_id for tx in block.txs)

        # block_transactions row for the preceding block is missing. If the
        # keyspace is genuinely empty, returning -1 is correct (allocator
        # starts at 0). If the block table already extends past last_block,
        # restarting from 0 would silently overwrite existing tx_ids.
        highest_block = self.get_highest_block(sanity_check=False)
        if highest_block is None:
            return -1

        raise DBInconsistencyException(
            f"block_transactions[{last_block}] is missing in keyspace "
            f"{self.get_keyspace()!r} but the block table extends to "
            f"{highest_block:,}. Refusing to start tx_id allocation at 0 — "
            f"this would overwrite existing tx_ids."
        )
