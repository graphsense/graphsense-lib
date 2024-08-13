from typing import Iterable, Optional

from ..utils import hex_to_bytes, strip_0x
from .analytics import RawDb, TransformedDb


class TransformedDbAccount(TransformedDb):
    def get_highest_cluster_id(self, sanity_check=True) -> Optional[int]:
        return None

    def get_highest_transaction_id(self):
        return self._get_hightest_id(
            table="transaction_ids_by_transaction_id_group",
            sanity_check=True,
            id_col="transaction_id",
        )


class RawDbAccount(RawDb):
    def get_logs_in_block(self, block: int, topic0=None, contract=None) -> Iterable:
        group = self.get_id_group(block, self.get_block_bucket_size())
        if topic0 is None:
            data = self.select_safe(
                "log", where={"block_id": block, "block_id_group": group}
            )
        else:
            data = self.select_safe(
                "log",
                where={"block_id": block, "block_id_group": group, "topic0": topic0},
            )
        if contract is not None:
            data = [log for log in data if log.address == contract]
        return data

    def get_transaction_ids_in_block(self, block: int) -> Iterable:
        raise NotImplementedError

    def get_transactions_in_block(self, block: int) -> Iterable:
        result = self.select(
            "transaction",
            where={"block_id": block},
        )
        return result

    def get_traces_in_block(self, block: int) -> Iterable:
        block_bucket_size = self.get_block_bucket_size()
        group = self.get_id_group(block, block_bucket_size)

        results = self.select(
            "trace", where={"block_id_group": group, "block_id": block}
        )

        return results

    def get_addresses_in_block(self, block: int) -> Iterable:
        group = self.get_id_group(block, self.get_block_bucket_size())

        # The fetch size is needed since traces currenly contain a lot of null values
        # the null values create tombestones and cassandra refuses to read more than
        # 100k tombestones by default per select, this is avoided by reading in chunks
        # https://community.datastax.com/questions/8110/read-operation-failure-error.html
        # the error happens for example in eth block 15676732 since it has more
        # than 50k traces
        result = self.select(
            "trace",
            columns=["from_address", "to_address", "block_id", "tx_hash"],
            where={"block_id_group": group, "block_id": block},
            fetch_size=10000,
        )
        return result

    def get_tx(self, tx_hash: str) -> object:
        tx_prefix_length = self.get_tx_prefix_length()
        prefix = strip_0x(tx_hash)[:tx_prefix_length]

        result = self.select_one_safe(
            "transaction",
            where={
                "tx_hash_prefix": f"{prefix}",
                "tx_hash": hex_to_bytes(tx_hash),
            },
        )
        return result


class RawDbAccountTrx(RawDbAccount):
    def get_addresses_in_block(self, block: int) -> Iterable:
        group = self.get_id_group(block, self.get_block_bucket_size())

        # The fetch size is needed since traces currenly contain a lot of null values
        # the null values create tombestones and cassandra refuses to read more than
        # 100k tombestones by default per select, this is avoided by reading in chunks
        # https://community.datastax.com/questions/8110/read-operation-failure-error.html
        # the error happens for example in eth block 15676732 since it has more
        # than 50k traces
        result = self.select(
            "trace",
            columns=["caller_address", "transferto_address", "block_id", "tx_hash"],
            where={"block_id_group": group, "block_id": block},
            fetch_size=10000,
        )
        for x in result:
            x["from_address"] = x.pop("caller_address")
            x["to_address"] = x.pop("transferto_address")

        return result
