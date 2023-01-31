from typing import Iterable, Optional

from .analytics import RawDb, TransformedDb


class TransformedDbAccount(TransformedDb):
    def get_highest_cluster_id(self, sanity_check=True) -> Optional[int]:
        return None


class RawDbAccount(RawDb):
    def get_logs_in_block(self, block: int, topic0=None, contract=None) -> Iterable:
        group = block // self.get_block_bucket_size()
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
        raise Exception("Not yet implemented.")

    def get_transactions_in_block(self, block: int) -> Iterable:
        raise Exception("Not yet implemented.")

    def get_addresses_in_block(self, block: int) -> Iterable:
        group = block // self.get_block_bucket_size()

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
