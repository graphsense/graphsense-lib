"""
Transaction Class. Functionality depending on the address type.
"""

from typing import Union

from ..utils import hex_to_bytes


class TransactionHashUtxo:
    def __init__(self, txhash: Union[str, bytearray], config):
        raise NotImplementedError("TransactionUtxo not implemented yet")


class TransactionHashAccount:
    def __init__(self, txhash: Union[str, bytearray], config):
        """Init a transaction instance.

        Args:
            txhash (Union[str, bytearray]): transaction hash
            config (ConfigRow): entry from the config table in the transformed keyspace

        Raises:
            Exception: Description
            ValueError: Description
        """
        self.prefix_length = int(config.tx_prefix_length)
        if isinstance(txhash, str):
            self.tx_hash_bytes = hex_to_bytes(txhash)
        elif isinstance(txhash, bytearray):
            self.tx_hash_bytes = txhash
        elif isinstance(txhash, bytes):
            self.tx_hash_bytes = bytearray(txhash)
        else:
            raise Exception(f"Unknown type for txhash type: {type(txhash)}")

        # todo potentially different length for tron, also saved in bytearray though
        if len(self.tx_hash_bytes) != 32:
            raise ValueError(
                f"Address is not the right length {len(self.tx_hash_bytes)}"
            )

    @property
    def hex(self) -> str:  # noqa
        return self.tx_hash_bytes.hex()

    @property
    def db_encoding(self) -> str:
        return self.bytearray
        # return f"0x{self.hex}"

    @property
    def db_encoding_query(self) -> str:
        return f"0x{self.hex}"

    @property
    def prefix(self) -> str:
        return self.hex.upper()[: self.prefix_length]

    @property
    def bytearray(self) -> bytearray:  # noqa
        return self.tx_hash_bytes
