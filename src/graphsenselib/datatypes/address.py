from typing import Union

from ..utils import hex_to_bytes


class AddressUtxo:
    def __init__(self, adr: Union[str], config):
        """Init an address instance.

        Args:
            adr (Union[str, bytearray]): address
            config (ConfigRow): entry from the config table in the transformed keyspace

        Raises:
            Exception: Description
            ValueError: Description
        """
        self.prefix_length = int(config.address_prefix_length)
        self.bech32_prefix = config.bech_32_prefix
        if isinstance(adr, str):
            self.address = adr
        else:
            raise Exception("Unknown address format")

    @property
    def is_bech32(self):
        return (
            self.bech32_prefix is not None
            and len(self.bech32_prefix) > 0
            and self.address.startswith(self.bech32_prefix)
        )

    @property
    def prefix(self) -> str:
        if self.is_bech32:
            s = len(self.bech32_prefix)
            return self.db_encoding[s : s + self.prefix_length]
        else:
            return self.db_encoding[: self.prefix_length]

    @property
    def db_encoding(self) -> str:
        return self.address

    @property
    def db_encoding_query(self) -> str:
        return f"'{self.address}'"


class AddressAccount:
    def __init__(self, adr: Union[str, bytearray], config):
        """Init an address instance.

        Args:
            adr (Union[str, bytearray]): address
            config (ConfigRow): entry from the config table in the transformed keyspace

        Raises:
            Exception: Description
            ValueError: Description
        """
        self.prefix_length = int(config.address_prefix_length)
        if isinstance(adr, str):
            self.address_bytes = hex_to_bytes(adr)
        elif isinstance(adr, bytearray):
            self.address_bytes = adr
        elif isinstance(adr, bytes):
            self.address_bytes = bytearray(adr)
        else:
            raise Exception("Unknown address type")

        if len(self.address_bytes) != 20:
            raise ValueError(
                f"Address is not the right length {len(self.address_bytes)}"
            )

    @property
    def hex(self) -> str:  # noqa
        return self.address_bytes.hex()

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
        return self.address_bytes


class AddressAccountTrx:
    def __init__(self, adr: Union[str, bytearray], config):
        raise NotImplementedError("AddressAccountTrx not implemented yet")
