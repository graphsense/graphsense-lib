from typing import Union

from ..utils import hex_to_bytearray


class AddressUtxo:
    def __init__(self, adr: Union[str], prefix_length: int):
        self.prefix_length = prefix_length
        if type(adr) == str:
            self.address = adr
        else:
            raise Exception("Unknown address format")

    @property
    def prefix(self) -> str:
        return self.db_encoding[: self.prefix_length]

    @property
    def db_encoding(self) -> str:
        return self.address

    @property
    def db_encoding_query(self) -> str:
        return f"'{self.address}'"


class AddressAccount:
    def __init__(self, adr: Union[str, bytearray], prefix_length: int):
        self.prefix_length = prefix_length
        if type(adr) == str:
            self.address_bytes = hex_to_bytearray(adr)
        elif type(adr) == bytearray:
            self.address_bytes = adr
        elif type(adr) == bytes:
            self.address_bytes = bytearray(adr)
        else:
            raise Exception("Unknown address type")

        if len(self.address_bytes) != 20:
            raise ValueError(
                f"Address is not the right length {len(self.address_bytes)}"
            )

    @property
    def hex(self) -> str:
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
    def bytearray(self) -> bytearray:
        return self.address_bytes
